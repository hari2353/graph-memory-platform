"""Precompute + prewarm tests (M4.6, research 06 levers 1 + 3).

The prebuilt payload tier is a staged-rebuild store like the graph
gateway: published rows only for readers, a failed rebuild never
publishes, and the 1s projection tick refreshes it. The prewarm service
installs those rows into the context cache and reports the falsifiable
hit-rate number (prebuilt pulls vs safety-net builds).
"""

from __future__ import annotations

import asyncio
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from graph_memory.context.prefetch import PrewarmService, ProfileMaterializer
from graph_memory.context.service import ContextCache, MemoryContextProvider
from graph_memory.core.clock import FixedClock
from graph_memory.core.errors import TenantMismatch
from graph_memory.core.types import Capability, Json, Principal, Scope, TenantId, UserKey, UserRef
from graph_memory.mapping.spec import load_spec
from graph_memory.ops.cli import _projection_loop
from graph_memory.projector.engine import CapabilityUnknown, Projector
from graph_memory.projector.gateway import InMemoryGraphGateway
from graph_memory.signals.envelope import Envelope
from graph_memory.signals.ledger import InMemoryLedger, record_from_envelope

_T = TenantId("acme")
_SPEC_PATH = Path(__file__).parent.parent / "fixtures" / "mapping_with_forget.yaml"
_NOW = datetime(2026, 6, 10, tzinfo=UTC)
_LEARN_AT = datetime(2026, 6, 1, tzinfo=UTC)


class _Raising(InMemoryGraphGateway):
    """A gateway whose read of user-2 blows up mid-rebuild (the broken pass)."""

    def observations(self, tenant: TenantId, user: UserKey) -> list[Any]:
        if str(user) == "user-2":
            raise RuntimeError("boom mid-rebuild")
        return super().observations(tenant, user)


def _record(eid: str, user: str, skill: str) -> Any:
    envelope = Envelope.parse(
        {
            "specversion": "1.0",
            "id": eid,
            "source": "https://app.example.com/signals",
            "type": "itf.skill_assessed.v1",
            "time": _LEARN_AT.isoformat(),
            "partitionkey": user,
            "data": {"skill": skill},
        }
    )
    return record_from_envelope(_T, envelope)


def _principal() -> Principal:
    return Principal(
        tenant=_T,
        scopes=frozenset({Scope.CONTEXT_READ, Scope.MEMORY_SEARCH}),
        subject="test",
    )


class _World:
    """One engine + ledger + materializer + prewarmer under a fixed clock."""

    def __init__(self) -> None:
        self.ledger = InMemoryLedger()
        self.ledger.append([_record("p1", "user-1", "rust"), _record("p2", "user-1", "go")])
        self.ledger.append([_record("p3", "user-2", "python")])
        self.engine = Projector(
            load_spec(_SPEC_PATH.read_text(encoding="utf-8")),
            self.ledger,
            InMemoryGraphGateway(),
            FixedClock(_NOW),
        )
        self.engine.replay()
        self.materializer = ProfileMaterializer(self.engine)
        self.cache = ContextCache()
        self.prewarm = PrewarmService(self.engine, self.cache, self.materializer)


def _payload(materializer: ProfileMaterializer, user: str) -> dict[str, Json] | None:
    doc = materializer.fetch(_T, UserKey(user), Capability("skills"))
    if doc is None:
        return None
    return cast("dict[str, Json]", doc.payload)  # JSON-safe by construction


# --- the materializer: staged rebuild, published-only reads -------------------


def test_materializer_publishes_every_cell_matching_the_engine() -> None:
    world = _World()
    built = world.materializer.rebuild()
    assert built == 2  # users × capabilities: 2 users × {"skills"}
    for user in ("user-1", "user-2"):
        prebuilt = _payload(world.materializer, user)
        assert prebuilt is not None
        served = world.engine.snapshot(_T, UserKey(user), Capability("skills")).payload
        assert prebuilt == served  # same read model, frozen at the tick instant


def test_a_failed_rebuild_never_publishes_half_built_rows() -> None:
    world = _World()
    assert world.materializer.rebuild() == 2
    before = _payload(world.materializer, "user-1")

    # A pass that blows up mid-build: the published set keeps serving and
    # nothing half-built leaks (the gateway's rebuild semantics, reused).
    raising = Projector(world.engine.spec, world.ledger, _Raising(), world.engine.clock)
    raising.replay()  # populate the raising gateway; reads of user-2 die
    broken = ProfileMaterializer(raising)
    with pytest.raises(RuntimeError):
        broken.rebuild()
    assert broken.published_size() == 0  # the failed pass published nothing
    assert _payload(world.materializer, "user-1") == before  # the OLD set still warm


def test_rebuild_is_deterministic_under_the_injected_clock() -> None:
    world = _World()
    first = ProfileMaterializer(world.engine)
    second = ProfileMaterializer(world.engine)
    first.rebuild()
    second.rebuild()
    assert _payload(first, "user-1") == _payload(second, "user-1")
    doc = first.fetch(_T, UserKey("user-1"), Capability("skills"))
    assert doc is not None and doc.generated_at == _NOW


def test_tick_rebuilds_the_materialization() -> None:
    """The 1s projection loop refreshes the prebuilt tier every pass."""
    engine = _World().engine
    materializer = ProfileMaterializer(engine)
    stop = threading.Event()
    thread = threading.Thread(
        target=_projection_loop,
        args=(engine, ContextCache(), stop),
        kwargs={"precompute": materializer},
        daemon=True,
    )
    thread.start()
    try:
        deadline = time.monotonic() + 5
        while materializer.published_size() == 0 and time.monotonic() < deadline:
            time.sleep(0.05)
        assert materializer.published_size() == 2
    finally:
        stop.set()
        thread.join(timeout=5)


# --- the prewarm service: hit-rate evidence + cache warm ----------------------


def test_prewarm_serves_prebuilt_rows_and_reports_the_hit() -> None:
    world = _World()
    world.materializer.rebuild()
    report = world.prewarm.prefetch(_principal(), UserRef(_T, UserKey("user-1")), None)
    assert report["user"] == "user-1"
    assert report["requested"] == 1 and report["prebuilt"] == 1
    assert report["safety_net_builds"] == 0
    assert report["hit_rate"] == 1.0
    # The cache is warm: provider.get() now serves the SAME prebuilt doc.
    provider = MemoryContextProvider(world.engine, world.cache)
    ctx = asyncio.run(
        provider.get(_principal(), UserRef(_T, UserKey("user-1")), Capability("skills"))
    )
    served = world.engine.snapshot(_T, UserKey("user-1"), Capability("skills")).payload
    assert ctx.payload == cast("dict[str, Json]", served)
    assert ctx.generated_at == _NOW  # staleness surfaced from the prebuilt doc


def test_prewarm_without_a_tick_meters_cold_builds() -> None:
    world = _World()  # the tick has NOT materialized anything yet
    report = world.prewarm.prefetch(
        _principal(), UserRef(_T, UserKey("user-1")), [Capability("skills")]
    )
    assert report["prebuilt"] == 0 and report["safety_net_builds"] == 1
    assert report["hit_rate"] == 0.0
    # The safety net still delivers a correct, cache-warm payload.
    assert report["generated_at"] == _NOW.isoformat()


def test_prewarm_defaults_to_every_declared_capability() -> None:
    world = _World()
    world.materializer.rebuild()
    report = world.prewarm.prefetch(_principal(), UserRef(_T, UserKey("user-1")), None)
    assert report["requested"] == len(world.engine.spec.capabilities)
    assert report["cache_scopes"] == [
        f"t/acme/u/user-1/c/{name}" for name in sorted(world.engine.spec.capabilities)
    ]


def test_prewarm_validates_capability_and_tenant_boundaries() -> None:
    world = _World()
    with pytest.raises(CapabilityUnknown):
        world.prewarm.prefetch(_principal(), UserRef(_T, UserKey("user-1")), [Capability("nope")])
    with pytest.raises(TenantMismatch):
        world.prewarm.prefetch(_principal(), UserRef(TenantId("other"), UserKey("user-1")), None)
    # A user the ledger never saw under THIS tenant: their own (empty)
    # view is warmed — never another user's graph.
    report = world.prewarm.prefetch(
        _principal(), UserRef(_T, UserKey("nobody")), [Capability("skills")]
    )
    assert report["hit_rate"] == 0.0
