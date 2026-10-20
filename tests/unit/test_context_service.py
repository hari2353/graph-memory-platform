"""Context service tests: hot-path law (verify → cached point lookup → staleness)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from graph_memory.context.service import ContextCache, MemoryContextProvider
from graph_memory.core.clock import FixedClock
from graph_memory.core.errors import ScopeDenied, TenantMismatch
from graph_memory.core.types import (
    Capability,
    Principal,
    Scope,
    TenantId,
    UserKey,
    UserRef,
)
from graph_memory.mapping.spec import MappingSpec, load_spec
from graph_memory.projector.engine import Projector
from graph_memory.projector.gateway import InMemoryGraphGateway
from graph_memory.signals.envelope import Envelope
from graph_memory.signals.ledger import InMemoryLedger, record_from_envelope

_T = TenantId("acme")
_SPEC_PATH = Path(__file__).parent.parent / "fixtures" / "mapping_skill_loop.yaml"


def _envelope(event_id: str, *, user: str = "user-1", ms: int = 123) -> Envelope:
    payload: dict[str, object] = {
        "specversion": "1.0",
        "id": event_id,
        "source": "https://app.example.com/signals",
        "type": "itf.skill_assessed.v1",
        "time": f"2026-06-01T12:00:00.{ms:03d}Z",
        "partitionkey": user,
        "data": {"skill": "rust", "score": 0.8},
    }
    return Envelope.parse(payload)


def _engine() -> tuple[Projector, InMemoryLedger]:
    spec: MappingSpec = load_spec(_SPEC_PATH.read_text(encoding="utf-8"))
    ledger = InMemoryLedger()
    gateway = InMemoryGraphGateway()
    clock = FixedClock(datetime(2026, 6, 10, tzinfo=UTC))
    engine = Projector(spec, ledger, gateway, clock)
    return engine, ledger


def _provider() -> tuple[MemoryContextProvider, Projector, InMemoryLedger]:
    engine, ledger = _engine()
    ledger.append([record_from_envelope(_T, _envelope("a"))])
    engine.process_events(ledger.read_events(_T))
    return MemoryContextProvider(engine, ContextCache()), engine, ledger


_PRINCIPAL = Principal(
    tenant=_T, scopes=frozenset({Scope.CONTEXT_READ, Scope.SIGNALS_WRITE}), subject="subj"
)


async def test_get_returns_staleness_contract() -> None:
    context_provider, _engine_ref, _ledger = _provider()
    ctx = await context_provider.get(
        _PRINCIPAL, UserRef(_T, UserKey("user-1")), Capability("skills")
    )
    items = ctx.payload["items"]
    assert isinstance(items, list)
    (first,) = [entry for entry in items if isinstance(entry, dict)]
    assert first["target"] == "skill:rust"
    assert ctx.max_age_ms == 60_000
    assert ctx.generated_at == datetime(2026, 6, 10, tzinfo=UTC)
    assert not ctx.is_stale(ctx.generated_at + timedelta(milliseconds=59_000))
    assert ctx.is_stale(ctx.generated_at + timedelta(milliseconds=61_000))


async def test_cache_hits_reuse_generated_doc() -> None:
    context_provider, engine, ledger = _provider()
    ledger.append([record_from_envelope(_T, _envelope("b"))])
    engine.process_events(ledger.read_events(_T))
    ref = UserRef(_T, UserKey("user-1"))
    first = await context_provider.get(_PRINCIPAL, ref, Capability("skills"))
    second = await context_provider.get(_PRINCIPAL, ref, Capability("skills"))
    assert second.generated_at == first.generated_at  # the cache window honored
    context_provider._cache.invalidate((str(_T), "user-1", "skills"))  # noqa: SLF001
    clock = cast("FixedClock", engine._clock)  # noqa: SLF001 — the clock is Fixed in tests
    clock.tick(120)
    refreshed = await context_provider.get(_PRINCIPAL, ref, Capability("skills"))
    assert refreshed.generated_at > first.generated_at


async def test_cross_tenant_access_is_denied() -> None:
    context_provider, _engine_ref, _ledger = _provider()
    intruder = UserRef(TenantId("other"), UserKey("user-1"))
    with pytest.raises(TenantMismatch):
        await context_provider.get(_PRINCIPAL, intruder, Capability("skills"))


async def test_scopeless_principal_is_denied() -> None:
    context_provider, _engine_ref, _ledger = _provider()
    scopeless = Principal(tenant=_T, scopes=frozenset({Scope.SIGNALS_WRITE}), subject="nocontext")
    with pytest.raises(ScopeDenied):
        await context_provider.get(scopeless, UserRef(_T, UserKey("user-1")), Capability("skills"))
