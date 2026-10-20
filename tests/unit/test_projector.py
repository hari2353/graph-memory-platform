"""Projector tests: determinism, replay convergence, decay math, snapshots."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from graph_memory.core.clock import FixedClock
from graph_memory.core.types import Capability, TenantId, UserKey
from graph_memory.mapping.spec import load_spec
from graph_memory.projector.engine import CapabilityUnknown, Projector, decayed_score
from graph_memory.projector.gateway import InMemoryGraphGateway
from graph_memory.signals.envelope import Envelope
from graph_memory.signals.ledger import InMemoryLedger, record_from_envelope

_T = TenantId("acme")
_SPEC_PATH = Path(__file__).parent.parent / "fixtures" / "mapping_skill_loop.yaml"


def _envelope(
    event_id: str, *, user: str = "user-1", ms: int = 123, **overrides: object
) -> Envelope:
    payload: dict[str, object] = {
        "specversion": "1.0",
        "id": event_id,
        "source": "https://app.example.com/signals",
        "type": "itf.skill_assessed.v1",
        "time": f"2026-06-01T12:00:00.{ms:03d}Z",
        "partitionkey": user,
        "data": {"skill": "rust", "score": 0.8},
    }
    payload.update(overrides)
    for key, value in list(payload.items()):
        if value is None:
            del payload[key]
    return Envelope.parse(payload)


def _engine(seed_events: int = 5) -> tuple[Projector, InMemoryLedger]:
    spec = load_spec(_SPEC_PATH.read_text(encoding="utf-8"))
    ledger = InMemoryLedger()
    gateway = InMemoryGraphGateway()
    clock = FixedClock(datetime(2026, 6, 10, tzinfo=UTC))
    for index in range(seed_events):
        envelope = _envelope(f"e{index}", ms=123 + index)
        ledger.append([record_from_envelope(_T, envelope)])
    return Projector(spec, ledger, gateway, clock), ledger


def test_incremental_projection_creates_observations_and_nodes() -> None:
    engine, ledger = _engine()
    digest = engine.process_events(ledger.read_events(_T))
    assert (digest.events, digest.plans) == (5, 5)
    assert digest.node_ops == 5  # one shared node key, upserted per event
    rows = engine._gateway.observations(_T, UserKey("user-1"))  # noqa: SLF001 — engine internals in-test
    assert len(rows) == 5


def test_decay_score_math() -> None:
    start = datetime(2026, 6, 1, tzinfo=UTC)
    assert decayed_score(1.0, 180, start, start) == 1.0
    later = start + timedelta(days=180)
    assert decayed_score(1.0, 180, start, later) == pytest.approx(0.5)
    negative = decayed_score(0.7, 100, start, start - timedelta(days=5))
    assert negative == 0.7  # future-mentioning events clamp to full weight


def test_replay_converges_with_incremental_even_after_duplicates() -> None:
    engine, ledger = _engine(seed_events=4)
    engine.process_events(ledger.read_events(_T))
    baseline = {
        (row.kind, row.target, row.valid_at, str(row.user))
        for row in engine._gateway.observations(_T, UserKey("user-1"))
    }
    incremental_nodes = {node.key for node in engine._gateway.nodes_of(_T, ["skill:rust"])}

    digests = engine.replay()
    rebuilt = {
        (row.kind, row.target, row.valid_at, str(row.user))
        for row in engine._gateway.observations(_T, UserKey("user-1"))
    }
    assert rebuilt == baseline
    assert {node.key for node in engine._gateway.nodes_of(_T, ["skill:rust"])} == incremental_nodes
    assert digests[_T].events == 4


def test_permuted_delivery_produces_identical_state() -> None:
    engine, ledger = _engine(seed_events=6)
    events = ledger.read_events(_T)
    import random

    def _state(view_projector: Projector) -> set[tuple[object, ...]]:
        gateway = InMemoryGraphGateway()
        probe = Projector(view_projector._spec, ledger, gateway, view_projector._clock)  # noqa: SLF001
        probe.process_events(list(reversed(events)))
        return {
            (row.kind, row.target, row.valid_at, row.weight, str(row.user))
            for row in gateway.observations(_T, UserKey("user-1"))
        }

    engine.process_events(events)
    forward = _state(engine)
    shuffled = events[:]
    random.Random(7).shuffle(shuffled)
    probe = Projector(engine._spec, ledger, InMemoryGraphGateway(), engine._clock)  # noqa: SLF001
    probe.process_events(shuffled)
    assert forward == _state(probe)


def test_snapshot_orders_by_decayed_score() -> None:
    spec = load_spec(_SPEC_PATH.read_text(encoding="utf-8"))
    ledger = InMemoryLedger()
    gateway = InMemoryGraphGateway()
    now = datetime(2026, 6, 10, tzinfo=UTC)
    engine = Projector(spec, ledger, gateway, FixedClock(now))
    ledger.append(
        [
            record_from_envelope(
                _T, _envelope("rust-old", ms=100, time="2026-05-01T12:00:00.000Z")
            ),
            record_from_envelope(
                _T,
                _envelope(
                    "python-new",
                    ms=101,
                    time="2026-06-10T00:00:00.000Z",
                    data={"skill": "python"},
                ),
            ),
        ]
    )
    engine.process_events(ledger.read_events(_T))
    doc = engine.snapshot(_T, UserKey("user-1"), Capability("skills"))
    items = doc.payload["items"]
    assert isinstance(items, list)
    targets = [entry["target"] for entry in items if isinstance(entry, dict)]
    assert targets[0] == "skill:python"  # the fresh assessment dominates
    assert targets[-1] == "skill:rust"
    labels = doc.payload["labels"]
    assert isinstance(labels, dict)
    assert labels["skill:rust"] == ["skill"]
    assert doc.max_age_ms == 60_000
    assert doc.generated_at == now


def test_unknown_capability_raises() -> None:
    engine, ledger = _engine(seed_events=1)
    engine.process_events(ledger.read_events(_T))
    with pytest.raises(CapabilityUnknown, match="not declared"):
        engine.snapshot(_T, UserKey("user-1"), Capability("nope"))
