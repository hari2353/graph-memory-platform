"""Market-steal additions: point-in-time reads (Zep/Graphiti's headline
correctness property), a human-readable memory audit (Letta/Mem0's 'can a
person read it?' column), and a user change timeline from the append-only
ledger (Letta's every-edit-is-a-commit audit)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from graph_memory.core.clock import FixedClock
from graph_memory.core.types import TenantId, UserKey
from graph_memory.mapping.spec import load_spec
from graph_memory.projector.engine import Projector
from graph_memory.projector.gateway import InMemoryGraphGateway
from graph_memory.signals.envelope import Envelope
from graph_memory.signals.ledger import InMemoryLedger, record_from_envelope

_T = TenantId("acme")
_U = UserKey("user-1")
_FORGET_SPEC = Path(__file__).parent.parent / "fixtures" / "mapping_with_forget.yaml"
_NOW = datetime(2026, 6, 10, tzinfo=UTC)


def _envelope(event_id: str, type_: str, time_: str, **data: object) -> Envelope:
    payload: dict[str, object] = {
        "specversion": "1.0",
        "id": event_id,
        "source": "https://app.example.com/signals",
        "type": type_,
        "time": time_,
        "partitionkey": "user-1",
        "data": dict(data),
    }
    return Envelope.parse(payload)


def _engine() -> tuple[Projector, InMemoryLedger]:
    spec = load_spec(_FORGET_SPEC.read_text(encoding="utf-8"))
    ledger = InMemoryLedger()
    engine = Projector(spec, ledger, InMemoryGraphGateway(), FixedClock(_NOW))
    ledger.append([
        record_from_envelope(_T, _envelope(
            "e1", "itf.skill_assessed.v1", "2026-06-01T12:00:00.100Z",
            skill="rust", score=0.8)),
        # a LATER fact, ingested before 'now' but after the slice
        record_from_envelope(_T, _envelope(
            "e2", "itf.skill_assessed.v1", "2026-06-08T12:00:00.100Z",
            skill="kafka", score=0.9)),
        # the whole HAS_SKILL kind is forgotten on 06-05 (between the two)
        record_from_envelope(_T, _envelope(
            "f1", "itf.forget_all_skills.v1", "2026-06-05T12:00:00.100Z")),
    ])
    engine.process_events(ledger.read_events(_T))
    return engine, ledger


# ── point-in-time reads ──────────────────────────────────────────────────────
def test_historical_slice_resurrects_a_later_forgotten_fact() -> None:
    engine, _ = _engine()
    now_targets = [
        item["target"] for item in engine.snapshot(_T, _U, "skills").payload["items"]
    ]
    assert now_targets == ["skill:kafka"]
    # the 06-05 forget kills only facts known by then: rust (06-01) dies,
    # kafka (06-08) post-dates the forget and survives at 'now'

    slice_doc = engine.snapshot_at(
        _T, _U, "skills", as_at=datetime(2026, 6, 3, tzinfo=UTC))
    slice_targets = [item["target"] for item in slice_doc.payload["items"]]
    assert slice_targets == ["skill:rust"]  # rust was alive on 06-03


def test_future_facts_are_absent_from_a_historical_slice() -> None:
    engine, _ = _engine()
    slice_doc = engine.snapshot_at(
        _T, _U, "skills", as_at=datetime(2026, 6, 3, tzinfo=UTC))
    targets = [item["target"] for item in slice_doc.payload["items"]]
    assert "skill:kafka" not in targets  # first observed 06-08


def test_snapshot_at_now_equals_the_default_snapshot() -> None:
    engine, _ = _engine()
    assert (
        engine.snapshot_at(_T, _U, "skills", as_at=_NOW).payload
        == engine.snapshot(_T, _U, "skills").payload
    )


def test_gateway_observations_at_agrees_with_visibility_predicate() -> None:
    engine, _ = _engine()
    gateway = engine.gateway
    live_now = {(r.kind, r.target) for r in gateway.observations(_T, _U)}
    assert live_now == {("HAS_SKILL", "skill:kafka")}  # rust is forgotten at now
    at_slice = {
        (r.kind, r.target)
        for r in gateway.observations_at(_T, _U, datetime(2026, 6, 3, tzinfo=UTC))
    }
    assert at_slice == {("HAS_SKILL", "skill:rust")}   # alive before the forget


# ── human-readable memory audit ──────────────────────────────────────────────
def test_inspect_user_renders_active_memory_and_forget_ledger() -> None:
    from graph_memory.context.inspect import inspect_user

    engine, _ = _engine()
    # rebuild the engine's state without the forget so there is an ACTIVE fact
    spec = load_spec(_FORGET_SPEC.read_text(encoding="utf-8"))
    ledger = InMemoryLedger()
    fresh = Projector(spec, ledger, InMemoryGraphGateway(), FixedClock(_NOW))
    ledger.append([
        record_from_envelope(_T, _envelope(
            "e1", "itf.skill_assessed.v1", "2026-06-01T12:00:00.100Z",
            skill="rust", score=0.8)),
    ])
    fresh.process_events(ledger.read_events(_T))

    markdown = inspect_user(fresh, _T, _U)
    assert "skill:rust" in markdown
    assert "HAS_SKILL" in markdown
    assert "2026-06-01" in markdown            # validity instant, human-readable
    assert "# memory audit" in markdown.lower() or "## " in markdown

    # the forgotten engine shows the forget ledger, not silent absence
    forgotten_md = inspect_user(engine, _T, _U)
    assert "forget" in forgotten_md.lower()
    assert "HAS_SKILL" in forgotten_md         # the directive names its kind
    assert "2026-06-05" in forgotten_md        # and when


def test_inspect_user_output_is_bounded() -> None:
    from graph_memory.context.inspect import inspect_user

    spec = load_spec(_FORGET_SPEC.read_text(encoding="utf-8"))
    ledger = InMemoryLedger()
    engine = Projector(spec, ledger, InMemoryGraphGateway(), FixedClock(_NOW))
    records = [
        record_from_envelope(_T, _envelope(
            f"e{i}", "itf.skill_assessed.v1", "2026-06-01T12:00:00.100Z",
            skill=f"skill-{i}", score=0.5))
        for i in range(300)
    ]
    ledger.append(records)
    engine.process_events(ledger.read_events(_T))
    markdown = inspect_user(engine, _T, _U)
    assert len(markdown) < 20_000               # bounded, not a 300-row dump


# ── user change timeline ─────────────────────────────────────────────────────
def test_user_history_is_ordered_bounded_and_filtered() -> None:
    from graph_memory.context.timeline import user_history

    engine, ledger = _engine()
    entries = user_history(ledger, _T, _U)
    assert [e.sequence for e in entries] == sorted(e.sequence for e in entries)
    kinds = {e.signal for e in entries}
    assert "itf.forget_all_skills.v1" in kinds   # forgets are visible history
    assert "itf.skill_assessed.v1" in kinds

    since = user_history(ledger, _T, _U, since=datetime(2026, 6, 6, tzinfo=UTC))
    assert all(e.occurred_at >= datetime(2026, 6, 6, tzinfo=UTC) for e in since)
    assert {e.signal for e in since} == {"itf.skill_assessed.v1"}  # kafka only

    bounded = user_history(ledger, _T, _U, limit=1)
    assert len(bounded) == 1
    assert bounded[0].dedup_key  # evidence: which signal produced the entry


def test_mcp_get_context_accepts_as_of_and_rejects_garbage() -> None:
    """The MCP tool's as_of path: a valid instant gives the historical
    slice; a malformed one is a caller error, never a crash."""
    import asyncio

    from graph_memory.context.service import parse_as_of
    from graph_memory.core.errors import InvalidRequest

    parsed = parse_as_of("2026-06-03T00:00:00Z")
    assert parsed is not None and parsed.year == 2026
    assert parse_as_of(None) is None
    try:
        parse_as_of("not-a-date")
    except InvalidRequest as exc:
        assert exc.code == "invalid_request"
    else:
        raise AssertionError("garbage as_of must raise InvalidRequest")


def test_rest_as_of_invalid_is_422() -> None:
    from graph_memory.channels.rest.api import _status_for
    from graph_memory.core.errors import InvalidRequest

    assert _status_for(InvalidRequest("bad as_of")) == 422
