"""Effectiveness loop tests: attach-by-correlation, deterministic scoring,
weight rows through the pure compiler, and the divergence they must cause."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from graph_memory.core.clock import FixedClock
from graph_memory.core.types import TenantId, UserKey
from graph_memory.loop import EffectivenessTick, apply_effectives
from graph_memory.mapping.spec import load_spec
from graph_memory.projector.engine import Projector
from graph_memory.projector.gateway import InMemoryGraphGateway
from graph_memory.signals.envelope import Envelope
from graph_memory.signals.ledger import InMemoryLedger, record_from_envelope

_T = TenantId("acme")
_SPEC_PATH = Path(__file__).parent.parent / "fixtures" / "mapping_skill_loop.yaml"


def _spec_text() -> str:
    return _SPEC_PATH.read_text(encoding="utf-8")


def _envelope(
    event_id: str,
    *,
    type_: str = "itf.reco.v1",
    user: str = "user-1",
    ms: int = 123,
    data: dict[str, object] | None = None,
) -> Envelope:
    payload: dict[str, object] = {
        "specversion": "1.0",
        "id": event_id,
        "source": "https://app.example.com/signals",
        "type": type_,
        "time": f"2026-06-01T12:00:00.{ms:03d}Z",
        "partitionkey": user,
        "data": {"skill": "rust", "cohort": "cohort-a", **(data or {})},
    }
    return Envelope.parse(payload)


def _ledger(*envelopes: Envelope) -> InMemoryLedger:
    ledger = InMemoryLedger()
    for envelope in envelopes:
        ledger.append([record_from_envelope(_T, envelope)])
    return ledger


def _tick(ledger: InMemoryLedger) -> EffectivenessTick:
    spec = load_spec(_spec_text())
    clock = FixedClock(datetime(2026, 6, 10, tzinfo=UTC))
    return EffectivenessTick(spec, ledger, clock)


def test_outcomes_attach_by_correlation_and_score() -> None:
    ledger = _ledger(
        _envelope("r1", data={"skill": "rust", "cohort": "a"}),
        _envelope("r2", data={"skill": "python", "cohort": "b"}, ms=124),
        _envelope(
            "o1",
            type_="itf.outcome.v1",
            ms=125,
            data={"skill": "rust", "correlation_id": "r1", "verdict": "acted_on"},
        ),
        _envelope(
            "o2",
            type_="itf.outcome.v1",
            ms=126,
            data={"skill": "rust", "correlation_id": "r1", "verdict": "acted_on"},
        ),
        _envelope(
            "o3",
            type_="itf.outcome.v1",
            ms=127,
            user="user-2",
            data={"skill": "python", "correlation_id": "r2", "verdict": "dismissed"},
        ),
    )
    report = _tick(ledger).score(_T)
    assert report.reco_delivered == 2
    assert report.attached == 3
    assert report.unattached == 0
    cells = {(cell.cohort, cell.reco_class): cell for cell in report.cells}
    a_cell = cells[("a", "")]
    b_cell = cells[("b", "")]
    # Laplace (alpha=1): 2 acted / 2 → rate (2+1)/(2+2)=0.75 → 1.25
    assert a_cell.rate == 0.75
    assert a_cell.multiplier == 1.25
    # 1 dismissed / 1 → rate (0+1)/3 ≈ 0.333 → 0.8333
    assert b_cell.rate == (0 + 1) / 3
    assert b_cell.multiplier == 0.5 + 1 / 3
    # Weight rows ride the origin's compiled edge targets, per cohort.
    assert report.weight_overrides == {
        ("RECOMMENDED", "skill:rust", "a"): 1.25,
        ("RECOMMENDED", "skill:python", "b"): 0.5 + 1 / 3,
    }


def test_unattached_outcomes_are_counted_never_dropped() -> None:
    ledger = _ledger(
        _envelope(
            "o-ghost",
            type_="itf.outcome.v1",
            ms=125,
            data={"skill": "rust", "correlation_id": "ghost", "verdict": "acted_on"},
        ),
    )
    report = _tick(ledger).score(_T)
    assert report.unattached == 1
    assert report.attached == 0
    assert report.cells == ()
    assert report.weight_overrides == {}


def test_tick_is_deterministic() -> None:
    envelopes = (
        _envelope("r1", data={"skill": "rust", "cohort": "a"}),
        _envelope(
            "o1",
            type_="itf.outcome.v1",
            ms=125,
            data={"skill": "rust", "correlation_id": "r1", "verdict": "outcome_improved"},
        ),
    )
    ledger = _ledger(*envelopes)
    first = _tick(ledger).score(_T)
    second = _tick(ledger).score(_T)
    assert first == second


def test_cells_carry_the_capability_the_reco_serves() -> None:
    ledger = _ledger(
        _envelope("r1", data={"skill": "rust", "cohort": "a"}),
        _envelope(
            "o1",
            type_="itf.outcome.v1",
            ms=125,
            data={"skill": "rust", "correlation_id": "r1", "verdict": "acted_on"},
        ),
    )
    cells = _tick(ledger).score(_T).cells
    assert len(cells) == 1
    assert cells[0].capability == "reco_effect"  # RECOMMENDED is exposed there


def test_weight_rows_move_gateway_edges_in_one_replay() -> None:
    spec = load_spec(_spec_text())
    clock = FixedClock(datetime(2026, 6, 10, tzinfo=UTC))
    # Baseline first: without the loop the reco lands at the spec weight.
    ledger = _ledger(
        _envelope("r1", data={"skill": "rust", "cohort": "a"}),
        _envelope(
            "o1",
            type_="itf.outcome.v1",
            ms=125,
            data={"skill": "rust", "correlation_id": "r1", "verdict": "acted_on", "cohort": "a"},
        ),
    )
    baseline_gateway = InMemoryGraphGateway()
    Projector(spec, ledger, baseline_gateway, clock).replay()
    baseline_weight = [
        row.weight
        for row in baseline_gateway.observations(_T, UserKey("user-1"))
        if row.kind == "RECOMMENDED" and row.target == "skill:rust"
    ]
    assert baseline_weight and baseline_weight[0] == 0.5

    # One outcome → Laplace rate (1+1)/3 = 2/3 → multiplier 0.5 + 2/3.
    reports = apply_effectives(spec, ledger, baseline_gateway, clock)
    rows = [
        row.weight
        for row in baseline_gateway.observations(_T, UserKey("user-1"))
        if row.kind == "RECOMMENDED" and row.target == "skill:rust"
    ]
    assert rows == [0.5 * (0.5 + 2 / 3)]
    assert reports[_T].weight_overrides == {("RECOMMENDED", "skill:rust", "a"): 0.5 + 2 / 3}


def test_cohorts_never_share_weight_rows_split_by_cohort() -> None:
    ledger = _ledger(
        _envelope("r1", data={"skill": "rust", "cohort": "a"}),
        _envelope("r2", ms=124, user="user-2", data={"skill": "rust", "cohort": "b"}),
        _envelope(
            "o1",
            type_="itf.outcome.v1",
            ms=125,
            data={"skill": "rust", "correlation_id": "r1", "verdict": "acted_on"},
        ),
        _envelope(
            "o2",
            type_="itf.outcome.v1",
            ms=126,
            data={"skill": "rust", "correlation_id": "r1", "verdict": "acted_on"},
        ),
        _envelope(
            "o3",
            type_="itf.outcome.v1",
            ms=127,
            data={"skill": "rust", "correlation_id": "r1", "verdict": "acted_on"},
        ),
        _envelope(
            "o4",
            type_="itf.outcome.v1",
            ms=128,
            user="user-2",
            data={"skill": "rust", "correlation_id": "r2", "verdict": "dismissed"},
        ),
    )
    report = _tick(ledger).score(_T)
    # The two cohorts touch the same (kind, target) but keep separate rows:
    # cohort-a's all-positive row (rate (3+1)/(3+2)=0.8 → 1.3) and
    # cohort-b's dismissed one (rate (0+1)/3 → 0.5 + 1/3).
    assert report.weight_overrides == {
        ("RECOMMENDED", "skill:rust", "a"): 1.3,
        ("RECOMMENDED", "skill:rust", "b"): 0.5 + 1 / 3,
    }


def test_same_cohort_class_collision_resolves_by_size_then_name() -> None:
    ledger = _ledger(
        _envelope("r1", data={"skill": "rust", "cohort": "a", "reco_class": "gap"}),
        _envelope("r2", ms=124, data={"skill": "rust", "cohort": "a", "reco_class": "explore"}),
        _envelope(
            "o1",
            type_="itf.outcome.v1",
            ms=125,
            data={"skill": "rust", "correlation_id": "r1", "verdict": "acted_on"},
        ),
        _envelope(
            "o2",
            type_="itf.outcome.v1",
            ms=126,
            data={"skill": "rust", "correlation_id": "r2", "verdict": "dismissed"},
        ),
        _envelope(
            "o3",
            type_="itf.outcome.v1",
            ms=127,
            data={"skill": "rust", "correlation_id": "r2", "verdict": "dismissed"},
        ),
    )
    report = _tick(ledger).score(_T)
    # One cohort, two classes touching skill:rust: "explore" carries more
    # outcomes (2 dismissed vs 1 acted) → its rate (0+1)/(2+2)=0.25 row
    # (multiplier 0.75) wins the collision.
    assert report.weight_overrides == {
        ("RECOMMENDED", "skill:rust", "a"): 0.75,
    }


_NO_LOOP_SPEC = """\
mapping_version: 1
signals:
  itf.skill_assessed.v1:
    edges:
      - kind: HAS_SKILL
        target: "skill:{data.skill}"
        weight: 1.0
        half_life_days: 180
  itf.reco.v1:
    edges:
      - kind: RECOMMENDED
        target: "skill:{data.skill}"
        weight: 0.5
        half_life_days: 14
"""


def test_spec_without_effectiveness_yields_a_null_tick() -> None:
    spec = load_spec(_NO_LOOP_SPEC)
    assert spec.effectiveness is None
    ledger = _ledger(_envelope("r1"))
    report = EffectivenessTick(spec, ledger, FixedClock(datetime(2026, 6, 10, tzinfo=UTC))).score(
        _T
    )
    assert report.cells == ()
    assert report.reco_delivered == 0
    assert report.weight_overrides == {}


def test_alpha_zero_pins_the_floor_for_all_negative_verdicts() -> None:
    spec = load_spec(
        _spec_text().replace("  correlation_field:", "  alpha: 0.0\n  correlation_field:")
    )
    ledger = _ledger(
        _envelope("r1", data={"skill": "rust"}),
        _envelope(
            "o1",
            type_="itf.outcome.v1",
            ms=125,
            data={"skill": "rust", "correlation_id": "r1", "verdict": "dismissed"},
        ),
    )
    report = EffectivenessTick(spec, ledger, FixedClock(datetime(2026, 6, 10, tzinfo=UTC))).score(
        _T
    )
    assert report.cells[0].rate == 0.0
    assert report.cells[0].multiplier == 0.5
