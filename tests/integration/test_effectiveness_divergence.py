"""Divergence e2e (SPEC §16.1) — the falsifiable flagship.

Two cohorts with IDENTICAL skill gaps. Only their outcome histories differ
(cohort-a acts on the "gap"-class reco, cohort-b acts on the "explore"-class
reco; outcomes are graph-inert loop truth). After ≥N outcomes accrue:

- the loop carrying context-service levels DIFFERENT top recommendations
  across the two cohorts (§16.1 divergence),
- the no-loop baseline returns IDENTICAL ones for everyone,
- the skills capability (the shared gaps) stays byte-identical,
- counterfactual replay: dropping the graph-inert loop signal changes
  NOTHING; dropping the reco signals changes exactly the recos.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

from graph_memory.core.clock import FixedClock
from graph_memory.core.types import Capability, Json, TenantId, UserKey
from graph_memory.evals import (
    counterfactual_replay,
    hit_rate_at_k,
    lift,
    over_personalization_probe,
)
from graph_memory.loop import EffectivenessTick, apply_effectives
from graph_memory.mapping.spec import MappingSpec, load_spec
from graph_memory.projector.engine import Projector
from graph_memory.projector.gateway import InMemoryGraphGateway
from graph_memory.signals.envelope import Envelope
from graph_memory.signals.ledger import InMemoryLedger, LedgerRecord, record_from_envelope

_T = TenantId("acme")
# The scored cohorts (outcome histories below) plus TWO cold controls:
# user-c1 carries a distinct cohort label with no outcomes ever attached;
# user-n1's recos carry NO cohort attribute at all (the "" route). Both are
# the OP-Bench probe surfaces: loop rows exist only for cohort-a/-b, so
# neither cold user may move under the loop.
_USERS = ("user-a1", "user-b1")
_COLD_USERS = ("user-c1", "user-n1")
_ALL_USERS = _USERS + _COLD_USERS
_SPEC = {
    "mapping_version": 1,
    "signals": {
        "itf.skill_assessed.v1": {
            "edges": [
                {
                    "kind": "HAS_SKILL",
                    "target": "skill:{data.skill}",
                    "weight": 1.0,
                    "half_life_days": 365,
                }
            ]
        },
        "itf.reco.v1": {
            "edges": [
                {
                    "kind": "RECOMMENDED",
                    "target": "skill:{data.skill}",
                    "weight": 0.5,
                    "half_life_days": 365,
                }
            ]
        },
        # Loop-only truth: outcomes ride the LEDGER, never the graph.
        "itf.reco_outcome.v1": {},
    },
    "effectiveness": {
        "reco_signals": ["itf.reco.v1"],
        "outcome_signals": ["itf.reco_outcome.v1"],
        "correlation_field": "data.correlation_id",
        "cohort_field": "data.cohort",
        "class_field": "data.reco_class",
        "verdict_field": "data.verdict",
    },
    "capabilities": {
        "skills": {"relations": ["HAS_SKILL"], "top_k": 5, "ttl_ms": 60000},
        "recos": {"relations": ["RECOMMENDED"], "top_k": 5, "ttl_ms": 60000},
    },
}

_MS = {"first": 123, "second": 124, "third": 125, "fourth": 126}


def _envelope(event_id: str, type_: str, user: str, ms: int, data: dict[str, object]) -> Envelope:
    return Envelope.parse(
        {
            "specversion": "1.0",
            "id": event_id,
            "source": "https://itf.example.com/signals",
            "type": type_,
            "time": f"2026-06-01T12:00:00.{ms:03d}Z",
            "partitionkey": user,
            "data": data,
        }
    )


def _ledger() -> InMemoryLedger:
    ledger = InMemoryLedger()
    records: list[LedgerRecord] = []
    for user in _USERS:
        cohort = "cohort-a" if user.endswith("a1") else "cohort-b"
        # Identical skill gaps for everyone.
        for skill in ("python", "rust"):
            records.append(
                record_from_envelope(
                    _T,
                    _envelope(
                        f"skill-{user}-{skill}",
                        "itf.skill_assessed.v1",
                        user,
                        _MS["first"],
                        {"skill": skill},
                    ),
                )
            )
        # Two reco classes, same two targets for every user.
        records.append(
            record_from_envelope(
                _T,
                _envelope(
                    f"r-{user}-python",
                    "itf.reco.v1",
                    user,
                    _MS["second"],
                    {"skill": "python", "cohort": cohort, "reco_class": "gap"},
                ),
            )
        )
        records.append(
            record_from_envelope(
                _T,
                _envelope(
                    f"r-{user}-rust",
                    "itf.reco.v1",
                    user,
                    _MS["second"],
                    {"skill": "rust", "cohort": cohort, "reco_class": "explore"},
                ),
            )
        )
        # Outcome histories: cohort-a acts on their gap-reco (python);
        # cohort-b acts on their explore-reco (rust). Mirrored dismissals.
        verdicts = {
            "user-a1": [("python", "gap", "acted_on"), ("rust", "explore", "dismissed")],
            "user-b1": [("python", "gap", "dismissed"), ("rust", "explore", "acted_on")],
        }
        for ms, (skill, _class, verdict) in zip(("third", "fourth"), verdicts[user], strict=False):
            records.append(
                record_from_envelope(
                    _T,
                    _envelope(
                        f"outcome-{user}-{skill}",
                        "itf.reco_outcome.v1",
                        user,
                        _MS[ms],
                        {"correlation_id": f"r-{user}-{skill}", "verdict": verdict},
                    ),
                )
            )
    # — Cold controls (M4.3): recos exist, outcomes never arrive. user-c1
    # labels its own (unscored) cohort; user-n1's recos OMIT the cohort
    # attribute entirely (the "" route — the leak-prone default a buggy
    # weight writer would land shared rows under).
    for cold_user in _COLD_USERS:
        cold_cohort = None if cold_user.endswith("n1") else "cohort-c"
        for skill in ("python", "rust"):
            records.append(
                record_from_envelope(
                    _T,
                    _envelope(
                        f"skill-{cold_user}-{skill}",
                        "itf.skill_assessed.v1",
                        cold_user,
                        _MS["first"],
                        {"skill": skill},
                    ),
                )
            )
        for skill, reco_class in (("python", "gap"), ("rust", "explore")):
            cold_data: dict[str, object] = {"skill": skill, "reco_class": reco_class}
            if cold_cohort is not None:
                cold_data["cohort"] = cold_cohort
            records.append(
                record_from_envelope(
                    _T,
                    _envelope(
                        f"r-{cold_user}-{skill}",
                        "itf.reco.v1",
                        cold_user,
                        _MS["second"],
                        cold_data,
                    ),
                )
            )
    ledger.append(records)
    return ledger


def _spec() -> MappingSpec:
    return load_spec(json.dumps(_SPEC))


def test_loop_diverges_cohorts_with_identical_skill_gaps() -> None:
    spec = _spec()
    clock = FixedClock(datetime(2026, 6, 10, tzinfo=UTC))
    ledger = _ledger()

    # — Baseline (loop off): identical recommendations for everyone.
    baseline = InMemoryGraphGateway()
    Projector(spec, ledger, baseline, clock).replay()
    baseline_views = {}
    for user in _USERS:
        doc = Projector(spec, ledger, baseline, clock).snapshot(
            _T,
            UserKey(user),
            Capability("recos"),
        )
        baseline_views[user] = (doc.payload["items"],)
    a_items = list(baseline_views["user-a1"])
    b_items = list(baseline_views["user-b1"])
    assert a_items == b_items  # the no-loop baseline is cohort-blind

    # — Loop on: the tick writes per-cohort weight rows, replay reprojects.
    report = EffectivenessTick(spec, ledger, clock).score(_T)
    cells = {(cell.cohort, cell.reco_class): cell for cell in report.cells}
    assert cells[("cohort-a", "gap")].multiplier == 0.5 + 2 / 3  # acted: rate (1+1)/3
    assert cells[("cohort-a", "gap")].capability == "recos"
    assert cells[("cohort-b", "gap")].multiplier == 0.5 + 1 / 3  # dismissed: rate (0+1)/3
    assert cells[("cohort-b", "explore")].multiplier == 0.5 + 2 / 3
    assert cells[("cohort-a", "explore")].multiplier == 0.5 + 1 / 3

    loop_gateway = InMemoryGraphGateway()
    apply_effectives(spec, ledger, loop_gateway, clock)
    loop_views: dict[str, list[str]] = {}
    for user in _USERS:
        doc = Projector(spec, ledger, loop_gateway, clock).snapshot(
            _T, UserKey(user), Capability("recos")
        )
        items = doc.payload["items"]
        assert isinstance(items, list)
        loop_views[user] = [str(item["target"]) for item in items]
    # DIVERGENCE: different top recommendation per cohort, per §16.1.
    assert loop_views["user-a1"][0] == "skill:python"
    assert loop_views["user-b1"][0] == "skill:rust"
    assert loop_views["user-a1"] != loop_views["user-b1"]

    # Identical skill gaps survived untouched (skills capability equal).
    for gateway in (baseline, loop_gateway):
        skill_views = [
            Projector(spec, ledger, gateway, clock)
            .snapshot(_T, UserKey(user), Capability("skills"))
            .payload["items"]
            for user in _USERS
        ]
        assert skill_views[0] == skill_views[1]


def test_counterfactual_replay_is_exact() -> None:
    spec = _spec()
    clock = FixedClock(datetime(2026, 6, 10, tzinfo=UTC))
    ledger = _ledger()
    users = tuple(UserKey(user) for user in _USERS)

    # The loop-only outcome signal is graph-inert: replaying without it
    # changes NOTHING (noise controllability — the rigorous half of the
    # counterfactual control).
    outcomes_only = counterfactual_replay(
        spec,
        ledger,
        clock,
        _T,
        "recos",
        drop=lambda event: event.signal == "itf.reco_outcome.v1",
        users=users,
    )
    assert all(not delta.diverged for delta in outcomes_only)

    # Dropping the reco events diverges exactly the reco capability...
    reco_drop = counterfactual_replay(
        spec,
        ledger,
        clock,
        _T,
        "recos",
        drop=lambda event: event.signal == "itf.reco.v1",
        users=users,
    )
    assert all(delta.diverged and delta.after == () for delta in reco_drop)

    # ...and never the skills capability.
    skills_drop = counterfactual_replay(
        spec,
        ledger,
        clock,
        _T,
        "skills",
        drop=lambda event: event.signal == "itf.reco.v1",
        users=users,
    )
    assert all(not delta.diverged for delta in skills_drop)


def test_eval_reports_hit_rate_and_lift() -> None:
    spec = _spec()
    clock = FixedClock(datetime(2026, 6, 10, tzinfo=UTC))
    ledger = _ledger()
    expectations = {UserKey("user-a1"): "skill:python", UserKey("user-b1"): "skill:rust"}
    baseline_gateway = InMemoryGraphGateway()
    Projector(spec, ledger, baseline_gateway, clock).replay()
    baseline_engine = Projector(spec, ledger, baseline_gateway, clock)
    assert hit_rate_at_k(baseline_engine, _T, "recos", expectations, k=1) == 0.5
    # The baseline ranks by alphabetical tie-break, so both cohorts see
    # python first: only one cohort's needle sits on top.

    loop_gateway = InMemoryGraphGateway()
    apply_effectives(spec, ledger, loop_gateway, clock)
    loop_engine = Projector(spec, ledger, loop_gateway, clock)
    with_loop = hit_rate_at_k(loop_engine, _T, "recos", expectations, k=1)
    assert with_loop == 1.0
    # §16.7: the loop carries the lift.
    assert lift(with_loop, 0.5) == 0.5


def _data(value: Json, key: str) -> dict[str, Json]:
    assert isinstance(value, dict), f"{key} must be a JSON object"
    return value


def _tops(engine: Projector, user: str, capability: str) -> tuple[str, ...]:
    items = engine.snapshot(_T, UserKey(user), Capability(capability)).payload["items"]
    assert isinstance(items, list)
    return tuple(str(item["target"]) for item in items if isinstance(item, dict))


def test_cold_cohorts_never_move_under_the_loop() -> None:
    """M4.3 over-personalization guard (research 06 benchmark chapter).

    Weight rows are per-cohort, so a cohort with NO outcome evidence keeps
    its own baseline byte-identical while evidence-carrying cohorts diverge.
    """
    spec = _spec()
    clock = FixedClock(datetime(2026, 6, 10, tzinfo=UTC))
    ledger = _ledger()

    baseline_engine = Projector(spec, ledger, InMemoryGraphGateway(), clock)
    baseline_engine.replay()

    # The tick scores ONLY cohorts that own attached outcomes.
    report = EffectivenessTick(spec, ledger, clock).score(_T)
    assert {cell.cohort for cell in report.cells} == {"cohort-a", "cohort-b"}

    loop_gateway = InMemoryGraphGateway()
    apply_effectives(spec, ledger, loop_gateway, clock)
    loop_engine = Projector(spec, ledger, loop_gateway, clock)

    # Cold: loop top == that user's OWN baseline top, byte-identical.
    for cold_user in _COLD_USERS:
        assert _tops(loop_engine, cold_user, "recos") == _tops(baseline_engine, cold_user, "recos")
    # Warm: divergence still happens where the evidence is.
    assert _tops(loop_engine, "user-a1", "recos")[0] == "skill:python"
    assert _tops(loop_engine, "user-b1", "recos")[0] == "skill:rust"
    assert _tops(loop_engine, "user-a1", "recos") != _tops(loop_engine, "user-b1", "recos")


def test_over_personalization_probe_publishes_the_matrix() -> None:
    """The published OP-Bench-style matrix: warm diverges, cold never degrades."""
    spec = _spec()
    clock = FixedClock(datetime(2026, 6, 10, tzinfo=UTC))
    ledger = _ledger()
    cohorts = {
        "cohort-a": (UserKey("user-a1"),),
        "cohort-b": (UserKey("user-b1"),),
        "cohort-c": (UserKey("user-c1"),),
        "": (UserKey("user-n1"),),
    }
    needles = {
        UserKey("user-a1"): "skill:python",
        UserKey("user-b1"): "skill:rust",
        UserKey("user-c1"): "skill:python",
        UserKey("user-n1"): "skill:python",
    }

    def run() -> dict[str, Json]:
        return over_personalization_probe(
            spec, ledger, clock, _T, "recos", cohorts=cohorts, needles=needles
        )

    matrix = run()
    assert matrix["warm_cohorts"] == ["cohort-a", "cohort-b"]
    assert matrix["cold_cohorts"] == ["", "cohort-c"]
    assert matrix["baseline_cohort_blind"] is True
    # Cohort-a's baseline tie-break ALREADY agrees with its evidence, so its
    # per-regime top is unchanged; the pairwise §16.1 divergence (cohort-b
    # flips) rides the warm-top distinctness instead.
    assert matrix["warm_diverged_cohorts"] == ["cohort-b"]
    assert matrix["warm_tops_mutually_distinct"] is True
    assert matrix["cold_degraded_cohorts"] == []
    assert matrix["beat_baseline"] is True
    assert matrix["verdict"] == "no-over-personalization"
    rates = _data(matrix["hit_rate"], "hit_rate")
    base = _data(rates["baseline"], "baseline")
    loop = _data(rates["loop"], "loop")
    assert loop["all"] == 1.0 and base["all"] == 0.75  # needles incl. cold hits
    assert loop["warm"] == 1.0 and base["warm"] == 0.5  # warm-only: the §16.1 lift
    assert matrix["beat_baseline"] is True and matrix["beat_baseline_warm"] is True
    assert base["cold"] == loop["cold"] and base["cold"] is not None  # cold untouched
    # Same (ledger, spec, clock) -> byte-identical published matrix.
    assert run() == matrix


def test_probe_flags_a_cohort_leak_as_over_personalization() -> None:
    """Negative control: the guard FAILS when a writer leaks rows onto the
    shared "" route — fresh users degrade and the verdict says so."""
    spec = _spec()
    clock = FixedClock(datetime(2026, 6, 10, tzinfo=UTC))
    ledger = _ledger()
    matrix = over_personalization_probe(
        spec,
        ledger,
        clock,
        _T,
        "recos",
        cohorts={
            "cohort-a": (UserKey("user-a1"),),
            "cohort-c": (UserKey("user-c1"),),
            "": (UserKey("user-n1"),),
        },
        needles={
            UserKey("user-a1"): "skill:python",
            UserKey("user-c1"): "skill:python",
            UserKey("user-n1"): "skill:python",
        },
        override_rows={("RECOMMENDED", "skill:rust", ""): 1.5},
    )
    # The "" cohort's users flip (their rust rewang outranks python); the
    # cohort-keyed cohorts keep their exact per-cohort semantics.
    assert matrix["cold_degraded_cohorts"] == [""]
    assert matrix["verdict"] == "over-personalized"
    assert matrix["beat_baseline"] is False
