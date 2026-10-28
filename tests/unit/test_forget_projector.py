"""Gateway + engine tests for governed forgetting (M4.1, research 06).

Acceptance bar from the research doc: supersession is pure and convergent —
forget flows ride the ledger like every signal, and re-projecting (any batch
ordering, with duplicates) reproduces the same visible state. The
counterfactual control closes the loop: dropping the forget event diverts the
projection back toward the unforgotten ranking.
"""

from __future__ import annotations

from datetime import UTC, datetime
from itertools import permutations
from pathlib import Path
from typing import cast

from graph_memory.core.clock import FixedClock
from graph_memory.core.types import Capability, Json, TenantId, UserKey
from graph_memory.evals.harness import counterfactual_replay
from graph_memory.mapping.compiler import EdgeOp, ForgetOp, NodeOp
from graph_memory.mapping.spec import MappingSpec, load_spec
from graph_memory.projector.engine import ProjectionDigest, Projector
from graph_memory.projector.gateway import (
    EdgeRow,
    InMemoryGraphGateway,
    observation_is_dead,
)
from graph_memory.signals.envelope import Envelope
from graph_memory.signals.ledger import InMemoryLedger, StoredEvent, record_from_envelope

_T = TenantId("acme")
_U = UserKey("user-1")
_SPEC_PATH = Path(__file__).parent.parent / "fixtures" / "mapping_with_forget.yaml"
_INSTANT = datetime(2026, 6, 10, tzinfo=UTC)


def _spec() -> MappingSpec:
    return load_spec(_SPEC_PATH.read_text(encoding="utf-8"))


def _envelope(signal: str, eid: str, data: dict[str, Json], at: datetime) -> Envelope:
    return Envelope.parse(
        {
            "specversion": "1.0",
            "id": eid,
            "source": "https://app.example.com/signals",
            "type": signal,
            "time": at.isoformat(),
            "partitionkey": "user-1",
            "data": data,
        }
    )


def _edge(
    user: UserKey, kind: str = "HAS_SKILL", target: str = "skill:rust", at: datetime = _INSTANT
) -> tuple[NodeOp | EdgeOp, ...]:
    return (
        NodeOp(tenant=_T, key=str(user), labels=("user",), props={"name": str(user)}),
        EdgeOp(
            tenant=_T,
            user=user,
            kind=kind,
            target=target,
            weight=1.0,
            half_life_days=180.0,
            valid_at=at,
        ),
    )


def _directive(target: str | None, at: datetime) -> list[ForgetOp]:
    """One directive for user-1; ``None`` = kind-wide, a str = scoped target."""
    return [ForgetOp(tenant=_T, user=_U, kind="HAS_SKILL", target=target, forget_at=at)]


def test_visibility_predicate_boundary_and_target_scope() -> None:
    """Equal instants die; a stricter instant leaves the row visible."""
    row = EdgeRow(
        tenant=_T,
        user=_U,
        kind="HAS_SKILL",
        target="skill:rust",
        weight=1.0,
        half_life_days=180.0,
        valid_at=datetime(2026, 6, 5, tzinfo=UTC),
    )
    equal = ForgetOp(tenant=_T, user=_U, kind="HAS_SKILL", target=None, forget_at=row.valid_at)
    earlier = ForgetOp(
        tenant=_T, user=_U, kind="HAS_SKILL", target=None, forget_at=row.valid_at.replace(day=1)
    )
    scoped_miss = ForgetOp(
        tenant=_T, user=_U, kind="HAS_SKILL", target="skill:go", forget_at=row.valid_at
    )
    other_kind = ForgetOp(
        tenant=_T, user=_U, kind="RECOMMENDED", target=None, forget_at=row.valid_at
    )
    assert observation_is_dead(row, [equal])
    assert not observation_is_dead(row, [earlier])
    assert not observation_is_dead(row, [scoped_miss])
    assert not observation_is_dead(row, [other_kind])


def test_kind_wide_directive_hides_only_its_kind() -> None:
    gateway = InMemoryGraphGateway()
    gateway.apply(_edge(_U, kind="HAS_SKILL", at=datetime(2026, 6, 1, tzinfo=UTC)))
    gateway.apply(_edge(_U, kind="RECOMMENDED", target="skill:go"))
    gateway.apply_forgets(_directive(None, _INSTANT))
    rows = gateway.observations(_T, _U)
    assert [(row.kind, row.target) for row in rows] == [("RECOMMENDED", "skill:go")]


def test_scoped_directive_hides_only_the_matching_target() -> None:
    gateway = InMemoryGraphGateway()
    gateway.apply(_edge(_U, target="skill:rust", at=datetime(2026, 6, 1, tzinfo=UTC)))
    gateway.apply(_edge(_U, target="skill:go", at=datetime(2026, 6, 1, tzinfo=UTC)))
    gateway.apply_forgets(_directive("skill:rust", _INSTANT))
    rows = gateway.observations(_T, _U)
    assert [row.target for row in rows] == ["skill:go"]


def test_facts_learned_after_the_forget_stay_alive() -> None:
    """A forget never harms the future: post-forget observations survive."""
    gateway = InMemoryGraphGateway()
    gateway.apply_forgets(_directive(None, datetime(2026, 6, 5, tzinfo=UTC)))
    gateway.apply(_edge(_U, at=datetime(2026, 6, 9, tzinfo=UTC)))
    assert [row.target for row in gateway.observations(_T, _U)] == ["skill:rust"]


def test_equal_instant_boundary_dies_and_directives_are_idempotent_by_identity() -> None:
    gateway = InMemoryGraphGateway()
    gateway.apply(_edge(_U, at=_INSTANT))
    first = _directive(None, _INSTANT)
    gateway.apply_forgets(first)
    assert gateway.observations(_T, _U) == []
    # Same directive again (duplicate event replays) changes nothing.
    gateway.apply_forgets(first)
    assert gateway.forget_directives(_T, _U) == first


def test_directives_are_tenant_and_user_scoped() -> None:
    gateway = InMemoryGraphGateway()
    other_user = UserKey("user-2")
    gateway.apply(_edge(_U, at=datetime(2026, 6, 1, tzinfo=UTC)))
    gateway.apply(_edge(other_user, at=datetime(2026, 6, 1, tzinfo=UTC)))
    gateway.apply_forgets(_directive(None, _INSTANT))
    # user-1's facts are hidden; the co-user in the SAME tenant stays visible.
    assert gateway.observations(_T, _U) == []
    assert [row.target for row in gateway.observations(_T, other_user)] == ["skill:rust"]
    # And another tenant's directive listing is empty (tenant predicate only).
    assert gateway.forget_directives(TenantId("other"), _U) == []


def test_engine_round_trips_forget_events_through_ledger_and_counts_them() -> None:
    spec = _spec()
    ledger = InMemoryLedger()
    engine = Projector(spec, ledger, InMemoryGraphGateway(), FixedClock(_INSTANT))
    learn_at = datetime(2026, 6, 1, tzinfo=UTC)
    forget_at = datetime(2026, 6, 20, tzinfo=UTC)
    ledger.append(
        [
            record_from_envelope(
                _T, _envelope("itf.skill_assessed.v1", "s1", {"skill": "rust"}, learn_at)
            ),
            record_from_envelope(_T, _envelope("itf.forget_all_skills.v1", "f1", {}, forget_at)),
        ]
    )
    digest = engine.process_events(ledger.read_events(_T))
    assert isinstance(digest, ProjectionDigest)
    assert digest.forget_ops == 1
    gateway = cast(InMemoryGraphGateway, engine.gateway)
    assert gateway.observations(_T, _U) == []
    # The audit trail survives the projection: the directive is queryable.
    [directive] = gateway.forget_directives(_T, _U)
    assert (directive.kind, directive.target, directive.forget_at) == (
        "HAS_SKILL",
        None,
        forget_at,
    )


def test_incremental_convergence_under_any_batch_ordering_with_duplicates() -> None:
    """research-06 acceptance: forget flows re-included, zero divergence.

    Every ordering of duplicated batch splits — including ARRIVING BEFORE
    the facts it forgets — converges to the same visible state: the
    kind-wide directive at 2026-06-20 supersedes both learned skills, so the
    canonical outcome is an empty observation list.
    """
    spec = _spec()
    learn_at = datetime(2026, 6, 1, tzinfo=UTC)
    mid_at = datetime(2026, 6, 5, tzinfo=UTC)
    forget_at = datetime(2026, 6, 20, tzinfo=UTC)
    fixtures: tuple[tuple[str, str, dict[str, Json], datetime], ...] = (
        ("itf.skill_assessed.v1", "s1", {"skill": "rust"}, learn_at),
        ("itf.skill_assessed.v1", "s2", {"skill": "go"}, mid_at),
        ("itf.forget_one_skill.v1", "f1", {"target": "skill:rust"}, forget_at),
        ("itf.forget_all_skills.v1", "f2", {}, forget_at),
    )
    envelopes = [_envelope(*fixture) for fixture in fixtures]

    def state_after(batches: list[list[int]]) -> list[tuple[str, str]]:
        ledger = InMemoryLedger()
        engine = Projector(spec, ledger, InMemoryGraphGateway(), FixedClock(_INSTANT))
        for indexes in batches:
            ledger.append([record_from_envelope(_T, envelopes[i]) for i in indexes])
            # Each pass re-derives state from the WHOLE ledger so far: the
            # honest incremental posture (idempotent, order-independent).
            engine.process_events(ledger.read_events(_T))
        rows = cast(InMemoryGraphGateway, engine.gateway).observations(_T, _U)
        return [(row.kind, row.target) for row in rows]

    order = list(range(len(envelopes)))
    orderings: list[list[list[int]]] = [
        [order],
        [list(perm) for perm in permutations(order)],
        [order + order],  # duplicates absorbed, never divergent
    ]
    outcomes = {tuple(state_after(batches)) for batches in orderings}
    assert outcomes == {()}
    # And the full-rebuild posture agrees with the incremental one.
    ledger = InMemoryLedger()
    ledger.append([record_from_envelope(_T, envelopes[i]) for i in order])
    engine = Projector(spec, ledger, InMemoryGraphGateway(), FixedClock(_INSTANT))
    engine.replay()
    assert (
        tuple(
            (row.kind, row.target)
            for row in cast(InMemoryGraphGateway, engine.gateway).observations(_T, _U)
        )
        == ()
    )


def test_counterfactual_replay_dropping_the_forget_event_diverges() -> None:
    """The eval-harness control: the forget event MATTERS (research 06)."""
    spec = _spec()
    ledger = InMemoryLedger()
    learn_at = datetime(2026, 6, 1, tzinfo=UTC)
    forget_at = datetime(2026, 6, 20, tzinfo=UTC)
    ledger.append(
        [
            record_from_envelope(
                _T, _envelope("itf.skill_assessed.v1", "s1", {"skill": "rust"}, learn_at)
            ),
            record_from_envelope(
                _T, _envelope("itf.skill_assessed.v1", "s2", {"skill": "go"}, learn_at)
            ),
            record_from_envelope(_T, _envelope("itf.forget_all_skills.v1", "f1", {}, forget_at)),
        ]
    )
    clock = FixedClock(_INSTANT)

    def drop_forgets(event: StoredEvent) -> bool:
        return event.signal == "itf.forget_all_skills.v1"

    deltas = counterfactual_replay(
        spec, ledger, clock, _T, "skills", drop=drop_forgets, users=(_U,)
    )
    [delta] = deltas
    # Full replay: the forget holds (no visible HAS_SKILL rows). Without the
    # forget: both skills visible. The event demonstrably shapes the output.
    assert delta.before == ()
    assert len(delta.after) >= 2
    assert delta.diverged


def test_snapshot_probe_exposes_the_superseded_state() -> None:
    """Read-model end: projections forget, snapshots honor the tombstone."""
    spec = _spec()
    ledger = InMemoryLedger()
    engine = Projector(spec, ledger, InMemoryGraphGateway(), FixedClock(_INSTANT))
    ledger.append(
        [
            record_from_envelope(
                _T,
                _envelope(
                    "itf.skill_assessed.v1",
                    "s1",
                    {"skill": "rust"},
                    datetime(2026, 6, 1, tzinfo=UTC),
                ),
            ),
            record_from_envelope(
                _T,
                _envelope(
                    "itf.forget_one_skill.v1",
                    "f1",
                    {"target": "skill:rust"},
                    datetime(2026, 6, 9, tzinfo=UTC),
                ),
            ),
        ]
    )
    engine.replay()
    snapshot = engine.snapshot(_T, _U, Capability("skills"))
    assert snapshot.payload["items"] == []
