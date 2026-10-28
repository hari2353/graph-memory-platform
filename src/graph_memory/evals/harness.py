"""Offline eval harness (SPEC §11, R7): how personalization is PROVEN.

Three instruments, all deterministic (same input ⇒ same number):

- ``hit_rate_at_k`` — fraction of held-out users whose ground-truth target
  appears in the top-k of the given capability's snapshot.
- ``counterfactual_replay`` — replay the ledger twice, once without a
  chosen subset of events; returns the per-user item-tuple diff. Removing
  an event that does not matter MUST produce an empty diff.
- ``lift`` — the hit-rate delta of a loop-carrying run against the
  no-loop baseline: the §16.7 report number.

No LLM, no wall clock: runs against an injected ``Projector`` + ``Clock``.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass

from graph_memory.core.clock import Clock
from graph_memory.core.types import Capability, Json, TenantId, UserKey
from graph_memory.loop.tick import EffectivenessTick
from graph_memory.mapping.spec import MappingSpec
from graph_memory.projector.engine import Projector
from graph_memory.projector.gateway import InMemoryGraphGateway
from graph_memory.signals.ledger import (
    InMemoryLedger,
    LedgerRepository,
    StoredEvent,
    record_from_envelope,
)


def _json_list(items: Iterable[str]) -> list[Json]:
    """A JSON-stable list half (list-str invariance under the Json alias)."""
    return [str(item) for item in sorted(items)]


def _top_targets(
    engine: Projector, tenant: TenantId, user: UserKey, capability: str, k: int
) -> tuple[str, ...]:
    doc = engine.snapshot(tenant, user, Capability(capability))
    items = doc.payload["items"]
    if not isinstance(items, list):  # pragma: no cover - engine contract
        raise TypeError("snapshot payload items must be a list")
    targets: list[str] = []
    for item in items[:k]:
        if isinstance(item, dict):
            targets.append(str(item["target"]))
    return tuple(targets)


def hit_rate_at_k(
    engine: Projector,
    tenant: TenantId,
    capability: str,
    expectations: Mapping[UserKey, str],
    *,
    k: int = 3,
) -> float:
    """Fraction of users whose needle target ranks inside the top-k."""
    if not expectations:
        raise ValueError("hit_rate_at_k needs at least one expectation")
    hits = 0
    for user in sorted(expectations):
        needle = expectations[user]
        if needle in _top_targets(engine, tenant, user, capability, k):
            hits += 1
    return hits / len(expectations)


@dataclass(frozen=True)
class UserDelta:
    """One user's item-tuple diff between the two replays."""

    user: UserKey
    before: tuple[str, ...]
    after: tuple[str, ...]

    @property
    def diverged(self) -> bool:
        return self.before != self.after


def counterfactual_replay(
    spec: MappingSpec,
    ledger: LedgerRepository,
    clock: Clock,
    tenant: TenantId,
    capability: str,
    *,
    drop: Callable[[StoredEvent], bool],
    users: tuple[UserKey, ...],
) -> tuple[UserDelta, ...]:
    """Replay twice — full ledger vs ledger-minus-``drop`` — and diff ranks.

    The counterfactual control: an event that influences the projection
    shows up as ``UserDelta.diverged``; noise events leave every user's
    item tuple byte-identical.
    """
    full = Projector(spec, ledger, InMemoryGraphGateway(), clock)
    full.replay()

    subset = InMemoryLedger()
    for tenant_id in ledger.tenants():
        for event in ledger.read_events(tenant_id):
            if tenant_id != tenant or not drop(event):
                subset.append([record_from_envelope(tenant_id, event.envelope)])
    partial = Projector(spec, subset, InMemoryGraphGateway(), clock)
    partial.replay()

    deltas: list[UserDelta] = []
    for user in users:
        deltas.append(
            UserDelta(
                user=user,
                before=_top_targets(full, tenant, user, capability, k=1000),
                after=_top_targets(partial, tenant, user, capability, k=1000),
            )
        )
    return tuple(deltas)


def lift(with_loop: float, without_loop: float) -> float:
    """The eval report number: hit-rate delta attributable to the loop."""
    return with_loop - without_loop


@dataclass(frozen=True)
class ProbeCell:
    """One user's BOTH-regime top-k, tagged with its cohort's evidence."""

    user: UserKey
    cohort: str
    warm: bool
    baseline_top: tuple[str, ...]
    loop_top: tuple[str, ...]

    @property
    def diverged(self) -> bool:
        return self.loop_top != self.baseline_top


def over_personalization_probe(
    spec: MappingSpec,
    ledger: LedgerRepository,
    clock: Clock,
    tenant: TenantId,
    capability: str,
    *,
    cohorts: Mapping[str, tuple[UserKey, ...]],
    needles: Mapping[UserKey, str],
    k: int = 1,
    override_rows: Mapping[tuple[str, str, str], float] | None = None,
) -> dict[str, Json]:
    """OP-Bench-style over-personalization instrument (research 06, M4.3).

    Replays the ledger under BOTH regimes — the no-loop baseline and a
    loop-carrying replay whose weight rows come from the ledger tick — and
    reports each mapped user's top-k per regime. The falsifiable contract:
    a cohort that carries outcome evidence (WARM) may diverge (that is the
    personalization the loop is FOR); a cohort with NO attached outcome
    (COLD — fresh/never-acted users whose cohort has no scored cells) must
    keep ITS OWN baseline top byte-identical: loop noise is forbidden from
    degrading users who never contributed evidence. The returned dict is
    JSON-stable and deterministic under the injected clock — the published
    matrix for eval fixtures (SPEC §16.7 report family, same discipline as
    hit-rate/lift).

    ``override_rows`` is the negative-control seam: when given, it replaces
    the tick's rows with a caller-chosen (possibly BUGGY) row set, so CI
    can prove the instrument fails when a writer leaks cohort rows. The
    clean path (default ``None``) always scores the ledger honestly.
    """
    cohort_of: dict[UserKey, str] = {}
    for cohort, members in cohorts.items():
        for user in members:
            if user in cohort_of:
                raise ValueError(f"cohort membership conflict for {user}")
            cohort_of[user] = cohort
    unknown = sorted({user for user in needles if user not in cohort_of})
    if unknown:
        raise ValueError(f"needles reference users outside the cohort map: {unknown}")
    if not needles:
        raise ValueError("over_personalization_probe needs at least one needle")

    # Regime 1 — the no-loop baseline (weight overrides absent by construction).
    baseline_engine = Projector(spec, ledger, InMemoryGraphGateway(), clock)
    baseline_engine.replay()
    # Regime 2 — the scored loop: tick the ledger once, replay with its rows.
    if override_rows is not None:
        # Negative-control path: no tick — the caller IS the (possibly
        # buggy) weight writer, and no cohort may claim outcome evidence.
        warm: frozenset[str] = frozenset()
        rows: dict[tuple[str, str, str], float] | None = dict(override_rows) or None
    else:
        report = EffectivenessTick(spec, ledger, clock).score(tenant)
        warm = frozenset(cell.cohort for cell in report.cells)
        rows = report.weight_overrides or None
    loop_engine = Projector(
        spec,
        ledger,
        InMemoryGraphGateway(),
        clock,
        weight_overrides=rows,
    )
    loop_engine.replay()

    cell_list: list[ProbeCell] = []
    for cohort in sorted(cohorts):
        for user in sorted(cohorts[cohort]):
            cell_list.append(
                ProbeCell(
                    user=user,
                    cohort=cohort,
                    warm=cohort in warm,
                    baseline_top=_top_targets(baseline_engine, tenant, user, capability, k),
                    loop_top=_top_targets(loop_engine, tenant, user, capability, k),
                )
            )
    cold: frozenset[str] = frozenset(cohorts) - warm
    cold_needles = {user: needle for user, needle in needles.items() if cohort_of[user] in cold}
    warm_needles = {user: needle for user, needle in needles.items() if cohort_of[user] in warm}

    def regime_hit_rate(engine: Projector, scope: Mapping[UserKey, str]) -> float | None:
        return hit_rate_at_k(engine, tenant, capability, scope, k=k) if scope else None

    baseline_views = [cell.baseline_top for cell in cell_list]
    degraded_cold = sorted({cell.cohort for cell in cell_list if not cell.warm and cell.diverged})
    cells: list[Json] = [
        {
            "cohort": cell.cohort,
            "user": str(cell.user),
            "warm": cell.warm,
            "baseline_top": list(cell.baseline_top),
            "loop_top": list(cell.loop_top),
            "diverged": cell.diverged,
        }
        for cell in cell_list
    ]
    baseline_rate = hit_rate_at_k(baseline_engine, tenant, capability, needles, k=k)
    loop_rate = hit_rate_at_k(loop_engine, tenant, capability, needles, k=k)
    return {
        "capability": capability,
        "spec_version": spec.mapping_version,
        "k": k,
        "warm_cohorts": _json_list(warm),
        "cold_cohorts": _json_list(cold),
        "baseline_cohort_blind": len(set(baseline_views)) <= 1,
        "cells": cells,
        "warm_diverged_cohorts": _json_list(
            {cell.cohort for cell in cell_list if cell.warm and cell.diverged}
        ),
        "warm_tops_mutually_distinct": len({cell.loop_top for cell in cell_list if cell.warm}) > 1,
        "cold_degraded_cohorts": _json_list(degraded_cold),
        "hit_rate": {
            "baseline": {
                "all": baseline_rate,
                "warm": regime_hit_rate(baseline_engine, warm_needles),
                "cold": regime_hit_rate(baseline_engine, cold_needles),
            },
            "loop": {
                "all": loop_rate,
                "warm": regime_hit_rate(loop_engine, warm_needles),
                "cold": regime_hit_rate(loop_engine, cold_needles),
            },
        },
        "beat_baseline": loop_rate > baseline_rate,
        "beat_baseline_warm": (
            (regime_hit_rate(loop_engine, warm_needles) or 0.0)
            > (regime_hit_rate(baseline_engine, warm_needles) or 0.0)
        ),
        "verdict": "over-personalized" if degraded_cold else "no-over-personalization",
    }
