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

from collections.abc import Callable, Mapping
from dataclasses import dataclass

from graph_memory.core.clock import Clock
from graph_memory.core.types import Capability, TenantId, UserKey
from graph_memory.mapping.spec import MappingSpec
from graph_memory.projector.engine import Projector
from graph_memory.projector.gateway import InMemoryGraphGateway
from graph_memory.signals.ledger import (
    InMemoryLedger,
    LedgerRepository,
    StoredEvent,
    record_from_envelope,
)


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
