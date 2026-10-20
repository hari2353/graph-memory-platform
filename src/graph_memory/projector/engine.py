"""The projector: ledger rows -> graph state -> snapshot documents.

Determinism rules (SPEC §3, plan §3 risk 2):

- The gateway stores **observations** by identity, so incremental batches and
  full replays converge to the same state for the same event set — the
  convergence test replays batch orderings that include duplicates and
  permutation, and requires zero divergence.
- Scores are pure functions ``(observations, now)`` evaluated at a single
  injected ``Clock`` instant: ``score = Σ w_i · 2^(−Δdays / half_life_i)``.
- Snapshots carry ``generated_at`` + ``max_age_ms`` — staleness is always
  surfaced, never silent.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from math import exp, log

from graph_memory.core.clock import Clock
from graph_memory.core.errors import PlatformError
from graph_memory.core.types import Capability, TenantId, UserKey
from graph_memory.mapping.compiler import EdgeOp, NodeOp, compile_plan
from graph_memory.mapping.spec import MappingSpec
from graph_memory.projector.gateway import GraphGateway
from graph_memory.signals.ledger import LedgerRepository, StoredEvent

_LN2 = log(2.0)


class CapabilityUnknown(PlatformError):
    """A context capability not declared by the mapping spec."""

    code = "capability_unknown"


@dataclass(frozen=True)
class ProjectionDigest:
    """Counts from one projection pass; stable across backends."""

    events: int
    plans: int
    node_ops: int
    edge_ops: int

    def __add__(self, other: ProjectionDigest) -> ProjectionDigest:
        return ProjectionDigest(
            events=self.events + other.events,
            plans=self.plans + other.plans,
            node_ops=self.node_ops + other.node_ops,
            edge_ops=self.edge_ops + other.edge_ops,
        )


@dataclass(frozen=True)
class SnapshotDoc:
    """The materialized read-model doc for one (tenant, user, capability)."""

    tenant: TenantId
    user: UserKey
    capability: str
    payload: dict[str, object]
    generated_at: datetime
    max_age_ms: int


def decayed_score(weight: float, half_life_days: float, valid_at: datetime, now: datetime) -> float:
    """Exponential decay by half-life; negative age clamps to full weight."""
    age_days = (now - valid_at).total_seconds() / 86_400.0
    if age_days <= 0:
        return weight
    return weight * exp(-_LN2 * age_days / half_life_days)


class Projector:
    """Projection engine over injected ports; sync by contract."""

    def __init__(
        self,
        spec: MappingSpec,
        ledger: LedgerRepository,
        gateway: GraphGateway,
        clock: Clock,
    ) -> None:
        self._spec = spec
        self._ledger = ledger
        self._gateway = gateway
        self._clock = clock

    def process_events(self, events: Sequence[StoredEvent]) -> ProjectionDigest:
        """Incremental projection for one batch, in canonical order."""
        digest = ProjectionDigest(events=len(events), plans=0, node_ops=0, edge_ops=0)
        relevant: list[StoredEvent] = []
        for event in events:
            if self._spec.signals.get(event.signal) is not None:
                relevant.append(event)
        relevant.sort(key=lambda e: (e.occurred_at, e.dedup_key))
        for event in relevant:
            plan = self._project(event)
            digest = digest + plan
        return digest

    def replay(self) -> dict[TenantId, ProjectionDigest]:
        """Full rebuild from the ledger; the projection is replaceable."""
        self._gateway.replace_all()
        digests: dict[TenantId, ProjectionDigest] = {}
        for tenant in self._ledger.tenants():
            digests[tenant] = self.process_events(self._ledger.read_events(tenant))
        return digests

    def _project(self, event: StoredEvent) -> ProjectionDigest:
        envelope = event.envelope
        rule = self._spec.signals.get(event.signal)
        if rule is None:
            return ProjectionDigest(events=0, plans=0, node_ops=0, edge_ops=0)
        plan = compile_plan(self._spec, event.tenant, envelope)
        ops: list[NodeOp | EdgeOp] = []
        ops.extend(plan.nodes)
        ops.extend(plan.edges)
        self._gateway.apply(ops)
        return ProjectionDigest(
            events=0,  # the fed-batch total is counted once by process_events
            plans=1,
            node_ops=len(plan.nodes),
            edge_ops=len(plan.edges),
        )

    def snapshot(self, tenant: TenantId, user: UserKey, capability: Capability) -> SnapshotDoc:
        """Build the read-model doc; group (kind, target), score, truncate top_k."""
        rule = self._spec.capabilities.get(capability)
        if rule is None:
            raise CapabilityUnknown(
                "capability is not declared by the mapping spec",
                details={"capability": capability},
            )
        now = self._clock.now()
        rows = [
            row for row in self._gateway.observations(tenant, user) if row.kind in rule.relations
        ]
        scores: dict[tuple[str, str], float] = {}
        for row in rows:
            key = (row.kind, row.target)
            scores[key] = scores.get(key, 0.0) + decayed_score(
                row.weight, row.half_life_days, row.valid_at, now
            )
        ranked = sorted(scores.items(), key=lambda item: (-item[1], item[0][0], item[0][1]))
        items = [
            {"kind": kind, "target": target, "score": round(score, 9)}
            for (kind, target), score in ranked[: rule.top_k]
        ]
        nodes = self._gateway.nodes_of(
            tenant, [target for (_kind, target), _score in ranked[: rule.top_k]]
        )
        labels = {node.key: list(node.labels) for node in nodes}
        payload: dict[str, object] = {
            "capability": capability,
            "top_k": rule.top_k,
            "items": items,
            "labels": labels,
        }
        return SnapshotDoc(
            tenant=tenant,
            user=user,
            capability=capability,
            payload=payload,
            generated_at=now,
            max_age_ms=rule.ttl_ms,
        )
