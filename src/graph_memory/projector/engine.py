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

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from itertools import combinations
from math import exp, log

from graph_memory.core.clock import Clock
from graph_memory.core.errors import PlatformError
from graph_memory.core.types import Capability, TenantId, UserKey
from graph_memory.mapping.compiler import EdgeOp, NodeOp, compile_plan
from graph_memory.mapping.spec import MappingSpec
from graph_memory.projector.gateway import GraphGateway
from graph_memory.signals.ledger import LedgerRepository, StoredEvent

_LN2 = log(2.0)
_SIMILAR_KIND = "SIMILAR_TO"
_SIMILAR_MIN_SHARED = 2
_SIMILAR_HALF_LIFE_DAYS = 3650.0  # similarity is cohort-level, not skill-recency


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
class ReflectionDigest:
    """Counts from one reflection tick; stable across backends."""

    users_scanned: int
    pairs_scored: int
    similar_edges_written: int


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


def _user_node(user: UserKey) -> str:
    """User nodes are keyed by the RAW user key (edges join on this id)."""
    return str(user)


class Projector:
    """Projection engine over injected ports; sync by contract."""

    def __init__(
        self,
        spec: MappingSpec,
        ledger: LedgerRepository,
        gateway: GraphGateway,
        clock: Clock,
        *,
        weight_overrides: Mapping[tuple[str, str], float] | None = None,
    ) -> None:
        self._spec = spec
        self._ledger = ledger
        self._gateway = gateway
        self._clock = clock
        self._overrides = weight_overrides

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
        plan = compile_plan(self._spec, event.tenant, envelope, weight_overrides=self._overrides)
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

    def reflect(self, *, min_shared: int = _SIMILAR_MIN_SHARED) -> ReflectionDigest:
        """One reflection tick: rewrite SIMILAR_TO edges from current state.

        Deterministic: users and pairs are visited in sorted order, the tick
        reads one settled observation snapshot, and the similarity set is
        rewritten wholesale (clear-first, never incrementally patched). The
        injected Clock stamps the tick instant.
        """
        users_scanned = 0
        pairs_scored = 0
        written = 0
        now = self._clock.now()
        for tenant in self._ledger.tenants():
            self._gateway.clear_kind(_SIMILAR_KIND)
            users = self._gateway.users(tenant)
            users_scanned += len(users)
            skills: dict[UserKey, set[tuple[str, str]]] = {}
            for user in users:
                skills[user] = {
                    (row.kind, row.target)
                    for row in self._gateway.observations(tenant, user)
                    if row.kind != _SIMILAR_KIND
                }
            ops: list[NodeOp | EdgeOp] = []
            for user_a, user_b in combinations(sorted(users), 2):
                pairs_scored += 1
                shared = len(skills[user_a] & skills[user_b])
                if shared < min_shared:
                    continue
                ops.append(
                    NodeOp(
                        tenant=tenant,
                        key=_user_node(user_a),
                        labels=("user",),
                        props={"name": user_a},
                    )
                )
                ops.append(
                    NodeOp(
                        tenant=tenant,
                        key=_user_node(user_b),
                        labels=("user",),
                        props={"name": user_b},
                    )
                )
                for source, target in ((user_a, user_b), (user_b, user_a)):
                    ops.append(
                        EdgeOp(
                            tenant=tenant,
                            user=source,
                            kind=_SIMILAR_KIND,
                            target=_user_node(target),
                            weight=float(shared),
                            half_life_days=_SIMILAR_HALF_LIFE_DAYS,
                            valid_at=now,
                        )
                    )
                written += 2
            self._gateway.apply(ops)
        return ReflectionDigest(
            users_scanned=users_scanned,
            pairs_scored=pairs_scored,
            similar_edges_written=written,
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
