"""The mapping compiler: (envelope, spec) -> a deterministic projection plan.

Purity is the whole point: no IO, no wall clock, no randomness. The same
envelope and spec version always produce the same plan, which is what makes
replay convergence provable (SPEC §3). Consequences:

- A signal type with rules emits ``nodes`` (idempotent upserts) then ``edges``
  (observations the gateway merges into decayed state).
- Signal types absent from the spec compile to an EMPTY plan — they land in
  the ledger (truth) but touch nothing: additive-only evolution, forward safe.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from graph_memory.core.types import TenantId, UserKey
from graph_memory.mapping.spec import MappingSpec, envelope_context, resolve_template
from graph_memory.signals.envelope import Envelope


@dataclass(frozen=True)
class NodeOp:
    """Upsert one node (idempotent by tenant+key)."""

    tenant: TenantId
    key: str
    labels: tuple[str, ...]
    props: dict[str, str]


@dataclass(frozen=True)
class EdgeOp:
    """Upsert one user->node edge observation (state merges at the gateway)."""

    tenant: TenantId
    user: UserKey
    kind: str
    target: str
    weight: float
    half_life_days: float
    valid_at: datetime


@dataclass(frozen=True)
class ProjectionPlan:
    """Everything one event contributes; ordered for byte-stable equivalence."""

    tenant: TenantId
    signal: str
    nodes: tuple[NodeOp, ...]
    edges: tuple[EdgeOp, ...]


def compile_plan(spec: MappingSpec, tenant: TenantId, envelope: Envelope) -> ProjectionPlan:
    """Compile one validated envelope into its projection plan."""
    context = envelope_context(envelope)
    user = UserKey(str(context["partitionkey"]))
    rule = spec.signals.get(envelope.type)
    if rule is None:
        return ProjectionPlan(tenant=tenant, signal=envelope.type, nodes=(), edges=())
    nodes = tuple(
        NodeOp(
            tenant=tenant,
            key=resolve_template(node.key, context),
            labels=node.labels,
            props={name: resolve_template(value, context) for name, value in node.props.items()},
        )
        for node in rule.nodes
    )
    edges = tuple(
        EdgeOp(
            tenant=tenant,
            user=user,
            kind=edge.kind,
            target=resolve_template(edge.target, context),
            weight=edge.weight,
            half_life_days=edge.half_life_days,
            valid_at=envelope.occurred_at,
        )
        for edge in rule.edges
    )
    return ProjectionPlan(tenant=tenant, signal=envelope.type, nodes=nodes, edges=edges)
