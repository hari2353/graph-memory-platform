"""The mapping compiler: (envelope, spec) -> a deterministic projection plan.

Purity is the whole point: no IO, no wall clock, no randomness. The same
envelope and spec version always produce the same plan, which is what makes
replay convergence provable (SPEC §3). Consequences:

- A signal type with rules emits ``nodes`` (idempotent upserts) then ``edges``
  (observations the gateway merges into decayed state); forget-only signals
  emit ``forgets`` (supersession directives the gateway keeps as overlay).
- Signal types absent from the spec compile to an EMPTY plan — they land in
  the ledger (truth) but touch nothing: additive-only evolution, forward safe.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime

from graph_memory.core.types import TenantId, UserKey
from graph_memory.mapping.spec import (
    MappingSpec,
    envelope_context,
    envelope_route,
    resolve_template,
)
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
class ForgetOp:
    """One forget directive (M4.1, research 06): supersede, never delete.

    Stored as its own overlay row in the gateway — NOT a mutation of the
    matching edge rows — so it stays an idempotent, identity-keyed pure
    function of its event (convergent under any delivery permutation) and
    the audit story is a fact about one event, not a delta over state.

    ``target=None`` forgets the whole relation kind; a set target forgets
    the exact observations whose resolved target matches. ``forget_at``
    bounds the supersession: only observations with ``valid_at <=
    forget_at`` die, so a forget never harms facts learned after it.
    """

    tenant: TenantId
    user: UserKey
    kind: str
    target: str | None
    forget_at: datetime


@dataclass(frozen=True)
class ProjectionPlan:
    """Everything one event contributes; ordered for byte-stable equivalence."""

    tenant: TenantId
    signal: str
    nodes: tuple[NodeOp, ...]
    edges: tuple[EdgeOp, ...]
    forgets: tuple[ForgetOp, ...] = ()


def compile_plan(
    spec: MappingSpec,
    tenant: TenantId,
    envelope: Envelope,
    *,
    weight_overrides: Mapping[tuple[str, str, str], float] | None = None,
) -> ProjectionPlan:
    """Compile one validated envelope into its projection plan.

    Every mapped event also upserts the USER node (keyed by the raw user key,
    label ``user``) — edge endpoints must exist for the graph to serve cypher
    matches without dangling references.

    ``weight_overrides`` (the effectiveness loop's output, keyed
    ``(kind, target, cohort)``) multiply the spec's base weight INSIDE the
    pure function — replay stays a pure function of
    ``(ledger, spec version, overrides, clock)``. The cohort comes from the
    event's declared route, so weight rows apply per cohort, not globally.

    Forget rules (M4.1) emit ``ForgetOp`` directives bound to the forget
    event's own instant; they carry no weight — supersession time, not
    magnitude, is their payload.
    """
    context = envelope_context(envelope)
    user = UserKey(str(context["partitionkey"]))
    rule = spec.signals.get(envelope.type)
    if rule is None:
        return ProjectionPlan(tenant=tenant, signal=envelope.type, nodes=(), edges=())
    cohort = (
        ""
        if not weight_overrides or spec.effectiveness is None
        else envelope_route(envelope, spec.effectiveness.cohort_field) or ""
    )
    user_node = NodeOp(tenant=tenant, key=str(user), labels=("user",), props={"name": str(user)})
    nodes = (
        user_node,
        *(
            NodeOp(
                tenant=tenant,
                key=resolve_template(node.key, context),
                labels=node.labels,
                props={
                    name: resolve_template(value, context) for name, value in node.props.items()
                },
            )
            for node in rule.nodes
        ),
    )
    edges: list[EdgeOp] = []
    for edge in rule.edges:
        target = resolve_template(edge.target, context)
        edges.append(
            EdgeOp(
                tenant=tenant,
                user=user,
                kind=edge.kind,
                target=target,
                weight=edge.weight * _override(weight_overrides, edge.kind, target, cohort),
                half_life_days=edge.half_life_days,
                valid_at=envelope.occurred_at,
            )
        )
    forgets: list[ForgetOp] = []
    for directive in rule.forgets:
        forget_target = (
            None if directive.target is None else resolve_template(directive.target, context)
        )
        forgets.append(
            ForgetOp(
                tenant=tenant,
                user=user,
                kind=directive.kind,
                target=forget_target,
                forget_at=envelope.occurred_at,
            )
        )
    return ProjectionPlan(
        tenant=tenant, signal=envelope.type, nodes=nodes, edges=tuple(edges), forgets=tuple(forgets)
    )


def _override(
    overrides: Mapping[tuple[str, str, str], float] | None, kind: str, target: str, cohort: str
) -> float:
    if not overrides:
        return 1.0
    return overrides.get((kind, target, cohort), 1.0)
