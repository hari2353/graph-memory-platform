"""GraphGateway port: the ONLY surface through which the projector mutates
or reads graph state. Observations are stored by identity —
``(tenant, user, kind, target, valid_at)`` — which makes state:
a) order-independent (deterministic under any delivery permutation),
b) trivially replayable (the same events produce the same keys),
c) merge-friendly toward a ClickHouse ReplacingMergeTree later.

Forget directives (M4.1, research 06) are stored as their OWN overlay rows —
never a mutation of edge rows — keyed ``(tenant, user, kind, target,
forget_at)``. Visibility is then a pure, tie-free predicate evaluated at
read time: an observation is dead iff a directive of its (tenant, user,
kind) covers its target with ``forget_at >= valid_at`` (equal instants
die). Both halves are per-event facts, so the pair converges under any
replay permutation without ReplacingMergeTree version-column surgery.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from graph_memory.core.lexicon import lexical_hits
from graph_memory.core.types import TenantId, UserKey
from graph_memory.mapping.compiler import EdgeOp, ForgetOp, NodeOp


@dataclass(frozen=True)
class NodeRow:
    """A node as stored."""

    tenant: TenantId
    key: str
    labels: tuple[str, ...]
    props: dict[str, str]


@dataclass(frozen=True)
class EdgeRow:
    """One edge observation at one instant; scoring happens at reflection."""

    tenant: TenantId
    user: UserKey
    kind: str
    target: str
    weight: float
    half_life_days: float
    valid_at: datetime


def observation_is_dead(row: EdgeRow, directives: Sequence[ForgetOp]) -> bool:
    """The shared bi-temporal visibility predicate (both backends use it).

    Dead iff some directive matches the observation's (user, kind) scope,
    covers its target (``None`` = whole kind), and happened at or after the
    observation. The equal-instant boundary is deliberately ``<=``: a
    total, tie-free rule — no delivery order can disagree about it.
    """
    return any(
        directive.kind == row.kind
        and (directive.target is None or directive.target == row.target)
        and row.valid_at <= directive.forget_at
        for directive in directives
    )


class GraphGateway(Protocol):
    """Graph state port; adapters add, core never changes.

    Rebuild protocol (used by full replays): ``replace_all`` opens a rebuild
    side so subsequent ``apply`` writes stage into a hold-out that readers do
    not see; ``commit_rebuild`` publishes the staged state atomically. A
    serving instance therefore never exposes a half-built graph: readers keep
    observing the last committed projection mid-rebuild.
    """

    def apply(self, ops: Sequence[NodeOp | EdgeOp]) -> None: ...

    def apply_forgets(self, forgets: Sequence[ForgetOp]) -> None: ...

    def observations(self, tenant: TenantId, user: UserKey) -> list[EdgeRow]: ...

    def search_edges(
        self, tenant: TenantId, user: UserKey, tokens: Sequence[str]
    ) -> list[tuple[EdgeRow, int]]: ...

    def forget_directives(self, tenant: TenantId, user: UserKey) -> list[ForgetOp]: ...

    def nodes_of(self, tenant: TenantId, keys: Sequence[str]) -> list[NodeRow]: ...

    def users(self, tenant: TenantId) -> list[UserKey]: ...

    def clear_kind(self, kind: str) -> None: ...

    def replace_all(self) -> None: ...

    def commit_rebuild(self) -> None: ...


class InMemoryGraphGateway:
    """Deterministic store; identical semantics to the future CH adapter.

    Rebuilds stage into a hold-out and publish with ``commit_rebuild`` —
    readers never observe a half-built graph.
    """

    def __init__(self) -> None:
        self._nodes: dict[tuple[str, str], NodeOp] = {}
        self._edges: dict[tuple[str, str, str, str, datetime], EdgeOp] = {}
        self._forgets: dict[tuple[str, str, str, str | None, datetime], ForgetOp] = {}
        self._staged_nodes: dict[tuple[str, str], NodeOp] | None = None
        self._staged_edges: dict[tuple[str, str, str, str, datetime], EdgeOp] | None = None

    def _node_target(self) -> dict[tuple[str, str], NodeOp]:
        """Writes land in the open rebuild's staging, else the published side."""
        return self._nodes if self._staged_nodes is None else self._staged_nodes

    def _edge_target(self) -> dict[tuple[str, str, str, str, datetime], EdgeOp]:
        return self._edges if self._staged_edges is None else self._staged_edges

    def apply(self, ops: Sequence[NodeOp | EdgeOp]) -> None:
        nodes, edges = self._node_target(), self._edge_target()
        for op in ops:
            match op:
                case NodeOp():
                    nodes[(op.tenant, op.key)] = op
                case EdgeOp():
                    key = (op.tenant, op.user, op.kind, op.target, op.valid_at)
                    edges[key] = op
                case _:
                    raise TypeError(f"unsupported graph op: {type(op).__name__}")

    def apply_forgets(self, forgets: Sequence[ForgetOp]) -> None:
        """Directives are pure overlay rows: identity-keyed, write-through.

        They deliberately skip the rebuild staging: a full replay re-applies
        the same directive set (one event => one identity => idempotent), so
        staging them would add a swap for zero convergence benefit. Readers
        of the published graph keep seeing the directives that govern it.
        """
        for op in forgets:
            self._forgets[(op.tenant, op.user, op.kind, op.target, op.forget_at)] = op

    def observations(self, tenant: TenantId, user: UserKey) -> list[EdgeRow]:
        directives = self.forget_directives(tenant, user)
        rows = [
            EdgeRow(
                tenant=op.tenant,
                user=op.user,
                kind=op.kind,
                target=op.target,
                weight=op.weight,
                half_life_days=op.half_life_days,
                valid_at=op.valid_at,
            )
            for (t, u, _, _, _), op in self._edges.items()
            if t == tenant and u == user
        ]
        rows.sort(key=lambda row: (row.kind, row.target, row.valid_at, row.weight))
        return [row for row in rows if not observation_is_dead(row, directives)]

    def search_edges(
        self, tenant: TenantId, user: UserKey, tokens: Sequence[str]
    ) -> list[tuple[EdgeRow, int]]:
        """Bounded per-user lexical retrieval; the ONE lexical metric applies.

        Contract (core.lexicon): a token hits when it is a case-insensitive
        substring of the observation's kind or target; the score is the
        number of tokens with at least one hit. Rows are the user's LIVE
        observations only (forget overlay applied) in the same canonical
        order ``observations`` serves — no independent ordering decisions.
        """
        directives = self.forget_directives(tenant, user)
        rows = [
            EdgeRow(
                tenant=op.tenant,
                user=op.user,
                kind=op.kind,
                target=op.target,
                weight=op.weight,
                half_life_days=op.half_life_days,
                valid_at=op.valid_at,
            )
            for (t, u, _, _, _), op in self._edges.items()
            if t == tenant and u == user
        ]
        rows.sort(key=lambda row: (row.kind, row.target, row.valid_at, row.weight))
        return [
            (row, lexical_hits((row.kind, row.target), tokens))
            for row in rows
            if not observation_is_dead(row, directives)
        ]

    def forget_directives(self, tenant: TenantId, user: UserKey) -> list[ForgetOp]:
        """The user's overlay, deterministic order: (kind, target, instant)."""
        rows = [op for pair, op in self._forgets.items() if pair[0] == tenant and pair[1] == user]
        rows.sort(key=lambda op: (op.kind, op.target or "", op.forget_at))
        return rows

    def nodes_of(self, tenant: TenantId, keys: Sequence[str]) -> list[NodeRow]:
        wanted = set(keys)
        return [
            NodeRow(tenant=op.tenant, key=op.key, labels=op.labels, props=op.props)
            for (t, key), op in self._nodes.items()
            if t == tenant and key in wanted
        ]

    def users(self, tenant: TenantId) -> list[UserKey]:
        return sorted(
            {UserKey(user) for (t, user, _kind, _target, _at) in self._edges if t == tenant}
        )

    def clear_kind(self, kind: str) -> None:
        self._edges = {key: op for key, op in self._edges.items() if op.kind != kind}

    def replace_all(self) -> None:
        """Open a rebuild: stage into a hold-out readers cannot observe."""
        self._staged_nodes = {}
        self._staged_edges = {}

    def commit_rebuild(self) -> None:
        """Publish the staged state atomically (single pointer swap)."""
        if self._staged_nodes is None or self._staged_edges is None:
            return
        self._nodes = self._staged_nodes
        self._edges = self._staged_edges
        self._staged_nodes = None
        self._staged_edges = None
