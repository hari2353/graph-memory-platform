"""GraphGateway port: the ONLY surface through which the projector mutates
or reads graph state. Observations are stored by identity —
``(tenant, user, kind, target, valid_at)`` — which makes state:
a) order-independent (deterministic under any delivery permutation),
b) trivially replayable (the same events produce the same keys),
c) merge-friendly toward a ClickHouse ReplacingMergeTree later.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from graph_memory.core.types import TenantId, UserKey
from graph_memory.mapping.compiler import EdgeOp, NodeOp


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


class GraphGateway(Protocol):
    """Graph state port; adapters add, core never changes.

    Rebuild protocol (used by full replays): ``replace_all`` opens a rebuild
    side so subsequent ``apply`` writes stage into a hold-out that readers do
    not see; ``commit_rebuild`` publishes the staged state atomically. A
    serving instance therefore never exposes a half-built graph: readers keep
    observing the last committed projection mid-rebuild.
    """

    def apply(self, ops: Sequence[NodeOp | EdgeOp]) -> None: ...

    def observations(self, tenant: TenantId, user: UserKey) -> list[EdgeRow]: ...

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

    def observations(self, tenant: TenantId, user: UserKey) -> list[EdgeRow]:
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
