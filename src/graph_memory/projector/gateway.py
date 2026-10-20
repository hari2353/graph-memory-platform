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
    """Graph state port; adapters add, core never changes."""

    def apply(self, ops: Sequence[NodeOp | EdgeOp]) -> None: ...

    def observations(self, tenant: TenantId, user: UserKey) -> list[EdgeRow]: ...

    def nodes_of(self, tenant: TenantId, keys: Sequence[str]) -> list[NodeRow]: ...

    def users(self, tenant: TenantId) -> list[UserKey]: ...

    def clear_kind(self, kind: str) -> None: ...

    def replace_all(self) -> None: ...


class InMemoryGraphGateway:
    """Deterministic store; identical semantics to the future CH adapter."""

    def __init__(self) -> None:
        self._nodes: dict[tuple[str, str], NodeOp] = {}
        self._edges: dict[tuple[str, str, str, str, datetime], EdgeOp] = {}

    def apply(self, ops: Sequence[NodeOp | EdgeOp]) -> None:
        for op in ops:
            match op:
                case NodeOp():
                    self._nodes[(op.tenant, op.key)] = op
                case EdgeOp():
                    key = (op.tenant, op.user, op.kind, op.target, op.valid_at)
                    self._edges[key] = op
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
        self._nodes.clear()
        self._edges.clear()
