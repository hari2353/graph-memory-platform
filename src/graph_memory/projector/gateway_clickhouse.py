"""ClickHouse-backed GraphGateway — the live tier of the graph.

Adapters add, and this one is the live counterpart of the in-memory gateway,
sharing the exact observation-identity semantics:

- inserts replace on ``(tenant, user, kind, target, valid_at)`` via
  ReplacingMergeTree — order-independent and replay-convergent server-side;
- every read carries the tenant predicate in the WHERE clause;
- ``clear_kind`` uses ``DELETE FROM`` (lightweight delete) for the
  reflection tick's wholesale similarity rewrite;
- ``replace_all`` truncates (an ops decision, used by replay rebuilds).

Sync by contract — call it from threads at the edges.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from datetime import UTC, datetime

from clickhouse_connect.driver.client import Client

from graph_memory.chclient import bind_thread_local
from graph_memory.chclient import default_client as default_client  # re-export (live tests)
from graph_memory.core.types import TenantId, UserKey
from graph_memory.mapping.compiler import EdgeOp, NodeOp
from graph_memory.projector.ddl import build_graph_ddl
from graph_memory.projector.gateway import EdgeRow, NodeRow
from graph_memory.settings import Settings

_NODES_COLUMNS = ("tenant_id", "key", "labels", "props_json")
_EDGES_COLUMNS = ("tenant_id", "user_key", "kind", "target", "weight", "half_life_days", "valid_at")


def _aware(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"expected datetime, got {type(value).__name__}")
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


class ClickHouseGateway:
    """Graph state over ClickHouse; identical semantics, real durability."""

    def __init__(
        self,
        settings: Settings,
        *,
        database: str | None = None,
        nodes_table: str | None = None,
        edges_table: str | None = None,
        client_factory: Callable[[Settings], Client] = default_client,
    ) -> None:
        self._database = database or settings.clickhouse_database
        self._nodes_table = nodes_table or settings.nodes_table
        self._edges_table = edges_table or settings.edges_table
        self._settings = settings
        self._client_factory = bind_thread_local(client_factory)

    def _client(self) -> Client:
        """The calling thread's client (one HTTP session per thread)."""
        return self._client_factory(self._settings)

    @property
    def client(self) -> Client:
        """Ops escape hatch (live tests use it for cleanup; caller's thread)."""
        return self._client()

    def ensure_graph_schema(self) -> None:
        for statement in build_graph_ddl(self._database, self._nodes_table, self._edges_table):
            self._client().command(statement)

    def apply(self, ops: Sequence[NodeOp | EdgeOp]) -> None:
        node_rows: list[tuple[object, ...]] = []
        edge_rows: list[tuple[object, ...]] = []
        for op in ops:
            if isinstance(op, NodeOp):
                node_rows.append(
                    (
                        op.tenant,
                        op.key,
                        list(op.labels),
                        json.dumps(op.props, sort_keys=True),
                    )
                )
            elif isinstance(op, EdgeOp):
                edge_rows.append(
                    (
                        op.tenant,
                        op.user,
                        op.kind,
                        op.target,
                        op.weight,
                        op.half_life_days,
                        op.valid_at,
                    )
                )
            else:
                raise TypeError(f"unsupported graph op: {type(op).__name__}")
        if node_rows:
            self._client().insert(
                f"{self._database}.{self._nodes_table}",
                node_rows,
                column_names=list(_NODES_COLUMNS),
            )
        if edge_rows:
            self._client().insert(
                f"{self._database}.{self._edges_table}",
                edge_rows,
                column_names=list(_EDGES_COLUMNS),
            )

    def observations(self, tenant: TenantId, user: UserKey) -> list[EdgeRow]:
        result = self._client().query(
            f"SELECT tenant_id, user_key, kind, target, weight, half_life_days, valid_at "
            f"FROM {self._database}.{self._edges_table} FINAL "
            "WHERE tenant_id = {tenant:String} AND user_key = {user:String} "
            "ORDER BY kind ASC, target ASC, valid_at ASC",
            parameters={"tenant": str(tenant), "user": str(user)},
        )
        return [
            EdgeRow(
                tenant=TenantId(str(row[0])),
                user=UserKey(str(row[1])),
                kind=str(row[2]),
                target=str(row[3]),
                weight=float(row[4]),
                half_life_days=float(row[5]),
                valid_at=_aware(row[6]),
            )
            for row in result.result_rows
        ]

    def nodes_of(self, tenant: TenantId, keys: Sequence[str]) -> list[NodeRow]:
        if not keys:
            return []
        result = self._client().query(
            f"SELECT tenant_id, key, labels, props_json "
            f"FROM {self._database}.{self._nodes_table} FINAL "
            "WHERE tenant_id = {tenant:String} AND key IN {keys:Array(String)}",
            parameters={"tenant": str(tenant), "keys": sorted(set(keys))},
        )
        return [
            NodeRow(
                tenant=TenantId(str(row[0])),
                key=str(row[1]),
                labels=tuple(str(label) for label in row[2]),
                props=dict(json.loads(str(row[3]))),
            )
            for row in result.result_rows
        ]

    def users(self, tenant: TenantId) -> list[UserKey]:
        result = self._client().query(
            f"SELECT DISTINCT user_key FROM {self._database}.{self._edges_table} FINAL "
            "WHERE tenant_id = {tenant:String} ORDER BY user_key ASC",
            parameters={"tenant": str(tenant)},
        )
        return [UserKey(str(row[0])) for row in result.result_rows]

    def clear_kind(self, kind: str) -> None:
        self._client().command(
            f"DELETE FROM {self._database}.{self._edges_table} WHERE kind = {{kind:String}}",
            parameters={"kind": kind},
        )

    def replace_all(self) -> None:
        self._client().command(f"TRUNCATE TABLE {self._database}.{self._nodes_table}")
        self._client().command(f"TRUNCATE TABLE {self._database}.{self._edges_table}")
