"""ClickHouse-backed GraphGateway — the live tier of the graph.

Adapters add, and this one is the live counterpart of the in-memory gateway,
sharing the exact observation-identity semantics:

- inserts replace on ``(tenant, user, kind, target, valid_at)`` via
  ReplacingMergeTree — order-independent and replay-convergent server-side;
- every read carries the tenant predicate in the WHERE clause;
- ``clear_kind`` uses ``DELETE FROM`` (lightweight delete) for the
  reflection tick's wholesale similarity rewrite;
- ``replace_all`` opens a staged rebuild; ``commit_rebuild`` publishes it
  with an atomic ``EXCHANGE TABLES`` — readers never see a half-built graph.

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
from graph_memory.mapping.compiler import EdgeOp, ForgetOp, NodeOp
from graph_memory.projector.ddl import build_graph_ddl
from graph_memory.projector.gateway import (
    EdgeRow,
    NodeRow,
    observation_is_dead,
)
from graph_memory.settings import Settings

_NODES_COLUMNS = ("tenant_id", "key", "labels", "props_json")
_EDGES_COLUMNS = ("tenant_id", "user_key", "kind", "target", "weight", "half_life_days", "valid_at")
_FORGETS_COLUMNS = ("tenant_id", "user_key", "kind", "target", "target_any", "forget_at")


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
        forgets_table: str | None = None,
        client_factory: Callable[[Settings], Client] = default_client,
    ) -> None:
        self._database = database or settings.clickhouse_database
        self._nodes_table = nodes_table or settings.nodes_table
        self._edges_table = edges_table or settings.edges_table
        self._forgets_table = forgets_table or settings.forgets_table
        self._settings = settings
        self._rebuild_open = False
        self._client_factory = bind_thread_local(client_factory)

    def _target(self, table: str) -> str:
        """Where writes land: staging during a rebuild, published otherwise."""
        base = f"{self._database}.{table}"
        return f"{base}__staging" if self._rebuild_open else base

    def _client(self) -> Client:
        """The calling thread's client (one HTTP session per thread)."""
        return self._client_factory(self._settings)

    @property
    def client(self) -> Client:
        """Ops escape hatch (live tests use it for cleanup; caller's thread)."""
        return self._client()

    def ensure_graph_schema(self) -> None:
        for statement in build_graph_ddl(
            self._database, self._nodes_table, self._edges_table, self._forgets_table
        ):
            self._client().command(statement)
        # Rebuild staging partners: nodes/edges only — the forget overlay is
        # replay-idempotent (one event => one identity row) so staging adds
        # no convergence value; readers keep governing by the live overlay.
        for statement in build_graph_ddl(
            self._database,
            self._staging_name(self._nodes_table),
            self._staging_name(self._edges_table),
        ):
            self._client().command(statement)

    def _staging_name(self, table: str) -> str:
        return f"{table}__staging"

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
                self._target(self._nodes_table),
                node_rows,
                column_names=list(_NODES_COLUMNS),
            )
        if edge_rows:
            self._client().insert(
                self._target(self._edges_table),
                edge_rows,
                column_names=list(_EDGES_COLUMNS),
            )

    def apply_forgets(self, forgets: Sequence[ForgetOp]) -> None:
        """Directives land in the live overlay table (never staging).

        Replaying a rebuild re-applies identical directive rows, and
        ReplacingMergeTree collapses them on their identity — so the overlay
        converges without participating in the EXCHANGE TABLES swap.
        """
        rows = []
        for op in forgets:
            wide = op.target is None
            target = "" if wide else op.target
            flag = 0 if wide else 1
            rows.append((op.tenant, op.user, op.kind, target, flag, op.forget_at))
        if rows:
            self._client().insert(
                self._target_forgets(),
                rows,
                column_names=list(_FORGETS_COLUMNS),
            )

    def _target_forgets(self) -> str:
        """The overlay is always written published-side, even mid-rebuild."""
        return f"{self._database}.{self._forgets_table}"

    def observations(self, tenant: TenantId, user: UserKey) -> list[EdgeRow]:
        result = self._client().query(
            f"SELECT tenant_id, user_key, kind, target, weight, half_life_days, valid_at "
            f"FROM {self._database}.{self._edges_table} FINAL "
            "WHERE tenant_id = {tenant:String} AND user_key = {user:String} "
            "ORDER BY kind ASC, target ASC, valid_at ASC",
            parameters={"tenant": str(tenant), "user": str(user)},
        )
        rows = [
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
        directives = self.forget_directives(tenant, user)
        return [row for row in rows if not observation_is_dead(row, directives)]

    def search_edges(
        self, tenant: TenantId, user: UserKey, tokens: Sequence[str]
    ) -> list[tuple[EdgeRow, int]]:
        """The in-memory contract, with the token filter pushed into SQL.

        ClickHouse does the case-insensitive substring matching
        (``positionCaseInsensitive`` per bounded token — the full-text half of
        research 06 lever 2; BM25-style ranking stays parked behind
        ``core.ports.EmbeddingProvider`` economics, not on the hot path). One
        integer per row equals ``core.lexicon.lexical_hits`` exactly, and the
        forget overlay filtering happens on the client side like everywhere
        else — one dead-visibility rule, evaluated once.
        """
        parameters: dict[str, str] = {"tenant": str(tenant), "user": str(user)}
        per_token: list[str] = []
        for index, token in enumerate(tokens):
            name = f"tok{index}"
            parameters[name] = str(token)
            per_token.append(
                f"(positionCaseInsensitive(concat(kind, ' ', target), {{{name}:String}}) > 0)"
            )
        hits_sql = " + ".join(per_token) if per_token else "0"
        result = self._client().query(
            f"SELECT tenant_id, user_key, kind, target, weight, half_life_days, valid_at, "
            f"({hits_sql}) AS token_hits "
            f"FROM {self._database}.{self._edges_table} FINAL "
            "WHERE tenant_id = {tenant:String} AND user_key = {user:String} "
            "ORDER BY kind ASC, target ASC, valid_at ASC",
            parameters=parameters,
        )
        rows = [
            (
                EdgeRow(
                    tenant=TenantId(str(row[0])),
                    user=UserKey(str(row[1])),
                    kind=str(row[2]),
                    target=str(row[3]),
                    weight=float(row[4]),
                    half_life_days=float(row[5]),
                    valid_at=_aware(row[6]),
                ),
                int(row[7]),
            )
            for row in result.result_rows
        ]
        directives = self.forget_directives(tenant, user)
        return [(row, hits) for row, hits in rows if not observation_is_dead(row, directives)]

    def forget_directives(self, tenant: TenantId, user: UserKey) -> list[ForgetOp]:
        result = self._client().query(
            f"SELECT tenant_id, user_key, kind, target, target_any, forget_at "
            f"FROM {self._database}.{self._forgets_table} FINAL "
            "WHERE tenant_id = {tenant:String} AND user_key = {user:String} "
            "ORDER BY kind ASC, target ASC, forget_at ASC",
            parameters={"tenant": str(tenant), "user": str(user)},
        )
        directives = [
            ForgetOp(
                tenant=tenant,
                user=user,
                kind=str(row[2]),
                target=None if int(row[4]) == 1 else str(row[3]),
                forget_at=_aware(row[5]),
            )
            for row in result.result_rows
        ]
        directives.sort(key=lambda op: (op.kind, op.target or "", op.forget_at))
        return directives

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
        """Open a rebuild: stage into hold-out tables readers never see.

        Truncating the STAGING side (not the published tables) keeps serving
        readers on the last committed projection while the pass runs.
        """
        self._client().command(
            f"TRUNCATE TABLE {self._database}.{self._staging_name(self._nodes_table)}"
        )
        self._client().command(
            f"TRUNCATE TABLE {self._database}.{self._staging_name(self._edges_table)}"
        )
        self._rebuild_open = True

    def commit_rebuild(self) -> None:
        """Publish the staged state atomically (metadata-level table swap).

        CLICKHOUSE ``EXCHANGE TABLES`` is atomic: readers see either the
        previous projection or the new one, never a mixture. Afterwards the
        (now stale, previously published) rows live in staging and are punted
        so the next pass starts clean.
        """
        if not self._rebuild_open:
            return
        self._client().command(
            f"EXCHANGE TABLES {self._database}.{self._nodes_table} "
            f"AND {self._database}.{self._staging_name(self._nodes_table)}"
        )
        self._client().command(
            f"EXCHANGE TABLES {self._database}.{self._edges_table} "
            f"AND {self._database}.{self._staging_name(self._edges_table)}"
        )
        self._client().command(
            f"TRUNCATE TABLE {self._database}.{self._staging_name(self._nodes_table)}"
        )
        self._client().command(
            f"TRUNCATE TABLE {self._database}.{self._staging_name(self._edges_table)}"
        )
        self._rebuild_open = False
