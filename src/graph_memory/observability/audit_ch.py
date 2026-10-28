"""The CH audit-span-stub table (SPEC §11/R7): unsampled traffic lands too.

Full auditability without egress cost: every span the hot path emits can go
to this local table — sampled or not — and the ops surface reads it without
Any external dependency. Langfuse/OTLP exporters are deploy-day adapters
over the same ``SpanSink`` port (the protocol decides; the exporter renders).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC

from clickhouse_connect.driver.client import Client

from graph_memory.chclient import bind_thread_local, default_client
from graph_memory.observability.observer import SpanStub
from graph_memory.settings import Settings

_DDL = (
    """
    CREATE DATABASE IF NOT EXISTS {database:Identifier}
    """,
    """
    CREATE TABLE IF NOT EXISTS {database:Identifier}.{table:Identifier} (
      tenant_id   LowCardinality(String),
      name        LowCardinality(String),
      at          DateTime64(3, 'UTC'),
      duration_ms Float64,
      attrs       String
    )
    ENGINE = MergeTree
    ORDER BY (tenant_id, at)
    """,
)
_BUFFER_LIMIT = 128  # flush automatically past this many buffered spans


class ClickHouseSpanSink:
    """Buffered span writer; bounded memory; rows land on flush or overflow."""

    def __init__(
        self,
        settings: Settings,
        *,
        database: str | None = None,
        table: str = "span_audit",
        client_factory: Callable[[Settings], Client] = default_client,
    ) -> None:
        self._database = database or settings.clickhouse_database
        self._table = table
        self._settings = settings
        self._client_factory = bind_thread_local(client_factory)
        self._buffer: list[list[object]] = []

    def _client(self) -> Client:
        return self._client_factory(self._settings)

    def ensure_schema(self) -> None:
        for statement in _DDL:
            self._client().command(
                statement, parameters={"database": self._database, "table": self._table}
            )

    def record(self, span: SpanStub) -> None:  # SpanSink face (append alias below)
        self._buffer.append(
            [
                str(span.tenant),
                span.name,
                span.at.astimezone(UTC),
                span.duration_ms,
                json.dumps(span.attributes, sort_keys=True),
            ]
        )
        if len(self._buffer) >= _BUFFER_LIMIT:
            self.flush()

    append = record  # the SpanSink protocol's verb

    def flush(self) -> int:
        """Force the buffered rows out; returns how many landed."""
        rows, self._buffer = self._buffer, []
        if not rows:
            return 0
        self._client().insert(
            f"{self._database}.{self._table}",
            rows,
            column_names=["tenant_id", "name", "at", "duration_ms", "attrs"],
        )
        return len(rows)

    def recent(self, tenant: str | None = None, limit: int = 20) -> list[tuple[object, ...]]:
        """Ops view: latest spans, tenant-filtered."""
        where = "WHERE tenant_id = {tenant:String} " if tenant is not None else ""
        result = self._client().query(
            f"SELECT tenant_id, name, at, duration_ms, attrs FROM {self._database}.{self._table} "
            f"{where}ORDER BY at DESC LIMIT {{limit:UInt32}}",
            parameters={"tenant": tenant, "limit": limit},
        )
        return [tuple(row) for row in result.result_rows]
