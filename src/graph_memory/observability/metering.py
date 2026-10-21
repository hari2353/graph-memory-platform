"""Per-tenant metering (SPEC §5 observability, R7, from [03]): from day 1.

Two halves of one port:

- ``MeteringCounters`` — the in-memory per-``(tenant, lane, event)`` ticker,
  thread-safe, bounded by tenants×lanes. Channels tick admitted traffic and
  throttles per request class.
- ``MeteringTable`` — the durable half: buffered appends to the
  ``metering_events`` table (tenant-scoped rows, one row per flush window).
  The composition root flushes on its interval; a crash loses at most one
  window (metering is evidence, never correctness).
"""

from __future__ import annotations

import threading
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Literal

from clickhouse_connect.driver.client import Client

from graph_memory.chclient import bind_thread_local, default_client
from graph_memory.core.types import TenantId
from graph_memory.settings import Settings

MeteringEvent = Literal["requests", "throttled"]

_DDL = (
    """
    CREATE DATABASE IF NOT EXISTS {database:Identifier}
    """,
    """
    CREATE TABLE IF NOT EXISTS {database:Identifier}.{table:Identifier} (
      tenant_id LowCardinality(String),
      lane      LowCardinality(String),
      event     LowCardinality(String),
      count     UInt64,
      at        DateTime64(3, 'UTC')
    )
    ENGINE = MergeTree
    ORDER BY (tenant_id, at)
    """,
)


class MeteringCounters:
    """Per-(tenant, lane, event) counters; thread-safe; never unbounded."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: Counter[tuple[str, str, str]] = Counter()

    def record(self, tenant: TenantId, lane: str, event: MeteringEvent) -> None:
        with self._lock:
            self._counters[(str(tenant), lane, event)] += 1

    def drain(self) -> dict[tuple[str, str, str], int]:
        """Snapshot-and-reset; the flush window's rows."""
        with self._lock:
            rows = dict(self._counters)
            self._counters.clear()
            return rows


class MeteringTable:
    """Buffered durable rows: run ``flush()`` from the composition root."""

    def __init__(
        self,
        settings: Settings,
        *,
        database: str | None = None,
        table: str = "metering_events",
        client_factory: Callable[[Settings], Client] = default_client,
    ) -> None:
        self._database = database or settings.clickhouse_database
        self._table = table
        self._settings = settings
        self._client_factory = bind_thread_local(client_factory)

    def _client(self) -> Client:
        return self._client_factory(self._settings)

    def ensure_schema(self) -> None:
        for statement in _DDL:
            self._client().command(
                statement, parameters={"database": self._database, "table": self._table}
            )

    def append(self, rows: dict[tuple[str, str, str], int], at: datetime | None = None) -> None:
        """Drained counter rows land in one batch insert."""
        payload = [
            (tenant, lane, event, count, at or datetime.now(tz=UTC))
            for (tenant, lane, event), count in rows.items()
        ]
        if not payload:
            return
        self._client().insert(
            f"{self._database}.{self._table}",
            payload,
            column_names=["tenant_id", "lane", "event", "count", "at"],
        )

    def recent(self, tenant: TenantId | None = None, limit: int = 20) -> list[tuple[object, ...]]:
        """Ops view: latest metering rows, tenant-filtered."""
        where = "WHERE tenant_id = {tenant:String} " if tenant is not None else ""
        parameters = {"tenant": str(tenant), "limit": limit}
        result = self._client().query(
            f"SELECT tenant_id, lane, event, count, at FROM {self._database}.{self._table} "
            f"{where}ORDER BY at DESC, tenant_id LIMIT {{limit:UInt32}}",
            parameters=parameters,
        )
        return [tuple(row) for row in result.result_rows]
