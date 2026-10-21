"""ClickHouse-backed tenant registry — the durable metadata rows.

Same face as the in-memory ``TenantRegistry`` (the conformance shape): the
registry owns ONLY metadata rows (§5 tenancy — no per-tenant DDL, ever).
Lifecycle transitions reuse the shared transition rules; reads carry the
real durability so ``platform tenancy`` ops survive restarts.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import UTC, datetime

from clickhouse_connect.driver.client import Client

from graph_memory.chclient import bind_thread_local, default_client
from graph_memory.core.types import TenantId, tenant_id
from graph_memory.settings import Settings
from graph_memory.tenancy.errors import TenantNotFound
from graph_memory.tenancy.registry import (
    TenantClass,
    TenantRecord,
    TenantState,
    _assert_transition,  # noqa: PLC2701 - shared transition rules
)

_REGISTRY_DDL = (
    """
    CREATE DATABASE IF NOT EXISTS {database:Identifier}
    """,
    """
    CREATE TABLE IF NOT EXISTS {database:Identifier}.{table:Identifier} (
      tenant_id    LowCardinality(String),
      tenant_class LowCardinality(String),
      state        LowCardinality(String),
      onboarded_at DateTime64(3, 'UTC'),
      updated_at   DateTime64(3, 'UTC')
    )
    ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY tenant_id
    """,
)


class ClickHouseTenantRegistry:
    """Metadata rows over ClickHouse; ops-visible and restart-durable."""

    def __init__(
        self,
        settings: Settings,
        *,
        database: str | None = None,
        table: str = "tenant_registry",
        client_factory: Callable[[Settings], Client] = default_client,
    ) -> None:
        self._database = database or settings.clickhouse_database
        self._table = table
        self._settings = settings
        self._client_factory = bind_thread_local(client_factory)

    def _client(self) -> Client:
        return self._client_factory(self._settings)

    def ensure_registry_schema(self) -> None:
        for statement in _REGISTRY_DDL:
            self._client().command(
                statement,
                parameters={"database": self._database, "table": self._table},
            )

    def onboard(
        self, tenant: str | TenantId, *, tenant_class: TenantClass = TenantClass.STANDARD
    ) -> TenantRecord:
        ref = tenant_id(str(tenant))
        now = datetime.now(tz=UTC)
        existing = self._fetch(str(ref))
        if existing is not None:
            return existing  # idempotent, retry-safe — same contract as memory
        self._client().insert(
            f"{self._database}.{self._table}",
            [(str(ref), str(tenant_class), str(TenantState.ACTIVE), now, now)],
            column_names=["tenant_id", "tenant_class", "state", "onboarded_at", "updated_at"],
        )
        return self.get(ref)

    def get(self, tenant: str | TenantId) -> TenantRecord:
        record = self._fetch(str(tenant_id(str(tenant))))
        if record is None:
            raise TenantNotFound(
                f"tenant {tenant!r} is not onboarded", details={"tenant": str(tenant)}
            )
        return record

    def set_state(self, tenant: str | TenantId, state: TenantState) -> TenantRecord:
        record = self.get(tenant)
        _assert_transition(record, state)
        now = datetime.now(tz=UTC)
        # ReplacingMergeTree(updated_at): the newest row wins after merges.
        self._client().insert(
            f"{self._database}.{self._table}",
            [
                (
                    str(record.tenant_id),
                    str(record.tenant_class),
                    str(state),
                    record.onboarded_at,
                    now,
                )
            ],
        )
        return TenantRecord(
            tenant_id=record.tenant_id,
            tenant_class=record.tenant_class,
            state=state,
            onboarded_at=record.onboarded_at,
            updated_at=now,
        )

    def tenants(self) -> list[TenantRecord]:
        result = self._client().query(
            f"SELECT tenant_id, tenant_class, state, onboarded_at, updated_at "
            f"FROM {self._database}.{self._table} FINAL ORDER BY tenant_id"
        )
        return [self._record(row) for row in result.result_rows]

    def count(self) -> int:
        result = self._client().query(f"SELECT count() FROM {self._database}.{self._table} FINAL")
        return int(result.result_rows[0][0])

    def _fetch(self, ref: str) -> TenantRecord | None:
        result = self._client().query(
            f"SELECT tenant_id, tenant_class, state, onboarded_at, updated_at "
            f"FROM {self._database}.{self._table} FINAL "
            f"WHERE tenant_id = {{tenant:String}}",
            parameters={"tenant": ref},
        )
        if not result.result_rows:
            return None
        return self._record(result.result_rows[0])

    def _record(self, row: Sequence[object]) -> TenantRecord:
        updated = row[4] if isinstance(row[4], datetime) else row[3]
        return TenantRecord(
            tenant_id=tenant_id(str(row[0])),
            tenant_class=TenantClass(str(row[1])),
            state=TenantState(str(row[2])),
            onboarded_at=row[3] if isinstance(row[3], datetime) else datetime.now(tz=UTC),
            updated_at=updated if isinstance(updated, datetime) else datetime.now(tz=UTC),
        )
