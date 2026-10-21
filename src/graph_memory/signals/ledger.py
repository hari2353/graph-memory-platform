"""The event ledger: append-only, idempotent, the single source of truth.

Two implementations, one contract (the same conformance suite runs against
both):

- :class:`InMemoryLedger` — deterministic memory backend for unit/conformance.
- :class:`ClickHouseLedger` — ClickHouse writes (ReplacingMergeTree dedup +
  FINAL reads; broker-level dedup is best-effort, the ledger hosts the
  correctness owner — research 02).

Dedup scope is **(tenant, dedup_key)**: the same CloudEvents id may exist in
two tenants; it is two facts, one per tenant. The port is deliberately
**sync**: DB IO runs in dedicated threads at the edges (FastAPI threadpool /
``asyncio.to_thread`` / projector workers), keeping the core licensing-free.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from clickhouse_connect.driver.client import Client

from graph_memory.chclient import bind_thread_local
from graph_memory.core.types import IngestResult, SignalType, TenantId, UserKey
from graph_memory.settings import Settings
from graph_memory.signals.envelope import Envelope
from graph_memory.signals.sql.ddl import build_ddl

_LEDGER_COLUMNS = (
    "tenant_id",
    "signal_type",
    "dedup_key",
    "user_key",
    "occurred_at",
    "ingested_at",
    "envelope_v",
    "payload",
)


@dataclass(frozen=True)
class LedgerRecord:
    """A tenant-bound signal as handed to the ledger; stored verbatim."""

    tenant: TenantId
    envelope: Envelope

    @property
    def dedup_key(self) -> str:
        return self.envelope.dedup_key

    def row(self, ingested_at: datetime | None = None) -> tuple[object, ...]:
        """Column tuple for insert; ``payload`` is the FULL envelope JSON."""
        stamp = ingested_at if ingested_at is not None else self.envelope.occurred_at
        return (
            self.tenant,
            self.envelope.type,
            self.dedup_key,
            self.envelope.partitionkey,
            self.envelope.occurred_at,
            stamp,
            1,  # ENVELOPE_VERSION — additive-only evolution
            self.envelope.model_dump_json(),
        )


def record_from_envelope(tenant: TenantId, envelope: Envelope) -> LedgerRecord:
    return LedgerRecord(tenant=tenant, envelope=envelope)


@dataclass(frozen=True)
class StoredEvent:
    """One accepted signal as read back from the ledger."""

    tenant: TenantId
    signal: SignalType
    dedup_key: str
    user_key: UserKey
    occurred_at: datetime
    ingested_at: datetime
    envelope_v: int
    payload: dict[str, object]

    @property
    def envelope(self) -> Envelope:
        """Rebuild the exact original envelope from the stored payload."""
        return Envelope.model_validate(self.payload)


class LedgerRepository(Protocol):
    """Append-only truth; idempotent; rebuildable projections pull from here."""

    def ensure_schema(self) -> None: ...
    def append(self, records: Sequence[LedgerRecord]) -> IngestResult: ...
    def existing_keys(self, tenant: TenantId, keys: Sequence[str]) -> set[str]: ...
    def tenants(self) -> list[TenantId]: ...
    def read_events(
        self,
        tenant: TenantId,
        *,
        user_key: UserKey | None = None,
        signal: SignalType | None = None,
        limit: int = 10_000,
    ) -> list[StoredEvent]: ...


class InMemoryLedger:
    """Deterministic conformance backend — identical semantics, zero IO."""

    def __init__(self) -> None:
        self._rows: dict[tuple[TenantId, str], LedgerRecord] = {}

    def ensure_schema(self) -> None:
        return None

    def append(self, records: Sequence[LedgerRecord]) -> IngestResult:
        accepted, duplicates, fresh = self._collapse(records, self._rows)
        for key, record in fresh.items():
            self._rows[key] = record
        return IngestResult(accepted=accepted, duplicates=duplicates)

    def _collapse(
        self, records: Sequence[LedgerRecord], existing: dict[tuple[TenantId, str], LedgerRecord]
    ) -> tuple[int, int, dict[tuple[TenantId, str], LedgerRecord]]:
        batch: dict[tuple[TenantId, str], LedgerRecord] = {}
        duplicates = 0
        for record in records:
            key = (record.tenant, record.dedup_key)
            if key in batch or key in existing:
                duplicates += 1
                continue
            batch[key] = record
        return len(batch), duplicates, batch

    def existing_keys(self, tenant: TenantId, keys: Sequence[str]) -> set[str]:
        wanted = set(keys)
        return {key for (t, key) in self._rows if t == tenant and key in wanted}

    def tenants(self) -> list[TenantId]:
        return sorted({tenant for (tenant, _) in self._rows})

    def read_events(
        self,
        tenant: TenantId,
        *,
        user_key: UserKey | None = None,
        signal: SignalType | None = None,
        limit: int = 10_000,
    ) -> list[StoredEvent]:
        rows = [record for (t, _), record in self._rows.items() if t == tenant]
        if user_key is not None:
            rows = [r for r in rows if r.envelope.partitionkey == user_key]
        if signal is not None:
            rows = [r for r in rows if r.envelope.type == signal]
        rows.sort(key=lambda r: (r.envelope.occurred_at, r.dedup_key))
        return [_stored(record) for record in rows[: min(limit, 10_000)]]


def _stored(record: LedgerRecord) -> StoredEvent:
    raw = record.envelope.model_dump(mode="json")
    occurred = record.envelope.occurred_at
    return StoredEvent(
        tenant=record.tenant,
        signal=SignalType(record.envelope.type),
        dedup_key=record.dedup_key,
        user_key=UserKey(record.envelope.partitionkey),
        occurred_at=occurred,
        ingested_at=occurred,
        envelope_v=1,
        payload=json_payload(raw),
    )


def json_payload(raw: object) -> dict[str, object]:
    """The verbatim JSON envelope object as read back (dict guaranteed)."""
    if not isinstance(raw, dict):
        raise TypeError("payload must be a JSON object")
    return {str(k): v for k, v in raw.items()}


def default_client(settings: Settings) -> Client:
    """The documented local CH posture; environments swap it via Settings."""
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_username,
        password=settings.clickhouse_password,
    )


class ClickHouseLedger:
    """ClickHouse-backed ledger; FINAL reads keep the projection detachable."""

    def __init__(
        self,
        settings: Settings,
        *,
        database: str | None = None,
        signals_table: str | None = None,
        dlq_table: str | None = None,
        client_factory: Callable[[Settings], Client] = default_client,
    ) -> None:
        self._database = database or settings.clickhouse_database
        self._signals_table = signals_table or settings.signals_table
        self._dlq_table = dlq_table or settings.dlq_table
        self._settings = settings
        self._client_factory = bind_thread_local(client_factory)

    def _client(self) -> Client:
        """The calling thread's client (one HTTP session per thread)."""
        return self._client_factory(self._settings)

    @property
    def client(self) -> Client:
        """Direct client escape hatch - used only by ops/epochs, never by core."""
        return self._client()

    def ensure_schema(self) -> None:
        for statement in build_ddl(self._database, self._signals_table, self._dlq_table):
            self._client().command(statement)

    def existing_keys(self, tenant: TenantId, keys: Sequence[str]) -> set[str]:
        if not keys:
            return set()
        unique = sorted(set(keys))
        result = self._client().query(
            f"SELECT DISTINCT dedup_key FROM {self._database}.{self._signals_table} "
            "WHERE tenant_id = {tenant:String} AND dedup_key IN {keys:Array(String)}",
            parameters={"tenant": str(tenant), "keys": unique},
        )
        return {str(row[0]) for row in result.result_rows}

    def tenants(self) -> list[TenantId]:
        result = self._client().query(
            f"SELECT DISTINCT tenant_id FROM {self._database}.{self._signals_table} "
            "ORDER BY tenant_id ASC"
        )
        return [TenantId(str(row[0])) for row in result.result_rows]

    def append(self, records: Sequence[LedgerRecord]) -> IngestResult:
        if not records:
            return IngestResult(accepted=0, duplicates=0)
        awaiting: dict[tuple[TenantId, str], LedgerRecord] = {}
        duplicates = 0
        for record in records:
            key = (record.tenant, record.dedup_key)
            if key in awaiting:
                duplicates += 1
                continue
            awaiting[key] = record
        precheck_tenants = sorted({record.tenant for record in awaiting.values()})
        existing_by_tenant: dict[TenantId, set[str]] = {}
        for tenant in precheck_tenants:
            existing_by_tenant[tenant] = self.existing_keys(
                tenant,
                [r.dedup_key for r in awaiting.values() if r.tenant == tenant],
            )
        fresh = [
            record
            for record in awaiting.values()
            if record.dedup_key not in existing_by_tenant[record.tenant]
        ]
        handled = len(awaiting) - len(fresh)
        if fresh:
            rows = [record.row() for record in fresh]
            self._client().insert(
                f"{self._database}.{self._signals_table}",
                rows,
                column_names=list(_LEDGER_COLUMNS),
            )
        return IngestResult(accepted=len(fresh), duplicates=duplicates + handled)

    def read_events(
        self,
        tenant: TenantId,
        *,
        user_key: UserKey | None = None,
        signal: SignalType | None = None,
        limit: int = 10_000,
    ) -> list[StoredEvent]:
        sql = (
            f"SELECT {', '.join(_LEDGER_COLUMNS)} "
            f"FROM {self._database}.{self._signals_table} FINAL "
            "WHERE tenant_id = {tenant:String}"
        )
        parameters: dict[str, object] = {"tenant": str(tenant)}
        if user_key is not None:
            sql += " AND user_key = {user_key:String}"
            parameters["user_key"] = str(user_key)
        if signal is not None:
            sql += " AND signal_type = {signal:String}"
            parameters["signal"] = str(signal)
        sql += " ORDER BY occurred_at ASC LIMIT {limit:UInt32}"
        parameters["limit"] = min(limit, 10_000)
        result = self._client().query(sql, parameters=parameters)
        return [stored_row(row) for row in result.result_rows]

    def write_dlq(self, tenant: TenantId, payload: str, reason: str) -> None:
        """Poison events land here; the DLQ row TTLs out at 180 days."""
        self._client().insert(
            f"{self._database}.{self._dlq_table}",
            [(str(tenant), payload, reason, 1)],
            column_names=["tenant_id", "payload", "reason", "attempts"],
        )


class LedgerPoisonSink:
    """PoisonSink adapter routing rejections into the ledger backend's DLQ.

    The deployment wiring for every non-HTTP ingest path (broker drains and
    any future channel): poison payloads become DLQ rows instead of dropping.
    """

    def __init__(self, ledger: ClickHouseLedger) -> None:
        self._ledger = ledger

    def record(self, tenant: TenantId, payload: str, reason: str) -> None:
        self._ledger.write_dlq(tenant, payload, reason)


def stored_row(row: Sequence[object]) -> StoredEvent:
    """Rebuild a StoredEvent from one FINAL-read ledger row."""
    return StoredEvent(
        tenant=TenantId(str(row[0])),
        signal=SignalType(str(row[1])),
        dedup_key=str(row[2]),
        user_key=UserKey(str(row[3])),
        occurred_at=as_datetime(row[4]),
        ingested_at=as_datetime(row[5]),
        envelope_v=int(str(row[6])),
        payload=json_payload(json.loads(str(row[7]))),
    )


def as_datetime(value: object) -> datetime:
    """DateTime64('UTC') reads may arrive naive (driver behavior) — normalize.

    The column is definitionally UTC, so attaching UTC is schema-faithful, not
    an assumption; the invariant (aware instants) stays true for every backend.
    """
    if not isinstance(value, datetime):
        raise TypeError(f"expected datetime, got {type(value).__name__}")
    if value.tzinfo is not None:
        return value
    return value.replace(tzinfo=UTC)
