"""Observability surfaces (SPEC §11/R7): metering, the CH span-stub sink,
and the durable tenant registry — all through injected stub clients so the
offline tier owns their logic; the live tier owns only transport truth.
"""

from __future__ import annotations

import threading
from collections.abc import Sequence
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest

from graph_memory.core.types import TenantId
from graph_memory.observability.audit_ch import ClickHouseSpanSink
from graph_memory.observability.metering import MeteringCounters, MeteringTable
from graph_memory.observability.observer import SpanStub
from graph_memory.settings import Settings
from graph_memory.tenancy.errors import TenantNotFound
from graph_memory.tenancy.registry import TenantState
from graph_memory.tenancy.registry_ch import ClickHouseTenantRegistry

_AT = datetime(2026, 6, 10, 12, 0, tzinfo=UTC)


class _StubClient:
    """Records driver calls; `query` replays a sequential row-set script."""

    def __init__(self, script: list[list[tuple[object, ...]]] | None = None) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.script: list[list[tuple[object, ...]]] = script or []
        self.mutated: list[list[tuple[object, ...]]] = []

    def command(self, statement: str, parameters: dict[str, object] | None = None) -> object:
        self.calls.append(("command", (statement, parameters)))
        return None

    def insert(
        self,
        table: str,
        rows: Sequence[Sequence[object]],
        column_names: list[str] | None = None,
    ) -> object:
        self.calls.append(("insert", (table, tuple(column_names or ()))))
        self.mutated.append([tuple(row) for row in rows])
        return None

    def query(self, sql: str, parameters: dict[str, object] | None = None) -> SimpleNamespace:
        self.calls.append(("query", (sql, parameters)))
        rows = self.script.pop(0) if self.script else []
        return SimpleNamespace(result_rows=list(rows))


def _span(tenant: str = "acme") -> SpanStub:
    return SpanStub(
        name="gmp.channel.rest.request",
        tenant=TenantId(tenant),
        at=_AT,
        duration_ms=1.5,
        attributes={"stages_ms": {"authn": 1.0}, "outcome": "success"},
    )


# ---------------------------------------------------------------- metering


def _stub_factory(stub: _StubClient) -> Any:
    """The client_factory port with a stub behind it (transport owned by live)."""
    return lambda _settings: cast("Any", stub)


def test_metering_counters_are_per_tenant_lane_and_drain_resets() -> None:
    counters = MeteringCounters()
    counters.record(TenantId("acme"), "rest", "requests")
    counters.record(TenantId("acme"), "rest", "requests")
    counters.record(TenantId("acme"), "rest", "throttled")
    counters.record(TenantId("other"), "rest", "requests")

    drained = counters.drain()
    assert drained == {
        ("acme", "rest", "requests"): 2,
        ("acme", "rest", "throttled"): 1,
        ("other", "rest", "requests"): 1,
    }
    # Drain-and-reset: the next window starts empty.
    assert counters.drain() == {}


def test_metering_counters_are_thread_safe() -> None:
    counters = MeteringCounters()

    def hammer() -> None:
        for _ in range(500):
            counters.record(TenantId("acme"), "rest", "requests")

    threads = [threading.Thread(target=hammer) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert counters.drain()[("acme", "rest", "requests")] == 4000


def test_metering_table_appends_drained_windows() -> None:
    stub = _StubClient()
    table = MeteringTable(Settings(), client_factory=_stub_factory(stub))
    table.ensure_schema()
    table.append({}, at=_AT)  # empty window is a no-op, no insert
    table.append({("acme", "rest", "requests"): 3}, at=_AT)

    command_calls = [call for call in stub.calls if call[0] == "command"]
    insert_calls = [call for call in stub.calls if call[0] == "insert"]
    assert len(command_calls) == 2  # database + table DDL
    assert len(insert_calls) == 1
    assert stub.mutated[0] == [("acme", "rest", "requests", 3, _AT)]

    stub.script = [[("acme", "rest", "requests", 3, _AT)]]
    rows = table.recent(TenantId("acme"), limit=1)
    assert rows == [("acme", "rest", "requests", 3, _AT)]


# --------------------------------------------------------- span audit stub


def test_span_sink_buffers_and_flushes_once_overflow_hits() -> None:
    stub = _StubClient()
    sink = ClickHouseSpanSink(Settings(), client_factory=_stub_factory(stub))
    sink.ensure_schema()
    for _ in range(128):
        sink.append(_span())
    # The 128th append triggered the automatic flush, exactly one batch.
    assert len([call for call in stub.calls if call[0] == "insert"]) == 1
    assert stub.mutated[0][0][0] == "acme"
    assert float(cast("float", stub.mutated[0][0][3])) == 1.5

    assert sink.flush() == 0  # empty flush is a no-op
    sink.append(_span(tenant="other"))
    assert sink.flush() == 1


def test_span_sink_recent_passes_tenant_filter() -> None:
    stub = _StubClient(script=[[("acme", "gmp.channel.rest.request", _AT, 1.5, "{}")]])
    sink = ClickHouseSpanSink(Settings(), client_factory=_stub_factory(stub))
    rows = sink.recent(tenant="acme", limit=5)
    assert rows == [("acme", "gmp.channel.rest.request", _AT, 1.5, "{}")]
    queried = [call for call in stub.calls if call[0] == "query"][0][1][0]
    assert "tenant_id" in queried


# ----------------------------------------------------- tenant registry (CH)

_ROW = ("acme", "standard", "active", _AT, _AT)


def test_registry_onboard_is_idempotent_and_writes_one_metadata_row() -> None:
    stub = _StubClient()
    registry = ClickHouseTenantRegistry(Settings(), client_factory=_stub_factory(stub))
    registry.ensure_registry_schema()
    # First onboard: pre-insert fetch empty, post-insert get reads the row.
    stub.script = [[], [_ROW]]
    record = registry.onboard("acme")
    assert record.tenant_id == "acme"
    assert record.state is TenantState.ACTIVE
    first_insert = [call for call in stub.calls if call[0] == "insert"]
    assert len(first_insert) == 1

    # Retry path: pre-insert fetch already finds the row; no second write.
    stub.script = [[_ROW]]
    record_again = registry.onboard("acme")
    assert record_again.tenant_id == "acme"
    assert [call for call in stub.calls if call[0] == "insert"] == first_insert


def test_registry_get_unknown_tenant_raises_tenant_not_found() -> None:
    registry = ClickHouseTenantRegistry(Settings(), client_factory=_stub_factory(_StubClient()))
    with pytest.raises(TenantNotFound):
        registry.get("ghost")


def test_registry_revoke_writes_a_new_state_row() -> None:
    stub = _StubClient(script=[[_ROW]])
    registry = ClickHouseTenantRegistry(Settings(), client_factory=_stub_factory(stub))
    record = registry.set_state("acme", TenantState.REVOKED)
    assert record.state is TenantState.REVOKED
    inserts = [call for call in stub.calls if call[0] == "insert"]
    assert len(inserts) == 1
    assert stub.mutated[0][0][2] == "revoked"  # state column


def test_registry_tenants_uses_final_reads() -> None:
    stub = _StubClient(script=[[_ROW], [(1,)]])
    registry = ClickHouseTenantRegistry(Settings(), client_factory=_stub_factory(stub))
    assert [record.tenant_id for record in registry.tenants()] == ["acme"]
    assert registry.count() == 1
    reads = [call[1][0] for call in stub.calls if call[0] == "query"]
    assert all("FINAL" in sql for sql in reads)
