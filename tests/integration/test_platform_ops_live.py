"""Live-tier ops surfaces (marked live): the durable registry, the span
audit-stub, metering rows, and the operator commands — against real ClickHouse.

Entrypoints skip when ClickHouse is unreachable (ledger-conformance posture).
Tenants use uuid suffixes so reruns never collide.
"""

from __future__ import annotations

import socket
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest

from graph_memory.core.types import TenantId
from graph_memory.observability.audit_ch import ClickHouseSpanSink
from graph_memory.observability.metering import MeteringTable
from graph_memory.observability.observer import SpanStub
from graph_memory.settings import Settings
from graph_memory.tenancy.registry_ch import ClickHouseTenantRegistry

_MAPPING = Path(__file__).resolve().parents[1] / "fixtures" / "mapping_skill_loop.yaml"


def _require() -> None:
    settings = Settings()
    try:
        with socket.create_connection(
            (settings.clickhouse_host, settings.clickhouse_port), timeout=2
        ):
            return
    except OSError:
        pytest.skip("ClickHouse not reachable at the configured endpoint")


@pytest.fixture()
def registry() -> ClickHouseTenantRegistry:
    _require()
    adapter = ClickHouseTenantRegistry(Settings())
    adapter.ensure_registry_schema()
    return adapter


@pytest.mark.live
class TestClickHouseRegistry:
    def test_onboard_get_and_idempotent_retry(self, registry: ClickHouseTenantRegistry) -> None:
        tenant = f"lt-on-{uuid.uuid4().hex[:8]}"
        record = registry.onboard(tenant)
        assert record.tenant_id == tenant
        assert record.state.value == "active"
        fetched = registry.get(tenant)
        assert fetched.tenant_id == record.tenant_id
        assert fetched.state == record.state
        # Idempotency: the retry returns the same face without a new write.
        again = registry.onboard(tenant)
        assert again.tenant_id == tenant
        assert again.state.value == "active"

    def test_revocation_via_set_state(self, registry: ClickHouseTenantRegistry) -> None:
        from graph_memory.tenancy.registry import TenantState

        tenant = f"lt-kill-{uuid.uuid4().hex[:8]}"
        registry.onboard(tenant)
        record = registry.set_state(tenant, TenantState.REVOKED)
        assert record.state.value == "revoked"
        assert registry.get(tenant).state.value == "revoked"

    def test_unknown_tenant_raises_tenant_not_found(
        self, registry: ClickHouseTenantRegistry
    ) -> None:
        from graph_memory.tenancy.errors import TenantNotFound

        with pytest.raises(TenantNotFound):
            registry.get(f"lt-ghost-{uuid.uuid4().hex[:8]}")

    def test_count_grows_inclusive_of_new_row(self, registry: ClickHouseTenantRegistry) -> None:
        before = registry.count()
        registry.onboard(f"lt-count-{uuid.uuid4().hex[:8]}")
        assert registry.count() >= before + 1


@pytest.mark.live
class TestSpanAuditTable:
    def test_append_flush_recent_roundtrip(self) -> None:
        _require()
        sink = ClickHouseSpanSink(Settings())
        sink.ensure_schema()
        span = SpanStub(
            name="gmp.channel.rest.request",
            tenant=f"lt-span-{uuid.uuid4().hex[:6]}",  # type: ignore[arg-type]
            at=datetime.now(tz=UTC),
            duration_ms=0.25,
            attributes={"stages_ms": {"authn": 0.1}, "outcome": "success"},
        )
        sink.append(span)
        assert sink.flush() == 1
        rows = sink.recent(tenant=str(span.tenant), limit=5)
        assert len(rows) == 1
        row = rows[0]
        assert float(cast("float", row[3])) == pytest.approx(0.25)
        assert "authn" in str(row[4])
        assert sink.flush() == 0  # nothing buffered anymore


@pytest.mark.live
class TestMeteringTableLive:
    def test_window_roundtrip(self) -> None:
        _require()
        table = MeteringTable(Settings())
        table.ensure_schema()
        tenant = f"lt-mtr-{uuid.uuid4().hex[:6]}"
        table.append(
            {
                (tenant, "rest", "requests"): 7,
                (tenant, "rest", "throttled"): 1,
            }
        )
        rows = table.recent(tenant=TenantId(tenant), limit=5)
        counts = {(row[0], row[1], row[2]): row[3] for row in rows}
        assert counts.get((tenant, "rest", "requests")) == 7
        assert counts.get((tenant, "rest", "throttled")) == 1


@pytest.mark.live
class TestOperatorCommands:
    def test_tenancy_onboard_list_status(self) -> None:
        _require()
        from click.testing import CliRunner

        from graph_memory.ops.cli import cli

        tenant = f"lt-cli-{uuid.uuid4().hex[:8]}"
        runner = CliRunner()
        onboard = runner.invoke(cli, ["tenancy", "onboard", tenant])
        assert onboard.exit_code == 0, onboard.output
        assert "active" in onboard.output
        listing = runner.invoke(cli, ["tenancy", "list", "--limit", "5"])
        assert listing.exit_code == 0
        assert "tenants:" in listing.output
        status = runner.invoke(cli, ["tenancy", "status", tenant])
        assert status.exit_code == 0
        assert tenant in status.output

    def test_loop_run_reports(self) -> None:
        _require()
        from click.testing import CliRunner

        from graph_memory.ops.cli import cli

        result = CliRunner().invoke(cli, ["loop", "run", "--mapping", str(_MAPPING)])
        assert result.exit_code == 0, result.output
        assert "loop[" in result.output

    def test_replay_plan_and_run(self) -> None:
        _require()
        from click.testing import CliRunner

        from graph_memory.ops.cli import cli

        plan = CliRunner().invoke(cli, ["replay"])
        assert plan.exit_code == 0
        assert "REPLAY PLAN" in plan.output
        executed = CliRunner().invoke(cli, ["replay", "run", "--mapping", str(_MAPPING)])
        assert executed.exit_code == 0, executed.output
        assert "replay[" in executed.output

    def test_spans_and_metering_read_views(self) -> None:
        _require()
        from click.testing import CliRunner

        from graph_memory.ops.cli import cli

        spans = CliRunner().invoke(cli, ["spans", "--limit", "3"])
        assert spans.exit_code == 0
        metered = CliRunner().invoke(cli, ["metering", "--limit", "3"])
        assert metered.exit_code == 0
