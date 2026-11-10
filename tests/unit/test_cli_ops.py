"""Ops CLI surfaces (replay/tenancy/loop/spans/metering) wired to fakes.

The CLI resolves the right view over the ledger truth without a live
ClickHouse: these tests patch the adapter classes at their seams — the same
`client_factory` port the offline-tier tests inject through — and drive the
command tree with CliRunner. Deterministic, no I/O, no servers.
"""

from __future__ import annotations

import sys
import threading
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, cast

import pytest
from click.testing import CliRunner

import graph_memory.loop.tick as tick_mod
import graph_memory.observability.audit_ch as audit_ch_mod
import graph_memory.observability.metering as metering_mod
import graph_memory.projector.gateway_clickhouse as gateway_mod
import graph_memory.signals.brokers as broker_pkg
import graph_memory.signals.brokers.pulsar_cli as pulsar_cli_mod
import graph_memory.tenancy.registry_ch as registry_ch_mod
from graph_memory.core.types import IngestResult, TenantId
from graph_memory.ops.cli import (
    _config_port,
    _config_text,
    _IntervalFlushes,
    _projection_loop,
    cli,
)
from graph_memory.projector.gateway import InMemoryGraphGateway
from graph_memory.signals.envelope import Envelope
from graph_memory.signals.ledger import InMemoryLedger, record_from_envelope
from graph_memory.signals.service import BatchOutcome, RejectedSignal
from graph_memory.tenancy.registry import TenantClass, TenantRecord, TenantState

# `graph_memory.ops` re-exports the click GROUP under the module's name, so
# `from graph_memory.ops import cli` binds the group, not the module. The
# module object (the monkeypatch target) resolves via sys.modules instead.
cli_mod: ModuleType = sys.modules["graph_memory.ops.cli"]

_AT = datetime(2026, 6, 10, 12, 0, tzinfo=UTC)
_TENANT = TenantId("acme")
_SPEC = Path(__file__).parent.parent / "fixtures" / "mapping_skill_loop.yaml"


def _record(eid: str, *, at: datetime = _AT) -> Any:
    """LedgerRow twin of the hybrid-search helper: one seeded skill event."""
    envelope = Envelope.parse(
        {
            "specversion": "1.0",
            "id": eid,
            "source": "https://app.example.com/signals",
            "type": "itf.skill_assessed.v1",
            "time": at.isoformat(),
            "partitionkey": "user-1",
            "data": {"skill": "rust"},
        }
    )
    return record_from_envelope(_TENANT, envelope)


def _run(*args: str) -> Any:
    return CliRunner().invoke(cli, list(args))


def _record_for(tenant: TenantId, eid: str) -> Any:
    return record_from_envelope(tenant, cast("Any", _record(eid)).envelope)


def test_replay_plan_lists_tenants_and_signals(monkeypatch: pytest.MonkeyPatch) -> None:
    ledger = InMemoryLedger()
    ledger.append([_record("e1"), _record("e2"), _record_for(TenantId("other"), "e9")])
    monkeypatch.setattr(cli_mod, "InMemoryLedger", lambda: ledger)
    result = _run("replay", "--backend", "memory")
    assert result.exit_code == 0
    assert "REPLAY PLAN (read-only)" in result.output
    assert "tenant acme: 2 events" in result.output
    assert "tenant other: 1 events" in result.output
    assert "itf.skill_assessed.v1: 2" in result.output


def test_replay_tenant_filter_hits_and_misses(monkeypatch: pytest.MonkeyPatch) -> None:
    ledger = InMemoryLedger()
    ledger.append([_record("e1"), _record_for(TenantId("other"), "e9")])
    monkeypatch.setattr(cli_mod, "InMemoryLedger", lambda: ledger)
    hit = _run("replay", "--tenant", "acme", "--backend", "memory")
    assert hit.exit_code == 0
    assert "tenant acme" in hit.output
    assert "tenant other" not in hit.output
    miss = _run("replay", "--tenant", "ghost", "--backend", "memory")
    assert miss.exit_code == 1
    assert "no such tenant" in miss.output


def test_replay_run_demand_live_tier() -> None:
    result = _run("replay", "run", "--mapping", "x.yaml", "--backend", "memory")
    assert result.exit_code == 1
    assert "replay run executes against the live tier" in result.output


def _patch_drain(
    monkeypatch: pytest.MonkeyPatch,
    outcomes: dict[str, BatchOutcome],
    *,
    fail: str | None = None,
) -> None:
    """Stub the transport seams; the real PulsarIngester.drain() runs."""

    class _StubReceiver:
        def __init__(self, *_a: Any, **_k: Any) -> None:
            pass

        def receive(self) -> list[tuple[str, str]]:
            if fail is not None:
                raise RuntimeError(fail)
            return []

    class _StubIngestor:
        def __init__(self, *_a: Any, **_k: Any) -> None:
            pass

        def deliver(self, _messages: list[Any]) -> dict[str, BatchOutcome]:
            return outcomes

    monkeypatch.setattr(
        cli_mod,
        "ClickHouseLedger",
        lambda _s: SimpleNamespace(ensure_schema=lambda: None),
    )
    monkeypatch.setattr(broker_pkg, "BrokerIngestor", _StubIngestor)
    monkeypatch.setattr(pulsar_cli_mod, "PulsarCliReceiver", _StubReceiver)
    monkeypatch.setattr(pulsar_cli_mod, "docker_exec_runner", lambda _container: object())


def test_drain_brokers_reports_outcomes_with_rejections(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outcomes: dict[str, BatchOutcome] = {
        "acme": BatchOutcome(
            result=IngestResult(accepted=3, duplicates=1),
            rejected=(RejectedSignal(index=2, error={"error": "schema mismatch"}),),
        ),
    }
    _patch_drain(monkeypatch, outcomes)
    result = _run("drain-brokers")
    assert result.exit_code == 0
    assert "DRAIN (broker -> ledger)" in result.output
    assert "tenant acme: accepted=3 duplicates=1 rejected=1" in result.output
    assert "rejected item 2: schema mismatch" in result.output


def test_drain_brokers_reports_an_empty_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_drain(monkeypatch, {})
    result = _run("drain-brokers")
    assert result.exit_code == 0
    assert "(no gmp.signals.* topics)" in result.output


def test_drain_brokers_translates_transport_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_drain(monkeypatch, {}, fail="pulsar down")
    result = _run("drain-brokers")
    assert result.exit_code == 1
    assert "pulsar drain failed: pulsar down" in result.output


def test_replay_plan_on_an_empty_ledger_is_honest(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_mod, "InMemoryLedger", lambda: InMemoryLedger())
    result = _run("replay", "--backend", "memory")
    assert result.exit_code == 0
    assert "REPLAY PLAN (read-only)" in result.output
    assert "(ledger empty)" in result.output


class _GraphGatewayForCli(InMemoryGraphGateway):
    """The in-memory twin plus the schema no-op the CLI calls pre-replay."""

    def ensure_graph_schema(self) -> None:
        return None


def test_replay_run_executes_a_fresh_replay_with_real_parts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Real spec fixture, real projector, real graph gateway — only the CH
    # ledger is swapped for the deterministic in-memory twin.
    ledger = InMemoryLedger()
    ledger.append([_record("e1"), _record("e2")])
    monkeypatch.setattr(cli_mod, "ClickHouseLedger", lambda _s: ledger)
    monkeypatch.setattr(gateway_mod, "ClickHouseGateway", lambda _s: _GraphGatewayForCli())
    result = _run("replay", "run", "--mapping", str(_SPEC))
    assert result.exit_code == 0
    assert "replay[acme]: events=2 plans=" in result.output


def test_replay_run_resume_from_after_every_event_is_a_noop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger = InMemoryLedger()
    ledger.append([_record("e1")])
    monkeypatch.setattr(cli_mod, "ClickHouseLedger", lambda _s: ledger)
    monkeypatch.setattr(gateway_mod, "ClickHouseGateway", lambda _s: _GraphGatewayForCli())
    result = _run("replay", "run", "--mapping", str(_SPEC), "--from", "2027-01-01T00:00:00+00:00")
    assert result.exit_code == 0
    assert "replay: nothing to apply" in result.output


class _FakeRegistry:
    """Same face as ClickHouseTenantRegistry, zero I/O."""

    def __init__(
        self,
        records: list[TenantRecord] | None = None,
        *_args: Any,
        **_kwargs: Any,
    ) -> None:
        self._records = list(records) if records else []

    def ensure_registry_schema(self) -> None:
        return None

    def onboard(
        self,
        tenant: str | TenantId,
        *,
        tenant_class: TenantClass = TenantClass.STANDARD,
    ) -> TenantRecord:
        record = TenantRecord(
            TenantId(str(tenant)),
            tenant_class,
            TenantState.ACTIVE,
            _AT,
            updated_at=_AT,
        )
        self._records.append(record)
        return record

    def tenants(self) -> list[TenantRecord]:
        return list(self._records)

    def get(self, tenant: str | TenantId) -> TenantRecord:
        return self._records[0]


def test_tenancy_onboard_then_reads_the_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(registry_ch_mod, "ClickHouseTenantRegistry", _FakeRegistry)
    onboard = _run("tenancy", "onboard", "acme")
    assert onboard.exit_code == 0
    assert "tenant acme" in onboard.output
    assert "class=standard" in onboard.output


def test_tenancy_list_prints_rows_and_honors_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed = [
        TenantRecord(
            TenantId("acme"), TenantClass.STANDARD, TenantState.ACTIVE, _AT, updated_at=_AT
        ),
        TenantRecord(
            TenantId("other"), TenantClass.STANDARD, TenantState.REVOKED, _AT, updated_at=_AT
        ),
    ]
    monkeypatch.setattr(
        registry_ch_mod, "ClickHouseTenantRegistry", lambda *_a, **_k: _FakeRegistry(list(seed))
    )
    result = _run("tenancy", "list", "--limit", "1")
    assert result.exit_code == 0
    assert "tenants: 2" in result.output
    assert "acme" in result.output
    assert "other" not in result.output  # limit cuts the rows


def test_tenancy_status_reads_one_record(monkeypatch: pytest.MonkeyPatch) -> None:
    seed = [
        TenantRecord(
            TenantId("acme"), TenantClass.STANDARD, TenantState.ACTIVE, _AT, updated_at=_AT
        )
    ]
    monkeypatch.setattr(
        registry_ch_mod, "ClickHouseTenantRegistry", lambda *_a, **_k: _FakeRegistry(list(seed))
    )
    result = _run("tenancy", "status", "acme")
    assert result.exit_code == 0
    assert "state=active" in result.output


def test_tenancy_status_on_an_empty_registry_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The unlucky path: the real registry raises TenantNotFound; the CLI does
    # not catch it, so the command exits non-zero with a traceback.
    monkeypatch.setattr(registry_ch_mod, "ClickHouseTenantRegistry", _FakeRegistry)
    result = _run("tenancy", "status", "ghost")
    assert result.exit_code == 1


def test_loop_run_scores_cells_when_reports_exist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cell = SimpleNamespace(
        cohort="android",
        reco_class="skill",
        reco_signal="itf.reco_shown.v1",
        attached=True,
        positive=2,
        rate=0.5,
        multiplier=1.4,
    )
    reports: dict[str, Any] = {
        "acme": SimpleNamespace(
            reco_delivered=3,
            attached=2,
            unattached=1,
            cells=[cell],
            weight_overrides=[cell],
        )
    }
    monkeypatch.setattr(
        gateway_mod,
        "ClickHouseGateway",
        lambda _s: SimpleNamespace(
            ensure_graph_schema=lambda: None,
        ),
    )
    monkeypatch.setattr(tick_mod, "apply_effectives", lambda *_a, **_k: reports)
    monkeypatch.setattr(
        cli_mod,
        "ClickHouseLedger",
        lambda _s: SimpleNamespace(
            ensure_schema=lambda: None,
        ),
    )
    result = _run("loop", "run", "--mapping", str(_SPEC))
    assert result.exit_code == 0
    assert "loop[acme]: recos=3 attached=2" in result.output
    assert "cohort='android'" in result.output
    assert "multiplier=1.4" in result.output


def test_loop_run_with_no_reports_is_a_clean_noop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        gateway_mod,
        "ClickHouseGateway",
        lambda _s: SimpleNamespace(
            ensure_graph_schema=lambda: None,
        ),
    )
    monkeypatch.setattr(tick_mod, "apply_effectives", lambda *_a, **_k: {})
    monkeypatch.setattr(
        cli_mod,
        "ClickHouseLedger",
        lambda _s: SimpleNamespace(
            ensure_schema=lambda: None,
        ),
    )
    result = _run("loop", "run", "--mapping", str(_SPEC))
    assert result.exit_code == 0
    assert "loop: ledger empty" in result.output


class _FakeSpanSink:
    """One batch per CLI read: the second command sees an empty table."""

    def __init__(self, batches: list[list[tuple[object, ...]]]) -> None:
        self._batches = batches

    def ensure_schema(self) -> None:
        return None

    def flush(self) -> int:
        return 0

    def recent(self, tenant: str | None = None, limit: int = 20) -> list[tuple[object, ...]]:
        del tenant, limit
        return self._batches.pop(0) if self._batches else []


def test_spans_read_and_the_empty_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    sink = _FakeSpanSink([[("acme", "gmp.channel.rest.request", _AT, 1.5, "{}")]])
    monkeypatch.setattr(audit_ch_mod, "ClickHouseSpanSink", lambda *_a, **_k: sink)
    read = _run("spans")
    assert read.exit_code == 0
    assert "gmp.channel.rest.request" in read.output
    assert "1.500ms" in read.output
    empty = _run("spans")
    assert empty.exit_code == 0
    assert "spans: table empty" in empty.output


def test_spans_tenant_filter_reaches_the_surface(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[dict[str, Any]] = []

    class _RecordingSink(_FakeSpanSink):
        def recent(self, tenant: str | None = None, limit: int = 20) -> list[tuple[object, ...]]:
            seen.append({"tenant": tenant, "limit": limit})
            return []

    recording = _RecordingSink([])  # one instance: history accumulates across calls
    monkeypatch.setattr(audit_ch_mod, "ClickHouseSpanSink", lambda *_a, **_k: recording)
    result = _run("spans", "--tenant", "acme", "--limit", "5")
    assert "spans: table empty" in result.output
    assert seen == [{"tenant": "acme", "limit": 5}]


def test_metering_read_and_the_empty_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    batches: list[list[tuple[object, ...]]] = [[("acme", "rest", "requests", 3, _AT)]]

    class _FakeMetering:
        def __init__(self, *_a: Any, **_k: Any) -> None:
            pass

        def ensure_schema(self) -> None:
            return None

        def recent(self, tenant: Any = None, limit: int = 20) -> list[tuple[object, ...]]:
            del tenant, limit
            return batches.pop(0) if batches else []

    monkeypatch.setattr(metering_mod, "MeteringTable", _FakeMetering)
    read = _run("metering")
    assert read.exit_code == 0
    assert "acme rest requests=3" in read.output
    empty = _run("metering")
    assert "metering: table empty" in empty.output


def test_flush_companion_drains_then_appends() -> None:
    drained: list[Any] = []
    appended: list[Any] = []
    sink = SimpleNamespace(flush=lambda: drained.append(1))
    counters = SimpleNamespace(drain=lambda: {"acme": 2})
    table = SimpleNamespace(append=lambda rows: appended.append(rows))
    flushes = _IntervalFlushes(cast("Any", sink), cast("Any", counters), cast("Any", table))
    flushes.flush()
    assert drained == [1]
    assert appended == [{"acme": 2}]


def test_flush_companion_survives_a_sink_failure() -> None:
    sink = SimpleNamespace(flush=lambda: (_ for _ in ()).throw(RuntimeError("ch down")))
    counters = SimpleNamespace(drain=lambda: {})
    table = SimpleNamespace(append=lambda rows: None)
    flushes = _IntervalFlushes(cast("Any", sink), cast("Any", counters), cast("Any", table))
    flushes.flush()  # must not raise — flush failure never kills serve


def test_projection_loop_replays_each_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cli_mod, "PROJECTION_INTERVAL_S", 0.01)
    replayed: list[int] = []
    cleared: list[int] = []
    engine = SimpleNamespace(replay=lambda: replayed.append(1))
    cache = SimpleNamespace(clear=lambda: cleared.append(1))
    stop = threading.Event()
    thread = threading.Thread(target=_projection_loop, args=(engine, cache, stop), daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 5
        while not replayed and time.monotonic() < deadline:
            time.sleep(0.005)
        assert replayed, "projection pass should run inside the first interval"
    finally:
        stop.set()
        thread.join(timeout=3)
    assert cleared, "cache must clear after every pass"


def test_projection_loop_survives_engine_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cli_mod, "PROJECTION_INTERVAL_S", 0.01)
    attempts: list[int] = []
    ended: list[int] = []

    def _boom() -> None:
        attempts.append(1)
        raise RuntimeError("tick down")

    cache = SimpleNamespace(clear=lambda: ended.append(1))
    stop = threading.Event()
    thread = threading.Thread(
        target=_projection_loop,
        args=(SimpleNamespace(replay=_boom), cache, stop),
        daemon=True,
    )
    thread.start()
    try:
        deadline = time.monotonic() + 5
        while not attempts and time.monotonic() < deadline:
            time.sleep(0.005)
        assert attempts, "a failed pass must still be attempted"
    finally:
        stop.set()
        thread.join(timeout=3)
    assert ended, "the finally block still clears the cache"


def test_projection_loop_drives_the_continuous_pass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cli_mod, "PROJECTION_INTERVAL_S", 0.01)
    passes: list[int] = []
    continuous = SimpleNamespace(pass_over=lambda _projector: passes.append(1))
    stop = threading.Event()
    thread = threading.Thread(
        target=_projection_loop,
        args=(SimpleNamespace(), SimpleNamespace(), stop),
        kwargs={"continuous": cast("Any", continuous)},
        daemon=True,
    )
    thread.start()
    try:
        deadline = time.monotonic() + 5
        while not passes and time.monotonic() < deadline:
            time.sleep(0.005)
        assert passes, "the loop pass over must reach the projector"
    finally:
        stop.set()
        thread.join(timeout=3)


def test_config_values_narrow_the_honest_way() -> None:
    config: Mapping[str, object] = {"host": "0.0.0.0", "port": 80, "bad": True}
    assert _config_text(config, "host", "127.0.0.1") == "0.0.0.0"
    assert _config_text(config, "missing", "127.0.0.1") == "127.0.0.1"
    assert _config_text(config, "port", "x") == "x"  # non-str value falls back
    assert _config_port(config, "port", 1) == 80
    assert _config_port(config, "missing", 1) == 1
    assert _config_port(config, "bad", 1) == 1  # bool is not an int port
