"""Ops CLI: ``platform replay|serve`` + ``platform bench`` gates."""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast, final

import click

from graph_memory.core.clock import SystemClock
from graph_memory.loop.continuous import ContinuousEffectivenessLoop
from graph_memory.observability.audit_ch import ClickHouseSpanSink
from graph_memory.observability.metering import MeteringCounters, MeteringTable
from graph_memory.projector.engine import Projector
from graph_memory.settings import Settings
from graph_memory.signals.ledger import ClickHouseLedger, InMemoryLedger, LedgerRepository

PROJECTION_INTERVAL_S = 1.0  # serve's convergent-replay cadence


@click.group()
def cli() -> None:
    """graph-memory-platform ops toolbelt."""


def _ledger(backend: str, settings: Settings) -> LedgerRepository:
    if backend == "memory":
        return InMemoryLedger()
    deployed = ClickHouseLedger(settings)
    deployed.ensure_schema()
    return deployed


@cli.group(invoke_without_command=True)
@click.option("--dry-run", is_flag=True, help="print the plan without executing")
@click.option("--tenant", "tenant_filter", default=None, help="plan only this tenant")
@click.option(
    "--backend",
    type=click.Choice(["clickhouse", "memory"]),
    default="clickhouse",
    show_default=True,
)
@click.pass_context
def replay(ctx: click.Context, dry_run: bool, tenant_filter: str | None, backend: str) -> None:
    """Ledger plan preview: per-tenant event counts by signal (read-only).

    Bare ``replay`` prints the plan (so ``replay --dry-run`` keeps its §13
    meaning); ``replay run`` executes the real projection (full or ``--from``
    resume). Serve boots and its projection loop replay continuously too —
    channels never project; the composition root does.
    """
    if ctx.invoked_subcommand is not None:
        return
    if not dry_run:
        click.echo("read-only plan; execute with: platform replay run")
    ledger = _ledger(backend, Settings())
    tenants = ledger.tenants()
    if tenant_filter is not None:
        tenants = [t for t in tenants if str(t) == tenant_filter]
        if not tenants:
            raise click.ClickException(f"no such tenant in the ledger: {tenant_filter!r}")
    click.echo("REPLAY PLAN (read-only)")
    if not tenants:
        click.echo("  (ledger empty)")
    for tenant in tenants:
        events = ledger.read_events(tenant)
        by_signal: dict[str, int] = {}
        for event in events:
            key = str(event.signal)
            by_signal[key] = by_signal.get(key, 0) + 1
        click.echo(f"  tenant {tenant}: {len(events)} events")
        for signal, count in sorted(by_signal.items()):
            click.echo(f"    {signal}: {count}")


@replay.command("run")
@click.option("--mapping", "mapping_path", required=True, help="mapping spec YAML path")
@click.option(
    "--from",
    "from_iso",
    default=None,
    help="resume point: only events with occurred_at >= FROM are re-applied (ISO datetime)",
)
@click.option(
    "--backend",
    type=click.Choice(["clickhouse", "memory"]),
    default="clickhouse",
    show_default=True,
)
def replay_run(mapping_path: str, from_iso: str | None, backend: str) -> None:
    """Execute a full replay of the ledger through the mapping (fresh state).

    Without ``--from`` this is the convergent full replay (fresh-projection
    parity). With ``--from`` it re-applies only the events that occurred at
    or after the resume point — an additive resume, older rows stay as-is.
    """
    from datetime import datetime
    from pathlib import Path

    from graph_memory.mapping.spec import load_spec
    from graph_memory.projector.engine import Projector
    from graph_memory.projector.gateway_clickhouse import ClickHouseGateway

    if backend != "clickhouse":
        raise click.ClickException("replay run executes against the live tier (clickhouse)")
    settings = Settings()
    ledger = _ledger(backend, settings)
    gateway = ClickHouseGateway(settings)
    gateway.ensure_graph_schema()
    spec = load_spec(Path(mapping_path).read_text(encoding="utf-8"))
    clock = SystemClock()
    # Same boot semantics as serve: the loop is part of the deterministic
    # path (stored rows carry the tick's weighted values).
    continuous = ContinuousEffectivenessLoop(spec, ledger, clock)
    engine = Projector(spec, ledger, gateway, clock, weight_overrides=continuous.rows())
    if from_iso is None:
        digests = engine.replay()
    else:
        floor = datetime.fromisoformat(from_iso.replace("Z", "+00:00"))
        digests = {}
        for tenant in ledger.tenants():
            events = [e for e in ledger.read_events(tenant) if e.occurred_at >= floor]
            if events:
                digests[tenant] = engine.process_events(events)
    if not digests:
        click.echo("replay: nothing to apply (ledger empty or --from after every event)")
    for tenant, digest in sorted(digests.items()):
        click.echo(
            f"replay[{tenant}]: events={digest.events} plans={digest.plans} "
            f"node_ops={digest.node_ops} edge_ops={digest.edge_ops}"
        )


@cli.group()
def tenancy() -> None:
    """Tenant metadata ops (SPEC §8): metadata rows only, no per-tenant DDL."""


@tenancy.command("onboard")
@click.argument("tenant")
@click.option(
    "--class",
    "tenant_class",
    default="standard",
    show_default=True,
    type=click.Choice(["standard"]),
)
def tenancy_onboard(tenant: str, tenant_class: str) -> None:
    """Onboard TENANT as one metadata row in the durable CH registry."""
    from graph_memory.tenancy.registry import TenantClass
    from graph_memory.tenancy.registry_ch import ClickHouseTenantRegistry

    registry = ClickHouseTenantRegistry(Settings())
    registry.ensure_registry_schema()
    record = registry.onboard(tenant, tenant_class=TenantClass(tenant_class))
    click.echo(
        f"tenant {record.tenant_id}: class={record.tenant_class.value} "
        f"state={record.state.value} onboarded_at={record.onboarded_at.isoformat()}"
    )


@tenancy.command("list")
@click.option("--limit", default=20, show_default=True)
def tenancy_list(limit: int) -> None:
    """Every tenant row (state included) — the 7k-tenants-is-rows proof."""
    from graph_memory.tenancy.registry_ch import ClickHouseTenantRegistry

    registry = ClickHouseTenantRegistry(Settings())
    records = registry.tenants()
    click.echo(f"tenants: {len(records)}")
    for record in records[:limit]:
        click.echo(
            f"  {record.tenant_id}: class={record.tenant_class.value} state={record.state.value}"
        )


@tenancy.command("status")
@click.argument("tenant")
def tenancy_status(tenant: str) -> None:
    """One tenant's lifecycle state (active/suspended/revoked/purged)."""
    from graph_memory.tenancy.registry_ch import ClickHouseTenantRegistry

    registry = ClickHouseTenantRegistry(Settings())
    record = registry.get(tenant)
    click.echo(
        f"tenant {record.tenant_id}: class={record.tenant_class.value} "
        f"state={record.state.value} onboarded_at={record.onboarded_at.isoformat()} "
        f"updated_at={record.updated_at.isoformat()}"
    )


@cli.group()
def loop() -> None:
    """Effectiveness-loop ops (SPEC §10): the scheduled reflection score."""


@loop.command("run")
@click.option("--mapping", "mapping_path", required=True, help="mapping spec YAML path")
@click.option(
    "--backend",
    type=click.Choice(["clickhouse"]),
    default="clickhouse",
    show_default=True,
)
def loop_run(mapping_path: str, backend: str) -> None:
    """Run the deterministic tick: score cells, write weight rows, replay.

    The operator artifact of ``apply_effectives``: score every tenant from
    ledger truth, merge (kind, target, cohort) multipliers, and replay the
    projection with the rows applied inside the pure compiler.
    """
    from pathlib import Path

    from graph_memory.loop.tick import apply_effectives
    from graph_memory.mapping.spec import load_spec
    from graph_memory.projector.gateway_clickhouse import ClickHouseGateway

    settings = Settings()
    ledger = _ledger(backend, settings)
    gateway = ClickHouseGateway(settings)
    gateway.ensure_graph_schema()
    spec = load_spec(Path(mapping_path).read_text(encoding="utf-8"))
    reports = apply_effectives(spec, ledger, gateway, SystemClock())
    if not reports:
        click.echo("loop: ledger empty — nothing to score")
        return
    for tenant, report in sorted(reports.items()):
        click.echo(
            f"loop[{tenant}]: recos={report.reco_delivered} attached={report.attached} "
            f"unattached={report.unattached} cells={len(report.cells)} "
            f"weight_rows={len(report.weight_overrides)}"
        )
        for cell in report.cells:
            click.echo(
                f"  cell cohort={cell.cohort!r} class={cell.reco_class!r} "
                f"signal={cell.reco_signal}: attached={cell.attached} "
                f"positive={cell.positive} rate={cell.rate:.3f} multiplier={cell.multiplier:.3f}"
            )


@cli.command()
@click.option("--tenant", "tenant_filter", default=None, help="filter by tenant")
@click.option("--limit", default=20, show_default=True)
def spans(tenant_filter: str | None, limit: int) -> None:
    """Read the span audit-stub table (SPEC §11: unsampled traffic lands)."""
    from graph_memory.observability.audit_ch import ClickHouseSpanSink

    sink = ClickHouseSpanSink(Settings())
    sink.ensure_schema()
    rows = sink.recent(tenant=tenant_filter, limit=limit)
    if not rows:
        click.echo("spans: table empty (serve writes rows once channels emit)")
    for row in rows:
        # Row columns arrive as opaque objects from the CH adapter; the
        # audit-stub's row shape is fixed by the DDL (str, str, dt, float, str).
        tenant_s, name, at, duration_ms, attrs = tuple(str(value) for value in row)
        click.echo(f"  [{at}] {tenant_s} {name} {float(duration_ms):.3f}ms {attrs}")


@cli.command()
@click.option("--tenant", "tenant_filter", default=None, help="filter by tenant")
@click.option("--limit", default=20, show_default=True)
def metering(tenant_filter: str | None, limit: int) -> None:
    """Read per-tenant request/quota/throttle metering rows (R7, day 1)."""
    from graph_memory.core.types import tenant_id
    from graph_memory.observability.metering import MeteringTable

    table = MeteringTable(Settings())
    table.ensure_schema()
    tenant = None if tenant_filter is None else tenant_id(tenant_filter)
    rows = table.recent(tenant=tenant, limit=limit)
    if not rows:
        click.echo("metering: table empty (serve flushes counters each interval)")
    for row_tenant, lane, event, count, at in rows:
        click.echo(f"  [{at}] {row_tenant} {lane} {event}={count}")


def _config_text(config: Mapping[str, object], key: str, default: str) -> str:
    """YAML values are untyped at rest; narrow the serve keys the honest way."""
    value = config.get(key, default)
    return value if isinstance(value, str) else default


def _config_port(config: Mapping[str, object], key: str, default: int) -> int:
    value = config.get(key, default)
    return value if isinstance(value, int) and not isinstance(value, bool) else default


def main() -> None:
    """Console entry point (pyproject [project.scripts])."""
    cli()


@cli.command()
@click.option("--config", "config_path", default=None, help="serve YAML: {mapping, host, ports}")
@click.option("--mapping", "mapping_path", default=None, help="mapping spec YAML path")
@click.option("--host", default=None)
@click.option("--ingest-port", "ingest_port", default=None, type=int)
@click.option("--rest-port", "rest_port", default=None, type=int)
@click.option("--mcp-port", "mcp_port", default=None, type=int)
def serve(
    config_path: str | None,
    mapping_path: str | None,
    host: str | None,
    ingest_port: int | None,
    rest_port: int | None,
    mcp_port: int | None,
) -> None:
    """Boot ingest + REST + MCP against the live ClickHouse tier.

    Boot replays the ledger first (the M1 convergence proof, as warmup), so
    every channel serves the deterministic projection of everything the
    ledger holds. Dev posture: the static bearer tokens from Settings.
    ``--config`` takes a YAML (SPEC §13's ``serve --config env/dev.yaml``):
    keys mapping/host/ingest_port/rest_port/mcp_port; CLI flags win.
    """
    import threading
    import time
    from pathlib import Path

    import uvicorn
    import yaml

    from graph_memory.channels.mcp.server import build_mcp_server, mcp_streamable_app
    from graph_memory.channels.rest.api import create_rest_app
    from graph_memory.context.explain import context_explainer, effectiveness_reporter
    from graph_memory.context.service import ContextCache, MemoryContextProvider
    from graph_memory.mapping.spec import load_spec
    from graph_memory.projector.engine import Projector
    from graph_memory.projector.gateway_clickhouse import ClickHouseGateway
    from graph_memory.signals.api import create_app as create_ingest_app
    from graph_memory.signals.ledger import ClickHouseLedger
    from graph_memory.tenancy.guard import StaticBearerGuard

    config: dict[str, object] = {}
    if config_path is not None:
        loaded = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            raise click.ClickException(f"config {config_path!r} must be a YAML mapping")
        config = loaded
    mapping_ref = mapping_path if mapping_path is not None else config.get("mapping")
    if not isinstance(mapping_ref, str):
        raise click.ClickException("serve needs --mapping PATH or --config with a mapping key")
    resolved_mapping = mapping_ref
    resolved_host = host if host is not None else _config_text(config, "host", "127.0.0.1")
    ingest = ingest_port or _config_port(config, "ingest_port", 8001)
    rest = rest_port or _config_port(config, "rest_port", 8002)
    mcp = mcp_port or _config_port(config, "mcp_port", 8003)
    settings = Settings()
    ledger = ClickHouseLedger(settings)
    ledger.ensure_schema()
    gateway = ClickHouseGateway(settings)
    gateway.ensure_graph_schema()
    resolved_spec = load_spec(Path(resolved_mapping).read_text(encoding="utf-8"))
    spare_clock = SystemClock()
    # The loop is part of the boot path: stored graph rows carry the tick's
    # weighted values, so a serve boot without it would REGRESS the loop.
    continuous = ContinuousEffectivenessLoop(resolved_spec, ledger, spare_clock)
    guard = StaticBearerGuard(settings.dev_static_tokens)
    engine = Projector(
        resolved_spec, ledger, gateway, continuous.clock, weight_overrides=continuous.rows()
    )
    digests = engine.replay()
    for tenant, digest in sorted(digests.items()):
        click.echo(
            f"replay[{tenant}]: events={digest.events} plans={digest.plans} "
            f"node_ops={digest.node_ops} edge_ops={digest.edge_ops}"
        )
    cache = ContextCache()
    provider = MemoryContextProvider(engine, cache)
    capabilities = tuple(sorted(resolved_spec.capabilities))
    # Observability (SPEC §11/R7): spans + metering are day-1 surfaces. The
    # audit stub table lands them durably; exporters attach to the same port.
    span_sink = ClickHouseSpanSink(settings)
    span_sink.ensure_schema()
    metering_table = MeteringTable(settings)
    metering_table.ensure_schema()
    metering_counters = MeteringCounters()
    clock = SystemClock()
    apps = (
        (create_ingest_app(ledger, guard, settings=settings), ingest, "ingest"),
        (
            create_rest_app(provider, guard, sink=span_sink, metering=metering_counters),
            rest,
            "rest",
        ),
        (
            mcp_streamable_app(
                build_mcp_server(
                    provider,
                    guard,
                    capabilities=capabilities,
                    signal_schemas={},
                    effectiveness=effectiveness_reporter(resolved_spec, ledger, clock),
                    explainer=context_explainer(engine),
                )
            ),
            mcp,
            "mcp",
        ),
    )
    servers: list[tuple[uvicorn.Server, threading.Thread]] = []
    for app, port, _label in apps:
        server = uvicorn.Server(
            uvicorn.Config(app, host=resolved_host, port=port, log_level="warning")
        )
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        servers.append((server, thread))
    deadline = time.monotonic() + 15
    while any(not server.started for server, _ in servers):
        if time.monotonic() > deadline:
            raise SystemExit("uvicorn did not start within 15s")
        time.sleep(0.1)
    # The pipeline stage: channels never project — the composition root does.
    # Re-run the convergent replay on an interval and drop stale cache
    # entries, so freshly ingested signals surface within one interval. The
    # same tick also flushes the span/metering buffers (evidence, not truth).
    projector_stop = threading.Event()
    flushes = _IntervalFlushes(span_sink, metering_counters, metering_table)
    projector = threading.Thread(
        target=_projection_loop,
        args=(engine, cache, projector_stop, continuous, flushes),
        daemon=True,
    )
    projector.start()
    click.echo(f"serving: ingest :{ingest}  rest :{rest}  mcp :{mcp} (Ctrl+C stops)")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        projector_stop.set()
        projector.join(timeout=5)
        for server, thread in servers:
            server.should_exit = True
            thread.join(timeout=5)
        click.echo("bye")


@final
@dataclass(frozen=True)
class _IntervalFlushes:
    """Serve-loop companions: drains span/metering buffers each interval."""

    span_sink: ClickHouseSpanSink
    counters: MeteringCounters
    metering_table: MeteringTable

    def flush(self) -> None:
        try:
            rows = self.counters.drain()
            self.span_sink.flush()
            self.metering_table.append(rows)
        except Exception as exc:  # noqa: BLE001 - flush failure never kills serve
            logging.getLogger("gmp.serve").warning("observability flush failed: %s", exc)


def _projection_loop(
    engine: object,
    cache: object,
    stop: threading.Event,
    continuous: ContinuousEffectivenessLoop | None = None,
    flushes: _IntervalFlushes | None = None,
) -> None:
    """Convergent replay every PROJECTION_INTERVAL_S; cache cleared after."""
    import logging

    from graph_memory.context.service import ContextCache

    logger = logging.getLogger("gmp.serve")
    projector = cast("Projector", engine)
    context_cache = cast("ContextCache", cache)
    while not stop.wait(PROJECTION_INTERVAL_S):
        # The pass is one deterministic function: tick the ledger, swap the
        # loop's rows in, replay. Rows are byte-convergent for the same
        # (ledger, spec, clock); a failed pass keeps serving the previous
        # graph and retries next interval.
        try:
            if continuous is not None:
                continuous.pass_over(projector)
            else:
                projector.replay()
        except Exception as exc:  # noqa: BLE001 - composition-root resilience
            logger.error("projection pass failed; serving previous graph: %s", exc)
        finally:
            context_cache.clear()
            if flushes is not None:
                flushes.flush()


@cli.group()
def bench() -> None:
    """Performance gates on the synthetic profile (SPEC §7/§12)."""


@bench.command("hotpath")
@click.option("--requests", default=1000, show_default=True, help="requests per lane")
@click.option("--users", default=30, show_default=True)
@click.option("--p99-budget", default="1s", show_default=True, help="e.g. 1s, 500ms")
def hotpath(requests: int, users: int, p99_budget: str) -> None:
    """Hot-path SLA gate: context get() p99 vs the budget (exit 1 on miss)."""
    from graph_memory.ops.bench import parse_budget, run_hotpath_bench

    report = asyncio.run(
        run_hotpath_bench(users=users, requests=requests, budget_ms=parse_budget(p99_budget))
    )
    click.echo(report.render())
    if not report.passed:
        raise SystemExit(1)


@bench.command("cost")
@click.option("--events-per-day", default=350_000_000, show_default=True)
@click.option("--months", default=12, show_default=True, help="retention window")
@click.option("--codec-ratio", default=10.0, show_default=True, help="ZSTD-3 columnar")
def cost(events_per_day: int, months: int, codec_ratio: float) -> None:
    """Recompute the §12 storage estimate from the synthetic fixture (exit 1 on drift)."""
    from graph_memory.ops.cost import recomputed_report, within_budget

    report = recomputed_report(events_per_day, months, codec_ratio=codec_ratio)
    click.echo(report.render())
    if not within_budget(report):
        raise SystemExit(1)


if __name__ == "__main__":
    main()  # python -m graph_memory.ops.cli serve --mapping config/mapping.yaml
