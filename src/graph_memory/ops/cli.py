"""Ops CLI: ``platform replay ...`` + ``platform bench ...`` gates."""

from __future__ import annotations

import asyncio

import click

from graph_memory.settings import Settings
from graph_memory.signals.ledger import ClickHouseLedger, InMemoryLedger, LedgerRepository


@click.group()
def cli() -> None:
    """graph-memory-platform ops toolbelt."""


def _ledger(backend: str, settings: Settings) -> LedgerRepository:
    if backend == "memory":
        return InMemoryLedger()
    deployed = ClickHouseLedger(settings)
    deployed.ensure_schema()
    return deployed


@cli.command()
@click.option("--dry-run", is_flag=True, help="print the plan without executing (M0: plan only)")
@click.option("--tenant", "tenant_filter", default=None, help="plan only this tenant")
@click.option(
    "--backend",
    type=click.Choice(["clickhouse", "memory"]),
    default="clickhouse",
    show_default=True,
)
def replay(dry_run: bool, tenant_filter: str | None, backend: str) -> None:
    """Replay the ledger through the current mapping spec.

    M0 prints a read-only plan (per-tenant event counts by signal). Actual
    projection execution lands with the projector at M1.
    """
    if not dry_run:
        raise click.ClickException(
            "replay execution arrives with the projector (M1); use --dry-run"
        )
    ledger = _ledger(backend, Settings())
    tenants = ledger.tenants()
    if tenant_filter is not None:
        tenants = [t for t in tenants if str(t) == tenant_filter]
        if not tenants:
            raise click.ClickException(f"no such tenant in the ledger: {tenant_filter!r}")
    click.echo("REPLAY PLAN (dry-run) — projector execution lands at M1")
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


def main() -> None:
    """Console entry point (pyproject [project.scripts])."""
    cli()


@cli.command()
@click.option("--mapping", "mapping_path", required=True, help="mapping spec YAML path")
@click.option("--host", default="127.0.0.1", show_default=True)
@click.option("--ingest-port", default=8001, show_default=True)
@click.option("--rest-port", default=8002, show_default=True)
@click.option("--mcp-port", default=8003, show_default=True)
def serve(mapping_path: str, host: str, ingest_port: int, rest_port: int, mcp_port: int) -> None:
    """Boot ingest + REST + MCP against the live ClickHouse tier.

    Boot replays the ledger first (the M1 convergence proof, as warmup), so
    every channel serves the deterministic projection of everything the
    ledger holds. Dev posture: the static bearer tokens from Settings.
    """
    import threading
    import time
    from pathlib import Path

    import uvicorn

    from graph_memory.channels.mcp.server import build_mcp_server, mcp_streamable_app
    from graph_memory.channels.rest.api import create_rest_app
    from graph_memory.context.service import ContextCache, MemoryContextProvider
    from graph_memory.core.clock import SystemClock
    from graph_memory.mapping.spec import load_spec
    from graph_memory.projector.engine import Projector
    from graph_memory.projector.gateway_clickhouse import ClickHouseGateway
    from graph_memory.signals.api import create_app as create_ingest_app
    from graph_memory.signals.ledger import ClickHouseLedger
    from graph_memory.tenancy.guard import StaticBearerGuard

    settings = Settings()
    ledger = ClickHouseLedger(settings)
    ledger.ensure_schema()
    gateway = ClickHouseGateway(settings)
    gateway.ensure_graph_schema()
    spec = load_spec(Path(mapping_path).read_text(encoding="utf-8"))
    guard = StaticBearerGuard(settings.dev_static_tokens)
    engine = Projector(spec, ledger, gateway, SystemClock())
    digests = engine.replay()
    for tenant, digest in sorted(digests.items()):
        click.echo(
            f"replay[{tenant}]: events={digest.events} plans={digest.plans} "
            f"node_ops={digest.node_ops} edge_ops={digest.edge_ops}"
        )
    provider = MemoryContextProvider(engine, ContextCache())
    capabilities = tuple(sorted(spec.capabilities))
    apps = (
        (create_ingest_app(ledger, guard, settings=settings), ingest_port, "ingest"),
        (create_rest_app(provider, guard), rest_port, "rest"),
        (
            mcp_streamable_app(
                build_mcp_server(provider, guard, capabilities=capabilities, signal_schemas={})
            ),
            mcp_port,
            "mcp",
        ),
    )
    servers: list[tuple[uvicorn.Server, threading.Thread]] = []
    for app, port, _label in apps:
        server = uvicorn.Server(uvicorn.Config(app, host=host, port=port, log_level="warning"))
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        servers.append((server, thread))
    deadline = time.monotonic() + 15
    while any(not server.started for server, _ in servers):
        if time.monotonic() > deadline:
            raise SystemExit("uvicorn did not start within 15s")
        time.sleep(0.1)
    click.echo(f"serving: ingest :{ingest_port}  rest :{rest_port}  mcp :{mcp_port} (Ctrl+C stops)")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        for server, thread in servers:
            server.should_exit = True
            thread.join(timeout=5)
        click.echo("bye")


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
