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
