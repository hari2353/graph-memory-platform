"""Ops CLI: ``platform replay ...`` — the ledger replay toolbelt."""

from __future__ import annotations

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
