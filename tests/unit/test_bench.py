"""Bench gate tests: budget parsing, report math, CLI exit-code gate."""

from __future__ import annotations

from click.testing import CliRunner

from graph_memory.ops.bench import parse_budget, run_hotpath_bench
from graph_memory.ops.cli import bench


def test_parse_budget_units() -> None:
    assert parse_budget("1s") == 1000.0
    assert parse_budget("500ms") == 500.0
    assert parse_budget("0.75") == 750.0
    assert parse_budget(" 2S ".strip().casefold()) == 2000.0


async def test_hotpath_bench_reports_and_passes_the_default_gate() -> None:
    report = await run_hotpath_bench(users=6, requests=40, concurrency=2)
    assert report.requests == 80
    assert report.users == 6
    assert 0.0 <= report.p50_ms <= report.p95_ms <= report.p99_ms
    assert report.passed  # in-memory hot path is far under 1s


async def test_tiny_budget_flips_the_gate_red() -> None:
    report = await run_hotpath_bench(users=4, requests=10, concurrency=1, budget_ms=0.00001)
    assert not report.passed
    assert "FAILED" in report.render()


def test_cli_gate_exit_codes() -> None:
    runner = CliRunner()
    green = runner.invoke(
        bench, ["hotpath", "--requests", "20", "--users", "4", "--p99-budget", "10s"]
    )
    assert green.exit_code == 0
    assert "PASSED" in green.output
