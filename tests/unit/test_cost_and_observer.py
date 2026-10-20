"""Cost gate + observer port tests (SPEC §12/§16.6, §10 Loop 2, §11/R7)."""

from __future__ import annotations

from datetime import UTC, datetime

from click.testing import CliRunner

from graph_memory.core.clock import FixedClock
from graph_memory.core.types import TenantId
from graph_memory.loop import EffectivenessReport
from graph_memory.observability import (
    InMemorySpanSink,
    NoopObserver,
    ScriptedLlmObserver,
)
from graph_memory.ops.cli import bench
from graph_memory.ops.cost import measured_record_bytes, recomputed_report, within_budget


def test_synthetic_profile_measures_near_the_1kb_target() -> None:
    assert 900.0 <= measured_record_bytes(5) <= 1200.0


def test_cost_recompute_matches_the_spec12_reference() -> None:
    report = recomputed_report(350_000_000, 12)
    # §16.6: the recompute stays within ±15% of §12's 1.07 TB/mo.
    assert abs(report.monthly_compressed_tb - 1.07) <= 1.07 * 0.15
    assert abs(report.retention_compressed_tb - 12.8) <= 12.8 * 0.15
    assert within_budget(report)


def test_double_sized_records_flip_the_cost_gate_red() -> None:
    red = recomputed_report(350_000_000, 12, record_bytes=2400.0)
    assert not within_budget(red)


def test_cli_cost_gate_exit_codes() -> None:
    runner = CliRunner()
    green = runner.invoke(bench, ["cost"])
    assert green.exit_code == 0
    assert "TB/mo" in green.output
    red = runner.invoke(bench, ["cost", "--events-per-day", "700_000_000", "--months", "12"])
    assert red.exit_code == 1


def _report(rows: int = 1) -> EffectivenessReport:
    return EffectivenessReport(
        tenant=TenantId("acme"),
        reco_delivered=5,
        attached=3,
        unattached=0,
        cells=(),
        weight_overrides={("RECOMMENDED", "skill:rust", "a"): 1.2},
    )


def test_scripted_observer_answers_and_records_advisory_spans() -> None:
    sink = InMemorySpanSink()
    observer = ScriptedLlmObserver(
        lambda report: {"verdict": "good", "reason": "rate above baseline"},
        sink,
        FixedClock(datetime(2026, 6, 10, tzinfo=UTC)),
    )
    report = _report()
    answered = observer.review(report)
    assert answered["verdict"] == "good"
    assert len(sink) == 1
    span = next(iter(sink))
    assert span.name == "gmp.observer.review"
    assert span.attributes["gmp.cells_reviewed"] == 0
    assert span.attributes["gmp.weight_rows"] == 1
    assert span.attributes["gmp.observer.verdict"] == "good"


def test_noop_observer_is_silent() -> None:
    sink = InMemorySpanSink()
    noop = NoopObserver()
    assert noop.review(_report()) == {}
    assert len(sink) == 0
