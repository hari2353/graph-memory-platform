"""Trajectory eval gate: step grading, golden digest, PR-blocking exit code.

The gate replays one case against a deterministic projector, records the
served targets at each step, and compares both the per-step targets and a
canonical digest. A projection/ranking change fails with the first divergent
step, so the PR cannot silently change what an agent remembers.
"""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from graph_memory.core.clock import FixedClock
from graph_memory.evals.trajectory import (
    TrajectoryTrace,
    check_golden,
    golden_payload,
    load_case,
    run_trajectory,
)
from graph_memory.ops.cli import cli

_MAPPING = """\
mapping_version: 1
signals:
  itf.skill_assessed.v1:
    nodes:
      - key: "skill:{data.skill}"
        labels: [skill]
    edges:
      - kind: HAS_SKILL
        target: "skill:{data.skill}"
        weight: 1.0
        half_life_days: 180
capabilities:
  skills:
    relations: [HAS_SKILL]
    top_k: 5
    ttl_ms: 60000
  community:
    relations: [MEMBER_OF, COMMUNITY_HAS_SKILL]
    top_k: 10
    ttl_ms: 60000
"""


def _event(event_id: str, user: str, skill: str, at: str) -> dict[str, object]:
    return {
        "specversion": "1.0",
        "id": event_id,
        "source": "https://app.example.com/signals",
        "type": "itf.skill_assessed.v1",
        "time": at,
        "partitionkey": user,
        "data": {"skill": skill},
    }


def _write_case(tmp_path: Path) -> Path:
    (tmp_path / "mapping.yaml").write_text(_MAPPING, encoding="utf-8")
    case = {
        "name": "skills-walk",
        "mapping": "mapping.yaml",
        "tenant": "acme",
        "user": "user-1",
        "now": "2026-06-10T00:00:00Z",
        "events": [
            _event("e1", "user-1", "rust", "2026-06-01T12:00:00Z"),
            _event("e2", "user-1", "python", "2026-06-01T12:00:01Z"),
            _event("e3", "user-2", "rust", "2026-06-01T12:00:02Z"),
            _event("e4", "user-2", "python", "2026-06-01T12:00:03Z"),
            _event("e5", "user-3", "go", "2026-06-01T12:00:04Z"),
        ],
        "steps": [
            {"capability": "skills", "k": 3},
            {"capability": "skills", "k": 3, "as_of": "2026-05-01T00:00:00Z"},
            {"capability": "community", "k": 10, "reflect": True},
        ],
    }
    path = tmp_path / "case.json"
    path.write_text(json.dumps(case), encoding="utf-8")
    return path


def _trace(case_path: Path) -> TrajectoryTrace:
    fixture = load_case(case_path)
    return run_trajectory(
        fixture.spec,
        fixture.ledger,
        FixedClock(fixture.now),
        case=fixture.case,
        tenant=fixture.tenant,
        user=fixture.user,
        steps=fixture.steps,
    )


def test_trajectory_grades_each_step_deterministically(tmp_path: Path) -> None:
    trace = _trace(_write_case(tmp_path))
    assert [step.targets for step in trace.steps][:2] == [
        ("skill:python", "skill:rust"),
        (),
    ]
    community = trace.steps[2].targets
    assert community[:2] == ("skill:python", "skill:rust")
    assert community[2].startswith("community:")
    assert trace.digest == _trace(tmp_path / "case.json").digest


def test_golden_catches_step_drift_and_digest_drift(tmp_path: Path) -> None:
    trace = _trace(_write_case(tmp_path))
    golden = golden_payload(trace)
    green = check_golden(trace, golden)
    assert green.passed and green.deltas == ()

    tampered = json.loads(json.dumps(golden))
    tampered["steps"][0]["targets"] = ["skill:go"]
    red = check_golden(trace, tampered)
    assert not red.passed
    assert red.deltas[0].diverged
    assert red.deltas[0].expected == ("skill:go",)
    assert red.deltas[0].actual == ("skill:python", "skill:rust")

    digest_only = json.loads(json.dumps(golden))
    digest_only["digest"] = "0" * 64
    assert not check_golden(trace, digest_only).passed


def test_cli_writes_then_enforces_the_golden(tmp_path: Path) -> None:
    case_path = _write_case(tmp_path)
    golden_path = tmp_path / "golden.json"
    runner = CliRunner()

    written = runner.invoke(
        cli,
        ["evals", "trajectory", "--case", str(case_path), "--write-golden", str(golden_path)],
    )
    assert written.exit_code == 0, written.output
    assert golden_path.exists()

    green = runner.invoke(
        cli,
        ["evals", "trajectory", "--case", str(case_path), "--golden", str(golden_path)],
    )
    assert green.exit_code == 0, green.output
    assert "PASSED" in green.output

    data = json.loads(case_path.read_text(encoding="utf-8"))
    data["events"][0]["data"]["skill"] = "go"  # user-1 no longer shares rust
    case_path.write_text(json.dumps(data), encoding="utf-8")
    red = runner.invoke(
        cli,
        ["evals", "trajectory", "--case", str(case_path), "--golden", str(golden_path)],
    )
    assert red.exit_code == 1
    assert "FAILED" in red.output
    assert "step 0" in red.output


def test_checked_in_trajectory_gate_is_green() -> None:
    fixture = Path(__file__).parent.parent / "fixtures" / "trajectory"
    result = CliRunner().invoke(
        cli,
        [
            "evals",
            "trajectory",
            "--case",
            str(fixture / "case.json"),
            "--golden",
            str(fixture / "golden.json"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "PASSED" in result.output
