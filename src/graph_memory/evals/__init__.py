"""Offline evals package (SPEC §11): hit-rate@k, counterfactual replay, lift,
and the trajectory gate."""

from graph_memory.evals.harness import (
    UserDelta,
    counterfactual_replay,
    hit_rate_at_k,
    lift,
    over_personalization_probe,
)
from graph_memory.evals.trajectory import (
    GoldenCheck,
    StepDelta,
    StepTrace,
    TrajectoryFixture,
    TrajectoryStep,
    TrajectoryTrace,
    check_golden,
    golden_payload,
    load_case,
    run_trajectory,
)

__all__ = [
    "GoldenCheck",
    "StepDelta",
    "StepTrace",
    "TrajectoryFixture",
    "TrajectoryStep",
    "TrajectoryTrace",
    "UserDelta",
    "check_golden",
    "counterfactual_replay",
    "golden_payload",
    "hit_rate_at_k",
    "lift",
    "load_case",
    "over_personalization_probe",
    "run_trajectory",
]
