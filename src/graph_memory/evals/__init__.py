"""Offline evals package (SPEC §11): hit-rate@k, counterfactual replay, lift."""

from graph_memory.evals.harness import (
    UserDelta,
    counterfactual_replay,
    hit_rate_at_k,
    lift,
    over_personalization_probe,
)

__all__ = [
    "UserDelta",
    "counterfactual_replay",
    "hit_rate_at_k",
    "lift",
    "over_personalization_probe",
]
