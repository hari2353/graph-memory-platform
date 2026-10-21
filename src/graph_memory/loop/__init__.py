"""Effectiveness loop package (SPEC §10)."""

from graph_memory.loop.continuous import ContinuousEffectivenessLoop
from graph_memory.loop.tick import (
    CellEffectiveness,
    EffectivenessReport,
    EffectivenessTick,
    apply_effectives,
)

__all__ = [
    "CellEffectiveness",
    "ContinuousEffectivenessLoop",
    "EffectivenessReport",
    "EffectivenessTick",
    "apply_effectives",
]
