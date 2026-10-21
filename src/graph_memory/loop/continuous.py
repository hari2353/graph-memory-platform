"""The §10 loop as a run-anywhere pass: score -> swap rows -> convergent replay.

``rows()`` is the deterministic (kind, target, cohort) multiplier state derived
purely from ledger truth; ``pass_over(engine)`` swaps those rows into the
projector and replays — the fixed-point pass the composition root runs on its
interval and the operator runs via ``platform loop run``'s engine. Because the
rows are a pure function of (ledger, spec, clock), a fresh boot re-derives the
same state: the replay-convergence identity (§16.2) holds with the loop ON.
"""

from __future__ import annotations

from typing import final

from graph_memory.core.clock import Clock
from graph_memory.core.types import TenantId
from graph_memory.loop.tick import EffectivenessTick
from graph_memory.mapping.spec import MappingSpec
from graph_memory.projector.engine import ProjectionDigest, Projector
from graph_memory.signals.ledger import LedgerRepository


@final
class ContinuousEffectivenessLoop:
    """Ticks every tenant from ledger truth; carries the rows into a projector."""

    def __init__(self, spec: MappingSpec, ledger: LedgerRepository, clock: Clock) -> None:
        self._spec = spec
        self._ledger = ledger
        self._clock = clock

    @property
    def clock(self) -> Clock:
        return self._clock

    def rows(self) -> dict[tuple[str, str, str], float]:
        """Merged (kind, target, cohort) multipliers; first tenant wins ties."""
        overrides: dict[tuple[str, str, str], float] = {}
        for tenant in self._ledger.tenants():
            report = EffectivenessTick(self._spec, self._ledger, self._clock).score(tenant)
            for key, multiplier in report.weight_overrides.items():
                overrides.setdefault(key, multiplier)
        return overrides

    def pass_over(self, engine: Projector) -> dict[TenantId, ProjectionDigest]:
        """One deterministic pass: swap rows in, replay to the fixed point."""
        engine.set_overrides(self.rows())
        return engine.replay()


__all__ = ["ContinuousEffectivenessLoop"]
