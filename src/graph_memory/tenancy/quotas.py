"""Per-tenant quotas: two token-bucket lanes, batch separated from
interactive (SPEC §8, [03] §capacity). A whale's burst can never starve a
co-tenant because every bucket is keyed (tenant, lane) — no shared masses.
"""

from __future__ import annotations

from dataclasses import dataclass

from graph_memory.core.clock import Clock
from graph_memory.core.errors import QuotaExceeded
from graph_memory.core.types import Principal, Scope

_LANES: frozenset[Scope] = frozenset(Scope)


def _lane_for(scope: Scope) -> str:
    """SIGNALS_WRITE is the batch lane; every other pack is interactive."""
    return "batch" if scope == Scope.SIGNALS_WRITE else "interactive"


@dataclass(frozen=True)
class Quotas:
    """Defaults sized for the reference tenant; per-class overrides later."""

    interactive_rps: float = 20.0
    interactive_burst: float = 40.0
    batch_rps: float = 200.0
    batch_burst: float = 5_000.0


class _TokenBucket:
    def __init__(self, rate_per_s: float, burst: float, clock: Clock) -> None:
        self._rate = rate_per_s
        self._burst = burst
        self._clock = clock
        self._tokens = burst
        self._last = clock.now()

    def try_take(self, amount: int = 1) -> bool:
        now = self._clock.now()
        elapsed = max(0.0, (now - self._last).total_seconds())
        self._last = now
        self._tokens = min(self._burst, self._tokens + elapsed * self._rate)
        if self._tokens < amount:
            return False
        self._tokens -= float(amount)
        return True


class QuotaGuard:
    """Admission per (tenant, lane); a quota hit is a 429, never a stall."""

    def __init__(self, quota: Quotas, clock: Clock) -> None:
        self._quota = quota
        self._clock = clock
        self._buckets: dict[tuple[str, str], _TokenBucket] = {}

    def _bucket(self, tenant: str, lane: str) -> _TokenBucket:
        key = (tenant, lane)
        bucket = self._buckets.get(key)
        if bucket is None:
            bucket = (
                _TokenBucket(self._quota.batch_rps, self._quota.batch_burst, self._clock)
                if lane == "batch"
                else _TokenBucket(
                    self._quota.interactive_rps, self._quota.interactive_burst, self._clock
                )
            )
            self._buckets[key] = bucket
        return bucket

    def admit(self, principal: Principal, needed: Scope, *, amount: int = 1) -> None:
        """Raise ``QuotaExceeded`` when the tenant's lane bucket is dry."""
        lane = _lane_for(needed)
        if not self._bucket(principal.tenant, lane).try_take(amount):
            raise QuotaExceeded(
                f"tenant {principal.tenant} exceeded its {lane} quota",
                details={"lane": lane},
            )
