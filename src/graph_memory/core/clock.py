"""Time is an injected dependency, never a wall-clock read inside logic.

Every port that needs time takes a ``Clock`` (SPEC §3: projection is a pure
function of ``(ledger rows, spec version, clock)`` — that is what makes replay
convergence provable). Invariant shared by all implementations: ``now()``
returns a timezone-**aware UTC** instant.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Protocol


class Clock(Protocol):
    """Reads the current instant; adapters may swap implementations freely."""

    def now(self) -> datetime:
        """Return the current instant (timezone-aware UTC)."""
        ...


class SystemClock:
    """Wall clock — the only place in the kernel allowed to read real time."""

    def now(self) -> datetime:
        return datetime.now(tz=UTC)


class FixedClock:
    """Deterministic clock for tests and sim runs; starts frozen at ``start``.

    ``tick`` moves the instant (negative steps allowed for rewind-style
    determinism checks); ``set`` jumps it outright. It never consults the wall.
    """

    def __init__(self, start: datetime) -> None:
        if start.tzinfo is None or start.utcoffset() is None:
            raise ValueError("start must be timezone-aware")
        self._moment = start.astimezone(UTC)

    def now(self) -> datetime:
        return self._moment

    def tick(self, seconds: float) -> datetime:
        """Advance (or rewind) by ``seconds`` and return the new instant."""
        self._moment += timedelta(seconds=seconds)
        return self._moment

    def set(self, instant: datetime) -> datetime:
        """Jump the frozen instant outright (must be timezone-aware)."""
        if instant.tzinfo is None or instant.utcoffset() is None:
            raise ValueError("instant must be timezone-aware")
        self._moment = instant.astimezone(UTC)
        return self._moment
