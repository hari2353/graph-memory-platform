"""Event windowing (cross-pollinated from Uber/DoorDash streaming).

Tumbling and sliding windows over a stream of timestamped events. Pure Python,
deterministic, no stream processor dependency.
"""

from __future__ import annotations


def tumbling_windows(
    events: list[dict], window_size: float, *, time_key: str = "ts"
) -> list[list[dict]]:
    """Partition events into non-overlapping windows of ``window_size``."""
    if window_size <= 0:
        raise ValueError("window_size must be > 0")
    if not events:
        return []
    ordered = sorted(events, key=lambda e: e[time_key])
    windows: list[list[dict]] = []
    current: list[dict] = []
    window_start = ordered[0][time_key]
    for event in ordered:
        ts = event[time_key]
        if ts >= window_start + window_size:
            if current:
                windows.append(current)
            window_start = (ts // window_size) * window_size
            current = [event]
        else:
            current.append(event)
    if current:
        windows.append(current)
    return windows


def sliding_window_count(events: list[dict], window_size: float, *, time_key: str = "ts") -> int:
    """Max number of events in any sliding window of ``window_size``."""
    if window_size <= 0:
        raise ValueError("window_size must be > 0")
    if not events:
        return 0
    ordered = sorted(e[time_key] for e in events)
    best = 0
    left = 0
    for right in range(len(ordered)):
        while ordered[right] - ordered[left] >= window_size:
            left += 1
        best = max(best, right - left + 1)
    return best
