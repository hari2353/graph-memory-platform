"""Idempotent event consumer (cross-pollinated from Spotify/DoorDash event delivery).

At-least-once delivery means duplicates; this consumer deduplicates by event
id so handlers run once per unique event. Pure Python.
"""

from __future__ import annotations


class IdempotentConsumer:
    def __init__(self) -> None:
        self._seen: set[str] = set()
        self._processed: list[dict] = []

    def handle(self, event: dict, handler) -> bool:
        """Return True if the event was processed, False if duplicate."""
        eid = str(event.get("id", ""))
        if not eid:
            raise ValueError("event must have an id")
        if eid in self._seen:
            return False
        self._seen.add(eid)
        handler(event)
        self._processed.append(event)
        return True

    def processed_count(self) -> int:
        return len(self._processed)

    def seen_ids(self) -> set[str]:
        return set(self._seen)
