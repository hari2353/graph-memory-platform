"""Change data capture cursor (cross-pollinated from Shopify 'Capturing Every Change').

A CDC cursor tracks how far a consumer has read a change stream. Pure Python,
deterministic, no DB dependency.
"""

from __future__ import annotations


class CdcCursor:
    def __init__(self, stream: str, position: int = 0) -> None:
        if not stream.strip():
            raise ValueError("stream must be non-empty")
        if position < 0:
            raise ValueError("position must be >= 0")
        self.stream = stream
        self.position = position

    def advance(self, n: int = 1) -> int:
        if n < 0:
            raise ValueError("n must be >= 0")
        self.position += n
        return self.position

    def reset(self, position: int = 0) -> None:
        if position < 0:
            raise ValueError("position must be >= 0")
        self.position = position

    def to_dict(self) -> dict[str, object]:
        return {"stream": self.stream, "position": self.position}


def changed_since(
    records: list[dict], last_position: int, *, key: str = "seq"
) -> tuple[list[dict], int]:
    """Return records with position > last_position and the new watermark."""
    if last_position < 0:
        raise ValueError("last_position must be >= 0")
    new = [r for r in records if int(r.get(key, 0)) > last_position]
    watermark = max((int(r.get(key, 0)) for r in new), default=last_position)
    return new, watermark
