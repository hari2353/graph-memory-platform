"""The documented token estimate: ~4 characters per token (no tokenizer).

Cheap and order-stable, so the same number bounds the projector's snapshot
payload and the bench gate. The honest unit is "estimated tokens", never a
model's true count.
"""

from __future__ import annotations


def estimate_tokens(text: str) -> int:
    return max(1, (len(text) + 3) // 4)
