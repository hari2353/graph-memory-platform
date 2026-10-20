"""Log-safe scrubbing: one line, bounded, control-character-free.

This is the platform kernel's port of ClickGraph's ``sanitize_query``
discipline: anything operator- or user-controlled that may reach logs or HTTP
payloads passes through here first, so a crafted value can never forge a log
line or break a structured payload (SPEC §17).
"""

from __future__ import annotations

# C0 controls (incl. \n, \r, \t), DEL, and C1 controls — the characters that
# let a value fake log structure or screen behavior.
_CONTROLS: frozenset[int] = frozenset([*range(0x00, 0x20), 0x7F, *range(0x80, 0xA0)])
_ELLIPSIS = "..."


def log_safe(value: str, *, max_len: int = 128) -> str:
    """Flatten ``value`` into a bounded, single-line, control-char-free string.

    Control characters are replaced with ``?`` (structure destroyed, content
    preserved); over-long values are truncated with a trailing ``...``.
    """
    if max_len < len(_ELLIPSIS):
        raise ValueError(f"max_len must be >= {len(_ELLIPSIS)} to fit the ellipsis")
    flat = "".join("?" if ord(ch) in _CONTROLS else ch for ch in value)
    if len(flat) <= max_len:
        return flat
    cut = max_len - len(_ELLIPSIS)
    return flat[:cut] + _ELLIPSIS
