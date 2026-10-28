"""The search lexicon: ONE deterministic token + lexical-metric definition.

Hybrid retrieval (M4.2, research 06 lever 2) is only comparative across
backends if "what matched" is one shared function — so tokenization and the
lexical hit metric live in core, and both gateways (in-memory and
ClickHouse) are contractually required to compute the same number:

- A query is a bounded tuple of tokens: casefolded, alphanumeric runs, each
  ≥ 2 chars, first-seen order, at most :data:`MAX_QUERY_TOKENS` tokens.
- A token HITS a haystack when it appears as a case-insensitive substring
  of any field. LLa-style token scoring is deliberately out of scope: the
  metric must stay a pure function of (fields, tokens) — no language models
  in the kernel (market gate, SPEC §3).
"""

from __future__ import annotations

import re
from collections.abc import Sequence

MAX_QUERY_TOKENS = 8
MIN_TOKEN_CHARS = 2

_TOKEN = re.compile(f"[0-9a-z]{{{MIN_TOKEN_CHARS},}}")


def query_tokens(query: str) -> tuple[str, ...]:
    """Bounded, deterministic token tuple for one search query."""
    seen: dict[str, None] = {}
    for raw in query.casefold().split():
        for token in _TOKEN.findall(raw):
            seen.setdefault(token, None)
            if len(seen) >= MAX_QUERY_TOKENS:
                return tuple(seen)
    return tuple(seen)


def lexical_hits(fields: Sequence[str], tokens: Sequence[str]) -> int:
    """Count tokens found (case-insensitive substring) in any field."""
    if not tokens:
        return 0
    haystack = "\x1f".join(fields).casefold()
    return sum(1 for token in tokens if token in haystack)
