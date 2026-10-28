"""The hybrid read face: ONE ranked search behind every channel (M4.2).

Research 06 lever 2, the falsifiable form:

- **lexical** — token hits over one user's graph text (kind + target from
  the gateway; node labels/props via the same store). The metric is
  ``core.lexicon``'s, identical across the in-memory and ClickHouse
  backends — ClickHouse pushes the token matching into SQL, nothing else.
- **typed** — the mapping spec's relation typing: kinds the queried
  capability serves rank above any spec-declared edge kind, which rank
  above reflection-only kinds (``SIMILAR_TO`` is reflection-derived).
- **loop** — the decayed score at ONE injected instant, whose weight
  already embeds the effectiveness loop's per-cohort weight overrides
  (``compile_plan`` multiplies them in) — loop evidence as a ranking
  feature is the piece no competitor has.

Ordering is lexicographic on (lexical, typed, loop) with the canonical
(kind, target) tie-break: same (events, spec version, query, clock) ->
byte-identical items on every channel. Retrieval is ad-hoc per-user point
work — it never replaces the hot-path context assembly (SPEC §7), and no
LLM runs anywhere in this path.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import final

from graph_memory.core.lexicon import lexical_hits, query_tokens
from graph_memory.core.types import Capability, Json, Principal, UserRef, require_tenant_match
from graph_memory.projector.engine import CapabilityUnknown, Projector, decayed_score
from graph_memory.projector.gateway import EdgeRow, NodeRow

_MAX_SEARCH_LIMIT = 25
_TYPED_CAPABILITY = 3
_TYPED_DECLARED = 2
_TYPED_OTHER = 1


@dataclass(frozen=True)
class RankedItem:
    """One (kind, target) group with its ranking features, evidence attached."""

    kind: str
    target: str
    lexical: int
    typed: int
    loop: float

    def to_payload(self) -> dict[str, Json]:
        return {
            "kind": self.kind,
            "target": self.target,
            "score": round(self.loop, 9),
            "features": {
                "lexical": self.lexical,
                "typed": self.typed,
                "loop": round(self.loop, 9),
            },
        }


def rank_search(
    rows_with_hits: Sequence[tuple[EdgeRow, int]],
    nodes: Mapping[str, NodeRow],
    capability_kinds: frozenset[str] | None,
    declared_kinds: frozenset[str],
    tokens: Sequence[str],
    now: datetime,
) -> list[RankedItem]:
    """The pure kernel: candidate rows -> ranked items, no IO and no randomness.

    ``rows_with_hits`` is the gateway's per-user lexical retrieval (its int is
    the shared kind/target metric). Node labels/props contribute their own
    token hits; groups aggregate like the snapshot does (max lexical, summed
    decayed score), so "search" and "context" always agree on what an item
    is. ``nodes`` maps target key -> node row; missing nodes contribute zero.
    """
    hits_by_group: dict[tuple[str, str], int] = {}
    rows_by_group: dict[tuple[str, str], list[EdgeRow]] = {}
    for row, hits in rows_with_hits:
        key = (row.kind, row.target)
        rows_by_group.setdefault(key, []).append(row)
        previous = hits_by_group.get(key)
        hits_by_group[key] = hits if previous is None else max(previous, hits)
    items: list[RankedItem] = []
    for key, rows in rows_by_group.items():
        kind, target = key
        text_hits = hits_by_group[key]
        node = nodes.get(target)
        node_text: tuple[str, ...] = ()
        if node is not None:
            node_text = (*node.labels, *node.props.values())
        lexical = text_hits + lexical_hits(node_text, tokens)
        if capability_kinds is not None and kind not in capability_kinds:
            continue
        if tokens and lexical == 0:
            # A real query that matches nothing about this group is not a
            # result — only the tokenless browse mode lists everything.
            continue
        if capability_kinds is not None:
            typed = _TYPED_CAPABILITY
        elif kind in declared_kinds:
            typed = _TYPED_DECLARED
        else:
            typed = _TYPED_OTHER
        loop = sum(decayed_score(r.weight, r.half_life_days, r.valid_at, now) for r in rows)
        items.append(RankedItem(kind=kind, target=target, lexical=lexical, typed=typed, loop=loop))
    items.sort(key=lambda item: (-item.lexical, -item.typed, -item.loop, item.kind, item.target))
    return items


@final
class MemorySearchService:
    """Implements ``core.ports.MemorySearch`` over the served projector.

    One engine, one injectable read: tenant from the verified principal,
    per-user bounded retrieval, capability filtering and a bounded result
    list. Sync by contract — channels run it off the event loop.
    """

    def __init__(self, engine: Projector, *, max_limit: int = _MAX_SEARCH_LIMIT) -> None:
        self._engine = engine
        self._max_limit = max_limit
        self._declared = frozenset(
            edge.kind for signal in engine.spec.signals.values() for edge in signal.edges
        )

    def search(
        self,
        principal: Principal,
        user: UserRef,
        query: str,
        *,
        capability: Capability | None,
        limit: int,
    ) -> dict[str, Json]:
        require_tenant_match(principal, user)
        tenant = principal.tenant
        rule = None
        if capability is not None:
            rule = self._engine.spec.capabilities.get(capability)
            if rule is None:
                raise CapabilityUnknown(
                    "capability is not declared by the mapping spec",
                    details={"capability": capability},
                )
        tokens = query_tokens(query)
        bounded = max(1, min(limit, self._max_limit))
        now = self._engine.clock.now()
        rows_with_hits = self._engine.gateway.search_edges(tenant, user.user_key, tokens)
        targets = sorted({row.target for row, _hits in rows_with_hits})
        # Never hit the store with an empty key set (an empty IN in SQL).
        nodes: dict[str, NodeRow] = {}
        for node in self._engine.gateway.nodes_of(tenant, targets) if targets else ():
            nodes[node.key] = node
        capability_kinds = None if rule is None else frozenset(rule.relations)
        ranked = rank_search(
            rows_with_hits,
            nodes,
            capability_kinds,
            self._declared,
            tokens,
            now,
        )
        return {
            "user": str(user.user_key),
            "query": query,
            "capability": None if capability is None else str(capability),
            "generated_at": now.isoformat(),
            "items": [item.to_payload() for item in ranked[:bounded]],
        }
