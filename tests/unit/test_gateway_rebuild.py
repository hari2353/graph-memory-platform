"""The rebuild contract: staged application, atomic publication.

A serving instance must never expose a half-built graph: while a rebuild is
open, readers keep observing the last committed projection; ``commit_rebuild``
publishes the staged state in one step. The in-memory adapter pins the
semantics; the live tier proves the ClickHouse exchange (marked live).
"""

from __future__ import annotations

from datetime import UTC, datetime

from graph_memory.core.types import TenantId, UserKey
from graph_memory.mapping.compiler import EdgeOp
from graph_memory.projector.gateway import InMemoryGraphGateway

_T = TenantId("acme")


def _edge(user: str, target: str) -> EdgeOp:
    return EdgeOp(
        tenant=_T,
        user=UserKey(user),
        kind="HAS_SKILL",
        target=target,
        weight=1.0,
        half_life_days=365.0,
        valid_at=datetime(2026, 10, 21, 9, 0, tzinfo=UTC),
    )


def _targets(gateway: InMemoryGraphGateway, user: str) -> set[str]:
    return {row.target for row in gateway.observations(_T, UserKey(user))}


def test_rebuild_staging_is_invisible_until_commit() -> None:
    gateway = InMemoryGraphGateway()
    gateway.apply([_edge("u1", "skill:old")])
    assert _targets(gateway, "u1") == {"skill:old"}

    gateway.replace_all()
    gateway.apply([_edge("u1", "skill:new")])
    # readers keep the committed projection while the rebuild runs
    assert _targets(gateway, "u1") == {"skill:old"}

    gateway.commit_rebuild()
    assert _targets(gateway, "u1") == {"skill:new"}


def test_commit_without_open_rebuild_is_a_noop() -> None:
    gateway = InMemoryGraphGateway()
    gateway.apply([_edge("u1", "skill:base")])
    gateway.commit_rebuild()
    assert _targets(gateway, "u1") == {"skill:base"}


def test_incremental_applies_outside_rebuild_publish_immediately() -> None:
    gateway = InMemoryGraphGateway()
    gateway.apply([_edge("u1", "skill:one")])
    assert _targets(gateway, "u1") == {"skill:one"}
    gateway.apply([_edge("u1", "skill:two")])
    assert _targets(gateway, "u1") == {"skill:one", "skill:two"}


def test_rebuild_replaces_the_whole_published_state() -> None:
    gateway = InMemoryGraphGateway()
    gateway.apply([_edge("u1", "skill:one")])
    gateway.replace_all()
    gateway.commit_rebuild()  # an empty staged state publishes (the rebuild's point)
    assert _targets(gateway, "u1") == set()
