"""M1 live tier: the staged rebuild publishes with an atomic exchange.

``replace_all`` stages into hold-out tables readers never see; while the
rebuild runs the published projection keeps serving; ``commit_rebuild``
publishes with ``EXCHANGE TABLES`` (metadata-level, atomic). This pins that a
serving instance never exposes a half-built graph — the defect that made
per-interval truncate+rebuild unsafe.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest

from graph_memory.core.types import TenantId, UserKey
from graph_memory.mapping.compiler import EdgeOp
from graph_memory.projector.gateway_clickhouse import ClickHouseGateway
from graph_memory.settings import Settings

_T = TenantId("acme")


def _edge(user_key: str, target: str) -> EdgeOp:
    return EdgeOp(
        tenant=_T,
        user=UserKey(user_key),
        kind="HAS_SKILL",
        target=target,
        weight=1.0,
        half_life_days=365.0,
        valid_at=datetime(2026, 6, 1, 12, 0, tzinfo=UTC),
    )


def _targets(gateway: ClickHouseGateway, user_key: str) -> set[str]:
    return {row.target for row in gateway.observations(_T, UserKey(user_key))}


@pytest.mark.live
def test_staged_rebuild_publishes_atomically_on_clickhouse() -> None:
    settings = Settings()
    gateway = ClickHouseGateway(settings)
    try:
        gateway.ensure_graph_schema()
        user_key = f"swap-{uuid.uuid4().hex[:8]}"

        # 1. seed the published projection
        gateway.apply([_edge(user_key, "skill:published")])
        assert _targets(gateway, user_key) == {"skill:published"}

        # 2. open a rebuild; the staged writes are invisible to readers
        gateway.replace_all()
        gateway.apply([_edge(user_key, "skill:staged")])
        assert _targets(gateway, user_key) == {"skill:published"}

        # 3. commit publishes the staged state in one atomic step
        gateway.commit_rebuild()
        assert _targets(gateway, user_key) == {"skill:staged"}

        # 4. post-commit incremental applies publish immediately again
        gateway.apply([_edge(user_key, "skill:incremental")])
        assert _targets(gateway, user_key) == {"skill:staged", "skill:incremental"}
    finally:
        gateway.client.command(
            f"DELETE FROM {settings.clickhouse_database}.gm_edges "
            f"WHERE tenant_id = 'acme' AND user_key LIKE 'swap-%'"
        )
        gateway.client.command(
            f"DELETE FROM {settings.clickhouse_database}.gm_edges__staging "
            f"WHERE tenant_id = 'acme' AND user_key LIKE 'swap-%'"
        )
