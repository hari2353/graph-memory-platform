"""Live-tier gating: the `live` marker (pyproject) means "requires the real
ClickHouse + ClickGraph server at the Settings endpoints". Skipped by
default when that server is unreachable, run as-is when it is up — the
README's "skipped by default" contract, finally enforced."""

from __future__ import annotations

import socket

import pytest


def _reachable(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    live_items = [item for item in items if "live" in item.keywords]
    if not live_items:
        return
    from graph_memory.settings import Settings

    settings = Settings()
    if _reachable(settings.clickhouse_host, settings.clickhouse_port):
        return  # the server is up: the live tier runs
    skip = pytest.mark.skip(
        reason=f"live tier: no ClickHouse at {settings.clickhouse_host}:{settings.clickhouse_port}"
    )
    for item in live_items:
        item.add_marker(skip)
