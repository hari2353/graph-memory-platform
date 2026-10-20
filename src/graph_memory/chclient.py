"""ClickHouse client construction + the adapters' thread-local session policy.

``clickhouse_connect`` forbids concurrent queries on one session — and
servers host threads (uvicorn worker pools, serve's projection loop), not
one-per-process. Adapters therefore resolve their client PER THREAD through
``bind_thread_local``: same server, same semantics, one HTTP session per
thread, lifetime bounded by the thread.
"""

from __future__ import annotations

import threading
from collections.abc import Callable

from clickhouse_connect.driver.client import Client

from graph_memory.settings import Settings


def default_client(settings: Settings) -> Client:
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_username,
        password=settings.clickhouse_password,
    )


def bind_thread_local[ClientT](
    factory: Callable[[Settings], ClientT],
) -> Callable[[Settings], ClientT]:
    """Wrap any client factory so each thread gets its own lazy instance."""
    local = threading.local()

    def factory_per_thread(settings: Settings) -> ClientT:
        client: ClientT | None = getattr(local, "client", None)
        if client is None:
            client = factory(settings)
            local.client = client
        return client

    return factory_per_thread
