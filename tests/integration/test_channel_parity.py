"""Channel parity e2e (SPEC §9): MCP == REST == SDK == pure provider.

One provider instance, three real HTTP surfaces. The invariant under test:
every channel renders the SAME payload for the same user/capability —
channels are renderers, never second brains.
"""

from __future__ import annotations

import asyncio
import json
import socket
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import uvicorn

from graph_memory.channels.mcp import build_mcp_server, mcp_streamable_app
from graph_memory.channels.rest import create_rest_app
from graph_memory.channels.sdk import GraphMemoryClient
from graph_memory.context.service import ContextCache, MemoryContextProvider
from graph_memory.core.clock import FixedClock
from graph_memory.core.types import TenantId
from graph_memory.mapping.spec import MappingSpec, load_spec
from graph_memory.projector.engine import Projector
from graph_memory.projector.gateway import InMemoryGraphGateway
from graph_memory.settings import Settings
from graph_memory.signals.api import create_app as create_ingest_app
from graph_memory.signals.ledger import InMemoryLedger
from graph_memory.tenancy.guard import StaticBearerGuard

_T = TenantId("acme")
_TOKEN = "dev-acme-token"
_SPEC_PATH = Path(__file__).parent.parent / "fixtures" / "mapping_skill_loop.yaml"
_PAYLOADS = [
    ("p1", "user-1", "rust"),
    ("p2", "user-1", "python"),
    ("p3", "user-1", "go"),
]

_HEADERS = {"Authorization": f"Bearer {_TOKEN}"}


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class _AppServer:
    """In-process uvicorn on an ephemeral port."""

    def __init__(self, app: Any) -> None:
        self.port = _free_port()
        self.server = uvicorn.Server(
            uvicorn.Config(app, host="127.0.0.1", port=self.port, log_level="warning")
        )
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.thread.start()
        deadline = time.monotonic() + 10
        while not self.server.started and time.monotonic() < deadline:
            time.sleep(0.05)
        assert self.server.started

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=5)


def _mcp_tools_call(port: int, arguments: dict[str, str]) -> dict[str, Any]:
    response = httpx.post(
        f"http://127.0.0.1:{port}/mcp",
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "get_context", "arguments": arguments},
        },
        headers={**_HEADERS, "Accept": "application/json"},
    )
    assert response.status_code == 200, response.text
    return dict(json.loads(response.text))


async def _drive() -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    spec: MappingSpec = load_spec(_SPEC_PATH.read_text(encoding="utf-8"))
    ledger = InMemoryLedger()
    clock = FixedClock(datetime(2026, 6, 10, tzinfo=UTC))
    engine = Projector(spec, ledger, InMemoryGraphGateway(), clock)
    provider = MemoryContextProvider(engine, ContextCache())

    guard = StaticBearerGuard(Settings().dev_static_tokens)
    ingest = _AppServer(create_ingest_app(ledger, guard))
    rest = _AppServer(create_rest_app(provider, guard))
    mcp = _AppServer(
        mcp_streamable_app(
            build_mcp_server(
                provider, guard, capabilities=tuple(sorted(spec.capabilities)), signal_schemas={}
            )
        )
    )
    try:
        client = GraphMemoryClient(
            f"http://127.0.0.1:{rest.port}",
            _TOKEN,
            ingest_base_url=f"http://127.0.0.1:{ingest.port}",
        )
        with client:
            signs = [
                {
                    "specversion": "1.0",
                    "id": eid,
                    "source": "https://app.example.com/signals",
                    "type": "itf.skill_assessed.v1",
                    "time": f"2026-06-01T12:00:00.{100 + i:03d}Z",
                    "partitionkey": user,
                    "data": {"skill": skill},
                }
                for i, (eid, user, skill) in enumerate(_PAYLOADS)
            ]
            ingested = client.ingest(signs)
            assert ingested == {"accepted": 3, "duplicates": 0, "rejected": []}
            # The ingest→projector pipeline stage (async in production): the
            # projector consumes the ledger and materializes gateway state
            # before any read channel runs.
            engine.process_events(ledger.read_events(_T))

            rest_body = client.context("user-1", "skills")
            walked = list(client.iter_items("user-1", "skills", page_size=2))
            prefetch = client.prefetch("user-1", "skills", idempotency_key="parity-1")
            replay = client.prefetch("user-1", "skills", idempotency_key="parity-1")
            assert replay.get("materialized") == prefetch.get("materialized")
        mcp_body = _mcp_tools_call(mcp.port, {"user": "user-1", "capability": "skills"})
        return rest_body, mcp_body, walked
    finally:
        ingest.stop()
        rest.stop()
        mcp.stop()


def test_channels_render_one_payload() -> None:
    rest_body, mcp_body, walked = asyncio.run(_drive())
    rest_payload = rest_body["payload"]
    mcp_structured = mcp_body["result"]["structuredContent"]
    # The parity invariant — one provider, identical rendered payloads.
    assert mcp_structured["payload"] == rest_payload
    assert rest_body["capability"] == "skills"
    targets_from_items = {item["target"] for item in walked}
    targets_from_payload = {item["target"] for item in rest_payload["items"]}
    assert targets_from_items == targets_from_payload == {"skill:rust", "skill:python", "skill:go"}
