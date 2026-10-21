"""MCP channel tests: stateless JSON mode (2026-07-28 posture) + compat mode.

Runs the real ASGI app under uvicorn in-process and drives it with the
official SDK client — transport, session posture, tool schemas and structured
results are all exercised honestly (SPEC §9: conformance pinned in CI).
"""

from __future__ import annotations

import socket
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import uvicorn

from graph_memory.channels.mcp import PROTOCOL_REVISION, build_mcp_server, mcp_streamable_app
from graph_memory.context.explain import context_explainer, effectiveness_reporter
from graph_memory.context.service import ContextCache, MemoryContextProvider
from graph_memory.core.clock import FixedClock
from graph_memory.core.types import TenantId
from graph_memory.mapping.spec import MappingSpec, load_spec
from graph_memory.projector.engine import Projector
from graph_memory.projector.gateway import InMemoryGraphGateway
from graph_memory.settings import Settings
from graph_memory.signals.envelope import Envelope
from graph_memory.signals.ledger import InMemoryLedger, record_from_envelope
from graph_memory.tenancy.guard import StaticBearerGuard

_T = TenantId("acme")
_SPEC_PATH = Path(__file__).parent.parent / "fixtures" / "mapping_skill_loop.yaml"
_TOKEN = "dev-acme-token"


def _fixture_provider() -> tuple[
    MemoryContextProvider, MappingSpec, Projector, InMemoryLedger, FixedClock
]:
    spec: MappingSpec = load_spec(_SPEC_PATH.read_text(encoding="utf-8"))
    ledger = InMemoryLedger()
    clock = FixedClock(datetime(2026, 6, 10, tzinfo=UTC))
    gateway = InMemoryGraphGateway()
    engine = Projector(spec, ledger, gateway, clock)
    for index, (user, skill) in enumerate([("user-1", "rust"), ("user-1", "python")]):
        payload: dict[str, object] = {
            "specversion": "1.0",
            "id": f"mcp-{index}",
            "source": "https://app.example.com/signals",
            "type": "itf.skill_assessed.v1",
            "time": f"2026-06-01T12:00:00.{100 + index:03d}Z",
            "partitionkey": user,
            "data": {"skill": skill},
        }
        ledger.append([record_from_envelope(_T, Envelope.parse(payload))])
    engine.process_events(ledger.read_events(_T))
    return MemoryContextProvider(engine, ContextCache()), spec, engine, ledger, clock


_SCHEMA_SUMMARY: dict[str, dict[str, Any]] = {
    "itf.skill_assessed.v1": {"relations": ["HAS_SKILL"], "decay_days": 180.0},
}


class _AppServer:
    """In-process uvicorn on an ephemeral port; threads end with the test."""

    def __init__(self, app: Any) -> None:  # noqa: ANN401 — ASGI app object
        self.port = _free_port()
        self.server = uvicorn.Server(
            uvicorn.Config(app, host="127.0.0.1", port=self.port, log_level="warning")
        )
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.thread.start()
        deadline = time.monotonic() + 10
        while not self.server.started and time.monotonic() < deadline:
            time.sleep(0.05)
        if not self.server.started:
            raise RuntimeError("uvicorn did not start")

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}/mcp"

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=5)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wired_server(*, with_faces: bool = True) -> Any:
    provider, spec, engine, ledger, clock = _fixture_provider()
    guard = StaticBearerGuard(Settings().dev_static_tokens)
    effectiveness = None
    explainer = None
    if with_faces:
        effectiveness = effectiveness_reporter(spec, ledger, clock)
        explainer = context_explainer(engine)
    return build_mcp_server(
        provider,
        guard,
        capabilities=tuple(sorted(spec.capabilities)),
        signal_schemas=_SCHEMA_SUMMARY,
        effectiveness=effectiveness,
        explainer=explainer,
    )


@pytest.fixture()
def stateless() -> Any:
    holder = _AppServer(mcp_streamable_app(_wired_server(), stateless=True))
    yield holder
    holder.stop()


@pytest.fixture()
def stateless_unwired() -> Any:
    holder = _AppServer(mcp_streamable_app(_wired_server(with_faces=False), stateless=True))
    yield holder
    holder.stop()


async def test_stateless_mode_serves_tools_with_structured_content(stateless: _AppServer) -> None:
    from mcp.client.session import ClientSession
    from mcp.client.streamable_http import streamable_http_client

    headers = {"Authorization": f"Bearer {_TOKEN}"}
    client = _authed_async_client(stateless, headers)
    async with (
        streamable_http_client(stateless.url, http_client=client) as (read, write),
        ClientSession(  # noqa: E501
            read, write
        ) as session,
    ):
        await session.initialize()
        listed = await session.list_tools()
        names = sorted(tool.name for tool in listed.tools)
        assert names == [
            "explain_context",
            "get_context",
            "get_effectiveness",
            "get_signal_schema",
            "list_capabilities",
            "search_memory",
        ]
        # Tenant is never a tool argument (SPEC §8).
        for tool in listed.tools:
            assert "tenant" not in (tool.input_schema.get("properties") or {})

        result = await session.call_tool("get_context", {"user": "user-1", "capability": "skills"})
        assert not result.is_error
        body = result.structured_content
        assert body is not None
        assert body["capability"] == "skills"
        assert body["max_age_ms"] == 60_000
        assert body["stale_at"] > body["generated_at"]

        search = await session.call_tool(
            "search_memory",
            {"user": "user-1", "capability": "skills", "query": "rust"},
        )
        assert search.structured_content is not None
        items = search.structured_content["items"]
        assert [item["target"] for item in items] == ["skill:rust"]

        caps = await session.call_tool("list_capabilities", {})
        assert caps.structured_content is not None
        assert "skills" in caps.structured_content["capabilities"]

        schema = await session.call_tool("get_signal_schema", {})
        assert schema.structured_content is not None
        signals = schema.structured_content["signals"]
        assert signals["itf.skill_assessed.v1"]["relations"] == ["HAS_SKILL"]


async def test_unknown_capability_is_a_tool_error(stateless: _AppServer) -> None:
    from mcp.client.session import ClientSession
    from mcp.client.streamable_http import streamable_http_client

    client = _authed_async_client(stateless, {"Authorization": f"Bearer {_TOKEN}"})
    async with (
        streamable_http_client(stateless.url, http_client=client) as (read, write),
        ClientSession(read, write) as session,
    ):
        await session.initialize()
        bad = await session.call_tool("get_context", {"user": "user-1", "capability": "nope"})
        # Anticipated failures surface as is-error results carrying our
        # stable payload JSON (not crashes).
        assert bad.is_error
        assert "not declared" in str(bad.model_dump())
        assert "capability_unknown" in str(bad.model_dump())


async def test_effectiveness_and_explain_faces_serve_when_wired(
    stateless: _AppServer,
) -> None:
    from mcp.client.session import ClientSession
    from mcp.client.streamable_http import streamable_http_client

    client = _authed_async_client(stateless, {"Authorization": f"Bearer {_TOKEN}"})
    async with (
        streamable_http_client(stateless.url, http_client=client) as (read, write),
        ClientSession(read, write) as session,
    ):
        await session.initialize()
        eff = await session.call_tool("get_effectiveness", {})
        assert not eff.is_error
        eff_body = eff.structured_content
        assert eff_body is not None
        assert eff_body["tenant"] == "acme"
        # The fixture ledger holds no reco/outcome pairs: an empty-but-honest
        # report, not an invented one.
        assert eff_body["reco_delivered"] == 0
        assert eff_body["cells"] == []
        assert eff_body["weight_overrides"] == {}

        provenance = await session.call_tool(
            "explain_context", {"user": "user-1", "capability": "skills"}
        )
        assert not provenance.is_error
        body = provenance.structured_content
        assert body is not None
        assert body["capability"] == "skills"
        assert body["user"] == "user-1"
        assert isinstance(body["top_k"], int) and body["top_k"] > 0
        observations = body["observations"]
        assert isinstance(observations, list) and len(observations) >= 2
        first = observations[0]
        assert set(first) == {
            "kind",
            "target",
            "weight",
            "half_life_days",
            "valid_at",
            "score",
            "produced_by",
        }
        assert first["kind"] == "HAS_SKILL"
        assert first["produced_by"] == ["itf.skill_assessed.v1"]
        # Deterministic descending score order (sorted on the tuple).
        scores = [float(entry["score"]) for entry in observations]
        assert scores == sorted(scores, reverse=True)


async def test_unwired_faces_fail_as_anticipated_tool_errors(
    stateless_unwired: _AppServer,
) -> None:
    from mcp.client.session import ClientSession
    from mcp.client.streamable_http import streamable_http_client

    client = _authed_async_client(stateless_unwired, {"Authorization": f"Bearer {_TOKEN}"})
    async with (
        streamable_http_client(stateless_unwired.url, http_client=client) as (read, write),
        ClientSession(read, write) as session,
    ):
        await session.initialize()
        eff = await session.call_tool("get_effectiveness", {})
        provenance = await session.call_tool(
            "explain_context", {"user": "user-1", "capability": "skills"}
        )
        assert eff.is_error
        assert "not_wired" in str(eff.model_dump())
        assert provenance.is_error
        assert "not_wired" in str(provenance.model_dump())


def _authed_async_client(holder: _AppServer, headers: dict[str, str]) -> Any:
    """The SDK's HTTP client (vendored httpx2) with credential headers."""
    import httpx2

    return httpx2.AsyncClient(
        base_url=f"http://127.0.0.1:{holder.port}", headers=headers, timeout=30
    )


def test_stateless_post_is_json_and_bears_no_session_id() -> None:
    """Stateless JSON posture: plain JSON responses, no session identity."""
    holder = _AppServer(mcp_streamable_app(_wired_server(), stateless=True))
    try:
        payload = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
        response = httpx.post(holder.url, json=payload, headers={"Accept": "application/json"})
        assert response.status_code == 200
        assert "application/json" in response.headers.get("content-type", "")
        assert "mcp-session-id" not in {key.casefold() for key in response.headers}
    finally:
        holder.stop()


def test_compat_mode_hands_back_a_session_id() -> None:
    """Same core riding the sessionful SSE posture — initialize gets a session."""
    holder = _AppServer(mcp_streamable_app(_wired_server(), stateless=False))
    try:
        payload = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
        response = httpx.post(holder.url, json=payload)
        assert response.status_code == 200
        assert "mcp-session-id" in {key.casefold() for key in response.headers}
        assert PROTOCOL_REVISION == "2026-07-28"
    finally:
        holder.stop()
