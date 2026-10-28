"""Channel tests for governed forgetting (M4.1): adapter + MCP + REST.

The forget is ONE surface wired three ways: the adapter (core.ports.MemoryForget),
the MCP ``delete_memory`` tool and REST ``POST /v1/forget``. Every path derives
the tenant from the bearer (never an argument), requires the ``memory:write``
scope, and emits through the same ledger every other signal uses; unwired
ports fail as anticipated errors, never silently.
"""

from __future__ import annotations

import json
import socket
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient

from graph_memory.channels.mcp.server import build_mcp_server, mcp_streamable_app
from graph_memory.channels.rest import create_rest_app
from graph_memory.context.explain import context_explainer
from graph_memory.context.service import ContextCache, MemoryContextProvider
from graph_memory.core.clock import FixedClock
from graph_memory.core.errors import ForgetKindUnknown, ForgetTargetMismatch, TenantMismatch
from graph_memory.core.types import Capability, Principal, Scope, TenantId, UserKey, UserRef
from graph_memory.mapping.spec import load_spec
from graph_memory.observability.metering import MeteringCounters
from graph_memory.observability.observer import InMemorySpanSink
from graph_memory.projector.engine import Projector
from graph_memory.projector.gateway import InMemoryGraphGateway
from graph_memory.settings import Settings
from graph_memory.signals.envelope import Envelope
from graph_memory.signals.forget import ForgetService, forget_routes
from graph_memory.signals.ledger import InMemoryLedger, record_from_envelope
from graph_memory.tenancy.guard import StaticBearerGuard

_T = TenantId("acme")
_TOKEN = "dev-acme-token"
_SPEC_PATH = Path(__file__).parent.parent / "fixtures" / "mapping_with_forget.yaml"
_NOW = datetime(2026, 6, 10, tzinfo=UTC)
_LEARN_AT = datetime(2026, 6, 1, tzinfo=UTC)
_FORGET_AT = datetime(2026, 6, 20, tzinfo=UTC)


def _spec_text() -> str:
    return _SPEC_PATH.read_text(encoding="utf-8")


def _principal(tenant: TenantId = _T) -> Principal:
    return Principal(
        tenant=tenant,
        scopes=frozenset({Scope.SIGNALS_WRITE, Scope.CONTEXT_READ, Scope.MEMORY_WRITE}),
        subject="test",
    )


def _service(ledger: InMemoryLedger) -> ForgetService:
    return ForgetService(
        ledger, FixedClock(_FORGET_AT), routes=forget_routes(load_spec(_spec_text()))
    )


# --- adapter ---------------------------------------------------------------


def test_forget_routes_cover_kinds_from_the_declared_mapping() -> None:
    routes = forget_routes(load_spec(_spec_text()))
    assert set(routes) == {"HAS_SKILL", "RECOMMENDED"}
    assert routes["RECOMMENDED"] == (("itf.drop_reco.v1", None),)
    # HAS_SKILL has three routes; names sorted, scoped one last in declared order.
    shapes = sorted((name, rule) for name, rule in routes["HAS_SKILL"])
    assert ("itf.forget_one_skill.v1", "{data.target}") in shapes
    assert ("itf.forget_all_skills.v1", None) in shapes


def test_forget_service_emits_a_ledger_event_for_the_whole_kind() -> None:
    ledger = InMemoryLedger()
    body = _service(ledger).forget(_principal(), UserRef(_T, UserKey("user-1")), "HAS_SKILL", None)
    forgotten = json.loads(json.dumps(body["forgotten"]))  # JSON-safe by contract
    assert forgotten["kind"] == "HAS_SKILL"
    assert forgotten["signal"] == "itf.forget_all_skills.v1"
    assert forgotten["time"] == _FORGET_AT.isoformat()
    assert body["accepted"] == 1
    [stored] = ledger.read_events(_T)
    assert stored.signal == "itf.forget_all_skills.v1"
    assert stored.user_key == UserKey("user-1")
    assert stored.envelope.tenantid == "acme"
    assert stored.envelope.data == {"kind": "HAS_SKILL"}


def test_forget_service_routes_the_scoped_shape() -> None:
    ledger = InMemoryLedger()
    _ = _service(ledger).forget(
        _principal(), UserRef(_T, UserKey("user-1")), "HAS_SKILL", "skill:rust"
    )
    stored = ledger.read_events(_T)[0]
    assert stored.signal == "itf.forget_one_skill.v1"
    assert stored.envelope.data == {"kind": "HAS_SKILL", "target": "skill:rust"}


def test_forget_service_rejects_unknown_kinds_and_wrong_shapes() -> None:
    service = _service(InMemoryLedger())
    with pytest.raises(ForgetKindUnknown):
        service.forget(_principal(), UserRef(_T, UserKey("user-1")), "NOT_A_KIND", None)
    with pytest.raises(ForgetTargetMismatch):
        # RECOMMENDED only has a kind-wide rule: a scoped call has no shape.
        service.forget(_principal(), UserRef(_T, UserKey("user-1")), "RECOMMENDED", "skill:go")


def test_forget_service_rejects_cross_tenant_users() -> None:
    service = _service(InMemoryLedger())
    with pytest.raises(TenantMismatch):
        service.forget(
            _principal(),
            UserRef(TenantId("other"), UserKey("user-1")),
            "HAS_SKILL",
            None,
        )


def test_forget_service_round_trips_into_projection() -> None:
    """Emit via the adapter -> project -> the user's context loses the fact."""
    spec = load_spec(_spec_text())
    ledger = InMemoryLedger()
    engine = Projector(spec, ledger, InMemoryGraphGateway(), FixedClock(_NOW))
    ledger.append(
        [
            record_from_envelope(_T, Envelope.parse(_signal("s1", {"skill": "rust"}))),
            record_from_envelope(_T, Envelope.parse(_signal("s2", {"skill": "go"}))),
        ]
    )
    engine.process_events(ledger.read_events(_T))
    # The adapter emits; the projector's pure pass takes it from here.
    _service(ledger).forget(_principal(), UserRef(_T, UserKey("user-1")), "HAS_SKILL", None)
    engine.process_events(ledger.read_events(_T))
    snapshot = engine.snapshot(_T, UserKey("user-1"), Capability("skills"))
    assert snapshot.payload["items"] == []
    # Provenance: the explainer shows the tombstone that did it.
    explained = context_explainer(engine).explain(
        _principal(), UserRef(_T, UserKey("user-1")), Capability("skills")
    )
    directives = explained["forget_directives"]
    assert isinstance(directives, list) and len(directives) == 1
    tombstone = directives[0]
    assert isinstance(tombstone, dict)
    assert tombstone["kind"] == "HAS_SKILL"
    assert tombstone["scope"] == "kind_wide"


def _signal(eid: str, data: dict[str, object]) -> dict[str, object]:
    """A learn-signal payload at the fixed learn time (JSON-ready)."""
    return {
        "specversion": "1.0",
        "id": eid,
        "source": "https://app.example.com/signals",
        "type": "itf.skill_assessed.v1",
        "time": _LEARN_AT.isoformat(),
        "partitionkey": "user-1",
        "data": data,
    }


# --- REST + MCP surfaces ----------------------------------------------------


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


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
        assert self.server.started

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=5)


class _World:
    """One projected engine + all forget surfaces wired from it."""

    def __init__(self) -> None:
        spec = load_spec(_spec_text())
        self.ledger = InMemoryLedger()
        self.engine = Projector(spec, self.ledger, InMemoryGraphGateway(), FixedClock(_NOW))
        self.ledger.append(
            [
                record_from_envelope(_T, Envelope.parse(_signal("s1", {"skill": "rust"}))),
                record_from_envelope(_T, Envelope.parse(_signal("s2", {"skill": "go"}))),
            ]
        )
        self.engine.process_events(self.ledger.read_events(_T))
        provider = MemoryContextProvider(self.engine, ContextCache())
        self.metering = MeteringCounters()
        self.spans = InMemorySpanSink()
        guard = StaticBearerGuard(Settings().dev_static_tokens)
        forget: ForgetService = _service(self.ledger)
        self.rest = _AppServer(
            create_rest_app(
                provider,
                guard,
                sink=self.spans,
                metering=self.metering,
                forget=forget,
            )
        )
        self.mcp = _AppServer(
            mcp_streamable_app(
                build_mcp_server(
                    provider,
                    guard,
                    capabilities=tuple(sorted(spec.capabilities)),
                    signal_schemas={},
                    explainer=context_explainer(self.engine),
                    forget=forget,
                )
            )
        )

    def stop(self) -> None:
        self.rest.stop()
        self.mcp.stop()


def _mcp_call(
    port: int, name: str, arguments: dict[str, object], token: str = _TOKEN
) -> dict[str, Any]:
    response = httpx.post(
        f"http://127.0.0.1:{port}/mcp",
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        },
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )
    assert response.status_code == 200, response.text
    return dict(json.loads(response.text))


def test_rest_forget_round_trip_reaches_the_read_model() -> None:
    world = _World()
    try:
        response = httpx.post(
            f"http://127.0.0.1:{world.rest.port}/v1/forget/user-1",
            json={"kind": "HAS_SKILL", "target": "skill:rust"},
            headers={"Authorization": f"Bearer {_TOKEN}"},
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["forgotten"]["kind"] == "HAS_SKILL"
        assert body["forgotten"]["target"] == "skill:rust"
        assert body["accepted"] == 1
        # The ledger event landed; a projection pass applies the tombstone.
        world.engine.process_events(world.ledger.read_events(_T))
        snapshot = world.engine.snapshot(_T, UserKey("user-1"), Capability("skills"))
        items = cast("list[dict[str, str]]", snapshot.payload["items"])
        targets = [item["target"] for item in items]
        assert targets == ["skill:go"]
        # Metering + audit: one forget row, one span, per request class.
        rows = world.metering.drain()
        assert rows[(_T, "rest", "forget")] == 1
        span_names = [span.name for span in world.spans if span.name == "gmp.channel.rest.forget"]
        assert len(span_names) == 1
    finally:
        world.stop()


def test_rest_forget_requires_the_memory_write_scope() -> None:
    scopeless = StaticBearerGuard(
        {"dev-acme-token": "acme"}, scopes=frozenset({Scope.CONTEXT_READ})
    )
    provider = MemoryContextProvider(_project_with_events(), ContextCache())
    app = TestClient(
        create_rest_app(
            provider,
            scopeless,
            forget=_service(InMemoryLedger()),
        )
    )
    response = app.post(
        "/v1/forget/user-1",
        json={"kind": "HAS_SKILL"},
        headers={"Authorization": f"Bearer {_TOKEN}"},
    )
    assert response.status_code == 403
    assert response.json()["detail"]["error"] == "scope_denied"


def test_rest_forget_rejects_unknown_credentials() -> None:
    world = _World()
    try:
        response = httpx.post(
            f"http://127.0.0.1:{world.rest.port}/v1/forget/user-1",
            json={"kind": "HAS_SKILL"},
            headers={"Authorization": "Bearer who-am-i"},
        )
        assert response.status_code == 401
        assert response.json()["detail"]["error"] == "scope_denied"
    finally:
        world.stop()


def test_rest_forget_maps_platform_errors_to_stable_statuses() -> None:
    world = _World()
    try:
        unknown = httpx.post(
            f"http://127.0.0.1:{world.rest.port}/v1/forget/user-1",
            json={"kind": "NOT_A_KIND"},
            headers={"Authorization": f"Bearer {_TOKEN}"},
        )
        assert unknown.status_code == 404
        payload = unknown.json()["detail"]
        assert payload["error"] == "forget_kind_unknown"
        mismatch = httpx.post(
            f"http://127.0.0.1:{world.rest.port}/v1/forget/user-1",
            json={"kind": "RECOMMENDED", "target": "skill:go"},
            headers={"Authorization": f"Bearer {_TOKEN}"},
        )
        # RECOMMENDED only declares a kind-wide rule: a scoped call has no shape.
        assert mismatch.status_code == 400
        shape = mismatch.json()["detail"]
        assert shape["error"] == "forget_target_mismatch"
    finally:
        world.stop()


def test_rest_forget_unwired_fails_as_an_anticipated_error() -> None:
    provider = MemoryContextProvider(_project_with_events(), ContextCache())
    app = TestClient(create_rest_app(provider, StaticBearerGuard(Settings().dev_static_tokens)))
    response = app.post(
        "/v1/forget/user-1",
        json={"kind": "HAS_SKILL"},
        headers={"Authorization": f"Bearer {_TOKEN}"},
    )
    assert response.status_code == 500
    detail = response.json()["detail"]
    assert detail["error"] == "platform_error"
    assert detail["details"]["reason"] == "not_wired"


def _project_with_events() -> Projector:
    spec = load_spec(_spec_text())
    engine = Projector(spec, InMemoryLedger(), InMemoryGraphGateway(), FixedClock(_NOW))
    return engine


def test_mcp_delete_memory_round_trip_parity_with_rest() -> None:
    world = _World()
    try:
        result = _mcp_call(world.mcp.port, "delete_memory", {"user": "user-1", "kind": "HAS_SKILL"})
        structured = result["result"]["structuredContent"]
        assert structured["forgotten"]["kind"] == "HAS_SKILL"
        assert structured["accepted"] == 1
        world.engine.process_events(world.ledger.read_events(_T))
        snapshot = world.engine.snapshot(_T, UserKey("user-1"), Capability("skills"))
        assert snapshot.payload["items"] == []

    finally:
        world.stop()


def test_mcp_delete_memory_scoped_target_and_unknown_kind_errors() -> None:
    world = _World()
    try:
        scoped = _mcp_call(
            world.mcp.port,
            "delete_memory",
            {"user": "user-1", "kind": "HAS_SKILL", "target": "skill:rust"},
        )
        assert scoped["result"]["structuredContent"]["forgotten"]["target"] == "skill:rust"
        bad = _mcp_call(world.mcp.port, "delete_memory", {"user": "user-1", "kind": "NOT_A_KIND"})
        error_text = json.dumps(bad["result"])
        assert "forget_kind_unknown" in error_text
    finally:
        world.stop()
