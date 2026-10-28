"""Export/portability tests (M4.5, research 06): adapter + REST channel.

``GET /v1/export/{user}`` is GDPR portability as a first-class READ: the
payload is the union of the served point reads (engine.snapshot per
declared capability) plus the user's OWN ledger-scoped event history,
bounded and honest about truncation. The proven contract:

- the tenant rule is structural (mismatch unrepresentable), history is
  user-scoped (a co-user exports nothing of the other's);
- unwired deployments fail as an anticipated ``not_wired`` error, never
  silently; scopes ride the same coarse ``context:read`` family;
- metering ticks one ``export`` row and the span lands per request class;
- adapter == REST byte-equality (channels are renderers, never second
  brains).
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
from fastapi.testclient import TestClient

from graph_memory.channels.rest import create_rest_app
from graph_memory.context.export import ExportService
from graph_memory.context.service import ContextCache, MemoryContextProvider
from graph_memory.core.clock import FixedClock
from graph_memory.core.errors import TenantMismatch
from graph_memory.core.ports import MemoryExport
from graph_memory.core.types import Capability, Principal, Scope, TenantId, UserKey, UserRef
from graph_memory.mapping.spec import load_spec
from graph_memory.observability.metering import MeteringCounters
from graph_memory.observability.observer import InMemorySpanSink
from graph_memory.projector.engine import Projector
from graph_memory.projector.gateway import InMemoryGraphGateway
from graph_memory.settings import Settings
from graph_memory.signals.envelope import Envelope
from graph_memory.signals.ledger import InMemoryLedger, record_from_envelope
from graph_memory.tenancy.guard import StaticBearerGuard

_T = TenantId("acme")
_TOKEN = "dev-acme-token"
_SPEC_PATH = Path(__file__).parent.parent / "fixtures" / "mapping_with_forget.yaml"
_NOW = datetime(2026, 6, 10, tzinfo=UTC)
_LEARN_AT = datetime(2026, 6, 1, tzinfo=UTC)


def _record(eid: str, user: str, skill: str, *, at: datetime = _LEARN_AT) -> Any:
    envelope = Envelope.parse(
        {
            "specversion": "1.0",
            "id": eid,
            "source": "https://app.example.com/signals",
            "type": "itf.skill_assessed.v1",
            "time": at.isoformat(),
            "partitionkey": user,
            "data": {"skill": skill},
        }
    )
    return record_from_envelope(_T, envelope)


def _world_state() -> tuple[Projector, InMemoryLedger]:
    """Fresh engine + ledger with two users, three events for user-1."""
    ledger = InMemoryLedger()
    ledger.append([_record("s1", "user-1", "rust"), _record("s2", "user-1", "go")])
    ledger.append([_record("s3", "user-1", "python", at=datetime(2026, 6, 2, tzinfo=UTC))])
    ledger.append([_record("other-1", "user-2", "hidden-skill")])
    engine = Projector(
        load_spec(_SPEC_PATH.read_text(encoding="utf-8")),
        ledger,
        InMemoryGraphGateway(),
        FixedClock(_NOW),
    )
    engine.replay()
    return engine, ledger


def _principal(tenant: TenantId = _T) -> Principal:
    return Principal(
        tenant=tenant,
        scopes=frozenset({Scope.CONTEXT_READ, Scope.MEMORY_SEARCH}),
        subject="test",
    )


# --- the adapter -------------------------------------------------------------


def test_export_payload_is_the_union_of_point_reads_plus_history() -> None:
    engine, ledger = _world_state()
    body = ExportService(engine, ledger).export(_principal(), UserRef(_T, UserKey("user-1")))
    assert body["user"] == "user-1"
    assert body["generated_at"] == _NOW.isoformat()
    assert body["counts"] == {"capabilities": 1, "events": 3}
    assert body["history_truncated"] is False
    capabilities = body["capabilities"]
    assert isinstance(capabilities, dict) and set(capabilities) == {"skills"}
    payload = capabilities["skills"]
    assert isinstance(payload, dict)  # the SAME shape a point read serves
    items = payload["items"]
    assert isinstance(items, list) and len(items) == 3
    history = body["history"]
    assert isinstance(history, list)
    [first, second, third] = (record for record in history if isinstance(record, dict))
    assert [first["id"], second["id"], third["id"]] == ["s1", "s2", "s3"]
    assert first["type"] == "itf.skill_assessed.v1"
    assert first["time"] == _LEARN_AT.isoformat()
    assert first["data"] == {"skill": "rust"}
    # Nothing of user-2's leaks into user-1's archive.
    assert "hidden-skill" not in repr(body)


def test_export_truncates_honestly_at_the_event_bound() -> None:
    engine, ledger = _world_state()
    body = ExportService(engine, ledger, max_events=2).export(
        _principal(), UserRef(_T, UserKey("user-1"))
    )
    assert body["counts"] == {"capabilities": 1, "events": 2}
    assert body["history_truncated"] is True


def test_export_enforces_the_tenant_rule_and_satisfies_the_port() -> None:
    engine, ledger = _world_state()
    service: MemoryExport = ExportService(engine, ledger)
    with pytest.raises(TenantMismatch):
        service.export(_principal(), UserRef(TenantId("other"), UserKey("user-1")))
    empty = service.export(_principal(), UserRef(_T, UserKey("user-2")))
    assert empty["counts"] == {"capabilities": 1, "events": 1}  # their OWN event
    history = empty["history"]
    assert isinstance(history, list)
    [event] = (record for record in history if isinstance(record, dict))
    assert event["data"] == {"skill": "hidden-skill"}  # theirs, not user-1's


# --- the REST channel ---------------------------------------------------------


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class _AppServer:
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


def _local_app(engine: Projector, guard: StaticBearerGuard) -> TestClient:
    return TestClient(create_rest_app(MemoryContextProvider(engine, ContextCache()), guard))


def test_rest_export_round_trip_parity_and_audit() -> None:
    engine, ledger = _world_state()
    metering = MeteringCounters()
    spans = InMemorySpanSink()
    guard = StaticBearerGuard(Settings().dev_static_tokens)
    service = ExportService(engine, ledger)
    rest = _AppServer(
        create_rest_app(
            MemoryContextProvider(engine, ContextCache()),
            guard,
            sink=spans,
            metering=metering,
            export=service,
        )
    )
    try:
        response = httpx.get(
            f"http://127.0.0.1:{rest.port}/v1/export/user-1",
            headers={"Authorization": f"Bearer {_TOKEN}"},
        )
        assert response.status_code == 200, response.text
        # Channel parity: REST renders the ADAPTER's payload verbatim.
        assert response.json() == service.export(_principal(), UserRef(_T, UserKey("user-1")))
        rows = metering.drain()
        assert rows[(_T, "rest", "export")] == 1
        span_names = [span.name for span in spans if span.name == "gmp.channel.rest.export"]
        assert len(span_names) == 1
    finally:
        rest.stop()


def test_rest_export_scopes_unwired_and_isolation() -> None:
    engine, _ledger = _world_state()
    scopeless_guard = StaticBearerGuard(
        {"dev-acme-token": "acme"}, scopes=frozenset({Scope.MEMORY_SEARCH})
    )
    guard = StaticBearerGuard(Settings().dev_static_tokens)
    app = _local_app(engine, scopeless_guard)

    # context:read is missing → the read family fails closed.
    denied = app.get("/v1/export/user-1", headers={"Authorization": f"Bearer {_TOKEN}"})
    assert denied.status_code == 403
    assert denied.json()["detail"]["error"] == "scope_denied"

    # A rejected credential is 401 with the stable unknown_credential code.
    bogus = app.get("/v1/export/user-1", headers={"Authorization": "Bearer no-such-token"})
    assert bogus.status_code == 401

    # Unwired deployments fail as an anticipated error, never silently.
    unwired = _local_app(engine, guard)
    gone = unwired.get("/v1/export/user-1", headers={"Authorization": f"Bearer {_TOKEN}"})
    assert gone.status_code == 500
    assert gone.json()["detail"]["error"] == "platform_error"
    assert gone.json()["detail"]["details"]["reason"] == "not_wired"

    # Tenant rides the credential: another tenant's bearer sees ONLY its
    # own (empty) state for the same user key — never acme's rows.
    cross = TestClient(
        create_rest_app(
            MemoryContextProvider(engine, ContextCache()),
            StaticBearerGuard({"dev-other-token": "otherco"}),
            export=ExportService(engine, _ledger),
        )
    )
    try:
        leaked = cross.get("/v1/export/user-1", headers={"Authorization": "Bearer dev-other-token"})
        assert leaked.status_code == 200
        body = leaked.json()
        assert body["counts"]["events"] == 0  # no acme events for 'otherco'
        capabilities = body["capabilities"]
        assert isinstance(capabilities, dict)
        payload = capabilities["skills"]
        assert isinstance(payload, dict)
        assert payload["items"] == []  # nobody else's graph, either
    finally:
        cross.close()


def test_rest_export_matches_the_served_point_reads() -> None:
    engine, ledger = _world_state()
    service = ExportService(engine, ledger)
    body = service.export(_principal(), UserRef(_T, UserKey("user-1")))
    point_read = engine.snapshot(_T, UserKey("user-1"), Capability("skills")).payload
    capabilities = body["capabilities"]
    assert isinstance(capabilities, dict)
    assert capabilities["skills"] == point_read  # union of point reads, no shadow format
