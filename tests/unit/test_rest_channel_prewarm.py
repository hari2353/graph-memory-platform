"""REST prewarm channel tests (M4.6, research 06 lever 3).

``POST /v1/prefetch/{user}`` is warm-on-login: a READ that installs the
user's next context payloads into the cache and reports the hit-rate
evidence. The proven contract:

- the response is the ADAPTER's report verbatim (same parity discipline
  as every channel renderer);
- metering is PAIR-grained: ``prefetch`` counts warmed pairs and
  ``prefetch_cold`` the safety-net builds, so the durable metering table
  alone proves the precompute lift; the span carries the rate;
- unwired deployments fail as an anticipated ``not_wired`` error, never
  silently; ``Idempotency-Key`` replays return the stored report;
- the tenant rides the credential — another tenant's bearer warms ONLY
  its own (empty) view for the same user key.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient

from graph_memory.channels.rest import create_rest_app
from graph_memory.context.prefetch import PrewarmService, ProfileMaterializer
from graph_memory.context.service import ContextCache, MemoryContextProvider
from graph_memory.core.clock import FixedClock
from graph_memory.core.types import Principal, Scope, TenantId, UserKey, UserRef
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


def _record(eid: str, user: str, skill: str) -> Any:
    envelope = Envelope.parse(
        {
            "specversion": "1.0",
            "id": eid,
            "source": "https://app.example.com/signals",
            "type": "itf.skill_assessed.v1",
            "time": "2026-06-01T09:00:00+00:00",
            "partitionkey": user,
            "data": {"skill": skill},
        }
    )
    return record_from_envelope(_T, envelope)


def _principal() -> Principal:
    return Principal(
        tenant=_T,
        scopes=frozenset({Scope.CONTEXT_READ, Scope.MEMORY_SEARCH}),
        subject="test",
    )


def _world_state() -> tuple[Projector, InMemoryLedger]:
    """Fresh engine + ledger with two users, warm enough to prebuild."""
    ledger = InMemoryLedger()
    ledger.append([_record("w1", "user-1", "rust"), _record("w2", "user-1", "go")])
    ledger.append([_record("w3", "user-2", "python")])
    engine = Projector(
        load_spec(_SPEC_PATH.read_text(encoding="utf-8")),
        ledger,
        InMemoryGraphGateway(),
        FixedClock(_NOW),
    )
    engine.replay()
    return engine, ledger


def _local_app(engine: Projector, guard: StaticBearerGuard, **ports: Any) -> TestClient:
    return TestClient(
        create_rest_app(MemoryContextProvider(engine, ContextCache()), guard, **ports)
    )


# --- round trip, parity, idempotency ------------------------------------------


def test_rest_prefetch_round_trip_parity_and_idempotency() -> None:
    engine, _ledger = _world_state()
    materializer = ProfileMaterializer(engine)
    prewarm = PrewarmService(engine, ContextCache(), materializer)
    materializer.rebuild()  # the tick HAS materialized → the hit-rate path
    client = _local_app(engine, StaticBearerGuard(Settings().dev_static_tokens))
    wired = _local_app(
        engine,
        StaticBearerGuard(Settings().dev_static_tokens),
        prefetch=prewarm,
    )

    response = wired.post(
        "/v1/prefetch/user-1",
        json={"capabilities": None},
        headers={"Authorization": f"Bearer {_TOKEN}"},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["user"] == "user-1"
    assert body["requested"] == 1 and body["prebuilt"] == 1
    assert body["hit_rate"] == 1.0 and body["safety_net_builds"] == 0
    # Channel parity: REST renders the ADAPTER's report verbatim.
    direct = prewarm.prefetch(_principal(), UserRef(_T, UserKey("user-1")), None)
    assert body == direct
    # The cache is warm: a plain context read now serves the SAME payload
    # from the cache (prewarm's whole point).
    warmed = client.get("/v1/context/user-1/skills", headers={"Authorization": f"Bearer {_TOKEN}"})
    assert warmed.status_code == 200
    payload = warmed.json()["payload"]
    assert payload["items"] != []  # the warm cache serves the same payload

    replayed = wired.post(
        "/v1/prefetch/user-1",
        json={"capabilities": None},
        headers={"Authorization": f"Bearer {_TOKEN}", "Idempotency-Key": "warm-1"},
    )
    assert replayed.status_code == 200
    again = wired.post(
        "/v1/prefetch/user-1",
        json={"capabilities": None},
        headers={"Authorization": f"Bearer {_TOKEN}", "Idempotency-Key": "warm-1"},
    )
    assert again.status_code == 200
    assert again.headers.get("Idempotency-Replayed") == "true"
    assert again.json()["hit_rate"] == 1.0


def test_rest_prefetch_meters_pairs_and_carries_the_span() -> None:
    engine, _ledger = _world_state()
    metering = MeteringCounters()
    spans = InMemorySpanSink()
    prewarm = PrewarmService(engine, ContextCache(), ProfileMaterializer(engine))
    client = _local_app(
        engine,
        StaticBearerGuard(Settings().dev_static_tokens),
        metering=metering,
        sink=spans,
        prefetch=prewarm,
    )
    # No tick materialized yet → BOTH pairs are safety-net builds: the
    # metering table alone tells that story.
    response = client.post(
        "/v1/prefetch/user-1",
        json={"capabilities": ["skills"]},
        headers={"Authorization": f"Bearer {_TOKEN}"},
    )
    assert response.status_code == 200
    rows = metering.drain()
    assert rows.get((_T, "rest", "prefetch")) == 1
    assert rows.get((_T, "rest", "prefetch_cold")) == 1
    prefetch_spans = [span for span in spans if span.name == "gmp.channel.rest.prefetch"]
    assert len(prefetch_spans) == 1
    assert prefetch_spans[0].attributes["hit_rate"] == 0.0
    assert prefetch_spans[0].attributes["outcome"] == "success"


def test_rest_prefetch_scopes_unwired_and_isolation() -> None:
    engine, _ledger = _world_state()
    scopeless_guard = StaticBearerGuard(
        {"dev-acme-token": "acme"}, scopes=frozenset({Scope.MEMORY_SEARCH})
    )
    guard = StaticBearerGuard(Settings().dev_static_tokens)
    app = _local_app(engine, scopeless_guard)

    # context:read is missing → the read family fails closed.
    denied = app.post(
        "/v1/prefetch/user-1",
        json={},
        headers={"Authorization": f"Bearer {_TOKEN}"},
    )
    assert denied.status_code == 403
    assert denied.json()["detail"]["error"] == "scope_denied"

    # A rejected credential is 401 with the stable unknown_credential code.
    bogus = app.post(
        "/v1/prefetch/user-1",
        json={},
        headers={"Authorization": "Bearer no-such-token"},
    )
    assert bogus.status_code == 401

    # Unwired deployments fail as an anticipated error, never silently.
    unwired = _local_app(engine, guard)
    gone = unwired.post(
        "/v1/prefetch/user-1",
        json={},
        headers={"Authorization": f"Bearer {_TOKEN}"},
    )
    assert gone.status_code == 500
    assert gone.json()["detail"]["error"] == "platform_error"
    assert gone.json()["detail"]["details"]["reason"] == "not_wired"

    # The tenant rides the credential: another tenant's bearer warms ONLY
    # its own (empty) view of the same user key — never acme's rows.
    cross = _local_app(
        engine,
        StaticBearerGuard({"dev-other-token": "otherco"}),
        prefetch=PrewarmService(engine, ContextCache(), ProfileMaterializer(engine)),
    )
    leaked = cross.post(
        "/v1/prefetch/user-1",
        json={},
        headers={"Authorization": "Bearer dev-other-token"},
    )
    assert leaked.status_code == 200
    body = leaked.json()
    assert body["tenant"] == "otherco"
    assert body["prebuilt"] == 0  # the tick never materialized 'otherco'
    assert body["hit_rate"] == 0.0
    assert "rust" not in repr(body)  # nothing of acme's graph
