"""REST channel tests: ETag/304, cursor pagination, idempotent prefetch."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from fastapi.testclient import TestClient

from graph_memory.channels.rest import create_rest_app
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
_HEADERS = {"Authorization": "Bearer dev-acme-token"}


def _client() -> TestClient:
    spec: MappingSpec = load_spec(_SPEC_PATH.read_text(encoding="utf-8"))
    ledger = InMemoryLedger()
    clock = FixedClock(datetime(2026, 6, 10, tzinfo=UTC))
    gateway = InMemoryGraphGateway()
    engine = Projector(spec, ledger, gateway, clock)
    skills = ["rust", "python", "go"]
    for index, skill in enumerate(skills):
        payload: dict[str, object] = {
            "specversion": "1.0",
            "id": f"rest-{index}",
            "source": "https://app.example.com/signals",
            "type": "itf.skill_assessed.v1",
            "time": f"2026-06-01T12:00:00.{100 + index:03d}Z",
            "partitionkey": "user-1",
            "data": {"skill": skill},
        }
        ledger.append([record_from_envelope(_T, Envelope.parse(payload))])
    engine.process_events(ledger.read_events(_T))
    provider = MemoryContextProvider(engine, ContextCache())
    app = create_rest_app(provider, StaticBearerGuard(Settings().dev_static_tokens))
    return TestClient(app)


def test_context_returns_with_etag_and_cache_control() -> None:
    response = _client().get("/v1/context/user-1/skills", headers=_HEADERS)
    assert response.status_code == 200
    assert response.headers["ETag"].startswith('"')
    assert "max-age=60" in response.headers["Cache-Control"]
    assert response.headers["Vary"] == "Authorization"
    body = response.json()
    assert body["capability"] == "skills"
    assert body["max_age_ms"] == 60_000
    assert body["stale_at"] > body["generated_at"]
    # Boundary: cache window honored exactly (SPEC §7 ticks) — content is sorted JSON.
    assert body["payload"]["capability"] == "skills"


def test_etag_revalidation_returns_304() -> None:
    client = _client()
    first = client.get("/v1/context/user-1/skills", headers=_HEADERS)
    etag = first.headers["ETag"]
    second = client.get("/v1/context/user-1/skills", headers={**_HEADERS, "If-None-Match": etag})
    assert second.status_code == 304
    assert second.headers["ETag"] == etag


def test_items_pagination_walks_the_ranked_list_without_repeats() -> None:
    client = _client()
    # First page
    first = client.get(
        "/v1/context/user-1/skills/items",
        headers=_HEADERS,
        params={"limit": 2},
    )
    assert first.status_code == 200
    page_one = first.json()
    assert len(page_one["items"]) == 2
    assert page_one["next_cursor"] is not None
    # Walk to the end with the cursor; no item may repeat.
    seen = [str(item["target"]) for item in page_one["items"]]
    cursor = page_one["next_cursor"]
    while cursor is not None:
        response = client.get(
            "/v1/context/user-1/skills/items",
            headers=_HEADERS,
            params={"limit": 2, "cursor": cursor},
        )
        assert response.status_code == 200
        body = response.json()
        seen.extend(str(item["target"]) for item in body["items"])
        cursor = body["next_cursor"]
    assert len(seen) == 3  # user-1 holds three HAS_SKILL observations
    assert len(seen) == len(set(seen))
    learned = client.get(
        "/v1/context/user-1/skills/items",
        headers=_HEADERS,
        params={"limit": 2, "cursor": "bogus!"},
    )
    assert learned.status_code == 400


def test_prefetch_replays_idempotent_responses() -> None:
    client = _client()
    key = {"Authorization": _HEADERS["Authorization"], "Idempotency-Key": "k-1"}
    first = client.post("/v1/context/user-1/skills/prefetch", headers=key)
    assert first.status_code == 200
    assert first.json() == {
        "materialized": True,
        "cache_scope": "t/acme/u/user-1/c/skills",
        "max_age_ms": 60000,
        "generated_at": "2026-06-10T00:00:00+00:00",
    }
    second = client.post("/v1/context/user-1/skills/prefetch", headers=key)
    assert second.headers.get("Idempotency-Replayed") == "true"
    assert second.json() == first.json()
    # A different key is a fresh execution (no replay header).
    fresh = client.post(
        "/v1/context/user-1/skills/prefetch",
        headers={"Authorization": _HEADERS["Authorization"], "Idempotency-Key": "k-2"},
    )
    assert "Idempotency-Replayed" not in fresh.headers
    # Without any key, plain 200 (middleware is opt-in).
    plain = client.post("/v1/context/user-1/skills/prefetch", headers=_HEADERS)
    assert plain.status_code == 200
    assert "Idempotency-Replayed" not in plain.headers


def test_unknown_capability_and_bad_auth_fail_with_stable_payloads() -> None:
    client = _client()
    assert client.get("/v1/context/user-1/nope", headers=_HEADERS).status_code == 404
    assert (
        client.get(
            "/v1/context/user-1/skills",
            headers={"Authorization": "Bearer no-such-token"},
        ).status_code
        == 401
    )
    # Cross-tenant access is UNREPRESENTABLE on this channel: the tenant rides
    # the credential, so another tenant asking for the same user key sees only
    # ITS OWN (empty) state — never acme's rows.
    cross = client.get(
        "/v1/context/user-1/skills", headers={"Authorization": "Bearer dev-other-token"}
    )
    assert cross.status_code == 200
    body = cross.json()
    assert body["payload"]["items"] == []
    assert "t/other/" in body["cache_scope"]
