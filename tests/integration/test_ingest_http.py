"""HTTP ingest edge tests — httpx ASGI transport, no server process needed."""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass

import pytest
from httpx import ASGITransport, AsyncClient

from graph_memory.core.types import Scope, TenantId
from graph_memory.settings import Settings
from graph_memory.signals.api import BatchSignalsResponse, create_app
from graph_memory.signals.ledger import InMemoryLedger, LedgerRepository
from graph_memory.signals.service import PoisonSink
from graph_memory.tenancy.guard import StaticBearerGuard


class RecordingPoisonSink:
    """Test double for the DLQ; assertions read recorded entries."""

    def __init__(self) -> None:
        self.entries: list[tuple[str, str, str]] = []

    def record(self, tenant: TenantId, payload: str, reason: str) -> None:
        self.entries.append((tenant, payload, reason))


@dataclass
class Edge:
    """Everything a test needs: the client, the ledger beneath it, the DLQ."""

    client: AsyncClient
    ledger: LedgerRepository
    poison: RecordingPoisonSink | None = None


def _signal(event_id: str, **overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "specversion": "1.0",
        "id": event_id,
        "source": "https://app.example.com/signals",
        "type": "itf.skill_assessed.v1",
        "time": "2026-06-01T12:00:00.123Z",
        "partitionkey": "user-1",
        "data": {"skill": "rust", "score": 0.8},
    }
    payload.update(overrides)
    for key, value in list(payload.items()):
        if value is None:
            del payload[key]
    return payload


_ACME = {"Authorization": "Bearer dev-acme-token"}
_OTHER = {"Authorization": "Bearer dev-other-token"}
_BATCH = "/v1/signals:batch"


def _client(
    ledger: LedgerRepository,
    guard: StaticBearerGuard,
    poison: PoisonSink | None = None,
) -> AsyncClient:
    app = create_app(ledger, guard, settings=Settings(max_batch_events=5), poison=poison)
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


@pytest.fixture
def edge() -> Iterator[Edge]:
    guard = StaticBearerGuard(
        {"dev-acme-token": "acme", "dev-other-token": "other"},
        scopes=frozenset({Scope.SIGNALS_WRITE, Scope.CONTEXT_READ}),
    )
    ledger = InMemoryLedger()
    yield Edge(
        client=_client(ledger, guard, RecordingPoisonSink()),
        ledger=ledger,
        poison=None,
    )


@pytest.fixture
def poisoned_edge() -> Iterator[Edge]:
    guard = StaticBearerGuard({"dev-acme-token": "acme"})
    ledger = InMemoryLedger()
    sink = RecordingPoisonSink()
    yield Edge(client=_client(ledger, guard, sink), ledger=ledger, poison=sink)


async def test_first_batch_accepted_then_replay_duplicates(edge: Edge) -> None:
    payload = {"signals": [_signal("a"), _signal("b", partitionkey="user-2")]}
    first = await edge.client.post(_BATCH, json=payload, headers=_ACME)
    assert first.status_code == 200
    body = BatchSignalsResponse.model_validate(first.json())
    assert (body.accepted, body.duplicates, body.rejected) == (2, 0, [])

    replay = await edge.client.post(_BATCH, json=payload, headers=_ACME)
    replayed = BatchSignalsResponse.model_validate(replay.json())
    assert (replayed.accepted, replayed.duplicates) == (0, 2)
    assert len(edge.ledger.read_events(TenantId("acme"))) == 2


async def test_invalid_item_is_rejected_not_failed(poisoned_edge: Edge) -> None:
    response = await poisoned_edge.client.post(
        _BATCH, json={"signals": [_signal("a"), {"specversion": "0.3"}]}, headers=_ACME
    )
    assert response.status_code == 200
    batch = BatchSignalsResponse.model_validate(response.json())
    assert (batch.accepted, len(batch.rejected)) == (1, 1)
    item = batch.rejected[0]
    assert item.index == 1
    assert item.error["error"] == "envelope_invalid"
    assert [entry[2] for entry in (poisoned_edge.poison or RecordingPoisonSink()).entries] == [
        "envelope_invalid"
    ]
    assert len(poisoned_edge.ledger.read_events(TenantId("acme"))) == 1


async def test_oversized_poison_payload_is_stored_bounded(poisoned_edge: Edge) -> None:
    bloated = json.dumps({"blob": "x" * 80_000})
    invalid = _signal("a")
    invalid["data"] = {"blob": bloated}
    response = await poisoned_edge.client.post(_BATCH, json={"signals": [invalid]}, headers=_ACME)
    batch = BatchSignalsResponse.model_validate(response.json())
    assert len(batch.rejected) == 1
    sink = poisoned_edge.poison
    assert sink is not None
    assert len(sink.entries) == 1
    tenant, stored_payload, reason = sink.entries[0]
    assert tenant == TenantId("acme")
    assert reason == "envelope_invalid"
    assert len(stored_payload) <= 4_096  # bounded DLQ entry


async def test_cross_tenant_claim_rejected_but_honest_body_accepted(poisoned_edge: Edge) -> None:
    forged = _signal("a", tenantid="other")
    response = await poisoned_edge.client.post(_BATCH, json={"signals": [forged]}, headers=_ACME)
    batch = BatchSignalsResponse.model_validate(response.json())
    assert batch.accepted == 0
    assert batch.rejected[0].error["error"] == "tenant_mismatch"
    assert [entry[2] for entry in (poisoned_edge.poison or RecordingPoisonSink()).entries] == [
        "tenant_mismatch"
    ]

    honest = _signal("b", tenantid="acme")
    ok = await poisoned_edge.client.post(_BATCH, json={"signals": [honest]}, headers=_ACME)
    ok_batch = BatchSignalsResponse.model_validate(ok.json())
    assert ok_batch.accepted == 1
    assert len(poisoned_edge.ledger.read_events(TenantId("acme"))) == 1


async def test_batch_over_budget_refused_whole(edge: Edge) -> None:
    too_many = {"signals": [_signal(str(i)) for i in range(6)]}
    response = await edge.client.post(_BATCH, json=too_many, headers=_ACME)
    assert response.status_code == 429
    detail = response.json()["detail"]
    assert detail["error"] == "quota_exceeded"
    assert edge.ledger.read_events(TenantId("acme")) == []


async def test_unknown_and_missing_credentials_are_401(edge: Edge) -> None:
    no_header = await edge.client.post(_BATCH, json={"signals": []})
    assert no_header.status_code == 401
    assert no_header.json()["detail"]["error"] == "scope_denied"
    assert no_header.json()["detail"]["details"]["reason"] == "unknown_credential"

    bogus = await edge.client.post(
        _BATCH, json={"signals": []}, headers={"Authorization": "Bearer nope"}
    )
    assert bogus.status_code == 401


async def test_limited_scope_principal_is_403() -> None:
    guard = StaticBearerGuard({"dev-acme-token": "acme"}, scopes=frozenset({Scope.CONTEXT_READ}))
    ledger = InMemoryLedger()
    client = _client(ledger, guard)
    response = await client.post(_BATCH, json={"signals": []}, headers=_ACME)
    assert response.status_code == 403
    assert response.json()["detail"]["error"] == "scope_denied"


async def test_healthz_is_open() -> None:
    guard = StaticBearerGuard({"dev-acme-token": "acme"})
    client = _client(InMemoryLedger(), guard)
    response = await client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
