"""REST channel observability (SPEC §11/R7): one span per request with
per-stage wall times — emitted sampled or not — plus metering ticks; the
InMemorySpanSink keeps this fully offline while the live tier proves the CH
sink's transport.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

from fastapi.testclient import TestClient

from graph_memory.channels.rest.api import create_rest_app
from graph_memory.context.service import ContextCache, MemoryContextProvider
from graph_memory.core.clock import FixedClock
from graph_memory.core.types import TenantId
from graph_memory.mapping.spec import load_spec
from graph_memory.observability.metering import MeteringCounters
from graph_memory.observability.observer import InMemorySpanSink
from graph_memory.projector.engine import Projector
from graph_memory.projector.gateway import InMemoryGraphGateway
from graph_memory.signals.envelope import Envelope
from graph_memory.signals.ledger import InMemoryLedger, record_from_envelope
from graph_memory.tenancy.guard import StaticBearerGuard
from graph_memory.tenancy.quotas import QuotaGuard, Quotas

_SPEC_BODY = {
    "mapping_version": 1,
    "signals": {
        "itf.skill_assessed.v1": {
            "edges": [
                {
                    "kind": "HAS_SKILL",
                    "target": "skill:{data.skill}",
                    "weight": 1.0,
                    "half_life_days": 365,
                }
            ]
        }
    },
    "capabilities": {"skills": {"relations": ["HAS_SKILL"], "top_k": 5, "ttl_ms": 60000}},
}


def _app(
    *,
    sink: InMemorySpanSink | None = None,
    metering: MeteringCounters | None = None,
    quota: QuotaGuard | None = None,
) -> TestClient:
    from datetime import datetime

    spec = load_spec(json.dumps(_SPEC_BODY))
    ledger = InMemoryLedger()
    ledger.append(
        [
            record_from_envelope(
                TenantId("acme"),
                Envelope.parse(
                    {
                        "specversion": "1.0",
                        "id": "rest-span-1",
                        "source": "https://app.example.com/signals",
                        "type": "itf.skill_assessed.v1",
                        "time": "2026-06-01T12:00:00.100Z",
                        "partitionkey": "user-1",
                        "data": {"skill": "rust"},
                    }
                ),
            )
        ]
    )
    engine = Projector(
        spec, ledger, InMemoryGraphGateway(), FixedClock(datetime(2026, 6, 10, tzinfo=UTC))
    )
    engine.replay()
    app = create_rest_app(
        MemoryContextProvider(engine, ContextCache()),
        StaticBearerGuard({"dev-acme-token": "acme"}),
        quota=quota,
        sink=sink,
        metering=metering,
    )
    return TestClient(app)


def test_ok_read_emits_one_span_with_stages_and_ticks_metering() -> None:
    sink = InMemorySpanSink()
    metering = MeteringCounters()
    client = _app(sink=sink, metering=metering)
    response = client.get(
        "/v1/context/user-1/skills", headers={"Authorization": "Bearer dev-acme-token"}
    )
    assert response.status_code == 200
    assert len(sink) == 1
    span = list(iter(sink))[0]
    assert span.name == "gmp.channel.rest.request"
    assert str(span.tenant) == "acme"
    stages_attr = span.attributes.get("stages_ms")
    stages = stages_attr if isinstance(stages_attr, dict) else {}
    # The admission stage exists only when a quota guard is wired.
    assert {"authn", "lookup_assemble"} <= {str(key) for key in stages}
    assert all(isinstance(value, float) for value in stages.values())
    assert span.attributes["outcome"] == "success"
    drained = metering.drain()
    assert drained == {("acme", "rest", "requests"): 1}


def test_bad_credential_emits_failure_span_without_metering_request() -> None:
    sink = InMemorySpanSink()
    metering = MeteringCounters()
    client = _app(sink=sink, metering=metering)
    response = client.get("/v1/context/user-1/skills", headers={"Authorization": "Bearer nope"})
    assert response.status_code == 401
    span = list(iter(sink))[0]
    assert span.attributes["outcome"] == "scope_denied"
    assert metering.drain() == {}


def test_unwired_sink_and_metering_are_not_required() -> None:
    client = _app()
    response = client.get(
        "/v1/context/user-1/skills", headers={"Authorization": "Bearer dev-acme-token"}
    )
    assert response.status_code == 200


def test_quota_throttle_ticks_both_windows() -> None:
    metering = MeteringCounters()
    # A dry burst leaves no tokens: the second read is a 429 within the same
    # instant (the clock never advances in this test).
    quota = QuotaGuard(
        Quotas(interactive_rps=0.0, interactive_burst=1.0),
        FixedClock(datetime(2026, 6, 10, 12, 0, tzinfo=UTC)),
    )
    client = _app(metering=metering, quota=quota)
    first = client.get(
        "/v1/context/user-1/skills", headers={"Authorization": "Bearer dev-acme-token"}
    )
    second = client.get(
        "/v1/context/user-1/skills", headers={"Authorization": "Bearer dev-acme-token"}
    )
    assert first.status_code == 200
    assert second.status_code == 429
    drained = metering.drain()
    assert drained.get(("acme", "rest", "requests")) == 1
    assert drained.get(("acme", "rest", "throttled")) == 1
