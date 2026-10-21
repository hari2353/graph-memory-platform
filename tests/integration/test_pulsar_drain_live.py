"""Whole-loop live gate: Pulsar (real broker) -> drain -> ClickHouse ledger (``-m live``).

The dev broker from ``ops/compose/pulsar.yaml`` is produced into through the
image's own client (ordering key = partition key, base64 payload per the CLI
framing contract), then drained with the exact composition the
``platform drain-brokers`` command runs. The ClickHouse ledger is the single
ingest truth (dedup owner); re-drain is forward-only and harmless.
"""

from __future__ import annotations

import asyncio
import base64
import json
import subprocess
import uuid

import httpx
import pytest

from graph_memory.core.types import TenantId, UserKey
from graph_memory.settings import Settings
from graph_memory.signals.brokers import BrokerIngestor, topic_for
from graph_memory.signals.brokers.pulsar import PulsarIngester
from graph_memory.signals.brokers.pulsar_cli import PulsarCliReceiver
from graph_memory.signals.ledger import ClickHouseLedger, LedgerPoisonSink

_ENVELOPE_SHELL = {
    "specversion": "1.0",
    "source": "https://live.example.com/broker",
    "type": "itf.skill_assessed.v1",
}


def _envelope(event_id: str, user: str) -> str:
    return json.dumps(
        {
            **_ENVELOPE_SHELL,
            "id": event_id,
            "time": "2026-10-21T06:00:00.000Z",
            "partitionkey": user,
            "data": {"skill": f"rust-{user}"},
        },
        separators=(",", ":"),
    )


def _produce(topic: str, user: str, payload: str) -> None:
    encoded = base64.b64encode(payload.encode("utf-8")).decode("ascii")
    produced = subprocess.run(
        [
            "docker",
            "exec",
            "gmp-dev-pulsar",
            "/pulsar/bin/pulsar-client",
            "produce",
            "-k",
            user,
            "-m",
            encoded,
            topic,
        ],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if produced.returncode != 0 or "successfully produced" not in produced.stdout:
        pytest.skip(f"dev broker not usable for produce: {produced.stderr[:200]}")


def _delete_topic(topic: str) -> None:
    subprocess.run(
        [
            "docker",
            "exec",
            "gmp-dev-pulsar",
            "/pulsar/bin/pulsar-admin",
            "topics",
            "delete",
            topic,
        ],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


@pytest.mark.live
def test_pulsar_drain_lands_in_the_clickhouse_ledger() -> None:
    settings = Settings()
    admin = httpx.Client(timeout=10.0)
    try:
        probe = admin.get(f"{settings.pulsar_admin_url}/admin/v2/brokers/standalone")
        probe.raise_for_status()
    except Exception:  # noqa: BLE001 - broker unreachable equals tier skip
        pytest.skip("Pulsar dev broker not reachable (compose up) - skipping live drain")

    tenant = TenantId(f"pulsar-live-{uuid.uuid4().hex[:8]}")
    topic = str(topic_for(tenant))
    http_client: httpx.Client | None = None
    try:
        _produce(topic, "user-1", _envelope("pl-a", "user-1"))
        _produce(topic, "user-1", _envelope("pl-b", "user-1"))
        _produce(topic, "user-2", _envelope("pl-c", "user-2"))

        ledger = ClickHouseLedger(settings)
        ingestor = BrokerIngestor(ledger, poison=LedgerPoisonSink(ledger))
        http_client = httpx.Client(timeout=15.0)
        receiver = PulsarCliReceiver(settings, client=http_client, max_messages=64)

        outcomes = asyncio.run(PulsarIngester(receiver, ingestor).drain())
        regarding = outcomes[str(tenant)].result
        assert regarding.accepted == 3

        rows = ledger.read_events(tenant)
        assert len(rows) == 3
        assert {row.user_key for row in rows} == {UserKey("user-1"), UserKey("user-2")}

        # re-drain: the durable subscription is forward-only; ledger unaffected
        outcomes_again = asyncio.run(PulsarIngester(receiver, ingestor).drain())
        again = outcomes_again.get(str(tenant))
        if again is not None:
            assert again.result.accepted == 0
        assert len(ledger.read_events(tenant)) == 3
    finally:
        if http_client is not None:
            http_client.close()
        _delete_topic(topic)
