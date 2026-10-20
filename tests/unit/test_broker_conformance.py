"""Broker conformance (spikit): order kept, dups absorbed, poison captured.

Every M2 adapter — Kafka, Pulsar, NATS — runs the same honest drain path
against a duck-typed fake transport seeded with the kit fixtures: interleaved
users, injected duplicates, and one poison payload. Real client libraries are
deployment-edge substitutions; the conformance surface is identical.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Literal, cast

import pytest

from graph_memory.core.types import TenantId
from graph_memory.signals.brokers import BrokerIngestor, tenant_from_topic, topic_for
from graph_memory.signals.brokers.kafka import KafkaIngester
from graph_memory.signals.brokers.kafka import outgoing_attributes as kafka_stamp
from graph_memory.signals.brokers.nats import NatsIngester
from graph_memory.signals.brokers.nats import outgoing_attributes as nats_stamp
from graph_memory.signals.brokers.pulsar import PulsarIngester
from graph_memory.signals.brokers.pulsar import outgoing_attributes as pulsar_stamp
from graph_memory.signals.envelope import Envelope
from graph_memory.signals.ledger import InMemoryLedger
from graph_memory.spikit import TENANT, assert_per_user_order, kit_envelopes, stamping

_TOPIC = topic_for(TENANT)  # gmp.signals.kit


def _wire(envelope: Envelope) -> str:
    """Exact wire bytes an adapter receives: the envelope's own JSON."""
    return json.dumps(envelope.model_dump(mode="json"))


def _seeded_frames() -> list[tuple[str, str]]:
    """Kit fixtures + two mid-stream duplicates + one poison payload."""
    kit = kit_envelopes()
    frames = [(_TOPIC, _wire(envelope)) for envelope in kit]
    frames.insert(2, (_TOPIC, _wire(kit[0])))  # duplicate of u9-a
    frames.insert(5, (_TOPIC, _wire(kit[4])))  # duplicate of u1-b
    frames.append((_TOPIC, "{not json"))  # poison: recorded, never dropped
    return frames


class PoisonRecorder:
    """Capture-everything poison sink (kit rule: poison must be retained)."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    def record(self, tenant: TenantId, payload: str, reason: str) -> None:
        self.calls.append((str(tenant), payload, reason))


class FakeTransport:
    """Duck-typed consumer: same surface all three adapters poll."""

    def __init__(self, frames: list[tuple[str, str]]) -> None:
        self._frames = frames

    def poll(self) -> list[tuple[str, str]]:
        return list(self._frames)

    receive = poll
    fetch = poll


@pytest.mark.parametrize("broker", ["kafka", "pulsar", "nats"])
async def test_adapter_conformance(broker: str) -> None:
    ledger = InMemoryLedger()
    poison = PoisonRecorder()
    ingestor = BrokerIngestor(ledger, poison=poison)
    frames = FakeTransport(_seeded_frames())
    # All three adapters expose the same drain() surface.
    adapter: KafkaIngester | PulsarIngester | NatsIngester
    match broker:
        case "kafka":
            adapter = KafkaIngester(frames, ingestor)
        case "pulsar":
            adapter = PulsarIngester(frames, ingestor)
        case _:
            adapter = NatsIngester(frames, ingestor)
    outcomes = await adapter.drain()
    outcome = outcomes[str(TENANT)]
    assert outcome.result.accepted == 6
    assert outcome.result.duplicates == 2
    assert len(outcome.rejected) == 1
    assert outcome.rejected[0].error["error"] == "envelope_invalid"
    assert poison.calls and poison.calls[0][2] == "envelope_invalid"
    # The mandatory rule: per-user sequence preserved through the broker hop.
    assert_per_user_order([record.envelope for record in ledger.read_events(TENANT)])


@pytest.mark.parametrize(
    ("module_stamp", "name"),
    [(kafka_stamp, "kafka"), (pulsar_stamp, "pulsar"), (nats_stamp, "nats")],
)
def test_outbound_stamping_matches_the_kit(
    module_stamp: Callable[[Envelope], object], name: str
) -> None:
    envelope = kit_envelopes()[0]
    stamp_target = cast('Literal["kafka", "pulsar", "nats"]', name)
    assert module_stamp(envelope) == stamping(envelope, stamp_target)


def test_topic_roundtrip_and_tenant_routing() -> None:
    assert tenant_from_topic("gmp.signals.acme") == TenantId("acme")
    assert topic_for("acme") == "gmp.signals.acme"
