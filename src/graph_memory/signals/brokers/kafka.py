"""Kafka adapter (M2): duck-typed consumer, shared ingest core, kit stamping.

Client library integration: pass any object with ``poll() ->
list[(topic, str)]`` (aiokafka/kafka-python wrappers); the adapter itself
uses no vendor imports, so conformance runs on fakes and deployments swap in
real consumers with zero code change. Outbound stamping (per SPEC/kit rules:
record key = user) is available for relay producers.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from graph_memory.signals.brokers.base import (
    BrokerIngestor,
    BrokerMessage,
    stamping_for_broker,
)
from graph_memory.signals.envelope import Envelope
from graph_memory.signals.service import BatchOutcome


class KafkaPoller(Protocol):
    """Minimal consumer surface (aiokafka/kafka-python adapt trivially)."""

    def poll(self) -> list[tuple[str, str]]: ...


class KafkaIngester:
    """Drains Kafka topics into the ledger, tenant-routed by topic name."""

    def __init__(self, consumer: KafkaPoller, ingestor: BrokerIngestor) -> None:
        self._consumer = consumer
        self._ingestor = ingestor

    async def drain(self) -> dict[str, BatchOutcome]:
        frames = self._consumer.poll()
        messages = [
            BrokerMessage(broker="kafka", topic=topic, payload=payload) for topic, payload in frames
        ]
        return self._ingestor.deliver(messages)


def outgoing_attributes(envelope: Envelope) -> Mapping[str, str]:
    """Publish-side rule: record key = user (per-user partition ordering)."""
    return stamping_for_broker("kafka", envelope)
