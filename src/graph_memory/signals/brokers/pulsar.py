"""Pulsar adapter (M2): duck-typed reader, shared ingest core, kit stamping.

Consumer library integration: pass any object with ``receive() ->
list[(topic, str)]``. Outbound stamping (ordering key = user) is available
for relay producers.
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


class PulsarReceiver(Protocol):
    """Minimal reader surface (pulsar-client readers adapt trivially)."""

    def receive(self) -> list[tuple[str, str]]: ...


class PulsarIngester:
    """Drains Pulsar topics into the ledger, tenant-routed by topic name."""

    def __init__(self, consumer: PulsarReceiver, ingestor: BrokerIngestor) -> None:
        self._consumer = consumer
        self._ingestor = ingestor

    async def drain(self) -> dict[str, BatchOutcome]:
        frames = self._consumer.receive()
        messages = [
            BrokerMessage(broker="pulsar", topic=topic, payload=payload)
            for topic, payload in frames
        ]
        return self._ingestor.deliver(messages)


def outgoing_attributes(envelope: Envelope) -> Mapping[str, str]:
    """Publish-side rule: ordering key = partition key (user)."""
    return stamping_for_broker("pulsar", envelope)
