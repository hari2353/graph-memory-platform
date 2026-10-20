"""NATS JetStream adapter (M2): duck-typed subscription, shared core.

Consumer library integration: pass any object with ``fetch() ->
list[(subject, str)]``. Publish-side rules (``Nats-Msg-Id`` = dedup key for
server-side duplicate suppression; subject carries the user) are available
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


class NatsFetcher(Protocol):
    """Minimal pull-subscription surface (nats-py JetStream adapts trivially)."""

    def fetch(self) -> list[tuple[str, str]]: ...


class NatsIngester:
    """Drains NATS subjects into the ledger, tenant-routed by subject name."""

    def __init__(self, consumer: NatsFetcher, ingestor: BrokerIngestor) -> None:
        self._consumer = consumer
        self._ingestor = ingestor

    async def drain(self) -> dict[str, BatchOutcome]:
        frames = self._consumer.fetch()
        messages = [
            BrokerMessage(broker="nats", topic=subject, payload=payload)
            for subject, payload in frames
        ]
        return self._ingestor.deliver(messages)


def outgoing_attributes(envelope: Envelope) -> Mapping[str, str]:
    """Publish-side rule: Nats-Msg-Id = dedup key (idempotent JetStream publish)."""
    return stamping_for_broker("nats", envelope)
