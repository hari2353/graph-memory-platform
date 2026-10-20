"""Broker ingest adapters (M2: Kafka, Pulsar, NATS — SPEC §5/§13)."""

from graph_memory.signals.brokers.base import (
    BrokerIngestor,
    BrokerMessage,
    tenant_from_topic,
    topic_for,
)
from graph_memory.signals.brokers.kafka import KafkaIngester
from graph_memory.signals.brokers.nats import NatsIngester
from graph_memory.signals.brokers.pulsar import PulsarIngester

__all__ = [
    "BrokerIngestor",
    "BrokerMessage",
    "KafkaIngester",
    "NatsIngester",
    "PulsarIngester",
    "tenant_from_topic",
    "topic_for",
]
