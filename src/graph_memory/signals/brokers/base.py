"""Broker ingest SPI: the shared drain/route/ingest core every adapter uses.

Adapters (Kafka/Pulsar/NATS at M2) stay thin: their client libraries are
lazy-imported at the deployment edge, and each adapter's only job is
1. pull raw frames ``(topic, payload)`` from a duck-typed consumer object,
2. derive the tenant from the topic (``gmp.signals.<tenant>`` — the tenant is
   a routing fact, never a payload claim),
3. hand the group to :class:`BrokerIngestor`, which reuses the ingest
   service's adjudication (envelope checks, idempotency, poison DLQ).

The conformance kit (`graph_memory.spikit`) owns the checks adapters must
pass: per-user order, duplicate absorption, poison capture.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol

from graph_memory.core.types import Principal, Scope, TenantId, tenant_id
from graph_memory.settings import Settings
from graph_memory.signals.envelope import Envelope
from graph_memory.signals.ledger import LedgerRepository
from graph_memory.signals.service import BatchOutcome, IngestService, PoisonSink
from graph_memory.spikit import BrokerName, stamping

_TOPIC_PREFIX = "gmp.signals"


def stamping_for_broker(broker: BrokerName, envelope: Envelope) -> dict[str, str]:
    """Kit-owned stamping rules, exposed under the adapter's own name."""
    return stamping(envelope, broker)


@dataclass(frozen=True)
class BrokerMessage:
    """A drained frame + its routing facts (kept explicit, never inferred twice)."""

    broker: BrokerName
    topic: str
    payload: str
    attributes: Mapping[str, str] = field(default_factory=dict)


def tenant_from_topic(topic: str) -> TenantId:
    """`gmp.signals.acme` / `acme` → tenant id (validated, bounded)."""
    tail = topic.rsplit(".", 1)[-1]
    return tenant_id(tail)


def topic_for(tenant: str | TenantId) -> str:
    """Inverse of :func:`tenant_from_topic` (producers configure topics with it)."""
    return f"{_TOPIC_PREFIX}.{tenant_id(str(tenant))}"


class CredentialFactory(Protocol):
    """Adapter-supplied principal per tenant (JWT/service identity in prod)."""

    def __call__(self, tenant: TenantId) -> Principal: ...


def _default_principal(tenant: TenantId) -> Principal:
    return Principal(
        tenant=tenant,
        scopes=frozenset({Scope.SIGNALS_WRITE}),
        subject=f"broker-adapter:{tenant}",
    )


class BrokerIngestor:
    """Groups drained frames by tenant and runs the adjudicating ingest service."""

    def __init__(
        self,
        ledger: LedgerRepository,
        *,
        credentials: CredentialFactory | None = None,
        settings: Settings | None = None,
        poison: PoisonSink | None = None,
    ) -> None:
        self._ledger = ledger
        self._credentials = credentials or _default_principal
        self._settings = settings
        self._poison = poison

    def ingest_service_for(self, tenant: TenantId) -> IngestService:
        return IngestService(
            self._credentials(tenant),
            self._ledger,
            settings=self._settings,
            poison=self._poison,
        )

    def deliver(self, messages: Sequence[BrokerMessage]) -> dict[str, BatchOutcome]:
        """Batch-per-tenant: every tenant gets one bounded adjudicated batch."""
        grouped: dict[str, list[str]] = {}
        for message in messages:
            grouped.setdefault(str(tenant_from_topic(message.topic)), []).append(message.payload)
        return {
            name: self.ingest_service_for(TenantId(name)).apply_raw(payloads)
            for name, payloads in grouped.items()
        }
