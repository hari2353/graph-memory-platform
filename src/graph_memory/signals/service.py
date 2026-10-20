"""The ingest service: raw request payloads -> ledger records.

Rules (SPEC §5):
- Batch bounded (event count + per-payload bytes) — an over-budget batch is
  refused whole, before anything lands.
- Per-item adjudication: envelope violations and cross-tenant claims are
  REJECTED ITEMS (never 500s); their raw payloads go to the poison sink
  (DLQ bounded — oversized payloads stored truncated).
- Successful items append idempotently; duplicates are acks, never errors.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol, cast

from graph_memory.core.errors import EnvelopeError, PlatformError, QuotaExceeded, TenantMismatch
from graph_memory.core.logsafe import log_safe
from graph_memory.core.types import IngestResult, Json, Principal, TenantId
from graph_memory.settings import Settings
from graph_memory.signals.envelope import Envelope
from graph_memory.signals.ledger import LedgerRecord, LedgerRepository, record_from_envelope

_DLQ_MAX_PAYLOAD = 4_096  # poison entries stay bounded (SPEC §17)


class PoisonSink(Protocol):
    """Where rejected raw payloads go; retention is bounded by the backend."""

    def record(self, tenant: TenantId, payload: str, reason: str) -> None: ...


class NullPoisonSink:
    """Dev/no-DLQ backend stand-in — drops poison after returning counts."""

    def record(self, tenant: TenantId, payload: str, reason: str) -> None:
        _ = tenant, payload, reason  # acknowledged-but-dropped


@dataclass(frozen=True)
class RejectedSignal:
    """One adjudicated-and-refused item, wire-shapeable."""

    index: int
    error: dict[str, Json]


@dataclass(frozen=True)
class BatchOutcome:
    """What one batch did: counts, plus per-item refusals."""

    result: IngestResult
    rejected: tuple[RejectedSignal, ...]


class IngestService:
    """Per-request, tenant-bound ingest; constructed per principal (cheap)."""

    def __init__(
        self,
        principal: Principal,
        ledger: LedgerRepository,
        *,
        settings: Settings | None = None,
        poison: PoisonSink | None = None,
    ) -> None:
        self._principal = principal
        self._ledger = ledger
        self._poison: PoisonSink = poison if poison is not None else NullPoisonSink()
        bounds = settings or Settings()
        self._max_batch = bounds.max_batch_events
        self._max_payload_bytes = bounds.max_payload_bytes

    def apply_raw(self, raw_events: Sequence[object]) -> BatchOutcome:
        """Adjudicate every item, store the survivors idempotently."""
        if len(raw_events) > self._max_batch:
            raise QuotaExceeded(
                "batch exceeds the event-count budget",
                details={"limit": self._max_batch, "got": len(raw_events)},
            )
        records: list[LedgerRecord] = []
        rejected: list[RejectedSignal] = []
        for index, raw in enumerate(raw_events):
            match self._adjudicate(raw):
                case (Envelope() as envelope, None):
                    records.append(record_from_envelope(self._principal.tenant, envelope))
                case (None, PlatformError() as failure):
                    rejected.append(RejectedSignal(index=index, error=failure.to_payload()))
                case _:  # pragma: no cover — _adjudicate returns exactly one candidate
                    raise AssertionError("adjudicator returned an impossible pair")
        result = self._ledger.append(records)
        return BatchOutcome(result=result, rejected=tuple(rejected))

    def _adjudicate(self, raw: object) -> tuple[Envelope, None] | tuple[None, PlatformError]:
        """Validate one raw item; on failure, poison it and report a payload.
        Exactly one of the pair is populated."""
        try:
            payload_bytes = _payload_bytes(raw)
            if payload_bytes > self._max_payload_bytes:
                raise EnvelopeError(
                    "payload exceeds the intake budget",
                    details={"limit": self._max_payload_bytes, "got": payload_bytes},
                )
            decoded: Mapping[str, object] = (
                json.loads(raw) if isinstance(raw, str) else cast("Mapping[str, object]", raw)
            )
            envelope = Envelope.parse(decoded)
            claimed = envelope.tenantid
            if claimed is not None and TenantId(claimed) != self._principal.tenant:
                raise TenantMismatch(
                    "envelope tenant claim must match the authenticated tenant",
                    details={"scope": "cross_tenant_ingest"},
                )
            return envelope, None
        except (EnvelopeError, TenantMismatch, QuotaExceeded) as failure:
            self._poison.record(
                self._principal.tenant,
                log_safe(_raw_text(raw), max_len=_DLQ_MAX_PAYLOAD),
                failure.code,
            )
            return None, failure


def _payload_bytes(raw: object) -> int:
    return len(raw.encode()) if isinstance(raw, str) else len(json.dumps(raw).encode())


def _raw_text(raw: object) -> str:
    return raw if isinstance(raw, str) else json.dumps(raw)
