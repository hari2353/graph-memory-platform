"""The platform signal envelope: CloudEvents 1.0.2 (JSON format) + extensions.

Conventions (SPEC §5, §18; research 02):
- ``source`` + ``id`` are the dedup identity (``dedup_key`` hashes both, so
  collision and PII leakage through the key are both impossible).
- ``partitionkey`` (the official CloudEvents partitioning extension) carries
  the user key — the per-user ordering knob every broker adapter must map to.
- ``type`` is the platform's namespaced, versioned signal type ``<ns>.<name>.vN``.
- ``time`` is REQUIRED — it feeds ledger ordering and bi-temporal validity.
- ``tenantid`` is an optional platform extension; the ingest API always
  cross-checks it against the authenticated principal's claims tenant.
- Unknown attributes are preserved (``extra="allow"`` → ``extensions``), because
  the ledger stores signals verbatim and forward compatibility is additive.
"""

import hashlib
from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, ValidationInfo, field_validator
from pydantic_core import ValidationError

from graph_memory.core.errors import EnvelopeError
from graph_memory.core.logsafe import log_safe
from graph_memory.core.types import Json, SignalType, signal_type, tenant_id, user_key

CLOUDEVENTS_SPECVERSION = "1.0"
ENVELOPE_VERSION = 1  # platform envelope grammar version; additive-only evolution

MAX_ID_LEN = 256
MAX_SOURCE_LEN = 2048
MAX_DATASCHEMA_LEN = 2048
MAX_SUBJECT_LEN = 256
_DEDUP_SEP = "\x1f"


class Envelope(BaseModel):
    """One validated signal, exactly as the ledger will remember it."""

    model_config = ConfigDict(frozen=True, extra="allow")

    specversion: Literal["1.0"] = "1.0"
    id: str
    source: str
    type: str
    time: datetime
    partitionkey: str
    data: dict[str, Json]
    dataschema: str | None = None
    subject: str | None = None
    tenantid: str | None = None

    @field_validator("id")
    @classmethod
    def _check_id(cls, value: str) -> str:
        if not value or value != value.strip() or len(value) > MAX_ID_LEN:
            raise ValueError(f"id must be trimmed, 1..{MAX_ID_LEN} chars")
        if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in value):
            raise ValueError("id must not contain control characters")
        return value

    @field_validator("source", "dataschema")
    @classmethod
    def _check_uri_reference(cls, value: str | None, info: ValidationInfo) -> str | None:
        if value is None:
            return None
        if not value or len(value) > MAX_SOURCE_LEN:
            raise ValueError(f"{info.field_name} must be 1..{MAX_SOURCE_LEN} chars")
        if any(ch.isspace() or ord(ch) < 0x20 or ord(ch) == 0x7F for ch in value):
            raise ValueError(f"{info.field_name} must be a URI-reference (no whitespace/controls)")
        return value

    @field_validator("type")
    @classmethod
    def _check_type(cls, value: str) -> str:
        signal_type(value)  # namespaced + versioned or reject
        return value

    @field_validator("time")
    @classmethod
    def _check_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("time must carry an explicit UTC offset (RFC 3339)")
        return value.astimezone(UTC)

    @field_validator("partitionkey")
    @classmethod
    def _check_partitionkey(cls, value: str) -> str:
        user_key(value)
        return value

    @field_validator("subject")
    @classmethod
    def _check_subject(cls, value: str | None) -> str | None:
        if value is not None and (
            not value or value != value.strip() or len(value) > MAX_SUBJECT_LEN
        ):
            raise ValueError(f"subject must be trimmed, 1..{MAX_SUBJECT_LEN} chars")
        return value

    @field_validator("tenantid")
    @classmethod
    def _check_tenantid(cls, value: str | None) -> str | None:
        if value is not None:
            tenant_id(value)
        return value

    @property
    def signal(self) -> SignalType:
        """The validated, namespaced signal type."""
        return SignalType(self.type)

    @property
    def occurred_at(self) -> datetime:
        """Instant the signal claims to have happened (UTC-normalized)."""
        return self.time

    @property
    def dedup_key(self) -> str:
        """Collision-proof, PII-free identity of this signal (``hash(source ␟ id)``)."""
        raw = f"{self.source}{_DEDUP_SEP}{self.id}".encode()
        return hashlib.sha256(raw).hexdigest()

    @property
    def extensions(self) -> dict[str, Json]:
        """CloudEvents extension attributes beyond the platform's known set."""
        extra = self.model_extra or {}
        return dict(extra)

    @classmethod
    def parse(cls, payload: dict[str, object]) -> "Envelope":
        """Validate raw decoded JSON; raise ``EnvelopeError`` (never pydantic's)."""
        try:
            return cls.model_validate(payload)
        except ValidationError as exc:
            issues: list[Json] = [
                {
                    "loc": log_safe(".".join(str(part) for part in error["loc"])),
                    "msg": log_safe(str(error["msg"])),
                }
                for error in list(exc.errors())[:10]
            ]
            raise EnvelopeError(
                "signal envelope failed validation",
                details={"envelope_version": ENVELOPE_VERSION, "issues": issues},
            ) from exc
