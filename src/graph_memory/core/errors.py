"""Platform error hierarchy with stable, machine-readable codes.

Every output channel renders these as JSON payloads / MCP tool errors. Two
public contracts live here and must never drift silently:

1. ``code`` — a wire-level error discriminator (renaming one is a channel
   breaking change; the deprecation ladder in SPEC §9 applies).
2. ``to_payload()`` — the exact shape channels render, with messages scrubbed
   through :func:`graph_memory.core.logsafe.log_safe`.
"""

from __future__ import annotations

from graph_memory.core.logsafe import log_safe
from graph_memory.core.types import Json


class PlatformError(Exception):
    """Base for every error the platform intentionally surfaces."""

    code = "platform_error"

    def __init__(self, message: str, *, details: dict[str, Json] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details

    def to_payload(self) -> dict[str, Json]:
        """JSON-ready shape shared by every channel adapter."""
        payload: dict[str, Json] = {"error": self.code, "message": log_safe(self.message)}
        if self.details:
            payload["details"] = self.details
        return payload


class EnvelopeError(PlatformError):
    """A signal failed envelope validation — rejected before the ledger."""

    code = "envelope_invalid"


class TenantMismatch(PlatformError):
    """A principal addressed a resource outside its claims-derived tenant."""

    code = "tenant_mismatch"


class ScopeDenied(PlatformError):
    """The principal's claims do not carry the required scope."""

    code = "scope_denied"


class QuotaExceeded(PlatformError):
    """Per-tenant admission control refused the request (SPEC §8 quotas)."""

    code = "quota_exceeded"
