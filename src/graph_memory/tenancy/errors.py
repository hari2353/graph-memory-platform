"""Tenancy lifecycle errors — stable codes, module-owned (SPEC §8)."""

from __future__ import annotations

from graph_memory.core.errors import PlatformError
from graph_memory.core.types import Json


class TenantNotFound(PlatformError):
    """The credential resolves to a tenant the registry has never onboarded."""

    code = "tenant_not_found"


class TenantSuspended(PlatformError):
    """Tenant temporarily paused (billing/abuse hold) — fail closed."""

    code = "tenant_suspended"


class TenantRevoked(PlatformError):
    """Offboarding phase 1 completed — every lane denies instantly."""

    code = "tenant_revoked"


class OffboardingClosed(PlatformError):
    """A lifecycle transition is illegal in the current state."""

    code = "offboarding_closed"


def lifecycle_details(tenant: str, state: str) -> dict[str, Json]:
    """Common safe details for lifecycle denials (no claim internals)."""
    return {"tenant": tenant, "state": state}
