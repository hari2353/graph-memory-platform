"""Domain vocabulary: strong ids, principals, scopes, read-model stubs.

Stdlib only. Ports in ``core.ports`` speak only these types, so adapters
cannot leak framework objects into each other. Identity rules line up with
`docs/SPEC.md` §3/§8: the tenant is structural (derived from verified claims,
never a tool argument); a user reference always carries its tenant so a
principal from the wrong tenant cannot address it.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import NewType

TenantId = NewType("TenantId", str)
UserKey = NewType("UserKey", str)
SignalType = NewType("SignalType", str)
Capability = NewType("Capability", str)

MAX_TENANT_LEN = 128
MAX_USER_KEY_LEN = 256
MAX_SIGNAL_TYPE_LEN = 128

type Json = str | int | float | bool | None | list[Json] | dict[str, Json]


class Scope(StrEnum):
    """Capabilities granted to a principal; coarse by design (SPEC §17:
    the tenant is never a tool argument, so scopes describe what, not where)."""

    SIGNALS_WRITE = "signals:write"
    CONTEXT_READ = "context:read"
    MEMORY_SEARCH = "memory:search"
    ADMIN = "platform:admin"


def tenant_id(raw: str) -> TenantId:
    """Validate + build a tenant id (no whitespace, no padding, bounded)."""
    return TenantId(_bounded_id(raw, MAX_TENANT_LEN, "tenant id"))


def user_key(raw: str) -> UserKey:
    """Validate + build an app's stable user key within one tenant."""
    return UserKey(_bounded_id(raw, MAX_USER_KEY_LEN, "user key"))


def signal_type(raw: str) -> SignalType:
    """Validate + build a namespaced, versioned signal type ``ns.type.vN``."""
    value = _bounded_id(raw, MAX_SIGNAL_TYPE_LEN, "signal type")
    if value.count(".") < 2 or not value.rsplit(".", 1)[-1].startswith("v"):
        raise ValueError(f"signal type {raw!r} must look like '<ns>.<name>.<vN>'")
    return SignalType(value)


def _bounded_id(raw: str, limit: int, what: str) -> str:
    if raw != raw.strip() or not raw or len(raw) > limit:
        raise ValueError(f"invalid {what}: {raw!r} (must be trimmed, 1..{limit} chars)")
    if any(ch.isspace() for ch in raw):
        raise ValueError(f"invalid {what}: {raw!r} (whitespace forbidden)")
    return raw


def parse_utc(raw: str) -> datetime:
    """Parse an ISO-8601 instant and require an explicit UTC offset of zero."""
    moment = datetime.fromisoformat(raw)
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError("instants must carry a UTC offset")
    return moment.astimezone(UTC)


@dataclass(frozen=True)
class Principal:
    """Verified identity + grants. ``tenant`` comes ONLY from verified claims."""

    tenant: TenantId
    scopes: frozenset[Scope]
    subject: str

    def allows(self, needed: Scope) -> bool:
        return needed in self.scopes


@dataclass(frozen=True)
class UserRef:
    """A user address: tenant-explicit so cross-tenant access is unrepresentable."""

    tenant: TenantId
    user_key: UserKey


def require_tenant_match(principal: Principal, user: UserRef) -> None:
    """The single check every context access performs before anything else."""
    from graph_memory.core.errors import TenantMismatch

    if principal.tenant != user.tenant:
        raise TenantMismatch(
            f"principal tenant {principal.tenant!r} cannot access user in {user.tenant!r}"
        )


@dataclass(frozen=True)
class IngestResult:
    """Outcome of one idempotent submit; duplicates are acks, not errors."""

    accepted: int
    duplicates: int

    def __post_init__(self) -> None:
        if self.accepted < 0 or self.duplicates < 0:
            raise ValueError("counts must be non-negative")


@dataclass(frozen=True)
class UserContext:
    """The hot-path payload stub; the real schema ships with ``context-service``.

    ``generated_at`` + ``max_age_ms`` exist so staleness is always surfaced
    (SPEC §17: silent staleness is forbidden) and so channels can honor cache
    hints instead of inventing their own.
    """

    user: UserRef
    capability: Capability
    generated_at: datetime
    max_age_ms: int
    cache_scope: str
    payload: dict[str, Json]

    def is_stale(self, at: datetime) -> bool:
        if at.tzinfo is None or at.utcoffset() is None:
            raise ValueError("comparison instant must be aware")
        age_ms = (at.astimezone(UTC) - self.generated_at.astimezone(UTC)).total_seconds() * 1000
        return age_ms > self.max_age_ms
