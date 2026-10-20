"""Tenant registry: onboarding is a metadata row, never DDL (SPEC §8).

7,000 tenants in one environment is the target posture: the registry is a
plain keyed store (in-memory here, ClickHouse-backed adapter can replace it
via the same ``TenantRegistry`` protocol), tenant state drives every guard
decision, and offboarding is TWO phases — revoke (instant, fail closed)
then purge (async, per-tenant, audit-retention aware).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

from graph_memory.core.clock import Clock
from graph_memory.core.types import TenantId, tenant_id
from graph_memory.tenancy.errors import OffboardingClosed, TenantNotFound


class TenantClass(StrEnum):
    """Routing/replica class; whales stay a config-map, not a code fork."""

    STANDARD = "standard"


class TenantState(StrEnum):
    """/Onboarding → ACTIVE; offboarding → REVOKED → PURGED."""

    ACTIVE = "active"
    SUSPENDED = "suspended"
    REVOKED = "revoked"
    PURGED = "purged"


@dataclass
class TenantRecord:
    """One metadata row — no per-tenant DDL, ever."""

    tenant_id: TenantId
    tenant_class: TenantClass
    state: TenantState
    onboarded_at: datetime
    updated_at: datetime = field(kw_only=True, default=None)  # type: ignore[assignment]


class TenantRegistry:
    """Protocol-free concrete registry; CH adapter implements the same face."""

    def __init__(self, clock: Clock) -> None:
        self._rows: dict[str, TenantRecord] = {}
        self._clock = clock

    def onboard(
        self, tenant: str | TenantId, *, tenant_class: TenantClass = TenantClass.STANDARD
    ) -> TenantRecord:
        ref = tenant_id(str(tenant))
        now = self._clock.now()
        existing = self._rows.get(str(ref))
        if existing is not None:
            return existing  # onboarding is idempotent (retry-safe)
        row = TenantRecord(
            tenant_id=ref,
            tenant_class=tenant_class,
            state=TenantState.ACTIVE,
            onboarded_at=now,
            updated_at=now,
        )
        self._rows[str(ref)] = row
        return row

    def get(self, tenant: str | TenantId) -> TenantRecord:
        ref = str(tenant_id(str(tenant)))
        row = self._rows.get(ref)
        if row is None:
            raise TenantNotFound(f"tenant {ref!r} is not onboarded", details={"tenant": ref})
        return row

    def set_state(self, tenant: str | TenantId, state: TenantState) -> TenantRecord:
        row = self.get(tenant)
        _assert_transition(row, state)
        row.state = state
        row.updated_at = self._clock.now()
        return row

    def suspend(self, tenant: str | TenantId) -> TenantRecord:
        return self.set_state(tenant, TenantState.SUSPENDED)

    def revoke(self, tenant: str | TenantId) -> TenantRecord:
        """Offboarding phase 1: instant fail-closed deny for every lane."""
        return self.set_state(tenant, TenantState.REVOKED)

    def complete_purge(self, tenant: str | TenantId) -> TenantRecord:
        """Offboarding phase 2 terminal hook (purge steps ran already)."""
        return self.set_state(tenant, TenantState.PURGED)

    def tenants(self) -> list[TenantRecord]:
        return [self._rows[key] for key in sorted(self._rows)]

    def count(self) -> int:
        return len(self._rows)


def _assert_transition(row: TenantRecord, target: TenantState) -> None:
    """Lifecycle law: revoke precedes purge; suspension is a reversible hold."""
    allowed: dict[TenantState, frozenset[TenantState]] = {
        TenantState.ACTIVE: frozenset({TenantState.SUSPENDED, TenantState.REVOKED}),
        TenantState.SUSPENDED: frozenset({TenantState.ACTIVE, TenantState.REVOKED}),
        TenantState.REVOKED: frozenset({TenantState.PURGED, TenantState.ACTIVE}),
        TenantState.PURGED: frozenset(),
    }
    if target not in allowed[row.state]:
        raise OffboardingClosed(
            f"tenant {row.tenant_id} cannot move from {row.state.value} to {target.value}",
            details={"tenant": str(row.tenant_id), "state": row.state.value},
        )


def purge_tenant(tenant: TenantId, steps: list[Callable[[TenantId], object]]) -> object:
    """Phase 2: run every registered purge step for ONE tenant (async from ops)."""
    results: list[object] = []
    for step in steps:
        results.append(step(tenant))
    return results
