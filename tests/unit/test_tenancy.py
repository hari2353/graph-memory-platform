"""Tenancy tests: onboard→active, fail-closed states, quotas, offboarding, 7k.

Acceptances wired to SPEC §8/§16.4: onboarding is metadata-only (scale bench),
revocation denies instantly (phase 1), purge is phase 2, a whale burst can
never starve co-tenants (per-(tenant,lane) buckets).
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from graph_memory.core.clock import FixedClock
from graph_memory.core.errors import QuotaExceeded
from graph_memory.core.types import Scope, TenantId
from graph_memory.settings import Settings
from graph_memory.tenancy import (
    QuotaGuard,
    Quotas,
    RegistryAwareGuard,
    StaticBearerGuard,
    TenantClass,
    TenantRegistry,
    TenantState,
    purge_tenant,
)
from graph_memory.tenancy.errors import (
    OffboardingClosed,
    TenantNotFound,
    TenantRevoked,
    TenantSuspended,
)

_NOW = datetime(2026, 6, 10, tzinfo=UTC)


def _registry() -> TenantRegistry:
    return TenantRegistry(FixedClock(_NOW))


def test_onboarding_is_idempotent_metadata_only() -> None:
    registry = _registry()
    row = registry.onboard(TenantId("acme"), tenant_class=TenantClass.STANDARD)
    again = registry.onboard("acme")
    assert row.tenant_id == again.tenant_id == TenantId("acme")
    assert row.state == TenantState.ACTIVE
    assert registry.count() == 1
    assert registry.get("acme") is row


def test_unknown_tenant_and_illegal_transitions_fail_closed() -> None:
    registry = _registry()
    with pytest.raises(TenantNotFound):
        registry.get("acme")
    row = registry.onboard("acme")
    with pytest.raises(OffboardingClosed):
        registry.complete_purge("acme")  # phase 1 (revoke) precedes any purge
    assert row.state == TenantState.ACTIVE


def test_revoke_then_guard_denies_then_suspend_flow() -> None:
    registry = _registry()
    registry.onboard("acme")
    inner = StaticBearerGuard(Settings().dev_static_tokens)
    guard = RegistryAwareGuard(inner, registry)

    active = guard.require_scope("dev-acme-token", Scope.CONTEXT_READ)
    assert active.tenant == TenantId("acme")

    registry.revoke("acme")
    with pytest.raises(TenantRevoked):
        guard.require_scope("dev-acme-token", Scope.CONTEXT_READ)

    resumed = registry.set_state("acme", TenantState.ACTIVE)
    assert resumed.state == TenantState.ACTIVE
    guard.require_scope("dev-acme-token", Scope.SIGNALS_WRITE)

    registry.suspend("acme")
    with pytest.raises(TenantSuspended):
        guard.require_scope("dev-acme-token", Scope.SIGNALS_WRITE)


def test_offboarding_two_phase_runs_purge_steps_then_purged() -> None:
    registry = _registry()
    registry.onboard("acme")
    purged: list[str] = []
    registry.revoke("acme")
    registry.complete_purge("acme")
    assert registry.get("acme").state == TenantState.PURGED
    with pytest.raises(OffboardingClosed):
        registry.revoke("acme")  # post-purge lifecycle is terminal
    assert purge_tenant(TenantId("acme"), [lambda t: purged.append(str(t))]) is not None
    assert purged == ["acme"]


def test_quota_gates_independent_lanes_per_tenant() -> None:
    quota = QuotaGuard(Quotas(interactive_burst=2.0, batch_burst=3.0), FixedClock(_NOW))
    import graph_memory.core.types as t

    principal_a = t.Principal(tenant=TenantId("a"), scopes=frozenset(Scope), subject="a")
    principal_b = t.Principal(tenant=TenantId("b"), scopes=frozenset(Scope), subject="b")
    quota.admit(principal_a, Scope.CONTEXT_READ)
    quota.admit(principal_a, Scope.CONTEXT_READ)
    with pytest.raises(QuotaExceeded):
        quota.admit(principal_a, Scope.CONTEXT_READ)
    # Lane separation: batch lane unaffected by interactive exhaustion.
    quota.admit(principal_a, Scope.SIGNALS_WRITE)
    # Tenant separation: principal_b's interactive bucket is untouched.
    quota.admit(principal_b, Scope.CONTEXT_READ)


def test_whale_burst_cannot_starve_cotenant_batches() -> None:
    quota = QuotaGuard(
        Quotas(batch_rps=1.0, batch_burst=3.0, interactive_rps=1.0, interactive_burst=3.0),
        FixedClock(_NOW),
    )
    import graph_memory.core.types as t

    whale = t.Principal(tenant=TenantId("whale"), scopes=frozenset(Scope), subject="w")
    calm = t.Principal(tenant=TenantId("calm"), scopes=frozenset(Scope), subject="c")
    for _ in range(3):
        quota.admit(whale, Scope.SIGNALS_WRITE)
    with pytest.raises(QuotaExceeded):
        quota.admit(whale, Scope.SIGNALS_WRITE)
    # calm tenant's own bucket unaffected.
    quota.admit(calm, Scope.SIGNALS_WRITE)


def test_onboarding_7k_tenants_is_metadata_scale() -> None:
    import time

    registry = _registry()
    start = time.perf_counter()
    for index in range(7_000):
        registry.onboard(TenantId(f"t-{index:05d}"))
    elapsed = time.perf_counter() - start
    assert registry.count() == 7_000
    assert elapsed < 10.0  # seconds-scale, not DDL-per-tenant scale
