"""Tenancy package (SPEC §8): registry, guard hardening, quotas."""

from graph_memory.tenancy.guard import RegistryAwareGuard, StaticBearerGuard
from graph_memory.tenancy.quotas import QuotaGuard, Quotas
from graph_memory.tenancy.registry import (
    TenantClass,
    TenantRecord,
    TenantRegistry,
    TenantState,
    purge_tenant,
)

__all__ = [
    "QuotaGuard",
    "Quotas",
    "RegistryAwareGuard",
    "StaticBearerGuard",
    "TenantClass",
    "TenantRecord",
    "TenantRegistry",
    "TenantState",
    "purge_tenant",
]
