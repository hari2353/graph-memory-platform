"""Core domain foundation: stdlib-only vocabulary, ports, errors, time, scrubbing."""

from __future__ import annotations

from graph_memory.core.clock import Clock, FixedClock, SystemClock
from graph_memory.core.errors import (
    EnvelopeError,
    PlatformError,
    QuotaExceeded,
    ScopeDenied,
    TenantMismatch,
)
from graph_memory.core.logsafe import log_safe
from graph_memory.core.ports import ContextProvider, SignalSink, TenantGuard
from graph_memory.core.types import (
    Capability,
    IngestResult,
    Json,
    Principal,
    Scope,
    SignalType,
    TenantId,
    UserContext,
    UserKey,
    UserRef,
    parse_utc,
    require_tenant_match,
    signal_type,
    tenant_id,
    user_key,
)

__all__ = [
    "Capability",
    "Clock",
    "ContextProvider",
    "EnvelopeError",
    "FixedClock",
    "IngestResult",
    "Json",
    "PlatformError",
    "Principal",
    "QuotaExceeded",
    "Scope",
    "ScopeDenied",
    "SignalSink",
    "SignalType",
    "SystemClock",
    "TenantGuard",
    "TenantId",
    "TenantMismatch",
    "UserContext",
    "UserKey",
    "UserRef",
    "log_safe",
    "parse_utc",
    "require_tenant_match",
    "signal_type",
    "tenant_id",
    "user_key",
]
