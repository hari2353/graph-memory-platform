"""Ports (the SOLID seams): Protocols owned by core, implemented by adapters.

Dependency rule (SPEC §3): channels, the projector and the ingest API depend
on these Protocols; adapters add implementations and core never changes. The
tenant rule is structural — a principal's tenant comes from verified claims
and access helpers reject cross-tenant references before any adapter IO runs.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, TypeVar

from graph_memory.core.types import (
    Capability,
    IngestResult,
    Json,
    Principal,
    Scope,
    TenantId,
    UserContext,
    UserRef,
)

EventT = TypeVar("EventT", contravariant=True)


class SignalSink(Protocol[EventT]):
    """Accepts an idempotent batch of signals (HTTP adapter now, brokers at M2).

    Duplicate events are acked, not errors (at-least-once upstream is the
    design assumption; ledger dedup owns correctness — SPEC §5).
    """

    async def submit(self, events: Sequence[EventT]) -> IngestResult: ...


class TenantGuard(Protocol):
    """Turns a raw credential into a claims-derived principal, scope-checked."""

    def require_scope(self, credential: str, needed: Scope) -> Principal: ...


class ContextProvider(Protocol):
    """Pure read model behind every output channel (MCP / REST / webhook / SDK).

    Implementations must respect the hot-path rules: at most three
    tenant-scoped point lookups, no graph traversal, staleness surfaced via
    ``UserContext.max_age_ms`` (SPEC §7).
    """

    async def get(
        self, principal: Principal, user: UserRef, capability: Capability
    ) -> UserContext: ...


class EffectivenessScorer(Protocol):
    """The loop's read face (SPEC §5 outputs): one tenant's tick as JSON data.

    Adapters render ``EffectivenessReport`` rows; channels only know the
    port — wiring is the composition root's job. When unwired, channels
    serve the tool as an anticipated failure (never a silent empty).
    """

    def score(self, tenant: TenantId) -> dict[str, Json]: ...


class ContextExplainer(Protocol):
    """Graph-annotated provenance for one (user, capability) read.

    Same structural tenant rule as ``ContextProvider``: the tenant comes
    from the verified principal, the ref contributes the user only.
    """

    def explain(
        self, principal: Principal, user: UserRef, capability: Capability
    ) -> dict[str, Json]: ...
