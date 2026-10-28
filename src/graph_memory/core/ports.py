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


class MemoryForget(Protocol):
    """User-facing memory control (research 06 §governance, M4.1): forget.

    A forget is emitted as a governed LEDGER EVENT under the new coarse
    scope ``memory:write`` — never a hidden row edit — so the tombstone is
    auditable and its supersession is the pure, replay-convergent
    projection every other signal goes through. Implementations bind the
    declared forget signal types from the mapping spec and return the
    emitted event's identity plus the ingest counts.
    """

    def forget(
        self, principal: Principal, user: UserRef, kind: str, target: str | None
    ) -> dict[str, Json]: ...


class MemorySearch(Protocol):
    """Ad-hoc ranked retrieval over ONE user's graph (research 06 lever 2, M4.2).

    Hybrid without an embedding service on the hot path: lexical matching
    (one shared metric — ``core.lexicon``), typed/traversal ordering from
    the mapping spec, and the behavior loop's weights as a ranking feature
    (no competitor ranks on loop evidence). Retrieval is per-user point
    work (decayed_score at one injected Clock instant; bounded result), ad
    hoc — never the default context assembly path. Item payloads carry the
    ranking features verbatim so channels render evidence, not vibes.
    """

    def search(
        self,
        principal: Principal,
        user: UserRef,
        query: str,
        *,
        capability: Capability | None,
        limit: int,
    ) -> dict[str, Json]: ...


class MemoryExport(Protocol):
    """GDPR portability (research 06, M4.5): one user's read model + history.

    Export is the user's OWN data, read-only: every declared capability's
    snapshot the read model serves them plus their ledger-scoped event
    history (bounded, oldest-first). The tenant rides the verified
    principal exactly like every other read; nothing here mutates state.
    """

    def export(self, principal: Principal, user: UserRef) -> dict[str, Json]: ...


class MemoryPrefetch(Protocol):
    """Warm-on-login (research 06 lever 3, M4.6): prebuilt payloads into cache.

    Reads only — it installs docs the context provider would otherwise
    build on demand; the tenant rides the verified principal and the
    payload set is the same point reads the provider serves. The returned
    dict is the hit-rate evidence (prebuilt-row pulls vs safety-net
    builds), JSON-stable like the other port results.
    """

    def prefetch(
        self, principal: Principal, user: UserRef, capabilities: Sequence[Capability] | None
    ) -> dict[str, Json]: ...


class EmbeddingProvider(Protocol):
    """Optional vector similarity seam (research 06 lever 2, M4.2).

    Parked DELIBERATELY: no embedding service rides the default context
    assembly or the hot-path SLA. A local ONNX adapter (cheap, deterministic
    batch embeds) implements this when a deployment opts in; nothing in
    core depends on it. Vectors are float tuples only — no numpy inside core.
    """

    def embed(self, texts: Sequence[str]) -> tuple[tuple[float, ...], ...]: ...
