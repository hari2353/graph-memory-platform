"""Precompute + prewarm (M4.6, research 06 levers 1 + 3).

Lever 1 — precompute: per-(tenant, user, capability) context payloads are
materialized on the projector's 1s tick into a payload store with the SAME
staged-rebuild semantics the graph gateway already ships for CH tables
(stage into a hold-out, publish with one pointer swap). Readers only ever
fetch the published side, so no consumer ever sees a half-built
materialization; the single-flight cache degrades to a safety net.

Lever 3 — warm-on-login (the Zep ``graph.warm`` analog): ``PrewarmService``
pushes the user's prebuilt payloads into the context cache at session open
and reports the falsifiable hit-rate number — prebuilt-row pulls vs
safety-net builds. A rate of 0.0 means every requested pair had to be
built on demand (the tick had not reached it); ~1.0 means the tick's
precompute carried the warming. Channels meter exactly these counters
(``prefetch`` / ``prefetch_cold``, pair-grained) so the lift is provable
from the durable metering table alone.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import final

from graph_memory.context.service import ContextCache
from graph_memory.core.types import (
    Capability,
    Json,
    Principal,
    TenantId,
    UserKey,
    UserRef,
    require_tenant_match,
)
from graph_memory.projector.engine import CapabilityUnknown, Projector, SnapshotDoc


@final
class ProfileMaterializer:
    """Prebuilt payload rows, refreshable on the projection tick.

    Rebuild protocol (the gateway's, reused verbatim): ``rebuild()`` opens
    a hold-out, materializes every declared capability for every user the
    graph knows, and publishes the staged state atomically. A serving
    caller therefore never observes a half-built payload set: ``fetch``
    reads the published side only. Determinism rides the engine's
    injected Clock — the same (ledger, spec, clock) rebuilds identical
    rows, so the prebuilt tier converges like the graph itself.
    """

    def __init__(self, engine: Projector) -> None:
        self._engine = engine
        self._published: dict[tuple[str, str, str], SnapshotDoc] = {}
        self._staged: dict[tuple[str, str, str], SnapshotDoc] | None = None

    def rebuild(self) -> int:
        """One replaceable rebuild: stage every (user × capability), publish.

        Returns the number of prebuilt rows published. If any build fails,
        the exception propagates WITHOUT the swap — the previous published
        set keeps serving, exactly like the graph gateway's rebuilds.
        """
        self._staged = {}
        try:
            count = 0
            for tenant in self._engine.ledger.tenants():
                for user in self._engine.gateway.users(tenant):
                    for capability in sorted(self._engine.spec.capabilities):
                        doc = self._engine.snapshot(tenant, user, Capability(capability))
                        self._staged[(str(tenant), str(user), capability)] = doc
                        count += 1
        except BaseException:
            # A failed rebuild never publishes; the staged hold-out is
            # dropped and the caller may retry on the next tick.
            self._staged = None
            raise
        self._published = self._staged
        self._staged = None
        return count

    def fetch(self, tenant: TenantId, user: UserKey, capability: Capability) -> SnapshotDoc | None:
        """The published prebuilt row (or None — never a half-built state)."""
        return self._published.get((str(tenant), str(user), capability))

    def published_size(self) -> int:
        """Ops view: how many rows the published prebuilt set holds."""
        return len(self._published)


@final
class PrewarmService:
    """Implements ``core.ports.MemoryPrefetch``: warm-on-login.

    ``prefetch`` installs the user's payloads into the context cache —
    from the tick's prebuilt rows when the precompute has reached them
    (the lever-1 read path), else by a direct engine build (the safety
    net). The dict it returns is the hit-rate evidence; boundaries are
    the same tenant rule and capability validation the provider applies.
    """

    def __init__(
        self,
        engine: Projector,
        cache: ContextCache,
        materializer: ProfileMaterializer,
    ) -> None:
        self._engine = engine
        self._cache = cache
        self._materializer = materializer

    def _doc(
        self, tenant: TenantId, user_key: UserKey, capability: Capability
    ) -> tuple[SnapshotDoc, bool]:
        """One payload plus its source flag: prebuilt row or safety-net build."""
        prebuilt = self._materializer.fetch(tenant, user_key, capability)
        if prebuilt is not None:
            return prebuilt, True
        return self._engine.snapshot(tenant, user_key, capability), False

    def prefetch(
        self,
        principal: Principal,
        user: UserRef,
        capabilities: Sequence[Capability] | None,
    ) -> dict[str, Json]:
        """Warm the cache for one user; return the hit-rate evidence dict."""
        require_tenant_match(principal, user)
        requested = (
            sorted(capabilities)
            if capabilities is not None
            else sorted(self._engine.spec.capabilities)
        )
        for capability in requested:
            if capability not in self._engine.spec.capabilities:
                raise CapabilityUnknown(
                    "capability is not declared by the mapping spec",
                    details={"capability": capability},
                )
        tenant, user_key = user.tenant, user.user_key
        prebuilt_count = 0
        cache_scopes: list[Json] = []
        latest_generated: str | None = None
        latest_max_age_ms = 0
        for capability in requested:
            doc, was_prebuilt = self._doc(tenant, user_key, Capability(capability))
            prebuilt_count += was_prebuilt
            self._cache.put(doc)
            cache_scopes.append(f"t/{tenant}/u/{user_key}/c/{capability}")
            generated = doc.generated_at.isoformat()
            if latest_generated is None or generated > latest_generated:
                latest_generated = generated
                latest_max_age_ms = doc.max_age_ms
        total = len(requested)
        safety_net_builds = total - prebuilt_count
        return {
            "user": str(user_key),
            "tenant": str(tenant),
            "requested": total,
            "prebuilt": prebuilt_count,
            "safety_net_builds": safety_net_builds,
            "hit_rate": (prebuilt_count / total) if total else None,
            "cache_scopes": cache_scopes,
            "generated_at": latest_generated,
            "max_age_ms": latest_max_age_ms,
        }
