"""The context service: the pure read model behind EVERY output channel.

Hot-path law (SPEC §7): at most one snapshot materialization per user per
capability (a point lookup), a jittered-TTL + single-flight cache on top,
tenant match and scope verified BEFORE any work, and staleness always
surfaced via ``UserContext.generated_at`` / ``max_age_ms``.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import cast

from graph_memory.core.errors import ScopeDenied
from graph_memory.core.types import (
    Capability,
    Json,
    Principal,
    Scope,
    UserContext,
    UserRef,
    require_tenant_match,
)
from graph_memory.projector.engine import Projector, SnapshotDoc


@dataclass
class _CacheEntry:
    doc: SnapshotDoc
    expires_at: float  # time.monotonic() based; performance only, not semantics


class ContextCache:
    """Jittered-TTL cache with per-key single-flight materialization."""

    def __init__(self, *, jitter: float = 0.2) -> None:
        self._jitter = jitter
        self._entries: dict[tuple[str, str, str], _CacheEntry] = {}
        self._locks: dict[tuple[str, str, str], asyncio.Lock] = {}

    async def get_or_materialize(
        self,
        key: tuple[str, str, str],
        loader: Callable[[], SnapshotDoc],
    ) -> SnapshotDoc:
        """Cache-aside with stampede protection; jitter keeps the herd calm."""
        entry = self._entries.get(key)
        if entry is not None and entry.expires_at > time.monotonic():
            return entry.doc
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            entry = self._entries.get(key)
            if entry is not None and entry.expires_at > time.monotonic():
                return entry.doc
            doc = await asyncio.to_thread(loader)
            ttl = doc.max_age_ms / 1_000.0
            window = ttl * self._jitter
            expires = time.monotonic() + ttl - (random.uniform(0.0, window) if window else 0.0)
            self._entries[key] = _CacheEntry(doc=doc, expires_at=expires)
            return doc

    def invalidate(self, key: tuple[str, str, str]) -> None:
        """Reflection-driven invalidation hook (projector may call this)."""
        self._entries.pop(key, None)

    def clear(self) -> None:
        """Composition-root hook: drop every entry after a re-projection."""
        self._entries.clear()


class MemoryContextProvider:
    """implements ``core.ports.ContextProvider`` over the projector."""

    def __init__(self, engine: Projector, cache: ContextCache | None = None) -> None:
        self._engine = engine
        self._cache = cache if cache is not None else ContextCache()

    async def get(self, principal: Principal, user: UserRef, capability: Capability) -> UserContext:
        """Hot path: verify, then one cached point lookup, then assemble."""
        require_tenant_match(principal, user)
        if not principal.allows(Scope.CONTEXT_READ):
            raise ScopeDenied(
                "principal does not carry context:read",
                details={"needed_scope": Scope.CONTEXT_READ.value},
            )
        doc = await self._cache.get_or_materialize(
            (str(user.tenant), str(user.user_key), capability),
            self._loader(user, capability),
        )
        return UserContext(
            user=user,
            capability=capability,
            generated_at=doc.generated_at,
            max_age_ms=doc.max_age_ms,
            cache_scope=f"t/{user.tenant}/u/{user.user_key}/c/{capability}",
            payload=cast("dict[str, Json]", doc.payload),  # JSON-safe by construction
        )

    def _loader(self, user: UserRef, capability: Capability) -> Callable[[], SnapshotDoc]:
        engine = self._engine
        return lambda: engine.snapshot(user.tenant, user.user_key, capability)
