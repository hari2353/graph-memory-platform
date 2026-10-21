"""MCP tool surface (SPEC §9: primary channel, same-core compat mode).

Stateless 2026-07-28 JSON mode is the default serving posture
(``stateless_http=True, json_response=True``); the compat posture rides the
same core with sessions/SSE. Per-request auth: a transport middleware copies
the ``Authorization`` header into a contextvar, then each tool resolves it
through the ``TenantGuard`` — the tenant derives from verified claims only and
is never a tool argument (SPEC §8/§15). Tool count is capped at four.
"""

from __future__ import annotations

import functools
import json
from collections.abc import Awaitable, Callable, Mapping
from contextvars import ContextVar
from typing import Any, cast

from mcp.server.context import HandlerResult
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from starlette.applications import Starlette

from graph_memory.core.errors import PlatformError, ScopeDenied
from graph_memory.core.ports import (
    ContextExplainer,
    ContextProvider,
    EffectivenessScorer,
    TenantGuard,
)
from graph_memory.core.types import (
    Capability,
    Json,
    Principal,
    Scope,
    UserRef,
    user_key,
)
from graph_memory.tenancy.quotas import QuotaGuard

PROTOCOL_REVISION = "2026-07-28"  # the stateless JSON posture this server pins
SERVER_VERSION = "0.1.0"
_MAX_SEARCH_LIMIT = 25
_DEFAULT_SEARCH_LIMIT = 8

_CURRENT_BEARER: ContextVar[str | None] = ContextVar("gmp_mcp_bearer", default=None)


class BearerHeaderTransport:
    """Auth-header glue: copies the credential into the request contextvar.

    The guard is invoked INSIDE each tool (spec §15 posture), so scope
    decisions stay next to the capability they protect; the middleware only
    makes the transport-level credential visible to tools.
    """

    async def __call__(
        self,
        ctx: Any,  # noqa: ANN401 — ServerRequestContext[Any, Any] per the SDK protocol
        call_next: Any,  # noqa: ANN401 — CallNext
    ) -> HandlerResult:
        bearer: str | None = None
        request = getattr(ctx, "request", None)
        headers = getattr(request, "headers", None)
        if headers is not None:
            bearer = headers.get("authorization")
        _CURRENT_BEARER.set(bearer)
        return cast("HandlerResult", await call_next(ctx))


def _platform_errors(
    handler: Callable[..., Awaitable[dict[str, Json]]],
) -> Callable[..., Awaitable[dict[str, Json]]]:
    """Render our stable error family as anticipated tool failures.

    A raised ``PlatformError`` keeps its (log-safe) payload JSON as the tool
    error text; anything else stays a crash the SDK withholds — channels
    never invent their own error shapes (SPEC §9/§15).
    """

    @functools.wraps(handler)  # preserves the signature the schema is built from
    async def wrapper(*args: Any, **kwargs: Any) -> dict[str, Json]:
        try:
            return await handler(*args, **kwargs)
        except PlatformError as exc:
            raise ToolError(json.dumps(exc.to_payload())) from exc

    return wrapper


def _bearer() -> str:
    """Fail closed when no transport credential was attached."""
    token = _CURRENT_BEARER.get()
    if token is None:
        raise ScopeDenied("missing Authorization header", details={"reason": "unknown_credential"})
    return token


def build_mcp_server(
    provider: ContextProvider,
    guard: TenantGuard,
    *,
    capabilities: tuple[str, ...],
    signal_schemas: Mapping[str, dict[str, Json]],
    name: str = "graph-memory-platform",
    version: str = SERVER_VERSION,
    quota: QuotaGuard | None = None,
    effectiveness: EffectivenessScorer | None = None,
    explainer: ContextExplainer | None = None,
) -> MCPServer:
    """Assemble the read-model tools around the injected ports (dependency inversion).

    ``capabilities`` and ``signal_schemas`` are declared summaries (built at
    the wiring edge from the mapping spec) so this module never imports the
    mapping package — channels depend on core ports only. ``effectiveness``
    and ``explainer`` are optional SPEC §5 faces; unwired, their tools fail
    as anticipated tool errors, never silently.
    """
    server = MCPServer(
        name=name,
        version=version,
        instructions=(
            "Read-model tools over a per-user memory graph. Resolve the user's "
            "context by capability, search within it, inspect declared "
            "capabilities, see what signal types the platform maps, explain a "
            "read's provenance, or read the effectiveness loop's report."
        ),
        middleware=[BearerHeaderTransport()],
    )

    def _admit(principal: Principal, needed: Scope) -> None:
        """Per-tenant metering on every request class (SPEC §11/§17)."""
        if quota is not None:
            quota.admit(principal, needed)

    @server.tool(
        name="get_context",
        title="Get user context",
        description=(
            "Hyperpersonalized context for one user: ranked relation items for a "
            "declared capability, with staleness metadata (stale_at/max_age_ms)."
        ),
        structured_output=True,
    )
    @_platform_errors
    async def get_context(user: str, capability: str) -> dict[str, Json]:
        principal = guard.require_scope(_bearer(), Scope.CONTEXT_READ)
        _admit(principal, Scope.CONTEXT_READ)
        ref = UserRef(tenant=principal.tenant, user_key=user_key(user))
        ctx = await provider.get(principal, ref, Capability(capability))
        stale_at = ctx.generated_at.timestamp() + ctx.max_age_ms / 1000.0
        return {
            "capability": str(ctx.capability),
            "payload": ctx.payload,
            "generated_at": ctx.generated_at.isoformat(),
            "stale_at": _to_iso(stale_at),
            "max_age_ms": ctx.max_age_ms,
            "cache_scope": ctx.cache_scope,
        }

    @server.tool(
        name="search_memory",
        title="Search user memory",
        description=(
            "Bounded keyword search within one user's context for a capability "
            "(post-filter of the same point-lookup payload; no graph traversal)."
        ),
        structured_output=True,
    )
    @_platform_errors
    async def search_memory(
        user: str,
        capability: str,
        query: str = "",
        limit: int = _DEFAULT_SEARCH_LIMIT,
    ) -> dict[str, Json]:
        principal = guard.require_scope(_bearer(), Scope.MEMORY_SEARCH)
        _admit(principal, Scope.MEMORY_SEARCH)
        ref = UserRef(tenant=principal.tenant, user_key=user_key(user))
        ctx = await provider.get(principal, ref, Capability(capability))
        return {
            "capability": str(ctx.capability),
            "generated_at": ctx.generated_at.isoformat(),
            "max_age_ms": ctx.max_age_ms,
            "items": _filter_items(ctx.payload, query, limit),
        }

    @server.tool(
        name="list_capabilities",
        title="List capabilities",
        description="Context capabilities this deployment declares.",
        structured_output=True,
    )
    @_platform_errors
    async def list_capabilities() -> dict[str, Json]:
        guard.require_scope(_bearer(), Scope.CONTEXT_READ)
        return {"capabilities": list(capabilities)}

    @server.tool(
        name="get_signal_schema",
        title="Get signal schema",
        description=(
            "What signal types this deployment maps, their derived relations, "
            "weights and half-lives — the contract for producers."
        ),
        structured_output=True,
    )
    @_platform_errors
    async def get_signal_schema() -> dict[str, Json]:
        guard.require_scope(_bearer(), Scope.SIGNALS_WRITE)
        return {"signals": dict(signal_schemas)}

    @server.tool(
        name="get_effectiveness",
        title="Get effectiveness report",
        description=(
            "The effectiveness loop's deterministic tick for the caller's "
            "tenant: per (cohort, recommendation class) cells with Laplace "
            "rates and the weight multipliers they wrote. Requires the loop "
            "to be wired on this deployment."
        ),
        structured_output=True,
    )
    @_platform_errors
    async def get_effectiveness() -> dict[str, Json]:
        principal = guard.require_scope(_bearer(), Scope.CONTEXT_READ)
        if effectiveness is None:
            raise PlatformError(
                "effectiveness loop is not wired on this deployment",
                details={"tool": "get_effectiveness", "reason": "not_wired"},
            )
        return effectiveness.score(principal.tenant)

    @server.tool(
        name="explain_context",
        title="Explain context provenance",
        description=(
            "Graph-annotated provenance for one (user, capability): the raw "
            "edge observations under the served snapshot with per-observation "
            "decayed scores and the mapping signals that produced each kind. "
            "Requires the explainer to be wired on this deployment."
        ),
        structured_output=True,
    )
    @_platform_errors
    async def explain_context(user: str, capability: str) -> dict[str, Json]:
        principal = guard.require_scope(_bearer(), Scope.CONTEXT_READ)
        if explainer is None:
            raise PlatformError(
                "context explainer is not wired on this deployment",
                details={"tool": "explain_context", "reason": "not_wired"},
            )
        ref = UserRef(tenant=principal.tenant, user_key=user_key(user))
        return explainer.explain(principal, ref, Capability(capability))

    return server


def mcp_streamable_app(
    server: MCPServer,
    *,
    stateless: bool = True,
) -> Starlette:
    """HTTP ASGI app: stateless JSON mode (default) or compat SSE sessions."""
    return server.streamable_http_app(
        json_response=stateless,
        stateless_http=stateless,
    )


def _to_iso(epoch_seconds: float) -> str:
    from datetime import UTC, datetime

    return datetime.fromtimestamp(epoch_seconds, tz=UTC).isoformat()


def _filter_items(payload: dict[str, Json], query: str, limit: int) -> Json:
    """Bounded search: keyword overlap over item targets/labels of one snapshot.

    Runs entirely on the already-compiled snapshot (≤3 lookups rule holds);
    ``limit`` is clamped to ``_MAX_SEARCH_LIMIT`` (SPEC §17 bounded payloads).
    """
    items = payload.get("items")
    labels = payload.get("labels")
    if not isinstance(items, list):
        return []
    label_map: dict[str, Json] = {}
    if isinstance(labels, dict):
        for key, value in labels.items():
            label_map[str(key)] = value
    tokens = query.casefold().split()
    bounded = max(1, min(limit, _MAX_SEARCH_LIMIT))
    hits: list[Json] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        target = item.get("target")
        haystack = str(target)
        labels_for = label_map.get(str(target))
        if isinstance(labels_for, list):
            haystack += " " + " ".join(str(label) for label in labels_for)
        if tokens and not any(token in haystack.casefold() for token in tokens):
            continue
        hits.append(item)
        if len(hits) >= bounded:
            break
    return hits
