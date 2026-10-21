"""REST ``/v1`` context endpoints (SPEC §9: channel over the same provider).

Same guards, same provider, same error payloads as MCP — only the wire differs:

- ``GET /v1/context/{user}/{capability}`` — snapshot with ``ETag``/304 and
  ``Cache-Control`` derived from the provider's own ``max_age_ms`` (channels
  honor the provider's staleness contract; they never invent TTLs).
- ``GET .../items`` — cursor pagination over the ranked item list.
- ``POST .../prefetch`` — pre-materializes the snapshot (single-flight cache);
  callers may send ``Idempotency-Key``; replays return the stored response.
"""

from __future__ import annotations

import base64
import hashlib
import json
import time
from datetime import UTC, datetime, timedelta
from typing import Annotated

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse, Response

from graph_memory.core.errors import PlatformError
from graph_memory.core.ports import ContextProvider, TenantGuard
from graph_memory.core.types import (
    Capability,
    Json,
    Scope,
    TenantId,
    UserContext,
    UserRef,
    user_key,
)
from graph_memory.observability.metering import MeteringCounters
from graph_memory.observability.observer import SpanSink, SpanStub
from graph_memory.tenancy.quotas import QuotaGuard

_IDEMPOTENCY_TTL_S = 86_400
_MAX_ITEM_LIMIT = 100


def _bearer_from(authorization: str | None) -> str:
    """Header value or hard fail; the guard rejects empty as unknown credential."""
    if authorization is None:
        raise PlatformError(
            "missing Authorization header",
            details={"reason": "unknown_credential"},
        )
    return authorization


def _status_for(exc: PlatformError) -> int:
    """Stable code -> HTTP status (same mapping family as the ingest edge)."""
    if exc.code == "scope_denied":
        details = exc.details or {}
        return 401 if details.get("reason") == "unknown_credential" else 403
    if exc.code == "capability_unknown":
        return 404
    if exc.code in {"tenant_revoked", "tenant_suspended", "tenant_not_found"}:
        return 403
    return {"tenant_mismatch": 400, "quota_exceeded": 429, "batch_over_budget": 429}.get(
        exc.code, 500
    )


def _stale_iso(generated_at: datetime, max_age_ms: int) -> str:
    return (generated_at + timedelta(milliseconds=max_age_ms)).astimezone(UTC).isoformat()


def _etag_for(cache_scope: str, generated: str, payload: dict[str, Json]) -> str:
    digest = hashlib.sha256(
        json.dumps(
            {"at": generated, "body": payload, "scope": cache_scope}, sort_keys=True
        ).encode()
    ).hexdigest()
    return f'"{digest}"'


def _encode_cursor(kind: str, target: str) -> str:
    return base64.urlsafe_b64encode(json.dumps({"a": f"{kind}::{target}"}).encode()).decode()


def _decode_cursor(cursor: str) -> tuple[str, str]:
    try:
        raw = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
    except Exception as exc:  # noqa: BLE001 — malformed cursors are client errors
        raise ValueError("malformed cursor") from exc
    if not isinstance(raw, dict) or not isinstance(raw.get("a"), str):
        raise ValueError("malformed cursor")
    after = str(raw["a"])
    kind, separator, target = after.partition("::")
    if not separator or not target or not kind:
        raise ValueError("malformed cursor")
    return kind, target


def _ranked_items(payload: dict[str, Json]) -> list[tuple[str, Json]]:
    """(kind, full item dict) in ranked order, as the snapshot already sorted."""
    items = payload.get("items")
    if not isinstance(items, list):
        return []
    ranked: list[tuple[str, Json]] = []
    for item in items:
        if isinstance(item, dict):
            ranked.append((str(item.get("kind", "")), item))
    return ranked


def create_rest_app(
    provider: ContextProvider,
    guard: TenantGuard,
    *,
    quota: QuotaGuard | None = None,
    sink: SpanSink | None = None,
    metering: MeteringCounters | None = None,
) -> FastAPI:
    """Assemble the REST channel around injected ports (no framework leakage).

    ``sink``/``metering`` are optional observability ports (SPEC §11/R7): when
    wired, every request emits one span with per-stage wall times — sampled or
    not; the audit stub's job is to land them all — and metering ticks the
    per-(tenant, lane, event) counters.
    """
    app = FastAPI(title="graph-memory-platform/rest", version="0.1.0")
    # Idempotency-Key replay store: (tenant, user, capability, key) -> (expiry, body).
    _replay: dict[tuple[str, str], tuple[float, dict[str, Json]]] = {}

    async def _context_of(
        authorization: str | None, scope: Scope, user: str, capability: str
    ) -> UserContext:
        """One checked read: guard first, tenant from claims only (SPEC §8/§15)."""
        started = time.perf_counter()
        principal = None
        error_code: str | None = None
        stages: dict[str, Json] = {}
        try:
            stage_start = time.perf_counter()
            principal = guard.require_scope(_bearer_from(authorization), scope)
            stages["authn"] = (time.perf_counter() - stage_start) * 1000.0
            if quota is not None:
                stage_start = time.perf_counter()
                quota.admit(principal, scope)
                stages["admission"] = (time.perf_counter() - stage_start) * 1000.0
            stage_start = time.perf_counter()
            context = await provider.get(
                principal, UserRef(principal.tenant, user_key(user)), Capability(capability)
            )
            stages["lookup_assemble"] = (time.perf_counter() - stage_start) * 1000.0
            if metering is not None:
                metering.record(principal.tenant, "rest", "requests")
            return context
        except PlatformError as exc:
            error_code = exc.code
            if exc.code == "quota_exceeded" and metering is not None and principal is not None:
                metering.record(principal.tenant, "rest", "throttled")
            raise HTTPException(status_code=_status_for(exc), detail=exc.to_payload()) from exc
        finally:
            if sink is not None:
                sink.append(
                    SpanStub(
                        name="gmp.channel.rest.request",
                        tenant=(principal.tenant if principal is not None else TenantId("unknown")),
                        at=datetime.now(tz=UTC),
                        duration_ms=(time.perf_counter() - started) * 1000.0,
                        attributes={
                            "stages_ms": stages,
                            "scope": str(scope),
                            "user": user,
                            "capability": capability,
                            "outcome": error_code or "success",
                        },
                    )
                )

    @app.get("/v1/context/{user}/{capability}")
    async def get_context(
        user: str,
        capability: str,
        if_none_match: Annotated[str | None, Header()] = None,
        authorization: Annotated[str | None, Header()] = None,
    ) -> Response:
        ctx = await _context_of(authorization, Scope.CONTEXT_READ, user, capability)
        generated = ctx.generated_at.isoformat()
        etag = _etag_for(ctx.cache_scope, generated, ctx.payload)
        if if_none_match == etag:
            return Response(
                status_code=304,
                headers={
                    "ETag": etag,
                    "Cache-Control": f"max-age={max(1, ctx.max_age_ms // 1000)}",
                    "Vary": "Authorization",
                },
            )
        body = {
            "capability": str(ctx.capability),
            "payload": ctx.payload,
            "generated_at": generated,
            "stale_at": _stale_iso(ctx.generated_at, ctx.max_age_ms),
            "max_age_ms": ctx.max_age_ms,
            "cache_scope": ctx.cache_scope,
        }
        return Response(
            content=json.dumps(body, sort_keys=True),
            media_type="application/json",
            headers={
                "ETag": etag,
                "Cache-Control": f"max-age={max(1, ctx.max_age_ms // 1000)}",
                "Vary": "Authorization",
            },
        )

    @app.get("/v1/context/{user}/{capability}/items")
    async def get_context_items(
        user: str,
        capability: str,
        limit: int = 20,
        cursor: str | None = None,
        authorization: Annotated[str | None, Header()] = None,
    ) -> JSONResponse:
        ctx = await _context_of(authorization, Scope.CONTEXT_READ, user, capability)
        try:
            page_size = max(1, min(limit, _MAX_ITEM_LIMIT))
            ranked = _ranked_items(ctx.payload)
            start = 0
            if cursor is not None:
                after_kind, after_target = _decode_cursor(cursor)
                for index, (kind, item) in enumerate(ranked):
                    if not isinstance(item, dict):
                        continue
                    if kind == after_kind and str(item.get("target", "")) == after_target:
                        start = index + 1
                        break
            page = ranked[start : start + page_size]
            next_cursor: str | None = None
            last_item = page[-1][1] if page else None
            if len(ranked) > start + page_size and isinstance(last_item, dict):
                next_cursor = _encode_cursor(page[-1][0], str(last_item.get("target", "")))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={"code": "invalid_cursor"}) from exc
        return JSONResponse(
            content={
                "items": [item for _kind, item in page],
                "next_cursor": next_cursor,
                "generated_at": ctx.generated_at.isoformat(),
                "max_age_ms": ctx.max_age_ms,
            }
        )

    @app.post("/v1/context/{user}/{capability}/prefetch")
    async def prefetch_context(
        user: str,
        capability: str,
        authorization: Annotated[str | None, Header()] = None,
        idempotency_key: Annotated[str | None, Header()] = None,
    ) -> JSONResponse:
        ctx = await _context_of(authorization, Scope.CONTEXT_READ, user, capability)
        body: dict[str, Json] = {
            "materialized": True,
            "cache_scope": ctx.cache_scope,
            "max_age_ms": ctx.max_age_ms,
            "generated_at": ctx.generated_at.isoformat(),
        }
        if idempotency_key is None:
            return JSONResponse(content=body)
        tenant = (
            ctx.cache_scope.split("/", 2)[1] if "/" in ctx.cache_scope else str(ctx.user.tenant)
        )
        key = (f"{tenant}/{user}/{capability}", idempotency_key)
        expiry, stored = _replay.get(key, (0.0, body))
        if expiry > time.monotonic():
            return JSONResponse(content=stored, headers={"Idempotency-Replayed": "true"})
        _replay[key] = (time.monotonic() + _IDEMPOTENCY_TTL_S, body)
        return JSONResponse(content=body)

    return app
