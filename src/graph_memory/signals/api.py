"""HTTP ingest edge: ``POST /v1/signals:batch`` — the first broker-free sink.

Auth is enforced at the edge; the tenant comes ONLY from the guard's
claims-derived principal (SPEC §8: the tenant is never a tool/body choice).
Errors render the stable ``PlatformError`` payloads; sync IO runs off the
event loop (``asyncio.to_thread``).
"""

from __future__ import annotations

import asyncio
from typing import Annotated

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from graph_memory.core.errors import PlatformError, ScopeDenied
from graph_memory.core.ports import TenantGuard
from graph_memory.core.types import Json, Scope
from graph_memory.settings import Settings
from graph_memory.signals.ledger import LedgerRepository
from graph_memory.signals.service import PoisonSink
from graph_memory.tenancy.quotas import QuotaGuard

_HEALTH = {"status": "ok"}


class BatchSignalsRequest(BaseModel):
    """Raw members; each item is adjudicated by the service, never the edge."""

    signals: list[dict[str, Json]]


class RejectedItem(BaseModel):
    index: int
    error: dict[str, Json]


class BatchSignalsResponse(BaseModel):
    accepted: int
    duplicates: int
    rejected: list[RejectedItem]


def _status_for(exc: PlatformError) -> int:
    """Stable code -> HTTP status; scope denial splits on its failure reason."""
    if exc.code == "scope_denied":
        details = exc.details or {}
        unknown = details.get("reason") == "unknown_credential"
        return 401 if unknown else 403
    if exc.code in {"tenant_revoked", "tenant_suspended", "tenant_not_found"}:
        return 403
    return {
        "envelope_invalid": 400,
        "tenant_mismatch": 400,
        "quota_exceeded": 429,
        "batch_over_budget": 429,
    }.get(exc.code, 500)


def create_app(
    ledger: LedgerRepository,
    guard: TenantGuard,
    *,
    settings: Settings | None = None,
    poison: PoisonSink | None = None,
    quota: QuotaGuard | None = None,
) -> FastAPI:
    """Assemble the ingest app around injected ports (no framework leakage)."""
    from graph_memory.signals.service import IngestService  # local: avoids cycles

    app = FastAPI(title="graph-memory-platform", version="0.1.0")
    bounds = settings or Settings()

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return _HEALTH

    @app.post("/v1/signals:batch", response_model=BatchSignalsResponse)
    async def ingest_batch(
        body: BatchSignalsRequest,
        authorization: Annotated[str | None, Header()] = None,
    ) -> BatchSignalsResponse:
        try:
            principal = guard.require_scope(_bearer_from(authorization), Scope.SIGNALS_WRITE)
        except PlatformError as exc:
            raise HTTPException(status_code=_status_for(exc), detail=exc.to_payload()) from exc
        if quota is not None:
            quota.admit(principal, Scope.SIGNALS_WRITE, amount=len(body.signals) or 1)
        service = IngestService(principal, ledger, settings=bounds, poison=poison)
        try:
            outcome = await asyncio.to_thread(service.apply_raw, list(body.signals))
        except PlatformError as exc:
            raise HTTPException(status_code=_status_for(exc), detail=exc.to_payload()) from exc
        return BatchSignalsResponse(
            accepted=outcome.result.accepted,
            duplicates=outcome.result.duplicates,
            rejected=[RejectedItem(index=r.index, error=r.error) for r in outcome.rejected],
        )

    return app


def _bearer_from(authorization: str | None) -> str:
    """Header value or empty string; guard rejects empty as unknown_credential."""
    if authorization is None:
        raise ScopeDenied("missing Authorization header", details={"reason": "unknown_credential"})
    return authorization
