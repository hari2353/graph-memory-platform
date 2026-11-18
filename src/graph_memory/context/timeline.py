"""User change timeline — Letta's 'every edit is a git commit' audit, on
this platform's append-only ledger: what changed for one user, in
ingestion order, bounded. The ledger is the truth; this is a read-only
projection of it."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from graph_memory.core.types import TenantId, UserKey
from graph_memory.signals.ledger import LedgerRepository


@dataclass(frozen=True)
class TimelineEntry:
    sequence: int  # ingestion order within this read
    signal: str  # the signal type that produced the change
    dedup_key: str  # evidence: which signal exactly
    occurred_at: datetime  # when it happened (event time)
    ingested_at: datetime  # when the platform learned it


def user_history(
    ledger: LedgerRepository,
    tenant: TenantId,
    user: UserKey,
    *,
    since: datetime | None = None,
    limit: int = 200,
) -> list[TimelineEntry]:
    """One user's change history from the append-only ledger, newest last
    (ingestion order), bounded by ``limit``."""
    limit = max(1, min(limit, 10_000))
    events = ledger.read_events(tenant, user_key=user, limit=limit)
    entries = [
        TimelineEntry(
            sequence=position,
            signal=str(event.signal),
            dedup_key=event.dedup_key,
            occurred_at=event.occurred_at,
            ingested_at=event.ingested_at,
        )
        for position, event in enumerate(events, start=1)
    ]
    if since is not None:
        entries = [e for e in entries if e.occurred_at >= since]
    return entries
