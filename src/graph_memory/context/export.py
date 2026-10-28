"""The portability face (M4.5, research 06): ONE user's export archive.

``GET /v1/export/{user}`` returns the read model — every declared
capability's snapshot for that user — plus their ledger-scoped event
history, bounded and oldest-first. GDPR portability is a first-class
READ here, not a database dump: the same tenant rule as every other
read, the same bounded-payload discipline, and zero mutation. Snapshots
reuse the exact ``engine.snapshot`` read-model core the context provider
serves, so an export is the union of point reads — never a shadow format
 Peace of mind for porters: what the app saw is exactly what it exported.
"""

from __future__ import annotations

from typing import final

from graph_memory.core.types import Capability, Json, Principal, UserRef, require_tenant_match
from graph_memory.projector.engine import Projector
from graph_memory.signals.ledger import LedgerRepository, StoredEvent

_MAX_EXPORT_EVENTS = 1000
_MAX_COUNT_COPY = 1_000_000


def _history_record(event: StoredEvent) -> dict[str, Json]:
    """One ledger event as the user's own record (no enrichment, no scrub)."""
    envelope = event.envelope
    return {
        "id": envelope.id,
        "source": envelope.source,
        "type": envelope.type,
        "time": envelope.time.isoformat(),
        "data": envelope.data,
    }


@final
class ExportService:
    """Implements ``core.ports.MemoryExport`` over the served projector.

    Sync by contract — channels run it off the event loop. One injected
    read: tenant from the verified principal, per-user snapshots, per-user
    bounded history; determinism rides the injected clock.
    """

    def __init__(
        self,
        engine: Projector,
        ledger: LedgerRepository,
        *,
        max_events: int = _MAX_EXPORT_EVENTS,
    ) -> None:
        self._engine = engine
        self._ledger = ledger
        self._max_events = max_events

    def export(self, principal: Principal, user: UserRef) -> dict[str, Json]:
        require_tenant_match(principal, user)
        tenant = principal.tenant
        now = self._engine.clock.now()
        capabilities: dict[str, Json] = {}
        for name in sorted(self._engine.spec.capabilities):
            capabilities[name] = self._engine.snapshot(
                tenant, user.user_key, Capability(name)
            ).payload
        # One-over probe so truncation is HONEST in the payload itself.
        events = self._ledger.read_events(
            tenant, user_key=user.user_key, limit=min(self._max_events + 1, _MAX_COUNT_COPY)
        )
        truncated = len(events) > self._max_events
        history: list[Json] = [
            _history_record(event) for event in events[: self._max_events]
        ]
        return {
            "user": str(user.user_key),
            "generated_at": now.isoformat(),
            "capabilities": capabilities,
            "history": history,
            "counts": {"capabilities": len(capabilities), "events": len(history)},
            "history_truncated": truncated,
        }
