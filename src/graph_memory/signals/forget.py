"""The governed forget: user memory controls -> a ledger event (M4.1).

research 06 M4.1: a forget is a signal like any other. This adapter binds the
mapping spec's declared forget rules to per-kind routes, validates the
requested target SHAPE against them (kind-wide vs scoped), and appends a
one-shot CloudEvents envelope under the caller's verified principal — tenant
from claims, never an argument (SPEC §8). No core state is touched from
here: the projector's pure supersession takes over on the next projection
pass, which keeps the forget replay-convergent by construction.

The forget event's ``time`` comes from the injected Clock (kernel purity);
its ``id`` is a fresh unique id per call — repeated calls become duplicate
DIRECTIVES at the gateway level (same identity, idempotent), never errors.
"""

from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from graph_memory.core.clock import Clock
from graph_memory.core.errors import (
    ForgetKindUnknown,
    ForgetTargetMismatch,
)
from graph_memory.core.types import Json, Principal, UserRef, require_tenant_match
from graph_memory.mapping.spec import MappingSpec
from graph_memory.signals.envelope import Envelope
from graph_memory.signals.ledger import LedgerRecord, LedgerRepository

_FORGET_SOURCE = "gmp://memory"


def forget_routes(
    spec: MappingSpec,
) -> dict[str, tuple[tuple[str, str | None], ...]]:
    """kind -> (signal type, declared target rule) pairs, deterministically.

    ``None`` target rule means the forget signal invalidates the WHOLE kind;
    a rule with a target template requires a concrete target. Signals are
    visited in sorted-name order and rules in declared order, so the route
    table is a pure function of the spec version (no dict-order guesses).
    """
    routes: dict[str, list[tuple[str, str | None]]] = {}
    for name in sorted(spec.signals):
        signal_rule = spec.signals[name]
        for forget in signal_rule.forgets:
            routes.setdefault(forget.kind, []).append((name, forget.target))
    return {kind: tuple(entries) for kind, entries in routes.items()}


class ForgetService:
    """Implements ``core.ports.MemoryForget``: one call => one ledger event."""

    def __init__(
        self,
        ledger: LedgerRepository,
        clock: Clock,
        *,
        routes: dict[str, tuple[tuple[str, str | None], ...]],
    ) -> None:
        self._ledger = ledger
        self._clock = clock
        self._routes = routes

    def forget(
        self, principal: Principal, user: UserRef, kind: str, target: str | None
    ) -> dict[str, Json]:
        """Emit the tombstone event; return its identity + ingest counts."""
        require_tenant_match(principal, user)
        entries = self._routes.get(kind)
        if entries is None:
            raise ForgetKindUnknown(
                "kind is not declared forgettable by the mapping spec",
                details={"kind": kind},
            )
        route = self._route_for(entries, target)
        if route is None:
            raise ForgetTargetMismatch(
                "forget target shape does not match any declared rule for this kind",
                details={"kind": kind, "target": target},
            )
        signal_name, _declared_target = route
        data: dict[str, Json] = {"kind": kind}
        if target is not None:
            data["target"] = target
        envelope = _forget_envelope(
            principal=principal, user=user, signal=signal_name, at=self._clock.now(), data=data
        )
        result = self._ledger.append([LedgerRecord(tenant=principal.tenant, envelope=envelope)])
        return {
            "forgotten": {
                "user": str(user.user_key),
                "kind": kind,
                "target": target,
                "signal": envelope.type,
                "event_id": envelope.id,
                "time": envelope.occurred_at.isoformat(),
            },
            "accepted": result.accepted,
            "duplicates": result.duplicates,
        }

    def _route_for(
        self, entries: tuple[tuple[str, str | None], ...], target: str | None
    ) -> tuple[str, str | None] | None:
        """First declared route whose target SHAPE matches the call.

        Kind-wide rules (declared target absent) serve ``target is None``
        calls; scoped rules (declared template) serve concrete targets.
        """
        for signal_name, declared_target in entries:
            if (declared_target is None) == (target is None):
                return signal_name, declared_target
        return None


def _forget_envelope(
    *,
    principal: Principal,
    user: UserRef,
    signal: str,
    at: datetime,
    data: dict[str, Json],
) -> Envelope:
    """One governed CloudEvent in the caller's own tenant/user frame."""
    return Envelope(
        id=f"forget-{uuid4().hex}",
        source=f"{_FORGET_SOURCE}/{principal.tenant}",
        type=signal,
        time=at,
        partitionkey=str(user.user_key),
        tenantid=str(principal.tenant),
        data=data,
    )
