"""The SPEC §13 ``tests/e2e`` tier: the whole platform story, offline and
deterministic — tenancy, ingest dedup, projection, provider reads across the
loop boundary, the effectiveness tick, the loop's fixed point, and the §16.2
fresh-replay convergence.

Flow (§1): signals -> idempotent ledger -> per-user graph -> context reads ->
the tick reads ledger truth and writes (kind, target, cohort) weight rows ->
replay re-converges to identical rows, twice.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

from graph_memory.context.service import ContextCache, MemoryContextProvider
from graph_memory.core.clock import FixedClock
from graph_memory.core.types import (
    Capability,
    Json,
    Principal,
    Scope,
    TenantId,
    UserKey,
    UserRef,
    user_key,
)
from graph_memory.loop import ContinuousEffectivenessLoop, EffectivenessTick
from graph_memory.mapping.spec import MappingSpec, load_spec
from graph_memory.projector.engine import Projector
from graph_memory.projector.gateway import InMemoryGraphGateway
from graph_memory.signals.envelope import Envelope
from graph_memory.signals.ledger import InMemoryLedger, LedgerRecord, record_from_envelope
from graph_memory.tenancy.guard import StaticBearerGuard
from graph_memory.tenancy.registry import TenantRegistry

_T = TenantId("acme")
_USERS = ("user-a1", "user-b1")
_TOKEN = "dev-acme-token"
_SPEC = {
    "mapping_version": 1,
    "signals": {
        "itf.skill_assessed.v1": {
            "edges": [
                {
                    "kind": "HAS_SKILL",
                    "target": "skill:{data.skill}",
                    "weight": 1.0,
                    "half_life_days": 365,
                }
            ]
        },
        "itf.reco.v1": {
            "edges": [
                {
                    "kind": "RECOMMENDED",
                    "target": "skill:{data.skill}",
                    "weight": 0.5,
                    "half_life_days": 365,
                }
            ]
        },
        # Loop-only truth: outcomes ride the LEDGER, never the graph.
        "itf.reco_outcome.v1": {},
    },
    "effectiveness": {
        "reco_signals": ["itf.reco.v1"],
        "outcome_signals": ["itf.reco_outcome.v1"],
        "correlation_field": "data.correlation_id",
        "cohort_field": "data.cohort",
        "class_field": "data.reco_class",
        "verdict_field": "data.verdict",
    },
    "capabilities": {
        "skills": {"relations": ["HAS_SKILL"], "top_k": 5, "ttl_ms": 60000},
        "recos": {"relations": ["RECOMMENDED"], "top_k": 5, "ttl_ms": 60000},
    },
}
_MS = {"skills": 100, "reco": 200, "acted": 300, "dismissed": 400}


def _envelope(event_id: str, type_: str, user: str, ms: int, data: dict[str, object]) -> Envelope:
    return Envelope.parse(
        {
            "specversion": "1.0",
            "id": event_id,
            "source": "https://itf.example.com/signals",
            "type": type_,
            "time": f"2026-06-01T12:00:00.{ms:03d}Z",
            "partitionkey": user,
            "data": data,
        }
    )


def _ledger() -> InMemoryLedger:
    ledger = InMemoryLedger()
    records: list[LedgerRecord] = []
    for user in _USERS:
        cohort = "cohort-a" if user.endswith("a1") else "cohort-b"
        for skill in ("python", "rust"):
            records.append(
                record_from_envelope(
                    _T,
                    _envelope(
                        f"skill-{user}-{skill}",
                        "itf.skill_assessed.v1",
                        user,
                        _MS["skills"],
                        {"skill": skill},
                    ),
                )
            )
            reco_class = "gap" if skill == "python" else "explore"
            records.append(
                record_from_envelope(
                    _T,
                    _envelope(
                        f"r-{user}-{skill}",
                        "itf.reco.v1",
                        user,
                        _MS["reco"],
                        {"skill": skill, "cohort": cohort, "reco_class": reco_class},
                    ),
                )
            )
        # cohort-a acts on their gap reco (python), dismisses explore;
        # cohort-b is the mirror image (per-class histories differ).
        verdicts = {
            "user-a1": [("python", "acted_on"), ("rust", "dismissed")],
            "user-b1": [("python", "dismissed"), ("rust", "acted_on")],
        }
        for skill, verdict in verdicts[user]:
            ms_key = "acted" if verdict == "acted_on" else "dismissed"
            records.append(
                record_from_envelope(
                    _T,
                    _envelope(
                        f"outcome-{user}-{skill}",
                        "itf.reco_outcome.v1",
                        user,
                        _MS[ms_key],
                        {"correlation_id": f"r-{user}-{skill}", "verdict": verdict},
                    ),
                )
            )
    ledger.append(records)
    return ledger


def _spec() -> MappingSpec:
    return load_spec(json.dumps(_SPEC))


def _rows_hash(gateway: InMemoryGraphGateway) -> list[tuple[str, str, float, str]]:
    """Graph fingerprint: every observation row, canonically ordered."""
    rows: list[tuple[str, str, float, str]] = []
    for user in _USERS:
        row_list = gateway.observations(_T, UserKey(user))
        rows.extend(
            (row.kind, row.target, round(row.weight, 9), row.valid_at.isoformat())
            for row in sorted(row_list, key=lambda item: (item.kind, item.target))
        )
    return sorted(rows)


def _provider(
    spec: MappingSpec, ledger: InMemoryLedger, gateway: InMemoryGraphGateway
) -> MemoryContextProvider:
    engine = Projector(spec, ledger, gateway, FixedClock(datetime(2026, 6, 10, tzinfo=UTC)))
    engine.replay()
    return MemoryContextProvider(engine, ContextCache())


def _first_target(payload: dict[str, object] | dict[str, Json]) -> str:
    """The top item's target, narrowed the honest (typed) way."""
    items = payload.get("items")
    if not isinstance(items, list) or not items or not isinstance(items[0], dict):
        return ""
    return str(items[0].get("target", ""))


async def test_full_loop_journey() -> None:
    spec = _spec()
    clock = FixedClock(datetime(2026, 6, 10, tzinfo=UTC))

    # --- tenancy: the metadata row (no DDL anywhere) -------------------
    registry = TenantRegistry(clock)
    record = registry.onboard("acme")
    assert record.state == "active"
    assert registry.count() == 1

    # --- ingest guards to ledger: duplicates acked, not stored twice ----
    guard: StaticBearerGuard = StaticBearerGuard({_TOKEN: "acme"})
    principal: Principal = guard.require_scope(f"Bearer {_TOKEN}", Scope.CONTEXT_READ)
    assert principal.tenant == "acme"

    ledger = _ledger()
    assert ledger.tenants() == [_T]
    assert len(ledger.read_events(_T)) == 12  # 2 users x (2 skills + 2 recos + 2 outcomes)
    duplicate = record_from_envelope(
        _T,
        _envelope(
            "r-user-a1-python",
            "itf.reco.v1",
            "user-a1",
            _MS["reco"],
            {"skill": "python", "cohort": "cohort-a", "reco_class": "gap"},
        ),
    )
    ledger.append([duplicate])  # at-least-once replay is a no-op
    assert len(ledger.read_events(_T)) == 12

    # --- baseline reads: identical ranked recos for every cohort --------
    baseline = InMemoryGraphGateway()
    baseline_provider = _provider(spec, ledger, baseline)
    baseline_tops: dict[str, str] = {}
    for user in _USERS:
        ref = UserRef(tenant=principal.tenant, user_key=user_key(user))
        ctx = await baseline_provider.get(principal, ref, Capability("recos"))
        baseline_tops[user] = _first_target(ctx.payload)
    assert baseline_tops["user-a1"] == baseline_tops["user-b1"]

    # --- the loop, wired the way serve runs it (continuous) ---------------
    continuous = ContinuousEffectivenessLoop(spec, ledger, clock)
    applied_gateway = InMemoryGraphGateway()
    applied_engine = Projector(
        spec, ledger, applied_gateway, clock, weight_overrides=continuous.rows()
    )
    applied_engine.replay()
    report = EffectivenessTick(spec, ledger, clock).score(_T)
    assert report.reco_delivered == 4
    assert report.attached == 4  # every outcome correlated; nothing silent
    assert report.unattached == 0
    rows = set(report.weight_overrides)
    assert all(len(key) == 3 for key in rows), "weight rows are (kind, target, cohort)"

    # --- post-loop reads: cohorts DIVERGE in their top reco --------------
    loop_provider = MemoryContextProvider(applied_engine, ContextCache())
    loop_tops: dict[str, str] = {}
    for user in _USERS:
        ref = UserRef(tenant=principal.tenant, user_key=user_key(user))
        ctx = await loop_provider.get(principal, ref, Capability("recos"))
        loop_tops[user] = _first_target(ctx.payload)
    assert loop_tops["user-a1"] != loop_tops["user-b1"]  # the 16.1 flagship

    # --- the loop reaches a fixed point: another pass changes nothing ----
    snapshot = _rows_hash(applied_gateway)
    continuous.pass_over(applied_engine)
    assert _rows_hash(applied_gateway) == snapshot

    # --- 16.2: a fresh boot (tick + replay) converges to the live graph ---
    # The loop is part of the deterministic boot path: a fresh empty graph
    # running the same (ledger, spec, clock) lands on identical rows — that
    # is the replay-convergence identity the live tier relies on.
    fresh = InMemoryGraphGateway()
    fresh_engine = Projector(spec, ledger, fresh, clock, weight_overrides=continuous.rows())
    fresh_engine.replay()
    assert _rows_hash(fresh) == snapshot
