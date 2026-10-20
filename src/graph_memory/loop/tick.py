"""The effectiveness loop (SPEC §10 — the differentiator).

Pipeline in one deterministic tick:

1. ``EffectivenessTick.score(tenant)`` reads the LEDGER (truth), attaches
   outcome verdicts to recommendation origins by ``correlation_id``, and
   aggregates per ``(cohort, recommendation-class)`` — cohorts are whatever
   attribute the app declares (``cohort_field``), never a platform opinion.
2. Each cell becomes a Laplace-smoothed acceptance rate → a weight
   multiplier (clamped ``[0.5, 1.5]``) for the target the recommendation
   pointed at.
3. ``apply_effectives`` feeds those weight rows back through the projector:
   ``compile_plan(..., weight_overrides=...)`` multiplies INSIDE the pure
   function, so replay convergence is preserved (same events + same
   overrides + same clock ⇒ byte-identical graph). No LLM in the kernel —
   that is Loop 2's separate port (``observability/observer.py``).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from graph_memory.core.clock import Clock
from graph_memory.core.types import Json, TenantId
from graph_memory.mapping.compiler import compile_plan
from graph_memory.mapping.spec import EffectivenessSpec, MappingSpec
from graph_memory.projector.engine import Projector
from graph_memory.projector.gateway import GraphGateway
from graph_memory.signals.ledger import LedgerRepository

_MIN_MULTIPLIER = 0.5
_MAX_MULTIPLIER = 1.5


@dataclass(frozen=True)
class CellEffectiveness:
    """One (cohort, recommendation-class) cell's effectiveness."""

    cohort: str
    reco_signal: str
    capability: str | None
    attached: int
    positive: int
    rate: float
    multiplier: float


@dataclass(frozen=True)
class EffectivenessReport:
    """The tick's evidence + the weight rows it wrote (audit trail)."""

    tenant: TenantId
    reco_delivered: int
    attached: int
    unattached: int
    cells: tuple[CellEffectiveness, ...]
    weight_overrides: dict[tuple[str, str], float] = field(default_factory=dict)


class EffectivenessTick:
    """Deterministic scoring job over ledger truth (+ the mapping spec)."""

    def __init__(self, spec: MappingSpec, ledger: LedgerRepository, clock: Clock) -> None:
        self._spec = spec
        self._ledger = ledger
        self._clock = clock

    def score(self, tenant: TenantId) -> EffectivenessReport:
        config = self._spec.effectiveness
        if config is None:
            return EffectivenessReport(
                tenant=tenant, reco_delivered=0, attached=0, unattached=0, cells=()
            )
        origins: dict[str, tuple[str, str, list[tuple[str, str]]]] = {}
        cells: dict[tuple[str, str], list[bool]] = {}
        attached = 0
        unattached = 0
        for event in self._ledger.read_events(tenant):
            envelope = event.envelope
            if envelope.type in config.reco_signals:
                origins[envelope.dedup_key] = (
                    _route(envelope, config.cohort_field) or "",
                    envelope.type,
                    self._edge_targets(tenant, envelope),
                )
            elif envelope.type in config.outcome_signals:
                correlation = _route(envelope, config.correlation_field)
                verdict = _route(envelope, config.verdict_field) or ""
                origin = origins.get(correlation) if correlation else None
                if origin is None:
                    unattached += 1
                    continue
                cohort, reco_signal, _targets = origin
                positive = verdict in config.verdicts_positive
                cells.setdefault((cohort, reco_signal), []).append(positive)
                attached += 1
        return self._assemble(tenant, config, origins, cells, attached, unattached)

    def _assemble(
        self,
        tenant: TenantId,
        config: EffectivenessSpec,
        origins: dict[str, tuple[str, str, list[tuple[str, str]]]],
        cells_raw: dict[tuple[str, str], list[bool]],
        attached: int,
        unattached: int,
    ) -> EffectivenessReport:
        cells: list[CellEffectiveness] = []
        rates: dict[tuple[str, str], float] = {}
        sizes: dict[tuple[str, str], int] = {}
        # Sorted (cohort, class) order — determinism over dict insertion.
        for (cohort, reco_signal), verdicts in sorted(cells_raw.items()):
            positive = sum(1 for verdict in verdicts if verdict)
            total = len(verdicts)
            rate = (positive + config.alpha) / (total + 2.0 * config.alpha)
            multiplier = max(_MIN_MULTIPLIER, min(_MAX_MULTIPLIER, 0.5 + rate))
            cells.append(
                CellEffectiveness(
                    cohort=cohort,
                    reco_signal=reco_signal,
                    capability=self._capability_of(reco_signal),
                    attached=total,
                    positive=positive,
                    rate=rate,
                    multiplier=multiplier,
                )
            )
            rates[(cohort, reco_signal)] = multiplier
            sizes[(cohort, reco_signal)] = total
        # Weight rows: one multiplier per (kind, target). Overlapping cohorts
        # resolve deterministically — the cell with the most outcomes wins;
        # ties go to the lexicographically-first cohort (sorted iteration).
        overrides: dict[tuple[str, str], tuple[int, str, float]] = {}
        for cohort, reco_signal in sorted(cells_raw):
            multiplier = rates[(cohort, reco_signal)]
            for origin in origins.values():
                if origin[1] != reco_signal or origin[0] != cohort:
                    continue
                for kind_target in origin[2]:
                    previous = overrides.get(kind_target)
                    if previous is None or sizes[(cohort, reco_signal)] > previous[0]:
                        overrides[kind_target] = (sizes[(cohort, reco_signal)], cohort, multiplier)
        rows = {key: value[2] for key, value in overrides.items()}
        return EffectivenessReport(
            tenant=tenant,
            reco_delivered=len(origins),
            attached=attached,
            unattached=unattached,
            cells=tuple(cells),
            weight_overrides=rows,
        )

    def _edge_targets(self, tenant: TenantId, envelope: object) -> list[tuple[str, str]]:
        """(kind, target) pairs the reco signal's rule projects to (compile-only)."""
        from graph_memory.signals.envelope import Envelope as EnvelopeT

        assert isinstance(envelope, EnvelopeT)
        plan = compile_plan(self._spec, tenant, envelope)
        return [(op.kind, op.target) for op in plan.edges]

    def _capability_of(self, reco_signal: str) -> str | None:
        """First (alphabetical) capability exposing the signal's edge kinds."""
        rule = self._spec.signals.get(reco_signal)
        if rule is None:
            return None
        kinds = {edge.kind for edge in rule.edges}
        for name in sorted(self._spec.capabilities):
            if kinds & set(self._spec.capabilities[name].relations):
                return name
        return None


def apply_effectives(
    spec: MappingSpec,
    ledger: LedgerRepository,
    gateway: GraphGateway,
    clock: Clock,
) -> dict[TenantId, EffectivenessReport]:
    """Tick every tenant, then replay through a projector carrying the rows."""
    reports: dict[TenantId, EffectivenessReport] = {}
    overrides: dict[tuple[str, str], float] = {}
    for tenant in ledger.tenants():
        report = EffectivenessTick(spec, ledger, clock).score(tenant)
        reports[tenant] = report
        for key, multiplier in report.weight_overrides.items():
            current = overrides.get(key)
            if current is None:
                overrides[key] = multiplier
    engine = Projector(spec, ledger, gateway, clock, weight_overrides=overrides or None)
    engine.replay()
    return reports


def _route(envelope: object, field_path: str) -> str | None:
    """``data.cohort`` / ``data.meta.cohort`` → string, or None when absent."""
    from graph_memory.signals.envelope import Envelope as EnvelopeT

    assert isinstance(envelope, EnvelopeT)
    parts = field_path.split(".")
    if parts[0] != "data":
        raise ValueError(f"effectiveness fields must route through data: {field_path!r}")
    node: Json = dict(envelope.data)
    for part in parts[1:]:
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    if isinstance(node, (dict, list)):
        raise ValueError(f"effectiveness field {field_path!r} must be scalar")
    return str(node)
