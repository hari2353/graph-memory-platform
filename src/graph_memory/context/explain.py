"""The read faces the composition root wires for provenance + loop tools.

Two ports from ``core.ports`` get their real renderers here — both are
read-only views built over the SAME engine the provider serves with, so the
explanation can never contradict the served graph:

- ``effectiveness_reporter`` — renders one tenant's ``EffectivenessTick``
  report as channel JSON (the ``get_effectiveness`` tool / ``platform loop run``).
- ``context_explainer`` — graph-annotated provenance for one read: the raw
  edge observations under a snapshot, per-observation decayed score, and
  which mapping signals can produce each edge kind.

Adapters compose closures behind two final façade classes so the ports stay
satisfiable under ``mypy --strict`` while the wiring edge passes functions.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import cast, final

from graph_memory.core.clock import Clock
from graph_memory.core.types import (
    Capability,
    Json,
    Principal,
    TenantId,
    UserRef,
)
from graph_memory.loop.tick import EffectivenessTick
from graph_memory.mapping.spec import MappingSpec
from graph_memory.projector.engine import CapabilityUnknown, Projector, decayed_score
from graph_memory.signals.ledger import LedgerRepository


@final
class EffectivenessScorerAdapter:
    """Façade: turn a tenant -> JSON renderer into the ``EffectivenessScorer``."""

    def __init__(self, render: Callable[[TenantId], dict[str, Json]]) -> None:
        self._render = render

    def score(self, tenant: TenantId) -> dict[str, Json]:
        return self._render(tenant)


@final
class ContextExplainerAdapter:
    """Façade: turn a provenance renderer into the ``ContextExplainer``."""

    def __init__(self, render: Callable[[Principal, UserRef, Capability], dict[str, Json]]) -> None:
        self._render = render

    def explain(
        self, principal: Principal, user: UserRef, capability: Capability
    ) -> dict[str, Json]:
        return self._render(principal, user, capability)


def cast_to_json_list(values: list[str]) -> list[Json]:
    """``list[str]`` into the ``Json`` union (invariant list; verifier-checked text)."""
    return cast("list[Json]", values)


def effectiveness_reporter(
    spec: MappingSpec, ledger: LedgerRepository, clock: Clock
) -> EffectivenessScorerAdapter:
    """The loop's JSON face for channels ( tenants come from claims, always )."""
    tick = EffectivenessTick(spec, ledger, clock)

    def render(tenant: TenantId) -> dict[str, Json]:
        report = tick.score(tenant)
        return {
            "tenant": str(report.tenant),
            "reco_delivered": report.reco_delivered,
            "attached": report.attached,
            "unattached": report.unattached,
            "cells": [
                {
                    "cohort": cell.cohort,
                    "reco_class": cell.reco_class,
                    "reco_signal": cell.reco_signal,
                    "capability": cell.capability,
                    "attached": cell.attached,
                    "positive": cell.positive,
                    "rate": cell.rate,
                    "multiplier": cell.multiplier,
                }
                for cell in report.cells
            ],
            "weight_overrides": {
                f"{kind}|{target}|{cohort}": multiplier
                for (kind, target, cohort), multiplier in report.weight_overrides.items()
            },
        }

    return EffectivenessScorerAdapter(render)


def context_explainer(engine: Projector) -> ContextExplainerAdapter:
    """Provenance over the same engine the provider serves from (§5 outputs)."""

    def render(principal: Principal, ref: UserRef, capability: Capability) -> dict[str, Json]:
        rule = engine.spec.capabilities.get(capability)
        if rule is None:
            raise CapabilityUnknown(
                "capability is not declared by the mapping spec",
                details={"capability": str(capability)},
            )
        now = engine.clock.now()
        producible: dict[str, set[str]] = {}
        for name, signal_rule in engine.spec.signals.items():
            for edge in signal_rule.edges:
                producible.setdefault(edge.kind, set()).add(name)
        rows = [
            row
            for row in engine.gateway.observations(principal.tenant, ref.user_key)
            if row.kind in rule.relations
        ]
        # Governed forgetting left an audit trail (M4.1): the directives that
        # superseded this user's observations are provenance, shown verbatim.
        forgotten: list[Json] = [
            {
                "kind": directive.kind,
                "target": directive.target,
                "scope": "kind_wide" if directive.target is None else "target",
                "forget_at": directive.forget_at.isoformat(),
            }
            for directive in engine.gateway.forget_directives(principal.tenant, ref.user_key)
        ]
        # Score once, sort on the tuple (provenance output is deterministic).
        scored: list[tuple[float, str, str, dict[str, Json]]] = []
        for row in rows:
            score = round(decayed_score(row.weight, row.half_life_days, row.valid_at, now), 9)
            produced_by = cast_to_json_list(sorted(producible.get(row.kind, set())))
            scored.append(
                (
                    score,
                    row.kind,
                    row.target,
                    {
                        "kind": row.kind,
                        "target": row.target,
                        "weight": row.weight,
                        "half_life_days": row.half_life_days,
                        "valid_at": row.valid_at.isoformat(),
                        "score": score,
                        "produced_by": produced_by,
                    },
                )
            )
        scored.sort(key=lambda entry: (-entry[0], entry[1], entry[2]))
        items: list[Json] = [entry[3] for entry in scored]
        producible_map: dict[str, Json] = {
            kind: cast_to_json_list(sorted(producers))
            for kind, producers in sorted(producible.items())
            if kind in rule.relations
        }
        relations: list[Json] = cast_to_json_list(sorted(rule.relations))
        return {
            "tenant": str(principal.tenant),
            "user": str(ref.user_key),
            "capability": str(capability),
            "generated_at": now.isoformat(),
            "relations": relations,
            "top_k": rule.top_k,
            "observations": items,
            "produced_by_map": producible_map,
            "forget_directives": forgotten,
        }

    return ContextExplainerAdapter(render)


__all__ = [
    "ContextExplainerAdapter",
    "EffectivenessScorerAdapter",
    "context_explainer",
    "effectiveness_reporter",
]
