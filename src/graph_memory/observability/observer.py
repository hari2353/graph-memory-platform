"""Observability ports (SPEC §11/R7 + §10 Loop 2): span audit + LLM observer.

Two ports, both strictly OUTSIDE the deterministic kernel:

- ``SpanSink`` — every hot-path request emits one span record; unsampled
  traffic still lands here (the CH audit-span-stub table contract: full
  auditability without egress cost). Langfuse/OTel exporters are
  deployment-edge adapters over this same surface.
- ``LlmObserver`` — Loop 2's research hook over effectiveness reports.
  Verdicts are ADVISORY: recorded as spans, never allowed to mutate graph
  state or block the deterministic tick (kernel purity, SPEC §10).
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from graph_memory.core.clock import Clock
from graph_memory.core.types import Json, TenantId
from graph_memory.loop.tick import EffectivenessReport

_OBSERVER_PREFIX = "gmp.observer."


@dataclass(frozen=True)
class SpanStub:
    """One audit span as stored (the CH span-stub table's row shape)."""

    name: str
    tenant: TenantId
    at: datetime
    duration_ms: float
    attributes: dict[str, Json]


def noop_span_sink(span: SpanStub) -> None:
    """The unsampled-drop sink: acknowledged, never stored."""
    _ = span


class SpanSink(Protocol):
    """Where audit spans go; backends append (never rewrite)."""

    def append(self, span: SpanStub) -> None: ...


class InMemorySpanSink:
    """Deterministic span list for tests and the scripted eval runs."""

    def __init__(self) -> None:
        self._spans: list[SpanStub] = []

    def append(self, span: SpanStub) -> None:
        self._spans.append(span)

    def __iter__(self) -> Iterator[SpanStub]:
        return iter(list(self._spans))

    def __len__(self) -> int:
        return len(self._spans)


class LlmObserver(Protocol):
    """Loop 2: reviews an effectiveness report, replies with advisory attrs."""

    def review(self, report: EffectivenessReport) -> dict[str, Json]: ...


class ScriptedLlmObserver:
    """Loop-2 stand-in: a fixed script answers per report, spans recorded.

    The script receives the report and returns the observer's attributes
    (verdict + reason). It can propose NOTHING — the deterministic tick
    already ran; the observer's output is evidence, not control flow.
    """

    def __init__(
        self,
        script: Callable[[EffectivenessReport], dict[str, Json]],
        sink: SpanSink,
        clock: Clock,
    ) -> None:
        self._script = script
        self._sink = sink
        self._clock = clock

    def review(self, report: EffectivenessReport) -> dict[str, Json]:
        answered = self._script(report)
        self._sink.append(
            SpanStub(
                name=f"{_OBSERVER_PREFIX}review",
                tenant=report.tenant,
                at=self._clock.now(),
                duration_ms=0.0,
                attributes={
                    "gmp.cells_reviewed": len(report.cells),
                    "gmp.weight_rows": len(report.weight_overrides),
                    **{f"{_OBSERVER_PREFIX}{key}": value for key, value in answered.items()},
                },
            )
        )
        return answered


class NoopObserver:
    """The zero-cost observer: nothing to review, nothing recorded."""

    def review(self, report: EffectivenessReport) -> dict[str, Json]:
        _ = report
        return {}
