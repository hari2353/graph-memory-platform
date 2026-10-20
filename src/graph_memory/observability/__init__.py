"""Observability package (SPEC §11): span audit + the Loop-2 observer port."""

from graph_memory.observability.observer import (
    InMemorySpanSink,
    LlmObserver,
    NoopObserver,
    ScriptedLlmObserver,
    SpanSink,
    SpanStub,
    noop_span_sink,
)

__all__ = [
    "InMemorySpanSink",
    "LlmObserver",
    "NoopObserver",
    "ScriptedLlmObserver",
    "SpanSink",
    "SpanStub",
    "noop_span_sink",
]
