"""The memory-mapping module: declarative specs and their deterministic compiler."""

from graph_memory.mapping.compiler import EdgeOp, NodeOp, ProjectionPlan, compile_plan
from graph_memory.mapping.spec import (
    VERSION,
    CapabilityRule,
    EdgeRule,
    MappingError,
    MappingSpec,
    NodeRule,
    SignalRule,
    envelope_context,
    load_spec,
    resolve_template,
)

__all__ = [
    "VERSION",
    "CapabilityRule",
    "EdgeOp",
    "EdgeRule",
    "MappingError",
    "MappingSpec",
    "NodeOp",
    "NodeRule",
    "ProjectionPlan",
    "SignalRule",
    "compile_plan",
    "envelope_context",
    "load_spec",
    "resolve_template",
]
