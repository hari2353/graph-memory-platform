"""The declarative memory-mapping spec: signals -> nodes/edges + capabilities.

v1 grammar (additive-only evolution — SPEC §5, §18):
- ``signals``: per full signal type, node/edge upsert rules. Node keys and
  property values are ``{placeholder}`` templates resolved against the
  envelope (``{data.<path>}``, ``{partitionkey}``, ``{time}``, ...).
- edges carry a constant weight, a decay half-life, and the target node key.
- ``capabilities``: which relation sets a context capability exposes, with
  ``top_k`` and ``ttl_ms`` (the staleness contract channels honor).

The compiled artifact is a pure function: same (events, spec version) ->
byte-identical projection plans. There is no LLM and no wall clock inside
compilation — reflection time comes from the injected Clock.
"""

from __future__ import annotations

import re
from collections.abc import Mapping  # noqa: TC003
from typing import Annotated, Literal, Self

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator
from pydantic_core import PydanticCustomError

from graph_memory.core.errors import PlatformError
from graph_memory.core.types import Json, signal_type
from graph_memory.signals.envelope import Envelope

VERSION = 1
_MAX_NODES_PER_SIGNAL = 32
_MAX_EDGES_PER_SIGNAL = 64

_KIND = re.compile(r"^[A-Z][A-Z0-9_]{1,63}$")
_LABEL = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,31}$")
_PLACEHOLDER = re.compile(r"\{([A-Za-z0-9_.]+)\}")
_RESERVED_PLACEHOLDERS = frozenset({"partitionkey", "id", "source", "type", "time"})
_SYSTEM_KINDS = frozenset({"SIMILAR_TO"})  # written by reflection, not by signals


class MappingError(PlatformError):
    """A mapping spec is invalid, or a template failed to resolve."""

    code = "mapping_error"


def resolve_template(raw: str, context: Mapping[str, Json]) -> str:
    """``{data.skill}`` / ``{partitionkey}`` -> concrete scalar rendered as str.

    Unknown routes or non-scalar results are hard errors — determinism
    forbids silent fallback substitution.
    """

    def _sub(match: re.Match[str]) -> str:
        route = match.group(1)
        head = route.split(".")[0]
        if head in _RESERVED_PLACEHOLDERS:
            node: Json = context.get(head, "")
            if head == "partitionkey":
                node = context["partitionkey"]
            if head == "time":
                node = context["time"]
            return _scalar_str(node)
        node = dict(context)  # Mapping -> concrete dict[str, Json] (a Json member)
        for part in route.split("."):
            if not isinstance(node, dict) or part not in node:
                raise MappingError("template route not resolvable", details={"route": route})
            node = node[part]
        return _scalar_str(node)

    return _PLACEHOLDER.sub(_sub, raw)


def _scalar_str(value: Json) -> str:
    if isinstance(value, (dict, list)):
        raise MappingError(
            "template route must resolve to a scalar", details={"value": "structured"}
        )
    return str(value)


def envelope_context(envelope: Envelope) -> dict[str, Json]:
    """The canonical compile-time context every template resolves against."""
    return {
        "id": envelope.id,
        "source": envelope.source,
        "type": envelope.type,
        "time": envelope.occurred_at.isoformat(),
        "partitionkey": envelope.partitionkey,
        "data": dict(envelope.data),
    }


def _validate_template(raw: str) -> None:
    if "\x00" in raw or len(raw) > 512:
        raise PydanticCustomError("mapping_error", "template too long or unsafe")
    for match in _PLACEHOLDER.finditer(raw):
        route = match.group(1)
        head = route.split(".")[0]
        if head not in _RESERVED_PLACEHOLDERS and head != "data":
            raise PydanticCustomError("mapping_error", f"unknown placeholder route {{{route}}}")
        if head == "data" and route.count(".") < 1:
            raise PydanticCustomError(
                "mapping_error", f"data template needs a sub-route: {{{route}}}"
            )


class NodeRule(BaseModel):
    """One node upsert per matching signal event."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    key: str
    labels: tuple[str, ...] = ()
    props: dict[str, str] = Field(default_factory=dict)

    @field_validator("labels")
    @classmethod
    def _labels_bounded(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) > 4:
            raise PydanticCustomError("mapping_error", "at most 4 labels per node")
        for label in value:
            if not _LABEL.match(label):
                raise PydanticCustomError("mapping_error", f"unsafe node label {label!r}")
        return value

    @field_validator("key")
    @classmethod
    def _key_template(cls, value: str) -> str:
        _validate_template(value)
        return value

    @field_validator("props")
    @classmethod
    def _props_templates(cls, value: dict[str, str]) -> dict[str, str]:
        for template in value.values():
            _validate_template(template)
        return value


class EdgeRule(BaseModel):
    """One edge upsert per matching signal event (weight-decayed on reflection)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: str
    target: str
    weight: Annotated[float, Field(gt=0, le=1_000)]
    half_life_days: Annotated[float, Field(gt=0, le=3650)]

    @field_validator("kind")
    @classmethod
    def _kind_shape(cls, value: str) -> str:
        if not _KIND.match(value):
            raise PydanticCustomError(
                "mapping_error", f"unsafe edge kind {value!r} (UPPER_SNAKE, 2..64)"
            )
        return value

    @field_validator("target")
    @classmethod
    def _target_template(cls, value: str) -> str:
        _validate_template(value)
        return value


class SignalRule(BaseModel):
    """Everything one signal type produces per event."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    nodes: tuple[NodeRule, ...] = ()
    edges: tuple[EdgeRule, ...] = ()

    @field_validator("nodes")
    @classmethod
    def _nodes_bounded(cls, value: tuple[NodeRule, ...]) -> tuple[NodeRule, ...]:
        if len(value) > _MAX_NODES_PER_SIGNAL:
            raise PydanticCustomError("mapping_error", "too many node rules")
        return value

    @field_validator("edges")
    @classmethod
    def _edges_bounded(cls, value: tuple[EdgeRule, ...]) -> tuple[EdgeRule, ...]:
        if len(value) > _MAX_EDGES_PER_SIGNAL:
            raise PydanticCustomError("mapping_error", "too many edge rules")
        return value


class CapabilityRule(BaseModel):
    """A context capability: relations + exposure budget + staleness."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    relations: tuple[str, ...]
    top_k: Annotated[int, Field(ge=1, le=100)] = 10
    ttl_ms: Annotated[int, Field(ge=1_000, le=86_400_000)] = 60_000

    @field_validator("relations")
    @classmethod
    def _relations_kinds(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value:
            raise PydanticCustomError("mapping_error", "capability needs at least one relation")
        for kind in value:
            if not _KIND.match(kind):
                raise PydanticCustomError("mapping_error", f"unsafe relation kind {kind!r}")
        return value


class MappingSpec(BaseModel):
    """Version-pinned, immutable mapping spec root."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    mapping_version: Literal[1]
    signals: dict[str, SignalRule]
    capabilities: dict[str, CapabilityRule] = Field(default_factory=dict)

    @field_validator("signals")
    @classmethod
    def _signals_are_platform_types(cls, value: dict[str, SignalRule]) -> dict[str, SignalRule]:
        if not value:
            raise PydanticCustomError("mapping_error", "a mapping spec needs at least one signal")
        for name in value:
            signal_type(name)  # namespaced + versioned, or reject
        return value

    @model_validator(mode="after")
    def _capabilities_reference_declared_kinds(self) -> Self:
        declared = {edge.kind for rule in self.signals.values() for edge in rule.edges}
        declared |= _SYSTEM_KINDS  # reflection kinds are always consumable
        for name, capability in self.capabilities.items():
            unknown = [kind for kind in capability.relations if kind not in declared]
            if unknown:
                raise PydanticCustomError(
                    "mapping_error",
                    f"capability {name!r} references undeclared kinds {unknown!r}",
                )
        return self


def load_spec(text: str) -> MappingSpec:
    """Parse + validate YAML text; anything wrong raises ``MappingError``."""
    try:
        candidate = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise MappingError(f"mapping spec is not valid YAML: {exc}") from exc
    try:
        return MappingSpec.model_validate(candidate)
    except ValidationError as exc:
        issues: list[Json] = [
            ".".join(str(part) for part in error["loc"]) + ": " + str(error["msg"])
            for error in list(exc.errors())[:10]
        ]
        raise MappingError("mapping spec failed validation", details={"issues": issues}) from exc
