"""Mapping spec validation + compiler determinism tests."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from graph_memory.core.types import TenantId
from graph_memory.mapping.compiler import compile_plan
from graph_memory.mapping.spec import MappingError, MappingSpec, load_spec, resolve_template
from graph_memory.signals import Envelope

_ACME = TenantId("acme")
_SPEC_PATH = Path(__file__).parent.parent / "fixtures" / "mapping_skill_loop.yaml"


def _spec() -> MappingSpec:
    return load_spec(_SPEC_PATH.read_text(encoding="utf-8"))


def _envelope(**overrides: object) -> Envelope:
    payload: dict[str, object] = {
        "specversion": "1.0",
        "id": "e1",
        "source": "https://app.example.com/signals",
        "type": "itf.skill_assessed.v1",
        "time": "2026-06-01T12:00:00.123Z",
        "partitionkey": "user-1",
        "data": {"skill": "rust", "score": 0.8},
    }
    payload.update(overrides)
    for key, value in list(payload.items()):
        if value is None:
            del payload[key]
    return Envelope.parse(payload)


def test_reference_spec_loads() -> None:
    mapping = _spec()
    assert sorted(mapping.signals) == ["itf.outcome.v1", "itf.reco.v1", "itf.skill_assessed.v1"]
    assert set(mapping.capabilities) == {"skills", "reco_effect", "peers"}
    assert mapping.mapping_version == 1


def test_compile_skill_assessed_is_deterministic() -> None:
    mapping = _spec()
    envelope = _envelope()
    plan_one = compile_plan(mapping, _ACME, envelope)
    plan_two = compile_plan(mapping, _ACME, envelope)
    assert plan_one == plan_two
    assert len(plan_one.nodes) == 2  # builtin user node + the spec's node rule
    node_keys = {node.key for node in plan_one.nodes}
    assert node_keys == {"user-1", "skill:rust"}
    (skill,) = [node for node in plan_one.nodes if node.key == "skill:rust"]
    assert skill.labels == ("skill",)
    assert skill.props == {"name": "rust"}
    edge = plan_one.edges[0]
    assert (edge.user, edge.kind, edge.target) == ("user-1", "HAS_SKILL", "skill:rust")
    assert edge.weight == 1.0
    assert edge.valid_at == datetime(2026, 6, 1, 12, 0, 0, 123_000, tzinfo=UTC)


def test_unmapped_signal_type_compiles_to_empty_plan() -> None:
    mapping = _spec()
    envelope = _envelope(type="itf.whatever.v9")
    plan = compile_plan(mapping, _ACME, envelope)
    assert plan.nodes == () and plan.edges == ()


def test_template_resolution_unknown_route_is_hard_error() -> None:
    with pytest.raises(MappingError, match="resolvable"):
        resolve_template("{data.missing_key}", {"data": {"skill": "rust"}})


def test_template_resolution_structured_result_is_hard_error() -> None:
    with pytest.raises(MappingError, match="scalar"):
        resolve_template("{data.details}", {"data": {"details": {"deep": 1}}})


def _edge_spec(kind: str, weight: int = 1) -> str:
    """A minimal valid-looking spec text with one edge; used for bad cases."""
    return (
        "mapping_version: 1\n"
        "signals:\n"
        "  itf.skill_assessed.v1:\n"
        "    edges:\n"
        f"      - kind: {kind}\n"
        "        target: 'skill:x'\n"
        f"        weight: {weight}\n"
        "        half_life_days: 30\n"
    )


@pytest.mark.parametrize(
    "text",
    [
        "mapping_version: 2\nsignals:\n"  # future version refused until additive migration lands
        "mapping_version: 1\nsignals: {}",
        _edge_spec("lower_case"),  # kinds must be UPPER_SNAKE
        _edge_spec("HAS_SKILL", weight=-1),
        _edge_spec("HAS_SKILL")
        + "capabilities:\n  skills:\n    relations: [NOT_DECLARED]\n    top_k: 5\n",
        "signals:\n  itf.skill_assessed:\n    edges: []",  # unversioned signal type
        "not: yaml: list: [unclosed",
    ],
)
def test_bad_specs_raise_mapping_error(text: str) -> None:
    with pytest.raises(MappingError):
        load_spec(text)
