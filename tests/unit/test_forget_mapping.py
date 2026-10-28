"""Mapping-spec + compiler tests for the governed forget (M4.1).

The forget grammar is the additive spec slice: dedicated forget-only signal
families, kind/target scoping, and pure ForgetOp compilation. Nothing here
touches the wall clock — determinism is the acceptance bar (SPEC §3).
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from graph_memory.core.types import TenantId
from graph_memory.mapping.compiler import ForgetOp, compile_plan
from graph_memory.mapping.spec import MappingError, load_spec
from graph_memory.signals.envelope import Envelope

_VALID_SPEC = """
mapping_version: 1
signals:
  itf.learn.v1:
    edges:
      - kind: HAS_SKILL
        target: "skill:{data.skill}"
        weight: 1.0
        half_life_days: 180
  itf.forget_all.v1:
    forgets:
      - kind: HAS_SKILL
  itf.forget_one.v1:
    forgets:
      - kind: HAS_SKILL
        target: "{data.target}"
"""

_T = TenantId("acme")
_INSTANT = datetime(2026, 6, 5, tzinfo=UTC)


@pytest.mark.parametrize(
    ("signal", "expected"),
    [
        ("itf.forget_all.v1", [("HAS_SKILL", None)]),
        ("itf.forget_one.v1", [("HAS_SKILL", "{data.target}")]),
    ],
)
def test_forget_rules_parse_from_yaml(signal: str, expected: list[tuple[str, str | None]]) -> None:
    spec = load_spec(_VALID_SPEC)
    rule = spec.signals[signal]
    assert [(directive.kind, directive.target) for directive in rule.forgets] == expected


def test_forget_and_writes_cannot_mix_in_one_signal() -> None:
    text = _VALID_SPEC + (
        "  itf.halting.v1:\n"
        "    forgets:\n      - kind: HAS_SKILL\n"
        "    edges:\n"
        '      - kind: BROKEN\n        target: "x"\n'
        "        weight: 1.0\n        half_life_days: 10\n"
    )
    with pytest.raises(MappingError) as raised:
        load_spec(text)
    assert "either declares nodes/edges or forgets" in _issues_text(raised.value)


def test_duplicate_forget_kind_in_one_signal_is_rejected() -> None:
    text = (
        _VALID_SPEC
        + "  itf.duplicate.v1:\n    forgets:\n      - kind: COHORT\n      - kind: COHORT\n"
    )
    with pytest.raises(MappingError) as raised:
        load_spec(text)
    assert "duplicate forget kind" in _issues_text(raised.value)


def test_forgetting_a_reflection_owned_kind_is_rejected() -> None:
    text = _VALID_SPEC + "  itf.bad.v1:\n    forgets:\n      - kind: SIMILAR_TO\n"
    with pytest.raises(MappingError) as raised:
        load_spec(text)
    assert "reflection-owned" in _issues_text(raised.value)


def test_forget_target_template_must_be_safe() -> None:
    text = (
        _VALID_SPEC
        + '  itf.bad.v1:\n    forgets:\n      - kind: HAS_SKILL\n        target: "{nope.route}"\n'
    )
    with pytest.raises(MappingError):
        load_spec(text)


def test_compile_plan_emits_forget_ops_kind_wide() -> None:
    spec = load_spec(_VALID_SPEC)
    envelope = _envelope("itf.forget_all.v1", {}, _INSTANT)
    plan = compile_plan(spec, _T, envelope)
    assert plan.nodes and plan.edges == ()
    [directive] = plan.forgets
    assert isinstance(directive, ForgetOp)
    assert (directive.tenant, directive.user, directive.kind) == ("acme", "user-1", "HAS_SKILL")
    assert directive.target is None
    assert directive.forget_at == _INSTANT


def test_compile_plan_resolves_scoped_forget_targets() -> None:
    spec = load_spec(_VALID_SPEC)
    envelope = _envelope("itf.forget_one.v1", {"target": "skill:rust"}, _INSTANT)
    [directive] = compile_plan(spec, _T, envelope).forgets
    assert directive.target == "skill:rust"


def test_compile_plan_is_pure_for_forget_flows() -> None:
    """Same (envelope, spec) -> byte-equal plan; replay convergence premise."""
    spec = load_spec(_VALID_SPEC)
    envelope = _envelope("itf.forget_one.v1", {"target": "skill:rust"}, _INSTANT)
    assert compile_plan(spec, _T, envelope) == compile_plan(spec, _T, envelope)


def test_unmapped_signal_compiles_to_an_empty_plan() -> None:
    spec = load_spec(_VALID_SPEC)
    plan = compile_plan(spec, _T, _envelope("itf.other.v1", {}, _INSTANT))
    assert (plan.nodes, plan.edges, plan.forgets) == ((), (), ())


def _issues_text(exc: MappingError) -> str:
    """validator messages render in the wrapped payload's issue list."""
    issues = (exc.details or {}).get("issues")
    text = "\n".join(str(item) for item in issues) if isinstance(issues, list) else ""
    assert text, "expected the issue list to carry the validator's message"
    return text


def _envelope(signal: str, data: dict[str, object], at: datetime) -> Envelope:
    return Envelope.parse(
        {
            "specversion": "1.0",
            "id": f"forget-test-{at.isoformat()}",
            "source": "https://app.example.com/signals",
            "type": signal,
            "time": at.isoformat(),
            "partitionkey": "user-1",
            "data": data,
        }
    )
