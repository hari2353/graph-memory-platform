"""Context-assembler budget: priority order, overflow prevention, explicit truncation.

The market claim is "summary <= 8k tokens"; the gate is ~2K. Before this
suite the only bound was ``top_k``, so 100 long targets produced 4.6K
estimated tokens while the bench passed on three short ones.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

from graph_memory.core.clock import FixedClock
from graph_memory.core.tokens import estimate_tokens
from graph_memory.core.types import Capability, TenantId, UserKey
from graph_memory.mapping.spec import load_spec
from graph_memory.projector.engine import Projector
from graph_memory.projector.gateway import InMemoryGraphGateway
from graph_memory.signals.envelope import Envelope
from graph_memory.signals.ledger import InMemoryLedger, record_from_envelope

_T = TenantId("acme")
_NOW = datetime(2026, 6, 10, tzinfo=UTC)
_LONG = "x" * 120


def _engine(*, max_tokens: int | None = 2000, skills: int = 100) -> Projector:
    budget = "" if max_tokens is None else f"\n    max_tokens: {max_tokens}"
    spec = load_spec(
        f"""\
mapping_version: 1
signals:
  itf.skill_assessed.v1:
    nodes:
      - key: "skill:{{data.skill}}"
        labels: [skill]
    edges:
      - kind: HAS_SKILL
        target: "skill:{{data.skill}}"
        weight: 1.0
        half_life_days: 180
capabilities:
  skills:
    relations: [HAS_SKILL]
    top_k: 100
    ttl_ms: 60000{budget}
"""
    )
    ledger = InMemoryLedger()
    engine = Projector(spec, ledger, InMemoryGraphGateway(), FixedClock(_NOW))
    for index in range(skills):
        payload: dict[str, object] = {
            "specversion": "1.0",
            "id": f"budget-{index}",
            "source": "https://budget.example.com/signals",
            "type": "itf.skill_assessed.v1",
            "time": _NOW.isoformat(),
            "partitionkey": "user-1",
            "data": {"skill": f"{index:03d}-{_LONG}"},
        }
        ledger.append([record_from_envelope(_T, Envelope.parse(payload))])
    engine.process_events(ledger.read_events(_T))
    return engine


def _snapshot(engine: Projector) -> dict[str, object]:
    return engine.snapshot(_T, UserKey("user-1"), Capability("skills")).payload


def test_budget_truncates_worst_case_and_reports_overflow() -> None:
    payload = _snapshot(_engine(max_tokens=2000))
    items = payload["items"]
    assert isinstance(items, list)
    assert 0 < len(items) < 100
    assert payload["max_tokens"] == 2000
    assert payload["truncated"] is True
    assert payload["omitted"] == 100 - len(items)
    served = estimate_tokens(json.dumps(payload, sort_keys=True))
    assert served <= 2000
    included = {str(item["target"]) for item in items if isinstance(item, dict)}
    labels = payload["labels"]
    assert isinstance(labels, dict)
    assert set(labels) == included  # dropped items never ship their labels


def test_default_budget_is_two_thousand_and_small_reads_are_untouched() -> None:
    payload = _snapshot(_engine(max_tokens=None, skills=3))
    assert payload["max_tokens"] == 2000
    assert payload["truncated"] is False
    assert payload["omitted"] == 0
    assert len(payload["items"]) == 3  # type: ignore[arg-type]


def test_truncation_keeps_the_priority_prefix() -> None:
    full = _snapshot(_engine(max_tokens=8000))
    capped = _snapshot(_engine(max_tokens=2000))
    full_items = full["items"]
    capped_items = capped["items"]
    assert isinstance(full_items, list) and isinstance(capped_items, list)
    full_targets = [item["target"] for item in full_items if isinstance(item, dict)]
    capped_targets = [item["target"] for item in capped_items if isinstance(item, dict)]
    assert capped_targets == full_targets[: len(capped_targets)]
    assert len(capped_targets) < len(full_targets)
    scores = [item["score"] for item in capped_items if isinstance(item, dict)]
    assert scores == sorted(scores, reverse=True)


def test_spec_ceiling_is_configurable() -> None:
    payload = _snapshot(_engine(max_tokens=8000, skills=3))
    assert payload["max_tokens"] == 8000
