"""Deterministic GraphRAG communities: 2-hop graph traversal, no LLM.

Reflection already computes pairwise SIMILAR_TO edges. This suite pins the
next hop: connected components become community nodes, each member gets a
MEMBER_OF edge, and every fact shared by at least two members becomes a
COMMUNITY_HAS_SKILL edge weighted by support. A `community` capability then
serves the summary through the normal context path (MCP/REST/SDK), so no
channel learns a new concept.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

from graph_memory.core.clock import FixedClock
from graph_memory.core.types import Capability, TenantId, UserKey
from graph_memory.mapping.spec import load_spec
from graph_memory.projector.engine import Projector
from graph_memory.projector.gateway import InMemoryGraphGateway
from graph_memory.signals.envelope import Envelope
from graph_memory.signals.ledger import InMemoryLedger, record_from_envelope

_T = TenantId("acme")
_NOW = datetime(2026, 6, 10, tzinfo=UTC)
_SPEC = """\
mapping_version: 1
signals:
  itf.skill_assessed.v1:
    nodes:
      - key: "skill:{data.skill}"
        labels: [skill]
        props:
          name: "{data.skill}"
    edges:
      - kind: HAS_SKILL
        target: "skill:{data.skill}"
        weight: 1.0
        half_life_days: 180
capabilities:
  skills:
    relations: [HAS_SKILL]
    top_k: 5
    ttl_ms: 60000
  peers:
    relations: [SIMILAR_TO]
    top_k: 5
    ttl_ms: 60000
  community:
    relations: [MEMBER_OF, COMMUNITY_HAS_SKILL]
    top_k: 20
    ttl_ms: 60000
"""


def _engine() -> tuple[Projector, InMemoryLedger]:
    ledger = InMemoryLedger()
    engine = Projector(
        load_spec(_SPEC),
        ledger,
        InMemoryGraphGateway(),
        FixedClock(_NOW),
    )
    return engine, ledger


def _seed(engine: Projector, ledger: InMemoryLedger, *pairs: tuple[str, str]) -> None:
    for index, (user, skill) in enumerate(pairs):
        payload: dict[str, object] = {
            "specversion": "1.0",
            "id": f"c{index}",
            "source": "https://app.example.com/signals",
            "type": "itf.skill_assessed.v1",
            "time": "2026-06-01T12:00:00.000Z",
            "partitionkey": user,
            "data": {"skill": skill},
        }
        ledger.append([record_from_envelope(_T, Envelope.parse(payload))])
    engine.process_events(ledger.read_events(_T))


def _items(payload: dict[str, object]) -> list[dict[str, object]]:
    items = payload["items"]
    assert isinstance(items, list)
    return [item for item in items if isinstance(item, dict)]


def _community_edges(engine: Projector, user: str) -> set[tuple[str, str, float]]:
    return {
        (row.kind, row.target, row.weight)
        for row in engine.gateway.observations(_T, UserKey(user))
        if row.kind in {"MEMBER_OF", "COMMUNITY_HAS_SKILL"}
    }


def test_community_projection_serves_membership_and_shared_facts() -> None:
    engine, ledger = _engine()
    _seed(
        engine,
        ledger,
        ("user-1", "rust"),
        ("user-1", "python"),
        ("user-2", "rust"),
        ("user-2", "python"),
        ("user-3", "go"),
    )
    digest = engine.reflect()
    assert (digest.communities_written, digest.community_edges_written) == (1, 6)

    payload = engine.snapshot(_T, UserKey("user-1"), Capability("community")).payload
    items = _items(payload)
    membership = [item for item in items if item["kind"] == "MEMBER_OF"]
    assert len(membership) == 1
    community_id = str(membership[0]["target"])
    assert community_id.startswith("community:")
    shared = {
        str(item["target"]): item["score"]
        for item in items
        if item["kind"] == "COMMUNITY_HAS_SKILL"
    }
    assert shared == {"skill:python": 2.0, "skill:rust": 2.0}
    labels = payload["labels"]
    assert isinstance(labels, dict)
    assert labels[community_id] == ["community"]


def test_lone_user_has_no_community() -> None:
    engine, ledger = _engine()
    _seed(engine, ledger, ("user-1", "rust"), ("user-2", "rust"))
    # one shared fact is below the default similarity threshold (2)
    digest = engine.reflect()
    assert digest.communities_written == 0
    payload = engine.snapshot(_T, UserKey("user-1"), Capability("community")).payload
    assert _items(payload) == []


def test_community_is_idempotent_and_ignores_its_own_edges() -> None:
    engine, ledger = _engine()
    _seed(
        engine,
        ledger,
        ("user-1", "rust"),
        ("user-1", "python"),
        ("user-2", "rust"),
        ("user-2", "python"),
    )
    first = engine.reflect()
    state_one = _community_edges(engine, "user-1")
    similar_one = {
        (row.kind, row.target, row.weight)
        for row in engine.gateway.observations(_T, UserKey("user-1"))
        if row.kind == "SIMILAR_TO"
    }

    second = engine.reflect()
    assert _community_edges(engine, "user-1") == state_one
    assert (
        first.communities_written,
        first.community_edges_written,
    ) == (
        second.communities_written,
        second.community_edges_written,
    )
    similar_two = {
        (row.kind, row.target, row.weight)
        for row in engine.gateway.observations(_T, UserKey("user-1"))
        if row.kind == "SIMILAR_TO"
    }
    assert similar_two == similar_one == {("SIMILAR_TO", "user-2", 2.0)}


def test_community_is_stable_after_replay() -> None:
    engine, ledger = _engine()
    _seed(
        engine,
        ledger,
        ("user-1", "rust"),
        ("user-1", "python"),
        ("user-2", "rust"),
        ("user-2", "python"),
    )
    engine.reflect()
    before = json.dumps(
        sorted(_community_edges(engine, "user-1")),
        default=str,
    )
    engine.replay()
    engine.reflect()
    after = json.dumps(
        sorted(_community_edges(engine, "user-1")),
        default=str,
    )
    assert after == before
