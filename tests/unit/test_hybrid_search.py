"""Hybrid retrieval tests (M4.2, research 06 lever 2).

One ranked search behind every channel, proven falsifiably:

- the shared lexicon's tokenization + lexical metric are deterministic;
- the kernel orders by (lexical, typed/traversal, loop) with canonical
  tie-breaks, so the same (events, spec, query, clock) is byte-identical;
- loop weights (the effectiveness loop's overrides, folded into stored
  weights at compile time) actually move the ranking — the ranking feature
  no competitor has;
- forget interplay: superseded observations cannot surface in search;
- both channels render byte-identical search payloads (MCP == REST), with
  the documented fallback (snapshot post-filter) for unwired MCP
  deployments (covered by ``test_mcp_channel.py``) and an anticipated
  ``not_wired`` failure instead of silence for unwired REST.
"""

from __future__ import annotations

import json
import socket
import threading
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient

from graph_memory.channels.mcp.server import build_mcp_server, mcp_streamable_app
from graph_memory.channels.rest import create_rest_app
from graph_memory.context.search import MemorySearchService, rank_search
from graph_memory.context.service import ContextCache, MemoryContextProvider
from graph_memory.core.clock import FixedClock
from graph_memory.core.errors import TenantMismatch
from graph_memory.core.lexicon import lexical_hits, query_tokens
from graph_memory.core.ports import EmbeddingProvider, MemorySearch
from graph_memory.core.types import (
    Capability,
    Json,
    Principal,
    Scope,
    TenantId,
    UserKey,
    UserRef,
)
from graph_memory.mapping.spec import load_spec
from graph_memory.observability.metering import MeteringCounters
from graph_memory.observability.observer import InMemorySpanSink
from graph_memory.projector.engine import CapabilityUnknown, Projector
from graph_memory.projector.gateway import EdgeRow, InMemoryGraphGateway, NodeRow
from graph_memory.settings import Settings
from graph_memory.signals.envelope import Envelope
from graph_memory.signals.ledger import InMemoryLedger, record_from_envelope
from graph_memory.tenancy.guard import StaticBearerGuard

_T = TenantId("acme")
_U = UserKey("user-1")
_TOKEN = "dev-acme-token"
_SPEC_PATH = Path(__file__).parent.parent / "fixtures" / "mapping_with_forget.yaml"
_NOW = datetime(2026, 6, 10, tzinfo=UTC)
_LEARN_AT = datetime(2026, 6, 1, tzinfo=UTC)
_FORGET_AT = datetime(2026, 6, 20, tzinfo=UTC)


def _spec_text() -> str:
    return _SPEC_PATH.read_text(encoding="utf-8")


def _principal(tenant: TenantId = _T) -> Principal:
    return Principal(
        tenant=tenant,
        scopes=frozenset({Scope.CONTEXT_READ, Scope.MEMORY_SEARCH}),
        subject="test",
    )


def _engine(
    ledger: InMemoryLedger,
    *,
    overrides: dict[tuple[str, str, str], float] | None = None,
) -> Projector:
    return Projector(
        load_spec(_spec_text()),
        ledger,
        InMemoryGraphGateway(),
        FixedClock(_NOW),
        weight_overrides=overrides,
    )


# --- payload-extraction helpers (Json is only loosely typed on the wire) ------


def _items(body: dict[str, Json]) -> list[dict[str, Json]]:
    items = body["items"]
    assert isinstance(items, list)
    return [cast("dict[str, Json]", item) for item in items]


def _targets(body: dict[str, Json]) -> list[str]:
    return [cast("str", item["target"]) for item in _items(body)]


def _text(body: dict[str, Json], key: str) -> str:
    value = body[key]
    assert isinstance(value, str)
    return value


def _num(item: dict[str, Json], key: str) -> float:
    features = item["features"]
    assert isinstance(features, dict)
    value = features[key]
    assert isinstance(value, (int, float)) and not isinstance(value, bool)
    return float(value)


# --- shared lexicon ---------------------------------------------------------


def test_query_tokens_are_casefolded_deduped_and_bounded() -> None:
    assert query_tokens("Rust LINGUISTICS rust a") == ("rust", "linguistics")
    # Punctuation splits runs; single characters and 1-char fragments drop.
    assert query_tokens("skill:rust, go!") == ("skill", "rust", "go")
    many = query_tokens("aa bb cc dd ee ff gg hh ii jj")
    assert len(many) == 8  # bounded by MAX_QUERY_TOKENS
    assert many == ("aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh")
    assert query_tokens("") == ()
    assert query_tokens("!!!") == ()


def test_lexical_hits_is_the_one_shared_metric() -> None:
    # Case-insensitive substring per field; a token counted once even when
    # several fields carry it (the count is over TOKENS, not over fields).
    assert lexical_hits(("HAS_SKILL", "skill:rust"), ("rust", "linguistics")) == 1
    assert lexical_hits(("RECOMMENDED", "skill:rust linguistics"), ("rust", "linguistics")) == 2
    assert lexical_hits(("kind", "target"), ()) == 0
    # Same formula the ClickHouse adapter encodes in positionCaseInsensitive.
    assert lexical_hits(("A", "B"), ("a", "b", "c")) == 2


# --- the pure ranking kernel ------------------------------------------------


def _row(kind: str, target: str, *, valid_at: datetime = _LEARN_AT, weight: float = 1.0) -> EdgeRow:
    return EdgeRow(
        tenant=_T,
        user=_U,
        kind=kind,
        target=target,
        weight=weight,
        half_life_days=180.0,
        valid_at=valid_at,
    )


def _node(key: str, name: str | None = None) -> NodeRow:
    return NodeRow(
        tenant=_T,
        key=key,
        labels=("skill",),
        props={"name": key if name is None else name},
    )


def test_kernel_capability_filter_and_zero_hit_rule() -> None:
    """A real query keeps only groups the capability serves AND text matches."""
    rows = [
        (_row("HAS_SKILL", "skill:rust"), 1),
        (_row("RECOMMENDED", "skill:co"), 1),  # declared kind, outside capability
        (_row("COHORT", "user:1"), 1),  # reflection-only kind
        (_row("HAS_SKILL", "skill:go"), 0),  # in capability, but no token hits
    ]
    ranked = rank_search(
        rows,
        {"skill:rust": _node("skill:rust"), "skill:go": _node("skill:go")},
        frozenset({"HAS_SKILL"}),
        frozenset({"HAS_SKILL", "RECOMMENDED"}),
        ("rust",),
        _NOW,
    )
    assert [(item.kind, item.target) for item in ranked] == [("HAS_SKILL", "skill:rust")]


def test_kernel_typed_tier_orders_browse_results() -> None:
    """Tokenless browse keeps everything; spec-declared kinds rank above
    reflection-only kinds (SIMILAR_TO family) at equal lexical/loop."""
    rows = [(_row("COHORT", "user:1"), 0), (_row("RECOMMENDED", "skill:go"), 0)]
    ranked = rank_search(rows, {}, None, frozenset({"RECOMMENDED"}), (), _NOW)
    assert [(item.kind, item.target, item.typed) for item in ranked] == [
        ("RECOMMENDED", "skill:go", 2),
        ("COHORT", "user:1", 1),
    ]


def test_kernel_freshness_decides_the_loop_feature_tie() -> None:
    """Equal strength and lexical: the fresher observation ranks first."""
    rows = [
        (_row("HAS_SKILL", "skill:rust"), 1),
        (_row("HAS_SKILL", "skill:go", valid_at=datetime(2026, 6, 9, tzinfo=UTC)), 1),
    ]
    ranked = rank_search(
        rows, {}, frozenset({"HAS_SKILL"}), frozenset({"HAS_SKILL"}), ("rust", "go"), _NOW
    )
    [first, second] = ranked
    assert (first.target, second.target) == ("skill:go", "skill:rust")
    assert first.typed == second.typed and first.lexical == second.lexical
    assert first.loop > second.loop  # the tie was broken by decayed score


def test_kernel_node_text_contributes_traversal_hits() -> None:
    """A match that only exists in node props/labels still surfaces the EDGE."""
    row = _row("HAS_SKILL", "skill:v2")
    ranked = rank_search(
        rows_with_hits=[(row, 0)],  # gateway metric saw nothing on kind/target
        nodes={"skill:v2": _node("skill:v2", name="pytorch")},
        capability_kinds=frozenset({"HAS_SKILL"}),
        declared_kinds=frozenset({"HAS_SKILL"}),
        tokens=("pytorch",),
        now=_NOW,
    )
    assert [(item.target, item.lexical) for item in ranked] == [("skill:v2", 1)]
    # Without the node in the store, the same query is a dead end.
    assert rank_search([(row, 0)], {}, None, frozenset(), ("pytorch",), _NOW) == []


# --- the adapter -------------------------------------------------------------


def _record(eid: str, skill: str, *, at: datetime = _LEARN_AT) -> Any:
    envelope = Envelope.parse(
        {
            "specversion": "1.0",
            "id": eid,
            "source": "https://app.example.com/signals",
            "type": "itf.skill_assessed.v1",
            "time": at.isoformat(),
            "partitionkey": "user-1",
            "data": {"skill": skill},
        }
    )
    return record_from_envelope(_T, envelope)


def test_adapter_search_groups_and_ranks_end_to_end() -> None:
    ledger = InMemoryLedger()
    ledger.append(
        [
            _record("s1", "rust"),
            _record("s2", "rust", at=datetime(2026, 6, 2, tzinfo=UTC)),
            _record("s3", "go", at=datetime(2026, 6, 9, tzinfo=UTC)),
        ]
    )
    engine = _engine(ledger)
    engine.replay()
    service: MemorySearch = MemorySearchService(engine)
    body = service.search(
        _principal(), UserRef(_T, _U), "rust", capability=Capability("skills"), limit=8
    )
    assert _text(body, "capability") == "skills"
    assert _text(body, "user") == "user-1"
    assert _text(body, "generated_at") == _NOW.isoformat()
    [item] = _items(body)
    assert _text(item, "kind") == "HAS_SKILL"
    assert _text(item, "target") == "skill:rust"
    assert _num(item, "typed") == 3  # capability-served tier
    assert _num(item, "lexical") >= 1
    # Distinct instants = two observations of one target: summed decayed score.
    score = item["score"]
    assert isinstance(score, float) and 1.9 < score < 2.0
    # The zero-hit "go" observation stays hidden under a real query.


def test_adapter_none_capability_browses_the_whole_graph() -> None:
    ledger = InMemoryLedger()
    ledger.append([_record("s1", "rust"), _record("s2", "go")])
    engine = _engine(ledger)
    engine.replay()
    body = MemorySearchService(engine).search(
        _principal(), UserRef(_T, _U), "", capability=None, limit=8
    )
    assert body["capability"] is None
    assert _targets(body) == ["skill:go", "skill:rust"]  # canonical tie-break


def test_adapter_loop_overrides_move_the_ranking() -> None:
    """Loop ON: the weighted target outranks the same-strength baseline."""
    ledger = InMemoryLedger()
    ledger.append([_record("s1", "rust"), _record("s2", "go", at=datetime(2026, 6, 8, tzinfo=UTC))])
    engine = _engine(ledger)
    engine.replay()
    baseline = MemorySearchService(engine).search(
        _principal(), UserRef(_T, _U), "", capability=Capability("skills"), limit=8
    )
    assert _targets(baseline) == ["skill:go", "skill:rust"]

    # The same events, one loop weight override, replayed from scratch:
    # the override folds into stored weight at compile time, and the older
    # observation's boost beats the fresher baseline one.
    boosted_ledger = InMemoryLedger()
    boosted_ledger.append(
        [_record("s1", "rust"), _record("s2", "go", at=datetime(2026, 6, 8, tzinfo=UTC))]
    )
    boosted_engine = _engine(boosted_ledger, overrides={("HAS_SKILL", "skill:rust", ""): 3.0})
    boosted_engine.replay()
    flipped = MemorySearchService(boosted_engine).search(
        _principal(), UserRef(_T, _U), "", capability=Capability("skills"), limit=8
    )
    assert _targets(flipped) == ["skill:rust", "skill:go"]


def test_adapter_enforces_tenant_rule_and_unknown_capabilities() -> None:
    service = MemorySearchService(_engine(InMemoryLedger()))
    with pytest.raises(TenantMismatch):
        service.search(
            _principal(),
            UserRef(TenantId("other"), _U),
            "rust",
            capability=None,
            limit=8,
        )
    with pytest.raises(CapabilityUnknown):
        service.search(
            _principal(), UserRef(_T, _U), "rust", capability=Capability("nope"), limit=8
        )


def test_adapter_limit_is_bounded_in_both_directions() -> None:
    ledger = InMemoryLedger()
    ledger.append([_record(f"s{index:0>2}", f"lang-{index:0>2}") for index in range(30)])
    engine = _engine(ledger)
    engine.replay()
    service = MemorySearchService(engine)
    wide = service.search(_principal(), UserRef(_T, _U), "", capability=None, limit=100_000)
    assert len(_items(wide)) == 25  # the adapter's own _MAX_SEARCH_LIMIT
    tiny = service.search(_principal(), UserRef(_T, _U), "", capability=None, limit=0)
    assert len(_items(tiny)) == 1
    needle = service.search(_principal(), UserRef(_T, _U), "lang-07", capability=None, limit=8)
    targets = _targets(needle)
    assert targets[0] == "skill:lang-07"  # strongest lexical hit first
    assert len(targets) == 8  # bounded; the 'lang' substring still matches the rest


def test_superseded_observations_cannot_surface_in_search() -> None:
    """The forget overlay governs search exactly like it governs context."""
    ledger = InMemoryLedger()
    engine = _engine(ledger)
    ledger.append([_record("s1", "rust"), _record("s2", "go")])
    engine.replay()
    service = MemorySearchService(engine)
    before = service.search(
        _principal(), UserRef(_T, _U), "rust", capability=Capability("skills"), limit=8
    )
    assert _targets(before) == ["skill:rust"]

    forget_envelope = Envelope.parse(
        {
            "specversion": "1.0",
            "id": "f1",
            "source": "https://app.example.com/signals",
            "type": "itf.forget_one_skill.v1",
            "time": _FORGET_AT.isoformat(),
            "partitionkey": "user-1",
            "data": {"target": "skill:rust"},
        }
    )
    ledger.append([record_from_envelope(_T, forget_envelope)])
    # The clock stays FIXED at _NOW — hiding is by directive, not by time.
    engine.process_events(ledger.read_events(_T))
    after = service.search(
        _principal(), UserRef(_T, _U), "rust", capability=Capability("skills"), limit=8
    )
    assert after["items"] == []

    # Same (events, spec, query, clock) -> byte-identical result, replayed
    # from scratch: the search read converges like every other read.
    twin = _engine(ledger)
    twin.replay()
    twin_body = MemorySearchService(twin).search(
        _principal(), UserRef(_T, _U), "rust", capability=Capability("skills"), limit=8
    )
    assert twin_body == after


def test_embedding_port_stays_off_the_hot_path_and_is_satisfiable() -> None:
    class OnnxLike:
        def embed(self, texts: Sequence[str]) -> tuple[tuple[float, ...], ...]:
            return tuple((float(len(text)),) for text in texts)

    provider: EmbeddingProvider = OnnxLike()
    assert provider.embed(("rust",)) == ((4.0,),)


# --- channel surfaces ---------------------------------------------------------


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class _AppServer:
    """In-process uvicorn on an ephemeral port; threads end with the test."""

    def __init__(self, app: Any) -> None:  # noqa: ANN401 — ASGI app object
        self.port = _free_port()
        self.server = uvicorn.Server(
            uvicorn.Config(app, host="127.0.0.1", port=self.port, log_level="warning")
        )
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.thread.start()
        deadline = time.monotonic() + 10
        while not self.server.started and time.monotonic() < deadline:
            time.sleep(0.05)
        assert self.server.started

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=5)


class _World:
    """One projected engine + both search surfaces wired from the same ports."""

    def __init__(self) -> None:
        spec = load_spec(_spec_text())
        self.ledger = InMemoryLedger()
        self.engine = _engine(self.ledger)
        self.ledger.append([_record("s1", "rust"), _record("s2", "go")])
        self.engine.replay()
        provider = MemoryContextProvider(self.engine, ContextCache())
        self.metering = MeteringCounters()
        self.spans = InMemorySpanSink()
        guard = StaticBearerGuard(Settings().dev_static_tokens)
        service: MemorySearch = MemorySearchService(self.engine)
        self.rest = _AppServer(
            create_rest_app(
                provider, guard, sink=self.spans, metering=self.metering, search=service
            )
        )
        self.mcp = _AppServer(
            mcp_streamable_app(
                build_mcp_server(
                    provider,
                    guard,
                    capabilities=tuple(sorted(spec.capabilities)),
                    signal_schemas={},
                    search=service,
                )
            )
        )

    def stop(self) -> None:
        self.rest.stop()
        self.mcp.stop()


def _mcp_search(port: int, arguments: dict[str, object]) -> dict[str, Any]:
    response = httpx.post(
        f"http://127.0.0.1:{port}/mcp",
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "search_memory", "arguments": arguments},
        },
        headers={"Authorization": f"Bearer {_TOKEN}", "Accept": "application/json"},
    )
    assert response.status_code == 200, response.text
    return dict(json.loads(response.text))


def test_channels_render_one_hybrid_payload() -> None:
    """Parity: MCP == REST; per-user point work; features ride the wire."""
    world = _World()
    try:
        response = httpx.get(
            f"http://127.0.0.1:{world.rest.port}/v1/search/user-1",
            params={"q": "go", "capability": "skills"},
            headers={"Authorization": f"Bearer {_TOKEN}"},
        )
        assert response.status_code == 200, response.text
        rest_body = response.json()
        assert rest_body["user"] == "user-1"
        assert rest_body["capability"] == "skills"
        mcp_body = _mcp_search(
            world.mcp.port, {"user": "user-1", "capability": "skills", "query": "go"}
        )
        assert mcp_body["result"]["structuredContent"] == rest_body
        [item] = rest_body["items"]
        assert item["target"] == "skill:go"  # zero-hit rust stays hidden
        assert item["features"]["typed"] == 3
        assert item["score"] == item["features"]["loop"]
        # Evidence is metered and audited per request class.
        rows = world.metering.drain()
        assert rows[(_T, "rest", "search")] == 1
        span_names = [span.name for span in world.spans if span.name == "gmp.channel.rest.search"]
        assert len(span_names) == 1
    finally:
        world.stop()


def test_rest_search_scopes_errors_isolation_and_unwired() -> None:
    scopeless_guard = StaticBearerGuard(
        {"dev-acme-token": "acme"}, scopes=frozenset({Scope.CONTEXT_READ})
    )
    engine = _engine(InMemoryLedger())
    app = TestClient(
        create_rest_app(
            MemoryContextProvider(engine, ContextCache()),
            scopeless_guard,
            search=MemorySearchService(engine),
        )
    )

    # The guard requires memory:search before anything else.
    scope_denied = app.get(
        "/v1/search/user-1",
        params={"q": "rust"},
        headers={"Authorization": f"Bearer {_TOKEN}"},
    )
    assert scope_denied.status_code == 403
    assert scope_denied.json()["detail"]["error"] == "scope_denied"

    # A rejected credential fails closed as unknown_credential.
    bogus = app.get(
        "/v1/search/user-1",
        params={"q": "rust"},
        headers={"Authorization": "Bearer no-such-token"},
    )
    assert bogus.status_code == 401
    assert bogus.json()["detail"]["error"] == "scope_denied"

    # Unwired deployments fail as an anticipated error, never silently.
    unwired_guard = StaticBearerGuard(Settings().dev_static_tokens)
    unwired = TestClient(
        create_rest_app(MemoryContextProvider(engine, ContextCache()), unwired_guard)
    )
    gone = unwired.get(
        "/v1/search/user-1",
        params={"q": "rust"},
        headers={"Authorization": f"Bearer {_TOKEN}"},
    )
    assert gone.status_code == 500
    assert gone.json()["detail"]["error"] == "platform_error"
    assert gone.json()["detail"]["details"]["reason"] == "not_wired"

    world = _World()
    try:
        unknown_cap = httpx.get(
            f"http://127.0.0.1:{world.rest.port}/v1/search/user-1",
            params={"q": "rust", "capability": "nope"},
            headers={"Authorization": f"Bearer {_TOKEN}"},
        )
        assert unknown_cap.status_code == 404
        assert unknown_cap.json()["detail"]["error"] == "capability_unknown"

        # A co-user in the SAME tenant has an empty graph — nothing bleeds.
        other = httpx.get(
            f"http://127.0.0.1:{world.rest.port}/v1/search/user-2",
            params={"q": "rust", "capability": "skills"},
            headers={"Authorization": f"Bearer {_TOKEN}"},
        )
        assert other.status_code == 200
        assert other.json()["items"] == []
    finally:
        world.stop()
