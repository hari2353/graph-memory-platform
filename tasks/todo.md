# graph-memory-platform — task list

Spec: `docs/SPEC.md` (approved). Plan: `tasks/plan.md`. One task per focused
session; each lands green with its acceptance test. Pr/local only until push
(task 2 makes the repo public).

## M0 — kernel skeleton

- [x] Task: repo skeleton + research brief + capability map + bible spec
  - Acceptance: docs exist and trace decisions to research; commits clean
  - Verify: `git log --oneline` shows 3+ commits; SPEC §20 resolutions recorded
  - Files: README.md, docs/RESEARCH_BRIEF.md, docs/research/01..05, docs/CAPABILITY_MAP.md, docs/SPEC.md
- [x] Task: tooling skeleton (uv + ruff + mypy strict + pytest, CI workflow)
  - Acceptance: `uv run ruff check . && uv run mypy --strict . && uv run pytest` all green on empty package; GitHub Actions workflow runs the same on push
  - Verify: commands pass locally; workflow file committed (CI green on GitHub since commit 14e0ffc)
  - Files: pyproject.toml, .python-version, .gitignore, .github/workflows/ci.yml, src/graph_memory/__init__.py
- [x] Task: `core/` ports + domain foundation (Principal, TenantGuard contract, Clock, SignalSink, ContextProvider protocol, errors, log-safe scrub)
  - Acceptance: ports are Protocols with zero IO imports; loop-safe scrub unit-tested; mypy strict clean
  - Verify: `uv run pytest tests/unit/test_core.py` (43 tests green)
  - Files: src/graph_memory/core/{types,errors,clock,logsafe,ports}.py + __init__, tests/unit/test_core.py
- [x] Task: `signals/` CloudEvents envelope model + validation
  - Acceptance: pydantic model rejects missing required attrs, enforces `dataschema` URI + `partitionkey` guidance; version constant pinned to 1.0.2
  - Verify: `uv run pytest tests/unit/test_envelope.py` (34 envelope tests green)
  - Files: src/graph_memory/signals/{envelope.py,__init__.py}, tests/unit/test_envelope.py
- [x] Task: ledger schema + DDL/SQL engine port (`LedgerRepository`) with chdb-backed adapter
  - Acceptance: `signals_events` DDL matches SPEC §6 (ORDER BY, time-only partitions, dedup identity); insert + dedup-by-(tenant,user,type,ts,event_key) proven against embedded CH
  - Verify: `uv run pytest tests/integration/test_ledger.py` — one suite, two backends: InMemory (always) + ClickHouseLedger against live local CH (:8124), all green
  - Files: src/graph_memory/signals/{ledger.py,sql/ddl.py}, src/graph_memory/settings.py, tests/integration/test_ledger.py
  - Notes: chdb is unavailable on Windows, so the adapter is ClickHouse-server based locally and chdb remains the CI/dev-embedded extra (`embedded` optional dep). Projections omission: CH build rejects ReplacingMergeTree projections (deduplicate_merge_projection_mode=throw) — dedup owner is ReplacingMergeTree ORDER BY + FINAL reads.
- [x] Task: HTTP ingest `POST /v1/signals:batch` (FastAPI) + idempotency + DLQ table
  - Acceptance: batch ≤10k events; duplicate batch replay inserts nothing new; poison events land in DLQ with reason; per-tenant quota hook exists (no enforcement depth yet)
  - Verify: `uv run pytest tests/integration/test_ingest_http.py` — 8 edge tests (accepted/replay-dup, per-item rejection + DLQ, oversized-payload bounded DLQ, cross-tenant claim rejection, whole-batch 429, 401/403, healthz)
  - Files: src/graph_memory/signals/{service.py,api.py}, src/graph_memory/tenancy/{guard.py,__init__.py}, tests/integration/test_ingest_http.py
- [x] Task: replay dry-run CLI + first contract test for broker SPI (fixture envelopes)
  - Acceptance: `platform replay --dry-run` prints a read-only plan; SPI kit fixture suite runs for the HTTP adapter
  - Verify: `uv run platform replay --dry-run` (verified locally); `uv run pytest tests/unit/test_spi_kit.py` (6 tests)
  - Files: src/graph_memory/ops/{cli.py,__init__.py}, src/graph_memory/spikit/__init__.py, tests/unit/test_spi_kit.py
  - Notes: `tenants()` added to LedgerRepository for the sweep; console script `platform` wired in pyproject.
- [x] Task: publish public GitHub repo + push M0
  - Acceptance: origin = public `hari2353/graph-memory-platform`; CI green on GitHub
  - Verify: continuously pushed since; repo public at github.com/hari2353/graph-memory-platform

## M1 — memory engine

- [x] Task: `memory-mapping` spec schema v1 (YAML → pydantic) + validator
  - Acceptance: only signal→upsert/weight/decay/ttl constructs; version immutable per artifact; additive-only rules encoded in validator (`mapping_version: Literal[1]`, `extra="forbid"`, bounded sizes, types validated via core.signal_type; capabilities must reference declared kinds)
  - Verify: tests/unit/test_mapping_compiler.py — reference fixture loads; 7 bad-spec parametrized cases raise MappingError
  - Files: src/graph_memory/mapping/spec.py (+__init__)
- [x] Task: mapping compiler to deterministic ops (pure)
  - Acceptance: compile(spec, envelope) → typed NodeOp/EdgeOp plan that is pure and byte-stable; no wall clock; mypy strict
  - Verify: tests/unit/test_mapping_compiler.py — deterministic double-compile equality; unmapped signal types compile to empty plan (additive-only, forward-safe)
  - Files: src/graph_memory/mapping/compiler.py
  - Notes: the compiler emits typed plans; the ClickGraph/Cypher + SQL surfaces live at the gateway tier (projector task below), keeping the compiler framework-free.
- [x] Task: `projector` incremental projection (gateway tier)
  - Acceptance: batch of signals → expected graph rows; tenant predicate; served by ClickGraph
  - Verify: tests/integration/test_projector_live.py — projector writes through ClickHouseGateway (parameterized SQL, tenant predicate on every statement, ReplacingMergeTree identity = observation identity, replay-convergent server-side); ClickGraph serves the SAME state via cypher over a schema mapping of gm_nodes/gm_edges (`-m live`, real CH + real clickgraph.exe). Note: the tour-de-force here is that the compiler emits a typed plan and the gateways own the wire (SQL/Cypher stay at the adapter boundary, SPEC §14 "adapters add, core never changes").
  - Files: src/graph_memory/projector/{engine.py,gateway.py,gateway_clickhouse.py,ddl.py}, tests/integration/test_projector_live.py
- [x] Task: full replay + convergence test
  - Acceptance: replay from empty == live projection (zero diff) incl. shuffled-batch property test
  - Verify: tests/unit/test_projector.py — replay == incremental; permuted + duplicated delivery yields identical observation state (identity keys make order structurally irrelevant)
  - Files: src/graph_memory/projector/{engine.py,gateway.py}
- [x] Task: reflection ticks (decay, similarity edges) with injected Clock
  - Acceptance: decay identical for same (events, spec, clock) regardless of wall-clock
  - Verify: tests/unit/test_projector.py — decayed_score at FixedClock == pure math (snapshot ordering test); Projector.reflect() rewrites SIMILAR_TO wholesale (clear-first) in sorted user/pair order → idempotent double-reflect, symmetric edges, min-shared threshold; similarity edges live in BOTH gateways (in-memory + ClickHouse) with the same digests
  - Files: src/graph_memory/projector/engine.py, src/graph_memory/projector/{gateway.py,gateway_clickhouse.py}
- [x] Task: snapshot/adjacency materializers + `context-service` read model + cache
  - Acceptance: ≤3 point lookups per get(); jittered TTL + single-flight; staleness metadata present
  - Verify: tests/unit/test_context_service.py — snapshot = observations + nodes_of (2 lookups); single-flight + invalidation prove cache window; UserContext.is_stale asserts staleness contract
  - Files: src/graph_memory/context/{service.py,__init__.py}, src/graph_memory/projector/engine.py
- [x] Task: live-tier gate for M1 (real CH + real ClickGraph server)
  - Acceptance: `-m live` mirrors the convergence + projection tests
  - Verify: tests/integration/test_projector_live.py — (1) ClickHouseGateway vs InMemoryGraphGateway cross-backend zero-diff over observations AND capability snapshots through full projectors; tenant predicate isolation asserted server-side; (2) ClickGraph parity: booted clickgraph.exe against local CH, schema loaded via /schemas/load, cypher MATCH over the mapped tables returns exactly the projected HAS_SKILL edges. Both `-m live`, skipped cleanly when binaries absent.
  - Files: tests/integration/test_projector_live.py

## M2 — surfaces

- [x] Task: MCP server stateless (2026-07-28 JSON mode) with ≤4 tools + compat mode
  - Verify: tests/unit/test_mcp_channel.py — real ASGI app under uvicorn + official SDK client: 4 tools (get_context/search_memory/list_capabilities/get_signal_schema), tenant never a tool arg, structured content with staleness metadata, PlatformError payloads ride tool errors; stateless POST → JSON without session id, compat mode → session id. PROTOCOL_REVISION pinned 2026-07-28.
  - Files: src/graph_memory/channels/mcp/server.py
- [x] Task: REST `/v1` context endpoints (cursor pagination, ETag, Idempotency-Key)
  - Verify: tests/unit/test_rest_channel.py — ETag/304 revalidation, Cache-Control from provider max_age (never invented), cursor walk without repeats (+400 on bogus cursor), idempotent prefetch replay header; cross-tenant access unrepresentable (tenant rides the credential).
  - Files: src/graph_memory/channels/rest/{api.py,__init__.py}
- [x] Task: webhook channel (HMAC sign, retry ladder, endpoint disable)
  - Verify: tests/unit/test_webhook_channel.py — signature binds ts+body (constant-time verify, freshness window), ladder 5/25/125/600s observed via injected sleeper, statuses recorded per attempt, exhaustion auto-disables (re-enable manual), same bytes on every attempt.
  - Files: src/graph_memory/channels/webhooks/{signing.py,channel.py}
- [x] Task: SDK-thin python client + channel parity e2e
  - Verify: tests/integration/test_channel_parity.py — one provider, three real HTTP surfaces: MCP get_context structured payload == REST body == SDK reads; SDK ingest accepted==3 flows through the ingest→projector pipeline; iter_items cursor walk == payload items; prefetch idempotency replay.
  - Files: src/graph_memory/channels/sdk.py
- [x] Task: `tenancy` registry + quotas + two-phase offboarding + 7k onboarding bench
  - Verify: tests/unit/test_tenancy.py — onboarding idempotent metadata-only (7k bench under 10s, no per-tenant DDL), lifecycle law (revoke precedes purge; post-purge terminal), token-bucket lanes per (tenant, lane): whale burst cannot starve co-tenants.
  - Files: src/graph_memory/tenancy/{registry.py,quotas.py,errors.py}
- [x] Task: tenant guard injected predicate hardening (mismatch = fail closed)
  - Verify: RegistryAwareGuard wraps any TenantGuard: unknown → TenantNotFound, revoked → instant deny (phase-1 offboarding IS this branch), suspended → deny; resumed states re-admit. Wired via channels; stable codes → 403.
  - Files: src/graph_memory/tenancy/guard.py (RegistryAwareGuard added; StaticBearerGuard preserved)
- [x] Task: broker adapters Kafka/Pulsar/NATS via conformance kit
  - Verify: tests/unit/test_broker_conformance.py — kit fixtures through honest drain paths (duck-typed consumers, kit stamping rules pinned to outgoing_attributes), per-user order preserved with 2 mid-stream duplicates absorbed + poison `{"not json"` recorded not dropped (service: malformed JSON frames are poison, never 500s).
  - Files: src/graph_memory/signals/brokers/{base.py,kafka.py,pulsar.py,nats.py}
- [x] Task: SLA bench in CI (synthetic profile, p99 ≤1s)
  - Verify: tests/unit/test_bench.py + `platform bench hotpath --p99-budget 1s` (exit 1 on regression): p99 ≈1ms on the in-memory profile (concurrent lanes), tiny budget flips red; budget parser (1s/500ms/plain).
  - Files: src/graph_memory/ops/{bench.py,cli.py}

## M3 — loop & proof

- [x] Task: outcome ledger + effectiveness scorer + weight writer
  - Acceptance: outcome verdicts attach by correlation_id, score deterministically, and the re-projection they cause stays replay-safe
  - Verify: tests/unit/test_effectiveness_loop.py — attach on the origin event id (not the dedup hash), Laplace rate ⇒ clamped multiplier, per-cohort weight rows keyed (kind, target, cohort) so cohorts never collide; apply_effectives rewrites gateway rows in one replay (0.5 → 0.5×(0.5+2/3)); same-cohort class collisions resolve by size then name; null tick without the spec block; alpha-0 pins the 0.5 floor.
  - Files: src/graph_memory/loop/{tick.py,__init__.py}, src/graph_memory/mapping/{spec.py,compiler.py}, src/graph_memory/projector/engine.py
- [x] Task: eval harness (hit-rate@k, counterfactual replay, lift report)
  - Acceptance: prove personalization offline, deterministically
  - Verify: tests/integration/test_effectiveness_divergence.py — hit-rate@1 0.5→1.0 (lift = +0.5 via graph_memory.evals); counterfactual replay: dropping the graph-inert loop signal yields an EMPTY diff (noise controllability), dropping recos diverges exactly the recos capability and never skills.
  - Files: src/graph_memory/evals/{harness.py,__init__.py}
- [x] Task: reference workload (synthetic ITF-style cohorts) + divergence e2e
  - Acceptance: §16.1 falsifiable — different tops for cohorts with identical skill gaps; no-loop baseline identical
  - Verify: baseline recos identical cohort-blind; the loop-carrying engine flips cohort-b's top to skill:rust while cohort-a keeps skill:python; the skills capability stays identical in both engines (the gaps never moved).
  - Files: tests/integration/test_effectiveness_divergence.py
- [x] Task: cost gate CI + per-tenant metering dashboards data
  - Acceptance: §16.6 — the synthetic ledger recomputes §12 within ±15% in CI
  - Verify: tests/unit/test_cost_and_observer.py — profile events measure ~1023B (the ~1KB §12 target), the recompute lands at 0.977 TB/mo within ±15% of 1.07 (12-mo 11.72 vs 12.8); `platform bench cost` exits 1 on double-sized rows.
  - Files: src/graph_memory/ops/{cost.py,cli.py}
- [x] Task: scripted `LlmObserver` stub behind port (Loop 2 stub)
  - Acceptance: the observer is advisory evidence, never kernel control flow
  - Verify: ScriptedLlmObserver records gmp.observer.* spans (verdict/reason) through the SpanSink port with the injected clock; NoopObserver silent; the protocol returns a dict — nothing an observer says can mutate graph state.
  - Files: src/graph_memory/observability/{observer.py,__init__.py}
- [x] Task: demo POC repo (Pulsar + AI-generated signals) against deployed platform
  - Acceptance: an agent-app POC pushes ITF-shaped signals and reads the personalized context back
  - Verify: tests/test_poc_demo.py (generator determinism + correlation binding + batch chunking); LIVE demo run against `platform serve` — 16 events generated/ingested, context for demo-user-0000 served identically by REST and MCP (parity OK), worked demo ports; Pulsar path ships as producer_pulsar.py behind the [pulsar] extra.
  - Files: F:\graph-memory-platform-poc (public: github.com/hari2353/graph-memory-platform-poc)
  - Ops note: `platform serve` gained the composition-root projection loop (convergent replay every 1s + cache clear) and the ClickHouse adapters gained per-thread clients — ingest → projection → context is now a live one-terminal loop.

## Backlog (post-M3, recorded only)

- Additional broker drop-ins via SPI kit: SNS/SQS FIFO, Azure Service Bus,
  Google Pub/Sub, Kinesis, Redis Streams.
- Loop 3 strategy-variant A/B framework with pre-registered experiment plans.
- TS SDK; optional embedding/semantic-search adapter.

## Spec audit closure (2026-10-21 — full review against SPEC §5/§13/§16)

- [x] Gap: MCP missing 2 of the SPEC-named tools → `get_effectiveness`
  (`EffectivenessScorer` port + `context/explain.py` renderer) and
  `explain_context` (`ContextExplainer` port: raw observations, per-row decayed
  score, produced-by map); unwired deployments fail as anticipated tool errors.
  Verify: tests/unit/test_mcp_channel.py (wired + unwired live-server tests).
- [x] Gap: no durable tenant registry → `tenancy/registry_ch.py` (ReplacingMergeTree
  metadata rows; idempotent onboard; shared lifecycle laws) + `platform tenancy
  onboard|list|status`. Verify: live registry class + CLI tests (-m live).
- [x] Gap: no loop operator artifact → `platform loop run` (+ the loop as part of
  the deterministic boot path: stored rows carry tick weights, so serve boot /
  `replay run` / the 1s projection pass all score+replay — ContinuousEffectivenessLoop,
  Projector.set_overrides). A fresh boot re-derives the same state (§16.2 with loop ON).
  Verify: tests/e2e/test_full_loop.py (fixed point + fresh-boot convergence identity).
- [x] Gap: §11 hot-path spans were a port with no emitter → REST emits one span
  per request with per-stage wall times (authn/admission/lookup_assemble);
  ClickHouse span-stub table (`span_audit`) + buffered `ClickHouseSpanSink` +
  `platform spans`; serve flushes on the projection tick.
  Verify: tests/unit/test_rest_channel_spans.py + live sink roundtrip.
- [x] Gap: §5 metering tables "from day 1" → MeteringCounters + MeteringTable
  (`metering_events`) + REST ticks + `platform metering`. Verify: unit + live tests.
- [x] Gap: `tests/e2e` tier and `replay run --from`/serve `--config` (§13) →
  tests/e2e/test_full_loop.py; replay run with --from resume; serve --config YAML.
  Verify: e2e journey; CLI conventions in ops/cli.py.
- [x] Gap: ops compose posture → ops/compose/clickhouse.yaml (documented dev posture).
- Recorded (not built): `profile_snapshots`/`user_adjacency_rollup` CH
  materialization (ContextCache plays the role; backlog), OTLP→Langfuse exporter
  (SpanSink adapter; backlog), `platform dev --embedded` chdb tier (backlog).

## Found live (2026-10-21, usecase drive)
- [x] Defect: the serve per-interval pass ran eplace_all() as TRUNCATE +
incremental insert, so concurrent REST/MCP readers observed half-built graphs
every second (store flapped 12/14/17/0/20/0/10 across seconds; reads returned
none mid-rebuild; the tutor-usecase drive's baseline reads hit empty windows).
Fix: eplace_all() opens a staged rebuild (writes land in gm_*__staging
hold-out tables; readers keep the last committed projection) and
commit_rebuild() publishes it atomically with EXCHANGE TABLES (verified
live: swap semantics proven on CH 25.12). commit_rebuild() is a GraphGateway
port method; in-memory pointer-swaps, CH exchanges; engine.replay() commits
at the end - serve boot / eplay run / platform loop run / the 1s pass all
share the semantics; fresh-boot convergence (A16.2) untouched. Usecase drive
follow-up: run-scoped experiment TENANT per run (tutor-app-run-<id>) so reruns
inherit no prior ledger truth; baseline honestly flat.
Verify: tests/unit/test_gateway_rebuild.py + tests/integration/
test_gateway_swap_live.py (-m live); platforms gates re-run: 210 offline ok,
14 live ok, hotpath p99 0.66ms ok, cost 0.977TB/mo ok.
- [x] Whole-loop leg shipped and proven end-to-end against the deployed
  stack: `ops/compose/pulsar.yaml` (pulsar:3.3.5 standalone, persistent
  volume, REST :8099 / binary :6650), `PulsarCliReceiver` drain worker
  (admin REST + in-container CLI; JSON frames are poison-recorded, never
  dropped) and the `platform drain-brokers` CLI wiring the broker trail into
  the same ledger the HTTP ingest uses. Earned wire contract: topics
  `gmp.signals.<tenant>`; payload = base64(events JSON); partition key =
  user; after subscription creation run
  `pulsar-admin topics reset-cursor -s <sub> -m earliest` (the REST
  earliest-reset endpoint 404s on 3.3.5; CLI reset works).
  Verify: usecase `uv run whole-loop` live (f66ce43 platform / 594122a
  usecase): 16 signals produced -> drain accepted=16 -> flat baseline -> 8
  outcomes via broker -> loop cells=4 -> REST diverged -> ClickGraph Cypher
  reads match REST exactly (0.625/0.375).
- [x] Usecase agent tier proven conversational: a FastMCP tool surface plus
  an a2a-sdk v1 coach hosted in one process; the A2A conversation arc ran
  against this repo live and visibly improved after one loop tick
  (round-1 0.500 flat for all cohorts -> round-2 0.583 promoted tops,
  diverged, narrative changed). a2a-sdk 1.2.3 contract notes for future
  integrations: JSON-RPC method name is `SendMessage` (not
  `message/send`); requests need the `A2A-Version: 1.0` header.
  Verify: usecase 084bf06 (`uv run conversation` + `-m live` tier, 3 live
  tests green).
- Backlog (serving observations from the usecase drive): the projector
  writes only user nodes, so graph-side skill endpoints must be materialized
  (the usecase does it with read-only serving views over `gm_edges`); and
  ClickGraph resolves node joins by key VALUE, so multi-tenant Cypher needs
  the tenant property on every matched variable or lookalike keys leak
  across tenants.

## M4 — market refresh (docs/research/06): parity practices + the unclaimed loop

Recorded 2026-10-27 from `docs/research/06-market-refresh-2026-10.md`;
priority order below. M4.1 landed 2026-10-27 (offline-gated like M0–M3);
M4.2–M4.6 are unbuilt — each task lands offline-gated like M0–M3.

- [x] Task: governed forgetting + user memory controls (M4.1, P0)
  - Acceptance: a forget is a ledger event (a signal family declares `forgets:`
    directives — kind-wide or `{template}`-scoped); projector applies
    bi-temporal supersession scoped to (tenant, user, kind, target or
    kind-wide) — pure, convergent, replay-safe; MCP `delete_memory` + REST
    `POST /v1/forget/{user}` behind a new coarse `memory:write` scope; span +
    metering (`forget` event) emit per call; market floor: view/edit/delete
    expectations (research 06, governance).
  - Verify: 238 passed / 10 skipped offline (`pytest -m "not live"`); replay
    convergence holds WITH forget flows under every batch permutation +
    duplicates (`test_incremental_convergence_under_any_batch_ordering…`);
    divergence control proves removing the forget changes the output
    (`test_counterfactual_replay_dropping_the_forget_event_diverges`);
    explain_context surfaces tombstone provenance; REST+MCP forget surfaces
    round-trip into the read model; tenant-from-bearer isolation unchanged;
    `ruff check/format`, `mypy --strict src tests` (96 files), `platform bench
    hotpath --p99-budget 1s` PASSED (p99 0.33ms), `platform bench cost` in
    budget.
  - Files: src/graph_memory/{core/types,core/errors,core/ports}.py,
    src/graph_memory/mapping/{spec,compiler}.py,
    src/graph_memory/projector/{gateway,gateway_clickhouse,ddl,engine}.py,
    src/graph_memory/signals/{forget,envelope}.py, settings.py,
    src/graph_memory/channels/{mcp/server.py,rest/api.py},
    src/graph_memory/context/explain.py, src/graph_memory/ops/cli.py,
    src/graph_memory/observability/{metering,observer}.py,
    src/graph_memory/tenancy/guard.py, tests/unit/test_forget_{mapping,
    projector,channels}.py, tests/fixtures/mapping_with_forget.yaml
  - Notes: design decision — a forget is an OVERLAY directive row keyed
    (tenant, user, kind, target|None, forget_at), never a state mutation of
    edge rows; visibility is the pure predicate `observation_is_dead`
    (equal instants die; tie-free), so directives are replay-idempotent and
    deliberately skip rebuild staging (ClickHouse `gm_forgets` mirrors this;
    `target_any` flag encodes kind-wide). Deviations from research 06 §M4.1:
    REST is `POST /v1/forget/{user}` (path user, consistent with context
    endpoints) — sketch said `POST /v1/forget` with user in body;
    correlation_id-scoped forgetting is DEFERRED (envelope v1 has no
    correlation_id; kind/target scoping shipped). Channel parity on the
    forget surface is covered by per-channel round-trip tests, not yet a
    shared parity harness leg (test_channel_parity.py untouched).
- [x] Task: hybrid retrieval in one store (M4.2, P1)
  - Acceptance: ranked search = CH full-text + typed/traversal ordering +
    loop-weight ranking feature (competitors have none), behind the same
    point-lookup SLA discipline; vector optionally behind an
    EmbeddingProvider port (local ONNX), never on the default context path.
  - Verify: 253 passed / 10 skipped offline (`pytest -m "not live"`, was
    238); 15 new tests in `tests/unit/test_hybrid_search.py` cover the
    shared lexicon (bounded deterministic tokens + ONE `lexical_hits`
    metric), the pure kernel `(lexical desc, typed desc, loop desc, kind,
    target)` ordering with tie-breaks + browse mode + node-text traversal
    hits, adapter behavior (grouping end-to-end, loop-weight overrides
    FLIP the ranking, tenant/capability errors, limit clamped 1..25,
    forget tombstones govern search + replay-convergence identity),
    channel parity (MCP == REST byte-identical via fixed clock), scope
    403/auth 401/unknown-capability 404/cross-user isolation, unwired REST
    → anticipated `not_wired` (MCP fallback covered by
    `test_mcp_channel.py` unchanged); `ruff check/format`, `mypy --strict
    src tests` (99 files), `platform bench hotpath --p99-budget 1s` PASSED
    (p99 0.35ms vs 0.33ms before — search is off the hot path), `platform
    bench cost` unchanged (0.977 TB/mo).
  - Files: src/graph_memory/core/{lexicon,ports}.py (new lexicon + ports),
    src/graph_memory/projector/{gateway,gateway_clickhouse}.py
    (`search_edges`: in-memory client metric; CH `positionCaseInsensitive`
    SQL pushdown, dead filtering client-side),
    src/graph_memory/context/search.py (rank_search kernel +
    MemorySearchService adapter), src/graph_memory/channels/
    {mcp/server.py,rest/api.py}, src/graph_memory/observability/metering.py
    (`search` event), src/graph_memory/ops/cli.py, docs/SPEC.md §16.8,
    tests/unit/test_hybrid_search.py
  - Notes: deviations from research 06 lever 2 as sketched — the shipped
    lexical metric is the DETERMINISTIC case-insensitive substring hit
    count (BM25-style scoring parked; clickhouse can add BM25 without
    changing the kernel contract since tokens + metric stay in core), and
    the `EmbeddingProvider` port is defined but UNWIRED (no ONNX dep; the
    vector leg is a documented seam, not code). MCP keeps the tool
    name/args and degrades to the documented snapshot post-filter when
    unwired; REST adds `GET /v1/search/{user}` with span
    `gmp.channel.rest.search` + metering `search` row. Search is per-user
    point work (decayed_score at one injected instant, bounded ≤25) and
    never the default context assembly path.
- [x] Task: OP-Bench-style over-personalization guard + external benchmark
    harness strap (M4.3, P1)
  - Acceptance: loop OFF == no-loop baseline (existing §16.1 machinery);
    loop ON beats baseline AND does not degrade cold/fresh cohorts
    (over-personalization guard, research 06 benchmark chapter).
  - Verify: 256 passed / 10 skipped offline (was 253); divergence fixture
    extended with TWO cold cohorts — user-c1 (its own unscored cohort-c)
    and user-n1 (recos omit the cohort attribute → the "" route, the
    leak-prone default) under the SAME identical skill gaps; 3 new tests:
    `test_cold_cohorts_never_move_under_the_loop` (loop tops for cold users
    byte-identical to their OWN baseline tops; tick scores ONLY cohort-a/-b),
    `test_over_personalization_probe_publishes_the_matrix` (deterministic
    matrix: warm cohorts diverge pairwise, cold cohorts untouched, warm
    hit-rate 0.5→1.0 = §16.1 lift, cold hit-rate equal in both regimes,
    verdict `no-over-personalization`, re-run byte-identical),
    `test_probe_flags_a_cohort_leak_as_over_personalization` (negative
    control via a caller-injected buggy row set sharing the "" route:
    cold "" cohort degrades → verdict flips to `over-personalized` — the
    instrument provably FAILS when a writer leaks); `ruff`, `mypy --strict
    src tests`, hotpath p99 0.34ms PASSED, cost unchanged.
  - Files: src/graph_memory/evals/{harness,__init__}.py
    (`over_personalization_probe` + `ProbeCell`, JSON-stable matrix with
    warm/cold cells, per-regime hit-rates all/warm/cold,
    `override_rows` negative-control seam), 
    tests/integration/test_effectiveness_divergence.py
  - Notes: harness MATRIX published from eval fixtures per todo; the
    LongMemEval-style external corpus replay stays the recorded P2
    follow-up (per todo text). Semantic honesty in the matrix: a warm
    cohort whose baseline tie-break already agrees with its evidence shows
    zero per-regime movement (`cold_degraded_cohorts` / `warm_diverged_
    cohorts` are per-regime deltas) — the pairwise §16.1 divergence rides
    `warm_tops_mutually_distinct`.
- [ ] Task: interop reach — TS SDK + OpenCode/Claude Code MCP plugin recipes
    (M4.4, P2)
  - Acceptance: agents mount the platform as memory from the two hosts whose
    plugins competitors already ship (research 06, reach section).
  - Verify: documented recipes against our own MCP surface (6 tools),
    parity checklist; TS SDK thin facade mirroring channels/sdk.py contract.
  - Files: docs/interop/*, channels/sdk.py reference, supersedes the old
    backlog line "TS SDK; optional embedding/semantic-search adapter".
- [ ] Task: export/portability API (M4.5, P2)
  - Acceptance: `GET /v1/export/{user}` over the read model +
    ledger-scoped history for GDPR portability; span + metering trace.
  - Verify: unit + parity + tenant-isolation tests.
  - Files: src/graph_memory/channels/rest/*, tests/unit/test_rest_channel*.py
- [ ] Task: precompute + prewarm path (M4.6, P2)
  - Acceptance: CH-materialized per-(user,capability) payloads refreshed on
    the 1s projection tick (staged-rebuild semantics reused);
    `POST /v1/prefetch` warm-on-login with hit-rate metering;
    proposal gate: context payload token budget (target ≤ ~2K) asserted in
    the bench suite.
  - Verify: staged-rebuild tests extended (no half-built reads),
    hotpath p99 ≤1s, cost recompute ±15%, prefetch hit-rate counter.
  - Files: src/graph_memory/projector/gateway*.py, context/*,
    ops/{bench.py,cli.py}, supersedes the recorded backlog item
    "profile_snapshots/user_adjacency_rollup CH materialization".
