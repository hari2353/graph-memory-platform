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
