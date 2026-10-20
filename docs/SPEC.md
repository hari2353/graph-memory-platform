# Spec: graph-memory-platform (the bible)

> **Status:** v1.0-draft — awaiting human review before any module spec or code.
> **Evidence:** decisions cite `docs/research/01..05` and the requirement record
> `docs/RESEARCH_BRIEF.md`. Every module must trace to a module id in
> `docs/CAPABILITY_MAP.md` (approved next).

---

## 1. Objective

A generic, enterprise-grade **memory platform for AI agents**. Any application
emits typed user signals; the platform folds them into a per-user **memory
graph**; agents read **hyperpersonalized context** back through pluggable
channels. The graph is the compounding asset (Karpathy's "delete everything,
keep the graph"): every signal, recommendation, and outcome becomes an edge
with a weight, and the ledger that produced it is never discarded.

**Who it serves**

| Persona | What they do | What they get |
|---|---|---|
| App developer | Emits signals (HTTP batch or a broker adapter) | Memory accrues with zero inference on their side |
| Agent author | Mounts the MCP server / calls the REST API | Typed, tenant-guaranteed, <2s context tools |
| Memory author | Writes a versioned mapping spec (YAML) | Declarative grammar; the kernel compiles it to SQL |
| Operator | Onboards a tenant, watches metering/evals | Metadata-only onboarding, per-tenant cost truth |

**Why this beats incumbents** ([01]): every shipped agent-memory platform runs
LLM inference on *every write* and meters it (Zep by episode bytes, Mem0's ADD
flow); only Zep is truly bi-temporal; none has an outcome→effectiveness loop.
This platform: deterministic, declarative projection (LLM refinement is an
*optional adapter*), bi-temporal edges from day one, and the loop as a
first-class feature.

**Requirements traced** (R-ids come from `docs/RESEARCH_BRIEF.md`):

| Req | Statement | Where it lands |
|---|---|---|
| R1 | Broker-agnostic ingest | [signals] — CloudEvents envelope + adapter seam |
| R2 | 7,000 tenants, one environment | [tenancy] — shared storage, keyed quotas |
| R3 | <2s context-output SLA | [context-service]+[outputs] — p99 ≤1s CI gate |
| R4 | Pluggable outputs (MCP + REST + webhook + SDK) | [outputs] — channel adapters |
| R5 | Cheap without quality loss | [platform-ops] — §12 cost budget |
| R6 | Simple generic architecture, SOLID, back/future compat | §3, §18 |
| R7 | Enterprise-ready from start (auth, quotas, audit, privacy) | [tenancy], [observability], §17 |

**Non-goals (v1):** no console/UI; no vector-embedding store inside the kernel
(retrieval uses the graph + ClickHouse text keys; embeddings are an adapter);
no mandatory online LLM anywhere (Loop 2/3 stay behind ports); no cross-region
federation; no per-tenant clusters except the whale-class tier.

---

## 2. Assumptions (explicit; wrong ones invalidate sections)

1. Kernel services are **Python 3.12**; ClickGraph (Rust) is consumed strictly
   through its public HTTP/Cypher API — no forks, no internal imports.
2. First-class ClickHouse posture: self-hosted 3-node + S3 cold tier; CH Cloud
   mode supported by config; dev/CI uses **embedded chdb**.
3. Tenant scoping is enforced in the platform's Cypher adapter (always-injected
   parameterized predicate). A native compiler-level tenant predicate inside
   ClickGraph is recorded as future upstream work, not assumed.
4. The reference workload is the ITF-style skill loop (synthetic cohorts).
5. Open-source platform repository under `hari2353`; SDKs: Python-first, TS later.

---

## 3. Architecture

```
 app / broker adapters ──▶ signals: CloudEvents → ClickHouse ledger (source of truth, append-only)
                              │  mapping spec (versioned, declarative YAML)
                              ▼
              projector: deterministic, replay-idempotent projection
              ├─ graph upserts (bi-temporal edges, weights, decay) via ClickGraph write path
              ├─ reflection jobs: recency/decay, similarity + community (Jaccard/Louvain)
              └─ materializations: profile_snapshots + adjacency rollups (the read model)
                              │
        effectiveness-loop ◀──┘ (outcomes → RECOMMENDED/ACTS_ON/OUTCOME edges → scoring → weights)
                              ▼
          context-service: assemble per-user context (≤3 point lookups + cache)
                              ▼
             outputs: MCP (primary) · REST · webhooks · SDKs
                              ▼
        observability: OTel spans → Langfuse; per-tenant metering; eval harness
```

**The load-bearing principle — ledger is truth.** The graph is a *projection*:
rebuilding it from the ledger must converge to the same state (replay
idempotence is a test, not a hope). Consequences the modules may not violate:

- Projection steps are pure functions of `(ledger rows, mapping spec version)`;
  no wall-clock inside projection logic (time comes in as an injected Clock).
- Signals are never mutated after landing; corrections land as new events
  (bi-temporal supersession, [01] Zep's model: `valid_at/invalid_at` for
  semantics vs `created_at/expired_at` for ingestion).

**SOLID seams (ports, protocol classes):** `SignalSink` (HTTP/brokers),
`GraphGateway` (ClickGraph client), `Clock`, `ContextProvider` (the pure read
model behind every channel), `ChannelAdapter` (MCP/REST/webhook/SDK),
`LlmObserver` (optional; Loop 2/3), `TokenVault`-ish principals arrive as auth
middleware. Diagram per-port single responsibility; adapters add without
modifying.

---

## 4. Tech stack

| Concern | Choice |
|---|---|
| Runtime | Python 3.12, uv-managed |
| API/idempotent ingest | FastAPI + pydantic v2 (CloudEvents JSON envelope) |
| ClickHouse | `clickhouse-connect` (DDL/HTTP admin) + `chdb` in dev/CI |
| Broker adapters (M2) | `confluent-kafka` (Kafka), `pulsar-client`, `nats-py` |
| Graph | [ClickGraph](https://github.com/genezhang/clickgraph) pinned release; HTTP `/query`, `/schemas/load` (+ persistence env) |
| MCP server | official `mcp` Python SDK, **stateless Streamable HTTP + JSON response mode**, target spec **2026-07-28** with a 2025-11-25 compat mode [04] |
| Observability | OpenTelemetry SDK → OTLP/HTTP → Langfuse (`Authorization: Basic%20…`, `x-langfuse-ingestion-version: 4`) [05] |
| Quality | ruff (format+lint), mypy `--strict`, pytest + coverage (fail <90% on `core/`), contract tests per broker adapter |

---

## 5. Modules (details live in `SPEC-<id>.md`, authored per map order)

| Module | Bible-level contract |
|---|---|
| `signals` | CloudEvents 1.0.2 envelope, `partitionkey=user_key`; HTTP `POST /v1/signals:batch` (idempotency by `source+id`); broker adapters in M2 (Kafka, Pulsar, NATS) map per-user ordering via key; ledger dedup is the correctness owner (broker dedup is best-effort [02]); DLQ per route, bounded redelivery; at-least-once in, at-most-once-effect visible |
| `tenancy` | Shared storage: `tenant_id` in primary keys, **time-only partitions** (never tenant partitions [03]); tenant registry rows + quota rows (quota keyed `tenant_id` on one service user; `queries_per_normalized_hash` flood control); two-phase offboarding (revoke → async purge w/ audit window); tenant-class tier map (≤5 replica-set classes) |
| `memory-mapping` | Versioned YAML: `signals → entities/edges/upserts`, weights, decay curves, ttl hints; compiled to deterministic SQL; additive-only evolution (v2 may not reinterpret v1 payloads); compiled artifact replayable against any ledger retention window |
| `projector` | Incremental projection per signal batch + full replay command; reflection jobs (decay tick, similarity edges, community recompute) scheduled by sim-friendly Clock; maintains `profile_snapshots` + `user_adjacency_rollup` (the read model) and staleness metadata |
| `context-service` | Pure read model: `ContextProvider.get(user, capability)` = ≤3 tenant-scoped point lookups over snapshots/rollups + assembly; per-user cache (jittered TTL + single-flight/XFetch); response includes `generated_at`, `max_age_ms` (staleness surfaced, never silent [04]) |
| `outputs` | MCP first: ≤4 tools — `get_context`, `search_memory`, `get_effectiveness` (scores), `explain_context` (a graph-annotated provenance view); compact `structuredContent` + `resource_link`, summary ≤8k tokens, `ttlMs`/`cacheScope` hints; REST `/v1` user-scoped reads (`Idempotency-Key` writes, cursor pagination, ETag); webhooks HMAC `{id}.{ts}.{raw}` with retry ladder; SDKs = thin REST clients prewired with OTel [05] |
| `effectiveness-loop` | Generic: any app registers a "recommendation" signal type; outcome signals close the loop (`acted_on`, `outcome_improved`, feedback); scoring job computes effectiveness per (cohort/space, recommendation); context-service biases via weights; observer-LLM behind `LlmObserver` port produces instincts (Loop 2) later |
| `observability` | Span-per-phase on hot path (`authn → authz → lookup → assemble → reply`) for falsifiable p99; sampling = config (per-tenant head + tail-on-error/p99) with unsampled CH span-stub audit table; metering: per-tenant requests/quota/throttle tables from day 1 [03]; eval harness: offline hit-rate@k, counterfactual replay, held-out lift; A/B with pre-registered guardrails, ≥1-week windows [05] |
| `platform-ops` | Embedded dev (chdb), docker-compose replica-set (3 nodes + MinIO S3 cold); schema carries the cost levers: TTL + `TTL … GROUP BY … SET` rollups, `Delta/ZSTD` codecs, `LowCardinality` enums;
      load CI: synthetic 7k-tenant onboarding (metadata-only ≤ seconds) + p99 ≤1s steady-state gate + replay-rebuild equality; per-tenant restore = tenant-filtered export/replay |

---

## 6. Data architecture (sketches; exact DDL owned by module specs)

**Ledger (truth):**

```sql
CREATE TABLE signals_events_on_disk (
  tenant_id     LowCardinality(String),
  signal_type   LowCardinality(String),   -- namespaced, versioned, e.g. `itf.skill_assessed.v1`
  event_key     String,                   -- CloudEvents `source` + `id` → dedup identity
  user_key      String,
  occurred_at   DateTime64(3, 'UTC'),
  ingested_at   DateTime64(3, 'UTC') DEFAULT now64(3),
  envelope_v    UInt32,                   -- envelope version (additive-only evolution)
  payload       String,                   -- verbatim JSON; interpretation owned by mapping spec vN
  PROJECTION ev_dedup (SELECT * ORDER BY event_key)   -- complementary dedup index
)
ENGINE = ReplacingMergeTree
ORDER BY (tenant_id, user_key, signal_type, occurred_at, event_key)
PARTITION BY toYYYYMM(occurred_at)          -- time-only partitions [03]
SETTINGS index_granularity = 8192;
```

**Graph edges (bi-temporal, [01]):** `valid_at`, `invalid_at` (semantic
truth-time) alongside `created_at`, `expired_at` (ingestion-time). Weights
decay at reflection ticks; supersession = new edge + `invalid_at` on the old
(never DELETE during replay; purge is an async GC).

**Read model:** `profile_snapshots(tenant_id, user_key, capability,
snapshot JSON, generated_at, max_age_ms)` and
`user_adjacency_rollup(tenant_id, user_key, relation, top_k items, weights)`
— both maintained by the projector; the hot path touches **only these**.

---

## 7. Hot path: the <2s SLA (p99 ≤1s CI gate)

From [04], the p99 budget (steady state, 2× headroom under the SLA):

| Stage | Budget | Notes |
|---|---|---|
| MCP transport + JSON parse | 50 ms | stateless 2026-07-28 mode |
| Auth (local JWT verify) | 25 ms | claims → tenant binding; introspection forbidden on hot path |
| Tenant→range guard | 10 ms | injected predicate, parameterized |
| Point lookups (≤3) | 300 ms | snapshots + adjacency + effectiveness weight row |
| Assembly + cache | 100 ms | single-flight on miss |
| Reply serialize | 50 ms | `structuredContent` |
| **Total** | **≈ 535 ms p99 gate** | CI fails the build if the synthetic profile regresses |

Design rules that make the budget honest: context assembly **never** runs
graph traversal (traversal is reflection-time, possibly minutes), lookups are
tenant-scoped ordered-key reads, caches carry jittered TTLs, and one tenant's
stampede cannot evict another's keys (quota-keyed admission).

---

## 8. Tenancy (R2)

Adopted from [03]: **one shared graph, tenant-scoped**. `tenant_id` on every
node/edge/ledger row; the Cypher adapter injects an always-true,
parameterized tenant predicate — tenant is *never* a free tool argument (tool
schemas don't even accept it). Quotas: CH quota keyed by `tenant_id` +
API-layer token buckets per tenant (interactive vs batch separated, [03]);
onboarding = registry row + quota row + topic grant (metadata-only, seconds);
offboarding = revoke (instant) → async purge with audit-retention window;
per-tenant restore = tenant-filtered export/replay. Whale-class tenants route
to a tenant-class replica set; routing is a map, not a fork in code.

---

## 9. Channels & compatibility (R4)

MCP is primary but the **pure `ContextProvider` is the contract**; MCP, REST,
webhooks, SDKs are adapters ([05]). Channel compatibility policy: channels are
versioned (`/v1`, MCP tool `vN` fields), deprecation = documented remap +
6-month window; MCP conformance pinned to a spec revision in CI (compat style
of Cloudflare's dual-mode servers [04]).

---

## 10. Effectiveness loop (the differentiator)

Generic (not skill-specific): an app registers `space.recommendation.v1`-type
signals; outcome signals (`acted_on`, `outcome_improved`, feedback) attach by
`correlation_id`; scoring job (reflection tick) computes effectiveness per
(cohort-space, recommendation-class, context-capability) and writes weight
rows the context-service exposes. Cohort grouping = declared signal attribute
(app-provided), not a platform opinion — the platform is generic; the ITF
reference workload instantiates it.

---

## 11. Observability & evidence of value (R7)

Every hot-path request emits one trace with per-stage spans → Langfuse
(research 05 wiring), sampled by policy; unsampled traffic still lands in a CH
audit-span-stub table (full auditability without egress cost). The **eval
harness** is how personalization is *proven*: offline hit-rate@k on held-out
users, counterfactual replay of recommendation histories, held-out outcome
lift; online experiments require a pre-registered plan (metric, guardrails,
window) — the divergence assertion (§16) is the flagship offline eval.

---

## 12. Cost budget (R5)

From [05]: at 7,000 tenants × 50k events/day (≈350M events/day, ~1 KB each):
≈ **1.07 TB compressed/mo**, ≈ 12.8 TB at 12-month retention →
**default posture: self-hosted 3 nodes + S3 cold ≈ $790/mo** (CH Cloud
equivalence ≈ $3,150–3,750/mo); with 6-month TTL + daily rollups ≈ $700 vs
$1.6–1.9k. Storage levers live in the schema (TTL/types/codecs), not ops
heroics. Budget regression = CI cost check on the synthetic fixture.

---

## 13. Commands

```
dev:        uv run platform dev --embedded            # chdb + embedded ClickGraph, sim clock
serve:      uv run platform serve --config env/dev.yaml
test:       uv run pytest -m "not live"               # unit + integration
test-live:  uv run pytest -m live                     # real CH (:8124-style) + real ClickGraph server
e2e:        uv run pytest tests/e2e                   # full loop incl. divergence assertion
lint:       uv run ruff check . && uv run ruff format --check .
types:      uv run mypy --strict .
perf-gate:  uv run platform bench hotpath --p99-budget 1s
replay:     uv run platform replay --from 2026-01-01 --dry-run   # rebuild projection equality
onboard:    uv run platform tenancy onboard --tenant demo --class standard
```

---

## 14. Project structure

```
src/graph_memory/
├── core/            → ports (Protocols), domain events, errors (zero-IO)
├── signals/         → envelope, ledger repo, HTTP ingest, (M2) broker adapters
├── tenancy/         → registry, quotas, tenant guard (adapter-level), onboarding
├── mapping/         → mapping-spec loader/validator + SQL compiler (pure)
├── projector/       → incremental + replay engine, reflection jobs, materializers
├── context/         → context-service read model + cache
├── channels/        → mcp/ rest/ webhooks/ sdk sync tooling
├── loop/            → outcome ledger, scorer, weight writer
├── obs/             → otel wiring, sampling, metering, eval harness
└── ops/             → deploy configs, bench, replay CLI, cost checks
tests/               → unit | integration (chdb + real ClickGraph) | e2e | perf
docs/                → RESEARCH_BRIEF, research/01..05, CAPABILITY_MAP, SPEC*, ADR/
```

---

## 15. Code style (one real snippet beats three paragraphs)

```python
class ContextProvider(Protocol):
    """Pure read model. All channels render from this; it touches nothing else."""
    async def get(self, principal: Principal, user: UserRef, cap: Capability) -> UserContext: ...


class McpChannel:
    def __init__(self, provider: ContextProvider, guard: TenantGuard) -> None:
        self._provider, self._guard = provider, guard  # dependency inversion: channels add, core never changes

    async def get_context(self, args: GetContextArgs) -> McpResult:
        principal = self._guard.require_scope(args.oauth_token, Scope.CONTEXT_READ)
        if principal.tenant != principal.claims_tenant:      # claims are the ONLY tenant source
            raise TenantMismatch("tool arguments can never select a tenant")
        ctx = await self._provider.get(principal, args.user, args.capability)
        return to_structured_content(ctx, ttl_ms=ctx.max_age_ms, cache_scope=ctx.cache_scope)
```

Conventions: `ruff` default + `line-length 100`; modules own their errors;
Protocols in `core/` are the only cross-module vocabulary; no `import` cycles
(enforced in CI); async everywhere at the edge, sync allowed only inside pure
compilers; every external call is parameterized (no string-built SQL or
Cypher, ever); log-safe scrubbing for any operator-controlled name
(`core/logsafe.py`).

---

## 16. Success criteria (falsifiable)

1. **Divergence:** in the reference workload, after ≥N outcomes accrue, the
   context-service returns different top recommendations for two cohorts with
   identical skill gaps; a no-loop baseline returns identical ones. (e2e)
2. **Replay convergence:** full replay of the ledger from empty converges to
   the live graph with a `diff` of zero rows. (integration)
3. **SLA:** synthetic hot-path bench p99 ≤1s at steady state with the CI
   budget table; SLA 2s documented with headroom. (perf gate)
4. **Tenancy scale:** onboarding 7,000 tenants = 7,000 metadata rows only
   (seconds-scale script); no DDL per tenant; a whale backfill cannot starve
   co-tenants (quota test). (integration + perf)
5. **Channel parity:** `get_context` through MCP, REST, and SDK returns the
   same context body (normalized) from the same runtime state. (e2e)
6. **Cost:** the synthetic ledger at target load recomputes the §12 monthly
   storage estimate within ±15% in CI. (cost gate)
7. **Observability:** a scripted eval run produces a hit-rate@k and lift
   report for the reference workload; Langfuse shows the spans locally.
   (e2e + dev command)

---

## 17. Boundaries

- **Always:** parameterized Cypher/SQL (tenant predicate injected, never
  concatenated); ledger truth + replay-first changes; tests before commits;
  staleness surfaced in every context response; per-tenant metering on every
  request class; bounded payloads (≤8k-token summaries).
- **Ask first:** new signal types in the platform's own reference set; new
  channel adapters; schema/TTL/codec changes; new broker adapter; anything
  touching ClickGraph's public API assumptions.
- **Never:** tenant as an untrusted tool argument; LLM inference inside the
  kernel projection loop; silent staleness; mutating landed signals; storing
  raw secrets or PII outside declared envelope fields; redaction bypass
  (query-text discipline like ClickGraph's `sanitize_query`); deleting ledger
  rows (supersede, then async purge).

---

## 18. Backward & future compatibility

- Signal envelopes versioned (`envelope_v`, CloudEvents `specversion`); new
  fields additive; `dataschema` URIs pinned per version [02].
- Mapping specs: `mapping_version` rides on every projected row; v+1 never
  reinterprets v; compiled projections are replay-safe by construction.
- MCP: target 2026-07-28 stateless; a compat mode rides the same core [04].
- Channels/SDKs: deprecation ladder §9. Projection artifacts (snapshot JSON)
  are themselves versioned with `max_age_ms`-style hints carried in responses.

---

## 19. Milestones (each gates on its map module's acceptance)

- **M0 — kernel skeleton:** repo wired (uv/ruff/mypy/pytest), `signals` HTTP
  ingest + ledger schema + dedup, embedded dev profile, first contract tests.
- **M1 — memory:** `memory-mapping` DSL + compiler, `projector` incremental +
  replay + reflection, `context-service` + snapshots/rollups; replay
  convergence test green.
- **M2 — surfaces:** MCP server (stateless) + REST + webhook + SDK-thin,
  tenancy guard + quotas + onboarding CLI; SLA bench in CI.
- **M3 — loop & proof:** effectiveness-loop + eval harness + reference
  workload divergence assertion + cost/tenancy gates; demo POC repo
  (Pulsar + AI-generated signals) built against the M3 platform.

## 20. Open questions (answer before module specs)

1. Broker adapters in M2 exactly: Kafka + Pulsar confirmed — is NATS in or out?
2. Whale-class tiering: virtual (one cluster, classes in routing map) for v1 —
   confirm, or reserve dedicated clusters out of the box?
3. Loop-2 observer: ship a scripted (heuristic) observer stub with the port, or
   wait for a Langfuse key/LLM budget decision?
4. Repo privacy now: push the platform public at M0, or private until M3?

---

## 21. Evidence index

`01-prior-art-agent-memory.md` — incumbent models, bi-temporal, metering → §1, §6, §10
`02-ingest-multi-broker.md`   — envelope, ordering, dedup, DLQ, capacity → §5, §18
`03-multitenancy-seven-thousand.md` — shared storage, quotas, offboarding → §5, §8
`04-mcp-hot-path-sla.md`      — MCP revisions, stateless, p99 budget → §4, §7, §9
`05-outputs-observability-cost.md` — channels, Langfuse, evals, cost → §4, §9, §11, §12
