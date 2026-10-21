# graph-memory-platform

A generic, enterprise-grade **memory platform for AI agents**: any application
emits typed signals (brokers: Kafka, Pulsar, NATS; or HTTP), the signals land in
an append-only ClickHouse event ledger, deterministic **memory-engineering**
rules project them into a per-user memory graph served by
[ClickGraph](https://github.com/genezhang/clickgraph) (Cypher over SQL), and
agents consume **hyperpersonalized context** through pluggable outputs (primary:
MCP; plus REST /v1 API, webhooks, and a thin Python SDK).

Status: **M0–M3 implemented** — kernel, projector, channels, tenancy, brokers,
effectiveness loop, evals, SLA/cost gates. Research basis in
[docs/research/](docs/research/); the bible is
[docs/SPEC.md](docs/SPEC.md); planning ledgers in [tasks/](tasks/).

## Hard requirements (recorded 2026-10-20)

| Requirement | Target |
|---|---|
| Ingest | Broker-agnostic: Pulsar, Kafka, others + direct HTTP; no lock-in |
| Multi-tenancy | **7,000 tenants in one environment**, isolated namespaces + quotas |
| Latency SLA | **< 2 s** end-to-end on the context-output hot path (MCP et al.) |
| Outputs | Pluggable: MCP primary; REST/webhooks/SDKs researched and enabled |
| Observability | OpenTelemetry / **Langfuse** (per-query + per-loop spans) |
| Cost | Cheap to run without compromising quality; horizontally scalable |
| Architecture | Simple, generic building blocks; SOLID; backward + future compatible |

## Architecture

```
apps ──signals──▶ ledger (ClickHouse, append-only, (tenant, dedup) identity)
                     │
                     ▼  deterministic projection (pure: events+spec+clock)
              per-user memory graph (ClickGraph-served)
                     │
                     ▼  context-service (single-flight cache, staleness contract)
        ┌────────────┼───────────────┬──────────────────┐
        MCP /mcp     REST /v1        webhooks (HMAC)    thin SDK
        (primary)    (ETag, cursors, (retry ladder,      (channels/sdk)
        stateless    idempotent      auto-disable)
        JSON mode)   prefetch)
```

The **effectiveness loop** (SPEC §10 — the differentiator): outcome signals
(`acted_on`, `outcome_improved`, feedback) attach to recommendation origins by
`correlation_id`; a deterministic scoring tick aggregates per
`(cohort, recommendation-class)` (both app-declared attributes), Laplace-smooths
the acceptance rate into weight multipliers, and writes weight rows keyed
`(kind, target, cohort)` back through the pure compiler. Two cohorts with
identical skill gaps get **different top recommendations**; a no-loop baseline
stays identical (proven in CI, SPEC §16.1). Loop 2's LLM observer is an advisory
port (`observability/observer.py`) — it records verdicts as span evidence, it
never mutates graph state.

## Layout

```
src/graph_memory/
  core/         ports + domain (Principal, TenantGuard, Clock, errors); zero IO
  signals/      envelope (CloudEvents-ish), ledger (InMemory + ClickHouse),
                HTTP ingest edge, DLQ, broker adapters (Kafka/Pulsar/NATS)
  mapping/      declarative spec (YAML), pure compiler with weight_overrides
  projector/    engine (process/replay/reflect), InMemory + ClickGraph/CH gateways
  context/      context-service: EngineContextProvider + single-flight cache;
                explain.py (provenance + loop report faces for channels)
  channels/     mcp/ rest/ webhooks/ sdk.py — every channel a renderer
  tenancy/      registry (in-memory + durable CH rows), quotas, guards
  loop/         effectiveness tick + apply_effectives + the continuous pass
  evals/        hit-rate@k, counterfactual replay, lift (SPEC §11/R7)
  observability/ CH span-stub audit sink + metering tables + observer port
  spikit/       broker conformance kit (the adapters' shared test kit)
  ops/          CLI: platform replay|serve|loop|tenancy|spans|metering|bench|drain-brokers
tests/
  unit/         determinism, channels, tenancy, brokers, loop, gates, spans
  e2e/          full-loop offline journey (tenancy→ingest→loop→fixed point)
  integration/  ClickGraph/ClickHouse live tier (`-m live`), channel parity,
                divergence e2e, ops surfaces
docs/           research/01..05, CAPABILITY_MAP.md, SPEC.md (the bible)
tasks/          plan.md, todo.md
```

## Channels (parity proven in CI)

**Broker drain** (the Whole-loop ingest front door besides HTTP):
`docker compose -f ops/compose/pulsar.yaml up -d` boots a real Apache Pulsar
standalone; producers publish `base64(envelope JSON)` with ordering key =
partition key to `gmp.signals.<tenant>`; `platform drain-brokers` consumes via
the admin REST + the broker image's own CLI (backlog-driven, bounded, cursor
reset at creation) and adjudicates through the same ingest service the HTTP
channel uses - one ledger, no second ingest truth; poison lands in the DLQ.

| Channel | Contract |
|---|---|
| **MCP** (primary) | 6 read-model tools over streamable HTTP — `get_context`, `search_memory`, `list_capabilities`, `get_signal_schema`, `get_effectiveness` (the loop's report for the caller's tenant), `explain_context` (graph-annotated provenance per read); stateless JSON mode (2026-07-28 protocol) or sessioned compat; tenant from the bearer token, never a tool argument; failures ride `is_error` tool results |
| **REST /v1** | `/v1/context/{user}/{capability}` with ETag/304 revalidation, `Cache-Control` from the provider's staleness window, cursor pagination on `/items`, `Idempotency-Key` replay on `/prefetch`; every request emits one per-stage span + a metering tick |
| **Webhooks** | `X-GMP-Signature` HMAC-SHA256 over `ts.body` (constant-time verify, 300s freshness), retry ladder 5·5ⁱ s capped 600s ×5 attempts, auto-disable on exhaustion |
| **SDK** | `GraphMemoryClient` — thin HTTP facade (ingest, context, item pages, prefetch); no retry logic by design |

Parity e2e: one provider, three real HTTP surfaces, identical rendered payloads
(`tests/integration/test_channel_parity.py`).

## Verification (the gates every commit passes)

```
uv run ruff check . && uv run ruff format .
uv run mypy --strict src tests
uv run pytest -m "not live"        # full offline suite (deterministic)
uv run pytest -m live              # real ClickHouse + ClickGraph tier
uv run platform bench hotpath --p99-budget 1s   # SLA gate: p99 ≤ 1s (exit 1 on miss)
uv run platform bench cost                      # §12 recompute within ±15% (exit 1 on drift)
```

Falsifiable success criteria (SPEC §16): divergence, replay convergence, SLA,
7k-tenant scale, channel parity, cost, observability evidence — each has a
named test; the index lives in `tasks/todo.md` per milestone.

## Operating the platform (ops commands)

```
uv run platform serve --mapping tests/fixtures/mapping_skill_loop.yaml     # or --config env/dev.yaml
uv run platform replay                       # read-only ledger plan
uv run platform replay run --mapping PATH    # full rebuild = tick + replay (loop included)
uv run platform replay run --mapping PATH --from 2026-06-01T12:00:00Z   # additive resume
uv run platform loop run  --mapping PATH     # effectiveness tick, standalone (report + revalidated replay)
uv run platform tenancy onboard TENANT       # durable CH metadata row (no per-tenant DDL)
uv run platform tenancy list | status TENANT # lifecycle visibility
uv run platform spans --limit 20             # span audit-stub table (§11)
uv run platform metering --limit 20          # per-tenant request/throttle rows (R7)
```

`serve` owns the whole pipeline: boot = score the loop once, then replay; a
projection thread re-runs **tick + swap rows + convergent replay** every
`PROJECTION_INTERVAL_S=1.0` and drops stale cache entries — channels never
project. Span/metering buffers flush on the same tick into their ClickHouse
tables; a failed pass keeps serving the previous graph.

## Tenancy

Onboarding is metadata-only (7,000 tenants in seconds — no per-tenant DDL).
Rows live in the durable ClickHouse registry (`tenancy/registry_ch.py`), so
lifecycle survives restarts and is inspectable from ops. Lifecycle:
ACTIVE ⇄ SUSPENDED, revoke precedes purge, purge terminal. Quotas are
per `(tenant, lane)` token buckets — a whale backfill cannot starve co-tenants.
The tenant always rides the credential; cross-tenant access is not
representable.

## Cost posture (SPEC §12)

7,000 tenants × 50k events/day ≈ 350M events/day ~1KB → **1.07 TB/mo
compressed** (recomputed in CI from the synthetic fixture), 12.8 TB at 12-month
retention. Storage levers (TTL, codecs, types) live in the schema
(`signals/ddl.py`), not in ops heroics. Dual-write is one profile for the
ledger; per-tenant metering rows land in `metering_events` (day-1 surface).

## Posture notes (what ships vs. what's a deploy-day adapter)

- **Read model materialization:** the spec's `profile_snapshots` /
  `user_adjacency_rollup` role is played in-process by the single-flight
  `ContextCache` with provider-staleness invalidation; a CH-materialized
  variant stays in the backlog (the read path never computes at request time
  beyond the ≤3 point lookups, so the SLA is unaffected).
- **Langfuse / OTLP exporters:** `SpanSink` is the port; the ClickHouse
  span-stub audit table ships (rows land sampled or not). OTLP→Langfuse is a
  deploy-day adapter over the same port — no channel code changes.
- **Loop 2 observer:** scripted observer stub (`ScriptedLlmObserver`) records
  verdicts as span evidence; a real LLM observer plugs the same port without
  touching graph state.
- **`platform dev --embedded` (chdb):** backlog — chdb is unavailable on this
  dev OS; the embedded tier would reuse the same conformance suite. Local dev
  runs ClickHouse via the compose file in `ops/compose/`.
- **Continuous loop cadence:** the serve thread re-ticks every second because
  the scoring job is deterministic from ledger truth; deployments wanting a
  wider interval set `PROJECTION_INTERVAL_S` in `ops/cli.py` (or run
  `platform loop run` out of cron against a stopped instance).

## License

Apache-2.0.
