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
  context/      context-service: EngineContextProvider + single-flight cache
  channels/     mcp/ rest/ webhooks/ sdk.py — every channel a renderer
  tenancy/      registry (7k onboard), quotas (token buckets per lane), guards
  loop/         effectiveness tick + apply_effectives (SPEC §10)
  evals/        hit-rate@k, counterfactual replay, lift (SPEC §11/R7)
  observability/ span-stub audit sink + scripted LLM observer port
  spikit/       broker conformance kit (the adapters' shared test kit)
  ops/          CLI: platform replay | serve | bench hotpath | bench cost
tests/
  unit/         determinism, channels, tenancy, brokers, loop, gates
  integration/  ClickGraph/ClickHouse live tier (`-m live`), channel parity,
                divergence e2e
docs/           research/01..05, CAPABILITY_MAP.md, SPEC.md (the bible)
tasks/          plan.md, todo.md
```

## Channels (parity proven in CI)

| Channel | Contract |
|---|---|
| **MCP** (primary) | 4 tools over streamable HTTP; stateless JSON mode (2026-07-28 protocol) or sessioned compat; tenant from the bearer token, never a tool argument; failures ride `is_error` tool results |
| **REST /v1** | `/v1/context/{user}/{capability}` with ETag/304 revalidation, `Cache-Control` from the provider's staleness window, cursor pagination on `/items`, `Idempotency-Key` replay on `/prefetch` |
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
uv run platform replay --dry-run                 # projection equality plan
```

Falsifiable success criteria (SPEC §16): divergence, replay convergence, SLA,
7k-tenant scale, channel parity, cost — each has a named test; the index lives
in `tasks/todo.md` per milestone.

## Tenancy

Onboarding is metadata-only (7,000 tenants in seconds — no per-tenant DDL).
Lifecycle: ACTIVE ⇄ SUSPENDED, revoke precedes purge, purge terminal. Quotas are
per `(tenant, lane)` token buckets — a whale backfill cannot starve co-tenants.
The tenant always rides the credential; cross-tenant access is not
representable.

## Cost posture (SPEC §12)

7,000 tenants × 50k events/day ≈ 350M events/day ~1KB → **1.07 TB/mo
compressed** (recomputed in CI from the synthetic fixture), 12.8 TB at 12-month
retention. Storage levers (TTL, codecs, types) live in the schema
(`signals/ddl.py`), not in ops heroics. Dual-write is one profile for the
ledger; per-tenant metering rows land in the same registry.

## License

Apache-2.0.
