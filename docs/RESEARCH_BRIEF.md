# Research brief — graph-memory-platform

Canonical requirements record for the research + spec phases (2026-10-20).
Written before any research; the spec must trace every major decision back to
one of these lines or to a research finding.

## Vision (one paragraph)

Any application — existing or new — emits entity signals about its users
(`user.created`, `content.viewed`, `skill.assessed`, `feedback.given`, …).
Signals flow broker-agnostically (Pulsar, Kafka, NATS, direct HTTP) into an
append-only ClickHouse ledger. Deterministic, declaratively-specified
**memory engineering** projects signals into a per-user memory graph
(entities, typed edges, weights, decay) served by ClickGraph. AI agents get
**hyperpersonalized context** through MCP (primary) and other output channels
(REST API, webhooks, SDKs — to be researched and enabled). A closed loop is a
first-class feature: recommendations → outcomes → effectiveness weights feed
back into future context. Every agent call and reflection run is observable
via OpenTelemetry/Langfuse (ClickGraph's per-query spans are already
Langfuse-compatible).

## Hard requirements

1. **Ingest**: broker-agnostic — Pulsar, Kafka, others ("etc"); direct HTTP too.
   No lock-in; adding a broker is an adapter, not a redesign.
2. **Enterprise multi-tenancy**: 7,000 tenants in ONE environment. Isolated
   namespaces, per-tenant quotas/keys; no noisy neighbors; cost model must hold
   at that count.
3. **Latency SLA**: context output (MCP et al.) responds in **< 2 seconds**
   end-to-end. The write/reflect path may be slow; the read path is the SLA.
4. **Outputs**: MCP is the primary surface but NOT the only one — REST API,
   webhooks, SDKs, and anything else research surfaces should be planned as
   enabled-by-architecture channels.
5. **Cost**: cheap to operate without compromising quality — deterministic
   batch reflection, ClickHouse-native storage levers (TTL/codecs/tiering),
   no always-on GPU inference in the kernel loop.
6. **Architecture**: simple, generic building blocks; SOLID seams; backward and
   future compatible (versioned signal envelopes; additive-only mapping specs;
   rebuildable projections — the ledger is truth, the graph is a projection).
7. **Quality**: enterprise-ready from the start (auth, quotas, audit, privacy
   posture, deterministic replay), not bolted on later.

## Research topics (one file each under docs/research/)

| File | Topic |
|---|---|
| `01-prior-art-agent-memory.md` | Zep/Graphiti, Mem0, Letta/MemGPT, LangMem, cognee, first-party memory writeups — data model, update model, retrieval model, tenancy, pricing, gaps |
| `02-ingest-multi-broker.md` | CloudEvents, broker comparison, exactly-once-ish ingestion into ClickHouse, idempotency/dedup, backpressure, DLQ |
| `03-multitenancy-seven-thousand.md` | ClickHouse tenancy patterns at 7k tenants + the graph-layer analog (one shared graph vs per-tenant graph instances) |
| `04-mcp-hot-path-sla.md` | Current MCP spec/auth/SDKs, production MCP latency patterns, the <2s budget breakdown (precomputed views, caching, no request-time multi-hop) |
| `05-outputs-observability-cost.md` | REST/webhook/SDK channel patterns, Langfuse/OTel integration, memory-quality evaluation, ClickHouse cost levers + a cost model for 7k tenants |

## Process

research (this) → `docs/SPEC.md` (the bible) → implementation plan → build
(kernel repo here; demo POC as a second repo feeding AI-generated signals via
Pulsar) → tests (unit + integration + e2e, local-first) → push to GitHub.
