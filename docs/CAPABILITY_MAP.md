# Capability Map: graph-memory-platform

**Version:** 1.0 · **Date:** 2026-10-20 · **Status:** awaiting human review

The initiative decomposes into independently testable modules. Module ids are
permanent (specs, plans, and tasks reference them). Dependencies point one way;
two modules that need each other are one module.

| Module id | Responsibility | Depends on |
|---|---|---|
| `signals` | Signal envelope (CloudEvents 1.0.2 + `partitionkey=user_id`), event ledger schema in ClickHouse, idempotent ingest API (HTTP now; broker adapters), dedup + DLQ policy | — |
| `tenancy` | Tenant registry, namespace rules, per-tenant quotas/keys, tenant-scoped Cypher guard (injected predicate), onboarding/offboarding two-phase, audit export | `signals` |
| `memory-mapping` | Declarative, versioned mapping spec (signal → node/edge upserts, weights, decay, bi-temporal fields) + its compiler into deterministic, replayable SQL | `signals` |
| `projector` | Incremental + full-replay projection engine; reflection jobs (decay, recency, similarity/community via ClickGraph traversal); `profile_snapshots` + adjacency rollup maintenance | `memory-mapping`, `tenancy` |
| `context-service` | Read model over snapshots + adjacency rollups: assemble per-user context (≤3 point lookups), per-user cache with ttl/cacheScope semantics, staleness metadata | `projector`, `tenancy` |
| `outputs` | Channel adapters over `context-service`: MCP server (2026-07-28 stateless mode + 2025-11-25 compat), REST `/v1`, webhooks (HMAC, retries), thin SDKs | `context-service` |
| `effectiveness-loop` | Loop 1 as generic feature: recommendation outcome events → RECOMMENDED/ACTED_ON/OUTCOME edges → scoring job → weights into context; observer-LLM sits behind a port (Loop 2/3 stubs) | `signals`, `projector`, `context-service` |
| `observability` | OTel→Langfuse wiring, span sampling policy, per-tenant metering tables, evaluation harness (hit-rate@k, counterfactual replay, lift) | all (consume-side) |
| `platform-ops` | Deployment topologies (embedded dev, 3-node self-hosted, CH Cloud mode), cost guardrails (TTL/codec/tiering policy in schema), load/budget CI, backup/restore per tenant | all (deploy-side) |

**Build order:**
`signals` → `tenancy` → `memory-mapping` → `projector` → `context-service`
→ `outputs` → `effectiveness-loop`; `observability` and `platform-ops` grow
alongside in thin slices (a hook per milestone), never blocked behind others.

**Reference workload (pins the whole map):** the ITF-style skill-memory loop —
a synthetic cohort app whose agents recommend skills, whose users give
feedback, and whose cohort agents must diverge after outcome accumulation.
`effectiveness-loop`+'s acceptance test is `context-service`'s divergence
assertion; nothing is "done" until it passes.
