# graph-memory-platform

A generic, enterprise-grade **memory platform for AI agents**: any application emits
typed signals (brokers: Kafka, Pulsar, NATS/…; or HTTP), the signals land in an
append-only ClickHouse event ledger, deterministic **memory-engineering** rules
project them into a per-user memory graph served by
[ClickGraph](https://github.com/genezhang/clickgraph) (Cypher→SQL over ClickHouse),
and agents consume **hyperpersonalized context** through pluggable outputs
(primary: MCP; plus REST API / webhooks / SDKs).

Status: **research phase** → then the spec (`docs/SPEC.md`, the single source of
truth) → then implementation.

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

## Layout (forming)

```
docs/research/   research findings, one topic per file (per-skill convention)
docs/SPEC.md     the bible spec (after research)
```
