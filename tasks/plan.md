# Plan: graph-memory-platform implementation

**Phase-2 artifact** (spec-driven-development). Source of truth for scope:
`docs/SPEC.md` v1.0 (approved 2026-10-20) + `docs/CAPABILITY_MAP.md`.
Resolutions in SPEC §20 are binding. Task list: `tasks/todo.md`.

## 1. Major components & dependencies

Build order (from the approved map, no cycles):

```
signals → tenancy → memory-mapping → projector → context-service
        → outputs → effectiveness-loop
observability (thin slice per milestone) · platform-ops (thin slice per milestone)
```

Key insight from the plan level: `context-service` is the wedge that makes
every output channel trivial (MCP/REST/webhook/SDK are thin adapters over it),
and `memory-mapping`'s compiler purity decides whether replay convergence
(success criterion 2) is provable. Both get extra test effort.

## 2. Implementation order & checkpoints

| Milestone | Contents (map modules) | Verification checkpoint |
|---|---|---|
| M0 | repo tooling; `signals` HTTP ingest + ledger schema + ledger dedup; embedded dev profile (chdb); first contract tests; **publish public repo** | `uv run pytest -m "not live"` green in CI; duplicate `source+id` absorbs on ingest; replay-dry-run prints a clean diff |
| M1 | `memory-mapping` DSL + compiler; `projector` incremental + full replay + reflection ticks; snapshot/adjacency materializers; `context-service` + per-user cache | **Replay convergence = zero diff** (criterion 2); mapping version pinned per row; staleness surfaced |
| M2 | `outputs` MCP (2026-07-28 stateless + compat) + REST + webhook + SDK-thin; `tenancy` guard + quotas + onboarding CLI; broker adapters Kafka/Pulsar/NATS via conformance kit; SLA bench in CI | Channel parity test (criterion 5); p99 ≤1s bench (criterion 3); 7k-tenant metadata-only onboarding timing (criterion 4) |
| M3 | `effectiveness-loop` + eval harness + reference workload; cost gate; scripted observer stub behind `LlmObserver` port; demo POC repo (Pulsar + AI-generated signals) | **Divergence assertion** (criterion 1) + cost gate (criterion 6) + eval report (criterion 7) |

Checkpoints are release gates: no milestone merges forward until its checkpoint
test exists and passes in CI.

## 3. Risks & mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Mapping DSL grows into a query language (scope creep) | Massive | DSL only expresses signal→upsert/weight/decay; anything else is a feature request; compiler stays a pure function |
| Replay convergence breaks under decay (non-determinism) | Reruns diverge | Clock injected into projector; decay evaluated against event timestamps, not wall-clock; property test replays shuffled batches |
| MCP spec churn after 2026-07-28 | Rebuild risk | Spec revision pinned in CI; compat mode isolates changes to the channel adapter [04] |
| 7k-tenant quota rows mis-price CH resources | Noisy neighbors | Metering tables from day 1 [03]; quota simulation test at scale in M2 |
| chdb behavior drift vs server CH | False-green CI | Live tier (`-m live`) runs against real CH + real ClickGraph server before each milestone gate |
| Public repo + enterprise claims invite review noise | Distraction | Boundaries (SPEC §17) state "ask first" for upstream touches; issues/ADRs document decisions (docs/ADR/) |

## 4. Parallelizable

- Contract tests per broker adapter (parallel after the SPI kit exists).
- Output channels (MCP / REST / webhook / SDK) once `context-service` exists.
- Eval harness scripts vs. POC repo scaffolding (M3).

## 5. Conventions

- Commits: `module: change` (e.g. `signals: ledger dedup via ReplacingMergeTree`).
- Each task lands with its acceptance test; no task is "done" red.
- PRs link the spec section implemented (skill rule).
- ClickGraph integration only via public HTTP API; pinned version recorded in
  `ops/config` and bumped deliberately.
