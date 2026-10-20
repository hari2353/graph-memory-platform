# graph-memory-platform — task list

Spec: `docs/SPEC.md` (approved). Plan: `tasks/plan.md`. One task per focused
session; each lands green with its acceptance test. Pr/local only until push
(task 2 makes the repo public).

## M0 — kernel skeleton

- [x] Task: repo skeleton + research brief + capability map + bible spec
  - Acceptance: docs exist and trace decisions to research; commits clean
  - Verify: `git log --oneline` shows 3+ commits; SPEC §20 resolutions recorded
  - Files: README.md, docs/RESEARCH_BRIEF.md, docs/research/01..05, docs/CAPABILITY_MAP.md, docs/SPEC.md
- [ ] Task: tooling skeleton (uv + ruff + mypy strict + pytest, CI workflow)
  - Acceptance: `uv run ruff check . && uv run mypy --strict . && uv run pytest` all green on empty package; GitHub Actions workflow runs the same on push
  - Verify: commands pass locally; workflow file committed
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
- [ ] Task: HTTP ingest `POST /v1/signals:batch` (FastAPI) + idempotency + DLQ table
  - Acceptance: batch ≤10k events; duplicate batch replay inserts nothing new; poison events land in DLQ with reason; per-tenant quota hook exists (no enforcement depth yet)
  - Verify: `uv run pytest tests/integration/test_ingest_http.py`
  - Files: src/graph_memory/signals/api.py, src/graph_memory/signals/service.py, tests/integration/test_ingest_http.py
- [ ] Task: replay dry-run CLI + first contract test for broker SPI (fixture envelopes)
  - Acceptance: `platform replay --dry-run` prints a read-only plan; SPI kit fixture suite runs for the HTTP adapter
  - Verify: `uv run platform replay --dry-run`; `uv run pytest tests/unit/test_spi_kit.py`
  - Files: src/graph_memory/ops/cli.py, tests/unit/test_spi_kit.py
- [ ] Task: publish public GitHub repo + push M0
  - Acceptance: origin = public `hari2353/graph-memory-platform`; CI green on GitHub
  - Verify: `gh repo view --json visibility` + first Actions run green

## M1 — memory engine

- [ ] Task: `memory-mapping` spec schema v1 (YAML → pydantic) + validator
  - Acceptance: only signal→upsert/weight/decay/ttl constructs; version immutable per artifact; additive-only rules encoded in validator
  - Verify: unit tests on good/bad specs
- [ ] Task: mapping compiler to deterministic SQL (pure)
  - Acceptance: compile(spec, sample events) → SQL that is byte-stable; no wall-clock; mypy strict; ≥90% coverage on compiler
- [ ] Task: `projector` incremental projection via ClickGraph HTTP write path
  - Acceptance: batch of signals → expected graph rows via parameterized Cypher; tenant predicate injected on every statement
- [ ] Task: full replay + convergence test
  - Acceptance: replay from empty == live projection (zero diff) incl. shuffled-batch property test
- [ ] Task: reflection ticks (decay, similarity edges) with injected Clock
  - Acceptance: decay identical for same (events, spec, clock) regardless of wall-clock
- [ ] Task: snapshot/adjacency materializers + `context-service` read model + cache
  - Acceptance: ≤3 point lookups per get(); jittered TTL + single-flight; staleness metadata present
- [ ] Task: live-tier gate for M1 (real CH + real ClickGraph server)
  - Acceptance: `-m live` mirrors the convergence + projection tests

## M2 — surfaces

- [ ] Task: MCP server stateless (2026-07-28 JSON mode) with ≤4 tools + compat mode
- [ ] Task: REST `/v1` context endpoints (cursor pagination, ETag, Idempotency-Key)
- [ ] Task: webhook channel (HMAC sign, retry ladder, endpoint disable)
- [ ] Task: SDK-thin python client + channel parity e2e
- [ ] Task: `tenancy` registry + quotas + two-phase offboarding + 7k onboarding bench
- [ ] Task: tenant guard injected predicate hardening (mismatch = fail closed)
- [ ] Task: broker adapters Kafka/Pulsar/NATS via conformance kit
- [ ] Task: SLA bench in CI (synthetic profile, p99 ≤1s)

## M3 — loop & proof

- [ ] Task: outcome ledger + effectiveness scorer + weight writer
- [ ] Task: eval harness (hit-rate@k, counterfactual replay, lift report)
- [ ] Task: reference workload (synthetic ITF-style cohorts) + divergence e2e
- [ ] Task: cost gate CI + per-tenant metering dashboards data
- [ ] Task: scripted `LlmObserver` stub behind port (Loop 2 stub)
- [ ] Task: demo POC repo (Pulsar + AI-generated signals) against deployed platform

## Backlog (post-M3, recorded only)

- Additional broker drop-ins via SPI kit: SNS/SQS FIFO, Azure Service Bus,
  Google Pub/Sub, Kinesis, Redis Streams.
- Loop 3 strategy-variant A/B framework with pre-registered experiment plans.
- TS SDK; optional embedding/semantic-search adapter.
