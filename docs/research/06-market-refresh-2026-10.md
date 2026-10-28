# Market refresh — agent-memory platforms (updates 01)

Research doc for graph-memory-platform, 2026-10-27.
Updates `docs/research/01-prior-art-agent-memory.md` (locked 2026-10-20) with what
the market moved since that first scan. Interested-parties format identical: first-party
sources (vendor docs, repos, papers) accessed 2026-10-27; anything known only through
aggregator write-ups is marked **secondary**. Companion to `docs/RESEARCH_BRIEF.md`.

## TL;DR

- **No incumbent moved into our open lanes.** Deterministic declarative projection,
  broker-native ingest with DLQ/adjudication, and the outcome→effectiveness loop
  (SPEC §10) still have no competitor implementation as of today.
- **What moved: (1) benchmark standardization + public disputes** — LoCoMo /
  LongMemEval / DMR / BEAM are the de facto surface, with known vendor-dispute
  methodology gaps; new benchmarks now target **task outcomes** (DolphinBench),
  **over-personalization** (OP-Bench), and **memory as learned workflow** (MemEval,
  PropMem, agent-memory-bench) — read path was instrumented; the write/learn path emerges.
- **(2) Governance became the product floor** — ChatGPT/Claude set
  view/edit/delete/incognito expectations; Zep audit logs are tier-gated; supermemory
  makes contradiction resolution + selective forgetting a headline feature.
  **User-facing memory controls are day-1 requirements for our 7k-tenant charter — we
  have none (verified: no forget/delete/erase surface in `src/graph_memory`).**
- **(3) The latency/token battle moved to precompute:** Zep sub-200ms + `graph.warm` +
  context-block assembly; MemOS ships KV-cache/activation-memory preloads
  ("Next-Scene Prediction"), MemScheduler async millisecond ops; Mem0 p50 0.88–1.09s
  at 6.7–7.0K retrieval tokens. SLA is won at projection time, not request time —
  exactly our 1s projection thread's premise.
- **(4) Reach through agent hosts is the adopted play:** supermemory ships official
  plugins for Claude Code **and OpenCode**; memU installs across Claude Code / Codex /
  OpenClaw hosts with one shared store. Our MCP-primary channel can join this race
  cheaply (plugin recipes; the TS SDK stays in the backlog).
- **(5) OS-style abstractions and file-shaped memory are competing framings**
  (MemOS MemCube; memU's folder/file model) — different states of the same problem;
  neither offers deterministic replay or typed declarative mapping.

## New entrants since 01

| Player | Data model | Update model | Retrieval | Tenancy | Notable numbers / claims |
|---|---|---|---|---|---|
| **memU** (NevaMind-AI/memU; OSS, custom license) | resource layer → memory items → category **markdown files** ("folders/files/symlinks") | agent-session capture via scheduled background tasks; self-evolving store→retrieve→evolve | LLM reads category files first, item-level RAG fallback (dual mode; embeddings optional) | personal/agent-centric; shared local store (SQLite/Postgres) or memU cloud | cross-host install (Claude Code, Codex, OpenClaw) + auto skill extraction from agent history |
| **MemOS** (MemTensor/MemOS, MIT) | **MemCube** unifies plaintext / activation (KV-cache) / parametric memory; 3-layer API/scheduler/storage | MemScheduler async ops (ms-level claims); tiered skill evolution (L1 traces / L2 policies / L3 world model); smart dedup | hybrid FTS+vector (Stardust local tier: SQLite); multi-cube KB isolation/sharing | system-level governance (access control, traceability) | LoCoMo +159% temporal vs OpenAI memory, +38.97% overall, **−60.95% tokens**; 94% latency cut via KV-cache preloads; "Next-Scene Prediction" preload |
| **Supermemory** (managed API) | facts + profiles + contradictions | LLM extraction w/ contradiction resolution + **selective forgetting** | claimed LongMemEval/LoCoMo/ConvoMem leadership (**secondary**; unverified) | memory APIs; MCP server | official **Claude Code & OpenCode plugins** — the reach play |
| **OpenMemory** (mem0ai) | local-first MCP memory server | dashboard-managed | RAG-ish | per-user local | memory follows the user across Claude Desktop/Cursor/Windsurf/VS Code |
| **Neo4j Labs agent-memory** (`neo4j-agent-memory`) | 3 memory types: short-term, long-term entities/facts (SPO triples w/ confidence), reasoning traces | `valid_from/valid_until` windows; auto-supersede on update; LLM contradiction detection | MCP tools incl. `memory_export_graph`, `get_context` | SDK-scoped sessions | point-in-time queries over validity windows — independent confirmation of our bi-temporal choice |
| **FalkorDB / Letta drifts** (**secondary**, aggregator-verified) | Graphiti's Kuzu backend deprecated → Neo4j/FalkorDB/Neptune; Letta active source moved to letta-ai/letta-code | — | — | — | backend fragmentation risk is real; our single-store (ClickHouse/ClickGraph) posture answers it |

## The benchmark race (read-path only in 01; now standards)

| Benchmark | Measures | Known results (Oct 2026) |
|---|---|---|
| LoCoMo | multi-session conversational QA (single/multi-hop, temporal, open) | Mem0 April-algo **92.5** (was 71.4); MemOS +38.97% claim; Zep 80.3 @ best config |
| LongMemEval | long-horizon conversational recall | Mem0 **94.4**; Zep 71.2 (GPT-4o) — disputed methodology both ways |
| DMR (Deep Memory, Zep) | short-horizon memory | Zep **94.8** (Mem0 not published) |
| BEAM (cognee) | very long conversations (up to 10M tok) | cognee 0.79 (100K) / 0.67 (10M) w/ acknowledged reproduction gap; Mem0 64.1 (1M) |
| **DolphinBench** (new, mem0 blog) | long histories **+ real tool calls + task outcomes**, 600 tasks | new; measures execution, not recall |
| **OP-Bench** (arXiv 2601.13722) | **over-personalization** for memory-augmented agents | retrieval helps **until it over-personalizes**; Mem0/MemU/MemOS as baselines |
| MemEval / PropMem / agent-memory-bench (new) | memory **learning from the agent's own work across sessions**; execution-graded, preregistered | field explicitly says the suite "does not yet measure whether a memory layer learns" |

Position deltas vs 01's ten points:

11. **Governed forgetting is table stakes, not a feature** (new floor): tombstone via
    the ledger, point-in-time valid windows, an audit trail per action, and an export
    API are the §11-adjacent gap to close first (§ below).
12. **Agent-host reach is a distribution channel**: MCP tools we already ship +
    OpenCode/Claude Code plugin recipes (supermemory's playbook) get competitors'
    adoption pattern without new kernel work.

## Cost / latency / scale — what the market proved, and our levers

The user charter (GitHub README): <2s end-to-end hot path, cheap to run, 7,000 tenants,
$790/mo self-host posture. Our CI gates already enforce more than the market's engines
(except Zep's retrieval latency claim): hot-path **p99 ≤ 1s** (in-process profile ≈ 1ms;
the live tier is the risk), §12 cost recompute **0.977–1.07 TB/mo** within ±15%.

Market reference points (first-party where noted):

- **Zep**: sub-200ms retrieval claim + `graph.warm` + `return_context` context-block
  assembly — read path fully precomputed; writes metered (1 credit/episode ≤350B):
  at ~1KB our signals they'd charge 3 credits ≈ $2.50/1k writes ≫ our ~$0 deterministic
  writes (storage only).
- **MemOS**: the most market-relevant cheap-token lever — activation/KV-cache memory +
  Next-Scene Prediction caching-as-resource; −60.95% tokens claimed.
- **Mem0**: 0.88–1.09s p50 retrieval through managed tier; 6.7–7.0K tokens per retrieved
  context is their cost of LLM-side ranking; our bounded `top_k` payloads are smaller.

Levers to implement, each falsifiable and slotted behind existing gates:

1. **CH-materialized read model** (the parked `profile_snapshots`/`user_adjacency_rollup`
   backlog item): precompute per-(user, capability) context payloads on the 1s projection
   tick (protected staged-rebuild semantics already shipped for CH tables); single-flight
   cache becomes a safety net only. This is the biggest SLA lever for the live tier —
   read path becomes one prebuilt-row fetch.
2. **Hybrid retrieval without an embedding service on the hot path**: ClickHouse full-text
   (BM25-able) + typed/traversal ordering + **loop weights as a ranking feature** (no
   competitor has loop-evidence ranking); vector similarity parked behind an
   `EmbeddingProvider` port (local ONNX keeps "reads cheap"); retrieval is the
   ad-hoc search surface, never the default context assembly.
3. **Warm-on-login** (Zep graph.warm analog): `POST /v1/prefetch` (`Idempotency-Key`)
   warms the cache at session open; metering counters prove hit-rate lift.
4. **Async inserts + schema-only cost levers** (TTL/codecs already in `signals/ddl.py`);
   dev profile adds `async_insert=1`; the cost gate recomputes unchanged.
5. **Token-budget gate** (proposal for a new falsifiable criterion): context payload
   tokens per retrieval view, Mem0's 6.7–7.0K is the industry's uncontrolled figure); enforced in `platform bench hotpath`/evals.
6. **Shard-by-tenant ops doc**: brokers already order by `gmp.signals.<tenant>` /
   user partition key; CH cluster keys by tenant hash — ops posture doc + compose
   profile for the 3-node cluster, no code change.

## Implementation order (feeds `tasks/todo.md` §M4)

1. **M4.1 Governed forgetting + user memory controls (P0)** — design sketch:
   - a forget is a **ledger event** like every signal: mapping spec declares any
     `<ns>.forget.vN` kind with `forget:` directives (target kind + optional
     `correlation_id` / target-key scoping);
   - projector semantics = bi-temporal **supersession**: set `invalid_at` on the rows
     the forget's scoped source signals created (pure function: events+spec+clock;
     convergent by identity, replay-safe — an incremental pass and a full rebuild land
     on the same state);
   - surfaces: MCP `delete_memory` + REST `POST /v1/forget` behind a new coarse scope
     `memory:write`; every call emits a span (audit) and lands in metering;
   - acceptance: replay-convergence + divergence tests hold **with** forget flows
     re-included; `explain_context` shows tombstone provenance; channel parity on the
     forget surface; tenant isolation unchanged (tenant from bearer, never argument).
    - **Landed 2026-10-27** (see tasks/todo.md M4.1): two design deltas from this
      sketch — (a) REST is `POST /v1/forget/{user}` (user in the path, consistent with
      the context endpoints), not the sketch's user-in-body `POST /v1/forget`; (b)
      supersession is an **overlay directive** (identity-keyed forget rows + a shared
      read-time visibility predicate, equal instants die) rather than mutating
      `invalid_at` on edge rows — same convergence guarantees, no version-column
      surgery, and directives skip rebuild staging as replay-idempotent facts.
      `correlation_id`-scoped forgetting is deferred (envelope v1 carries no
      correlation_id).
2. **M4.2 Hybrid retrieval in one store (P1)** — per lever 2 above; acceptance via
   existing channel-parity + new ranked-search fixtures; hot-path gate must stay ≤1s.
   - **Landed 2026-10-28** (see tasks/todo.md M4.2): deltas from the sketch — (a) the
     lexical half is ONE deterministic metric in `core/lexicon.py` (bounded token
     tuple + case-insensitive substring hit count), identical across backends;
     ClickHouse pushes the matching into `positionCaseInsensitive` SQL and nothing
     else (BM25-style scoring parked — tokens/metric live in core, so a BM25
     upgrade cannot fork the kernel). (b) The loop-weight ranking feature rides the
     already-compiled stored weights (effectiveness overrides fold in at compile),
     so search reads zero extra state. (c) Vector similarity is parked exactly as
     proposed: `EmbeddingProvider` port defined, unwired, no ONNX dep. (d) The
     search surface REPLACES the old snapshot post-filter tool body when wired —
     MCP `search_memory` (same name/args, falls back to the old post-filter when
     unwired) and REST `GET /v1/search/{user}` (scope `memory:search`, span +
     metering). Criterion added as SPEC §16.8.
3. **M4.3 External benchmark harness (P1)** — replay a LongMemEval-style corpus
   through our own ingest as message signals; **OP-Bench-style over-personalization
   probe** reusing the divergence machinery (loop OFF must equal baseline; loop ON must
   beat baseline AND not degrade cold/fresh cohorts); publish the matrix.
   - **Landed 2026-10-28** (see tasks/todo.md M4.3): the probe IS shipped as
     `graph_memory.evals.over_personalization_probe` — a deterministic JSON matrix over
     BOTH regimes (no-loop baseline vs ticked loop rows), with warm/cold cohorts as the
     falsifiable split: warm diverges and lifts (0.5→1.0 warm hit-rate, `warm_tops_
     mutually_distinct`), cold cohorts keep byte-identical tops (`cold_degraded_
     cohorts` empty ⇒ verdict `no-over-personalization`), and an `override_rows`
     negative-control seam lets CI inject a leaky row set to prove the instrument
     fails. The LongMemEval-style external corpus reload remains the recorded P2
     follow-up (deliberate — ingest-shaped corpus replay is gated behind M4.5/M4.6
     surface changes).
4. **M4.4 Interop reach (P2)** — TS SDK parity + OpenCode/Claude Code MCP plugin
   recipes; documented against our own MCP surface.
5. **M4.5 Export/portability API (P2)** — `GET /v1/export/{user}` over the read model
   (+ ledger-scoped history) for GDPR portability; span + metering trace.
   - **Landed 2026-10-28** (see tasks/todo.md M4.5): portability is shipped as a
     first-class READ — new `MemoryExport` port + `ExportService` adapter (engine
     snapshot per declared capability = the union of point reads, never a shadow
     format, plus the user's OWN ledger history bounded oldest-first with honest
     `history_truncated` via a one-over read), REST `GET /v1/export/{user}` behind
     `context:read` with span `gmp.channel.rest.export` + metering `"export"`,
     unwired → anticipated 500 `not_wired`. Confirms research line 63: the market
     vendors treat export as an audit/data-subject surface, and parity with the
     served point reads is the test of "no shadow export format".
6. **M4.6 Precompute + prewarm path (P2)** — lever 1 + 3; gates: hot-path p99, cost,
   plus new token-budget check.

Anything LLM-on-write and file-shaped (Letta self-edit, memU files) stays out of scope —
it violates the determinism/cost/tenancy hard requirements (01's conclusion stands).

## Sources (accessed 2026-10-27 unless noted)

- mem0ai/blog: "State of AI Agent Memory 2026" (DolphinBench; OpenMemory scope)
- arXiv 2601.13722 — OP-Bench: Over-Personalization for Memory-Augmented Agents
- MemTensor/MemOS README + arXiv 2507.03724 (MemCube, Next-Scene Prediction claims)
- NevaMind-AI/memU README + memu.pro blog (three-layer file memory; host adapters)
- neo4j.com/labs/agent-memory (validity windows; contradiction detection; MCP tools)
- supermemory claims via atlan.com survey (**secondary**)
- Letta/FalkorDB drifts via developersdigest.tech + digitalapplied.com (**secondary**)
- mem0 vs zep dispute posts: mem0.ai/library/agent-memory/zep-vs-mem0-...;
  "Lies, Damn Lies, and Statistics: Is Mem0 Really SOTA in Agent Memory?" (Zep, 2025)
