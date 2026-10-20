# Prior art — agent-memory platforms

Research doc for graph-memory-platform, 2026-10-20. First-party sources only (official docs, repos, blogs), accessed 2026-10-20. Companion to `docs/RESEARCH_BRIEF.md`.

## TL;DR

- Same architecture everywhere: raw events (episodes/messages) in, facts/edges derived, provenance kept to source ([Zep concepts](https://help.getzep.com/concepts), [cognee architecture](https://docs.cognee.ai/core-concepts/architecture)) — mirrors our ledger→projection design.
- Only Zep/Graphiti has true bi-temporal facts: `valid_at`/`invalid_at` (+`created_at`/`expired_at`) per edge; contradictions invalidate, never delete ([Zep facts](https://help.getzep.com/facts)).
- Every platform runs LLM-on-write (extraction, dedupe, invalidation, consolidation); none projects deterministically from declarative rules — our open lane.
- Retrieval is hybrid: Zep semantic+BM25+traversal; Mem0 Platform adds entity+temporal scoring ([Zep performance](https://help.getzep.com/performance.md), [Mem0](https://docs.mem0.ai/core-concepts/how-it-works)).
- Tenancy is string-ID scoping (Zep user-graphs + ABAC; Mem0 `user_id`/`agent_id`/`run_id`; Graphiti `group_id`; LangGraph namespaces), enforced mainly in managed tiers ([Zep governance](https://help.getzep.com/governance.md)).
- Pricing meters writes, not reads: Zep 1 credit/Episode ≤350 bytes, reads free ([Zep pricing](https://www.getzep.com/pricing)); Mem0 add vs search ([Mem0 pricing](https://mem0.ai/pricing)).
- Read latency is engineered: Zep sub-200ms + `graph.warm` ([README](https://github.com/getzep/graphiti)); Mem0 p50 0.88s managed ([README](https://github.com/mem0ai/mem0)). Our <2s SLA needs that precomputation.
- MCP is the standardizing agent surface: Zep Context-MCP seats, Mem0 MCP, cognee/Graphiti MCP servers ([Zep pricing](https://www.getzep.com/pricing), [cognee](https://github.com/topoteretes/cognee)).
- No typed declarative edges (co-occurrence only — [Mem0](https://docs.mem0.ai/platform/features/graph-memory)); no rebuildable projections; no outcome-feedback loop anywhere.
- Category drift is real: Letta archived its V1 OSS API server ([README](https://github.com/letta-ai/letta)).

## Comparison

| Platform | Data model | Update model | Retrieval | Tenancy | API | Pricing | Key weakness |
|---|---|---|---|---|---|---|---|
| **Zep** (managed) | Context graph: entities, fact-edges, episodes; 4 timestamps/fact | LLM extraction + contradiction invalidation | Hybrid semantic+BM25+traversal; context block | user graphs + shared graphs; RBAC/ABAC; SOC 2 II, HIPAA | Python/TS/Go SDKs; `graph.*`/`thread.*`; MCP seats | Free 10k credits; Flex $125 (50k, then $25/10k, 600 RPM); Plus $375 (200k, $75/40k, 1k RPM); reads unmetered | Closed engine; writes priced per byte |
| **Graphiti** (OSS) | Same; episodes→facts via structured-output LLM; Pydantic types | LLM extract/dedupe; decision-models (Jev) optional | Hybrid + rerankers | `group_id` partition; reference servers no authz | Python lib; reference MCP/REST (non-prod) | Free Apache-2.0; pays graph DB + LLM | Separate graph DB (Neo4j/FalkorDB/Neptune+OpenSearch); LLM mandatory; 429-prone (`SEMAPHORE_LIMIT` 10) |
| **Mem0** | SQL facts; vector DB; entity store; graph = co-occurrence | LLM ADD-only; explicit update/delete; temporal scoring at read | Platform: semantic+BM25+entity+temporal fused; OSS: not | `user_id`/`agent_id`/`run_id`/`app_id`; workspaces | REST/SDKs; async `add` (+`event_id`); MCP | Free 10k add/1k search; Starter $19 (50k/5k); Pro $249 (500k/50k); Enterprise on-prem/SSO/audit | No invalidation; graph ranking-only; fusion paywalled |
| **Letta/MemGPT** | Agent state: memory blocks + MemFS (git) + messages | Agent edits own memory; dreaming = background subagents | In-context blocks; message search; no index | Org/teams; per-agent conversations | TS SDK, CLI, web; cloud or self-host | Free (3 agents, 5 MiB repo); Pro $20 (20 agents, 50 MiB); devs usage-based | File/prompt-shaped memory; V1 server archived |
| **LangMem/LangGraph** | Semantic/episodic/procedural → collections+profiles in namespaces | LLM primitives + `BaseStore` upserts | `get` by key; `search` semantic+metadata | Namespace templates `{user_id}`; Postgres store | Memory tools for LangGraph agents; hot-path/background | OSS free; managed = LangGraph Platform/LangSmith | Namespace-KV, no graph/entity/temporal model |
| **cognee** | Relational (provenance) + vector + graph; ECL pipelines | LLM pipelines; keyless GLiNER + local embeddings | `recall`: vector/graph/hybrid; keyless ok | API multi-tenant default (auth on `/api/v1`); Cloud managed | Python/TS/Rust, CLI, REST, MCP | OSS free; Cloud usage-based; 1-Postgres prod licensed | Multi-store sprawl; prod tier paid |
| **ChatGPT memory** (ref.) | Saved memories + chat-history refs + summary | LLM auto-save; user-editable | Inference-time, with sources | Per user/workspace; admin toggle | In-product | In plan tiers | Assistant feature, not an API |
| **Claude memory** (ref.) | Per-user + per-project summaries; memory-tool files | Agent-summarized, user-editable | Summary injection; client-side files | Admin controls; incognito | `memory_20250818` tool via tool-use | In plan tiers | No structure beyond summaries/files |

## Zep + Graphiti

**Data model.** Facts live on edges with four timestamps — `created_at`, `valid_at` (true in the world), `invalid_at` (superseded), `expired_at` (when Zep learned); "old facts are invalidated — not deleted" ([facts](https://help.getzep.com/facts)). Episodes are the raw provenance every fact traces to ([concepts](https://help.getzep.com/concepts)).
**Update model.** On add, Zep decides insert-vs-invalidate via LLM contradiction detection ([facts](https://help.getzep.com/facts)); Graphiti mirrors this with structured-output LLM prompts, optionally routing dedupe/edge-resolution to decision models (Jev, OpenAI Decisions) ([README](https://github.com/getzep/graphiti)).
**Retrieval.** "Hybrid retrieval combining semantic (vector) similarity, BM25 full-text search, and graph traversal in a single ranked result"; plus Context Block assembly, `return_context=True`, `graph.warm` ([performance](https://help.getzep.com/performance.md)).
**Tenancy.** Per-user graphs keyed by UUID; shared graphs via `graph_uuid`; RBAC for humans, ABAC on API keys/UserGroups; audit + API logs; SOC 2 Type II, HIPAA BAA, BYOK/BYOC ([concepts](https://help.getzep.com/concepts), [governance](https://help.getzep.com/governance.md)).
**API/Pricing.** Python/TS/Go SDKs; MCP seats. Free 10,000 credits/mo; **Flex $125/mo: 50,000 credits then $25/10k, 600 RPM; Flex Plus $375/mo: 200,000 then $75/40k, 1k RPM; 1 credit per Episode ≤350 bytes (+1 per extra 350); retrieval, storage, users unmetered**; Enterprise: BYOC, SLA, 1-yr logs ([pricing](https://www.getzep.com/pricing)).
**Numbers.** Zep paper: DMR 94.8% vs MemGPT 93.4%; LongMemEval up to +18.5% accuracy, −90% latency ([arXiv:2501.13956](https://arxiv.org/abs/2501.13956)); site claims sub-200ms retrieval ([README](https://github.com/getzep/graphiti)).
**Weakness.** OSS is a framework: run Neo4j/FalkorDB/Neptune+OpenSearch, structured-output LLM every episode, manage rate limits (`SEMAPHORE_LIMIT` 10) ([README](https://github.com/getzep/graphiti)). Reference servers: "Any caller can read, change, or delete data in any `group_id`" — buy-Zep or DIY isolation.

## Mem0

**Data model.** SQL (facts+metadata, source of truth), vector DB, entity store (→ Platform Graph Memory) ([how it works](https://docs.mem0.ai/core-concepts/how-it-works)); graph links memories via shared entities — "connections are inferred from co-occurrence rather than declared" ([graph memory](https://docs.mem0.ai/platform/features/graph-memory)).
**Update model.** April 2026 algorithm: **single-pass ADD-only extraction — one LLM call, no UPDATE/DELETE**; memories accumulate; corrections need explicit `update`/`delete`; `infer=False` risks duplicates ([README](https://github.com/mem0ai/mem0), [add ops](https://docs.mem0.ai/core-concepts/memory-operations/add)).
**Retrieval.** Platform fuses semantic + BM25 + entity boost + temporal into one `score`; OSS has vector + entity-overlap only ([how it works](https://docs.mem0.ai/core-concepts/how-it-works)).
**Tenancy.** `user_id`/`agent_id`/`run_id`/`app_id` attach per memory; searches must filter or memories mix; audit + workspaces are Platform features ([how it works](https://docs.mem0.ai/core-concepts/how-it-works), [overview](https://docs.mem0.ai/overview)).
**API/Pricing.** REST/SDKs; async `add` → PENDING + `event_id`; Platform auto-joins prior turns by `user_id`/`run_id` ([add ops](https://docs.mem0.ai/core-concepts/memory-operations/add)). Free 10k add + 1k search/mo; Starter $19 (50k/5k); Pro $249/mo (500k/50k, unlimited projects, Graph view, Dream); Enterprise: on-prem, audit, SSO, SLA ([pricing](https://mem0.ai/pricing)).
**Numbers.** LoCoMo 92.5 (+21), LongMemEval 94.4 (+27), BEAM-1M 64.1, retrieval p50 0.88s — explicitly managed-platform-only ([README](https://github.com/mem0ai/mem0)).
**Weakness.** No invalidation (staleness rides temporal ranking); graph affects ranking only; numbers/graph view gated to the managed platform.

## Letta / MemGPT

**Data model.** All state persists: memory blocks (named editable strings), messages, tools; MemFS = git-backed memory repository — `system/` files pin into the prompt, others load on demand; shared repositories serve multiple agents; skills/persona are files ([memory](https://docs.letta.com/agent-sdk/memory)).
**Update model.** Agents edit their own memory via tools; **dreaming** (formerly sleep-time compute — [arXiv:2504.13171](https://arxiv.org/abs/2504.13171)) runs background subagents that "review recent conversations, consolidate lessons, and update memory" on step-count/compaction triggers ([dreaming](https://docs.letta.com/letta-code/memory)).
**Retrieval.** In-context blocks by construction plus message search; no vector or graph store ([dreaming](https://docs.letta.com/letta-code/memory)).
**Tenancy.** Org/teams + permissions; per-agent conversations; no user-graph scoping like Zep/Mem0 ([SDK](https://docs.letta.com/agent-sdk/memory)).
**API/Pricing.** TS Agent SDK, CLI, chat.letta.com; cloud or `letta server`. Free (BYOK, 3 agents, 5 MiB/repo); Pro $20/mo (20 agents, 50 MiB, Letta Auto quota); usage-based developer plans ([pricing](https://www.letta.com/pricing)).
**Weakness.** Memory is prompt/file-shaped, not queryable; V1 OSS API server retired into `archive` — tags remain for reproducibility, "active projects should use the current source" ([README](https://github.com/letta-ai/letta)).

## LangMem / LangGraph store

**Data model.** A taxonomy, not a graph: semantic (collections/profiles), episodic (past-experience examples), procedural (prompts/rules); generic LangGraph `BaseStore` items in namespaces with `{user_id}` templates ([guide](https://langchain-ai.github.io/langmem/concepts/conceptual_guide/), [store](https://langchain-ai.github.io/langgraph/reference/store/)).
**Update model.** Two layers: storage-free LLM primitives (`create_memory_manager`, `create_prompt_optimizer`) + LangGraph-integrated upsert/delete; all LLM-driven — "the best memory systems are often application-specific" ([guide](https://langchain-ai.github.io/langmem/concepts/conceptual_guide/)).
**Retrieval.** `BaseStore.get` by key; `search` semantic + metadata filter; hot-path vs background. Concedes recall should factor "importance" and "strength" ("how recently/frequently it was used") but implements no weighting ([guide](https://langchain-ai.github.io/langmem/concepts/conceptual_guide/)).
**Tenancy.** Namespaces + templates; Postgres (`AsyncPostgresStore`) in production; isolation = namespace discipline ([guide](https://langchain-ai.github.io/langmem/concepts/conceptual_guide/)).
**Weakness.** Library-only; no entity/temporal model; no graph.

## cognee

**Data model.** Three cooperating stores — relational (provenance), vector (embeddings), graph (entities, Cypher-queryable) — over Task/DataPoint pipelines for session and permanent memory ([architecture](https://docs.cognee.ai/core-concepts/architecture)).
**Update model.** `remember`/`recall`/`improve`/`forget`; LLM pipelines dominate, but keyless mode ingests with local GLiNER + embeddings — deterministic ingest is feasible ([README](https://github.com/topoteretes/cognee)).
**Retrieval.** `recall` routes semantic/structural/hybrid/code; keyless returns matching source text, no LLM answer ([README](https://github.com/topoteretes/cognee)).
**Tenancy/API.** API "defaults to multi-tenant mode and every `/api/v1` call requires an authenticated user" (demo disables it); permissions; managed Cognee Cloud; Python/TS/Rust, CLI, REST, MCP; COGX import from Mem0/Letta/Zep/Graphiti ([README](https://github.com/topoteretes/cognee)).
**Numbers.** BEAM: 0.79 (100K), 0.67 (10M, exploratory) with an acknowledged reproduction gap ([BEAM REPORT](https://github.com/topoteretes/cognee/blob/main/cognee/eval_framework/beam/REPORT.md)).
**Weakness.** Multi-store sprawl; "whole memory layer on one Postgres" is demo-grade, production "available as a licensed product" ([README](https://github.com/topoteretes/cognee)).

## ChatGPT & Claude memory (product-shape references)

- **ChatGPT**: saved memories + "Reference chat history" + editable memory summary; per-source deletion; incognito/temporary chats; enterprise "Regulated Workspace" defaults improved memory off, excluded from the healthcare BAA ([Memory FAQ](https://help.openai.com/en/articles/8590148-memory-faq)).
- **Claude**: per-user summary + **separate memory per project** (Team/Enterprise Sep 11 2025; Pro/Max Oct 23 2025), incognito, admin kill switch ([announcement](https://anthropic.com/news/memory)); API memory tool is client-side files (`memory_20250818`) ([memory tool](https://docs.anthropic.com/en/docs/agents-and-tools/tool-use/memory-tool)); +39% internal agentic-search eval with memory tool + context editing ([context management](https://www.anthropic.com/news/context-management)).
- Implication: view/edit/delete/incognito/admin controls are the product floor.

## Positioning for graph-memory-platform

1. **Adopt Zep's four-timestamp bi-temporal schema** (`valid_at`, `invalid_at`, `created_at`, `expired_at`) on every edge, `DateTime64` columns; invalidation is a new row version, never UPDATE/DELETE ([facts](https://help.getzep.com/facts)) — ClickHouse-shaped.
2. **Episodes = our event ledger.** "Every derived fact traces back to episodes" ([concepts](https://help.getzep.com/concepts)); our advance: typed, broker-neutral signal envelopes, so projections replay from the ledger.
3. **Reject LLM-on-every-write.** Zep meters write bytes ([pricing](https://www.getzep.com/pricing)); Mem0 needs an LLM call per add ([README](https://github.com/mem0ai/mem0)) — untenable at 7,000 tenants. Deterministic declarative mapping default; optional LLM refinement behind a provider port (à la Graphiti's Jev routing).
4. **Typed edges are the wedge.** Mem0 declines typed relationships by design ([graph memory](https://docs.mem0.ai/platform/features/graph-memory)); Graphiti needs LLM extraction ([README](https://github.com/getzep/graphiti)). Declarative rules give typed edges deterministically — what multi-hop Cypher needs.
5. **Hybrid retrieval in one store.** Match Zep's semantic+BM25+traversal single ranked result ([performance](https://help.getzep.com/performance.md)) and Mem0's temporal scoring natively in ClickHouse/ClickGraph — no Neo4j/OpenSearch tax.
6. **Tenancy in the row.** Mem0-style always-on scoping IDs ([add ops](https://docs.mem0.ai/core-concepts/memory-operations/add)) + Zep-style ABAC on keys ([governance](https://help.getzep.com/governance.md)): mandatory `tenant_id`+`user_id` columns and query predicates; Graphiti's authless servers are the cautionary tale.
7. **Precompute the context block.** Sub-200ms + `graph.warm` + `return_context=True` (Zep) show the SLA is won on reads: materialize per-user context views during reflection, warm on login, no request-time multi-hop ([performance](https://help.getzep.com/performance.md)).
8. **Price writes, keep reads free.** Both incumbents meter writes, free reads ([Zep](https://www.getzep.com/pricing), [Mem0](https://mem0.ai/pricing)); deterministic writes on ClickHouse undercut structurally.
9. **Ship the closed loop nobody has.** LangMem names importance/strength as missing recall signals but implements none ([guide](https://langchain-ai.github.io/langmem/concepts/conceptual_guide/)); recommendations→outcomes→effectiveness weights is unclaimed.
10. **Governance is table stakes, not upsell.** Zep gates audit logs by tier ([pricing](https://www.getzep.com/pricing)); ChatGPT/Claude set view/edit/delete/incognito expectations ([FAQ](https://help.openai.com/en/articles/8590148-memory-faq), [Claude](https://anthropic.com/news/memory)). OTel/Langfuse spans on every projection + MCP call, plus delete/export APIs, day one.
