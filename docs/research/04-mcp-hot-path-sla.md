# 04 — MCP Surface & Hot-Path Runtime Design (<2s Context SLA)

*Research note for graph-memory-platform · 2026-10-20. Scope: MCP surface + read/serve runtime only; reflection/scoring may be slow or offline.*

## TL;DR

- Five spec revisions: 2024-11-05 (HTTP+SSE), 2025-03-26 (Streamable HTTP, OAuth 2.1), 2025-06-18 (structured output, RFC 8707), 2025-11-25 (URL elicitation, tasks, OIDC discovery), and 2026-07-28, which **made MCP stateless** — no `Mcp-Session-Id`, no initialize handshake, no GET stream ([changelog](https://modelcontextprotocol.io/specification/2026-07-28/changelog)).
- Stateless Streamable HTTP is first-class in both official SDKs: TS (`sessionIdGenerator: undefined`, transport-per-request) and Python (`StreamableHTTPSessionManager(stateless=True)`) ([TS](https://ts.sdk.modelcontextprotocol.io/server.html), [Python](https://github.com/modelcontextprotocol/python-sdk/blob/main/src/mcp/server/streamable_http_manager.py)).
- MCP servers are OAuth 2.1 resource servers that MUST validate bearer token + audience (RFC 8707) on **every request** ([authorization spec](https://modelcontextprotocol.io/specification/2026-07-28/basic/authorization)) — a natural frame for stateless per-request tenant resolution.
- Tools are model-controlled, resources app-driven, prompts user-controlled; hot-path tools should return compact `structuredContent` plus `resource_link` for bulk ([tools](https://modelcontextprotocol.io/specification/2025-11-25/server/tools), [resources](https://modelcontextprotocol.io/specification/2025-06-18/server/resources)).
- Client result budgets are enforced: Claude Code warns at 10k and defaults to 25k max MCP output tokens ([docs issue](https://github.com/anthropics/claude-code/issues/42869)); tool definitions alone were measured at 50–134k tokens ([Anthropic](https://www.anthropic.com/engineering/advanced-tool-use)).
- Hot path = **≤3 ClickHouse point queries + assembly** on precomputed snapshot tables; open multi-hop traversal is reflection/analytical work — recursive CTEs "time out at 3 hops" and self-join plans "explode" ([ClickHouse graph blog](https://clickhouse.com/blog/zero-copy-graph-analytics)).
- Per-user cache: cache-aside keyed `tenant|user|capability`, jittered TTL, single-flight locks or probabilistic early expiration (XFetch) versus stampedes ([VLDB'15](https://dl.acm.org/doi/10.14778/2757807.2757813)); ClickHouse's own query cache (per-process, AST-keyed, 60s TTL, not user-shared) is a supplement, not the design ([docs](https://clickhouse.com/docs/operations/query-cache)).
- A workable p99: ingress 50ms + authn/authz 80ms + 3 parallel CH point-lookups ~250ms + assembly 80ms + transport 150ms ≈ 600ms per tool call — 2× headroom.

## 1. MCP spec state: five revisions, one direction (less state)

- **2024-11-05** — first spec; per-connection HTTP+SSE. Deprecated since 2025-03-26 ([transports](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http)).
- **2025-03-26** — introduced **Streamable HTTP** (PR #206) and the OAuth 2.1 authorization framework (PR #133), plus tool annotations (PR #185) ([changelog](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/main/docs/specification/2025-03-26/changelog.mdx)).
- **2025-06-18** — removed JSON-RPC batching (PR #416); added structured tool output (`structuredContent`/`outputSchema`); servers classified as **OAuth Resource Servers** (PR #371); clients MUST implement RFC 8707 resource indicators (PR #734); added elicitation (PR #382), resource links (PR #603), required `MCP-Protocol-Version` header (PR #548) ([changelog](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/main/docs/specification/2025-06-18/changelog.mdx)).
- **2025-11-25** — OIDC discovery, icons (SEP-973), step-up `WWW-Authenticate` scopes (SEP-835), tool-name guidance (SEP-986), elicitation enum overhaul (SEP-1330) + URL mode (SEP-1036), sampling tool-calling (SEP-1577), Client ID Metadata Documents (SEP-991), experimental **tasks** (SEP-1686), SSE polling (SEP-1699), RFC 9728 alignment (SEP-985), JSON Schema 2020-12 (SEP-1613), 403 for invalid `Origin` (PR #1439), validation failures as tool-execution errors (SEP-1303) ([changelog](https://modelcontextprotocol.io/specification/2025-11-25/changelog)).
- **2026-07-28** — stateless: removes `Mcp-Session-Id` sessions (SEP-2567) and the `initialize` handshake (version/capabilities per-request in `_meta`); removes GET stream and SSE resumability (broken stream → client re-issues, SEP-2575); MRTR (SEP-2322); tasks → `io.modelcontextprotocol/tasks` extension (SEP-2663); `CacheableResult` (`ttlMs`/`cacheScope`) on list/read results (SEP-2106); deterministic `tools/list` ordering; DCR deprecated for Client ID Metadata Documents ([changelog](https://modelcontextprotocol.io/specification/2026-07-28/changelog)).

Trajectory: the spec keeps removing per-connection state so any pod can answer any POST.

## 2. Transport & sessions in practice

Streamable HTTP (2026-07-28): one endpoint, POST per JSON-RPC message; each request answered with a single JSON object or a request-scoped SSE stream ([transports](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http)). For the hot path use **JSON-response mode** (TS `enableJsonResponse`, Python `json_response=True`).

Both official SDKs ship stateless modes: TypeScript `new StreamableHTTPServerTransport({ sessionIdGenerator: undefined })` — no session validation; a stateless transport serves one request, so server+transport are per-request — and Python `StreamableHTTPSessionManager(..., stateless=True)` ([TS docs](https://ts.sdk.modelcontextprotocol.io/server.html), [source](https://github.com/modelcontextprotocol/python-sdk/blob/main/src/mcp/server/streamable_http_manager.py)). Caveats: per-request `McpServer` construction is measurable ([typescript-sdk#2090](https://github.com/modelcontextprotocol/typescript-sdk/issues/2090)) — keep the per-request object graph tiny; stateless mode is also the multi-node "any node can handle any request" pattern ([docs](https://ts.sdk.modelcontextprotocol.io/server.html)). Cloudflare's hosted endpoints already "support the new MCP 2026-07-28 Specification and stateless requests from 2025 Streamable HTTP clients" ([docs](https://developers.cloudflare.com/agents/model-context-protocol/cloudflare/servers-for-cloudflare)).

## 3. Tools vs resources vs prompts; result shapes, pagination, elicitation

- **Tools** are model-controlled ([tools spec](https://modelcontextprotocol.io/specification/2025-11-25/server/tools)); the same spec requires server-side input validation, access controls, and rate limiting. Ours: read-only `readOnlyHint` tools, server-side rate limits.
- **Resources** are application-driven, URI-identified, with `audience`/`priority`/`lastModified` annotations ([resources spec](https://modelcontextprotocol.io/specification/2025-06-18/server/resources)). Ship richer context as `memory://tenant/{t}/user/{u}/snapshot` resources; return **resource links** from tool results so bulk data is pulled only on demand.
- **Result shape**: `content` blocks (text/image/audio/resource_link/embedded resource), optional `structuredContent` (SHOULD mirror the JSON in a text block), `isError: true` for tool-execution errors so models self-correct (SEP-1303); model-caused validation failures must be tool errors, never `-32602` ([tools spec](https://modelcontextprotocol.io/specification/2025-11-25/server/tools)).
- **Pagination**: opaque cursors on list operations; clients must not persist cursors across sessions; invalid cursors SHOULD yield `-32602` ([pagination](https://modelcontextprotocol.io/specification/2025-06-18/server/utilities/pagination)). Hot-path schemas stay cursor-free.
- **Elicitation** (PR #382; URL mode SEP-1036; MRTR SEP-2322) is human-gated and interactive ([elicitation](https://modelcontextprotocol.io/specification/2025-11-25/client/elicitation)) — off the hot path; degrade to labeled partial results instead.

## 4. Reference implementations: auth/tenancy patterns

- **GitHub** — remote `https://api.githubcopilot.com/mcp/`, per-user OAuth or PAT; access "respects the user's permissions," gated by PAT/OAuth policies ([README](https://github.com/github/github-mcp-server), [policies](https://github.com/github/github-mcp-server/blob/main/docs/policies-and-governance.md)).
- **Cloudflare** — `workers-oauth-provider` stores the upstream token encrypted in Workers KV and issues its own token to the MCP client; handlers see already-authenticated user details per request ([blog](https://blog.cloudflare.com/remote-model-context-protocol-servers-mcp)). Their API MCP server collapses 2,594 endpoints into 2 tools (~1,000 tokens vs ~1.17M of native schemas) ([docs](https://developers.cloudflare.com/agents/model-context-protocol/cloudflare/servers-for-cloudflare)).
- **Atlassian** — cloud-hosted, OAuth 2.1 (3LO) or API token; "every action respects the user's existing access controls" ([repo](https://github.com/atlassian/atlassian-mcp-server)).
- **Notion** — hosted `https://mcp.notion.com/mcp`, per-user OAuth; enterprises mint **ID-JAGs** (JWT bearer grants) bound to one user with `resource=https://mcp.notion.com/mcp`; issuer binding + user access rechecked every request ([managed auth](https://developers.notion.com/guides/mcp/enterprise-managed-authorization), [AS metadata](https://mcp.notion.com/.well-known/oauth-authorization-server)).
- **Giant Swarm mcp-prometheus** — closest tenancy analog: "resolves the authenticated user's Mimir tenant IDs and enforces them on every query"; explicit `org_id` validated against the allowed set ([pkg docs](https://pkg.go.dev/github.com/giantswarm/mcp-prometheus)).

## 5. Auth → tenant+user mapping: design options

Normative floor ([authorization spec](https://modelcontextprotocol.io/specification/2026-07-28/basic/authorization), [RFC 8707](https://www.rfc-editor.org/rfc/rfc8707.html), [RFC 9728](https://datatracker.ietf.org/doc/html/rfc9728)): bearer in `Authorization` on **every** request; the server MUST verify the token was issued for its canonical URI (RFC 8707 §2 audience); RFC 9728 metadata advertises the AS. Four strategies for `principal → (tenant, user, capabilities)`:

1. **Per-tenant issuer, single server** — `iss` maps to tenant; the mandatory `aud` check already isolates us. One claim parse; strong isolation.
2. **Single issuer + tenant claim** (`org_id` + `sub`) — deny-by-default check; tenant never a free tool argument. The mcp-prometheus model ([docs](https://pkg.go.dev/github.com/giantswarm/mcp-prometheus)).
3. **ID-JAG / federated enterprise auth** (Notion) — tenant IdP signs a short-lived JWT for one user; server revalidates issuer + user per request; JWKS handling (refetch ≤30s/issuer) ([Notion](https://developers.notion.com/guides/mcp/enterprise-managed-authorization)).
4. **Token brokerage/vault** (Cloudflare) — MCP tier exchanges at our auth service, stores the upstream token encrypted, reissues its own `tenant|user|scopes` token ([Cloudflare](https://blog.cloudflare.com/remote-model-context-protocol-servers-mcp)). Most work; decouples MCP auth from platform cycles.

Latency: **locally verified JWT ≈ free; network introspection is a budget-killing hop.** Prefer 1–2, cache JWKS, and embed tenant/user into ClickHouse keys so authorization is a constant-time claim check prefix to a point lookup.

## 6. Hot-path runtime: precompute, then point-lookup

**ClickHouse serving layout.** Move context production off the query path: incremental materialized views "shift the cost of computation from query time to insert time," refreshable MVs cover periodic heavy recomputation ([MV docs](https://clickhouse.com/docs/concepts/features/materialized-views/index.md)); projections pre-sort/pre-aggregate so reads scan minimal data ([projections docs](https://clickhouse.com/docs/data-modeling/projections)). Concretely: `profile_snapshots ORDER BY (tenant_id, user_id, as_of)` written by the projector; top-K adjacency rollups per `(tenant, user, capability)` incrementally materialized; aggregates as projections; serving = primary-key point queries. There is **no separate per-request "CacheEngine"** — ClickHouse's cache inventory separates the server query cache, mark/uncompressed caches, and filesystem caches ([cache types](https://clickhouse.com/docs/concepts/features/performance/caches/caches)). The query cache is AST-keyed, 60s default TTL, results **not shared between users by default** (security) — so per-user results must come from snapshot tables ([query cache docs](https://clickhouse.com/docs/operations/query-cache)).

**Why no open traversal on the hot path.** SQL graph traversal runs via recursive CTEs (since 24.4, [release notes](https://clickhouse.com/blog/clickhouse-release-24-04)) but degrades sharply: "You write a recursive CTE. It times out at 3 hops. You try self-joins. The query plan explodes." ([Zero-Copy Graph Analytics](https://clickhouse.com/blog/zero-copy-graph-analytics)); practitioners likewise cap recursion depth on dense graphs ([OneUptime](https://oneuptime.com/blog/post/2026-03-31-clickhouse-graph-traversal-queries/view)). Hence: hot path = 3 point queries (snapshot row + embedded adjacency + rollup); deeper provenance is precomputed at reflection time or served from a dedicated non-SLA path.

**Warm cache & stampede control.** Cache-aside keyed `tenant_id|user_id|capability|schema_rev`; jittered TTL (60–300s); single-flight locks or probabilistic early expiration so no two requests rebuild one key — the stampede problem XFetch was optimized for ([Vattani et al., VLDB 2015](https://dl.acm.org/doi/10.14778/2757807.2757813)); production mechanics in [Cloudflare's Sometimes I Cache](https://blog.cloudflare.com/sometimes-i-cache), recipes in [Redis patterns](https://redis.antirez.com/fundamental/cache-stampede-prevention.html).

## 7. Sample p99 latency budget (<2s SLA)

Anchors: local JWT verification is ms-scale ([real-time use case](https://clickhouse.com/docs/get-started/use-cases/real-time-analytics): tenant-scoped primary-key point queries over materialized views are ClickHouse's documented fast path, "exceptionally fast" over MVs).

| # | Stage | Budget (p99) | Cum. | Notes |
|---|-------|--------------|------|-------|
| 1 | Ingress/gateway | 50 | 50 | keep-alive, no per-call OAuth hops |
| 2 | Authn: JWT verify + `iss`/`aud` → tenant | 30 | 80 | cached JWKS; strategy 1–2 |
| 3 | Authz: capability vs tool scope | 50 | 130 | in-process policy check |
| 4 | Per-user cache lookup | 10 | 140 | hit path; single-flight on miss |
| 5 | CH: 3 point-lookups **in parallel** | 250 | 390 | not serialized; 500ms/query timeout → degraded stale result |
| 6 | Assembly (merge, truncate to token budget) | 80 | 470 | pure CPU, no I/O |
| 7 | Serialize + reply | 30 | 500 | JSON mode |
| 8 | Network agent↔gateway | 100 | 600 | cross-region worst case |
| 9 | Agent-side ingest margin | 400 | 1000 | ~25k-token result budget (§8) |
| — | **p99 per tool call** | **≤1000** | | 2× headroom under SLA |

**Design rule:** *context = ≤3 tenant-scoped point-lookups + assembly; graph traversal runs at reflection time.* Corollaries: unbounded-hop/row tools are reflection-path concerns; on overload serve stale-but-labeled snapshots; no elicitation or interactive auth round-trips here.

## 8. Agent-side consumption patterns

- **Client budgets** (Claude Code): 10k-token warning, 25k default hard cap (`MAX_MCP_OUTPUT_TOKENS`), per-result `_meta["anthropic/maxResultSizeChars"]` up to 500k chars ([docs issue](https://github.com/anthropics/claude-code/issues/42869)); Anthropic measured 50–134k tokens lost to tool definitions and recommends on-demand discovery ([engineering blog](https://www.anthropic.com/engineering/advanced-tool-use)).
- **Latency/caching**: OpenAI ties Responses-API latency to verbose tool lists and whole-record returns; recommends `allowed_tools` and trimmed data ([MCP tool guide](https://developers.openai.com/cookbook/examples/mcp/mcp_tool_guide)); 2026-07-28 formalizes client-side caching with `CacheableResult` (`ttlMs`/`cacheScope`) and deterministic `tools/list` ordering ([changelog](https://modelcontextprotocol.io/specification/2026-07-28/changelog)).
- **Streaming vs blocking**: blocking is right for a ≤600ms response; interactive hosts time every tool call, and the spec directs clients to implement timeouts ([tools spec](https://modelcontextprotocol.io/specification/2025-11-25/server/tools)) — a ~1s p99 stays inside any sane client timeout.

## So what for the spec

1. Target **MCP 2026-07-28** stateless Streamable HTTP + JSON-response mode; keep a 2025-11-25 compatibility mode (Cloudflare runs both side by side).
2. Stateless tool server: no sessions, no GET stream; state lives in ClickHouse + the per-user cache, never in process memory.
3. Tool surface ≤4 per capability: `get_context(tenant,user,capability)` (hot path), `search_memory`, snapshot resource link, admin/reflection tools (non-SLA).
4. Compact `structuredContent` + companion `resource_link`; summary ≤8k tokens; emit `ttlMs`/`cacheScope`; deterministic `tools/list`.
5. Auth: strategies 1–2 (per-tenant issuer or tenant claim), locally verifiable, `aud` = canonical MCP URI (RFC 8707); introspection/ID-JAG only for enterprise — never on the hot path.
6. Tenant/user from claims, baked into ClickHouse keys; tenant is never a free tool argument.
7. The reflection/projector pipeline owns all graph math: it writes `profile_snapshots` and adjacency rollups (incremental + refreshable MVs, projections); the serve path touches only those outputs.
8. Per-user cache: jittered TTL + single-flight/XFetch; staleness surfaced via timestamps in results, never silent.
9. Rate limits + per-tenant quota so one tenant can't stampede co-tenants; JWKS cached with bounded refresh (Notion: ≤30s/issuer).
10. CI the budget: p99 ≤1s at steady state, with per-stage traces (authn/authz → CH → assembly → reply) keeping §7 anchors falsifiable.
