# Interop surface (M4.4, research 06 reach section)

graph-memory-platform is mounted as agent memory two ways: as an **MCP
server** (the agent-facing face, what hosts like OpenCode and Claude Code
mount) and as a **raw HTTP API + SDK** (the application-facing face). This
directory documents both honestly against the shipped surface — every row
below is verifiable in `src/graph_memory` today.

## The surfaces, side by side

Endpoints that exist on one face but not another are listed as such — the
list is the parity contract, not marketing.

| Capability | MCP tool (port 8003) | REST (port 8002) | Python SDK (`channels/sdk.py`) | Scope |
| --- | --- | --- | --- | --- |
| Read one user's context snapshot | `get_context` | `GET /v1/context/{u}/{c}` | `context()` | `context:read` |
| Paged items walk | — (use `search_memory` or REST) | `GET /v1/context/{u}/{c}/items` | `iter_items()` | `context:read` |
| Ranked hybrid search (loop-weighted, M4.2) | `search_memory` | `GET /v1/search/{u}` | — (channel-only) | `memory:search` |
| Warm-on-login batch (M4.6) | — | `POST /v1/prefetch/{u}` | — (channel-only) | `context:read` |
| Prefetch one snapshot (per-capability) | — | `POST /v1/context/{u}/{c}/prefetch` | `prefetch()` | `context:read` |
| Governed forget (M4.1) | `delete_memory` | `POST /v1/forget/{u}` | — (channel-only) | `memory:write` |
| Self-data export (M4.5) | — | `GET /v1/export/{u}` | — (channel-only) | `context:read` |
| Ingest signals | — (ledger/broker feeds it) | `POST /v1/signals:batch` (port 8001) | `ingest()` | `signals:write` |
| Capability catalog | `list_capabilities` | — (MCP-only reflection) | — | `context:read` |
| Signal schema projection | `get_signal_schema` | — (MCP-only reflection) | — | `signals:write` |
| Effectiveness loop report | `get_effectiveness` | — (MCP-only reflection) | — | `context:read` |
| Provenance/decision explanation | `explain_context` | — (MCP-only reflection) | — | `context:read` |

The MCP server exposes **7 tools**: `get_context`, `search_memory`,
`list_capabilities`, `get_signal_schema`, `get_effectiveness`,
`explain_context`, `delete_memory`.

## Mounting basics (both hosts)

- **Transport**: MCP Streamable HTTP at `http://127.0.0.1:8003/mcp` —
  the MCP plane of `platform serve` (`--mcp-port`; default 8003, REST
  8002, ingest 8001).
- **Auth**: `Authorization: Bearer <token>` on every call. Dev posture:
  the static tokens from `Settings().dev_static_tokens`
  (`dev-acme-token` → tenant `acme`, `dev-other-token` → tenant
  `other`). Production replaces the guard (`TenantGuard` port) without
  changing these recipes.
- **Tenancy**: the tenant always rides the CREDENTIAL, never a
  parameter — a mounted agent only ever reads/writes the tenant its
  token names. Cross-tenant user keys host-local resources with the
  same key are not a read path (structural rule, tested per channel).
- **Idempotency**: mutating reads that admit replays accept an
  `Idempotency-Key` header (`Idempotency-Replayed: true` on replay).
- **Unwired surfaces fail loudly**: any port a deployment did not wire
  returns an anticipated error (`details.reason = "not_wired"`), never
  a silent skip.

## Recipes

- [OpenCode](mcp-opencode.md) — `opencode mcp add` + manual
  `opencode.json` shape.
- [Claude Code](mcp-claude-code.md) — `claude mcp add` + `.mcp.json`
  shape.
- [TypeScript SDK](graph-memory.ts) — a thin facade maintained 1:1
  against `channels/sdk.py` (see the module header for the parity
  checklist and how to diff the two).

## Parity checklist (re-run whenever a channel changes)

1. Same read-model docs on every face: MCP `get_context` payload ==
   REST `GET /v1/context/...` body == SDK `context()` body (one
   `engine.snapshot` core; channels are renderers).
2. Ordering kernels live in exactly one place: ranked search orders by
   `core/context/search.py` (`rank_search`) on BOTH MCP and REST (the
   lexical `positionCaseInsensitive` pushdown lives in both gateways).
3. Scope map above matches `Scope` values in `core/types.py`; every
   face verifies the scope BEFORE any work.
4. Errors are `PlatformError` payloads (`error`/`code`/`details`)
   everywhere; unwired → `not_wired`; unknown capability → 404
   `capability_unknown`.
5. Metering/spans cover admitted calls identically
   (`rest`-lane + `mcp`-lane events; spans per stage).
6. The TS facade methods byte-map to these REST routes and to
   `channels/sdk.py` (same headers, same bodies, no extra policy).
