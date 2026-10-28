# OpenCode recipe (M4.4)

Mount `graph-memory-platform` as OpenCode agent memory: the agent gets the
7 MCP tools (`get_context`, `search_memory`, `list_capabilities`,
`get_signal_schema`, `get_effectiveness`, `explain_context`,
`delete_memory`) against the `platform serve` MCP plane.

## 0. Boot the platform

```sh
.venv\Scripts\platform.exe serve --mapping svc/skills.yaml
# serving: ingest :8001  rest :8002  mcp :8003
```

Dev bearer tokens come from `Settings().dev_static_tokens`:
`dev-acme-token` speaks for tenant `acme`, `dev-other-token` for tenant
`other`. In production the same recipe works with your real
`TenantGuard` tokens.

## 1. Add the remote MCP server

MCP Streamable HTTP at `/mcp` on the MCP plane (default 8003).

```sh
opencode mcp add graph-memory --url http://127.0.0.1:8003/mcp --header "Authorization=Bearer dev-acme-token"
```

`add` writes the server into the project `opencode.json`(c) (add
`--global` to make it available in every project). Check the connection:

```sh
opencode mcp list
# ✓ graph-memory  connected
```

## 2. Manual configuration (what the command writes)

```jsonc title="opencode.jsonc"
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "servers": {
      "graph-memory": {
        "type": "remote",
        "url": "http://127.0.0.1:8003/mcp",
        // The server is header-credential only — disable OAuth probing.
        "oauth": false,
        "headers": {
          "Authorization": "Bearer {env:GMP_TOKEN}"
        }
      }
    }
  }
}
```

- `{env:GMP_TOKEN}` is expanded by OpenCode — keep the secret in the
  environment, not in the repo (substitute `dev-acme-token` in dev).
- `oauth: false` because the platform authenticates by header only.
- Config changes hot-reload; no restart needed. `/mcps` in the TUI
  connects, disconnects, and shows status per server.

## 3. What the agent gets

OpenCode names every tool `<server>_<tool>` — it replaces characters
other than letters, numbers, `_`, and `-` with `_`, and the `-` in
`graph-memory` is kept, so the mounted tools are:

```text
graph-memory_get_context      graph-memory_list_capabilities
graph-memory_search_memory    graph-memory_get_signal_schema
graph-memory_explain_context  graph-memory_get_effectiveness
graph-memory_delete_memory
```

Under the default Code Mode the same tools are grouped under the
normalized server name — `tools["graph-memory"].get_context(...)` in
script-call form. The server name `graph-memory` is deliberately
normalization-stable.

## 4. Notes and gotchas

- **Tenancy rides the token.** With `dev-acme-token` the agent can only
  read/write tenant `acme`; a cross-tenant user key is not a read path.
  Use a separate `opencode mcp add` name per tenant credential if you
  need two views (e.g. `memory-acme`, `memory-other`).
- **`search_memory` never replaces context.** It is the ranked surface
  (loop-weighted hybrid search, M4.2); context assembly stays the
  point-lookup `get_context`.
- **Unwired reflections fail loudly.** `get_effectiveness` /
  `explain_context` need the corresponding port wired at serve boot;
  otherwise they return `not_wired` instead of pretending.
- **Idempotency replay.** mutating REST reads accept
  `Idempotency-Key`; the MCP face is safe to re-call by design (drafted
  forget directives are per-event identity rows).
- If `opencode mcp list` shows `needs authentication`, you mounted
  without a header — drop and re-add with `--header` (the server never
  redirects to an OAuth provider).
