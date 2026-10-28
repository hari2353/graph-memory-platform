# Claude Code recipe (M4.4)

Mount `graph-memory-platform` as Claude Code memory: the session gets the
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

## 1. Add the remote HTTP server

MCP Streamable HTTP at `/mcp` on the MCP plane (default 8003), with the
bearer header:

```sh
claude mcp add --transport http graph-memory http://127.0.0.1:8003/mcp \
  --header "Authorization: Bearer dev-acme-token"
```

Scopes (`--scope`): `local` (default — you, this project), `user`
(you, every project), `project` (shared via the committed
`.mcp.json`). Confirm with:

```sh
claude mcp list        # ✔ Connected — 7 tools
claude mcp get graph-memory
```

Inside a session, `/mcp` shows the server and its tool count.

## 2. Manual configuration (`.mcp.json`, project shared)

```json title=".mcp.json"
{
  "mcpServers": {
    "graph-memory": {
      "type": "http",
      "url": "http://127.0.0.1:8003/mcp",
      "headers": {
        "Authorization": "Bearer dev-acme-token"
      }
    }
  }
}
```

- `type` is REQUIRED when hand-writing JSON (`"http"`; `streamable-http`
  is accepted as an alias) — an entry with a `url` and no `type` is
  read as stdio and skipped.
- Commit this file to share the server with the team; the dev token is
  not a secret in the dev posture. For production secrets, keep the
  token in environment-driven settings (`claude mcp add` with
  `${VAR}`-style expansion where supported) or manage the server via
  organization config, not a committed file.
- Project-scoped servers from `.mcp.json` sit in `Pending approval`
  until you approve them in the trust prompt (`claude` interactively)
  or allow them via `enableAllProjectMcpServers` /
  `enabledMcpjsonServers` settings — approval is per workspace.
- macOS/Linux variant of the same steps:
  `claude mcp add --transport http graph-memory http://127.0.0.1:8003/mcp --header "Authorization: Bearer dev-acme-token"`.

## 3. What the session gets

Claude Code may prefix tool names with the server name
(`mcp__graph-memory__get_context`); the 7 tools are:

```text
get_context        search_memory      delete_memory
list_capabilities  get_signal_schema
get_effectiveness  explain_context
```

- `get_context(user, capability)` — the pure read model snapshot.
- `search_memory(user, capability, query?, limit?)` — ranked hybrid
  search (lexical + typed/traversal + loop weights, M4.2); never the
  default context path.
- `delete_memory(user, kind, target?)` — governed forgetting (M4.1):
  the directive lands as a ledger event through the SAME spec/loop; the
  visibility predicate hides covered observations.
- `get_signal_schema`, `list_capabilities`, `get_effectiveness`,
  `explain_context` — the reflection tools; unwired reflections return
  `not_wired`, never a fake result.

## 4. Notes and gotchas

- **Tenancy rides the token.** `dev-acme-token` means the session can
  only read/write tenant `acme` — a project-scoped `.mcp.json` PINs one
  tenant per credential; add a second server entry under a different
  name+token to expose another tenant.
- **The equal-instant forget boundary is `<=`**: a directive at instant
  `t` covers observations valid at or before `t` (tie-free, tested).
- Long-running sessions: the streamable HTTP transport reconnects
  automatically on a mid-session drop; tool results follow the
  platform's bounded-payload discipline (`top_k`-truncated), so no
  25K-token output clamps should appear on context reads.
