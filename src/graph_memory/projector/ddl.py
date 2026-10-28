"""Graph-table DDL (nodes + edge observations + forget overlay) — the served
graph layer.

Identity semantics mirror the ledger: replacing rows on their primary key.
Edge rows are keyed by the OBSERVATION identity ``(tenant, user, kind,
target, valid_at)`` and node rows by ``(tenant, key)`` — the same identity
the in-memory gateway uses, so both backends converge structurally. Forget
rows add the M4.1 overlay keyed ``(tenant, user, kind, target, target_any,
forget_at)``: visibility filtering happens at read time (research 06),
never by mutating edge rows.
"""

import re

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")

_DATABASE = "CREATE DATABASE IF NOT EXISTS {database}"

_NODES = """CREATE TABLE IF NOT EXISTS {database}.{nodes_table} (
  -- Edge endpoints must exist, so every mapped event upserts its user node.
  tenant_id  LowCardinality(String),
  key        String,
  labels     Array(String),
  props_json String
)
ENGINE = ReplacingMergeTree
ORDER BY (tenant_id, key)"""

_EDGES = """CREATE TABLE IF NOT EXISTS {database}.{edges_table} (
  tenant_id      LowCardinality(String),
  user_key       String,
  kind           LowCardinality(String),
  target         String,
  weight         Float64,
  half_life_days Float64,
  valid_at       DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
ORDER BY (tenant_id, user_key, kind, target, valid_at)
PARTITION BY toYYYYMM(valid_at)"""

_FORGETS = """CREATE TABLE IF NOT EXISTS {database}.{forgets_table} (
  -- Governed-forgetting overlay (M4.1): identity rows, not state mutations.
  -- target_any = 1 means the directive covers the WHOLE kind (target empty).
  tenant_id   LowCardinality(String),
  user_key    String,
  kind        LowCardinality(String),
  target      String,
  target_any  UInt8,
  forget_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
ORDER BY (tenant_id, user_key, kind, target, target_any, forget_at)
PARTITION BY toYYYYMM(forget_at)"""


def build_graph_ddl(
    database: str, nodes_table: str, edges_table: str, forgets_table: str | None = None
) -> tuple[str, ...]:
    """Idempotent DDL: safe database + the graph tables.

    ``forgets_table`` (M4.1) adds the forget overlay table; pass ``None``
    to build the base pair only (e.g. the rebuild staging partners, which
    are nodes/edges — the overlay is replay-idempotent and never staged).
    """
    names: list[tuple[str, str]] = [
        (database, "database"),
        (nodes_table, "nodes table"),
        (edges_table, "edges table"),
    ]
    if forgets_table is not None:
        names.append((forgets_table, "forgets table"))
    for name, what in names:
        if not _IDENT.match(name):
            raise ValueError(f"unsafe SQL identifier for {what}: {name!r}")
    statements: tuple[str, ...] = (
        _DATABASE.format(database=database),
        _NODES.format(database=database, nodes_table=nodes_table),
        _EDGES.format(database=database, edges_table=edges_table),
    )
    if forgets_table is not None:
        statements += (_FORGETS.format(database=database, forgets_table=forgets_table),)
    return statements
