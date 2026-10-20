"""Graph-table DDL (nodes + edge observations) — the served graph layer.

Identity semantics mirror the ledger: replacing rows on their primary key.
Edge rows are keyed by the OBSERVATION identity ``(tenant, user, kind,
target, valid_at)`` and node rows by ``(tenant, key)`` — the same identity
the in-memory gateway uses, so both backends converge structurally.
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


def build_graph_ddl(database: str, nodes_table: str, edges_table: str) -> tuple[str, ...]:
    """Idempotent DDL: safe database + the two graph tables."""
    for name, what in (
        (database, "database"),
        (nodes_table, "nodes table"),
        (edges_table, "edges table"),
    ):
        if not _IDENT.match(name):
            raise ValueError(f"unsafe SQL identifier for {what}: {name!r}")
    return (
        _DATABASE.format(database=database),
        _NODES.format(database=database, nodes_table=nodes_table),
        _EDGES.format(database=database, edges_table=edges_table),
    )
