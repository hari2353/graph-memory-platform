"""Ledger + DLQ DDL (SPEC §6, research 02/03)."""

import re

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")

_DATABASE = "CREATE DATABASE IF NOT EXISTS {database}"

_SIGNALS = """CREATE TABLE IF NOT EXISTS {database}.{signals_table} (
  -- Dedup identity: (tenant_id, dedup_key); ReplacingMergeTree(ingested_at) +
  -- FINAL reads own correctness. Projections are deliberately omitted: current
  -- CH builds reject them here (deduplicate_merge_projection_mode = throw).
  tenant_id    LowCardinality(String),
  signal_type  LowCardinality(String),
  dedup_key    String,
  user_key     String,
  occurred_at  DateTime64(3, 'UTC'),
  ingested_at  DateTime64(3, 'UTC') DEFAULT now64(3),
  envelope_v   UInt32,
  payload      String
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (tenant_id, user_key, signal_type, occurred_at, dedup_key)
PARTITION BY toYYYYMM(occurred_at)
SETTINGS non_replicated_deduplication_window = 1000, index_granularity = 8192"""

_DLQ = """CREATE TABLE IF NOT EXISTS {database}.{dlq_table} (
  tenant_id   String,
  occurred_at DateTime64(3, 'UTC') DEFAULT now64(3),
  payload     String,
  reason      String,
  attempts    UInt32 DEFAULT 1
)
ENGINE = MergeTree
ORDER BY (tenant_id, occurred_at)
TTL occurred_at + INTERVAL 180 DAY"""


def build_ddl(database: str, signals_table: str, dlq_table: str) -> tuple[str, ...]:
    """Idempotent DDL: safe database + tables for the ledger and its DLQ."""
    for name, limit in ((database, 63), (signals_table, 63), (dlq_table, 63)):
        if not _IDENT.match(name):
            raise ValueError(f"unsafe SQL identifier: {name!r} (max {limit} chars)")
    return (
        _DATABASE.format(database=database),
        _SIGNALS.format(database=database, signals_table=signals_table),
        _DLQ.format(database=database, dlq_table=dlq_table),
    )
