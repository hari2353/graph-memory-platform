"""Platform settings (env-driven, GMP_ prefix).

Defaults target the documented local posture: ClickHouse HTTP on 127.0.0.1:8124
(user ``clickhouse``) — the same local server the ClickGraph e2e tier uses.
Every value is overridable via ``GMP_*`` environment variables.
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Kernel configuration; infra endpoints + ingest bounds live here."""

    model_config = SettingsConfigDict(env_prefix="GMP_")

    clickhouse_host: str = "127.0.0.1"
    clickhouse_port: int = 8124
    clickhouse_username: str = "clickhouse"
    clickhouse_password: str = "e2e-ch-pass-1234"
    clickhouse_database: str = "gmp"
    signals_table: str = "signals_events"
    dlq_table: str = "signal_dlq"
    nodes_table: str = "gm_nodes"
    edges_table: str = "gm_edges"

    max_batch_events: int = 10_000
    max_payload_bytes: int = 65_536  # CloudEvents' 64 KiB intermediary guidance

    # Pulsar dev posture (ops/compose/pulsar.yaml + the drain-brokers command)
    pulsar_admin_url: str = "http://127.0.0.1:8099"
    pulsar_container: str = "gmp-dev-pulsar"
    pulsar_namespace: str = "public/default"
    pulsar_drain_subscription: str = "gmp-drain"

    dev_static_tokens: dict[str, str] = {
        "dev-acme-token": "acme",
        "dev-other-token": "other",
    }
