"""Ledger DDL contract: CH 24.8 accepts every statement it emits.

The live tier caught a version-specific footgun: a TTL expression must return
Date/DateTime, so a bare DateTime64 column is rejected at CREATE time.
"""

from __future__ import annotations

import re

from graph_memory.signals.sql.ddl import build_ddl


def test_dlq_ttl_is_projected_to_datetime() -> None:
    _, _, dlq = build_ddl("gmp", "signals_events", "signal_dlq")
    ttl = re.search(r"TTL ([^\n]+)", dlq)
    assert ttl is not None
    assert ttl.group(1) == "toDateTime(occurred_at) + INTERVAL 180 DAY"


def test_ddl_rejects_unsafe_identifiers() -> None:
    import pytest

    with pytest.raises(ValueError, match="unsafe SQL identifier"):
        build_ddl("gmp; DROP DATABASE x", "signals_events", "signal_dlq")
