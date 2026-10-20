"""Ledger conformance: one suite, two backends (memory always; CH when up).

Any future/backend implementation must pass this suite unmodified.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import UTC, datetime

import pytest

from graph_memory.core.types import IngestResult, SignalType, TenantId, UserKey
from graph_memory.settings import Settings
from graph_memory.signals.envelope import Envelope
from graph_memory.signals.ledger import (
    ClickHouseLedger,
    InMemoryLedger,
    LedgerRecord,
    LedgerRepository,
    default_client,  # noqa: PLC2701 — conformance probe
    record_from_envelope,
)


def _envelope(event_id: str, *, ms: int = 123, **overrides: object) -> Envelope:
    payload: dict[str, object] = {
        "specversion": "1.0",
        "id": event_id,
        "source": "https://app.example.com/signals",
        "type": "itf.skill_assessed.v1",
        "time": f"2026-06-01T12:00:00.{ms:03d}Z",
        "partitionkey": "user-1",
        "data": {"skill": "rust", "score": 0.8},
    }
    payload.update(overrides)
    for key, value in list(payload.items()):
        if value is None:
            del payload[key]
    return Envelope.parse(payload)


def _record(tenant: str, envelope: Envelope) -> LedgerRecord:
    return record_from_envelope(TenantId(tenant), envelope)


def _ch_available(settings: Settings) -> bool:
    try:
        client = default_client(settings)
        client.command("SELECT 1")
    except Exception:  # noqa: BLE001 — any failure means "not available"
        return False
    return True


@pytest.fixture(params=["memory", "clickhouse"])
def ledger(request: pytest.FixtureRequest) -> Iterator[LedgerRepository]:
    """The same contract against both backends; CH skipped when unreachable."""
    if request.param == "memory":
        yield InMemoryLedger()
        return
    settings = Settings()
    if not _ch_available(settings):
        pytest.skip("ClickHouse not reachable at the configured endpoint")
    run = uuid.uuid4().hex[:8]
    instance = ClickHouseLedger(
        settings,
        database="gmp_ledger_test",
        signals_table=f"signals_{run}",
        dlq_table=f"dlq_{run}",
    )
    instance.ensure_schema()
    yield instance
    instance.client.command(f"DROP DATABASE IF EXISTS gmp_ledger_test_{run}")
    instance.client.command("DROP DATABASE IF EXISTS gmp_ledger_test")


T_ACME = TenantId("acme")


def test_append_then_read_roundtrip(ledger: LedgerRepository) -> None:
    ledger.ensure_schema()
    records = [
        _record("acme", _envelope("a", ms=123)),
        _record("acme", _envelope("b", ms=124, partitionkey="user-2")),
        _record("acme", _envelope("c", ms=125, type="itf.reco.v1", data={"choice": 3})),
    ]
    result = ledger.append(records)
    assert (result.accepted, result.duplicates) == (3, 0)

    events = ledger.read_events(T_ACME)
    assert [event.dedup_key for event in events] == [record.dedup_key for record in records]
    first = events[0]
    assert first.tenant == T_ACME
    assert first.signal == SignalType("itf.skill_assessed.v1")
    assert first.user_key == UserKey("user-1")
    assert first.occurred_at == datetime(2026, 6, 1, 12, 0, 0, 123_000, tzinfo=UTC)
    assert first.envelope_v == 1
    assert first.envelope == records[0].envelope


def test_reappending_the_same_batch_is_idempotent(ledger: LedgerRepository) -> None:
    ledger.ensure_schema()
    records = [_record("acme", _envelope("a")), _record("acme", _envelope("b", ms=124))]
    assert ledger.append(records) == IngestResult(accepted=2, duplicates=0)
    again = ledger.append(records)
    assert (again.accepted, again.duplicates) == (0, 2)
    assert len(ledger.read_events(T_ACME)) == 2


def test_intra_batch_duplicates_collapse(ledger: LedgerRepository) -> None:
    ledger.ensure_schema()
    same = _envelope("a")
    result = ledger.append([_record("acme", same), _record("acme", same)])
    assert (result.accepted, result.duplicates) == (1, 1)
    assert len(ledger.read_events(T_ACME)) == 1


def test_same_event_across_tenants_is_two_facts(ledger: LedgerRepository) -> None:
    ledger.ensure_schema()
    shared = _envelope("shared-id")
    result = ledger.append([_record("acme", shared), _record("other", shared)])
    assert (result.accepted, result.duplicates) == (2, 0)
    assert len(ledger.read_events(T_ACME)) == 1
    assert len(ledger.read_events(TenantId("other"))) == 1


def test_existing_keys_are_scoped_to_tenant(ledger: LedgerRepository) -> None:
    ledger.ensure_schema()
    shared = _envelope("shared-id")
    ledger.append([_record("acme", shared)])
    key = shared.dedup_key
    assert ledger.existing_keys(T_ACME, [key]) == {key}
    assert ledger.existing_keys(TenantId("other"), [key]) == set()


def test_filters_narrow_by_user_and_signal(ledger: LedgerRepository) -> None:
    ledger.ensure_schema()
    ledger.append(
        [
            _record("acme", _envelope("a", ms=123)),
            _record("acme", _envelope("b", ms=124, partitionkey="user-2")),
            _record("acme", _envelope("c", ms=125, type="itf.reco.v1")),
        ]
    )
    users = ledger.read_events(T_ACME, user_key=UserKey("user-2"))
    assert [event.user_key for event in users] == [UserKey("user-2")]
    skills = ledger.read_events(T_ACME, signal=SignalType("itf.skill_assessed.v1"))
    assert len(skills) == 2
    assert all(event.signal == SignalType("itf.skill_assessed.v1") for event in skills)


def test_verbatim_extensions_survive_the_roundtrip(ledger: LedgerRepository) -> None:
    ledger.ensure_schema()
    envelope = _envelope("a", traceparent="00-abcdef-01")
    ledger.append([_record("acme", envelope)])
    (event,) = ledger.read_events(T_ACME)
    assert event.envelope.extensions["traceparent"] == "00-abcdef-01"
    assert event.payload["id"] == "a"  # full envelope stored verbatim
    assert len(event.dedup_key) == 64  # identity is the hash, never the raw key


def test_empty_append_is_a_clean_noop(ledger: LedgerRepository) -> None:
    ledger.ensure_schema()
    assert ledger.append([]) == IngestResult(accepted=0, duplicates=0)
    assert ledger.read_events(T_ACME) == []


def test_clickhouse_schema_creates_ledger_and_dlq() -> None:
    settings = Settings()
    if not _ch_available(settings):
        pytest.skip("ClickHouse not reachable at the configured endpoint")
    run = uuid.uuid4().hex[:8]
    ledger = ClickHouseLedger(
        settings,
        database="gmp_ledger_test",
        signals_table=f"signals_{run}",
        dlq_table=f"dlq_{run}",
    )
    try:
        ledger.ensure_schema()
        names = {
            row[0]
            for row in ledger.client.query(
                "SELECT name FROM system.tables WHERE database = {db:String}",
                parameters={"db": "gmp_ledger_test"},
            ).result_rows
        }
        assert f"signals_{run}" in names
        assert f"dlq_{run}" in names
    finally:
        ledger.client.command("DROP DATABASE IF EXISTS gmp_ledger_test")
