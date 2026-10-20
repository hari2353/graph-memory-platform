"""SPI kit conformance: fixtures + per-user order + broker stampings."""

from __future__ import annotations

from typing import cast

import pytest

from graph_memory.core.types import IngestResult, TenantId
from graph_memory.signals.ledger import InMemoryLedger, LedgerRepository, record_from_envelope
from graph_memory.spikit import TENANT, BrokerName, assert_per_user_order, kit_envelopes, stamping


def test_fixtures_cover_three_users_two_types() -> None:
    kit = kit_envelopes()
    assert len({envelope.partitionkey for envelope in kit}) == 3
    assert len({envelope.type for envelope in kit}) >= 2


def test_in_order_delivery_passes() -> None:
    assert_per_user_order(kit_envelopes())


def test_duplicate_replays_do_not_break_order() -> None:
    kit = kit_envelopes()
    delivered = [*kit[:3], *kit[:3], *kit]  # immediate replay of a prefix
    assert_per_user_order(delivered)


def test_shuffled_user_events_fail_conformance() -> None:
    kit = kit_envelopes()
    users = [envelope for envelope in kit if envelope.partitionkey == "user-9"]
    middling = [envelope for envelope in kit if envelope.partitionkey != "user-9"]
    broken = [*users[1:], *middling, users[0]]  # user-9's first event arrives LAST
    with pytest.raises(AssertionError, match="per-user order"):
        assert_per_user_order(broken)


@pytest.mark.parametrize("broker", ["kafka", "pulsar", "nats", "http"])
def test_broker_stampings_keep_the_user_key(broker: str) -> None:
    (envelope,) = [envelope for envelope in kit_envelopes() if envelope.id == "u1-a"]
    marks = stamping(envelope, cast("BrokerName", broker))
    if broker in ("kafka", "pulsar"):
        assert marks["key" if broker == "kafka" else "orderingKey"] == "user-1"
    elif broker == "nats":
        assert marks["Nats-Msg-Id"] == envelope.dedup_key
        assert marks["Subject"].endswith("user-1")
    else:
        assert marks["batchMember"] == envelope.dedup_key


async def test_ledger_absorbs_kit_delivery_with_duplicates() -> None:
    ledger: LedgerRepository = InMemoryLedger()
    kit = kit_envelopes()
    delivered = [*kit[:3], *kit[:3], *kit]
    assert_per_user_order(delivered)
    result = ledger.append([record_from_envelope(TENANT, envelope) for envelope in delivered])
    assert result == IngestResult(accepted=len(kit), duplicates=len(delivered) - len(kit))
    assert set(ledger.tenants()) == {TenantId("kit")}
