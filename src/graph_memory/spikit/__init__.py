"""Broker adapter conformance kit (the SPI's shared fixtures + checks).

Every broker adapter (M2: Kafka, Pulsar, NATS; later SNS/SQS, Azure Service
Bus, Google Pub/Sub) must deliver kit fixtures to the ledger such that:

- per-user event ORDER is preserved exactly (research 02's mandatory rule,
  via each broker's key mechanism — see :func:`stamping`);
- duplicates MAY be delivered at any point (at-least-once) and MUST be
  absorbed by ledger dedup, harming no order;
- poison payloads are recorded, never dropped silently.

The kit runs without a live broker: adapter conformance = fixture replay
through an in-memory "transport" the adapter's delivery functions fill.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Literal

from graph_memory.core.types import TenantId
from graph_memory.signals.envelope import Envelope

BrokerName = Literal["kafka", "pulsar", "nats", "http"]

TENANT: TenantId = TenantId("kit")


def kit_envelopes() -> list[Envelope]:
    """Shared fixtures: three users, interleaved, two signal types."""
    raw = [
        ("u9-a", "user-9", "itf.skill_assessed.v1", 123),
        ("u1-a", "user-1", "itf.skill_assessed.v1", 124),
        ("u9-b", "user-9", "itf.reco.v1", 125),
        ("u2-a", "user-2", "itf.feedback.v2", 126),
        ("u1-b", "user-1", "itf.skill_assessed.v1", 127),
        ("u9-c", "user-9", "itf.skill_assessed.v1", 128),
    ]
    envelopes = []
    for event_id, user, signal, ms in raw:
        payload: dict[str, object] = {
            "specversion": "1.0",
            "id": event_id,
            "source": "https://kit.example.com/signals",
            "type": signal,
            "time": f"2026-06-01T12:00:00.{ms:03d}Z",
            "partitionkey": user,
            "data": {"kit": True, "seq": event_id},
        }
        envelopes.append(Envelope.parse(payload))
    return envelopes


def assert_per_user_order(delivered: Sequence[Envelope]) -> None:
    """Order check on deduped delivery: per-user sequence must match fixtures.

    Duplicates are legal; they must be absorbable (first occurrence holds
    the position). Reordering a user's events is a conformance FAILURE.
    """
    kit = kit_envelopes()
    expected: dict[str, list[str]] = {}
    for envelope in kit:
        expected.setdefault(envelope.partitionkey, []).append(envelope.dedup_key)

    seen: dict[str, list[str]] = {}
    for envelope in delivered:
        user = envelope.partitionkey
        if envelope.dedup_key not in seen.setdefault(user, []):
            seen[user].append(envelope.dedup_key)

    for user, keys in seen.items():
        if user not in expected:
            raise AssertionError(f"delivery contained an unknown user {user!r}")
        if keys != expected[user]:
            raise AssertionError(
                f"per-user order violated for {user!r}: {keys} != {expected[user]}"
            )


def stamping(envelope: Envelope, broker: BrokerName) -> dict[str, str]:
    """How an adapter must stamp the envelope on each broker.

    Kafka: record key = user. Pulsar: ordering key = user. NATS JetStream:
    ``Nats-Msg-Id`` = dedup key (duplicate suppression) and subject includes
    the user. HTTP: batch member (order = request semantics).
    """
    if broker == "kafka":
        return {"key": envelope.partitionkey}
    if broker == "pulsar":
        return {"orderingKey": envelope.partitionkey}
    if broker == "nats":
        return {"Nats-Msg-Id": envelope.dedup_key, "Subject": f"signals.{envelope.partitionkey}"}
    return {"batchMember": envelope.dedup_key}
