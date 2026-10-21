"""Pulsar dev-drain receiver: wire contract + full adapter chain (offline).

The receiver is exercised over fake transport seams in the same composition
the ``platform drain-brokers`` command uses: backlog-driven CLI consume,
base64 frame decode, and the ingest adjudication exactly the HTTP channel
gets (duplicates absorbed, poison captured by the kit rule - never dropped).
"""

from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import Callable
from typing import Any

import httpx

from graph_memory.core.types import TenantId
from graph_memory.settings import Settings
from graph_memory.signals.brokers import BrokerIngestor, topic_for
from graph_memory.signals.brokers.pulsar import PulsarIngester
from graph_memory.signals.brokers.pulsar_cli import PulsarCliReceiver, decode_frame
from graph_memory.signals.ledger import InMemoryLedger

_TENANT = TenantId("acme")
_TOPIC = str(topic_for(_TENANT))
_SUBSCRIPTION = "gmp-drain"


class _PoisonRecorder:
    """Capture-everything poison sink (the conformance kit's own recorder)."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    def record(self, tenant: TenantId, payload: str, reason: str) -> None:
        self.calls.append((str(tenant), payload, reason))


def _envelope_json(event_id: str, user: str) -> str:
    return json.dumps(
        {
            "specversion": "1.0",
            "id": event_id,
            "source": "https://unit.example/signals",
            "type": "itf.skill_assessed.v1",
            "time": "2026-06-01T12:00:00.000Z",
            "partitionkey": user,
            "data": {"skill": "rust"},
        },
        separators=(",", ":"),
    )


def _b64(payload: str) -> str:
    return base64.b64encode(payload.encode("utf-8")).decode("ascii")


def _frame(key: str, content: str) -> str:
    return f"publishTime:[10], eventTime:[0], key:[{key}], properties:[], content:{content}"


def _consumed_stdout(frames: list[tuple[str, str]], *raw: str) -> str:
    """CLI output shape: client log noise first, then one line per message."""
    lines = ["2026-10-21T04:00:00 [main] INFO consumer created", "----- got message -----"]
    lines.extend(_frame(key, content) for key, content in frames)
    lines.extend(_frame("x", blob) for blob in raw)
    return "\n".join(lines)


def _fake_admin(*, topics: list[str], backlogs: dict[str, int], puts: list[str]) -> httpx.Client:
    """Admin REST fake: stats honor PUT-created subs; creation path is real.

    ``backlogs`` is the post-creation backlog. Before a PUT, the subscription
    is absent (stats responses carry no ``subscriptions`` key) - so the
    receiver's ensure-then-consume path is exercised for real.
    """

    state: dict[str, bool] = {name: False for name in topics}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "GET" and path == "/admin/v2/persistent/public/default":
            return httpx.Response(
                200, json=[f"persistent://public/default/{name}" for name in topics]
            )
        if "/subscription/" in path and request.method == "PUT":
            puts.append(path)
            state[path.rsplit("/", 3)[-3]] = True
            return httpx.Response(204)
        if request.method == "GET" and path.endswith("/stats"):
            topic = path.rsplit("/", 2)[-2]
            if not state.get(topic):
                return httpx.Response(200, json={})  # drain subscription absent
            return httpx.Response(
                200, json={"subscriptions": {_SUBSCRIPTION: {"msgBacklog": backlogs[topic]}}}
            )
        return httpx.Response(404, json={"reason": f"unhandled {path}"})

    return httpx.Client(transport=httpx.MockTransport(handler))


def _fake_runner(
    stdout: str, calls: list[list[str]], *, reset_rc: int = 0
) -> Callable[[Any], tuple[int, str, str]]:
    def run(args: Any) -> tuple[int, str, str]:
        assert isinstance(args, list)
        calls.append(list(args))
        if args[0] == "/pulsar/bin/pulsar-admin":
            return reset_rc, "", ""
        return 0, stdout, ""

    return run


def _reset_call(calls: list[list[str]]) -> list[str]:
    admin = [call for call in calls if call[0] == "/pulsar/bin/pulsar-admin"]
    assert len(admin) == 1, "cursor reset happens exactly at creation"
    return admin[0]


def _consume_calls(calls: list[list[str]]) -> list[list[str]]:
    return [call for call in calls if call[0] == "/pulsar/bin/pulsar-client"]


def _second_user_event() -> str:
    return _envelope_json("u1-b", "user-1")


def test_decode_frame_roundtrips_base64_and_passes_raw_frames_on() -> None:
    payload = _envelope_json("evt-1", "user-9")
    assert decode_frame(_frame("user-9", _b64(payload))) == payload
    assert decode_frame("content:log noise without frames") is None
    assert decode_frame(_frame("user-9", "{not base64")) == "{not base64"


def test_receiver_drains_topic_backlogs_exactly_once() -> None:
    calls: list[list[str]] = []
    puts: list[str] = []
    client = _fake_admin(topics=[_TOPIC], backlogs={_TOPIC: 2}, puts=puts)
    stdout = _consumed_stdout(
        [("user-9", _b64(_envelope_json("u9-a", "user-9"))), ("user-1", _b64(_second_user_event()))]
    )
    receiver = PulsarCliReceiver(Settings(), runner=_fake_runner(stdout, calls), client=client)

    frames = receiver.receive()

    # bounded, no-guess consume; creation runs the one-time earliest reset
    consume = _consume_calls(calls)
    assert consume == [
        [
            "/pulsar/bin/pulsar-client",
            "consume",
            "-s",
            _SUBSCRIPTION,
            "-n",
            "2",
            _TOPIC,
        ]
    ]
    _reset_call(calls)  # exactly one, with creation
    assert puts
    assert frames == [
        (_TOPIC, _envelope_json("u9-a", "user-9")),
        (_TOPIC, _second_user_event()),
    ]


def test_receiver_units_bounded_passes_and_skips_zero_backlog() -> None:
    other = "gmp.signals.other"
    calls: list[list[str]] = []
    client = _fake_admin(topics=[_TOPIC, other], backlogs={_TOPIC: 5, other: 0}, puts=[])
    stdout = _consumed_stdout(
        [("user-9", _b64(_envelope_json("u9-a", "user-9"))), ("user-1", _b64(_second_user_event()))]
    )
    receiver = PulsarCliReceiver(
        Settings(), runner=_fake_runner(stdout, calls), client=client, max_messages=2
    )
    frames = receiver.receive()

    consume = _consume_calls(calls)
    assert len(consume) == 1, "zero-backlog topics are never consumed"
    assert consume[0][5] == "2", "the -n cap is min(backlog, max_messages)"
    assert frames == [
        (_TOPIC, _envelope_json("u9-a", "user-9")),
        (_TOPIC, _second_user_event()),
    ]


def test_drain_chain_delivers_envelopes_and_poisons_junk() -> None:
    """Full composition: receiver -> adapter -> BrokerIngestor -> ledger.

    Two valid events plus one retry duplicate and one undecodable frame: the
    ingest accepts the two, absorbs the duplicate, and the junk arrives raw
    at the adjudication - poisoned, never silently dropped.
    """
    calls: list[list[str]] = []
    client = _fake_admin(topics=[_TOPIC], backlogs={_TOPIC: 4}, puts=[])
    stdout = _consumed_stdout(
        [
            ("user-9", _b64(_envelope_json("u9-a", "user-9"))),
            ("user-9", _b64(_envelope_json("u9-b", "user-9"))),
            ("user-9", _b64(_envelope_json("u9-a", "user-9"))),  # duplicate retry
        ],
        "{not base64 and not json",  # raw junk; ingest owns the poison
    )
    receiver = PulsarCliReceiver(Settings(), runner=_fake_runner(stdout, calls), client=client)
    ledger = InMemoryLedger()
    poison = _PoisonRecorder()

    outcome = asyncio.run(PulsarIngester(receiver, BrokerIngestor(ledger, poison=poison)).drain())

    result = outcome[str(_TENANT)].result
    assert result.accepted == 2
    assert result.duplicates == 1
    assert len(outcome[str(_TENANT)].rejected) == 1
    assert outcome[str(_TENANT)].rejected[0].error["error"] == "envelope_invalid"
    assert poison.calls and poison.calls[0][2] == "envelope_invalid"


def test_drain_chain_keeps_per_user_order_through_the_broker() -> None:
    calls: list[list[str]] = []
    client = _fake_admin(topics=[_TOPIC], backlogs={_TOPIC: 3}, puts=[])
    stdout = _consumed_stdout(
        [
            ("user-9", _b64(_envelope_json("u9-a", "user-9"))),
            ("user-9", _b64(_envelope_json("u9-b", "user-9"))),
            ("user-1", _b64(_second_user_event())),
        ]
    )
    receiver = PulsarCliReceiver(Settings(), runner=_fake_runner(stdout, calls), client=client)
    ledger = InMemoryLedger()

    outcome = asyncio.run(PulsarIngester(receiver, BrokerIngestor(ledger)).drain())

    assert outcome[str(_TENANT)].result.accepted == 3
    envelopes = [record.envelope for record in ledger.read_events(_TENANT)]
    # the kit's mandatory rule, expressed for these fixtures: per-user delivery
    # sequence equals the broker frame order (duplicates stay absorbed)
    for user, expected_ids in (
        ("user-9", ["u9-a", "u9-b"]),
        ("user-1", ["u1-b"]),
    ):
        delivered = [
            str(envelope.partitionkey)
            for envelope in envelopes
            if str(envelope.partitionkey) == user
        ]
        identity: list[str] = [
            str(envelope.id) for envelope in envelopes if str(envelope.partitionkey) == user
        ]
        assert delivered  # every user's frames survived
        assert identity == expected_ids
