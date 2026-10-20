"""Webhook channel tests: HMAC signature, bounded retry ladder, auto-disable."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

import httpx
import pytest

from graph_memory.channels.webhooks import (
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    WebhookChannel,
    WebhookEndpoint,
    WebhookRegistry,
    signature,
    verify,
)
from graph_memory.core.clock import Clock

_NOW = datetime(2026, 6, 10, 12, 0, tzinfo=UTC)


class RecordingSleeper:
    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)
        return None


class _FixedClock(Clock):
    def now(self) -> datetime:
        return _NOW


def _transport(responder: Callable[[httpx.Request], httpx.Response]) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(responder))


def test_signature_binds_timestamp_and_body() -> None:
    body = b'{"event":"acted_on"}'
    ts = "1782000000"
    sig = signature("whsec_123", body, ts)
    assert sig.startswith("sha256=")
    assert verify("whsec_123", body, provided_signature=sig, provided_timestamp=ts)
    # wrong secret / wrong body / stale timestamp all fail the receiver check
    assert not verify("whsec_other", body, provided_signature=sig, provided_timestamp=ts)
    assert not verify("whsec_123", b"{} ", provided_signature=sig, provided_timestamp=ts)
    assert not verify(
        "whsec_123",
        body,
        provided_signature=sig,
        provided_timestamp=ts,
        age_tolerance_s=1,
        now_epoch_s=1782009999,
    )


async def test_retry_ladder_escalates_and_records_statuses() -> None:
    requests: list[httpx.Request] = []

    def flaky_then_ok(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) < 3:
            return httpx.Response(500)
        return httpx.Response(200)

    sleeper = RecordingSleeper()
    registry = WebhookRegistry()
    channel = WebhookChannel(
        registry,
        _transport(flaky_then_ok),
        clock=_FixedClock(),
        sleeper=sleeper,
    )
    channel.register(WebhookEndpoint(id="wh-1", url="https://hooks.example.com/x", secret="s1"))
    [report] = await channel.dispatch({"event": "acted_on"})
    assert report.delivered and report.attempts == 3
    assert report.statuses == ("500", "500", "200")
    assert not report.disabled
    # Ladder: first retry after 5s, then 25s — capped later at 600s.
    assert sleeper.delays[0] == pytest.approx(5.0)
    assert sleeper.delays[1] == pytest.approx(25.0)
    # Same bytes on every attempt — the signature bound one serialization.
    assert requests[0].content == requests[2].content


async def test_exhaustion_auto_disables_the_endpoint() -> None:
    sleeper = RecordingSleeper()
    registry = WebhookRegistry()
    channel = WebhookChannel(
        registry,
        _transport(lambda request: httpx.Response(500)),
        clock=_FixedClock(),
        sleeper=sleeper,
    )
    channel.register(WebhookEndpoint(id="wh-d", url="https://hooks.example.com/y", secret="s2"))
    [report] = await channel.dispatch({"event": "acted_on"})
    assert not report.delivered and report.disabled
    assert report.statuses == ("500", "500", "500", "500", "500")
    assert channel._registry.get("wh-d") is not None  # noqa: SLF001
    assert channel._registry.get("wh-d").enabled is False  # noqa: SLF001  # type: ignore[union-attr]
    assert sleeper.delays == [
        pytest.approx(5.0),
        pytest.approx(25.0),
        pytest.approx(125.0),
        pytest.approx(600.0),
    ]


async def test_disabled_endpoint_is_skipped_and_can_be_reenabled() -> None:
    async def _no_sleep(_seconds: float) -> None:
        return None

    sent: list[httpx.Request] = []

    def tally(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return httpx.Response(202)

    registry = WebhookRegistry()
    channel = WebhookChannel(registry, _transport(tally), clock=_FixedClock(), sleeper=_no_sleep)
    channel.register(WebhookEndpoint(id="wh-off", url="https://hooks.example.com/off", secret="s3"))
    endpoint = channel._registry.get("wh-off")  # noqa: SLF001
    assert endpoint is not None
    endpoint.enabled = False
    reports = await channel.dispatch({"event": "acted_on"})
    assert reports == []  # endpoint disabled → silently not targeted
    endpoint.enabled = True
    await channel.dispatch({"event": "acted_on"})
    assert len(sent) == 1


async def test_headers_carry_signature_and_timestamp() -> None:
    seen: dict[str, str] = {}

    def capture(request: httpx.Request) -> httpx.Response:
        seen.update(dict(request.headers))
        return httpx.Response(200)

    registry = WebhookRegistry()
    channel = WebhookChannel(
        registry,
        _transport(capture),
        clock=_FixedClock(),
        sleeper=RecordingSleeper(),
    )
    channel.register(WebhookEndpoint(id="wh-sig", url="https://hooks.example.com/z", secret="s4"))
    [report] = await channel.dispatch({"event": "acted_on"})
    assert report.delivered
    assert seen[TIMESTAMP_HEADER.casefold()] == str(int(_NOW.timestamp()))  # httpx lowercases
    assert verify(
        "s4",
        seen_body_of(seen) if seen else b"",
        provided_signature=seen[SIGNATURE_HEADER.casefold()],
        provided_timestamp=seen[TIMESTAMP_HEADER.casefold()],
    )


def seen_body_of(headers: dict[str, str]) -> bytes:
    """The verify above re-checks through the single-event serialization."""
    import json

    return json.dumps({"event": "acted_on"}, sort_keys=True).encode("utf-8")
