"""Webhook delivery: HMAC-signed payloads, bounded retry ladder, auto-disable.

Semantics (SPEC §9 webhook channel):

- one serialization per event — signature binds the EXACT bytes every attempt
  sends (no re-serialization drift);
- retry ladder ``0, 5, 25, 125, 600`` seconds (attempt indexes 0..4), each
  attempt recorded with its HTTP status (``"error"`` for transport failures);
- exhausting the ladder auto-disables the endpoint (a fail-signal honored by
  later dispatches until an operator re-enables it);
- 2xx = delivered. Anything else retries; the ladder is bounded on purpose.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Protocol

import httpx

from graph_memory.channels.webhooks import signing
from graph_memory.core.clock import Clock
from graph_memory.core.types import Json

_LONGEST_RETRY_S = 600.0


@dataclass
class WebhookEndpoint:
    id: str
    url: str
    secret: str
    max_attempts: int = 5
    enabled: bool = True


@dataclass(frozen=True)
class DeliveryReport:
    endpoint_id: str
    delivered: bool
    attempts: int
    statuses: tuple[str, ...]
    disabled: bool


class Sleeper(Protocol):
    """Injectable delay so tests observe the ladder without waiting."""

    async def __call__(self, seconds: float) -> None: ...


@dataclass
class WebhookRegistry:
    """Endpoints in memory; persistence is an adapter's M3 concern."""

    endpoints: dict[str, WebhookEndpoint] = field(default_factory=dict)

    def add(self, endpoint: WebhookEndpoint) -> None:
        self.endpoints[endpoint.id] = endpoint

    def get(self, endpoint_id: str) -> WebhookEndpoint | None:
        return self.endpoints.get(endpoint_id)

    def list(self) -> list[WebhookEndpoint]:
        return sorted(self.endpoints.values(), key=lambda e: e.id)


class WebhookChannel:
    """Bound delivery loop; sync IO is injected as an ``httpx.AsyncClient``."""

    def __init__(
        self,
        registry: WebhookRegistry,
        client: httpx.AsyncClient,
        *,
        clock: Clock,
        sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._registry = registry
        self._client = client
        self._clock = clock
        self._sleeper = sleeper

    def register(self, endpoint: WebhookEndpoint) -> None:
        self._registry.add(endpoint)

    async def dispatch(
        self, event: Mapping[str, Json], *, endpoint_id: str | None = None
    ) -> list[DeliveryReport]:
        targets = (
            [e for e in self._registry.list() if e.enabled]
            if endpoint_id is None
            else [e for e in self._registry.list() if e.id == endpoint_id and e.enabled]
        )
        body = json.dumps(event, sort_keys=True).encode("utf-8")
        timestamp = str(int(self._clock.now().timestamp()))
        return [
            report
            for report in await asyncio.gather(
                *(self._deliver(e, body, timestamp) for e in targets)
            )
        ]

    def _signature_headers(
        self, endpoint: WebhookEndpoint, body: bytes, timestamp: str
    ) -> dict[str, str]:
        # The signature binds THE EXACT bytes; re-signing per attempt is a no-op
        # with a frozen clock but stays correct under live clocks too.
        return {
            signing.SIGNATURE_HEADER: signing.signature(endpoint.secret, body, timestamp),
            signing.TIMESTAMP_HEADER: timestamp,
            "Content-Type": "application/json",
        }

    async def _deliver(
        self, endpoint: WebhookEndpoint, body: bytes, timestamp: str
    ) -> DeliveryReport:
        headers = self._signature_headers(endpoint, body, timestamp)
        statuses: list[str] = []
        delivered = False
        for attempt in range(max(1, endpoint.max_attempts)):
            success, record = await self._attempt(endpoint.url, body, headers)
            statuses.append(record)
            if success:
                delivered = True
                break
            if attempt + 1 < endpoint.max_attempts:
                await self._sleeper(_retry_delay_s(attempt))
        disabled = False
        if not delivered and endpoint.enabled:
            endpoint.enabled = False  # fatigue: stop hammering a dead receiver
            disabled = True
        return DeliveryReport(
            endpoint_id=endpoint.id,
            delivered=delivered,
            attempts=len(statuses),
            statuses=tuple(statuses),
            disabled=disabled,
        )

    async def _attempt(self, url: str, body: bytes, headers: dict[str, str]) -> tuple[bool, str]:
        try:
            response = await self._client.post(url, content=body, headers=headers)
        except httpx.HTTPError:
            return False, "error"
        return 200 <= response.status_code < 300, str(response.status_code)


def _retry_delay_s(attempt_index: int) -> float:
    """0→first retry is 5s, then 25, 125, 600 (capped) — the SPEC ladder."""
    return min(_LONGEST_RETRY_S, 5.0 * (5.0**attempt_index))
