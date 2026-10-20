"""SDK-thin client (SPEC §9): raw HTTP over the platform's own channels.

A thin client on purpose — every method is a plain HTTP call the CLI could
make by hand; no retry policies, no caching, no fallback semantics. The
complexity budget stays in one place (the platform) instead of being
duplicated per language.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import Any

import httpx


class GraphMemoryClient:
    """Thin HTTPS facade: ingest + context reads (+ prefetch/idempotency)."""

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        timeout_s: float = 2.0,
        ingest_base_url: str | None = None,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._ingest_base = (ingest_base_url or base_url).rstrip("/")
        self._token = token
        self._http = httpx.Client(timeout=timeout_s)

    def ingest(self, signals: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        """``POST /v1/signals:batch`` (the ingest edge)."""
        response = self._http.post(
            f"{self._ingest_base}/v1/signals:batch",
            json={"signals": list(signals)},
            headers=self._auth(),
        )
        response.raise_for_status()
        data: dict[str, Any] = response.json()
        return data

    def context(self, user: str, capability: str) -> dict[str, Any]:
        """``GET /v1/context/{user}/{capability}`` — the full snapshot body."""
        response = self._http.get(
            f"{self._base}/v1/context/{user}/{capability}",
            headers=self._auth(),
        )
        response.raise_for_status()
        data: dict[str, Any] = response.json()
        return data

    def iter_items(
        self, user: str, capability: str, *, page_size: int = 20
    ) -> Iterator[dict[str, Any]]:
        """Cursor-walk ``GET .../items`` until ``next_cursor`` is null."""
        cursor: str | None = None
        while True:
            params: dict[str, Any] = {"limit": page_size}
            if cursor is not None:
                params["cursor"] = cursor
            response = self._http.get(
                f"{self._base}/v1/context/{user}/{capability}/items",
                headers=self._auth(),
                params=params,
            )
            response.raise_for_status()
            body: dict[str, Any] = response.json()
            yield from body["items"]
            cursor = body["next_cursor"]
            if cursor is None:
                return

    def prefetch(
        self, user: str, capability: str, *, idempotency_key: str | None = None
    ) -> dict[str, Any]:
        """``POST .../prefetch``; ``Idempotency-Key`` replays the response."""
        headers = self._auth()
        if idempotency_key is not None:
            headers["Idempotency-Key"] = idempotency_key
        response = self._http.post(
            f"{self._base}/v1/context/{user}/{capability}/prefetch",
            headers=headers,
        )
        response.raise_for_status()
        data: dict[str, Any] = response.json()
        return data

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> GraphMemoryClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _auth(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"}
