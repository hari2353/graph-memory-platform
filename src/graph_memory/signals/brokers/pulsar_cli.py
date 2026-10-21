"""Pulsar dev drain receiver: admin REST + the broker image's own CLI.

Deployment-edge driver for the whole-loop posture (``ops/compose/pulsar.yaml``
+ ``platform drain-brokers``). The duck-typed receiver the M2 ``Pulsar adapter
expects — real broker traffic, no python client library dependency.

Wire contract (earned against ``apachepulsar/pulsar:3.3.5`` standalone):

- Admin REST creates the durable drain subscription (``PUT .../subscription/``
  → 204; 409 when it already exists) and reports per-subscription backlogs
  (``stats.subscriptions[name].msgBacklog``). REST-created subscriptions start
  at the tail, so creation also runs ``pulsar-admin topics reset-cursor -m
  earliest``: messages published before the first drain still count into the
  backlog. Existing subscriptions are never reset (durable forward-only).
- The CLI's ``consume`` BLOCKS until exactly ``-n`` messages arrive, so the
  receiver consumes exactly the current backlog each pass (0 → no call):
  bounded, never hanging, and forward-only across passes.
- The CLI's text framing splits ``-m`` values on commas and reprints content
  without quoting — both destroy JSON. Producers therefore publish
  ``base64(envelope JSON)`` — pure alphanumeric text that survives both — and
  this receiver decodes before delivering, so the ingest service still sees
  raw envelope payloads (idempotency unchanged: dedup happens post-decode).
- Frames that don't decode arrive raw and get poisoned downstream
  (``envelope_invalid`` → DLQ), keeping the kit's poison-capture rule.
"""

from __future__ import annotations

import base64
import re
import subprocess
import time
from collections.abc import Callable, Mapping, Sequence

import httpx

from graph_memory.settings import Settings

_FRAME_CONTENT = re.compile(r"content:(.+)$")
_SUB_CREATE_OK = (204, 409)

Runner = Callable[[Sequence[str]], tuple[int, str, str]]

_CLIENT_BIN = "/pulsar/bin/pulsar-client"
_ADMIN_BIN = "/pulsar/bin/pulsar-admin"


def docker_exec_runner(container: str, timeout: float = 300.0) -> Runner:
    """Default runner: the dev broker's packaged CLIs via ``docker exec``.

    ``args`` is the full command inside the container (binary + arguments):
    the drain runs both ``pulsar-client`` (consume) and ``pulsar-admin``
    (cursor reset on subscription creation).
    """

    def run(args: Sequence[str]) -> tuple[int, str, str]:
        completed = subprocess.run(  # noqa: PLW1510 — the configured timeout IS the bound
            ["docker", "exec", container, *args],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return completed.returncode, completed.stdout, completed.stderr

    return run


def decode_frame(line: str) -> str | None:
    """One consumed frame line → payload str, or ``None`` for CLI log noise.

    Content is base64(envelope). Undecodable frames are returned raw so the
    ingest adjudication — not this driver — owns the rejection (poison).
    """
    if "key:[" not in line or "content:" not in line:
        return None
    encoded = _FRAME_CONTENT.search(line)
    if encoded is None:
        return None
    text = encoded.group(1).strip()
    try:
        return base64.b64decode(text, validate=True).decode("utf-8")
    except (ValueError, UnicodeDecodeError):
        return text  # not base64 → let the ingest service poison it


class PulsarCliReceiver:
    """``PulsarReceiver`` over the dev broker: topics, backlog, exact consume."""

    def __init__(
        self,
        settings: Settings,
        *,
        runner: Runner | None = None,
        client: httpx.Client | None = None,
        max_messages: int | None = None,
    ) -> None:
        self._admin_url = settings.pulsar_admin_url.rstrip("/")
        self._namespace = settings.pulsar_namespace
        self._drain = settings.pulsar_drain_subscription
        self._runner = runner or docker_exec_runner(settings.pulsar_container)
        self._client = client or httpx.Client(timeout=15.0)
        self._max = max_messages

    def topics(self) -> list[str]:
        """Persistent topics in the dev namespace, filtered to ``gmp.signals.``."""
        response = self._client.get(f"{self._admin_url}/admin/v2/persistent/{self._namespace}")
        response.raise_for_status()
        listing = response.json()
        found: list[str] = []
        if isinstance(listing, list):
            for item in listing:
                if isinstance(item, str):
                    name = item.rsplit("/", 1)[-1]
                    if name.startswith("gmp.signals."):
                        found.append(name)
        found.sort()
        return found

    def backlog(self, topic: str) -> int | None:
        """Pending messages for the drain subscription, or ``None`` if absent."""
        response = self._client.get(
            f"{self._admin_url}/admin/v2/persistent/{self._namespace}/{topic}/stats"
        )
        response.raise_for_status()
        stats = response.json()
        if not isinstance(stats, dict):
            return None
        subscriptions = stats.get("subscriptions")
        if not isinstance(subscriptions, dict):
            return None
        entry = subscriptions.get(self._drain)
        if not isinstance(entry, Mapping):
            return None
        return int(entry.get("msgBacklog", 0) or 0)

    def ensure_subscription(self, topic: str) -> None:
        """Create the durable drain subscription, positioned at the retained start.

        Publishes can precede the first drain, so on creation this also runs
        ``pulsar-admin topics reset-cursor -m earliest``: the backlog then
        counts every retained message, not only ones published after the
        subscription appeared. Existing subscriptions are never reset
        (durable forward-only cursors stay at-least-once per broker contract).
        """
        if self.backlog(topic) is not None:
            return
        for _ in range(5):
            created = self._client.put(
                f"{self._admin_url}/admin/v2/persistent/"
                f"{self._namespace}/{topic}/subscription/{self._drain}"
            )
            if created.status_code not in _SUB_CREATE_OK:
                created.raise_for_status()
            if self.backlog(topic) is not None:
                if created.status_code == 204:  # ours → position at retained start
                    self._reset_cursor_to_earliest(topic)
                return
            time.sleep(0.1)
        raise RuntimeError(f"drain subscription never appeared for topic {topic}")

    def _reset_cursor_to_earliest(self, topic: str) -> None:
        returncode, _stdout, stderr = self._runner(
            [
                _ADMIN_BIN,
                "topics",
                "reset-cursor",
                "-s",
                self._drain,
                "-m",
                "earliest",
                topic,
            ]
        )
        if returncode != 0:
            raise RuntimeError(
                f"pulsar-admin reset-cursor failed ({returncode}): {stderr.strip()[:300]}"
            )

    def receive(self) -> list[tuple[str, str]]:
        """The adapter protocol method: one bounded, backlog-driven pass."""
        frames: list[tuple[str, str]] = []
        taken = 0
        for topic in self.topics():
            self.ensure_subscription(topic)
            backlog = self.backlog(topic) or 0
            pending = backlog if self._max is None else min(backlog, self._max - taken)
            if pending > 0:
                frames.extend(self._consume(topic, pending))
                taken += pending
            if self._max is not None and taken >= self._max:
                break
        return frames

    def _consume(self, topic: str, count: int) -> list[tuple[str, str]]:
        returncode, stdout, stderr = self._runner(
            [_CLIENT_BIN, "consume", "-s", self._drain, "-n", str(count), topic]
        )
        if returncode != 0:
            raise RuntimeError(f"pulsar CLI consume failed ({returncode}): {stderr.strip()[:300]}")
        return [
            (topic, payload)
            for line in stdout.splitlines()
            if (payload := decode_frame(line)) is not None
        ]
