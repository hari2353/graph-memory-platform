"""End-to-end live path: ingest -> serve projection -> reflect -> both channels.

This is the operator journey the pitch claims, against the real tier:

1. boot ``platform serve`` (real ClickHouse, REST + MCP + ingest);
2. POST signals over HTTP for two users sharing two skills;
3. wait until the projection loop serves ``skills``;
4. stop serve and run ``platform reflect`` (the scheduled derivation tick);
5. boot serve again and read ``community`` over REST and MCP;
6. wait through two replay ticks and read again — a rebuild must preserve
   reflection-owned rows, otherwise communities vanish every second.

The tenant is unique per run via ``GMP_DEV_STATIC_TOKENS`` so no prior data
can satisfy the assertions. Skipped when ClickHouse is unreachable.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import httpx
import pytest

_SPEC = Path(__file__).parent / ".." / "fixtures" / "mapping_community.yaml"
_INGEST_PORT = 8141
_REST_PORT = 8142
_MCP_PORT = 8143
_RUN = uuid.uuid4().hex[:8]
_TOKEN = "e2e-token"
_TENANT = f"e2e-{_RUN}"
_USER_1 = f"e2e-{_RUN}-u1"
_USER_2 = f"e2e-{_RUN}-u2"
_HEADERS = {"Authorization": f"Bearer {_TOKEN}"}
_ENV = {
    **os.environ,
    "GMP_DEV_STATIC_TOKENS": json.dumps({_TOKEN: _TENANT}),
}


def _serve() -> subprocess.Popen[str]:
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "graph_memory.ops.cli",
            "serve",
            "--mapping",
            str(_SPEC),
            "--ingest-port",
            str(_INGEST_PORT),
            "--rest-port",
            str(_REST_PORT),
            "--mcp-port",
            str(_MCP_PORT),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=Path(__file__).parent.parent.parent,
        env=_ENV,
    )
    assert proc.stdout is not None
    deadline = time.monotonic() + 90
    collected: list[str] = []
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            break
        line = proc.stdout.readline()
        if not line:
            time.sleep(0.1)
            continue
        collected.append(line)
        if "serving:" in line:
            return proc
    proc.terminate()
    raise AssertionError(f"serve never started:\n{''.join(collected)[-2000:]}")


def _stop(proc: subprocess.Popen[str]) -> None:
    proc.terminate()
    proc.wait(timeout=15)


def _signal(event_id: str, user: str, skill: str) -> dict[str, object]:
    return {
        "specversion": "1.0",
        "id": event_id,
        "source": "https://e2e.example.com/signals",
        "type": "itf.skill_assessed.v1",
        "time": "2026-06-01T12:00:00.000Z",
        "partitionkey": user,
        "data": {"skill": skill},
    }


def _wait_skills() -> None:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        response = httpx.get(
            f"http://127.0.0.1:{_REST_PORT}/v1/context/{_USER_1}/skills",
            headers=_HEADERS,
            timeout=5,
        )
        if response.status_code == 200:
            items = response.json()["payload"]["items"]
            if len(items) == 2:
                return
        time.sleep(0.25)
    raise AssertionError("projection never served both skills")


def _int_field(pattern: str, text: str) -> int:
    match = re.search(pattern, text)
    assert match is not None, f"{pattern!r} missing from {text!r}"
    return int(match.group(1))


def _community_rest() -> list[dict[str, Any]]:
    response = httpx.get(
        f"http://127.0.0.1:{_REST_PORT}/v1/context/{_USER_1}/community",
        headers=_HEADERS,
        timeout=5,
    )
    assert response.status_code == 200, response.text
    items = response.json()["payload"]["items"]
    assert isinstance(items, list)
    return items


def _community_mcp() -> list[dict[str, Any]]:
    import httpx2
    from mcp.client.session import ClientSession
    from mcp.client.streamable_http import streamable_http_client

    async def call() -> list[dict[str, Any]]:
        async with (
            httpx2.AsyncClient(headers=_HEADERS, timeout=10) as client,
            streamable_http_client(f"http://127.0.0.1:{_MCP_PORT}/mcp", http_client=client) as (
                read,
                write,
            ),
            ClientSession(read, write) as session,
        ):
            await session.initialize()
            result = await session.call_tool(
                "get_context", {"user": _USER_1, "capability": "community"}
            )
            assert not result.is_error, result
            body = result.structured_content
            assert body is not None
            payload = body["payload"]
            assert isinstance(payload, dict)
            items = payload["items"]
            assert isinstance(items, list)
            return items

    return asyncio.run(call())


def _assert_community(items: list[dict[str, Any]]) -> None:
    kinds = {(item["kind"], item["target"]) for item in items}
    membership = {(kind, target) for kind, target in kinds if kind == "MEMBER_OF"}
    assert len(membership) == 1
    community_id = next(iter(membership))[1]
    assert community_id.startswith("community:")
    shared = {
        target: score
        for kind, target, score in ((item["kind"], item["target"], item["score"]) for item in items)
        if kind == "COMMUNITY_HAS_SKILL"
    }
    assert shared == {
        "skill:python": pytest.approx(2.0),
        "skill:rust": pytest.approx(2.0),
    }


@pytest.mark.live
def test_ingest_reflect_community_over_both_channels() -> None:
    try:
        with httpx.Client(timeout=2) as probe:
            probe.get(f"http://127.0.0.1:{_REST_PORT}/healthz")
    except Exception:  # noqa: BLE001
        pass
    else:
        pytest.skip(f"ports {_INGEST_PORT}-{_MCP_PORT} already in use")

    serve = _serve()
    try:
        signals = [
            _signal(f"e2e-{_RUN}-1", _USER_1, "rust"),
            _signal(f"e2e-{_RUN}-2", _USER_1, "python"),
            _signal(f"e2e-{_RUN}-3", _USER_2, "rust"),
            _signal(f"e2e-{_RUN}-4", _USER_2, "python"),
        ]
        posted = httpx.post(
            f"http://127.0.0.1:{_INGEST_PORT}/v1/signals:batch",
            json={"signals": signals},
            headers=_HEADERS,
            timeout=10,
        )
        assert posted.status_code == 200, posted.text
        assert posted.json()["accepted"] == 4
        _wait_skills()

        # Reflect while serve is LIVE: both writers take the projection lock,
        # so the tick cannot land inside a replay's copy/swap window.
        reflected = subprocess.run(
            [
                sys.executable,
                "-m",
                "graph_memory.ops.cli",
                "reflect",
                "--mapping",
                str(_SPEC),
            ],
            env=_ENV,
            cwd=Path(__file__).parent.parent.parent,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=60,
        )
        assert reflected.returncode == 0, reflected.stdout + reflected.stderr
        communities = _int_field(r"communities=(\d+)", reflected.stdout)
        edges = _int_field(r"community_edges=(\d+)", reflected.stdout)
        assert communities >= 1
        assert edges >= 6

        # The still-running serve must serve the tick's rows without a restart.
        _assert_community(_community_rest())
    finally:
        _stop(serve)

    # Restart proves the rows are durable, not just cached in the live process.
    serve = _serve()
    try:
        rest_items = _community_rest()
        assert rest_items, f"tenant={_TENANT} user={_USER_1} reflect={reflected.stdout.strip()!r}"
        _assert_community(rest_items)
        mcp_items = _community_mcp()
        _assert_community(mcp_items)
        assert {(i["kind"], i["target"]) for i in rest_items} == {
            (i["kind"], i["target"]) for i in mcp_items
        }

        # Serve replays every second: reflection rows must survive the swap.
        time.sleep(2.5)
        _assert_community(_community_rest())
    finally:
        _stop(serve)
