"""`platform serve` smoke test (live tier): all three channels boot and serve.

Marks: live — needs the real ClickHouse at the Settings endpoints. The
command replays the ledger at boot, then serves ingest, REST, and MCP from
three uvicorn threads; we drive each once and tear the process down.
"""

from __future__ import annotations

import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx
import pytest

_SPEC = Path(__file__).parent.parent / "fixtures" / "mapping_skill_loop.yaml"
_INGEST_PORT = 8101
_REST_PORT = 8102
_MCP_PORT = 8103
_HEADERS = {"Authorization": "Bearer dev-acme-token"}


def _wait_output(proc: subprocess.Popen[str], needle: str, timeout_s: float) -> str:
    """Drain stdout until `needle` appears (or fail loudly)."""
    assert proc.stdout is not None  # Popen(text=True) wires the pipe
    deadline = time.monotonic() + timeout_s
    collected: list[str] = []
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            break
        line = proc.stdout.readline()
        if not line:
            time.sleep(0.1)
            continue
        collected.append(line)
        if needle in line:
            return "".join(collected)
    raise AssertionError(f"serve never reported {needle!r}:\n{''.join(collected)[-2000:]}")


@pytest.mark.live
def test_serve_boots_ingest_rest_and_mcp() -> None:
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
    )
    try:
        _wait_output(proc, "serving:", timeout_s=90)

        ingest = httpx.get(f"http://127.0.0.1:{_INGEST_PORT}/healthz", timeout=5)
        assert ingest.status_code == 200

        payload = {
            "specversion": "1.0",
            "id": f"serve-smoke-{uuid.uuid4()}",
            "source": "https://serve.example.com/signals",
            "type": "itf.skill_assessed.v1",
            "time": "2026-06-01T12:00:00.123Z",
            "partitionkey": "user-serve",
            "data": {"skill": "rust"},
        }
        posted = httpx.post(
            f"http://127.0.0.1:{_INGEST_PORT}/v1/signals:batch",
            json={"signals": [payload]},
            headers=_HEADERS,
            timeout=10,
        )
        assert posted.status_code == 200
        assert posted.json()["accepted"] == 1

        read = httpx.get(
            f"http://127.0.0.1:{_REST_PORT}/v1/context/user-serve/skills",
            headers=_HEADERS,
            timeout=10,
        )
        assert read.status_code == 200
        assert read.json()["payload"]["items"]  # the signal reached the graph

        init = httpx.post(
            f"http://127.0.0.1:{_MCP_PORT}/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2026-07-28",
                    "capabilities": {},
                    "clientInfo": {"name": "smoke", "version": "0"},
                },
            },
            headers={**_HEADERS, "Accept": "application/json"},
            timeout=10,
        )
        assert init.status_code == 200
        assert "serverInfo" in init.text
    finally:
        proc.terminate()
        proc.wait(timeout=15)
