"""M1 live-tier gate: real CH + real ClickGraph server (``-m live`` only).

What it proves, together:

1. The ClickHouse gateway converges with the in-memory gateway through full
   projectors over the same event set (cross-backend zero-diff), and the
   tenant predicate keeps tenants isolated server-side.
2. ClickGraph — booted against the local ClickHouse — SERVES the same graph:
   a cypher read over the mapping of ``gm_nodes``/``gm_edges`` matches the
   projected state exactly.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.request
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest

from graph_memory.core.clock import FixedClock
from graph_memory.core.types import Capability, TenantId, UserKey
from graph_memory.mapping.spec import MappingSpec, load_spec
from graph_memory.projector.engine import Projector
from graph_memory.projector.gateway import GraphGateway, InMemoryGraphGateway
from graph_memory.projector.gateway_clickhouse import ClickHouseGateway, default_client
from graph_memory.settings import Settings
from graph_memory.signals.envelope import Envelope
from graph_memory.signals.ledger import InMemoryLedger, record_from_envelope

_T = TenantId("acme")
_SPEC_PATH = Path(__file__).parent.parent / "fixtures" / "mapping_skill_loop.yaml"
_CG_BINARY = Path(r"F:\cg\target\debug\clickgraph.exe")
_CG_PORT = 7478
_USERS_SKILLS = [
    ("user-1", "rust"),
    ("user-1", "go"),
    ("user-2", "rust"),
    ("user-2", "go"),
    ("user-3", "python"),
]


def _envelope(event_id: str, user: str, skill: str, *, ms: int) -> Envelope:
    payload: dict[str, object] = {
        "specversion": "1.0",
        "id": event_id,
        "source": "https://live.example.com/signals",
        "type": "itf.skill_assessed.v1",
        "time": f"2026-06-01T12:00:00.{ms:03d}Z",
        "partitionkey": user,
        "data": {"skill": skill},
    }
    return Envelope.parse(payload)


def _seed_and_project(engine: Projector, ledger: InMemoryLedger) -> None:
    for index, (user, skill) in enumerate(_USERS_SKILLS):
        ledger.append(
            [record_from_envelope(_T, _envelope(f"live-{index}", user, skill, ms=100 + index))]
        )
    engine.process_events(ledger.read_events(_T))


def _observation_tuples(
    gateway: GraphGateway, tenant: TenantId, user: UserKey
) -> set[tuple[object, ...]]:
    return {
        (row.kind, row.target, round(row.weight, 6), row.valid_at.isoformat())
        for row in gateway.observations(tenant, user)
    }


def _ch_online(settings: Settings) -> bool:
    try:
        default_client(settings).command("SELECT 1")
    except Exception:  # noqa: BLE001 — any failure means "not available"
        return False
    return True


@pytest.mark.live
def test_clickhouse_gateway_converges_with_memory_and_isolates_tenants() -> None:
    settings = Settings()
    if not _ch_online(settings):
        pytest.skip("ClickHouse not reachable at the configured endpoint")
    spec: MappingSpec = load_spec(_SPEC_PATH.read_text(encoding="utf-8"))
    clock = FixedClock(datetime(2026, 6, 10, tzinfo=UTC))

    memory_ledger = InMemoryLedger()
    memory_gateway = InMemoryGraphGateway()
    memory_projector = Projector(spec, memory_ledger, memory_gateway, clock)
    _seed_and_project(memory_projector, memory_ledger)
    memory_projector.reflect()

    run = uuid.uuid4().hex[:8]
    ch_gateway = ClickHouseGateway(
        settings, database=f"gmp_live_{run}", nodes_table=f"nodes_{run}", edges_table=f"edges_{run}"
    )
    ch_gateway.ensure_graph_schema()
    try:
        ch_ledger = InMemoryLedger()
        ch_projector = Projector(spec, ch_ledger, ch_gateway, clock)
        _seed_and_project(ch_projector, ch_ledger)
        ch_projector.reflect()

        # Cross-backend zero-diff: observations AND snapshots agree.
        for user, _skill in _USERS_SKILLS:
            ref = UserKey(user)
            assert _observation_tuples(ch_gateway, _T, ref) == _observation_tuples(
                memory_gateway, _T, ref
            )
        ch_doc = ch_projector.snapshot(_T, UserKey("user-1"), Capability("skills"))
        memory_doc = memory_projector.snapshot(_T, UserKey("user-1"), Capability("skills"))
        assert ch_doc.payload == memory_doc.payload

        # Tenant isolation: another tenant's scope observes NOTHING of acme.
        assert ch_gateway.observations(TenantId("other"), UserKey("user-1")) == []

        # Reflection parity: user-3 shares nothing ≥2 → no SIMILAR_TO; 1↔2 yes.
        similar_one = {
            row.target
            for row in ch_gateway.observations(_T, UserKey("user-1"))
            if row.kind == "SIMILAR_TO"
        }
        assert similar_one == {"user-2"}  # out-edge only; the reverse is user-2's
        similar_three = {
            row.target
            for row in ch_gateway.observations(_T, UserKey("user-3"))
            if row.kind == "SIMILAR_TO"
        }
        assert similar_three == set()
    finally:
        ch_gateway.client.command(f"DROP DATABASE IF EXISTS gmp_live_{run}")


def _post(url: str, body: dict[str, object]) -> dict[str, object]:
    request = urllib.request.Request(
        url, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        payload: dict[str, object] = json.loads(response.read().decode())
        return payload


def _cg_up() -> bool:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{_CG_PORT}/health", timeout=5):
            return True
    except Exception:  # noqa: BLE001
        return False


def _start_clickgraph() -> subprocess.Popen[bytes] | None:
    if not _CG_BINARY.exists():
        return None
    settings = Settings()
    env = {
        **os.environ,
        "CLICKHOUSE_URL": f"http://127.0.0.1:{settings.clickhouse_port}",
        "CLICKHOUSE_USER": settings.clickhouse_username,
        "CLICKHOUSE_PASSWORD": settings.clickhouse_password,
        "RUST_LOG": "warn",
    }
    process = subprocess.Popen(
        [
            str(_CG_BINARY),
            "--http-host",
            "127.0.0.1",
            "--http-port",
            str(_CG_PORT),
            "--disable-bolt",
            "--log-level",
            "warn",
        ],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
    )
    for _ in range(50):
        if process.poll() is not None:
            return None
        if _cg_up():
            return process
        time.sleep(0.2)
    process.terminate()
    return None


def _serve_schema_yaml(name: str, database: str, nodes_table: str, edges_table: str) -> str:
    return f"""name: {name}
graph_schema:
  nodes:
    - label: user
      database: {database}
      table: {nodes_table}
      node_id: key
      property_mappings:
        key: key
        name: name
        tenant_id: tenant_id
      filter: "labels[1] = 'user'"
    - label: skill
      database: {database}
      table: {nodes_table}
      node_id: key
      property_mappings:
        key: key
        tenant_id: tenant_id
      filter: "labels[1] = 'skill'"
  edges:
    - type: HAS_SKILL
      database: {database}
      table: {edges_table}
      from_id: user_key
      to_id: target
      from_node: user
      to_node: skill
      property_mappings:
        tenant_id: tenant_id
      filter: "kind = 'HAS_SKILL'"
    - type: SIMILAR_TO
      database: {database}
      table: {edges_table}
      from_id: user_key
      to_id: target
      from_node: user
      to_node: user
      property_mappings:
        tenant_id: tenant_id
      filter: "kind = 'SIMILAR_TO'"
"""


@pytest.mark.live
def test_clickgraph_serves_the_projected_graph() -> None:
    settings = Settings()
    if not _ch_online(settings):
        pytest.skip("ClickHouse not reachable at the configured endpoint")
    if not _CG_BINARY.exists():
        pytest.skip("clickgraph.exe not present")

    spec: MappingSpec = load_spec(_SPEC_PATH.read_text(encoding="utf-8"))
    ledger = InMemoryLedger()
    clock = FixedClock(datetime(2026, 6, 10, tzinfo=UTC))
    gateway = ClickHouseGateway(
        settings, database="gmp_cg_serve", nodes_table="gm_nodes_cg", edges_table="gm_edges_cg"
    )
    gateway.ensure_graph_schema()
    try:
        engine = Projector(spec, ledger, gateway, clock)
        _seed_and_project(engine, ledger)

        process = _start_clickgraph()
        if process is None:
            pytest.skip("clickgraph server could not start")
        try:
            loaded = _post(
                f"http://127.0.0.1:{_CG_PORT}/schemas/load",
                {
                    "schema_name": "gmp_serve_test",
                    "config_content": _serve_schema_yaml(
                        "gmp_serve_test", "gmp_cg_serve", "gm_nodes_cg", "gm_edges_cg"
                    ),
                },
            )
            assert "successfully" in str(loaded.get("message", ""))

            cypher = (
                "MATCH (u:user {tenant_id: 'acme'})-[:HAS_SKILL]->(s:skill) "
                "WHERE u.key = 'user-1' RETURN s.key AS skill"
            )
            result = _post(
                f"http://127.0.0.1:{_CG_PORT}/query",
                {"query": cypher, "schema_name": "gmp_serve_test"},
            )
            rows = cast("list[dict[str, object]]", result.get("results", []))
            skills = {str(row.get("skill")) for row in rows}
            expected = {target for (user, target) in _USERS_SKILLS if user == "user-1"}
            assert skills == {f"skill:{skill}" for skill in expected}
        finally:
            process.terminate()
    finally:
        gateway.client.command("DROP DATABASE IF EXISTS gmp_cg_serve")
