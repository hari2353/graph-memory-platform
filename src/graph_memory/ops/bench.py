"""Hot-path SLA bench (SPEC §7): context get() latency vs the p99 budget.

The CI gate: on the synthetic profile (users × skill observations wired
through the REAL provider — engine compile → snapshot → single-flight
cache), ``p99`` must stay under the budget (default ``1s``). Regression =
red build. Runs on the in-memory tier by construction — it measures the hot
path's algorithmic latency, not a network round-trip.

The payload-token gate (M4.6, research 06 lever 5): Mem0's 6.7–7.0K tokens
per retrieved context is the industry's uncontrolled figure; our bounded
``top_k`` payloads must stay under the proposal budget (default ~2K
tokens) — measured on the same synthetic profile through the real
provider, with the documented 4-chars-per-token estimate (no tokenizer on
the hot path; the estimate is the falsifiable proxy the gate asserts).
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass

from graph_memory.context.service import ContextCache, MemoryContextProvider
from graph_memory.core.types import Capability, Principal, Scope, TenantId, UserRef, user_key
from graph_memory.mapping.spec import MappingSpec, load_spec
from graph_memory.projector.engine import Projector
from graph_memory.projector.gateway import InMemoryGraphGateway
from graph_memory.signals.envelope import Envelope
from graph_memory.signals.ledger import InMemoryLedger, record_from_envelope

_PROFILE_SPEC = """\
mapping_version: 1
signals:
  itf.skill_assessed.v1:
    edges:
      - kind: HAS_SKILL
        target: "skill:{data.skill}"
        weight: 1.0
        half_life_days: 180
capabilities:
  skills:
    relations: [HAS_SKILL]
    top_k: 5
    ttl_ms: 60000
"""


def parse_budget(raw: str) -> float:
    """``1s`` / ``500ms`` / ``0.75`` (seconds) → milliseconds."""
    text = raw.strip().casefold()
    if text.endswith("ms"):
        return float(text[:-2])
    if text.endswith("s"):
        return float(text[:-1]) * 1000.0
    return float(text) * 1000.0


@dataclass(frozen=True)
class BenchReport:
    """The gate's outcome: percentiles, budget, verdict."""

    requests: int
    users: int
    p50_ms: float
    p95_ms: float
    p99_ms: float
    budget_ms: float

    @property
    def passed(self) -> bool:
        return self.p99_ms <= self.budget_ms

    def render(self) -> str:
        return (
            f"HOTPATH bench: requests={self.requests} users={self.users} "
            f"p50={self.p50_ms:.2f}ms p95={self.p95_ms:.2f}ms "
            f"p99={self.p99_ms:.2f}ms budget={self.budget_ms:.0f}ms "
            f"-> {'PASSED' if self.passed else 'FAILED'}"
        )


def build_hotpath_profile(users: int, *, skills_per_user: int = 3) -> MemoryContextProvider:
    """Synthetic profile: every user holds ``skills_per_user`` observations."""
    spec: MappingSpec = load_spec(_PROFILE_SPEC)
    ledger = InMemoryLedger()
    gateway = InMemoryGraphGateway()
    from datetime import UTC, datetime

    from graph_memory.core.clock import SystemClock

    engine = Projector(spec, ledger, gateway, SystemClock())
    for index in range(users * skills_per_user):
        user = f"user-{index % users:04d}"
        skill = f"skill-{index // users:03d}"
        payload: dict[str, object] = {
            "specversion": "1.0",
            "id": f"bench-{index}",
            "source": "https://bench.example.com/signals",
            "type": "itf.skill_assessed.v1",
            "time": f"{datetime.now(tz=UTC).isoformat()}",
            "partitionkey": user,
            "data": {"skill": skill},
        }
        ledger.append([record_from_envelope(TenantId("bench"), Envelope.parse(payload))])
    engine.process_events(ledger.read_events(TenantId("bench")))
    return MemoryContextProvider(engine, ContextCache())


def _percentile(sorted_ms: list[float], pct: float) -> float:
    """Nearest-rank percentile over an ascending sample."""
    if not sorted_ms:
        return 0.0
    rank = min(len(sorted_ms) - 1, max(0, int(round(pct / 100 * len(sorted_ms)))))
    return sorted_ms[rank]


def estimate_tokens(text: str) -> int:
    """The documented token estimate: ~4 characters per token (no tokenizer).

    Cheap and order-stable; the gate asserts against this proxy, never
    against a model's true count — the honest unit is "estimated tokens".
    """
    return max(1, (len(text) + 3) // 4)


@dataclass(frozen=True)
class PayloadBudgetReport:
    """The token-budget gate's outcome: worst payload vs the proposal cap."""

    samples: int
    max_tokens: int
    budget_tokens: float

    @property
    def passed(self) -> bool:
        return self.max_tokens <= self.budget_tokens

    def render(self) -> str:
        return (
            f"PAYLOAD bench: samples={self.samples} max_tokens={self.max_tokens} "
            f"budget={self.budget_tokens:.0f} "
            f"-> {'PASSED' if self.passed else 'FAILED'}"
        )


async def run_payload_budget_bench(
    *,
    users: int = 30,
    budget_tokens: float = 2000.0,
) -> PayloadBudgetReport:
    """Assert every served context payload stays under the token budget."""
    provider = await asyncio.to_thread(build_hotpath_profile, users)

    def serve_all() -> list[int]:
        async def collect() -> list[int]:
            tokens: list[int] = []
            for index in range(users):
                ref = UserRef(
                    tenant=_PRINCIPAL.tenant,
                    user_key=user_key(f"user-{index % users:04d}"),
                )
                ctx = await provider.get(_PRINCIPAL, ref, Capability("skills"))
                text = json.dumps(ctx.payload, sort_keys=True)
                tokens.append(estimate_tokens(text))
            return tokens

        return asyncio.run(collect())

    tokens = await asyncio.to_thread(serve_all)
    return PayloadBudgetReport(
        samples=len(tokens),
        max_tokens=max(tokens),
        budget_tokens=budget_tokens,
    )


_PRINCIPAL = Principal(
    tenant=TenantId("bench"),
    scopes=frozenset({Scope.CONTEXT_READ, Scope.MEMORY_SEARCH}),
    subject="bench:profile",
)


async def run_hotpath_bench(
    *,
    users: int = 30,
    requests: int = 1000,
    budget_ms: float = 1000.0,
    concurrency: int = 8,
) -> BenchReport:
    """Fire ``requests`` concurrent gets per lane across ``users``; report p99."""
    provider = await asyncio.to_thread(build_hotpath_profile, users)

    async def one(request_index: int) -> float:
        ref = UserRef(
            tenant=_PRINCIPAL.tenant,
            user_key=user_key(f"user-{(request_index * 13) % users:04d}"),
        )
        start = time.perf_counter_ns()
        await provider.get(_PRINCIPAL, ref, Capability("skills"))
        return (time.perf_counter_ns() - start) / 1_000_000.0

    async def lane() -> list[float]:
        return [await one(index) for index in range(requests)]

    streams: list[list[float]] = await asyncio.gather(*(lane() for _ in range(concurrency)))
    durations = sorted(duration for stream in streams for duration in stream)
    return BenchReport(
        requests=requests * concurrency,
        users=users,
        p50_ms=_percentile(durations, 50),
        p95_ms=_percentile(durations, 95),
        p99_ms=_percentile(durations, 99),
        budget_ms=budget_ms,
    )
