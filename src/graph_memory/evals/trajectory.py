"""Trajectory eval gate: grade the journey, not just one snapshot.

A case is a deterministic ledger plus an ordered script of reads. The gate
replays the ledger once, applies optional reflection between steps, records
the top-k targets each step served, and compares them with a checked-in
golden. Two checks run: the per-step targets (the first divergence names the
step) and a canonical SHA-256 digest (metadata drift also fails). The CLI
exits 1 on either, so a projection or ranking change cannot land silently.

No LLM and no wall clock: the fixture's ``now`` is the injected instant.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from pathlib import Path

from graph_memory.context.service import parse_as_of
from graph_memory.core.clock import Clock
from graph_memory.core.types import Json, TenantId, UserKey
from graph_memory.evals.harness import _top_targets
from graph_memory.mapping.spec import MappingSpec, load_spec
from graph_memory.projector.engine import Projector
from graph_memory.projector.gateway import InMemoryGraphGateway
from graph_memory.signals.envelope import Envelope
from graph_memory.signals.ledger import InMemoryLedger, LedgerRepository, record_from_envelope

_MAX_K = 100


@dataclass(frozen=True)
class TrajectoryStep:
    """One read in the journey; ``reflect`` runs the reflection tick first."""

    capability: str
    k: int = 3
    as_of: datetime | None = None
    reflect: bool = False


@dataclass(frozen=True)
class StepTrace:
    """What one step actually served."""

    index: int
    capability: str
    k: int
    as_of: str | None
    targets: tuple[str, ...]

    def to_payload(self) -> dict[str, Json]:
        return {
            "index": self.index,
            "capability": self.capability,
            "k": self.k,
            "as_of": self.as_of,
            "targets": list(self.targets),
        }


@dataclass(frozen=True)
class TrajectoryTrace:
    """The full journey plus its canonical digest."""

    case: str
    tenant: TenantId
    user: UserKey
    steps: tuple[StepTrace, ...]

    def to_payload(self) -> dict[str, Json]:
        return {
            "case": self.case,
            "tenant": str(self.tenant),
            "user": str(self.user),
            "steps": [step.to_payload() for step in self.steps],
        }

    @property
    def digest(self) -> str:
        canonical = json.dumps(self.to_payload(), sort_keys=True, separators=(",", ":"))
        return sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class StepDelta:
    """One step's expected-vs-actual targets."""

    index: int
    capability: str
    expected: tuple[str, ...]
    actual: tuple[str, ...]

    @property
    def diverged(self) -> bool:
        return self.expected != self.actual


@dataclass(frozen=True)
class GoldenCheck:
    """The gate's verdict: digest plus per-step diffs."""

    expected_digest: str | None
    actual_digest: str
    digest_matches: bool
    deltas: tuple[StepDelta, ...]

    @property
    def passed(self) -> bool:
        return self.digest_matches and not self.deltas


@dataclass(frozen=True)
class TrajectoryFixture:
    """A parsed case: spec, ledger, injected clock instant and the script."""

    case: str
    spec: MappingSpec
    ledger: InMemoryLedger
    tenant: TenantId
    user: UserKey
    now: datetime
    steps: tuple[TrajectoryStep, ...]


def run_trajectory(
    spec: MappingSpec,
    ledger: LedgerRepository,
    clock: Clock,
    *,
    case: str,
    tenant: TenantId,
    user: UserKey,
    steps: Sequence[TrajectoryStep],
) -> TrajectoryTrace:
    """Replay once, then execute the step script in order."""
    if not steps:
        raise ValueError("a trajectory needs at least one step")
    engine = Projector(spec, ledger, InMemoryGraphGateway(), clock)
    engine.replay()
    traces: list[StepTrace] = []
    for index, step in enumerate(steps):
        if not step.capability:
            raise ValueError(f"step {index} has no capability")
        bounded_k = max(1, min(step.k, _MAX_K))
        if step.reflect:
            engine.reflect()
        targets = _top_targets(engine, tenant, user, step.capability, bounded_k, as_of=step.as_of)
        traces.append(
            StepTrace(
                index=index,
                capability=step.capability,
                k=bounded_k,
                as_of=None if step.as_of is None else step.as_of.isoformat(),
                targets=targets,
            )
        )
    return TrajectoryTrace(case=case, tenant=tenant, user=user, steps=tuple(traces))


def golden_payload(trace: TrajectoryTrace) -> dict[str, Json]:
    """The checked-in form: digest + per-step targets (no volatile fields)."""
    return {
        "digest": trace.digest,
        "steps": [
            {"capability": step.capability, "targets": list(step.targets)} for step in trace.steps
        ],
    }


def check_golden(trace: TrajectoryTrace, golden: Mapping[str, Json]) -> GoldenCheck:
    """Compare a trace with a golden payload; malformed goldens are errors."""
    raw_steps = golden.get("steps")
    if not isinstance(raw_steps, list):
        raise ValueError("golden must carry a steps list")
    if len(raw_steps) != len(trace.steps):
        raise ValueError(f"golden has {len(raw_steps)} steps, trace has {len(trace.steps)}")
    deltas: list[StepDelta] = []
    for step, raw in zip(trace.steps, raw_steps, strict=True):
        if not isinstance(raw, dict):
            raise ValueError(f"golden step {step.index} is not an object")
        raw_targets = raw.get("targets")
        if not isinstance(raw_targets, list) or not all(isinstance(t, str) for t in raw_targets):
            raise ValueError(f"golden step {step.index} needs a string targets list")
        expected = tuple(str(target) for target in raw_targets)
        if expected != step.targets:
            deltas.append(
                StepDelta(
                    index=step.index,
                    capability=step.capability,
                    expected=expected,
                    actual=step.targets,
                )
            )
    expected_digest = golden.get("digest")
    if expected_digest is not None and not isinstance(expected_digest, str):
        raise ValueError("golden digest must be a string")
    return GoldenCheck(
        expected_digest=expected_digest,
        actual_digest=trace.digest,
        digest_matches=expected_digest is None or expected_digest == trace.digest,
        deltas=tuple(deltas),
    )


def load_case(path: Path) -> TrajectoryFixture:
    """Parse one JSON case; the mapping path is relative to the case file."""
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("trajectory case must be a JSON object")
    mapping_name = raw.get("mapping", "mapping.yaml")
    if not isinstance(mapping_name, str):
        raise ValueError("mapping must be a path string")
    spec = load_spec((path.parent / mapping_name).read_text(encoding="utf-8"))
    tenant = TenantId(_required_text(raw, "tenant"))
    user = UserKey(_required_text(raw, "user"))
    now = parse_as_of(_required_text(raw, "now"))
    if now is None:
        raise ValueError("now is required")
    events = raw.get("events")
    if not isinstance(events, list):
        raise ValueError("events must be a list")
    ledger = InMemoryLedger()
    for index, event in enumerate(events):
        if not isinstance(event, dict):
            raise ValueError(f"event {index} is not an object")
        ledger.append([record_from_envelope(tenant, Envelope.parse(event))])
    raw_steps = raw.get("steps")
    if not isinstance(raw_steps, list) or not raw_steps:
        raise ValueError("steps must be a non-empty list")
    steps: list[TrajectoryStep] = []
    for index, step in enumerate(raw_steps):
        if not isinstance(step, dict):
            raise ValueError(f"step {index} is not an object")
        capability = step.get("capability")
        if not isinstance(capability, str) or not capability:
            raise ValueError(f"step {index} needs a capability")
        k = step.get("k", 3)
        if not isinstance(k, int) or isinstance(k, bool):
            raise ValueError(f"step {index} k must be an integer")
        reflect = step.get("reflect", False)
        if not isinstance(reflect, bool):
            raise ValueError(f"step {index} reflect must be a boolean")
        as_of_raw = step.get("as_of")
        if as_of_raw is not None and not isinstance(as_of_raw, str):
            raise ValueError(f"step {index} as_of must be an ISO string")
        steps.append(
            TrajectoryStep(
                capability=capability,
                k=k,
                as_of=parse_as_of(as_of_raw),
                reflect=reflect,
            )
        )
    return TrajectoryFixture(
        case=str(raw.get("name", path.stem)),
        spec=spec,
        ledger=ledger,
        tenant=tenant,
        user=user,
        now=now,
        steps=tuple(steps),
    )


def _required_text(data: Mapping[str, object], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"case needs a non-empty {key!r}")
    return value
