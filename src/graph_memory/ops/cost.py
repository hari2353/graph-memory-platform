"""Cost gate (SPEC §12/R5, §16.6): the recomputed §12 estimate in CI.

Method: measure the synthetic fixture's record bytes (the §12 profile shape,
~1 KB each), recompute the monthly compressed terabytes at target load with
the documented codec ratio, and assert the §12 reference (1.07 TB/mo,
12.8 TB at 12 months) within ±15%. Storage levers are schema properties
(TTL/types/codecs) — the estimate drifts only when the schema drifts.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from graph_memory.core.types import TenantId
from graph_memory.signals.envelope import Envelope
from graph_memory.signals.ledger import LedgerRecord, record_from_envelope

# §12's derivation: 1.07 TB/mo compressed at ratio ≈10 (ZSTD-3 columnar).
_CODEC_RATIO = 10.0
_REFERENCE_MONTHLY_TB = 1.07
_REFERENCE_RETENTION_TB = 12.8
_TOLERANCE = 0.15  # §16.6: recompute within ±15%
_SECONDS_PER_DAY = 86_400
_PROFILE_BYTES = 1_024  # "~1 KB each" (§12)


def profile_record(index: int) -> LedgerRecord:
    """One §12-shaped event: source+id+data fields, padded to ~1 KB."""
    data: dict[str, object] = {"skill": "python", "score": 0.8, "blob": "x"}
    template: dict[str, object] = {
        "specversion": "1.0",
        "id": f"cost-{index}",
        "source": "https://app.example.com/signals",
        "type": "itf.skill_assessed.v1",
        "time": f"2026-06-01T12:00:00.{index % 1000:03d}Z",
        "partitionkey": "cost-user",
        "data": data,
    }
    envelope = Envelope.parse(template)
    # Pad so the on-wire JSON lands at the documented ~1 KB event size.
    padding = _PROFILE_BYTES - len(json.dumps(envelope.model_dump(mode="json")))
    assert padding > 0, "profile envelope already exceeds the 1 KB target"
    template["data"] = {**data, "blob": "x" * padding}
    return record_from_envelope(TenantId("cost"), Envelope.parse(template))


def measured_record_bytes(sample: int = 25) -> float:
    """Mean uncompressed record bytes over the synthetic profile."""
    sizes = [
        len(json.dumps(profile_record(index).envelope.model_dump(mode="json")))
        for index in range(sample)
    ]
    return sum(sizes) / len(sizes)


@dataclass(frozen=True)
class CostReport:
    """The §12 recompute from measured bytes + documented codec ratio."""

    events_per_day: int
    record_bytes: float
    codec_ratio: float
    months_retained: int
    monthly_compressed_tb: float
    retention_compressed_tb: float

    def render(self) -> str:
        return (
            f"COST bench: {self.events_per_day:,} events/day x {self.record_bytes:.0f}B "
            f"/ codec {self.codec_ratio:.0f} -> {self.monthly_compressed_tb:.3f} TB/mo "
            f"(ref 1.07 +/-15%) | {self.months_retained}-mo retention "
            f"{self.retention_compressed_tb:.2f} TB (ref 12.8)"
        )


def recomputed_report(
    events_per_day: int,
    months_retained: int,
    *,
    record_bytes: float | None = None,
    codec_ratio: float = _CODEC_RATIO,
) -> CostReport:
    """Recompute §12 from the SYNTHETIC fixture's measured bytes."""
    bytes_per_event = record_bytes if record_bytes is not None else measured_record_bytes()
    raw_bytes_month = events_per_day * bytes_per_event * 30.0
    monthly_tb = raw_bytes_month / codec_ratio / 2**40
    return CostReport(
        events_per_day=events_per_day,
        record_bytes=bytes_per_event,
        codec_ratio=codec_ratio,
        months_retained=months_retained,
        monthly_compressed_tb=monthly_tb,
        retention_compressed_tb=monthly_tb * months_retained,
    )


def within_budget(report: CostReport) -> bool:
    """§16.6 gate: monthly within ±15% of 1.07; retention ≤ 12.8 + 15%."""
    monthly_delta = abs(report.monthly_compressed_tb - _REFERENCE_MONTHLY_TB)
    retention_ceiling = _REFERENCE_RETENTION_TB * (1.0 + _TOLERANCE)
    return (
        monthly_delta <= _REFERENCE_MONTHLY_TB * _TOLERANCE
        and report.retention_compressed_tb <= retention_ceiling
    )
