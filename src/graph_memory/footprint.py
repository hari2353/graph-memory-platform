"""Operational footprint for graph-memory queries (Harvard Edge cross-pollination).

Cross-pollinated from harvard-edge/TinyML-Footprint: report how much data a
memory query moves and what it costs to run, the same way a model datasheet
reports its footprint. Counts and byte estimates are deterministic; energy
figures depend on explicit power/PUE assumptions.
"""

from __future__ import annotations

JOULES_PER_KWH = 3_600_000.0


def estimate_index_bytes(
    entities: int = 0,
    episodes: int = 0,
    observations: int = 0,
    edges: int = 0,
) -> int:
    """Order-of-magnitude byte estimate for a memory graph's stored state."""
    for value, label in (
        (entities, "entities"), (episodes, "episodes"),
        (observations, "observations"), (edges, "edges"),
    ):
        if value < 0:
            raise ValueError(f"{label} must be >= 0")
    return entities * 120 + episodes * 240 + observations * 96 + edges * 40


def estimate_query_footprint(
    rows: int,
    *,
    bytes_per_row: int = 256,
    cpu_seconds: float = 0.0,
    power_w: float = 0.0,
    pue: float = 1.15,
    carbon_intensity_g_per_kwh: float = 400.0,
) -> dict:
    """Footprint of one memory query: data moved plus compute energy."""
    if rows < 0:
        raise ValueError("rows must be >= 0")
    if bytes_per_row < 0:
        raise ValueError("bytes_per_row must be >= 0")
    if cpu_seconds < 0 or power_w < 0:
        raise ValueError("cpu_seconds and power_w must be >= 0")
    if pue < 1.0:
        raise ValueError("pue must be >= 1.0")
    if carbon_intensity_g_per_kwh < 0:
        raise ValueError("carbon intensity must be >= 0")

    data_bytes = rows * bytes_per_row
    kwh = (cpu_seconds * power_w * pue) / (1000 * 3600) if power_w else 0.0
    kg = kwh * carbon_intensity_g_per_kwh / 1000
    return {
        "rows": rows,
        "bytes_per_row": bytes_per_row,
        "data_bytes": data_bytes,
        "cpu_seconds": round(cpu_seconds, 4),
        "kwh": round(kwh, 6),
        "kgco2e": round(kg, 6),
    }
