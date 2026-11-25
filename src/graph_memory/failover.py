"""Replication failover decision (cross-pollinated from GitHub MySQL HA).

Given primary/replica lag measurements, decide whether promoting a replica is
safe. Pure function, no DB dependency.
"""

from __future__ import annotations


def should_failover(
    *,
    primary_lag_s: float,
    replica_lag_s: float,
    max_lag_s: float = 30.0,
    replica_healthy: bool = True,
) -> dict:
    if primary_lag_s < 0 or replica_lag_s < 0 or max_lag_s < 0:
        raise ValueError("lag values must be >= 0")
    reasons: list[str] = []
    safe = True
    if not replica_healthy:
        safe = False
        reasons.append("replica unhealthy")
    if replica_lag_s > max_lag_s:
        safe = False
        reasons.append(f"replica lag {replica_lag_s}s exceeds {max_lag_s}s")
    if primary_lag_s <= max_lag_s and replica_lag_s > primary_lag_s + max_lag_s:
        safe = False
        reasons.append("replica too far behind primary")
    return {"failover": safe, "reasons": reasons}
