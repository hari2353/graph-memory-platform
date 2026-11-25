"""Shard planning (cross-pollinated from Notion/Pinterest/Shopify).

Consistent hashing for shard assignment plus a balance report, so a
multi-tenant graph store can be partitioned deterministically and audited
for skew. Pure Python, no DB dependency.
"""

from __future__ import annotations

import hashlib


def shard_for(key: str, num_shards: int) -> int:
    if num_shards < 1:
        raise ValueError("num_shards must be >= 1")
    if not isinstance(key, str) or not key.strip():
        raise ValueError("key must be a non-empty str")
    digest = hashlib.md5(key.encode("utf-8")).hexdigest()
    return int(digest, 16) % num_shards


def balance_shards(keys: list[str], num_shards: int) -> dict:
    if num_shards < 1:
        raise ValueError("num_shards must be >= 1")
    counts = [0] * num_shards
    for key in keys:
        counts[shard_for(key, num_shards)] += 1
    total = len(keys)
    max_count = max(counts) if counts else 0
    min_count = min(counts) if counts else 0
    return {
        "num_shards": num_shards,
        "total_keys": total,
        "counts": counts,
        "max": max_count,
        "min": min_count,
        "skew": round((max_count - min_count) / max(1, total / num_shards), 4),
    }
