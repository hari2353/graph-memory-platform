"""Multi-hop traversal over a projected graph (cross-pollinated from RISELab).

Pure functions over an adjacency mapping ``{node: [neighbor, ...]}``. The
platform projects a memory graph into ClickHouse; these helpers let a caller
walk it (BFS layers, shortest path) without a graph database round-trip.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping
from typing import Any


def _validate_adj(adj: Mapping[Any, Any]) -> None:
    if not isinstance(adj, dict):
        raise TypeError("adj must be a dict")
    for node, neighbors in adj.items():
        if not isinstance(neighbors, (list, tuple, set)):
            raise TypeError(f"neighbors of {node!r} must be a list/tuple/set")


def bfs(adj: Mapping[Any, Any], start: Any, *, max_depth: int = 3) -> list[tuple[Any, int]]:
    """Breadth-first traversal returning (node, depth) pairs in visit order."""
    _validate_adj(adj)
    if max_depth < 0:
        raise ValueError("max_depth must be >= 0")
    visited: set[Any] = {start}
    queue: deque[tuple[Any, int]] = deque([(start, 0)])
    result: list[tuple[Any, int]] = []
    while queue:
        node, depth = queue.popleft()
        result.append((node, depth))
        if depth >= max_depth:
            continue
        neighbors = adj.get(node, ())
        for neighbor in neighbors:
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append((neighbor, depth + 1))
    return result


def shortest_path(adj: Mapping[Any, Any], src: Any, dst: Any) -> list[Any] | None:
    """Return the shortest node list from src to dst, or None if unreachable."""
    _validate_adj(adj)
    if src == dst:
        return [src]
    visited: set[Any] = {src}
    queue: deque[tuple[Any, list[Any]]] = deque([(src, [src])])
    while queue:
        node, path = queue.popleft()
        neighbors = adj.get(node, ())
        for neighbor in neighbors:
            if neighbor == dst:
                return path + [neighbor]
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append((neighbor, path + [neighbor]))
    return None


def reachable(adj: Mapping[Any, Any], start: Any, *, max_depth: int = 3) -> set[Any]:
    """Set of nodes reachable from start within max_depth hops (inclusive)."""
    return {node for node, _depth in bfs(adj, start, max_depth=max_depth)}
