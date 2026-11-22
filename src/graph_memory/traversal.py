"""Multi-hop traversal over a projected graph (cross-pollinated from RISELab).

Pure functions over an adjacency mapping ``{node: [neighbor, ...]}``. The
platform projects a memory graph into ClickHouse; these helpers let a caller
walk it (BFS layers, shortest path) without a graph database round-trip.
"""

from __future__ import annotations

from collections import deque


def _validate_adj(adj) -> None:
    if not isinstance(adj, dict):
        raise TypeError("adj must be a dict")
    for node, neighbors in adj.items():
        if not isinstance(neighbors, (list, tuple, set)):
            raise TypeError(f"neighbors of {node!r} must be a list/tuple/set")


def bfs(adj: dict, start, *, max_depth: int = 3) -> list[tuple]:
    """Breadth-first traversal returning (node, depth) pairs in visit order."""
    _validate_adj(adj)
    if max_depth < 0:
        raise ValueError("max_depth must be >= 0")
    visited = {start}
    queue: deque = deque([(start, 0)])
    result: list[tuple] = []
    while queue:
        node, depth = queue.popleft()
        result.append((node, depth))
        if depth >= max_depth:
            continue
        for neighbor in adj.get(node, ()):
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append((neighbor, depth + 1))
    return result


def shortest_path(adj: dict, src, dst) -> list | None:
    """Return the shortest node list from src to dst, or None if unreachable."""
    _validate_adj(adj)
    if src == dst:
        return [src]
    visited = {src}
    queue: deque = deque([(src, [src])])
    while queue:
        node, path = queue.popleft()
        for neighbor in adj.get(node, ()):
            if neighbor == dst:
                return path + [neighbor]
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append((neighbor, path + [neighbor]))
    return None


def reachable(adj: dict, start, *, max_depth: int = 3) -> set:
    """Set of nodes reachable from start within max_depth hops (inclusive)."""
    return {node for node, _depth in bfs(adj, start, max_depth=max_depth)}
