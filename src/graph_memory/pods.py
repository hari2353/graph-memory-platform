"""Pod assignment (cross-pollinated from Shopify pods architecture).

Assigns tenants to isolated pods (database instances) with a capacity cap,
deterministically, so a multi-tenant platform can shard by pod without a
central router. Pure Python.
"""

from __future__ import annotations


def assign_pods(tenants: list[str], pod_capacity: int) -> dict[str, object]:
    if pod_capacity < 1:
        raise ValueError("pod_capacity must be >= 1")
    if not isinstance(tenants, list):
        raise TypeError("tenants must be a list")
    assignment: dict[str, int] = {}
    pod_loads: list[int] = [0]
    for tenant in tenants:
        if not isinstance(tenant, str) or not tenant.strip():
            raise ValueError("tenant must be a non-empty str")
        # find the least-loaded pod with capacity
        candidates = [i for i, load in enumerate(pod_loads) if load < pod_capacity]
        if not candidates:
            pod_loads.append(0)
            candidates = [len(pod_loads) - 1]
        # deterministic: smallest index among least-loaded
        target = min(candidates, key=lambda i: (pod_loads[i], i))
        assignment[tenant] = target
        pod_loads[target] += 1
    return {
        "assignment": assignment,
        "pod_count": len(pod_loads),
        "pod_loads": pod_loads,
    }
