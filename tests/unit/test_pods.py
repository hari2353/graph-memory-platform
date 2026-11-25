from graph_memory.pods import assign_pods


def test_assigns_all_tenants() -> None:
    out = assign_pods(["a", "b", "c"], 2)
    assert set(out["assignment"]) == {"a", "b", "c"}  # type: ignore
    assert all(load <= 2 for load in out["pod_loads"])  # type: ignore


def test_respects_capacity() -> None:
    out = assign_pods(["a", "b", "c", "d"], 2)
    assert out["pod_count"] == 2
    assert out["pod_loads"] == [2, 2]


def test_deterministic() -> None:
    a = assign_pods(["x", "y", "z"], 2)
    b = assign_pods(["x", "y", "z"], 2)
    assert a == b


def test_empty() -> None:
    out = assign_pods([], 2)
    assert out["assignment"] == {}
    assert out["pod_count"] == 1


def test_invalid() -> None:
    try:
        assign_pods(["a"], 0)
    except ValueError:
        pass
    else:
        raise AssertionError("zero capacity accepted")
    try:
        assign_pods("nope", 2)  # type: ignore
    except TypeError:
        pass
    else:
        raise AssertionError("non-list accepted")
    try:
        assign_pods([""], 2)
    except ValueError:
        pass
    else:
        raise AssertionError("empty tenant accepted")
