from graph_memory.traversal import bfs, reachable, shortest_path


def test_bfs_layers():
    adj = {"a": ["b", "c"], "b": ["d"], "c": [], "d": []}
    out = bfs(adj, "a", max_depth=2)
    assert [n for n, _ in out] == ["a", "b", "c", "d"]


def test_bfs_respects_max_depth():
    adj = {"a": ["b"], "b": ["c"], "c": []}
    out = bfs(adj, "a", max_depth=1)
    assert [n for n, _ in out] == ["a", "b"]


def test_shortest_path_found():
    adj = {"a": ["b", "c"], "b": ["d"], "c": ["d"], "d": []}
    assert shortest_path(adj, "a", "d") == ["a", "b", "d"]


def test_shortest_path_unreachable():
    adj = {"a": ["b"], "b": [], "c": []}
    assert shortest_path(adj, "a", "c") is None


def test_shortest_path_same_node():
    adj = {"a": ["b"]}
    assert shortest_path(adj, "a", "a") == ["a"]


def test_reachable_set():
    adj = {"a": ["b", "c"], "b": ["d"], "c": [], "d": []}
    assert reachable(adj, "a", max_depth=1) == {"a", "b", "c"}


def test_invalid_inputs():
    try:
        bfs("nope", "a")
    except TypeError:
        pass
    else:
        raise AssertionError("non-dict accepted")
    try:
        bfs({"a": []}, "a", max_depth=-1)
    except ValueError:
        pass
    else:
        raise AssertionError("negative depth accepted")
    try:
        shortest_path({"a": "b"}, "a", "b")
    except TypeError:
        pass
    else:
        raise AssertionError("bad neighbors accepted")
