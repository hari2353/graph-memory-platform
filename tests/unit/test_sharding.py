from graph_memory.sharding import balance_shards, shard_for


def test_shard_for_deterministic():
    assert shard_for("tenant-a", 4) == shard_for("tenant-a", 4)


def test_shard_for_range():
    for key in ("a", "b", "c", "tenant-1", "tenant-2"):
        assert 0 <= shard_for(key, 4) < 4


def test_balance_counts_sum():
    keys = ["t1", "t2", "t3", "t4", "t5"]
    out = balance_shards(keys, 3)
    assert sum(out["counts"]) == 5
    assert out["total_keys"] == 5


def test_balance_empty():
    out = balance_shards([], 3)
    assert out["counts"] == [0, 0, 0]
    assert out["total_keys"] == 0


def test_invalid():
    try:
        shard_for("", 4)
    except ValueError:
        pass
    else:
        raise AssertionError("empty key accepted")
    try:
        shard_for("a", 0)
    except ValueError:
        pass
    else:
        raise AssertionError("zero shards accepted")
    try:
        balance_shards(["a"], 0)
    except ValueError:
        pass
    else:
        raise AssertionError("zero shards accepted")
