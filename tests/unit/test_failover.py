from graph_memory.failover import should_failover


def test_failover_safe():
    out = should_failover(primary_lag_s=5, replica_lag_s=2, max_lag_s=30)
    assert out["failover"] is True
    assert out["reasons"] == []


def test_failover_blocked_unhealthy_replica():
    out = should_failover(primary_lag_s=5, replica_lag_s=2, replica_healthy=False)
    assert out["failover"] is False
    assert "replica unhealthy" in out["reasons"]


def test_failover_blocked_high_replica_lag():
    out = should_failover(primary_lag_s=5, replica_lag_s=60, max_lag_s=30)
    assert out["failover"] is False
    assert any("exceeds" in r for r in out["reasons"])


def test_failover_blocked_replica_too_far_behind():
    out = should_failover(primary_lag_s=1, replica_lag_s=100, max_lag_s=30)
    assert out["failover"] is False


def test_invalid_inputs():
    for kwargs in (
        {"primary_lag_s": -1, "replica_lag_s": 1, "max_lag_s": 30},
        {"primary_lag_s": 1, "replica_lag_s": -1, "max_lag_s": 30},
        {"primary_lag_s": 1, "replica_lag_s": 1, "max_lag_s": -1},
    ):
        try:
            should_failover(**kwargs)
        except ValueError:
            continue
        raise AssertionError(f"accepted invalid input: {kwargs}")
