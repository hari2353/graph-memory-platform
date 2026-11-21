from graph_memory.footprint import estimate_index_bytes, estimate_query_footprint


def test_index_bytes_zero():
    assert estimate_index_bytes() == 0


def test_index_bytes_scales():
    assert estimate_index_bytes(entities=10, episodes=5) > 0


def test_index_bytes_rejects_negative():
    for kwargs in ({"entities": -1}, {"episodes": -1}, {"observations": -1}, {"edges": -1}):
        try:
            estimate_index_bytes(**kwargs)
        except ValueError:
            continue
        raise AssertionError(f"accepted invalid input: {kwargs}")


def test_query_footprint_data_bytes():
    out = estimate_query_footprint(100, bytes_per_row=256)
    assert out["data_bytes"] == 25600
    assert out["rows"] == 100


def test_query_footprint_energy_math():
    out = estimate_query_footprint(10, cpu_seconds=3600, power_w=100, pue=1.0, carbon_intensity_g_per_kwh=400)
    assert out["kwh"] == 0.1
    assert out["kgco2e"] == 0.04


def test_query_footprint_rejects_bad_inputs():
    for kwargs in (
        {"rows": -1, "bytes_per_row": 256},
        {"rows": 10, "bytes_per_row": -1},
        {"rows": 10, "cpu_seconds": -1, "power_w": 10},
        {"rows": 10, "power_w": -1, "cpu_seconds": 1},
        {"rows": 10, "pue": 0.9},
        {"rows": 10, "carbon_intensity_g_per_kwh": -1},
    ):
        try:
            estimate_query_footprint(**kwargs)
        except ValueError:
            continue
        raise AssertionError(f"accepted invalid input: {kwargs}")
