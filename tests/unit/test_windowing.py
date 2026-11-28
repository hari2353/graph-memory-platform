from graph_memory.windowing import sliding_window_count, tumbling_windows


def test_tumbling_windows_basic():
    events = [{"ts": 1}, {"ts": 2}, {"ts": 10}, {"ts": 11}]
    windows = tumbling_windows(events, 5)
    assert len(windows) == 2
    assert len(windows[0]) == 2
    assert len(windows[1]) == 2


def test_tumbling_empty():
    assert tumbling_windows([], 5) == []


def test_tumbling_invalid():
    try:
        tumbling_windows([{"ts": 1}], 0)
    except ValueError:
        pass
    else:
        raise AssertionError("zero window accepted")


def test_sliding_window_count():
    events = [{"ts": 1}, {"ts": 2}, {"ts": 3}, {"ts": 10}]
    assert sliding_window_count(events, 3) == 3


def test_sliding_empty():
    assert sliding_window_count([], 5) == 0


def test_sliding_invalid():
    try:
        sliding_window_count([{"ts": 1}], 0)
    except ValueError:
        pass
    else:
        raise AssertionError("zero window accepted")
