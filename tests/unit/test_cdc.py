from graph_memory.cdc import CdcCursor, changed_since


def test_cursor_advance_and_reset() -> None:
    c = CdcCursor("events", 0)
    assert c.advance(5) == 5
    c.reset(2)
    assert c.position == 2
    assert c.to_dict() == {"stream": "events", "position": 2}


def test_changed_since_filters() -> None:
    records = [{"seq": 1, "v": "a"}, {"seq": 2, "v": "b"}, {"seq": 3, "v": "c"}]
    new, wm = changed_since(records, 1)
    assert [r["v"] for r in new] == ["b", "c"]
    assert wm == 3


def test_changed_since_none_new() -> None:
    records = [{"seq": 1, "v": "a"}]
    new, wm = changed_since(records, 5)
    assert new == []
    assert wm == 5


def test_invalid() -> None:
    try:
        CdcCursor("  ")
    except ValueError:
        pass
    else:
        raise AssertionError("blank stream accepted")
    try:
        CdcCursor("s", position=-1)
    except ValueError:
        pass
    else:
        raise AssertionError("negative position accepted")
    c = CdcCursor("s")
    try:
        c.advance(-1)
    except ValueError:
        pass
    else:
        raise AssertionError("negative advance accepted")
    try:
        changed_since([], -1)
    except ValueError:
        pass
    else:
        raise AssertionError("negative watermark accepted")
