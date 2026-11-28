from graph_memory.consumer import IdempotentConsumer


def test_processes_unique_events():
    c = IdempotentConsumer()
    seen = []
    assert c.handle({"id": "1"}, lambda e: seen.append(e["id"])) is True
    assert c.handle({"id": "2"}, lambda e: seen.append(e["id"])) is True
    assert seen == ["1", "2"]
    assert c.processed_count() == 2


def test_dedups_duplicate_events():
    c = IdempotentConsumer()
    calls = []
    c.handle({"id": "1"}, lambda e: calls.append(1))
    assert c.handle({"id": "1"}, lambda e: calls.append(1)) is False
    assert calls == [1]
    assert c.processed_count() == 1


def test_requires_event_id():
    c = IdempotentConsumer()
    try:
        c.handle({}, lambda e: None)
    except ValueError:
        pass
    else:
        raise AssertionError("missing id accepted")
