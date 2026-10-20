"""The thread-local client policy (same thread ⇒ same session, never shared)."""

from __future__ import annotations

import threading

from graph_memory.chclient import bind_thread_local
from graph_memory.settings import Settings


def test_same_thread_reuses_its_lazy_client() -> None:
    class _FakeClient:
        pass

    factory = bind_thread_local(lambda _settings: _FakeClient())
    settings = Settings()
    first = factory(settings)
    second = factory(settings)
    assert first is second  # one lazy build, reused for the thread's lifetime


def test_threads_never_share_one_session() -> None:
    class _FakeClient:
        pass

    factory = bind_thread_local(lambda _settings: _FakeClient())
    settings = Settings()
    seen: dict[str, object] = {}

    def grab(name: str) -> None:
        seen[name] = factory(settings)

    a = threading.Thread(target=grab, args=("a",))
    b = threading.Thread(target=grab, args=("b",))
    a.start()
    b.start()
    a.join()
    b.join()
    assert seen["a"] is not seen["b"]  # the concurrency invariant from CH docs
