"""Cross-process lock for the projection writer.

Serve replays every second; ``platform reflect`` rewrites the same derived
kinds. Both mutate the published tables (replay stages a copy then swaps,
reflect clears then writes), so one pass landing inside the other's window
can drop rows until the next tick. Every writer pass takes this lock.

``flock`` is released by the kernel when the process dies, and two processes
on the same host see the same file. ``shortcut:`` the lock is host-local —
put ``GMP_PROJECTION_LOCK`` on a shared volume if serve and reflect ever run
on different hosts.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows; the live tier is POSIX-only
    fcntl = None  # type: ignore[assignment]


@contextmanager
def projection_lock(path: Path | None) -> Iterator[None]:
    """Hold the exclusive projection lock; ``None`` disables (in-memory only)."""
    if path is None:
        yield
        return
    if fcntl is None:  # pragma: no cover - Windows
        raise RuntimeError("the projection lock needs POSIX fcntl; run serve and reflect on POSIX")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)
