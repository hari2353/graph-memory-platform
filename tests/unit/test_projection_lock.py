"""Projection writer lock: ``reflect`` and serve's replay must not overlap.

Both passes mutate the same published tables (serve stages a copy and swaps;
reflect clears and rewrites derived kinds). Before the lock, a tick landing
inside the copy/swap window lost its write until the next tick. These tests
pin that every writer pass takes the same cross-process lock, and that the
kernel releases it on every exit path.
"""

from __future__ import annotations

import fcntl
import threading
from datetime import UTC, datetime
from pathlib import Path

import pytest

from graph_memory.core.clock import FixedClock
from graph_memory.mapping.spec import load_spec
from graph_memory.projector.engine import Projector
from graph_memory.projector.gateway import InMemoryGraphGateway
from graph_memory.signals.ledger import InMemoryLedger

_SPEC = Path(__file__).parent.parent / "fixtures" / "mapping_skill_loop.yaml"


class _BlockingGateway(InMemoryGraphGateway):
    """Parks inside the writer pass so the test can probe the lock."""

    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.release = threading.Event()

    def _park(self) -> None:
        self.entered.set()
        assert self.release.wait(5)

    def clear_kind(self, kind: str) -> None:
        self._park()
        super().clear_kind(kind)

    def replace_all(self) -> None:
        self._park()
        super().replace_all()


def _held(path: Path) -> bool:
    """True when another open file description owns the exclusive lock."""
    with path.open("a+") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(handle, fcntl.LOCK_UN)
        return False


@pytest.mark.parametrize("action", ["reflect", "replay"])
def test_writer_passes_hold_the_cross_process_lock(tmp_path: Path, action: str) -> None:
    lock_path = tmp_path / "projection.lock"
    gateway = _BlockingGateway()
    engine = Projector(
        load_spec(_SPEC.read_text(encoding="utf-8")),
        InMemoryLedger(),
        gateway,
        FixedClock(datetime(2026, 6, 10, tzinfo=UTC)),
        lock_path=lock_path,
    )
    worker = threading.Thread(target=getattr(engine, action))
    worker.start()
    try:
        assert gateway.entered.wait(5)
        assert _held(lock_path), f"{action} wrote without holding the lock"
    finally:
        gateway.release.set()
        worker.join(5)
    assert not worker.is_alive()
    assert not _held(lock_path), f"{action} leaked the lock"
