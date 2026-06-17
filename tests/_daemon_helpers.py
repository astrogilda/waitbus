"""Shared daemon-introspection helpers for in-process broadcast tests.

Distinct from ``_wire_helpers`` (client-side wire I/O): these poll the
daemon's in-process state directly, so they are only usable by tests that
hold a live ``Broadcast`` instance.
"""

from __future__ import annotations

import asyncio
import os
import threading
import time
from pathlib import Path

from waitbus import broadcast


async def await_subscribers(daemon: broadcast.Broadcast, *, added: int = 1, timeout: float = 5.0) -> None:
    """Block until ``added`` net new subscribers register with the daemon.

    Captures the current subscriber count at entry and polls until the
    daemon's map grows by ``added``. The snapshot-based contract handles
    back-to-back wait threads in a single test correctly: a prior wait's
    subscriber still in the map when this call enters does NOT satisfy the
    condition, since the new wait must contribute strictly more registrations.

    Replaces the prior blind ``await asyncio.sleep`` registration window
    with a deterministic check; avoids load-induced flakes where a slow
    ``open_subscriber`` round-trip races a subsequent event insertion.
    """
    baseline = len(daemon.subscribers)
    target = baseline + added
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if len(daemon.subscribers) >= target:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(
        f"daemon did not add {added} subscriber(s) within {timeout}s "
        f"(baseline={baseline}, current={len(daemon.subscribers)})"
    )


async def await_thread(t: threading.Thread, timeout: float = 4.0) -> None:
    """Yield to the event loop until thread ``t`` finishes or ``timeout`` elapses.

    Polls ``t.is_alive()`` on the asyncio loop so a test that started a worker
    thread (which itself drives a blocking subscribe / ``wait_for`` round-trip)
    can join it without blocking the loop. Returns on either the thread exiting
    or the deadline; the caller asserts on the thread's recorded result, so a
    timeout surfaces as a missing or late result rather than an exception here.
    """
    deadline = time.monotonic() + timeout
    while t.is_alive() and time.monotonic() < deadline:
        await asyncio.sleep(0.05)


def isolated_subprocess_env(tmp_path: Path, **extra: str) -> tuple[dict[str, str], dict[str, Path]]:
    """Build a subprocess env carrying the isolated WAITBUS_*_DIR triple.

    The subprocess-shaped twin of the in-process ``serve_dirs`` fixture:
    creates state/runtime/config under ``tmp_path`` and returns
    ``(env, dirs)`` so callers can layer test-specific overrides via
    ``extra``. The per-test state dir has no ``secrets.json``, so the
    subprocess starts secret-free unless a caller stages one.
    """
    dirs = {
        "state": tmp_path / "state",
        "runtime": tmp_path / "runtime",
        "config": tmp_path / "config",
    }
    for d in dirs.values():
        d.mkdir(exist_ok=True)
    env = dict(os.environ)
    env.update(
        {
            "WAITBUS_STATE_DIR": str(dirs["state"]),
            "WAITBUS_RUNTIME_DIR": str(dirs["runtime"]),
            "WAITBUS_CONFIG_DIR": str(dirs["config"]),
        }
    )
    env.update(extra)
    return env, dirs
