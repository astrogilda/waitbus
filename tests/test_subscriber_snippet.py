"""End-to-end test for the canonical Python subscriber snippet.

Runs ``docs/snippets/minimal_subscriber.py`` as a subprocess against the
``running_daemon`` fixture, emits one event per source via the
in-process emit path, and asserts the snippet's stdout carries all four
delivery_ids in the expected ``delivery_id\\tsource=...\\ttype=...``
format. The test catches protocol drift at the same commit that ships
the change: if the wire schema, frame layout, or default-path resolver
changes in ``_frame.py`` or ``_paths.py`` and the snippet is not
updated, this test fails immediately.

The snippet imports nothing from ``waitbus``; it is run as a
fully-isolated subprocess so the test exercises the same binary an
external operator would invoke.
"""

from __future__ import annotations

import asyncio
import fcntl
import os
import subprocess
import sys
import time
from pathlib import Path

import msgspec
import pytest

from tests._wire_helpers import read_nonblocking
from waitbus import _emit as emit_mod
from waitbus._types import EventInsert

# pyright + mypy import-checking of fixtures happens via conftest auto-discovery.
pytestmark = pytest.mark.asyncio

_SNIPPET = Path(__file__).resolve().parents[1] / "docs" / "snippets" / "minimal_subscriber.py"


_SOURCE_EVENT_TYPE: dict[str, str] = {
    "github": "workflow_run",
    "pytest": "pytest_session",
    "docker": "docker_container",
    "fs": "fs_change",
}


def _build_event(source: str, delivery_id: str) -> EventInsert:
    """Build one EventInsert with a known delivery_id we can grep for."""
    return EventInsert(
        delivery_id=delivery_id,
        source=source,
        event_type=_SOURCE_EVENT_TYPE.get(source, "generic_event"),
        owner="bench",
        repo="snippet-test",
        received_at=time.time_ns(),
        payload_json=msgspec.json.encode({"i": 0}).decode(),
        ingest_method="snippet-test",
        status="completed",
        conclusion="success",
    )


async def _drive_warmup_until_received(
    *,
    proc: subprocess.Popen[str],
    db_path: Path,
    timeout_sec: float = 5.0,
) -> None:
    """Emit warmup events in a loop until the snippet prints one of them.

    The snippet's connect-and-subscribe races the test's first emit: a
    pure ``sleep`` would either be too short (flaky) or always-long
    (wasteful). Instead, emit warmup events at 20 Hz with unique
    delivery_ids and drain stdout; the first time a warmup line lands
    on stdout, the snippet is fully registered and we return. Caller
    can then emit the real test events with confidence.
    """
    if proc.stdout is None:  # defensive: subprocess.PIPE always sets this
        raise RuntimeError("snippet subprocess has no stdout pipe")
    flags = fcntl.fcntl(proc.stdout.fileno(), fcntl.F_GETFL)
    fcntl.fcntl(proc.stdout.fileno(), fcntl.F_SETFL, flags | os.O_NONBLOCK)

    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        warmup_id = f"snippet-test:warmup:{time.time_ns()}"
        emit_mod.emit_batch([_build_event("pytest", warmup_id)], db_path=db_path)
        await asyncio.sleep(0.05)
        data = read_nonblocking(proc.stdout.fileno())
        if data and "snippet-test:warmup:" in data:
            # Restore blocking mode so the assertion loop's readline()
            # is line-buffered as the caller expects.
            fcntl.fcntl(proc.stdout.fileno(), fcntl.F_SETFL, flags)
            return
    # Drain stderr so the timeout error carries useful context.
    if proc.stderr is not None:
        flags_err = fcntl.fcntl(proc.stderr.fileno(), fcntl.F_GETFL)
        fcntl.fcntl(proc.stderr.fileno(), fcntl.F_SETFL, flags_err | os.O_NONBLOCK)
        err_data = read_nonblocking(proc.stderr.fileno())
    else:
        err_data = ""
    proc.poll()
    raise TimeoutError(
        f"snippet did not receive any warmup event within {timeout_sec}s; "
        f"subprocess returncode={proc.returncode}; stderr={err_data!r}"
    )


async def test_python_snippet_streams_all_four_sources(
    running_daemon: tuple[object, dict[str, Path]], tmp_path: Path
) -> None:
    """The Python subscriber snippet receives one event per source.

    Spawns the snippet pointing at the test daemon's socket (via
    ``WAITBUS_BROADCAST_SOCKET``), emits four events (one per source) via
    :func:`emit_mod.emit_batch`, then reads stdout until all four
    delivery_ids have been observed or a 10-second deadline elapses.
    """
    _, paths = running_daemon
    socket_path = paths["broadcast"]
    db_path = paths["db"]

    # Snippet subprocess: own env, isolated socket. Capture stdout
    # line-by-line so the assertion loop can drain in real time.
    snippet_env = os.environ.copy()
    snippet_env["WAITBUS_BROADCAST_SOCKET"] = str(socket_path)
    snippet_env["PYTHONUNBUFFERED"] = "1"
    proc = subprocess.Popen(
        [sys.executable, "-u", str(_SNIPPET)],
        env=snippet_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
        text=True,
    )
    try:
        await _drive_warmup_until_received(proc=proc, db_path=db_path)

        # Emit four events; delivery_ids carry the source label so the
        # assertion can match the exact rows on stdout.
        delivery_ids: dict[str, str] = {}
        for source in ("github", "pytest", "docker", "fs"):
            did = f"snippet-test:{source}:{time.time_ns()}"
            delivery_ids[source] = did
            emit_mod.emit_batch([_build_event(source, did)], db_path=db_path)

        # Stream stdout non-blocking; assertion loop accumulates a buffer
        # and scans newline-delimited records as they land.
        if proc.stdout is None:
            raise RuntimeError("snippet subprocess has no stdout pipe")
        flags = fcntl.fcntl(proc.stdout.fileno(), fcntl.F_GETFL)
        fcntl.fcntl(proc.stdout.fileno(), fcntl.F_SETFL, flags | os.O_NONBLOCK)

        seen: set[str] = set()
        buffer = ""
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and len(seen) < 4:
            chunk = read_nonblocking(proc.stdout.fileno())
            if chunk:
                buffer += chunk
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    for source, did in delivery_ids.items():
                        if did in line:
                            assert f"\tsource={source}\t" in line, line
                            seen.add(did)
            await asyncio.sleep(0.02)

        assert seen == set(delivery_ids.values()), f"missing: {set(delivery_ids.values()) - seen}"
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2.0)
        # Close the captured stdout / stderr pipes explicitly so the
        # GC-time __del__ on subprocess.Popen does not leak file
        # descriptors past the test's end. pytest elevates
        # ResourceWarning to an error by default, so the close is
        # load-bearing.
        for stream in (proc.stdout, proc.stderr):
            if stream is not None:
                stream.close()
