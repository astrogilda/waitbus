"""Shared test harness for sync subscriber-engine tests.

``drive_sync_engine`` builds a ``socket.socketpair``, pre-loads bytes on
the server side, runs an ``engine(SubscriberHandle, emit) -> WaitOutcome``
in a daemon thread on the client side, and returns the engine's outcome and
any frames the engine emitted via the supplied ``emit`` callback.

The client socket is always closed in the ``finally`` block.  The server
socket is closed in ``finally`` by default, which means the engine sees a
live (open) peer during its idle window and exits via ``timed_out``.  Pass
``close_server_before_engine=True`` when the test needs the engine to see
an immediate EOF after draining the pre-loaded bytes (``peer_closed``
outcome); the server socket is then closed right after sending, before the
engine thread starts.

Replaces the inline ``_drive`` pattern in test_coalesce.py and the inline
socketpair scaffolding scattered across other test files.

Concurrency note: waitbus's sync engines (await_predicate, coalesce_replay)
are driven from sync CLI verbs in production; the test harness's
``threading.Thread`` model matches that production caller shape.  Tests
that drive async/asyncio.to_thread-based engines (e.g. the MCP
_tail_events_blocking path) use a different harness shape — that is
intentional, not an inconsistency.
"""

from __future__ import annotations

import socket
import threading
from collections.abc import Callable
from typing import Any

from waitbus._broadcast_sub import SubscriberHandle, WaitOutcome


def drive_sync_engine(
    bytes_to_send: list[bytes],
    *,
    engine: Callable[[SubscriberHandle, Callable[[dict[str, Any]], None]], WaitOutcome],
    close_server_before_engine: bool = False,
    idle_seconds_extra: float = 5.0,
    deadline_seconds: float = 5.0,
) -> tuple[WaitOutcome, list[dict[str, Any]]]:
    """Drive ``engine`` over a socketpair pre-loaded with ``bytes_to_send``.

    The ``engine`` callable receives a fresh ``SubscriberHandle`` and an
    ``emit`` callback.  The harness runs it on a daemon thread with a join
    timeout of ``deadline_seconds + idle_seconds_extra``; if the engine has
    not returned by then the test fails with an explicit message.

    Args:
        bytes_to_send: Frames to write to the server side before the engine
            thread starts.
        engine: Callable that receives ``(sub, emit)`` and returns a
            ``WaitOutcome``.  Run on a daemon thread so a buggy engine
            cannot hang the suite.
        close_server_before_engine: When ``True``, close the server socket
            immediately after sending so the engine sees EOF after draining.
            When ``False`` (default), the server socket stays open until
            ``finally``, so the engine exits via its idle timeout.
        idle_seconds_extra: Added to ``deadline_seconds`` for the thread
            join timeout, giving the engine's idle window time to fire.
        deadline_seconds: Base join timeout in seconds.
    """
    server, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        for chunk in bytes_to_send:
            server.sendall(chunk)

        if close_server_before_engine:
            server.close()

        emitted: list[dict[str, Any]] = []
        outcome_holder: list[WaitOutcome] = []

        def _emit(frame: dict[str, Any]) -> None:
            emitted.append(frame)

        def _run() -> None:
            sub = SubscriberHandle(sock=client)
            outcome_holder.append(engine(sub, _emit))

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        t.join(timeout=deadline_seconds + idle_seconds_extra)
        assert not t.is_alive(), f"engine did not exit within {deadline_seconds + idle_seconds_extra:.1f}s"
        assert outcome_holder, "engine returned no outcome"
        return outcome_holder[0], emitted
    finally:
        if not close_server_before_engine:
            server.close()
        client.close()
