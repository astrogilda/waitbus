"""Conformance tests for broadcast wire protocol v1 (daemon side).

Exercises the v1 behaviours the migrated unit tests do not assert directly:
protocol-version negotiation + reject, the ``subscribe_ack`` control-frame
field contract, and the ``caught_up_at`` replay/live watermark. These boot a
real daemon via the ``running_daemon`` fixture and speak the raw wire.
"""

from __future__ import annotations

import asyncio
import contextlib
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, cast

import pytest

from tests._wire_helpers import connect as _connect
from tests._wire_helpers import recv as _recv
from tests._wire_helpers import recv_until as _recv_until
from tests._wire_helpers import subscribe as _subscribe
from waitbus import _db, broadcast
from waitbus._frame import (
    FRAME_PROTO_VERSION,
    MAX_FRAME_BYTES,
)
from waitbus._types import EventInsert

_DaemonPaths = tuple[broadcast.Broadcast, dict[str, Path]]

# The broadcast daemon's SO_PEERCRED gate is Linux-only; the wire tests follow.
pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="broadcast daemon SO_PEERCRED check is Linux-only",
)


def _insert(db: Path, delivery_id: str) -> str:
    """Insert one event and return its daemon-generated ULID ``event_id``."""
    with contextlib.closing(sqlite3.connect(db)) as conn:
        _db.insert_event(
            conn,
            EventInsert(
                delivery_id=delivery_id,
                source="github",
                event_type="workflow_run",
                owner="o",
                repo="r",
                received_at=time.time_ns(),
                payload_json="{}",
                ingest_method="webhook",
            ),
        )
        row = conn.execute("SELECT event_id FROM events WHERE delivery_id = ?", (delivery_id,)).fetchone()
    return cast(str, row[0])


@pytest.mark.asyncio
async def test_unsupported_proto_is_rejected(running_daemon: _DaemonPaths) -> None:
    """A subscribe carrying an unsupported wire ``proto`` gets exactly one
    ``subscribe_rejected{reason:"version", supported:[1]}`` frame, then EOF."""
    _daemon, paths = running_daemon
    reader, writer = await _connect(paths["broadcast"])
    try:
        await _subscribe(writer, proto=999, filters=["*"])
        frame = await _recv(reader)
        assert frame is not None
        assert frame["kind"] == "subscribe_rejected"
        assert frame["reason"] == "version"
        assert frame["supported"] == [FRAME_PROTO_VERSION]
        # The daemon closes the connection after the single reject frame.
        assert await _recv(reader) is None
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_proto",
    [1.0, True, "1"],
    ids=["float-1.0", "bool-true", "string-1"],
)
async def test_non_int_proto_is_rejected_as_version(running_daemon: _DaemonPaths, bad_proto: Any) -> None:
    """Non-integer ``proto`` values are rejected with the version reason.

    Python's ``1 == 1.0 == True`` makes a naive comparison accept JSON
    ``1.0`` and ``true`` as v1; the validator enforces strict int (and
    excludes bool, which is an int subclass) so the wire contract is
    integer-only. JSON-decoded string ``"1"`` falls through the same
    isinstance gate. All three yield ``subscribe_rejected{reason:"version"}``.
    """
    _daemon, paths = running_daemon
    reader, writer = await _connect(paths["broadcast"])
    try:
        await _subscribe(writer, proto=bad_proto, filters=["*"])
        frame = await _recv(reader)
        assert frame is not None
        assert frame["kind"] == "subscribe_rejected"
        assert frame["reason"] == "version"
        assert frame["supported"] == [FRAME_PROTO_VERSION]
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


@pytest.mark.asyncio
async def test_subscribe_ack_fields(running_daemon: _DaemonPaths) -> None:
    """A successful (no-``since``) subscribe yields a ``subscribe_ack`` that
    advertises the negotiated proto, the heartbeat cadence, and the frame cap;
    with no replay, ``caught_up_at`` is ``None`` (the watermark is undefined
    when nothing was replayed)."""
    _daemon, paths = running_daemon
    reader, writer = await _connect(paths["broadcast"])
    try:
        await _subscribe(writer, proto=FRAME_PROTO_VERSION, filters=["*"])
        ack = await _recv_until(reader, "subscribe_ack")
        assert ack["kind"] == "subscribe_ack"
        assert ack["proto"] == FRAME_PROTO_VERSION
        assert ack["max_frame_bytes"] == MAX_FRAME_BYTES
        assert isinstance(ack["heartbeat_sec"], int) and ack["heartbeat_sec"] > 0
        assert ack["caught_up_at"] is None
        # Control frames carry no event identity.
        assert "event_id" not in ack
        assert "event_type" not in ack
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


@pytest.mark.asyncio
async def test_subscribe_ack_caught_up_at_is_null_even_with_existing_rows(
    running_daemon: _DaemonPaths,
) -> None:
    """No-``since`` subscribe yields ``caught_up_at = None`` regardless of
    whether the DB has rows. The watermark is a replay-dedup cursor, and the
    consumer with no ``since`` has nothing to dedup."""
    _daemon, paths = running_daemon
    _insert(paths["db"], "d-1")
    _insert(paths["db"], "d-2")
    reader, writer = await _connect(paths["broadcast"])
    try:
        await _subscribe(writer, proto=FRAME_PROTO_VERSION, filters=["*"])
        ack = await _recv_until(reader, "subscribe_ack")
        assert ack["caught_up_at"] is None
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


@pytest.mark.asyncio
async def test_proto_omitted_defaults_to_v1(running_daemon: _DaemonPaths) -> None:
    """Omitting ``proto`` is accepted as v1 (the only version today): the
    subscribe succeeds and the ack echoes proto == 1."""
    _daemon, paths = running_daemon
    reader, writer = await _connect(paths["broadcast"])
    try:
        await _subscribe(writer, filters=["*"])  # no proto key
        ack = await _recv_until(reader, "subscribe_ack")
        assert ack["proto"] == FRAME_PROTO_VERSION
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


@pytest.mark.asyncio
async def test_subscribe_ack_is_strictly_first_frame_on_wire(running_daemon: _DaemonPaths) -> None:
    """Wire ordering invariant: the daemon emits ``subscribe_ack`` as the
    FIRST frame on the wire, even when a live emit is pushed through
    ``_broadcast_pass`` during the registration→ack window (which the
    pre-ack buffer absorbs and drains after the ack)."""
    _daemon, paths = running_daemon
    reader, writer = await _connect(paths["broadcast"])
    try:
        await _subscribe(writer, proto=FRAME_PROTO_VERSION, filters=["*"])
        # Wait until the daemon has actually registered this subscriber
        # before injecting. The pre-ack buffer's contract (see Subscriber
        # docstring) covers only the registration->ack window; an event
        # rung BEFORE registration completes is `since`-replay's job, not
        # the buffer's, and with no `since` cursor here it would simply be
        # dropped. On a fast box registration wins the race against the
        # doorbell pass implicitly, but under CI scheduling latency the
        # pass can run first and lose the event. Polling the live in-loop
        # daemon's subscriber set lands the injection inside the window
        # deterministically. The daemon is in-process (conftest
        # running_daemon yields the Broadcast instance), so this reads the
        # real registration state, not a proxy.
        reg_deadline = time.monotonic() + 5.0
        while time.monotonic() < reg_deadline and not _daemon.subscribers:
            await asyncio.sleep(0.01)
        assert _daemon.subscribers, "daemon did not register the subscriber before injection"
        # Now push an event into the daemon's broadcast pipeline. _fan_out
        # captures it into the registered subscriber's pre_ack_buffer when
        # the ack send has not yet completed; the drain that follows the
        # ack writes it onto the wire after the ack frame.
        eid = _insert(paths["db"], "d-pre-ack")
        from waitbus import _doorbell as _doorbell_mod

        _doorbell_mod.ring()
        # First frame MUST be the ack, not the event.
        first = await _recv(reader)
        assert first is not None
        assert first["kind"] == "subscribe_ack", (
            f"expected subscribe_ack as the first frame on the wire; got {first['kind']!r}"
        )
        assert first["caught_up_at"] is None
        # The buffered event arrives next (drained after ack).
        deadline = time.monotonic() + 3.0
        seen: list[str] = []
        while time.monotonic() < deadline and eid not in seen:
            frame = await _recv(reader, timeout=3.0)
            if frame is None:
                break
            if frame.get("kind") == "event":
                seen.append(frame["event_id"])
        assert eid in seen, "pre-ack-buffered event was not drained onto the wire"
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


@pytest.mark.asyncio
async def test_subscribe_ack_is_first_then_replay_watermark(running_daemon: _DaemonPaths) -> None:
    """With ``since``, the daemon emits the ``subscribe_ack`` as the FIRST
    frame on the wire (ack-strictly-first); replay frames follow. The ack's
    ``caught_up_at`` is the replay watermark — every replayed event has
    ``event_id <= caught_up_at`` and there are no event frames before the
    ack."""
    _daemon, paths = running_daemon
    first = _insert(paths["db"], "d-1")
    second = _insert(paths["db"], "d-2")
    reader, writer = await _connect(paths["broadcast"])
    try:
        await _subscribe(writer, proto=FRAME_PROTO_VERSION, filters=["*"], since="0" * 26)
        # First frame MUST be the ack.
        ack = await _recv(reader, timeout=3.0)
        assert ack is not None
        assert ack["kind"] == "subscribe_ack", (
            f"expected subscribe_ack as the first frame on the wire; got {ack['kind']!r}"
        )
        caught_up_at = ack["caught_up_at"]
        assert caught_up_at is not None
        # Replay frames follow the ack.
        replayed: list[str] = []
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline and len(replayed) < 2:
            frame = await _recv(reader, timeout=3.0)
            assert frame is not None
            if frame["kind"] == "event":
                replayed.append(frame["event_id"])
        assert {first, second} <= set(replayed)
        assert all(eid <= caught_up_at for eid in replayed)
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()
