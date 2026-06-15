"""Tests for the public subscribe SDK (``waitbus.subscribe``).

Mirrors the ``test_waitbus_wait_universal.py`` daemon+emit+assert shape: the
``running_daemon`` fixture runs the daemon in the test's event loop, so the
SYNCHRONOUS ``wait_for`` / ``subscribe`` calls run on a worker thread (calling
them inline would block the loop and starve the daemon). ``asubscribe`` runs
in-loop (its own worker thread + queue bridge the blocking engine).
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
import threading
from pathlib import Path
from typing import Any

import pytest

from tests._agent_harness import insert_event_row
from tests._daemon_helpers import (
    await_subscribers as _await_subscribers,
)
from tests._daemon_helpers import (
    await_thread as _await_thread,
)
from waitbus import _subscribe as sdk
from waitbus import broadcast
from waitbus._subscribe import EventFrame

pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="broadcast daemon SO_PEERCRED check is Linux-only",
)


# --- wait_for ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_wait_for_returns_matched_eventframe(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    daemon, paths = running_daemon
    result: list[EventFrame | None] = []

    def run() -> None:
        result.append(
            sdk.wait_for(
                'fields.event_type="docker_container"',
                source="docker",
                timeout=3.0,
                socket_path=str(paths["broadcast"]),
            )
        )

    t = threading.Thread(target=run, daemon=True)
    t.start()
    await _await_subscribers(daemon)
    insert_event_row(paths["db"], "d-1", source="docker", event_type="docker_container")
    await _await_thread(t)
    assert not t.is_alive(), "wait_for hung"
    event = result[0]
    assert isinstance(event, EventFrame)
    assert event.event_type == "docker_container"
    assert event.fields.get("source") == "docker"


@pytest.mark.asyncio
async def test_wait_for_timeout_returns_none(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    daemon, paths = running_daemon
    result: list[EventFrame | None] = []

    def run() -> None:
        result.append(sdk.wait_for(source="docker", timeout=0.4, socket_path=str(paths["broadcast"])))

    t = threading.Thread(target=run, daemon=True)
    t.start()
    await _await_subscribers(daemon)
    # No matching event inserted -> times out.
    await _await_thread(t)
    assert not t.is_alive()
    assert result == [None]


# --- subscribe (sync generator) --------------------------------------------


@pytest.mark.asyncio
async def test_subscribe_streams_multiple_then_closes(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    daemon, paths = running_daemon
    got: list[EventFrame] = []

    def run() -> None:
        with contextlib.closing(sdk.subscribe(source="docker", socket_path=str(paths["broadcast"]))) as stream:
            for ev in stream:
                got.append(ev)
                if len(got) >= 2:
                    break

    t = threading.Thread(target=run, daemon=True)
    t.start()
    await _await_subscribers(daemon)
    insert_event_row(paths["db"], "s-1", source="docker", event_type="docker_container")
    insert_event_row(paths["db"], "s-2", source="docker", event_type="docker_container")
    await _await_thread(t)
    assert not t.is_alive()
    assert len(got) == 2
    assert {e.delivery_id for e in got} == {"s-1", "s-2"}


# --- asubscribe (async generator: queue + worker thread + teardown) ---------


@pytest.mark.asyncio
async def test_asubscribe_yields_then_tears_down_on_break(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    daemon, paths = running_daemon
    got: list[EventFrame] = []

    async def consume() -> None:
        async for ev in sdk.asubscribe(source="docker", socket_path=str(paths["broadcast"])):
            got.append(ev)
            break  # exits the async-gen -> finally{} closes socket, joins worker

    task = asyncio.create_task(consume())
    # Let the coroutine open the subscriber + start its worker before the barrier.
    await asyncio.sleep(0)
    await _await_subscribers(daemon)
    insert_event_row(paths["db"], "a-1", source="docker", event_type="docker_container")
    await asyncio.wait_for(task, timeout=4.0)
    assert len(got) == 1
    assert got[0].delivery_id == "a-1"


# --- predicate composition / errors (no daemon needed) ----------------------


def test_wait_for_requires_a_spec() -> None:
    with pytest.raises(ValueError, match="requires a match spec, a source, or a recipient"):
        sdk.wait_for()


def test_compose_predicate_accepts_match_sequence() -> None:
    # A list of specs AND-composed + a source filter; must not raise.
    pred = sdk._compose_predicate(['fields.event_type="docker_container"'], "docker")
    assert pred({"fields": {"event_type": "docker_container", "source": "docker"}}) is True
    assert pred({"fields": {"event_type": "other", "source": "docker"}}) is False


def test_malformed_match_spec_raises_valueerror() -> None:
    # a malformed spec surfaces as ValueError, never a bare KeyError.
    with pytest.raises(ValueError):
        sdk._compose_predicate(["this is not a valid spec"], None)


# --- async-bridge error / EOF / backpressure (no daemon: patch the drain) ----
# These exercise the thread->loop hand-off itself, deterministically: the engine
# is replaced by a patched ``_drain_one`` and the subscriber socket by a local
# socketpair (so teardown's shutdown/close are real), so the tests pin the
# bridge's contract (forward typed rejects, deliver EOF, real backpressure)
# independent of a live daemon.


def _mk_frame(delivery_id: str) -> EventFrame:
    import msgspec

    return msgspec.convert(
        {
            "event_id": delivery_id,
            "event_type": "docker_container",
            "owner": "local",
            "repo": "docker",
            "received_at": 0,
            "delivery_id": delivery_id,
            "summary": "",
            "fields": {"source": "docker"},
            "kind": "event",
        },
        EventFrame,
        strict=False,
    )


@pytest.fixture
def _fake_sub(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Patch ``open_subscriber`` to a socketpair-backed handle -- no daemon."""
    import socket as _socket

    from waitbus._broadcast_sub import SubscriberHandle

    a, b = _socket.socketpair()
    monkeypatch.setattr(sdk, "open_subscriber", lambda **_kw: SubscriberHandle(sock=a))
    yield
    for sock in (a, b):
        with contextlib.suppress(OSError):
            sock.close()


def _reject(*_a: Any, **_k: Any) -> Any:
    from waitbus._broadcast_sub import TokenRequiredError

    raise TokenRequiredError("subscribe rejected: reason='token'", "configure a broadcast token")


def test_wait_for_propagates_typed_reject(_fake_sub: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    from waitbus._broadcast_sub import TokenRequiredError

    monkeypatch.setattr(sdk, "_drain_one", _reject)
    with pytest.raises(TokenRequiredError):
        sdk.wait_for(source="docker", socket_path="x")


def test_subscribe_propagates_typed_reject(_fake_sub: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    from waitbus._broadcast_sub import TokenRequiredError

    monkeypatch.setattr(sdk, "_drain_one", _reject)
    with pytest.raises(TokenRequiredError):
        for _ in sdk.subscribe(source="docker", socket_path="x"):
            pass


# --- outcome surfacing (raise on EOF/framing; None only for timeout) ---


def _outcome(**kw: Any) -> Any:
    from waitbus._broadcast_sub import WaitOutcome

    base: dict[str, bool] = {
        "matched": False,
        "timed_out": False,
        "cancelled": False,
        "peer_closed": False,
        "framing_error": False,
    }
    base.update(kw)
    return WaitOutcome(**base)


def test_wait_for_raises_on_daemon_close(_fake_sub: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """One-shot wait_for raises when the daemon closes mid-wait (dead -> abort)."""
    from waitbus._broadcast_sub import BroadcastConnectionError

    monkeypatch.setattr(sdk, "await_predicate", lambda *a, **k: _outcome(peer_closed=True))
    with pytest.raises(BroadcastConnectionError):
        sdk.wait_for(source="docker", socket_path="x")


def test_wait_for_raises_on_framing_error(_fake_sub: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """A wire-framing violation raises on every path (here the one-shot wait_for)."""
    from waitbus._broadcast_sub import BroadcastConnectionError

    monkeypatch.setattr(sdk, "await_predicate", lambda *a, **k: _outcome(peer_closed=True, framing_error=True))
    with pytest.raises(BroadcastConnectionError):
        sdk.wait_for(source="docker", socket_path="x")


def test_wait_for_returns_none_on_timeout_not_close(_fake_sub: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """A timeout (daemon alive, no match) stays None -- a slow/absent peer, retry."""
    monkeypatch.setattr(sdk, "await_predicate", lambda *a, **k: _outcome(timed_out=True))
    assert sdk.wait_for(source="docker", socket_path="x") is None


def test_subscribe_ends_cleanly_on_daemon_close(_fake_sub: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """A stream subscribe ends (no raise) on a clean daemon close -- the websockets
    ConnectionClosedOK shape; only a framing error raises."""
    monkeypatch.setattr(sdk, "await_predicate", lambda *a, **k: _outcome(peer_closed=True))
    assert list(sdk.subscribe(source="docker", socket_path="x")) == []


def test_subscribe_raises_on_framing_error(_fake_sub: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """A stream subscribe raises on a wire-framing violation (a broken connection)."""
    from waitbus._broadcast_sub import BroadcastConnectionError

    monkeypatch.setattr(sdk, "await_predicate", lambda *a, **k: _outcome(peer_closed=True, framing_error=True))
    with pytest.raises(BroadcastConnectionError):
        list(sdk.subscribe(source="docker", socket_path="x"))


def test_compose_predicate_to_filter_roundtrips_special_chars() -> None:
    """A recipient name with quotes/backslashes round-trips by exact equality:
    json.dumps -> json.loads is the escaping boundary, so the
    string match-spec never mis-parses an arbitrary value."""
    weird = 'agent"x\\y'
    composed = sdk._compose_predicate(None, None, to=weird)
    assert composed({"fields": {"msg_to": weird}})
    assert not composed({"fields": {"msg_to": "agent"}})


@pytest.mark.asyncio
async def test_asubscribe_propagates_typed_reject_not_silent_eof(
    _fake_sub: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Regression guard: the async path MUST re-raise the typed reject the engine
    # raises, not swallow it into a clean EOF (the sync paths above propagate it).
    from waitbus._broadcast_sub import TokenRequiredError

    monkeypatch.setattr(sdk, "_drain_one", _reject)

    async def consume() -> None:
        async for _ in sdk.asubscribe(source="docker", socket_path="x"):
            pass

    with pytest.raises(TokenRequiredError):
        await asyncio.wait_for(consume(), timeout=4.0)


@pytest.mark.asyncio
async def test_asubscribe_terminates_cleanly_on_eof(_fake_sub: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    # peer-closed / EOF -> the async generator ends without hanging.
    monkeypatch.setattr(sdk, "_drain_one", lambda *_a, **_k: None)

    async def collect() -> list[EventFrame]:
        return [ev async for ev in sdk.asubscribe(source="docker", socket_path="x")]

    got = await asyncio.wait_for(collect(), timeout=4.0)
    assert got == []


@pytest.mark.asyncio
async def test_asubscribe_backpressure_delivers_all_in_order(_fake_sub: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    # A tiny bounded queue + a slow consumer forces the worker to BLOCK on put.
    # The real blocking hand-off must deliver every frame in order (the old
    # fire-and-forget put_nowait would drop frames on QueueFull).
    n = 50
    seq = iter([_mk_frame(str(i)) for i in range(n)] + [None])
    monkeypatch.setattr(sdk, "_drain_one", lambda *_a, **_k: next(seq))
    monkeypatch.setattr(sdk, "_ASUBSCRIBE_QUEUE_MAXSIZE", 4)
    got: list[str] = []

    async def consume() -> None:
        async for ev in sdk.asubscribe(source="docker", socket_path="x"):
            got.append(ev.delivery_id)
            await asyncio.sleep(0)  # yield so the bounded queue fills -> backpressure

    await asyncio.wait_for(consume(), timeout=6.0)
    assert got == [str(i) for i in range(n)]


@pytest.mark.asyncio
async def test_asubscribe_forwards_non_oserror_engine_error(_fake_sub: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    # a non-OSError engine error (e.g. an EventFrame decode failure) is
    # forwarded across the queue and re-raised in the consumer, not swallowed.
    class _BoomError(ValueError):
        pass

    def _raise(*_a: Any, **_k: Any) -> Any:
        raise _BoomError("decode failed")

    monkeypatch.setattr(sdk, "_drain_one", _raise)

    async def consume() -> None:
        async for _ in sdk.asubscribe(source="docker", socket_path="x"):
            pass

    with pytest.raises(_BoomError):
        await asyncio.wait_for(consume(), timeout=4.0)
