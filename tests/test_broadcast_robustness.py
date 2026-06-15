"""Stateful property tests for broadcast subscriber-lifecycle invariants.

Covers four subscriber-lifecycle paths through the broadcast daemon:

1. Lag-eviction via ``_fan_out``, pre-ack buffer drain, ``_replay``, and
   ``_heartbeat_loop`` -- all funnel through ``_close_subscriber``.
2. ``sqlite3.Error`` during ``_replay``: silent close with
   ``reason="replay_db_error"`` (no wire frame per ``_TERMINAL_REJECT_FRAMES``).
3. Subscribe-reject taxonomy: the consumer-facing wire reasons
   (``token``, ``version``, ``lag_limit_exceeded``) and the daemon-internal
   close reasons that map onto them.
4. ``PRE_ACK_BUFFER`` overflow: the frame-count and byte-count gates both
   evict with the unified ``lag_limit_exceeded`` wire reason.

The state machine constructs a ``Broadcast`` instance synchronously
against a temporary SQLite DB and uses ``_FakeSock`` stubs in place of
real Unix sockets. This keeps every rule deterministic without a live
asyncio event loop -- ``_fan_out`` and ``_close_subscriber`` are
synchronous and exercise the full drain-path surface.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import uuid
from pathlib import Path

from hypothesis import HealthCheck, settings
from hypothesis.database import (
    DirectoryBasedExampleDatabase,
    GitHubArtifactDatabase,
    MultiplexedDatabase,
    ReadOnlyDatabase,
)
from hypothesis.stateful import Bundle, RuleBasedStateMachine, initialize, invariant, rule
from hypothesis.strategies import text

from tests._wire_helpers import FakeWireSocket as _FakeSock


def _example_database() -> MultiplexedDatabase | DirectoryBasedExampleDatabase:
    """Return the Hypothesis example database for this test module.

    Local development always reads + writes ``.hypothesis/examples``;
    that is the default Hypothesis sets and the floor below which we
    never drop. The cross-environment CI ↔ local replay layer is
    opt-in via ``WAITBUS_HYPOTHESIS_ARTIFACT_DB=1`` (which presupposes
    a ``GITHUB_TOKEN`` with read access to the
    ``astrogilda/waitbus`` artifact store): when both are set we
    layer a read-only ``GitHubArtifactDatabase`` underneath the local
    DB so a counterexample that landed in CI is automatically replayed
    on the developer's next ``pytest`` invocation. CI is responsible
    for publishing its own counterexample artifact; we never publish
    a developer-local DB upstream.

    The artifact layer is opt-in rather than auto-on because
    ``GitHubArtifactDatabase`` emits a ``HypothesisWarning`` and
    disables the whole database when the artifact does not yet exist
    (typical for a young repo or an offline developer). Gating the
    layer behind the explicit env var keeps the developer happy path
    silent. The ``cpython#132316`` pitfall around a missing
    ``GITHUB_TOKEN`` is addressed by the same gate.
    """
    local = DirectoryBasedExampleDatabase(".hypothesis/examples")
    if os.environ.get("WAITBUS_HYPOTHESIS_ARTIFACT_DB") == "1" and os.environ.get("GITHUB_TOKEN"):
        artifact = GitHubArtifactDatabase("astrogilda", "waitbus")
        return MultiplexedDatabase(local, ReadOnlyDatabase(artifact))
    return local


_STATE_MACHINE_SETTINGS = settings(
    max_examples=50,
    deadline=None,
    database=_example_database(),
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)


class BroadcastDrainMachine(RuleBasedStateMachine):
    """Exercise drain paths and assert daemon-state invariants under arbitrary rule sequences."""

    live_subs: Bundle[int] = Bundle("live_subs")

    def __init__(self) -> None:
        super().__init__()
        from waitbus import _db, _metrics
        from waitbus import broadcast as bc

        self._bc = bc
        self._metrics = _metrics
        # Reset the process-global Prometheus registry so each Hypothesis
        # example starts with a known-clean SUBSCRIBER_COUNT gauge. Without
        # this, a prior example's teardown could leave the gauge at N,
        # making the absolute value drift across examples and masking real
        # gauge-stability bugs that the subscriber_count_consistent
        # invariant exists to catch.
        _metrics.reset()

        self._tmpdir = tempfile.mkdtemp(prefix="bcast_drain_")
        self._db_path = Path(self._tmpdir) / "events.db"
        _db.ensure_schema(self._db_path)

        self._daemon = bc.Broadcast(db_path=str(self._db_path))
        self._next_fd = 100
        self._socks: dict[int, _FakeSock] = {}
        self._subs: dict[int, bc.Subscriber] = {}
        self._wire_frames: dict[int, list[bytes]] = {}
        self._closed_fds: set[int] = set()
        self._close_reasons: dict[int, str] = {}
        # Addressed-messaging model state. open_requests maps
        # correlation_id -> (sender, recipient) so the round-trip
        # invariant can verify that every respond matched an
        # earlier request. closed_correlations is the union of
        # correlations that have been matched by a respond, so a
        # second respond on the same correlation is detectable.
        # recipient_inboxes maps agent_id -> list of correlations
        # the subscriber set as ``to=...`` would observe -- the
        # model representation of the SDK's recipient-inbox filter.
        self._open_requests: dict[str, tuple[str, str]] = {}
        self._closed_correlations: set[str] = set()
        self._recipient_inboxes: dict[str, list[str]] = {}

    def teardown(self) -> None:
        """Drain any remaining subscribers and remove the temp directory."""
        for fd in list(self._socks):
            if fd in self._daemon.subscribers:
                self._daemon._close_subscriber(fd, reason="shutdown")
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def _alloc_fd(self) -> int:
        fd = self._next_fd
        self._next_fd += 1
        return fd

    def _register_sub(
        self,
        fd: int,
        *,
        exc: BaseException | None = None,
        pre_ack: bool = False,
    ) -> _FakeSock:
        """Register a fake subscriber at ``fd`` and increment the gauge."""
        from waitbus import broadcast as bc

        sock = _FakeSock(fileno=fd, exc=exc)
        sub = bc.Subscriber(
            sock=sock,  # type: ignore[arg-type]
            filters=["*"],
            event_types=frozenset({"workflow_run"}),
            remote_uid=os.getuid(),
        )
        if pre_ack:
            sub.pre_ack_buffer = []
            sub.pre_ack_buffered_bytes = 0
        self._daemon.subscribers[fd] = sub
        self._metrics.SUBSCRIBER_COUNT.inc()
        self._socks[fd] = sock
        self._subs[fd] = sub
        self._wire_frames[fd] = []
        return sock

    @initialize(target=live_subs)
    def add_initial_subscriber(self) -> int:
        """Seed the machine with one live subscriber so other rules can act on it."""
        fd = self._alloc_fd()
        self._register_sub(fd)
        return fd

    @rule(target=live_subs)
    def subscribe_normal(self) -> int:
        """Register a well-behaved subscriber (post-ack, no pre-ack buffer)."""
        fd = self._alloc_fd()
        self._register_sub(fd)
        return fd

    @rule(target=live_subs)
    def subscribe_with_pre_ack_buffer(self) -> int:
        """Register a subscriber in the pre-ack window (buffer active)."""
        fd = self._alloc_fd()
        self._register_sub(fd, pre_ack=True)
        return fd

    @rule(fd=live_subs)
    def trigger_lag_via_fan_out(self, fd: int) -> None:
        """Force a subscriber to lag by raising EAGAIN on every send.

        After ``LAG_LIMIT`` consecutive ``BlockingIOError`` results,
        ``_fan_out`` calls ``_close_subscriber`` with
        ``reason="lag_limit_exceeded"``.
        """
        from waitbus import broadcast as bc

        if fd not in self._daemon.subscribers:
            return
        sock = self._socks[fd]
        sock.exc = BlockingIOError()
        blob = b"y" * 64
        event_id = "01" + "0" * 24
        for _ in range(bc.LAG_LIMIT + 1):
            self._daemon._fan_out(1, event_id, "test-owner", "test-repo", "workflow_run", blob)
            if fd not in self._daemon.subscribers:
                break
        if fd not in self._daemon.subscribers:
            self._closed_fds.add(fd)
            self._close_reasons[fd] = "lag_limit_exceeded"
            self._wire_frames[fd] = list(sock.sent)

    @rule(fd=live_subs)
    def trigger_partial_send_lag(self, fd: int) -> None:
        """Force a subscriber to lag via short-count partial sends.

        ``send_limit=1`` makes the first send accept one byte and buffer the
        tail (lag 1); every subsequent ``_fan_out`` delivery hits the
        append-behind-buffer branch until ``LAG_LIMIT`` consecutive
        non-clean sends evict the subscriber with
        ``reason="lag_limit_exceeded"``.
        """
        from waitbus import broadcast as bc

        if fd not in self._daemon.subscribers:
            return
        sock = self._socks[fd]
        sock.send_limit = 1
        blob = b"y" * 64
        event_id = "01" + "0" * 24
        for _ in range(bc.LAG_LIMIT + 1):
            self._daemon._fan_out(1, event_id, "test-owner", "test-repo", "workflow_run", blob)
            if fd not in self._daemon.subscribers:
                break
        if fd not in self._daemon.subscribers:
            self._closed_fds.add(fd)
            self._close_reasons[fd] = "lag_limit_exceeded"
            self._wire_frames[fd] = list(sock.sent)

    @rule(fd=live_subs)
    def close_subscriber_lag(self, fd: int) -> None:
        """Close a subscriber with ``reason="lag_limit_exceeded"`` (lag wire frame)."""
        if fd not in self._daemon.subscribers:
            return
        sock = self._socks[fd]
        self._daemon._close_subscriber(fd, reason="lag_limit_exceeded")
        self._closed_fds.add(fd)
        self._close_reasons[fd] = "lag_limit_exceeded"
        self._wire_frames[fd] = list(sock.sent)

    @rule(fd=live_subs)
    def close_subscriber_heartbeat_lag(self, fd: int) -> None:
        """Close a subscriber with ``reason="heartbeat_lag"`` (lag wire frame)."""
        if fd not in self._daemon.subscribers:
            return
        sock = self._socks[fd]
        self._daemon._close_subscriber(fd, reason="heartbeat_lag")
        self._closed_fds.add(fd)
        self._close_reasons[fd] = "heartbeat_lag"
        self._wire_frames[fd] = list(sock.sent)

    @rule(fd=live_subs)
    def close_subscriber_replay_lag(self, fd: int) -> None:
        """Close a subscriber with ``reason="replay_lag_limit_exceeded"`` (lag wire frame)."""
        if fd not in self._daemon.subscribers:
            return
        sock = self._socks[fd]
        self._daemon._close_subscriber(fd, reason="replay_lag_limit_exceeded")
        self._closed_fds.add(fd)
        self._close_reasons[fd] = "replay_lag_limit_exceeded"
        self._wire_frames[fd] = list(sock.sent)

    @rule(fd=live_subs)
    def close_subscriber_replay_db_error(self, fd: int) -> None:
        """Close a subscriber with ``reason="replay_db_error"`` (silent wire close)."""
        if fd not in self._daemon.subscribers:
            return
        sock = self._socks[fd]
        self._daemon._close_subscriber(fd, reason="replay_db_error")
        self._closed_fds.add(fd)
        self._close_reasons[fd] = "replay_db_error"
        self._wire_frames[fd] = list(sock.sent)

    @rule(fd=live_subs)
    def trigger_pre_ack_frame_overflow(self, fd: int) -> None:
        """Fill the pre-ack buffer to the frame-count cap and trip the overflow gate."""
        from waitbus import broadcast as bc

        if fd not in self._daemon.subscribers:
            return
        sub = self._daemon.subscribers[fd]
        if sub.pre_ack_buffer is None:
            return
        sub.pre_ack_buffer = [b"x"] * bc.PRE_ACK_BUFFER_FRAMES
        sock = self._socks[fd]
        event_id = "01" + "0" * 24
        self._daemon._fan_out(1, event_id, "test-owner", "test-repo", "workflow_run", b"overflow")
        if fd not in self._daemon.subscribers:
            self._closed_fds.add(fd)
            self._close_reasons[fd] = "lag_limit_exceeded"
            self._wire_frames[fd] = list(sock.sent)

    @rule(fd=live_subs)
    def trigger_pre_ack_byte_overflow(self, fd: int) -> None:
        """Set the pre-ack buffered-bytes counter to the cap and trip the overflow gate."""
        from waitbus import broadcast as bc

        if fd not in self._daemon.subscribers:
            return
        sub = self._daemon.subscribers[fd]
        if sub.pre_ack_buffer is None:
            return
        if not sub.pre_ack_buffer:
            sub.pre_ack_buffer = [b"x"]
        sub.pre_ack_buffered_bytes = bc.PRE_ACK_BUFFER_BYTES
        sock = self._socks[fd]
        event_id = "01" + "0" * 24
        self._daemon._fan_out(1, event_id, "test-owner", "test-repo", "workflow_run", b"overflow")
        if fd not in self._daemon.subscribers:
            self._closed_fds.add(fd)
            self._close_reasons[fd] = "lag_limit_exceeded"
            self._wire_frames[fd] = list(sock.sent)

    @rule(fd=live_subs)
    def close_subscriber_subscribe_ack_send_failed(self, fd: int) -> None:
        """Close a subscriber with ``reason="subscribe_ack_send_failed"`` (silent wire close).

        Exercises the sixth daemon-internal close reason -- the narrow
        case where the daemon cannot deliver the post-registration
        ``subscribe_ack`` frame to the peer (write error or close-during-ack).
        Like ``replay_db_error``, it is not in ``_TERMINAL_REJECT_FRAMES``;
        the wire close is silent because the channel has already failed.
        """
        if fd not in self._daemon.subscribers:
            return
        sock = self._socks[fd]
        self._daemon._close_subscriber(fd, reason="subscribe_ack_send_failed")
        self._closed_fds.add(fd)
        self._close_reasons[fd] = "subscribe_ack_send_failed"
        self._wire_frames[fd] = list(sock.sent)

    @rule(sender=text(min_size=1, max_size=12), recipient=text(min_size=1, max_size=12))
    def request_op(self, sender: str, recipient: str) -> None:
        """Model an addressed-messaging request: track an open ``(corr_id, sender, recipient)``.

        Does not invoke the public ``request()`` SDK directly: the
        machine's ``_FakeSock`` stubs cannot satisfy the SDK's
        subscribe + select path, and the SDK's correctness round-trip
        is exercised separately in ``tests/test_addressed_messaging.py``.
        This rule lets the state machine drive long sequences of
        outstanding requests so the round-trip invariant catches a
        model-level regression (e.g. a respond rule that matched a
        correlation that was never open).
        """
        # Skip if either side is exactly the canonical 'lag_limit_exceeded'
        # string or contains a newline -- those would clash with the daemon
        # log line parser further downstream and the rule's purpose is the
        # model invariant, not exotic-string fuzzing.
        if "\n" in sender or "\n" in recipient:
            return
        correlation_id = f"corr-{uuid.uuid4()}"
        self._open_requests[correlation_id] = (sender, recipient)

    @rule()
    def respond_op(self) -> None:
        """Model an addressed-messaging response: close an open correlation.

        Picks an arbitrary currently-open correlation and marks it
        closed. The round-trip invariant then verifies the closure
        leaves the model in a consistent state -- no
        ``respond`` on a never-opened or already-closed correlation.
        """
        if not self._open_requests:
            return
        # Pick the lexicographically first open correlation so the
        # rule is deterministic relative to the Hypothesis-generated
        # rule order; the model's correctness is independent of which
        # correlation we close on this step.
        correlation_id = sorted(self._open_requests)[0]
        sender, _recipient = self._open_requests.pop(correlation_id)
        self._closed_correlations.add(correlation_id)
        self._recipient_inboxes.setdefault(sender, []).append(correlation_id)

    @rule(agent_id=text(min_size=1, max_size=12))
    def subscribe_to_op(self, agent_id: str) -> None:
        """Model an SDK ``subscribe(to=agent_id)`` recipient-inbox registration.

        Adds the agent to ``recipient_inboxes`` so the round-trip
        invariant treats it as a valid receiver target. Idempotent
        on repeated registrations: a second ``subscribe(to=X)`` on the
        same agent must not corrupt the inbox state.
        """
        if "\n" in agent_id:
            return
        self._recipient_inboxes.setdefault(agent_id, [])

    @rule(fd=live_subs)
    def double_close_is_idempotent(self, fd: int) -> None:
        """A second ``_close_subscriber`` on the same fd must not decrement the gauge again."""
        if fd in self._daemon.subscribers:
            self._daemon._close_subscriber(fd, reason="shutdown")
            self._closed_fds.add(fd)
        gauge_before = self._metrics.SUBSCRIBER_COUNT.value()
        self._daemon._close_subscriber(fd, reason="shutdown")
        gauge_after = self._metrics.SUBSCRIBER_COUNT.value()
        assert gauge_after == gauge_before, (
            f"double _close_subscriber decremented SUBSCRIBER_COUNT a second time "
            f"(fd={fd}, before={gauge_before}, after={gauge_after})"
        )

    @invariant()
    def addressed_messaging_round_trip(self) -> None:
        """Every closed correlation must have been opened first, and no double-close.

        The invariant catches two model-level failure modes that would
        each indicate a bus-level addressed-messaging regression: (a) a
        response for a correlation_id that was never registered as an
        open request (orphan response), and (b) a correlation_id that
        appears in both the open-requests map and the closed set at the
        same time (double respond / leaked request).
        """
        overlap = self._closed_correlations & self._open_requests.keys()
        assert not overlap, (
            f"correlation(s) appear in both open and closed sets, indicating an "
            f"orphan response or a double-respond: {sorted(overlap)!r}"
        )

    @invariant()
    def subscriber_count_consistent(self) -> None:
        """``SUBSCRIBER_COUNT`` gauge must equal ``len(daemon.subscribers)``."""
        gauge = int(self._metrics.SUBSCRIBER_COUNT.value())
        actual = len(self._daemon.subscribers)
        assert gauge == actual, f"SUBSCRIBER_COUNT gauge ({gauge}) diverged from len(daemon.subscribers) ({actual})"

    @invariant()
    def no_fd_minus_one_in_map(self) -> None:
        """The subscriber map must never contain key ``-1``.

        A ``-1`` key arises when ``sock.fileno()`` is called after the
        socket is closed -- the old wrong-key-pop bug. The leak left a
        real subscriber entry in the map forever.
        """
        assert -1 not in self._daemon.subscribers, (
            "subscriber map contains key -1 (post-close fileno() leaked into map)"
        )

    @invariant()
    def pre_ack_buffer_within_bounds(self) -> None:
        """Every registered subscriber's pre-ack buffer must be within its caps."""
        from waitbus import broadcast as bc

        for fd, sub in self._daemon.subscribers.items():
            if sub.pre_ack_buffer is None:
                continue
            frame_count = len(sub.pre_ack_buffer)
            byte_count = sub.pre_ack_buffered_bytes
            assert frame_count <= bc.PRE_ACK_BUFFER_FRAMES, (
                f"fd={fd}: pre_ack_buffer has {frame_count} frames (cap={bc.PRE_ACK_BUFFER_FRAMES})"
            )
            assert byte_count <= bc.PRE_ACK_BUFFER_BYTES, (
                f"fd={fd}: pre_ack_buffered_bytes={byte_count} exceeds cap={bc.PRE_ACK_BUFFER_BYTES}"
            )

    @invariant()
    def evicted_subscriber_tx_buffer_empty(self) -> None:
        """A subscriber that has left the daemon map must hold no buffered tx bytes.

        Retaining unsent bytes past eviction would orphan memory and leave a
        stale watcher target; ``_close_subscriber`` must discard the buffer
        with the connection.
        """
        for fd in self._closed_fds:
            sub = self._subs.get(fd)
            if sub is None:
                continue
            assert sub.tx_buffered_bytes() == 0, (
                f"fd={fd}: evicted subscriber retains {sub.tx_buffered_bytes()} buffered tx bytes"
            )
        for fd, sub in self._subs.items():
            if fd in self._daemon.subscribers:
                continue
            assert sub.tx_buffered_bytes() == 0, (
                f"fd={fd}: subscriber absent from the map retains {sub.tx_buffered_bytes()} buffered tx bytes"
            )

    @invariant()
    def close_reason_wire_contract(self) -> None:
        """Non-lag close reasons must never write a reject frame on the wire.

        Lag-class reasons (those in ``_TERMINAL_REJECT_FRAMES``) attempt
        a best-effort frame send before closing -- ``broadcast.py`` wraps
        the ``sendall`` in ``contextlib.suppress(OSError)`` because the
        subscriber is by definition already EAGAIN-saturated; the wire
        emission may legitimately fail. The strict half of the contract
        is on the negative side: a non-lag close (``replay_db_error``,
        ``shutdown``, ``subscribe_ack_send_failed``, ...) must not emit
        a reject frame at all.
        """
        from waitbus import broadcast as bc

        terminal_frames = set(bc._TERMINAL_REJECT_FRAMES.values())
        for fd, reason in self._close_reasons.items():
            if reason in bc._TERMINAL_REJECT_FRAMES:
                continue
            frames_sent = self._wire_frames.get(fd, [])
            assert all(f not in terminal_frames for f in frames_sent), (
                f"fd={fd} reason={reason!r}: non-lag close emitted a reject frame "
                f"that should only appear on lag-class closes: {frames_sent!r}"
            )

    @invariant()
    def reject_reasons_in_consumer_taxonomy(self) -> None:
        """Every lag-class wire frame's ``reason`` is in the consumer-facing taxonomy."""
        from waitbus import broadcast as bc
        from waitbus._broadcast_sub import _REJECT_REASON_EXCEPTIONS
        from waitbus._frame import _LENGTH_PREFIX_BYTES

        for fd, reason in self._close_reasons.items():
            if reason not in bc._TERMINAL_REJECT_FRAMES:
                continue
            for raw in self._wire_frames.get(fd, []):
                if len(raw) <= _LENGTH_PREFIX_BYTES:
                    continue
                payload = raw[_LENGTH_PREFIX_BYTES:]
                try:
                    decoded = json.loads(payload.decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                wire_reason = decoded.get("reason")
                assert wire_reason in _REJECT_REASON_EXCEPTIONS, (
                    f"fd={fd}: wire frame has reason={wire_reason!r} which is not in "
                    f"the consumer-facing taxonomy {set(_REJECT_REASON_EXCEPTIONS)!r}"
                )


BroadcastDrainMachine.TestCase.settings = _STATE_MACHINE_SETTINGS
TestBroadcastDrainMachine = BroadcastDrainMachine.TestCase
