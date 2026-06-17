"""Public subscribe SDK: ``wait_for`` / ``subscribe`` / ``asubscribe``.

The symmetric consumer-side partner to the public ``emit()`` producer API.
A Python agent (Pydantic AI, LangGraph, a plain script) blocks on, or
iterates, broadcast events with zero polling -- without shelling out to
``waitbus wait`` per event or hand-decoding the AF_UNIX wire.

Local-only boundary: this module is intentionally local -- no relay,
network-coordination, or auth fields. It MUST stay purely local --
no relay / account / multi-tenant / network-coordination parameters,
behaviour, or imports. A separate transport can layer its own delivery
*on top of* this (yielding the same :class:`EventFrame`) without reaching
into this module. The ``socket_path`` argument is the sole seam: a
local proxy daemon is reached by passing a different path, with zero change
to the logic here. ``tests/test_subscribe_local_boundary.py`` enforces the
no-network-symbol contract by AST walk (mirroring
``tests/test_sourcespec_local_boundary.py``).

Concurrency: the engine (:func:`_broadcast_sub.await_predicate`) is a
SYNCHRONOUS blocking ``select`` loop, so the sync API owns no event loop and
has no ``asyncio.run`` re-entrancy hazard. All three entry points share one
drain path (:func:`_drain_one`). :func:`asubscribe` bridges to asyncio via a
dedicated worker thread that hands each item to the loop with a real blocking
``run_coroutine_threadsafe(...).result()`` (genuine backpressure, never a
fire-and-forget ``put_nowait``); the hand-off carries a tagged union so a typed
subscribe-reject is forwarded and re-raised in the consumer rather than
swallowed. Teardown closes the socket (unblocking the worker's blocking read)
and drains the queue while joining the worker, so it cannot deadlock.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import json
import socket
import threading
from collections.abc import AsyncIterator, Callable, Generator, Sequence
from typing import Any, Final

import msgspec

from . import _predicate
from ._broadcast_sub import (
    BroadcastConnectionError,
    FrameDecision,
    SubscriberHandle,
    WaitOutcome,
    await_predicate,
    open_subscriber,
)
from ._compose import AllOfTracker, clause_predicate, parse_clause
from ._frame import EventFrame

__all__ = ("EventFrame", "asubscribe", "subscribe", "wait_for")

# End-of-stream sentinel. The asubscribe worker's thread->loop hand-off carries
# a tagged union over ONE queue: an EventFrame (a match), a BaseException (a
# forwarded typed reject / decode error, re-raised in the consumer), or this
# sentinel (clean EOF).
_STREAM_DONE: Final = object()

# Bounded hand-off queue: the worker blocks on a real ``put`` (via
# run_coroutine_threadsafe(...).result()) when the async consumer falls behind
# the wire, so the wire exerts genuine backpressure instead of unbounded growth.
_ASUBSCRIBE_QUEUE_MAXSIZE: Final = 256

# Backstop join for the worker thread on teardown. The consumer drains the queue
# while joining, so the worker reaches its sentinel ``put`` and exits well under
# this bound; it is daemon=True, so a pathological timeout is reaped at
# interpreter exit rather than hanging the loop.
_WORKER_JOIN_TIMEOUT: Final = 5.0


def _compose_predicate(
    match: str | Sequence[str] | None,
    source: str | None,
    to: str | None = None,
) -> _predicate.Predicate:
    """Compose the match spec(s) and any ``source`` / ``to`` filter into one predicate.

    Mirrors ``wait._build_predicate``'s source-as-predicate treatment
    (``--source`` narrows the match table, not just the daemon subscription)
    but sits one layer lower: it calls ``_predicate.parse_match`` directly so
    a malformed spec raises ``ValueError`` to the SDK caller rather than
    exiting the process (the CLI wrapper is what maps that to exit-2).

    ``to`` is the addressed-messaging inbox filter: a predicate over the wire
    ``fields.msg_to`` projected from the agent-message addressing facet, so
    ``subscribe(to="agent_b")`` yields only the messages addressed to agent_b.

    Escaping boundary: ``source`` / ``to`` values are encoded into
    the spec with ``json.dumps`` and decoded back by the matcher with
    ``json.loads``, so a value round-trips by exact equality even if it contains
    quotes or backslashes -- the ``key=json_literal`` grammar is waitbus's Layer-1
    predicate and ``json.dumps`` is its escaping boundary. (``request()`` does
    not route a correlation id through this string grammar at all; it matches the
    decoded frame structurally -- see ``_messaging._reply_decide``.)
    """
    if match is None:
        specs: list[str] = []
    elif isinstance(match, str):
        specs = [match]
    else:
        specs = list(match)
    if source is not None:
        # Same spec shape the CLI emits (`wait._build_predicate`): a predicate
        # over the frame's `fields.source`, JSON-quoted.
        specs.append(f"fields.source={json.dumps(source)}")
    if to is not None:
        specs.append(f"fields.msg_to={json.dumps(to)}")
    if not specs:
        raise ValueError("subscribe/wait_for requires a match spec, a source, or a recipient (to=) (got none)")
    try:
        return _predicate.compose(_predicate.parse_match(specs))
    except KeyError as exc:
        # parse_match can leak a bare KeyError on an unknown field path; the SDK
        # contract is a single ValueError for any malformed spec.
        raise ValueError(f"invalid match spec: {exc}") from exc


def _make_capture_decide(
    composed: _predicate.Predicate,
    captured: list[dict[str, Any]],
) -> Callable[[dict[str, Any]], FrameDecision]:
    """Build the ``await_predicate`` decide closure: match -> capture -> MATCHED.

    NOT ``wait._build_decide`` -- that bakes in the CLI's
    GitHub-conclusion exit-code bucketing. The SDK is the clean primitive:
    the caller's predicate IS the match condition; a matching frame is
    captured and returned verbatim. Truncated frames carry no ``fields`` and
    so never match (consistent with the CLI).
    """

    def _decide(frame: dict[str, Any]) -> FrameDecision:
        fields = frame.get("fields")
        if not isinstance(fields, dict):
            return FrameDecision.CONTINUE
        if not composed(frame):
            return FrameDecision.CONTINUE
        captured.append(frame)
        return FrameDecision.MATCHED

    return _decide


def _to_event(frame: dict[str, Any]) -> EventFrame:
    """Decode a matched wire-frame dict into the frozen public ``EventFrame``.

    ``EventFrame`` is the wire data-frame contract pinned to
    ``CONSUMER_API.md`` §2a -- the right public return type for a wire
    consumer (NOT the DB read-shape ``_types.Event``). ``strict=False`` so an
    additively-introduced future wire field does not break decoding.
    """
    return msgspec.convert(frame, EventFrame, strict=False)


def _drain_one(
    handle: SubscriberHandle,
    composed: _predicate.Predicate,
    *,
    deadline_seconds: float | None,
    raise_on_peer_close: bool = False,
) -> EventFrame | None:
    """Run the engine once; return the next matching event, or ``None`` on EOF.

    The single drain path shared by :func:`wait_for` (one-shot), :func:`subscribe`
    (the sync generator), and :func:`asubscribe`'s worker thread, so all three
    ride the engine's select / deadline / EOF / heartbeat-skip loop (the "ripgrep
    model") and the same error contract. Returns ``None`` when the daemon closed
    the connection, the wait was cancelled (SIGINT), or ``deadline_seconds``
    elapsed with no match. Propagates ``BroadcastConnectionError`` (and
    subclasses) on a daemon subscribe-reject and ``msgspec.ValidationError`` on
    an undecodable matched frame -- the caller chooses to raise (sync) or forward
    across the queue (async).
    """
    captured: list[dict[str, Any]] = []
    outcome = await_predicate(
        handle,
        decide=_make_capture_decide(composed, captured),
        deadline_seconds=deadline_seconds,
    )
    return _finish_drain(outcome, captured, raise_on_peer_close=raise_on_peer_close)


def _finish_drain(
    outcome: WaitOutcome,
    captured: list[dict[str, Any]],
    *,
    raise_on_peer_close: bool,
) -> EventFrame | None:
    """Map an engine outcome + captured frame to the drain contract.

    The single error-contract tail shared by :func:`_drain_one` and
    :func:`_drain_all_of`, so the framing-error / peer-close semantics stay
    single-sourced across the one-shot, streaming, and conjunction paths.
    """
    if outcome.matched and captured:
        return _to_event(captured[0])
    if outcome.framing_error:
        # A protocol violation is a broken connection, not a clean end -- surface
        # it on every path (one-shot and stream) rather than masking it as None.
        raise BroadcastConnectionError(
            "daemon violated the wire framing protocol",
            remediation="A framing error is a wire-protocol bug or a truncated daemon write; check the daemon logs.",
        )
    if raise_on_peer_close and outcome.peer_closed:
        # One-shot path (wait_for / request): a daemon that closed the connection
        # before a match is a DEAD peer (abort), distinct from a timeout / no-match
        # (a slow or absent peer -> None -> retry). A streaming caller leaves
        # raise_on_peer_close unset so a clean close just ends the stream.
        raise BroadcastConnectionError(
            "daemon closed the connection before a matching event arrived",
            remediation="Confirm the broadcast daemon is running and did not restart mid-wait.",
        )
    return None


def _drain_all_of(
    handle: SubscriberHandle,
    tracker: AllOfTracker,
    *,
    deadline_seconds: float | None,
) -> EventFrame | None:
    """Run the engine once over a sticky conjunction; return the COMPLETING frame.

    Each frame folds into the tracker (a clause once satisfied stays
    satisfied); the frame that satisfies the last outstanding clause is
    captured and returned. Same error contract as :func:`_drain_one`
    (via :func:`_finish_drain`, with the one-shot peer-close semantics).
    """
    captured: list[dict[str, Any]] = []

    def _decide(frame: dict[str, Any]) -> FrameDecision:
        fields = frame.get("fields")
        if not isinstance(fields, dict):
            return FrameDecision.CONTINUE
        if not tracker.update(frame):
            return FrameDecision.CONTINUE
        captured.append(frame)
        return FrameDecision.MATCHED

    outcome = await_predicate(handle, decide=_decide, deadline_seconds=deadline_seconds)
    return _finish_drain(outcome, captured, raise_on_peer_close=True)


def _validate_compose_kwargs(
    all_of: Sequence[str] | None,
    first_of: Sequence[str] | None,
    match: str | Sequence[str] | None,
    source: str | None,
    to: str | None,
) -> None:
    """Reject illegal clause-composition keyword combinations (eager ValueError)."""
    if all_of is not None and first_of is not None:
        raise ValueError("all_of and first_of are mutually exclusive")
    if match is not None or source is not None or to is not None:
        compose_name = "all_of" if all_of is not None else "first_of"
        raise ValueError(
            f"{compose_name} cannot be combined with match/source/to; each clause carries its own source scoping"
        )


def _clause_predicates(specs: Sequence[str]) -> list[_predicate.Predicate]:
    """Lower a clause list to per-clause Predicates (eager ValueError on garbage)."""
    if not specs:
        raise ValueError("clause list must be non-empty")
    return [clause_predicate(parse_clause(spec)) for spec in specs]


def _resolve_stream_predicate(
    match: str | Sequence[str] | None,
    source: str | None,
    to: str | None,
    first_of: Sequence[str] | None,
) -> _predicate.Predicate:
    """Pick the stream predicate: a first_of disjunction or the classic composition."""
    if first_of is not None:
        _validate_compose_kwargs(None, first_of, match, source, to)
        return _predicate.compose_any(*_clause_predicates(first_of))
    return _compose_predicate(match, source, to)


async def _drain_forever(queue: asyncio.Queue[object]) -> None:
    """Discard queue items until cancelled.

    Run during :func:`asubscribe` teardown so a worker blocked in ``put`` always
    has a free slot to deliver its remaining item + the EOF sentinel and exit,
    instead of deadlocking against a consumer that has stopped reading.
    """
    while True:
        await queue.get()


def wait_for(
    match: str | Sequence[str] | None = None,
    *,
    source: str | None = None,
    to: str | None = None,
    all_of: Sequence[str] | None = None,
    first_of: Sequence[str] | None = None,
    timeout: float | None = None,
    since: str | None = None,
    socket_path: str | None = None,
) -> EventFrame | None:
    """Block until one event matches ``match`` (and/or ``source``); return it.

    Synchronous and blocking. Returns the matched :class:`EventFrame`, or
    ``None`` if ``timeout`` elapsed or the wait was cancelled (SIGINT) before a
    match -- a slow or absent peer, safe to retry. Raises
    :class:`BroadcastConnectionError` if the daemon closed the connection or
    violated the wire framing mid-wait -- a dead daemon, abort rather than retry.

    Args:
        match: A match spec (``'fields.conclusion="failure"'``) or a sequence
            of specs AND-composed. Same grammar as ``waitbus wait --match``.
        source: Restrict to one source (``"github"``/``"pytest"``/...); added
            as a ``fields.source=`` predicate. At least one of ``match`` /
            ``source`` / ``to`` is required.
        to: Addressed-messaging inbox filter -- restrict to messages whose
            recipient (``fields.msg_to``) equals this agent name.
        all_of: Cross-source ``"source:key=json_literal"`` clauses with STICKY
            conjunction semantics: each clause may be satisfied by a different
            event over time and stays satisfied once matched; the call returns
            the frame that satisfied the LAST outstanding clause. Mutually
            exclusive with ``match`` / ``source`` / ``to`` and ``first_of``.
        first_of: Same clause grammar, single-event disjunction: return the
            FIRST frame matching any clause. Mutually exclusive with
            ``match`` / ``source`` / ``to`` and ``all_of``.
        timeout: Seconds to wait; ``None`` blocks until match / EOF / SIGINT.
        since: ULID cursor for replay; ``None`` starts from now.
        socket_path: Override the broadcast socket path (the local-proxy /
            test seam).

    Raises:
        ValueError: ``match`` is malformed or neither ``match`` nor ``source``
            was given.
        BroadcastConnectionError / subclasses: daemon unavailable or
            unsupported wire version.
        msgspec.ValidationError: a matched frame could not be decoded into
            :class:`EventFrame` (rare; ``strict=False`` tolerates additive wire
            fields).

    An async caller bridges via ``await asyncio.to_thread(wait_for, ...)``.
    """
    if all_of is not None or first_of is not None:
        _validate_compose_kwargs(all_of, first_of, match, source, to)
        clauses = _clause_predicates(all_of if all_of is not None else first_of or ())
        handle = open_subscriber(since=since, socket_path=socket_path)
        try:
            if all_of is not None:
                return _drain_all_of(handle, AllOfTracker(clauses), deadline_seconds=timeout)
            composed = _predicate.compose_any(*clauses)
            return _drain_one(handle, composed, deadline_seconds=timeout, raise_on_peer_close=True)
        finally:
            handle.sock.close()
    composed = _compose_predicate(match, source, to)
    handle = open_subscriber(since=since, socket_path=socket_path)
    try:
        return _drain_one(handle, composed, deadline_seconds=timeout, raise_on_peer_close=True)
    finally:
        handle.sock.close()


def subscribe(
    match: str | Sequence[str] | None = None,
    *,
    source: str | None = None,
    to: str | None = None,
    first_of: Sequence[str] | None = None,
    since: str | None = None,
    socket_path: str | None = None,
) -> Generator[EventFrame, None, None]:
    """Yield each matching event as it arrives (a synchronous generator).

    Streams by re-entering the one-shot engine per event, so the engine still
    owns the ``select`` / deadline / EOF / heartbeat-skip loop (the "ripgrep
    model"); this generator never re-implements it. Iteration ends on a clean
    daemon close or a SIGINT cancellation; a wire-framing violation raises
    :class:`BroadcastConnectionError` (a broken connection, not a clean end).

    The generator owns the socket and closes it on exhaustion OR when the
    caller closes the generator (``gen.close()`` / leaving a ``for`` loop /
    ``contextlib.closing``). For deterministic teardown prefer
    ``with contextlib.closing(subscribe(...)) as events:``.

    Cancellation is single-threaded: ``gen.close()`` interrupts a
    ``select``-blocked generator only when called from the *owning* thread. A
    generator blocked in ``select`` on another thread will not observe a
    cross-thread ``close()`` -- to cancel from elsewhere, run the stream on a
    thread you can join, or use :func:`asubscribe` (async) or
    ``asyncio.to_thread(wait_for, ...)`` instead.

    Args/Raises: as :func:`wait_for` (minus ``timeout`` -- a stream has no
    one-shot deadline; stop by closing the generator). ``subscribe(to="agent_b")``
    is the addressed-messaging inbox: every message addressed to agent_b.
    ``first_of`` streams every event matching ANY clause, across sources;
    there is no ``all_of`` on a stream -- a sticky conjunction is one-shot
    semantics (use :func:`wait_for`).
    """
    composed = _resolve_stream_predicate(match, source, to, first_of)
    handle = open_subscriber(since=since, socket_path=socket_path)
    try:
        while True:
            event = _drain_one(handle, composed, deadline_seconds=None)
            if event is None:
                # peer_closed / cancelled: the stream is over.
                return
            yield event
    finally:
        handle.sock.close()


async def asubscribe(
    match: str | Sequence[str] | None = None,
    *,
    source: str | None = None,
    to: str | None = None,
    first_of: Sequence[str] | None = None,
    since: str | None = None,
    socket_path: str | None = None,
) -> AsyncIterator[EventFrame]:
    """Async generator yielding each matching event (for async agents).

    Runs the synchronous engine on a dedicated worker thread that pushes
    matched events onto a bounded :class:`asyncio.Queue`. On normal
    exhaustion, or when the consumer stops early (``break`` / ``aclose()`` /
    task cancellation), the worker is torn down deterministically: the socket
    is closed from this coroutine, which unblocks the worker's blocking read
    so it cannot deadlock waiting for a frame that never comes.

    Args/Raises: as :func:`subscribe`. Predicate composition (and any
    ``ValueError``) happens eagerly before the worker starts.
    """
    composed = _resolve_stream_predicate(match, source, to, first_of)
    loop = asyncio.get_running_loop()
    # One bounded queue carries the tagged union {EventFrame | BaseException |
    # _STREAM_DONE}. Opened in the coroutine (not the worker) so teardown can
    # close the socket to unblock the worker's blocking read.
    output: asyncio.Queue[object] = asyncio.Queue(maxsize=_ASUBSCRIBE_QUEUE_MAXSIZE)
    handle: SubscriberHandle = open_subscriber(since=since, socket_path=socket_path)

    def _put(item: object) -> bool:
        """Blocking hand-off into the loop's queue -- real backpressure.

        Blocks the worker thread until the bounded ``put`` completes inside the
        loop, so a full queue suspends the worker instead of dropping the item.
        Returns ``False`` if the loop is gone or the put was cancelled during
        teardown (the only non-fatal outcomes), so the worker stops quietly.
        """
        try:
            asyncio.run_coroutine_threadsafe(output.put(item), loop).result()
            return True
        except (RuntimeError, concurrent.futures.CancelledError):
            return False

    def _worker() -> None:
        try:
            while True:
                try:
                    event = _drain_one(handle, composed, deadline_seconds=None)
                except BroadcastConnectionError as exc:
                    _put(exc)  # forward the typed reject; the consumer re-raises it
                    return
                except OSError:
                    return  # socket closed during teardown -- expected, fall to EOF
                except Exception as exc:  # forward decode / other engine errors
                    _put(exc)
                    return
                if event is None:
                    return  # peer_closed / cancelled
                if not _put(event):
                    return  # consumer / loop gone
        finally:
            _put(_STREAM_DONE)

    worker = threading.Thread(target=_worker, name="waitbus-asubscribe", daemon=True)
    worker.start()
    try:
        while True:
            item = await output.get()
            if isinstance(item, EventFrame):
                yield item
            elif isinstance(item, BaseException):
                raise item  # forwarded typed reject / decode error
            else:
                return  # _STREAM_DONE
    finally:
        # Close the socket to unblock the worker's blocking read, then drain the
        # queue CONTINUOUSLY while joining: a worker blocked in put() needs a
        # free slot to land its remaining item + the EOF sentinel, and a single
        # pre-join drain can deadlock if the queue refills before the join.
        with contextlib.suppress(OSError):
            handle.sock.shutdown(socket.SHUT_RDWR)
        handle.sock.close()
        drainer = asyncio.ensure_future(_drain_forever(output))
        try:
            await loop.run_in_executor(None, worker.join, _WORKER_JOIN_TIMEOUT)
        finally:
            drainer.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await drainer
