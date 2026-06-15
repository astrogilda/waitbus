"""Shared subscriber-open helper for broadcast tap and replay commands.

Opens an authenticated AF_UNIX SOCK_STREAM connection to the broadcast
daemon and sends the subscribe frame. The returned socket is blocking
and ready for ``_frame.sync_read_frame`` calls.

Token lookup order:
1. ``token`` kwarg (explicit override, for tests).
2. ``WAITBUS_BROADCAST_TOKEN`` environment variable.
3. ``$CREDENTIALS_DIRECTORY/broadcast-token`` (systemd-creds, runtime).

Bookmark mechanism:
Named bookmarks persist the last-consumed event ID so a subscriber can
resume exactly where it left off on reconnect. A bookmark is an arbitrary
operator-chosen name (``^[A-Za-z0-9_.-]+$``); its cursor file is owned by
a ``BookmarkCursor`` instance which enforces name validation and heartbeat
filtering at the advance() boundary. ``open_subscriber`` accepts
``bookmark_id`` to load and inject the cursor automatically at subscribe time.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import select
import socket
import tempfile
import time
from collections.abc import Callable
from enum import Enum
from typing import Any, Final, NamedTuple, NoReturn

import msgspec

from . import _secrets
from ._frame import (
    DRAINABLE_CONTROL_KINDS,
    FRAME_PROTO_VERSION,
    encode_frame,
    sync_read_frame,
)
from ._paths import broadcast_socket, cursors_dir

_BOOKMARK_ID_RE = re.compile(r"^[A-Za-z0-9_.\-]+$")


class SubscriberHandle(msgspec.Struct, frozen=True):
    """An authenticated broadcast-subscriber connection.

    Returned by :func:`open_subscriber` and consumed by
    :func:`await_predicate` (and any caller layered on top, e.g.
    :func:`coalesce.coalesce_replay`).

    Attributes:
        sock: A connected, blocking AF_UNIX SOCK_STREAM socket. The
            caller owns its lifecycle (close via ``sock.close()`` or a
            ``contextlib`` wrapper); :func:`await_predicate` never
            closes it.

    Under wire protocol v1 the daemon's terminal handshake frame is
    either ``subscribe_rejected`` (token/version failure -> the read
    engine raises) or ``subscribe_ack`` (registration confirmed, carrying
    the replay/live ``caught_up_at`` watermark). Both are handled by the
    shared read engine, so :func:`open_subscriber` performs no client-side
    probe and the handle carries no pre-read frame.
    """

    sock: socket.socket


class BookmarkCursor:
    """Owns one named bookmark's load/advance/persist lifecycle.

    Subsumes load_bookmark, save_bookmark, and _validate_bookmark_id.
    Filters heartbeat frames at the advance() boundary so subscribers
    cannot accidentally persist a heartbeat ULID as their resume cursor:
    heartbeat frames carry a fresh ULID per tick, so persisting one would
    advance the cursor past real events committed in the heartbeat-
    interval window and skip them on the next reconnect.
    """

    __slots__ = ("_last", "_path", "name")

    def __init__(self, name: str) -> None:
        """Bind to the bookmark file ``cursors_dir()/bookmark-{name}.txt``.

        Validates ``name`` (raises ValueError on empty / bad chars) and creates
        the parent directory at mode 0o700. The cursor value is loaded lazily.
        """
        self.validate_name(name)
        self.name = name
        self._path = cursors_dir() / f"bookmark-{name}.txt"
        self._path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._last: str | None = None

    @staticmethod
    def validate_name(name: str) -> None:
        """Validate a bookmark name; empty and bad-char are distinct operator errors."""
        if not name:
            raise ValueError("bookmark name must be a non-empty string")
        if not _BOOKMARK_ID_RE.match(name):
            raise ValueError(f"bookmark name {name!r} contains characters outside [A-Za-z0-9_.-]")

    def load(self) -> str | None:
        """Return the persisted cursor, or None if no bookmark yet."""
        try:
            cursor = self._path.read_text(encoding="utf-8").strip() or None
        except FileNotFoundError:
            cursor = None
        self._last = cursor
        return cursor

    @classmethod
    def resolve_since(cls, explicit: str | None, name: str | None) -> tuple[str | None, BookmarkCursor | None]:
        """Apply the shared since-cursor precedence in one place.

        An explicit cursor wins; otherwise a named bookmark's stored
        cursor is loaded. Returns ``(effective_since, handle)`` where
        ``handle`` is the live cursor to ``advance()`` per frame and is
        non-None only when the cursor came from ``name`` (an explicit
        cursor yields no handle to advance). ``name`` validation
        (:meth:`validate_name`, via the constructor) raises
        ``ValueError`` here -- before any socket I/O -- exactly as the
        previously-inlined form did.
        """
        if explicit is not None:
            return explicit, None
        if name is not None:
            handle = cls(name)
            return handle.load(), handle
        return None, None

    def advance(self, frame: dict[str, Any]) -> None:
        """Persist the frame's ULID as the new cursor — UNLESS it is a
        heartbeat (which carries a fresh ULID per tick and would clobber
        the real-event cursor). Atomic rename via tempfile + os.replace.
        """
        if frame.get("kind") == "daemon_heartbeat":
            return
        # Only event-bearing frames carry event_id; control frames
        # (subscribe_ack / subscribe_rejected / daemon_heartbeat) have none
        # and are skipped by the guard below so the cursor never advances
        # onto a non-event.
        event_id = frame.get("event_id")
        if not isinstance(event_id, str) or not event_id:
            return
        fd, tmp = tempfile.mkstemp(prefix=".bookmark-", suffix=".tmp", dir=str(self._path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(event_id)
            os.replace(tmp, self._path)
        except OSError:
            with contextlib.suppress(OSError):
                os.unlink(tmp)
            raise
        self._last = event_id

    @property
    def current(self) -> str | None:
        """Cached last-read or last-written value; None until load() or advance() fires."""
        return self._last


class BroadcastConnectionError(OSError):
    """Raised when the broadcast daemon socket cannot be reached.

    Carries a ``remediation`` attribute so callers can emit a consistent
    operator hint without duplicating the message string.
    """

    def __init__(self, message: str, remediation: str) -> None:
        super().__init__(message)
        self.remediation = remediation


class TokenRequiredError(BroadcastConnectionError):
    """Raised when the daemon rejects the subscribe with ``reason="token"``.

    A subclass of :class:`BroadcastConnectionError`: a token rejection is a
    connection-level auth failure surfaced by the daemon's ``subscribe_rejected``
    frame, not a programmer error. Callers that catch ``BroadcastConnectionError``
    catch this transparently; callers wanting token-specific remediation may
    catch it by name.
    """


class ProtocolVersionError(BroadcastConnectionError):
    """Raised when the daemon rejects the subscribe with ``reason="version"``.

    The client's wire ``proto`` is unsupported by the daemon. Subclass of
    :class:`BroadcastConnectionError`; remediation is to align client/daemon
    wire-protocol versions.
    """


class SubscriberLaggedError(BroadcastConnectionError):
    """Raised when the daemon drops the subscriber with ``reason="lag_limit_exceeded"``.

    The subscriber could not keep up with the fan-out, pre-ack drain, or
    heartbeat cadence and was evicted. Subclass of
    :class:`BroadcastConnectionError`; remediation is to reconnect with backoff,
    narrower filters, or a ``since`` cursor.
    """


# Maps each ``subscribe_rejected`` wire reason to its typed exception. The
# keys are exactly the consumer-facing reasons in CONSUMER_API.md §3
# (enforced by tests/test_broadcast_exception_mapping.py). An unknown future
# reason falls to the base ``BroadcastConnectionError`` — NOT ``TokenRequiredError``;
# defaulting to token mislabels lag/version drops as auth failures. Internal
# faults (e.g. the daemon's ``replay_db_error``) close the socket silently and
# never reach this map.
_REJECT_REASON_EXCEPTIONS: Final[dict[str, type[BroadcastConnectionError]]] = {
    "token": TokenRequiredError,
    "version": ProtocolVersionError,
    "lag_limit_exceeded": SubscriberLaggedError,
}


def _raise_for_reject(frame: dict[str, Any]) -> NoReturn:
    """Raise the typed exception for a decoded ``subscribe_rejected`` frame.

    The daemon writes one terminal reject frame, then FINs. Map each
    documented wire reason to its typed exception so a consumer gets
    reason-appropriate remediation. Every type subclasses
    :class:`BroadcastConnectionError`, so a caller catching the base catches
    all of them. An unknown future reason falls to the base (NOT token) --
    defaulting to token mislabels lag/version drops as auth failures. The
    single shared mapping keeps the handshake reader and the streaming
    engine from drifting apart.
    """
    reason = frame.get("reason", "token")
    if not isinstance(reason, str):
        # A malformed frame can carry an unhashable reason (list/dict);
        # coerce any non-string to a printable placeholder so the dict
        # lookup below cannot raise a raw TypeError and the frame still
        # maps to the unknown-reason base exception.
        reason = f"<non-string:{type(reason).__name__}>"
    remediation = str(
        frame.get("remediation") or "Verify the broadcast token and wire protocol version match the daemon."
    )
    exc_cls = _REJECT_REASON_EXCEPTIONS.get(reason, BroadcastConnectionError)
    raise exc_cls(f"broadcast subscribe rejected (reason={reason!r})", remediation=remediation)


def _resolve_token(explicit: str | None) -> str | None:
    """Return the broadcast token from the first available source.

    Lookup order:
    1. ``explicit`` kwarg (test override / caller-supplied value).
    2. ``WAITBUS_BROADCAST_TOKEN`` environment variable.
    3. ``_secrets.get_secret(\"broadcast-token\")`` — the canonical
       credential reader; honours
       ``WAITBUS_SECRETS_BACKEND={systemd-creds|age}`` so the
       subscriber side picks up the age backend automatically when an
       operator has configured one (parity with the daemon side).

    Returns ``None`` when no source produces a token. A misconfigured
    backend (age binary missing, age identity unset, etc.) raises
    ``SecretNotConfigured`` from ``_secrets.get_secret`` — the
    subscriber side fails loud rather than silently degrading to
    no-auth.
    """
    if explicit is not None:
        return explicit
    env_token = os.environ.get("WAITBUS_BROADCAST_TOKEN")
    if env_token:
        return env_token
    return _secrets.get_secret("broadcast-token")


def open_subscriber(
    *,
    filters: list[str] | None = None,
    event_types: list[str] | None = None,
    since: str | None = None,
    token: str | None = None,
    socket_path: str | None = None,
    bookmark_id: str | None = None,
) -> SubscriberHandle:
    """Open and authenticate a broadcast subscriber socket.

    Connects to the broadcast daemon, sends the subscribe frame with
    the supplied parameters, and returns a :class:`SubscriberHandle`
    wrapping the connected blocking socket. The caller is responsible
    for closing ``handle.sock``. Frames are read via
    :func:`_frame.sync_read_frame` or through :func:`await_predicate`
    (which skips the daemon's control frames and raises on a reject).

    When ``bookmark_id`` is given the function loads the saved cursor
    from ``cursors_dir() / "bookmark-{bookmark_id}.txt"`` and injects it
    as the ``since`` value in the subscribe envelope (unless the caller
    also passes an explicit ``since``, which takes precedence). After
    subscribe use ``BookmarkCursor(bookmark_id).advance(frame)`` for each
    frame; heartbeat frames are silently skipped by ``advance``.

    Args:
        filters: List of ``owner/repo`` or ``owner/*`` or ``*`` patterns.
            Defaults to ``["*"]`` (all repos) when ``None``.
        event_types: List of event-type strings to subscribe to.
            Defaults to all supported types when ``None``.
        since: ULID cursor for replay. ``None`` means subscribe from now
            unless ``bookmark_id`` supplies a stored cursor.
        token: Explicit bearer token. Overrides env / creds-dir lookup.
        socket_path: Override the default broadcast socket path (tests).
        bookmark_id: Persistent bookmark name. Must match
            ``^[A-Za-z0-9_.-]+$``. When set and no ``since`` is given,
            the stored cursor (if any) is used as the replay starting
            point.

    Returns:
        A :class:`SubscriberHandle` wrapping a connected, blocking
        AF_UNIX SOCK_STREAM socket. The handle is sock-only; the
        daemon's deterministic ``subscribe_ack`` / ``subscribe_rejected``
        handshake (read by ``await_predicate``) is the registration
        signal -- the handle does not stash any pre-read frame.

    Raises:
        ValueError: ``bookmark_id`` contains characters outside
            ``[A-Za-z0-9_.-]`` or is empty.
        BroadcastConnectionError: The daemon socket is absent, refuses
            the connection, or the subscribe-frame send fails.

    Note: a token or wire-version rejection is NOT raised here. The daemon
    answers a bad token / unsupported ``proto`` with a ``subscribe_rejected``
    control frame; :func:`await_predicate` (the read engine) raises the typed
    ``TokenRequiredError`` / ``BroadcastConnectionError`` when the caller
    reads — there is no synchronous open-time probe.
    """
    # Resolve the effective ``since`` cursor before opening the socket so
    # a ValueError from bookmark validation surfaces before any I/O.
    effective_since, _ = BookmarkCursor.resolve_since(since, bookmark_id)

    path = socket_path if socket_path is not None else str(broadcast_socket())
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.setblocking(True)
    try:
        sock.connect(path)
    except (FileNotFoundError, ConnectionRefusedError) as exc:
        sock.close()
        raise BroadcastConnectionError(
            f"broadcast socket at {path} unavailable ({type(exc).__name__})",
            remediation=("Start the broadcast daemon via `systemctl --user start waitbus-broadcast.service`"),
        ) from exc

    subscribe: dict[str, object] = {"proto": FRAME_PROTO_VERSION}
    if filters is not None:
        subscribe["filters"] = filters
    if event_types is not None:
        subscribe["event_types"] = event_types
    if effective_since is not None:
        subscribe["since"] = effective_since

    resolved_token = _resolve_token(token)
    if resolved_token is not None:
        subscribe["token"] = resolved_token

    try:
        sock.sendall(encode_frame(json.dumps(subscribe).encode("utf-8")))
    except OSError as exc:
        sock.close()
        raise BroadcastConnectionError(
            f"subscribe frame send failed: {exc}",
            remediation=("Start the broadcast daemon via `systemctl --user start waitbus-broadcast.service`"),
        ) from exc

    # No client-side probe. Under wire protocol v1 the daemon's terminal
    # handshake frame is deterministic: ``subscribe_rejected`` (token,
    # version, or lag-limit failure, then FIN) or ``subscribe_ack``
    # (registration confirmed, structurally the first frame on the wire).
    # The shared read engine (``await_predicate``) recognises both — raising
    # a typed ``TokenRequiredError`` / ``BroadcastConnectionError`` on a
    # reject and skipping the ack as a control frame — so there is no
    # timing window to probe and no first-frame to stash.
    return SubscriberHandle(sock=sock)


class FrameDecision(Enum):
    """A caller's verdict on one decoded non-heartbeat frame.

    ``CONTINUE``  -- frame consumed; keep streaming.
    ``MATCHED``   -- the wait condition is satisfied; stop, ``matched=True``.

    The engine owns the read/select/deadline loop (the ripgrep model:
    one engine owns the loop, callers push only the decision). Callers
    never see the socket, the deadline, or EOF/framing handling.
    """

    CONTINUE = "continue"
    MATCHED = "matched"


class WaitOutcome(NamedTuple):
    """Structured result of :func:`await_predicate` (no exit codes here).

    The primary decision axis is the Temporal-shaped triple
    ``(matched, timed_out, cancelled)`` -- exactly one is True on a
    terminal outcome unless the peer closed first, in which case all
    three are False and ``peer_closed`` is True. The CLI verb (or MCP
    projection) maps the outcome to an exit code or response; no
    ``typer.Exit`` is raised inside the primitive.

    ``peer_closed`` / ``framing_error`` are EOF-detail metadata, not a
    fourth/fifth decision axis: ``framing_error`` implies ``peer_closed``
    (a protocol violation *is* a connection loss) and distinguishes a
    daemon that violated the length-prefix framing (``ConnectionError``
    from ``sync_read_frame``) from a graceful zero-byte EOF. Every legacy
    loop conflated or hand-rolled this; surfacing it once in the engine
    lets ``read_events.watch`` keep its documented 0 (clean EOF) vs 1
    (framing error) split and gives ``waitbus wait`` the same signal for
    free (a greenfield improvement over the three divergent loops).

    The triple is widened from Temporal's ``(ok, err)`` so SIGINT
    teardown is first-class rather than smuggled through an error value.
    """

    matched: bool
    timed_out: bool
    cancelled: bool
    peer_closed: bool
    framing_error: bool


# Immutable terminal-outcome singletons. NamedTuple instances are
# frozen, so sharing one per terminal reason is safe and avoids
# reconstructing identical tuples on every loop exit.
_OUTCOME_TIMED_OUT = WaitOutcome(
    matched=False,
    timed_out=True,
    cancelled=False,
    peer_closed=False,
    framing_error=False,
)
_OUTCOME_CANCELLED = WaitOutcome(
    matched=False,
    timed_out=False,
    cancelled=True,
    peer_closed=False,
    framing_error=False,
)
_OUTCOME_MATCHED = WaitOutcome(
    matched=True,
    timed_out=False,
    cancelled=False,
    peer_closed=False,
    framing_error=False,
)
_OUTCOME_CLEAN_EOF = WaitOutcome(
    matched=False,
    timed_out=False,
    cancelled=False,
    peer_closed=True,
    framing_error=False,
)
_OUTCOME_FRAMING_EOF = WaitOutcome(
    matched=False,
    timed_out=False,
    cancelled=False,
    peer_closed=True,
    framing_error=True,
)


def read_subscribe_ack(handle: SubscriberHandle, *, timeout_seconds: float = 5.0) -> None:
    """Block until the daemon's ``subscribe_ack`` arrives; raise on reject / EOF.

    Reads exactly one frame off the wire and asserts the deterministic wire-v1
    handshake: ``subscribe_ack`` (registration confirmed -- structurally the
    first frame on the wire) returns; ``subscribe_rejected`` raises the typed
    reason exception via the same :data:`_REJECT_REASON_EXCEPTIONS` mapping
    :func:`await_predicate` uses; an EOF or any other first frame raises
    :class:`BroadcastConnectionError`.

    This is the **registration barrier** for callers that must confirm the
    subscriber is registered server-side BEFORE producing an event on a separate
    path -- otherwise the emit could race registration and the daemon's fan-out
    would not yet include this subscriber. The ``demo`` and ``swarm-demo``
    synthesizers are exactly that case: they emit (via :func:`emit`, a separate
    SQLite + doorbell path) immediately after subscribing. Pure-streaming
    consumers do NOT need this -- :func:`await_predicate` skips the ack as a
    drainable control frame.

    Synchronous and blocking by design (it mirrors :func:`await_predicate`,
    which is also sync); an ``asyncio`` caller wraps it in
    :func:`asyncio.to_thread`. The caller owns ``handle.sock``'s lifecycle.

    Socket-timeout contract: this temporarily sets ``handle.sock``'s timeout to
    ``timeout_seconds`` for the one-frame read and restores it to ``None``
    (blocking) on the way out, so a subsequent :func:`await_predicate` on the same
    socket sees the blocking mode it expects. Callers that rely on a non-default
    socket timeout must re-apply it after this returns.
    """
    sock = handle.sock
    sock.settimeout(timeout_seconds)
    try:
        frame_bytes = sync_read_frame(sock)
    except TimeoutError as exc:
        # socket.timeout (an OSError subclass) would otherwise escape every
        # caller's `except BroadcastConnectionError`, leaking the socket the
        # caller closes in that handler. Translate it to the typed exception.
        raise BroadcastConnectionError(
            f"daemon did not send subscribe_ack within {timeout_seconds:g}s",
            remediation="Confirm the broadcast daemon is running and accepting subscribers.",
        ) from exc
    finally:
        sock.settimeout(None)
    if frame_bytes is None:
        raise BroadcastConnectionError(
            "daemon closed the connection before sending subscribe_ack",
            remediation="Confirm the broadcast daemon is running and accepting subscribers.",
        )
    try:
        frame = json.loads(frame_bytes)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        # A raw decode error would escape every caller's
        # `except BroadcastConnectionError`, leaking the socket the caller
        # closes in that handler. Translate it to the typed exception (the
        # streaming engine guards its decode the same way).
        raise BroadcastConnectionError(
            "daemon sent an undecodable first frame instead of subscribe_ack",
            remediation="This indicates a wire-protocol violation by the broadcast daemon.",
        ) from exc
    if not isinstance(frame, dict):
        # Valid JSON that is not an object (e.g. an array) would raise
        # AttributeError at frame.get -- the same escape class.
        raise BroadcastConnectionError(
            f"expected a JSON object as the first frame on the wire; got {type(frame).__name__}",
            remediation="This indicates a wire-protocol violation by the broadcast daemon.",
        )
    kind = frame.get("kind")
    if kind == "subscribe_ack":
        return
    if kind == "subscribe_rejected":
        _raise_for_reject(frame)
    raise BroadcastConnectionError(
        f"expected subscribe_ack as the first frame on the wire; got kind={kind!r}",
        remediation="This indicates a wire-protocol violation by the broadcast daemon.",
    )


def _always_continue(_frame: dict[str, Any]) -> FrameDecision:
    """Default ``await_predicate`` predicate: treat every non-heartbeat frame
    as a wake signal and never match, so the engine returns only on deadline,
    peer-close, or SIGINT. Housekeeping consumers (e.g. ``pr_monitor``) that
    re-aggregate from the local cache on each wake use this instead of a
    hand-rolled always-CONTINUE closure."""
    return FrameDecision.CONTINUE


def await_predicate(
    sub: SubscriberHandle,
    *,
    decide: Callable[[dict[str, Any]], FrameDecision] = _always_continue,
    deadline_seconds: float | None,
    cursor: BookmarkCursor | None = None,
    idle_reset: bool = False,
) -> WaitOutcome:
    """Read frames off ``sock`` until a caller predicate matches, the
    deadline elapses, the peer closes, or SIGINT arrives.

    This is the single shared egress engine. It is extracted from the
    bounded ``select`` + shrinking-deadline loop (NOT an unbounded
    ``recv``): the deadline is **self-enforced** by recomputing the
    ``select`` budget every iteration against one overall monotonic
    cutoff. That is the load-bearing correctness property -- an
    ``asyncio.to_thread`` worker blocked in ``recv()`` cannot be
    cancel-killed, so the primitive must bound *itself*. The MCP path
    relies on this: the worker is guaranteed to terminate by the
    deadline with no leaked thread.

    Frame handling (engine-owned, identical for every caller):

    * ``daemon_heartbeat`` frames are liveness pings -- consumed off the
      wire (so the socket does not back up) but never passed to
      ``decide``, never advance ``cursor``, and never count as activity
      for ``idle_reset``. This preserves the server-side replay->live
      watermark guarantee: every real frame, including the
      watermark-clearing first live frame, is read in order and handed
      to ``decide``; nothing is dropped client-side and no gap is
      introduced between ``open_subscriber`` returning and the first
      read.
    * Undecodable frames (bad UTF-8 / JSON) are skipped silently, as the
      three legacy loops did.
    * Every decoded non-heartbeat frame is handed to ``decide`` and, if
      ``cursor`` is set, advances it (heartbeat-safe via
      ``BookmarkCursor.advance``).

    Args:
        sub: A :class:`SubscriberHandle` from :func:`open_subscriber`.
            The caller owns ``sub.sock``'s lifecycle; this function
            never closes it.
        decide: Caller predicate invoked once per decoded non-heartbeat
            frame. Returning :data:`FrameDecision.MATCHED` stops the
            loop with ``matched=True``; any side effect the caller wants
            (printing the frame, counting) happens inside ``decide``.
        deadline_seconds: Single overall wall-clock budget measured from
            entry. ``<= 0`` returns immediately with ``timed_out=True``.
            ``None`` means *no deadline* -- the engine blocks on
            ``select`` indefinitely (the ``broadcast tap`` smoke-test
            semantics: stream until ``--count`` / EOF / SIGINT). A
            ``None`` deadline can never produce ``timed_out=True``.
            ``idle_reset`` is ignored when ``deadline_seconds is None``.
        cursor: Optional resume bookmark advanced after every decoded
            non-heartbeat frame (the unified cursor model).
        idle_reset: When True the deadline is measured from the last
            non-heartbeat frame rather than from entry -- the
            replay "caught up after N idle seconds" semantics. When
            False the deadline is a single fixed overall budget -- the
            ``waitbus wait`` / MCP ``tail_events`` semantics.

    Returns:
        A :class:`WaitOutcome`. No ``typer.Exit`` is raised here.
    """
    if deadline_seconds is not None and deadline_seconds <= 0:
        return _OUTCOME_TIMED_OUT

    sock = sub.sock
    anchor = time.monotonic()
    try:
        while True:
            if deadline_seconds is None:
                select_budget: float | None = None
            else:
                select_budget = deadline_seconds - (time.monotonic() - anchor)
                if select_budget <= 0:
                    return _OUTCOME_TIMED_OUT

            ready, _, _ = select.select([sock], [], [], select_budget)
            if not ready:
                # Only reachable with a finite budget (select returns
                # empty only on timeout); None blocks until readable.
                return _OUTCOME_TIMED_OUT

            try:
                data: bytes | None = sync_read_frame(sock)
            except ConnectionError:
                # Daemon violated the length-prefix framing mid-stream.
                return _OUTCOME_FRAMING_EOF
            if data is None:
                # Graceful zero-byte EOF (daemon shut down / closed sub).
                return _OUTCOME_CLEAN_EOF

            try:
                frame: dict[str, Any] = json.loads(data.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue

            kind = frame.get("kind")
            if kind in DRAINABLE_CONTROL_KINDS:
                # Control frames: the liveness ping and the post-replay
                # registration ack (which carries ``caught_up_at``). Both
                # are drained off the wire but never passed to ``decide``,
                # never advance the cursor, and never count as idle-reset
                # activity. This preserves the replay->live watermark
                # ordering: the ack lands after replay, and every real
                # frame is still handed to ``decide`` in order.
                continue

            if kind == "subscribe_rejected":
                _raise_for_reject(frame)

            if cursor is not None:
                cursor.advance(frame)

            if idle_reset:
                anchor = time.monotonic()

            if decide(frame) is FrameDecision.MATCHED:
                return _OUTCOME_MATCHED
    except KeyboardInterrupt:
        # SIGINT is first-class via WaitOutcome.cancelled; SIGTERM is
        # NOT trapped here on purpose: every caller of this engine is a
        # session-level / interactive subscriber (waitbus wait, waitbus
        # replay, waitbus broadcast tap, waitbus --watch, the MCP tail_events
        # to_thread worker), none of which is supervised by a systemd
        # unit.
        # Installing a signal handler in a library function would also
        # leak process-global state into the library API boundary; the MCP
        # worker thread cannot install signal handlers anyway. The MCP
        # path is bounded by the self-enforced ``deadline_seconds``.
        return _OUTCOME_CANCELLED


def emit_frame(frame: dict[str, object], *, as_json: bool) -> None:
    """Write one frame to stdout in the requested format.

    JSON mode: ``json.dumps`` compact output, one frame per line.
    Text mode: ``<id>  <kind>  <owner>/<repo>  <summary>``
    """
    if as_json:
        print(json.dumps(frame, separators=(",", ":"), default=str), flush=True)
        return
    event_id = frame.get("event_id", "")
    label = str(frame.get("event_type") or frame.get("kind", ""))
    owner = frame.get("owner", "")
    repo = frame.get("repo", "")
    summary = frame.get("summary", "")
    if owner and repo:
        print(f"{event_id}  {label}  {owner}/{repo}  {summary}", flush=True)
    else:
        # Control / truncated frames omit owner/repo.
        print(f"{event_id}  {label}  {summary}", flush=True)


def _emit_predicate(
    on_frame: Callable[[dict[str, Any]], None],
    *,
    count: int | None = None,
) -> Callable[[dict[str, Any]], FrameDecision]:
    """Build an ``await_predicate`` ``decide`` that runs ``on_frame`` for
    each frame and continues, signalling ``MATCHED`` once ``count`` frames
    have been emitted (``None`` streams until the engine's own EOF / idle
    / SIGINT terminus).

    Factors the emit-then-CONTINUE/MATCHED control flow that ``replay``,
    ``broadcast tap``, and ``--watch`` each previously hand-rolled, so the
    three differ only by their per-frame ``on_frame`` side effect.
    """
    seen = 0

    def _decide(frame: dict[str, Any]) -> FrameDecision:
        nonlocal seen
        on_frame(frame)
        seen += 1
        if count is not None and seen >= count:
            return FrameDecision.MATCHED
        return FrameDecision.CONTINUE

    return _decide


# run_typer_app moved to cli/_shared.py (the CLI-side, next to
# _exit_with_error) so this engine module carries NO typer surface --
# it is a pure typed-API module the public API contract keeps
# relay/account/network-free. Subscriber CLIs import it from there.
