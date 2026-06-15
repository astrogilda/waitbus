"""Docker Events -> waitbus watcher (stdlib-only, no ``docker`` SDK).

What it does
------------
Streams the Docker Engine ``GET /events`` API over the Unix domain
socket ``/var/run/docker.sock`` using only :mod:`http.client` /
:mod:`socket` / :mod:`json` (stdlib). On a container ``die`` or ``stop``
event it builds a write-shape :class:`waitbus._types.EventInsert`
(``source="docker"``) and calls
:func:`waitbus.emit` — one row per container exit, with the
container exit code mapped onto the bus's ``success``/``failure``
conclusion vocabulary so a ``waitbus wait`` predicate works unchanged.

No ``docker`` Python SDK dependency is taken: the SDK pulls
``requests`` + ``urllib3`` + ``websocket-client`` and a large API
surface for what is one long-lived chunked GET. The Engine API is a
plain HTTP/1.1 endpoint on a Unix socket; the stdlib speaks it directly.

Cursor / resume
---------------
The Engine API supports ``since`` / ``until`` query parameters (Unix
epoch seconds). The watcher passes the last-seen event's ``time`` as
``since`` when it reconnects after a transport drop, so an event-stream
interruption does not silently lose the window between disconnect and
reconnect (Engine replays the gap). The idempotent ``delivery_id``
(derived from the container id + the event's nanosecond timestamp)
makes a replayed overlap a no-op.

Socket-permission prerequisite (documented)
-------------------------------------------
``/var/run/docker.sock`` is ``root:docker`` mode ``0660`` on a stock
install. The process running this watcher must be ``root`` or in the
``docker`` group, or be pointed at a rootless/TCP endpoint via
``--socket``. A clear, actionable error is raised if the socket is
absent or permission is denied — the watcher does not silently no-op.
"""

from __future__ import annotations

import contextlib
import http.client
import json
import logging
import socket
import threading
import time
from collections.abc import Generator
from pathlib import Path
from typing import Any

from .. import _emit as emit_mod
from .._log import structured
from .._types import NS_PER_SECOND, EventInsert

logger = logging.getLogger("waitbus.docker")

_DEFAULT_SOCK = "/var/run/docker.sock"
_INGEST_METHOD = "docker_events"
_EVENT_TYPE = "docker_container"
# Reconnect backoff bounds. Mirrors the capped-exponential shape of
# ``etag_poll._do_conditional_get`` (stamina wait_initial=1.0,
# wait_max=30.0): start at 1 s, double per consecutive failure, never
# sleep longer than 30 s so a wedged socket is retried at a steady
# operator-visible cadence rather than spun on at 1 Hz forever.
_RECONNECT_BACKOFF_BASE_S = 1.0
_RECONNECT_BACKOFF_CEIL_S = 30.0
# Container lifecycle actions that map to a terminal "the workload
# finished" signal. "die" carries the exit code; "stop"/"kill" are an
# operator-initiated terminal transition. "start"/"create" are
# deliberately ignored — this watcher reports completions, not launches.
_TERMINAL_ACTIONS = frozenset({"die", "stop", "kill"})


class DockerSocketError(RuntimeError):
    """Raised with an actionable message when the docker socket is
    unreachable (absent path, ``PermissionError``, or refused
    connection). Never swallowed into a silent no-op."""


def _shutdown_socket(conn: http.client.HTTPConnection) -> None:
    """Half-close the connection's transport to wake a blocked reader.

    ``shutdown(SHUT_RDWR)``, deliberately NOT ``close()``, and for two
    reasons: on Linux a ``close()`` from another thread does not wake a
    thread already blocked in ``recv`` on that fd, and a cross-thread
    ``close()`` races ``http.client``'s own response-buffer teardown
    mid-read (observed as ``AttributeError: 'NoneType' ... close`` from
    ``_read_chunked``). The half-close turns the in-flight blocking
    read into an immediate EOF; the reading thread then owns the real
    ``close()`` (the ``finally`` in :func:`_iter_event_lines`). Errors
    are suppressed: the socket may already be dead, and the caller only
    needs "no longer readable".
    """
    sock = getattr(conn, "sock", None)
    if sock is not None:
        with contextlib.suppress(OSError):
            sock.shutdown(socket.SHUT_RDWR)


class WatchStopper:
    """Cross-thread stop seam for :func:`watch`.

    ``stop()`` flips the stop flag and wakes any in-flight blocking
    ``/events`` read by shutting down (half-closing) the registered
    connection's socket from the calling thread. The woken read
    surfaces inside :func:`watch` as EOF / ``IncompleteRead`` /
    ``OSError``; the loop sees the flag set and returns 0 instead of
    treating the wake as a transport drop. The same flag also bounds
    the reconnect-backoff sleep, so a stop request during a backoff
    window is honored immediately rather than after up to 30 s.
    """

    def __init__(self) -> None:
        self._event = threading.Event()
        self._lock = threading.Lock()
        self._conn: http.client.HTTPConnection | None = None

    @property
    def stopped(self) -> bool:
        """True once :meth:`stop` has been called."""
        return self._event.is_set()

    def wait(self, timeout: float) -> bool:
        """Block up to ``timeout`` seconds; True when stop was requested."""
        return self._event.wait(timeout)

    def stop(self) -> None:
        """Request stop and wake the in-flight blocking read, if any."""
        self._event.set()
        with self._lock:
            conn, self._conn = self._conn, None
        if conn is not None:
            _shutdown_socket(conn)

    def _register(self, conn: http.client.HTTPConnection) -> None:
        """Track the live connection so ``stop()`` can wake its read.

        ``stop()`` may run between the watch loop's flag check and this
        registration; re-checking the flag under the lock guarantees the
        connection is shut down on that interleaving too, instead of
        blocking forever on a read nobody will ever wake.
        """
        with self._lock:
            self._conn = conn
            lost_race = self._event.is_set()
        if lost_race:
            _shutdown_socket(conn)

    def _clear(self) -> None:
        """Drop the registration once the read loop owns cleanup again."""
        with self._lock:
            self._conn = None


class _UnixHTTPConnection(http.client.HTTPConnection):
    """``http.client`` connection whose transport is an AF_UNIX socket.

    The Docker Engine API is HTTP/1.1 over a Unix domain socket;
    ``http.client`` only needs its ``.sock`` swapped for a connected
    ``AF_UNIX`` socket and the rest of the HTTP machinery (chunked
    transfer decoding for the streamed ``/events`` body) works as-is.
    """

    def __init__(self, unix_path: str, *, timeout: float | None = None) -> None:
        """Bind the unix-socket HTTPConnection adapter to ``unix_path``."""
        super().__init__("localhost", timeout=timeout)
        self._unix_path = unix_path

    def connect(self) -> None:
        """Open the AF_UNIX transport, mapping socket errors to DockerSocketError."""
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        if self.timeout is not None:
            sock.settimeout(self.timeout)
        # On any connect failure the just-created socket must be closed
        # here -- it never reaches self.sock, so http.client's own close()
        # cannot reach it and it would otherwise leak to GC finalization.
        try:
            sock.connect(self._unix_path)
        except FileNotFoundError as exc:
            sock.close()
            raise DockerSocketError(
                f"docker socket {self._unix_path!r} does not exist; "
                "is the Docker Engine running? Point --socket at a "
                "rootless/TCP endpoint if you use one."
            ) from exc
        except PermissionError as exc:
            sock.close()
            raise DockerSocketError(
                f"permission denied opening docker socket "
                f"{self._unix_path!r}; run as root or add the user to "
                "the 'docker' group (the socket is root:docker 0660 on "
                "a stock install)."
            ) from exc
        except (ConnectionRefusedError, OSError) as exc:
            sock.close()
            raise DockerSocketError(f"cannot connect to docker socket {self._unix_path!r}: {exc}") from exc
        self.sock = sock


def _iter_event_lines(
    socket_path: str,
    *,
    since: int | None,
    until: int | None,
    stopper: WatchStopper | None = None,
) -> Generator[bytes, None, None]:
    """Yield raw JSON lines from the Engine ``GET /events`` stream.

    The Engine writes one JSON object per line for the lifetime of the
    connection. ``since``/``until`` (epoch seconds) bound the replay
    window on (re)connect. The response body is read line-wise so a
    long-lived stream is processed incrementally with no buffering of
    the (unbounded) body.

    When ``stopper`` is given, the live connection is registered with
    it for the duration of the stream so a cross-thread ``stop()`` can
    wake the blocking read. The connection is closed on every exit
    path of the generator (callers ``close()`` it explicitly when they
    abandon it unexhausted).
    """
    query = []
    if since is not None:
        query.append(f"since={since}")
    if until is not None:
        query.append(f"until={until}")
    path = "/events"
    if query:
        path += "?" + "&".join(query)
    conn = _UnixHTTPConnection(socket_path)
    conn.connect()
    if stopper is not None:
        stopper._register(conn)
    try:
        conn.request("GET", path, headers={"Host": "localhost"})
        resp = conn.getresponse()
        if resp.status != 200:
            body = resp.read(512)
            raise DockerSocketError(f"docker /events returned HTTP {resp.status}: {body!r}")
        buf = b""
        while True:
            chunk = resp.read(4096)
            if not chunk:
                break
            buf += chunk
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                if line.strip():
                    yield line
    finally:
        if stopper is not None:
            stopper._clear()
        conn.close()


def _event_epoch_s(message: dict[str, Any]) -> int | None:
    """Epoch *seconds* for one Engine event, ``timeNano`` taking precedence.

    Single source of truth for the event clock. ``_build_event`` derives
    ``received_at`` (ns) and ``watch`` derives the reconnect ``since``
    cursor (s) from the SAME precedence — ``timeNano`` (epoch ns) when a
    positive int, else ``time`` (epoch s) — so the persisted timestamp
    and the replay cursor cannot drift apart across a transport drop.
    Returns ``None`` when the message carries neither usable field.
    """
    time_nano = message.get("timeNano")
    if isinstance(time_nano, int) and time_nano > 0:
        return time_nano // NS_PER_SECOND
    t = message.get("time")
    if isinstance(t, int):
        return t
    return None


def _conclusion_for(action: str, exit_code_raw: Any) -> str:
    """Map a terminal container action to the bus conclusion vocabulary.

    A ``die`` is graded by its ``exitCode``: ``int(exit_code_raw) == 0``
    -> ``success``, any other numeric -> ``failure``. ``exitCode`` is
    coerced with ``int(...)`` so the Engine's documented string form
    (``"0"``) and a numeric form both classify; absent / non-numeric on
    a ``die`` -> ``failure`` (conservative — an exit we cannot prove was
    clean is not reported as ``success``). An operator ``stop``/``kill``
    is ``cancelled`` (a deliberate non-failure termination).
    """
    if action != "die":
        return "cancelled"
    try:
        return "success" if int(exit_code_raw) == 0 else "failure"
    except (TypeError, ValueError):
        return "failure"


def _received_at_ns(message: dict[str, Any]) -> int:
    """Event clock in ns, ``timeNano``-over-``time`` (shared precedence).

    Same ordering as :func:`_event_epoch_s` (which the reconnect cursor
    uses) so the persisted timestamp and the replay cursor share one
    source of truth. The raw ``timeNano`` ns value is kept (not
    truncated through epoch-s) because the ``delivery_id`` encodes it;
    falls back to ``time`` (epoch s) then wall-clock when absent.
    """
    time_nano = message.get("timeNano")
    if isinstance(time_nano, int) and time_nano > 0:
        return time_nano
    epoch_s = _event_epoch_s(message)
    if epoch_s is not None:
        return epoch_s * NS_PER_SECOND
    return int(time.time()) * NS_PER_SECOND


def _build_event(message: dict[str, Any], *, owner: str, repo: str) -> EventInsert | None:
    """Map one Engine event message to an EventInsert, or None to skip.

    Only container ``die``/``stop``/``kill`` actions are emitted (a
    terminal "workload finished" signal). The ``die`` event's
    ``Actor.Attributes.exitCode`` (when present) drives the
    GitHub-style conclusion: ``0`` -> ``success``, non-zero ->
    ``failure``; an operator ``stop``/``kill`` with no exit code is
    ``cancelled`` (it is a deliberate non-failure termination).

    Conclusion grading (incl. ``exitCode`` coercion) is in
    :func:`_conclusion_for`; the ns event clock and its shared
    ``timeNano``-over-``time`` precedence with the reconnect cursor are
    in :func:`_received_at_ns` / :func:`_event_epoch_s`.
    """
    if message.get("Type") != "container":
        return None
    action = message.get("Action", "")
    # Docker emits health/exec sub-actions like "die" but also
    # "exec_die:..." — only the bare lifecycle actions are terminal.
    if action not in _TERMINAL_ACTIONS:
        return None
    actor = message.get("Actor", {})
    attrs = actor.get("Attributes", {})
    container_id = str(actor.get("ID", ""))
    name = str(attrs.get("name", ""))
    conclusion = _conclusion_for(action, attrs.get("exitCode"))
    received_at = _received_at_ns(message)
    payload = json.dumps(message, separators=(",", ":"))
    return EventInsert(
        delivery_id=f"docker:{container_id or name}:{action}:{received_at}",
        source="docker",
        event_type=_EVENT_TYPE,
        owner=owner,
        repo=repo,
        received_at=received_at,
        payload_json=payload,
        ingest_method=_INGEST_METHOD,
        status="completed",
        conclusion=conclusion,
        workflow_name=name or None,
    )


def _advance_cursor(message: dict[str, Any], current: int | None) -> int | None:
    """Return the reconnect ``since`` cursor after seeing ``message``.

    Resumes one second behind the last-seen event (same
    :func:`_event_epoch_s` precedence the persisted ``received_at``
    uses) so the boundary event is replayed — idempotent via the
    ``delivery_id`` — rather than skipped across a transport drop.
    Leaves the cursor unchanged when the message has no usable clock.
    """
    epoch_s = _event_epoch_s(message)
    if epoch_s is None:
        return current
    return epoch_s - 1


def _reconnect_backoff(attempt: int, *, error: BaseException, socket_path: str, stop: WatchStopper) -> None:
    """Log a structured reconnect warning and sleep capped-exponentially.

    Mirrors ``etag_poll._do_conditional_get``'s resilience shape:
    capped exponential backoff (base 1 s, doubling, 30 s ceiling) plus
    one ``structured(logger, WARNING, ...)`` line per attempt so a
    wedged docker socket is operator-visible instead of a silent 1 Hz
    spin. ``attempt`` is the 1-based consecutive-failure count.

    The sleep waits on ``stop``'s event rather than ``time.sleep`` so a
    stop request that lands during a backoff window unblocks at once —
    otherwise a supervisor teardown could sit behind up to 30 s of
    backoff and overrun its bounded join.

    Carries ``socket_path`` so a multi-source operator can correlate
    the wedge with the unit / config that supplied the path. The key
    name ``socket_path`` is the project convention for AF_UNIX paths
    (vs ``path`` which is reserved for HTTP request paths, per the
    upcoming ``docs/LOGGING_CONVENTIONS.md``).
    """
    delay = min(
        _RECONNECT_BACKOFF_BASE_S * (2.0 ** (attempt - 1)),
        _RECONNECT_BACKOFF_CEIL_S,
    )
    structured(
        logger,
        logging.WARNING,
        "docker_reconnect",
        error=str(error),
        attempt=attempt,
        backoff_s=delay,
        socket_path=socket_path,
    )
    stop.wait(delay)


def watch(
    *,
    socket_path: str = _DEFAULT_SOCK,
    owner: str = "local",
    repo: str = "docker",
    since: int | None = None,
    until: int | None = None,
    db_path: Path | None = None,
    stopper: WatchStopper | None = None,
    _max_events: int | None = None,
) -> int:
    """Stream container-exit events and emit each one. Blocks until EOF/SIGINT/stop.

    Reconnects with an advanced ``since`` cursor (the last-seen event's
    epoch-second time, :func:`_event_epoch_s` precedence) after a
    transport drop so the disconnect window is replayed rather than
    lost; the idempotent ``delivery_id`` makes the replayed overlap a
    no-op. Transient ``OSError`` / ``http.client.HTTPException`` drops
    (a connection cut mid-chunk surfaces as ``IncompleteRead``, an
    ``HTTPException``, not an ``OSError``) back off capped-exponentially
    (1 s -> 30 s ceiling) with a ``docker_reconnect`` warning per
    attempt, so a wedged socket is operator-visible rather than a
    silent 1 Hz spin; the attempt counter resets once the stream
    yields again. ``_max_events`` bounds the loop for tests (emit N
    then return) — it is not a public CLI knob.

    ``stopper`` is the cross-thread stop seam (the ``waitbus serve``
    supervisor holds one): :meth:`WatchStopper.stop` wakes the blocking
    ``/events`` read by shutting down its socket and the loop returns
    ``0``. A private stopper is created when none is given so the loop
    logic has a single shape.

    Returns ``0`` on a clean EOF / ``_max_events`` / requested stop and
    ``130`` on SIGINT (the coreutils SIGINT convention the rest of the
    CLI uses).
    """
    if stopper is None:
        stopper = WatchStopper()
    emitted = 0
    cursor = since
    reconnect_attempt = 0
    try:
        while not stopper.stopped:
            lines = _iter_event_lines(socket_path, since=cursor, until=until, stopper=stopper)
            try:
                for line in lines:
                    reconnect_attempt = 0
                    try:
                        message = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    cursor = _advance_cursor(message, cursor)
                    insert = _build_event(message, owner=owner, repo=repo)
                    if insert is None:
                        continue
                    emit_mod.emit(insert, db_path=db_path)
                    emitted += 1
                    if _max_events is not None and emitted >= _max_events:
                        return 0
            except DockerSocketError:
                raise
            except (OSError, http.client.HTTPException) as exc:
                # A stop-seam wake surfaces as EOF / IncompleteRead /
                # OSError on the shut-down socket: that is a requested
                # stop, not a transport drop, so no reconnect.
                if stopper.stopped:
                    return 0
                if until is not None:
                    return 0
                reconnect_attempt += 1
                _reconnect_backoff(
                    reconnect_attempt,
                    error=exc,
                    socket_path=socket_path,
                    stop=stopper,
                )
                continue
            finally:
                # Explicit close so the connection held inside the
                # generator is released deterministically on every exit
                # path (the _max_events return abandons it unexhausted).
                lines.close()
            if until is not None:
                return 0
        return 0
    except KeyboardInterrupt:
        return 130
