"""Filesystem -> waitbus watcher (``watchdog``, opt-in ``[fs]`` extra).

What it does
------------
Watches a directory tree and, when a file is *finished being written*
(an atomic close-write or a moved-in temp->final rename — the standard
"editor saved a file" shape), builds a write-shape
:class:`waitbus._types.EventInsert` (``source="fs"``)
and calls :func:`waitbus.emit`.

Dependency choice: ``watchdog``, not hand-rolled inotify/kqueue/FSEvents
-----------------------------------------------------------------------
Hand-rolling cross-platform filesystem watching is an anti-pattern.
inotify (Linux), kqueue (BSD/macOS) and FSEvents (macOS) have
incompatible event models, ordering, coalescing and overflow semantics,
and ``watchdog`` already provides a correct portable abstraction over
them (the de-facto Python file-watching library). waitbus sits on it
rather than re-deriving it. ``watchdog`` is therefore a real
dependency — but an **opt-in** one: it is declared under
``[project.optional-dependencies].fs`` (mirroring the existing
``analyze``/duckdb extra exactly), imported **lazily** inside
:func:`watch`, so a plain ``pip install waitbus`` stays
stdlib-plus-sqlite3 and is completely unaffected.

If ``[fs]`` is not installed, :func:`watch` raises a clear, actionable
:class:`FsWatchDependencyError` ("install waitbus[fs]") rather than an
opaque ``ModuleNotFoundError`` — it does not silently no-op.

Atomic-save semantics (documented)
----------------------------------
Most editors and many tools save by writing a temporary file and
``rename(2)``-ing it over the target. A naive ``on_modified`` watcher
fires repeatedly mid-write on the temp file and may emit a half-written
state. This watcher reacts only to terminal signals:

* ``FileClosedEvent`` (``IN_CLOSE_WRITE`` on Linux — the file was
  written and the writer closed it; the canonical "it is now complete"
  signal); and
* ``FileMovedEvent`` whose destination is inside the watched tree
  (``IN_MOVED_TO`` — the temp->final atomic rename).

Bare ``on_modified`` / ``on_created`` are **not**
emitted, so the temp-file churn of an atomic save is ignored and
exactly one event fires per completed save.

Coalescing / overflow caveat (documented)
-----------------------------------------
Kernel watch queues are bounded; under a burst (e.g. a large
``rsync``/checkout) inotify can drop events (``IN_Q_OVERFLOW``) and
``watchdog`` coalesces rapid repeats. This watcher therefore guarantees
*at-least-eventually* per-file signalling, not one-event-per-syscall.
The idempotent ``delivery_id`` (path + mtime-ns) makes a coalesced
duplicate a no-op. A dropped overflow event is a known, accepted bound:
unlike the broadcast doorbell -- whose delay-not-loss guarantee is
backed by a durable SQLite row that survives a missed wakeup -- inotify
has no durable backing, so a file touched exactly once whose sole event
is the dropped one is genuine loss. The *at-least-eventually* guarantee
holds for files that see subsequent activity, which re-triggers a watch
event.
"""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

import msgspec

from .. import _emit as emit_mod
from .._types import EventInsert

if TYPE_CHECKING:  # pragma: no cover - typing only
    from watchdog.events import FileSystemEvent

_INGEST_METHOD = "fs_watch"
_EVENT_TYPE = "fs_change"

#: Window over which observer-thread events are coalesced (by
#: ``delivery_id``) and flushed as one batched emit. Bounds the
#: per-event sqlite-connection cost under a burst (rsync / checkout)
#: while keeping completed-save latency sub-second.
_FS_DEBOUNCE_SECONDS = 0.5


class FsWatchDependencyError(RuntimeError):
    """Raised when :func:`watch` is called but the optional ``watchdog``
    dependency (the ``[fs]`` extra) is not installed. Carries the exact
    install command rather than surfacing a bare ImportError."""


def _require_watchdog() -> Any:
    """Import ``watchdog`` lazily; raise an actionable error if absent.

    Keeps the core install free of ``watchdog`` (the ``analyze``/duckdb
    pattern): the import only happens when an operator actually runs the
    fs watcher.
    """
    try:
        import watchdog.events as wd_events
        import watchdog.observers as wd_observers
    except ModuleNotFoundError as exc:  # pragma: no cover - env-dependent
        raise FsWatchDependencyError(
            "the fs watcher needs the optional 'watchdog' dependency. "
            "Install it with:  pip install 'waitbus[fs]'  (or "
            "uv tool install 'waitbus[fs]'). Hand-rolling inotify/kqueue/"
            "FSEvents is not done — waitbus sits on the "
            "battle-tested watchdog library."
        ) from exc
    return wd_events, wd_observers


def _build_event(path: str, *, owner: str, repo: str) -> EventInsert | None:
    """Build the EventInsert for a completed write to ``path``.

    ``delivery_id`` is ``fs:<abspath>:<mtime_ns>`` — deterministic, so a
    coalesced duplicate notification for the same on-disk state is an
    idempotent no-op while a genuine subsequent save (new mtime) is a
    distinct event. Returns None if the path vanished between the
    notification and the ``stat`` (a transient temp that was removed) —
    nothing terminal happened, so nothing is emitted.
    """
    try:
        st = os.stat(path)
    except OSError:
        return None
    abspath = os.path.abspath(path)
    mtime_ns = st.st_mtime_ns
    received_at = int(time.time_ns())
    # msgspec is the single JSON encoder used across the sources (pytest /
    # docker also use it), so the payload is correctly escaped rather than
    # hand-rolled.
    payload = msgspec.json.encode({"path": abspath, "size": st.st_size, "mtime_ns": mtime_ns}).decode()
    return EventInsert(
        delivery_id=f"fs:{abspath}:{mtime_ns}",
        source="fs",
        event_type=_EVENT_TYPE,
        owner=owner,
        repo=repo,
        received_at=received_at,
        payload_json=payload,
        ingest_method=_INGEST_METHOD,
        status="completed",
        conclusion="success",
        workflow_name=os.path.basename(abspath) or None,
    )


class _Debouncer:
    """Coalesce observer-thread fs events and flush them as one batch.

    The watchdog observer thread only ever does the cheap
    ``add(insert)`` (a dict assignment under a lock); a daemon thread
    flushes the accumulated inserts via :func:`waitbus._emit.emit_batch`
    every :data:`_FS_DEBOUNCE_SECONDS`. Buffering is keyed by
    ``delivery_id`` (last-wins): ``delivery_id`` already encodes
    ``path + mtime_ns``, so two notifications for the same on-disk state
    collapse to one row before the DB is ever touched, and a genuine
    later save (new mtime) is a distinct key. :meth:`stop` flushes any
    remainder so SIGINT / a test stop never drops a completed save.
    """

    def __init__(self, db_path: Path | None) -> None:
        self._db_path = db_path
        self._pending: dict[str, EventInsert] = {}
        self._lock = threading.Lock()
        self._stop_evt = threading.Event()
        self._thread = threading.Thread(target=self._run, name="waitbus-fs-debounce", daemon=True)

    def start(self) -> None:
        """Start the background flush loop."""
        self._thread.start()

    def add(self, insert: EventInsert) -> None:
        """Buffer one insert (observer-thread side; no DB I/O here)."""
        with self._lock:
            self._pending[insert.delivery_id] = insert

    def flush(self) -> None:
        """Drain the buffer and emit it as one batch (DB I/O off-lock)."""
        with self._lock:
            if not self._pending:
                return
            batch = list(self._pending.values())
            self._pending.clear()
        emit_mod.emit_batch(batch, db_path=self._db_path)

    def _run(self) -> None:
        """Background loop: flush every ``_FS_DEBOUNCE_SECONDS`` until stopped.

        The loop wakes either on the debounce timer expiring (normal
        case -- flush whatever the observer thread enqueued during
        the window) or on ``_stop_evt`` being set (shutdown case --
        the predicate returns True and the loop exits without a
        final flush; :meth:`stop` performs the final flush on the
        caller's thread).
        """
        while not self._stop_evt.wait(_FS_DEBOUNCE_SECONDS):
            self.flush()

    def stop(self) -> None:
        """Stop the loop, join the thread, and flush the final remainder."""
        self._stop_evt.set()
        if self._thread.is_alive():
            self._thread.join(timeout=5.0)
        self.flush()


def _make_handler(base_cls: Any, *, owner: str, repo: str, sink: Callable[[EventInsert], None]) -> Any:
    """Build a watchdog handler that reacts only to terminal signals.

    ``on_closed`` (IN_CLOSE_WRITE) and ``on_moved`` (IN_MOVED_TO) are
    the only hooks implemented; ``on_modified``/``on_created`` are left
    as the no-op base so atomic-save temp churn is ignored entirely.
    Each terminal event builds an :class:`EventInsert` and hands it to
    ``sink`` (the debouncer's ``add`` in production) -- the handler
    itself never touches the store, keeping the observer thread off the
    DB-connection path.
    """

    class _Handler(base_cls):  # type: ignore[misc]
        """Closure-bound watchdog event handler for the active watch."""

        def _emit(self, path: str | bytes) -> None:
            """Build the EventInsert for ``path`` and hand it to ``sink``.

            Returns silently when :func:`_build_event` declines the path
            (e.g. the file vanished between notification and stat).
            """
            text = path.decode() if isinstance(path, bytes) else path
            insert = _build_event(text, owner=owner, repo=repo)
            if insert is not None:
                sink(insert)

        def on_closed(self, event: FileSystemEvent) -> None:
            """IN_CLOSE_WRITE hook: emit on the writer's completing close."""
            if not event.is_directory:
                self._emit(event.src_path)

        def on_moved(self, event: FileSystemEvent) -> None:
            """IN_MOVED_TO hook: emit on the destination of an atomic save.

            Atomic save = write temp + rename over target. The
            destination is the completed file; the source temp is not a
            terminal state and is ignored.
            """
            dest = getattr(event, "dest_path", "")
            if dest and not event.is_directory:
                self._emit(dest)

    return _Handler()


def watch(
    path: str | Path,
    *,
    recursive: bool = True,
    owner: str = "local",
    repo: str = "fs",
    db_path: Path | None = None,
    stop_event: threading.Event | None = None,
    _stop_after: float | None = None,
) -> int:
    """Watch ``path`` and emit one event per completed file write.

    Blocks until SIGINT (returns ``130``), until ``stop_event`` is set by
    another thread (returns ``0``; an embedding supervisor's graceful-stop
    seam), or, for tests, until ``_stop_after`` seconds elapse (returns
    ``0``). All three exits route through the same ``finally`` (observer
    stop/join, debouncer stop with a final flush) so a completed save is
    never dropped on shutdown. Raises :class:`FsWatchDependencyError`
    immediately if the ``[fs]`` extra is not installed (clear, actionable
    — never a silent no-op).
    """
    wd_events, wd_observers = _require_watchdog()
    target = Path(path)
    if not target.exists():
        raise FileNotFoundError(f"watch path does not exist: {target}")
    debouncer = _Debouncer(db_path)
    debouncer.start()
    handler = _make_handler(
        wd_events.FileSystemEventHandler,
        owner=owner,
        repo=repo,
        sink=debouncer.add,
    )
    observer = wd_observers.Observer()
    observer.schedule(handler, str(target), recursive=recursive)
    observer.start()
    try:
        if _stop_after is not None:
            time.sleep(_stop_after)
            return 0
        if stop_event is not None:
            stop_event.wait()
            return 0
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        return 130
    finally:
        observer.stop()
        observer.join(timeout=5.0)
        debouncer.stop()
