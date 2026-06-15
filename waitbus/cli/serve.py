"""``waitbus serve`` — foreground supervisor over the daemon entry points.

One process that composes the daemons the systemd units run, against the
real platformdirs state directory:

- the broadcast daemon always runs (it is the bus), as an asyncio task;
- the webhook listener runs in a daemon thread only when the
  ``github-webhook-secret`` credential resolves;
- the fs and docker watchers run in daemon threads only when their
  prerequisites hold (a configured ``fs_watch_path``; a connectable
  Docker socket);
- the etag-poll and watchdog single-shot passes run on internal asyncio
  timers when ``--poll`` is set.

Every component prints exactly one startup-manifest line to stdout,
``started (...)`` or ``skipped (<reason>)`` — degrade loudly, never
silently. Structured daemon logs stay on stderr.

This is a supervisor, not a new daemon: it calls the same entry points
the units call (``broadcast.Broadcast.run``, ``listener.build_server`` +
``serve_forever``, ``fs_watch.watch``, ``docker_watch.watch``) so there
is exactly one implementation of binding, schema bootstrap, and shutdown
semantics. Before booting anything it connect-probes the broadcast
socket and refuses to start when a daemon already serves there — the
daemon's manual bind path unlinks an existing socket file and would
otherwise silently steal a running daemon's socket. The probe-then-bind
sequence has a small TOCTOU window: a daemon that binds between the
probe and our bind would still lose its socket file. Closing it would
need a bind-side lock shared with the daemon. The window is tiny and
the failure mode (an operator racing ``waitbus serve`` against
``systemctl start`` on the same box) is operator-visible, so waitbus
leaves it unlocked.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import socket
import sys
import threading
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from socketserver import BaseServer
from typing import TYPE_CHECKING

import typer

if TYPE_CHECKING:  # pragma: no cover - typing only
    from ..broadcast import Broadcast

from .. import _config, _paths, listener
from .._log import structured
from ..sources import docker_watch, fs_watch

logger = logging.getLogger("waitbus.serve")

_COMPONENTS = ("broadcast", "listener", "fs", "docker")

# Poll cadences mirror the installed timer cadences (45 s etag poll,
# 5 min watchdog check) so the foreground supervisor and a unit-managed
# install observe the same backfill freshness.
_ETAG_POLL_PERIOD_S = 45.0
_WATCHDOG_PERIOD_S = 300.0

_TEARDOWN_TIMEOUT_S = 5.0
_BIND_TIMEOUT_S = 10.0

# Budget for the AWAIT on one poll tick. A tick can legitimately run
# long (etag_poll's stamina retry chain stacks a 15 s socket timeout,
# a Retry-After sleep capped at 60 s, and exponential backoff per
# attempt), so the budget sits above one full healthy retry cycle.
# Python cannot kill the thread behind ``asyncio.to_thread``; on
# timeout only the await is abandoned — the thread finishes on its own
# (every HTTP/subprocess call in etag_poll and watchdog_check carries
# an explicit timeout) — so the loop, and teardown after it, are never
# held hostage by one wedged tick.
_POLL_TICK_TIMEOUT_S = 120.0


@dataclass(frozen=True)
class ComponentStatus:
    """One startup-manifest entry: a component either started or was skipped.

    ``detail`` carries the parenthesised suffix — the bind address for a
    started component, or the human-actionable skip reason. The skip
    reasons double as a doctor checklist: every skipped line names the
    exact prerequisite that was missing.
    """

    name: str
    started: bool
    detail: str


def _format_status(status: ComponentStatus) -> str:
    """Render one manifest line: ``serve: <name>: started|skipped (<detail>)``."""
    verb = "started" if status.started else "skipped"
    return f"serve: {status.name}: {verb} ({status.detail})"


def _parse_components(
    subset: str | None,
    *,
    all_components: bool,
    no_listener: bool,
) -> frozenset[str]:
    """Resolve the CLI shape to the set of components to run.

    Exactly one of ``--all`` / an explicit comma-separated subset is
    required. ``broadcast`` is always included (it is the bus every
    other component feeds). ``--no-listener`` removes the listener from
    whatever was selected.
    """
    if all_components and subset is not None:
        raise typer.BadParameter("pass either --all or an explicit component list, not both")
    if not all_components and subset is None:
        raise typer.BadParameter(f"pass --all or an explicit component list (e.g. {','.join(_COMPONENTS[:3])})")
    chosen = set(_COMPONENTS) if all_components else _parse_subset(subset or "")
    chosen.add("broadcast")
    if no_listener:
        chosen.discard("listener")
    return frozenset(chosen)


def _parse_subset(subset: str) -> set[str]:
    """Validate one comma-separated component list against the known names."""
    names = [name.strip() for name in subset.split(",") if name.strip()]
    if not names:
        raise typer.BadParameter("the component list is empty")
    unknown = sorted(set(names) - set(_COMPONENTS))
    if unknown:
        raise typer.BadParameter(f"unknown component(s): {', '.join(unknown)}; choose from {', '.join(_COMPONENTS)}")
    return set(names)


def _probe_broadcast_bound(socket_path: Path, *, timeout: float = 0.5) -> bool:
    """True iff a live broadcast daemon accepts connections at ``socket_path``.

    A connect-probe rather than a path-exists check: a stale socket file
    left by a killed daemon exists on disk but refuses connections, and
    must not block a fresh start. The probe MUST run before constructing
    ``Broadcast`` — the daemon's manual bind path unlinks an existing
    socket file, which would silently steal a running daemon's socket.
    """
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    probe.settimeout(timeout)
    try:
        probe.connect(str(socket_path))
    except OSError:
        return False
    finally:
        probe.close()
    return True


def _start_listener(statuses: list[ComponentStatus]) -> tuple[BaseServer, threading.Thread] | None:
    """Start the webhook listener in a daemon thread when a secret resolves.

    Skips with ``no github-webhook-secret`` when the credential is
    unavailable, and with the bind error when the port is taken — both
    leave the rest of the supervisor running.
    """
    secret = listener.load_secret_optional(listener.GITHUB_WEBHOOK_CRED)
    if secret is None:
        statuses.append(ComponentStatus("listener", False, "no github-webhook-secret"))
        return None
    am_secret = listener.load_secret_optional(listener.ALERTMANAGER_HMAC_CRED)
    try:
        server = listener.build_server(secret, am_secret)
    except OSError as exc:
        statuses.append(
            ComponentStatus("listener", False, f"cannot bind {listener.LISTEN_HOST}:{listener.LISTEN_PORT}: {exc}")
        )
        return None
    thread = threading.Thread(target=server.serve_forever, name="waitbus-serve-listener", daemon=True)
    thread.start()
    bound_port = server.server_address[1]
    statuses.append(ComponentStatus("listener", True, f"{listener.LISTEN_HOST}:{bound_port}"))
    return server, thread


def _start_fs_watch(
    cfg: _config.CiStatusConfig, statuses: list[ComponentStatus]
) -> tuple[threading.Event, threading.Thread] | None:
    """Start the fs watcher in a daemon thread when its prerequisites hold.

    Prerequisites: a configured ``fs_watch_path`` that exists on disk,
    and the optional ``watchdog`` dependency. Returns the stop event +
    thread pair the teardown path uses for a graceful join.
    """
    path = cfg.fs_watch_path
    if path is None:
        statuses.append(ComponentStatus("fs", False, "no fs_watch_path configured"))
        return None
    if not Path(path).exists():
        statuses.append(ComponentStatus("fs", False, f"watch path does not exist: {path}"))
        return None
    try:
        fs_watch._require_watchdog()
    except fs_watch.FsWatchDependencyError as exc:
        statuses.append(ComponentStatus("fs", False, str(exc).splitlines()[0]))
        return None
    stop = threading.Event()
    thread = threading.Thread(
        target=fs_watch.watch,
        args=(path,),
        kwargs={"stop_event": stop},
        name="waitbus-serve-fs",
        daemon=True,
    )
    thread.start()
    statuses.append(ComponentStatus("fs", True, f"watching {path}"))
    return stop, thread


def _start_docker_watch(
    docker_socket: str, statuses: list[ComponentStatus]
) -> tuple[docker_watch.WatchStopper, threading.Thread] | None:
    """Start the docker watcher in a daemon thread when the socket connects.

    Returns the (stopper, thread) handle the teardown path uses for a
    graceful stop: :meth:`docker_watch.WatchStopper.stop` shuts down the
    live ``/events`` socket from the supervisor thread, which wakes the
    watcher's blocking chunked read, and the thread is then joined with
    the same bounded timeout as the other components. ``daemon=True``
    stays on as a backstop so a join that overruns the budget cannot
    block process exit.
    """
    probe = docker_watch._UnixHTTPConnection(docker_socket, timeout=1.0)
    try:
        probe.connect()
    except docker_watch.DockerSocketError as exc:
        statuses.append(ComponentStatus("docker", False, str(exc)))
        return None
    finally:
        probe_sock = getattr(probe, "sock", None)
        if probe_sock is not None:
            probe_sock.close()
    stopper = docker_watch.WatchStopper()
    thread = threading.Thread(
        target=docker_watch.watch,
        kwargs={"socket_path": docker_socket, "stopper": stopper},
        name="waitbus-serve-docker",
        daemon=True,
    )
    thread.start()
    statuses.append(ComponentStatus("docker", True, f"socket {docker_socket}"))
    return stopper, thread


async def _poll_loop(period_s: float, tick: Callable[[], int], name: str) -> None:
    """Run ``tick`` every ``period_s`` seconds until cancelled.

    Each tick is fault-isolated: a failing pass (no GitHub token, no
    watched repos, transient network error) logs a structured warning
    and the next tick retries — a poll failure must never kill the
    supervisor. The await on each tick is bounded by
    ``_POLL_TICK_TIMEOUT_S`` so a wedged tick cannot pin this task (and
    through it, teardown) indefinitely.
    """
    while True:
        await asyncio.sleep(period_s)
        try:
            await asyncio.wait_for(asyncio.to_thread(tick), timeout=_POLL_TICK_TIMEOUT_S)
        except TimeoutError:
            # The timeout bounds only the AWAIT: the tick's thread
            # cannot be killed and runs to completion in the background
            # (its own socket/subprocess timeouts bound that).
            structured(
                logger,
                logging.WARNING,
                "poll_tick_timeout",
                component=name,
                timeout_s=_POLL_TICK_TIMEOUT_S,
            )
        except Exception as exc:
            structured(
                logger,
                logging.WARNING,
                "poll_tick_error",
                component=name,
                error=str(exc),
                error_type=type(exc).__name__,
            )


def _start_poll_timers(poll: bool, statuses: list[ComponentStatus]) -> list[asyncio.Task[None]]:
    """Create the etag-poll and watchdog timer tasks when ``--poll`` is set."""
    if not poll:
        statuses.append(ComponentStatus("poll", False, "--poll not set"))
        return []
    from waitbus import etag_poll, watchdog_check

    tasks = [
        asyncio.create_task(_poll_loop(_ETAG_POLL_PERIOD_S, etag_poll.main, "etag_poll")),
        asyncio.create_task(_poll_loop(_WATCHDOG_PERIOD_S, lambda: watchdog_check.main([]), "watchdog_check")),
    ]
    statuses.append(
        ComponentStatus("poll", True, f"etag {int(_ETAG_POLL_PERIOD_S)}s, watchdog {int(_WATCHDOG_PERIOD_S)}s")
    )
    return tasks


async def _await_socket(
    path: Path,
    timeout_s: float,
    daemon_task: asyncio.Task[int],
    stop: asyncio.Event,
) -> bool:
    """Wait until the broadcast socket accepts connections.

    Connect-probes rather than checking ``path.exists()`` — a stale
    socket file left by a killed daemon exists on disk without ever
    accepting, and would fake a successful startup. The wait also races
    against ``daemon_task`` (so a daemon that fails during startup
    surfaces its real exception instead of a generic timeout) and
    against ``stop`` (so a signal during the boot window goes straight
    to teardown). Returns True when the socket is up, False when stop
    was requested first.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    stop_task = asyncio.create_task(stop.wait())
    try:
        while loop.time() < deadline:
            if _probe_broadcast_bound(path, timeout=0.05):
                return True
            done, _ = await asyncio.wait({daemon_task, stop_task}, timeout=0.02, return_when=asyncio.FIRST_COMPLETED)
            if daemon_task in done:
                exc = daemon_task.exception()
                if exc is not None:
                    raise RuntimeError("broadcast daemon failed during startup") from exc
                raise RuntimeError("broadcast daemon exited during startup before binding its socket")
            if stop_task in done:
                # A stop that lands in the bind window must not discard a
                # daemon that already came up: the probe only runs at the top
                # of the loop, so a stop set while we are parked in the wait
                # above would otherwise report a live socket as a boot abort.
                # Re-probe — if it now accepts, report the successful bind (the
                # caller prints the manifest, then the already-set stop drives
                # an immediate graceful teardown). Only a stop with no live
                # socket is a true pre-bind abort.
                return _probe_broadcast_bound(path, timeout=0.05)
        raise RuntimeError(f"broadcast socket did not accept connections at {path}")  # pragma: no cover
    finally:
        stop_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await stop_task


def _install_signal_handlers(stop: asyncio.Event) -> None:
    """Install exactly one SIGINT/SIGTERM handler routing to ``stop``.

    Installed BEFORE the daemon task is created (and the daemon runs
    with ``install_signal_handlers=False``), so there is no window in
    which a signal stops the bus directly while the supervisor keeps
    booting. The process has exactly ONE handler and the supervisor
    owns shutdown ordering — the daemon must stop LAST, after the
    listener and watcher threads are down, or events emitted during
    teardown would land on a dead bus.
    """
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)


def _start_components(
    components: frozenset[str],
    docker_socket: str,
    statuses: list[ComponentStatus],
) -> tuple[
    tuple[BaseServer, threading.Thread] | None,
    tuple[threading.Event, threading.Thread] | None,
    tuple[docker_watch.WatchStopper, threading.Thread] | None,
]:
    """Start the requested non-broadcast components; report the rest as skipped."""
    listener_handle = None
    fs_handle = None
    docker_handle = None
    if "listener" in components:
        listener_handle = _start_listener(statuses)
    else:
        statuses.append(ComponentStatus("listener", False, "not requested"))
    if "fs" in components:
        fs_handle = _start_fs_watch(_config.get_config(), statuses)
    else:
        statuses.append(ComponentStatus("fs", False, "not requested"))
    if "docker" in components:
        docker_handle = _start_docker_watch(docker_socket, statuses)
    else:
        statuses.append(ComponentStatus("docker", False, "not requested"))
    return listener_handle, fs_handle, docker_handle


async def _teardown(
    daemon: Broadcast,
    daemon_task: asyncio.Task[int],
    listener_handle: tuple[BaseServer, threading.Thread] | None,
    fs_handle: tuple[threading.Event, threading.Thread] | None,
    docker_handle: tuple[docker_watch.WatchStopper, threading.Thread] | None,
    poll_tasks: list[asyncio.Task[None]],
) -> BaseException | None:
    """Graceful bounded teardown: timers, listener, fs/docker watchers, the bus.

    Returns the broadcast daemon's exception when it died with one
    (whether before teardown started or during the bounded join), else
    ``None``. The caller maps a non-None outcome to a nonzero exit:
    teardown surfaces, never suppresses, the daemon's exception.
    """
    for task in poll_tasks:
        task.cancel()
    for task in poll_tasks:
        with contextlib.suppress(asyncio.CancelledError):
            await task
    if listener_handle is not None:
        server, thread = listener_handle
        server.shutdown()
        server.server_close()
        thread.join(_TEARDOWN_TIMEOUT_S)
        _warn_if_alive(thread, "listener")
    if fs_handle is not None:
        stop, thread = fs_handle
        stop.set()
        thread.join(_TEARDOWN_TIMEOUT_S)
        _warn_if_alive(thread, "fs")
    if docker_handle is not None:
        stopper, thread = docker_handle
        stopper.stop()
        thread.join(_TEARDOWN_TIMEOUT_S)
        _warn_if_alive(thread, "docker")
    # The bus stops last so every component above could still emit
    # during its own drain. Mirrors the daemon-stop pattern the demo
    # proves: explicit stop(), then a bounded join of the run task.
    await daemon.stop()
    daemon_exc: BaseException | None = None
    if daemon_task.done():
        # Already exited (startup failure or unexpected death). Retrieve
        # the exception and hand it back so the caller can surface it —
        # silently consuming it here would let a crash exit 0.
        if not daemon_task.cancelled():
            daemon_exc = daemon_task.exception()
        print("serve: stopped", flush=True)
        return daemon_exc
    try:
        await asyncio.wait_for(daemon_task, timeout=_TEARDOWN_TIMEOUT_S)
    except (TimeoutError, asyncio.CancelledError):
        print(
            f"serve: broadcast did not stop within {_TEARDOWN_TIMEOUT_S:g}s",
            file=sys.stderr,
            flush=True,
        )
    except Exception as exc:
        daemon_exc = exc
        print(
            f"serve: broadcast daemon raised during shutdown: {type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )
    print("serve: stopped", flush=True)
    return daemon_exc


def _warn_if_alive(thread: threading.Thread, name: str) -> None:
    """Print a teardown warning when a component thread outlived its join."""
    if thread.is_alive():
        print(
            f"serve: {name}: component did not stop within {_TEARDOWN_TIMEOUT_S:g}s",
            file=sys.stderr,
            flush=True,
        )


async def _run_serve(
    components: frozenset[str],
    *,
    poll: bool,
    docker_socket: str,
    stop_event: asyncio.Event | None = None,
) -> int:
    """Boot the supervisor, print the manifest, wait for stop, tear down.

    ``stop_event`` is the test seam: when injected, the caller controls
    shutdown instead of a signal. Returns 0 on a clean stop, 1 when the
    broadcast daemon died with an exception (during the run or during
    shutdown), 2 when a broadcast daemon is already serving on the
    socket.
    """
    from .. import broadcast
    from ..sources._registry import discover_plugins_once

    _paths.ensure_state_dirs()
    discover_plugins_once()
    socket_path = _paths.broadcast_socket()
    if _probe_broadcast_bound(socket_path):
        print(
            f"serve: refusing to start: a broadcast daemon is already serving on {socket_path}; "
            "stop it first (systemctl --user stop waitbus-broadcast.service)",
            file=sys.stderr,
            flush=True,
        )
        return 2
    stop = stop_event if stop_event is not None else asyncio.Event()
    # Handlers go in BEFORE the daemon task exists (and the daemon runs
    # with install_signal_handlers=False): no window where a signal
    # stops the bus while the supervisor keeps booting toward "ready".
    _install_signal_handlers(stop)
    daemon = broadcast.Broadcast()
    daemon_task = asyncio.create_task(daemon.run(install_signal_handlers=False))
    listener_handle: tuple[BaseServer, threading.Thread] | None = None
    fs_handle: tuple[threading.Event, threading.Thread] | None = None
    docker_handle: tuple[docker_watch.WatchStopper, threading.Thread] | None = None
    poll_tasks: list[asyncio.Task[None]] = []
    code = 0
    try:
        socket_up = await _await_socket(Path(daemon.socket_path), _BIND_TIMEOUT_S, daemon_task, stop)
        if socket_up:
            statuses = [ComponentStatus("broadcast", True, f"socket {daemon.socket_path}")]
            listener_handle, fs_handle, docker_handle = _start_components(components, docker_socket, statuses)
            poll_tasks = _start_poll_timers(poll, statuses)
            for status in statuses:
                print(_format_status(status), flush=True)
            print("serve: ready (press Ctrl-C to stop)", flush=True)
            stop_task = asyncio.create_task(stop.wait())
            try:
                await asyncio.wait({stop_task, daemon_task}, return_when=asyncio.FIRST_COMPLETED)
            finally:
                stop_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await stop_task
            if daemon_task.done() and not stop.is_set():
                exc = daemon_task.exception()
                detail = f": {type(exc).__name__}: {exc}" if exc is not None else ""
                print(f"serve: broadcast daemon exited unexpectedly{detail}", file=sys.stderr, flush=True)
                code = 1
        # else: signal arrived during boot; fall through to teardown.
    finally:
        daemon_exc = await _teardown(daemon, daemon_task, listener_handle, fs_handle, docker_handle, poll_tasks)
    if code == 0 and daemon_exc is not None:
        # The daemon crashed in the stop window (stop already set, so the
        # unexpected-exit branch above did not fire). Exiting 0 here would
        # mask the crash behind a clean-looking shutdown.
        print(
            f"serve: broadcast daemon failed during shutdown: {type(daemon_exc).__name__}: {daemon_exc}",
            file=sys.stderr,
            flush=True,
        )
        return 1
    return code


def serve_cmd(
    components: str | None = typer.Argument(
        None,
        help=f"Comma-separated subset of {','.join(_COMPONENTS)}.",
    ),
    all_components: bool = typer.Option(
        False,
        "--all",
        help="Run every component the local configuration supports.",
    ),
    no_listener: bool = typer.Option(
        False,
        "--no-listener",
        help="With --all: do not start the webhook listener.",
    ),
    poll: bool = typer.Option(
        False,
        "--poll",
        help="Run the etag poll and watchdog checks on internal timers.",
    ),
    docker_socket: str = typer.Option(
        "/var/run/docker.sock",
        "--docker-socket",
        help="Docker Engine socket probed for the docker watcher.",
    ),
) -> None:
    """Run the waitbus components in one foreground process (no systemd).

    Boots the broadcast daemon against the real state directory and
    starts every requested component whose prerequisites hold, printing
    one started/skipped manifest line per component. Ctrl-C stops
    everything gracefully.
    """
    plan = _parse_components(components, all_components=all_components, no_listener=no_listener)
    cfg = _config.get_config()
    logging.basicConfig(level=cfg.log_level, format="%(message)s", stream=sys.stderr)
    try:
        code = asyncio.run(_run_serve(plan, poll=poll, docker_socket=docker_socket))
    except KeyboardInterrupt:
        code = 0
    except RuntimeError as exc:
        # A daemon startup failure (raised by _await_socket) escapes
        # asyncio.run as RuntimeError; surface it as one operator-readable
        # stderr line — never a raw traceback — and exit 1.
        cause = exc.__cause__
        detail = f": {type(cause).__name__}: {cause}" if cause is not None else ""
        print(f"serve: {exc}{detail}", file=sys.stderr, flush=True)
        raise typer.Exit(1) from exc
    raise typer.Exit(code)
