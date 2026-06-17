"""``waitbus on`` -- block until a predicate matches, then run a command.

The action counterpart to ``waitbus wait``: where ``wait`` answers *whether* an
event matched (via its exit code), ``on`` runs an operator-supplied command
*when* one matches, with the matched event handed to that command as context.
It is the watchexec / entr / direnv adjacency, specialised to the waitbus event
bus and the source-agnostic predicate surface ``wait`` already exposes.

    waitbus on --source pytest --match 'fields.event_type="pytest_session"' -- ./deploy.sh
    waitbus on --sha 7f3a1b2 -- notify-send "CI done"
    waitbus on --source fs --match 'fields.event_type="fs_change"' --loop --restart -- ./serve

Two modes:

* **once (default).** Block for the first match, run the command once, and exit
  with the command's own exit code -- so ``waitbus on ... -- pytest`` is directly
  composable in a script, just like ``waitbus wait ... && pytest`` but with the
  matched event passed through as context. This is the dumb, composable
  primitive; it is barely more than ``wait`` plus an exec.
* **loop (``--loop``).** Keep reacting: run the command on every match until an
  idle ``--timeout`` elapses with no new match (exit 124) or SIGINT (exit 130).
  Without ``--restart`` the command runs to completion before the next match is
  serviced (entr's sequential default); with ``--restart`` a new match
  terminates the still-running command first (SIGTERM to its process group, then
  SIGKILL after ``--stop-timeout``, watchexec's restart semantics) before
  launching the new one.

Event context. The matched event is written as a JSON object to a temp file
whose path is exported in ``$WAITBUS_EVENT_FILE``, plus scalar convenience
variables (``WAITBUS_EVENT_ID``, ``WAITBUS_EVENT_TYPE``, ``WAITBUS_SOURCE``,
``WAITBUS_OWNER``, ``WAITBUS_REPO``, ``WAITBUS_HEAD_SHA``, ``WAITBUS_CONCLUSION``). This
mirrors watchexec's events-file channel; waitbus does NOT use the
deprecated path of stuffing the full event into the process environment.

Security. The command is operator-supplied argv and is run **without a shell**;
event field values are passed only through the environment / event file and are
**never** interpolated into the command line. A predicate or event payload that
an agent or LLM can influence therefore cannot inject a command -- there is no
shell metacharacter surface and no substitution of untrusted data into argv. The
command runs in the foreground CLI process the operator launched, never in the
broadcast daemon (the daemon owns no subprocesses, by design).

Signals and process groups. once mode and the sequential ``--loop`` run the child
in the supervisor's own process group with inherited stdin/stdout/stderr -- the
child IS the foreground job, so a terminal Ctrl-C reaches it directly and the
supervisor maps its own resulting ``KeyboardInterrupt`` to exit 130. The
``--loop --restart`` path instead runs each child in a NEW process group
(``process_group=0``, Python 3.11+) so a new match can terminate the
still-running child without the supervisor signalling itself; teardown signals
the whole group (``SIGTERM`` -> ``--stop-timeout`` grace -> unconditional
``SIGKILL``) so grandchildren do not leak. Because a restart child lives in its
own group it does not receive terminal signals, so the restart loop forwards
``SIGTERM``/``SIGHUP`` sent to the supervisor into that teardown (otherwise the
child would be orphaned). Residual: a grandchild that ``setsid()``s into its own
session escapes the group ``SIGKILL`` (only cgroup v2 ``cgroup.kill`` is fully
leak-proof; out of scope for a single-user workstation tool). On ``--restart``,
the previous match's event file is invalidated (unlinked) immediately when the
old child is terminated -- its context is stale by design. ``waitbus on`` reads the
subscribe-ack barrier before parking in ``await_predicate`` (unlike ``waitbus
wait``) so a separate-process emit cannot race registration; the cost is a short
blocking window at startup.

Exit codes:

    <child>  once mode, matched: the command's own exit code (a signal-killed
             child maps to 128 + signum, the shell convention; e.g. SIGKILL -> 137)
    126      the command was found but could not be executed (not executable /
             permission denied) -- the bash / coreutils ``timeout`` convention
    127      the command was not found on PATH
    124      --timeout elapsed with no match (once mode) or no new match for
             that long (loop mode); the coreutils ``timeout`` convention
    130      SIGINT (Ctrl-C), after terminating any running command
    143/129  the supervisor itself was sent SIGTERM (143) / SIGHUP (129), after
             tearing down any running command (128 + signum)
    2        startup failure (no command after ``--``, daemon down, bad --repo /
             --match / --timeout) or the daemon closed the connection

Exit-code collision (documented, inherent). once mode passes the child's own exit
code through unchanged, so a child that legitimately exits 124, 130, or 2 is
indistinguishable from waitbus's own timeout / SIGINT / startup sentinels -- the
same unavoidable ambiguity GNU ``timeout`` documents for its 124/125/126/127
range. The contract is therefore: 1-125 means "the child said so"; 126/127 mean
the command could not be exec'd; 128+ means a signal; {2, 124, 130} could be
waitbus OR a child that happened to use them. ``--timeout`` bounds only the WAIT for
a match, never a running command, so a hung once-mode child is not force-killed by
``waitbus on`` (use the command's own timeout, or ``--loop --restart``).
"""

from __future__ import annotations

import contextlib
import dataclasses
import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Annotated, Any

import typer

from ._broadcast_sub import (
    BroadcastConnectionError,
    FrameDecision,
    SubscriberHandle,
    WaitOutcome,
    await_predicate,
    open_subscriber,
    read_subscribe_ack,
)
from ._duration import parse_duration
from .cli._shared import _exit_with_error, run_typer_app

# The predicate / repo plumbing is shared with ``waitbus wait`` -- ``on`` is a
# wait-family verb (wait, then act), so it reuses the same builder rather than
# duplicating the --sha sugar, --source narrowing, and validation. The exit-code
# constants are likewise the shared wait-family contract.
from .wait import (
    EXIT_SIGINT,
    EXIT_STARTUP,
    EXIT_TIMEOUT,
    _build_predicate,
    _exit_code_if_terminal,
    _resolve_repo,
)

if TYPE_CHECKING:
    from types import FrameType

    from ._predicate import Predicate

_app = typer.Typer(
    name="on",
    help="Block until a predicate matches an event, then run a command.",
    no_args_is_help=False,
    add_completion=False,
)

# Default grace period before a restart escalates SIGTERM to SIGKILL, matching
# watchexec's --stop-timeout default of 10 seconds.
_DEFAULT_STOP_TIMEOUT = "10s"


def _parse_timeout_or_exit(label: str, raw: str) -> float:
    """Parse a duration option into seconds, or exit 2 with a labelled error.

    Shared by ``--timeout`` and ``--stop-timeout`` so the parse-or-startup-fail
    pattern is written once. ``_exit_with_error`` does not return, so this either
    yields a positive float or terminates the process.
    """
    try:
        return parse_duration(raw)
    except ValueError as exc:
        _exit_with_error(f"invalid {label}: {exc}", code=EXIT_STARTUP)


def _event_env(frame: dict[str, Any]) -> dict[str, str]:
    """Project the matched frame into ``WAITBUS_*`` scalar environment variables.

    Only fields that are present are exported; absent / null fields are omitted
    rather than exported empty. Top-level frame keys (``event_id``,
    ``event_type``, ``owner``, ``repo``) and the nested ``fields`` projection
    (``source``, ``head_sha``, ``conclusion``) are the stable, useful subset; the
    full event is available as JSON via ``$WAITBUS_EVENT_FILE``.
    """
    raw_fields = frame.get("fields")
    fields: dict[str, Any] = raw_fields if isinstance(raw_fields, dict) else {}
    out: dict[str, str] = {}
    pairs = (
        ("WAITBUS_EVENT_ID", frame.get("event_id")),
        ("WAITBUS_EVENT_TYPE", frame.get("event_type")),
        ("WAITBUS_OWNER", frame.get("owner")),
        ("WAITBUS_REPO", frame.get("repo")),
        ("WAITBUS_SOURCE", fields.get("source")),
        ("WAITBUS_HEAD_SHA", fields.get("head_sha")),
        ("WAITBUS_CONCLUSION", fields.get("conclusion")),
    )
    for key, value in pairs:
        if value is not None:
            out[key] = str(value)
    return out


def _child_env(frame: dict[str, Any], event_file: str) -> dict[str, str]:
    """Build the child process environment: the parent env plus the event context."""
    env = dict(os.environ)
    env.update(_event_env(frame))
    env["WAITBUS_EVENT_FILE"] = event_file
    return env


def _write_event_file(frame: dict[str, Any]) -> str:
    """Write the matched frame as JSON to a fresh temp file, returning its path.

    The caller owns the file's lifetime. This is waitbus's canonical event-context
    channel: the command reads the full event via ``$WAITBUS_EVENT_FILE`` (the
    process environment carries only the scalar convenience variables).
    """
    fd, path = tempfile.mkstemp(prefix="waitbus-event-", suffix=".json")
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(json.dumps(frame))
    return path


@contextlib.contextmanager
def _event_file(frame: dict[str, Any]) -> Iterator[str]:
    """Yield a temp event-file path (see :func:`_write_event_file`), removed on exit."""
    path = _write_event_file(frame)
    try:
        yield path
    finally:
        with contextlib.suppress(OSError):
            os.unlink(path)


def _normalise_returncode(rc: int) -> int:
    """Map a subprocess return code to a process exit code.

    A signal-killed child has a negative ``returncode`` (``-signum``); the shell
    convention reports that as ``128 + signum``. A normal exit passes through.
    """
    return rc if rc >= 0 else 128 + (-rc)


# Exec-failure exit codes, the bash / GNU ``timeout`` convention (see the
# module docstring's exit-code contract). A non-exec OSError (rare: ENOMEM,
# E2BIG) maps to the startup-failure class.
_EXIT_EXEC_NO_PERMISSION = 126  # command found but not executable / permission denied
_EXIT_EXEC_NOT_FOUND = 127  # command not found on PATH


def _exec_error_code(exc: OSError) -> int:
    """Map a spawn-time ``OSError`` to its bash/timeout exec-failure exit code."""
    if isinstance(exc, FileNotFoundError):
        return _EXIT_EXEC_NOT_FOUND
    if isinstance(exc, PermissionError):
        return _EXIT_EXEC_NO_PERMISSION
    return EXIT_STARTUP


class _ExecError(Exception):
    """Raised when the operator's command cannot be exec'd at spawn time.

    Carries the bash/timeout exec-failure exit code (127 not-found / 126
    not-executable / EXIT_STARTUP for a rarer OSError). Both the once-and-sequential
    spawn (:func:`_run_blocking`) and the restart spawn (:meth:`_RunningChild.spawn`)
    raise it, so :func:`_supervise` maps an exec failure to the same documented exit
    code in EVERY mode -- closing the prior asymmetry where once mode returned the
    code but the loop modes diverged (a raw traceback on restart, a silently-ignored
    code in the sequential loop). It is distinct from a child that itself *runs* and
    exits 126/127, which is passed through as the child's own code (once mode).
    """

    def __init__(self, code: int) -> None:
        super().__init__(f"command could not be executed (exit {code})")
        self.code = code


class _SignalShutdown(Exception):  # noqa: N818 -- a control-flow signal, not an error condition
    """Raised by the restart-loop's SIGTERM/SIGHUP handler to unwind to teardown.

    Mirrors how the default SIGINT handler raises ``KeyboardInterrupt``: the
    handler raises this in the main thread so the ``_run_loop`` ``finally`` tears
    the running child's group down before the process exits, rather than the
    default disposition killing the supervisor and orphaning a child that lives
    in its own (terminal-detached) process group. Carries ``signum`` so the exit
    code can follow the ``128 + signum`` convention.
    """

    def __init__(self, signum: int) -> None:
        super().__init__(f"received signal {signum}")
        self.signum = signum


def _run_blocking(command: list[str], frame: dict[str, Any]) -> int:
    """Run ``command`` to completion (foreground, shared group) and return its code.

    Used by once mode and the sequential ``--loop`` path: the child IS the
    foreground job, so it shares the supervisor's process group and inherits
    stdio -- a terminal Ctrl-C reaches it directly (no forwarding needed). No
    shell: ``command`` is the operator's argv exactly; event field values reach
    the child only through the environment and ``$WAITBUS_EVENT_FILE``, never
    interpolated into argv, so an agent-influenced predicate/payload has no
    command-injection surface. A spawn-time exec failure raises :class:`_ExecError`
    carrying 126/127 (so :func:`_supervise` maps it uniformly across modes); a
    signal-killed child that actually ran maps to ``128 + signum``
    (:func:`_normalise_returncode`).
    """
    with _event_file(frame) as event_file:
        try:
            proc = subprocess.run(command, env=_child_env(frame, event_file), check=False)
        except OSError as exc:
            raise _ExecError(_exec_error_code(exc)) from exc
    return _normalise_returncode(proc.returncode)


def _terminate_group(proc: subprocess.Popen[bytes], stop_timeout: float) -> None:
    """Stop a child's whole process group: SIGTERM, grace, then UNCONDITIONAL SIGKILL.

    Signals the *negative* PGID via :func:`os.killpg` so grandchildren receive it
    too, not just the direct child. After the grace window the group is SIGKILLed
    **unconditionally** (a no-op if already dead): a child that exits gracefully
    on SIGTERM may have left grandchildren in the group, and force-killing only on
    a ``wait`` timeout would orphan them (the watchexec / containerd #4594
    grandchild-leak class). ``proc.pid`` is the PGID because the child was spawned
    with ``process_group=0`` (``setpgid(0,0)`` => PGID == child PID).

    A second, impatient Ctrl-C arriving during the grace ``wait`` raises
    ``KeyboardInterrupt`` here; it is caught so it shortcuts straight to the
    SIGKILL below rather than escaping as a traceback or leaving a half-killed
    tree. Residual limitation: a grandchild that ``setsid()``s into its own new
    session escapes the PGID kill -- only cgroup v2 ``cgroup.kill`` is fully
    leak-proof, which is out of scope for a single-user workstation CLI.
    """
    if proc.poll() is None:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGTERM)
        # A TimeoutExpired (grace elapsed) or an impatient 2nd Ctrl-C during the
        # wait both fall through to the unconditional group SIGKILL below.
        with contextlib.suppress(subprocess.TimeoutExpired, KeyboardInterrupt):
            proc.wait(timeout=stop_timeout)
    with contextlib.suppress(ProcessLookupError):
        os.killpg(proc.pid, signal.SIGKILL)
    with contextlib.suppress(subprocess.TimeoutExpired):
        proc.wait(timeout=2.0)


@dataclasses.dataclass
class _RunningChild:
    """A spawned child in its own process group, owning its event-context file.

    The single owner of ``(proc, event_file)`` so spawn / terminate / cleanup
    stay atomic across the restart-loop path. :meth:`spawn` writes the event file
    and launches the child in a NEW process group (``process_group=0`` =>
    ``setpgid(0,0)``, Python 3.11+) -- a killable PGID that, unlike
    ``start_new_session``/``setsid``, KEEPS the controlling terminal. On a spawn
    failure the event file is unlinked so it cannot leak (the bug a bare
    ``(proc, path)`` tuple permitted).
    """

    proc: subprocess.Popen[bytes]
    event_file: str

    @classmethod
    def spawn(cls, command: list[str], frame: dict[str, Any]) -> _RunningChild:
        """Write the event file and launch ``command`` in a fresh process group.

        Raises :class:`_ExecError` (carrying 126/127) if the exec fails, after
        unlinking the just-written event file -- the same exec-failure signal
        :func:`_run_blocking` raises, so :func:`_supervise` handles all modes
        uniformly instead of the restart path surfacing a raw ``OSError`` traceback.
        """
        path = _write_event_file(frame)
        try:
            proc = subprocess.Popen(command, env=_child_env(frame, path), process_group=0)
        except OSError as exc:
            with contextlib.suppress(OSError):
                os.unlink(path)
            raise _ExecError(_exec_error_code(exc)) from exc
        return cls(proc=proc, event_file=path)

    def terminate_and_cleanup(self, stop_timeout: float) -> None:
        """Tear the child's group down (:func:`_terminate_group`) and unlink the event file."""
        try:
            _terminate_group(self.proc, stop_timeout)
        finally:
            with contextlib.suppress(OSError):
                os.unlink(self.event_file)


def _make_decide(
    composed: Predicate,
    captured: list[dict[str, Any]],
) -> Callable[[dict[str, Any]], FrameDecision]:
    """Build the ``await_predicate`` decision closure that captures the matched frame.

    Matches the composed predicate, and for GitHub frames additionally requires a
    terminal ``conclusion`` (an in-flight run keeps waiting -- the same gate
    ``waitbus wait`` applies), then captures the full frame for the caller to feed
    to the command. The matched frame is surfaced via this closure, not via
    ``WaitOutcome`` (which is intentionally payload-free).
    """

    def _decide(frame: dict[str, Any]) -> FrameDecision:
        raw_fields = frame.get("fields")
        if not isinstance(raw_fields, dict) or not composed.evaluate(frame):
            return FrameDecision.CONTINUE
        if raw_fields.get("source") == "github" and _exit_code_if_terminal(raw_fields.get("conclusion")) is None:
            # In-flight GitHub run for our matcher: keep waiting for the terminal frame.
            return FrameDecision.CONTINUE
        captured.append(frame)
        return FrameDecision.MATCHED

    return _decide


def _outcome_exit(outcome: WaitOutcome) -> int | None:
    """Map a non-match terminal outcome to an exit code, or None if it matched.

    Shared by the once and loop paths: SIGINT -> 130, timeout -> 124, a peer
    close / framing error -> 2 (the daemon went away). ``None`` means the outcome
    matched and the caller should run the command.
    """
    if outcome.cancelled:
        print("waitbus on: interrupted", file=sys.stderr, flush=True)
        return EXIT_SIGINT
    if outcome.timed_out:
        return EXIT_TIMEOUT
    if outcome.matched:
        return None
    # peer_closed / framing_error: the daemon closed the connection.
    print("waitbus on: broadcast daemon closed the connection", file=sys.stderr, flush=True)
    return EXIT_STARTUP


def _run_once(sub: SubscriberHandle, composed: Predicate, deadline: float, command: list[str]) -> int:
    """Block for the first match, run the command once, return its exit code."""
    captured: list[dict[str, Any]] = []
    outcome = await_predicate(sub, decide=_make_decide(composed, captured), deadline_seconds=deadline)
    mapped = _outcome_exit(outcome)
    if mapped is not None:
        if mapped == EXIT_TIMEOUT:
            print(f"waitbus on: timed out with no match for `{composed.source}`", file=sys.stderr, flush=True)
        return mapped
    return _run_blocking(command, captured[0])


def _next_match_or_exit(sub: SubscriberHandle, composed: Predicate, idle_deadline: float) -> dict[str, Any] | int:
    """Block for the next match: return the matched frame, or an exit code on a terminal outcome.

    Shared by both ``--loop`` paths. A non-match terminal outcome (idle timeout,
    SIGINT, daemon close) returns its exit code; the idle-timeout message is
    emitted here so both loops report it identically.
    """
    captured: list[dict[str, Any]] = []
    outcome = await_predicate(sub, decide=_make_decide(composed, captured), deadline_seconds=idle_deadline)
    mapped = _outcome_exit(outcome)
    if mapped is not None:
        if mapped == EXIT_TIMEOUT:
            print(
                f"waitbus on --loop: no new match for `{composed.source}` within the idle timeout",
                file=sys.stderr,
                flush=True,
            )
        return mapped
    return captured[0]


def _install_signal_forwarders(stack: contextlib.ExitStack) -> None:
    """Install SIGTERM/SIGHUP handlers (restored via ``stack``) that raise :class:`_SignalShutdown`.

    Used by the restart loop only: the restart child runs in its OWN process
    group, so a signal sent to the supervisor is NOT delivered to the child by the
    kernel. Without forwarding, a SIGTERM/SIGHUP to ``waitbus on`` would kill the
    supervisor and orphan a running child. The handler raises in the main thread
    (exactly as the default SIGINT handler raises ``KeyboardInterrupt``), unwinding
    to the restart loop's teardown ``finally``. Handlers are restored on stack exit
    so they never leak past the loop.

    No-op off the main thread: ``signal.signal()`` only works there, and the kernel
    only delivers process signals to the main thread, so forwarding is both
    impossible and unnecessary when ``waitbus on`` is embedded in a worker thread
    (e.g. an in-process test executor). The restart teardown ``finally`` still runs.
    """
    if threading.current_thread() is not threading.main_thread():
        return

    def _handler(signum: int, _frame: FrameType | None) -> None:
        raise _SignalShutdown(signum)

    for signum in (signal.SIGTERM, signal.SIGHUP):
        previous = signal.signal(signum, _handler)
        stack.callback(signal.signal, signum, previous)


def _run_loop_sequential(sub: SubscriberHandle, composed: Predicate, idle_deadline: float, command: list[str]) -> int:
    """Sequential ``--loop``: run each matched command to completion before the next match.

    entr's default. The child shares the supervisor's process group and inherits
    stdio (the foreground-job model, like once mode); a terminal Ctrl-C reaches it
    directly, and the supervisor's own ``KeyboardInterrupt`` is mapped to exit 130
    by :func:`_on`. Individual command exit codes do not end the loop; only an idle
    ``--timeout`` (124), SIGINT (130), or a daemon close (2) does.
    """
    while True:
        match = _next_match_or_exit(sub, composed, idle_deadline)
        if isinstance(match, int):
            return match
        _run_blocking(command, match)


def _run_loop_restart(
    sub: SubscriberHandle,
    composed: Predicate,
    idle_deadline: float,
    command: list[str],
    *,
    stop_timeout: float,
) -> int:
    """Restart ``--loop``: a new match terminates the still-running child, then relaunches.

    watchexec restart semantics. The child runs in its own process group (so the
    supervisor can kill it without killing itself); SIGTERM/SIGHUP to the
    supervisor are forwarded into teardown (:func:`_install_signal_forwarders`) so
    the child is never orphaned. ``--stop-timeout`` (default 10s) bounds the
    SIGTERM grace before SIGKILL. A SIGINT (Ctrl-C) propagates as
    ``KeyboardInterrupt`` to :func:`_on` (exit 130) after the ``finally`` tears the
    child down; a second, impatient Ctrl-C during the grace window shortcuts to
    SIGKILL inside :func:`_terminate_group`.
    """
    running: _RunningChild | None = None
    with contextlib.ExitStack() as stack:
        _install_signal_forwarders(stack)
        try:
            while True:
                match = _next_match_or_exit(sub, composed, idle_deadline)
                if isinstance(match, int):
                    return match
                if running is not None:
                    running.terminate_and_cleanup(stop_timeout)
                running = _RunningChild.spawn(command, match)
        finally:
            if running is not None:
                running.terminate_and_cleanup(stop_timeout)


def _run_loop(
    sub: SubscriberHandle,
    composed: Predicate,
    idle_deadline: float,
    command: list[str],
    *,
    restart: bool,
    stop_timeout: float,
) -> int:
    """React to every match until an idle timeout, SIGINT, or a forwarded signal.

    Dispatches to the sequential (run-to-completion) or restart (terminate-then-
    relaunch) loop. The two differ in process-group and signal handling — see
    :func:`_run_loop_sequential` and :func:`_run_loop_restart`.
    """
    if restart:
        return _run_loop_restart(sub, composed, idle_deadline, command, stop_timeout=stop_timeout)
    return _run_loop_sequential(sub, composed, idle_deadline, command)


def _open_acked_subscriber(effective_repo: str | None) -> SubscriberHandle:
    """Open a subscriber and read the registration-ack barrier, or exit 2 on failure.

    The ack barrier is read before the caller parks in ``await_predicate`` so a
    separate-process emit cannot race the daemon-side registration (a divergence
    from ``waitbus wait``, which does not read the ack -- see the module docstring's
    Signals section). ``_exit_with_error`` does not return, so on any failure this
    terminates the process with exit 2 rather than returning.
    """
    try:
        sub = open_subscriber(filters=[effective_repo] if effective_repo is not None else None)
    except BroadcastConnectionError as exc:
        _exit_with_error(str(exc), hint=exc.remediation, code=EXIT_STARTUP)
    try:
        read_subscribe_ack(sub)
    except BroadcastConnectionError as exc:
        sub.sock.close()
        _exit_with_error(str(exc), hint=exc.remediation, code=EXIT_STARTUP)
    return sub


def _supervise(
    sub: SubscriberHandle,
    composed: Predicate,
    command: list[str],
    *,
    loop: bool,
    restart: bool,
    deadline_seconds: float,
    stop_timeout_seconds: float,
) -> int:
    """Run the matched command(s) and translate run-region faults to the exit code.

    The outer error/signal-translation boundary: an exec failure maps to 127/126,
    a Ctrl-C to 130, a forwarded SIGTERM/SIGHUP to ``128 + signum``, and the
    subscriber socket is always closed. Catching :class:`_ExecError` here is what
    makes exec-failure handling uniform across once / sequential / restart (a bad
    command aborts with the documented code in every mode, not just once mode).
    """
    try:
        if loop:
            code = _run_loop(
                sub, composed, deadline_seconds, command, restart=restart, stop_timeout=stop_timeout_seconds
            )
        else:
            code = _run_once(sub, composed, deadline_seconds, command)
    except _ExecError as exec_error:
        # The command could not be exec'd (not found / not executable) in any mode.
        code = exec_error.code
    except KeyboardInterrupt:
        # Ctrl-C during the command run (or while parked). In once / sequential mode
        # the child shares our group and already received the terminal SIGINT; in
        # restart mode the loop's ``finally`` has torn the child down. Map to the
        # documented exit 130 instead of escaping as a raw traceback.
        code = EXIT_SIGINT
    except _SignalShutdown as shutdown:
        # SIGTERM/SIGHUP forwarded by the restart loop (child already torn down).
        code = _normalise_returncode(-shutdown.signum)
    finally:
        sub.sock.close()
    return code


@_app.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def _on(
    ctx: typer.Context,
    sha: Annotated[
        str | None,
        typer.Option("--sha", help="Run when a GitHub commit SHA reaches a terminal conclusion (prefix match)."),
    ] = None,
    match: Annotated[
        list[str] | None,
        typer.Option("--match", help="Predicate 'dotted.key=<json>' (repeatable; same grammar as `waitbus wait`)."),
    ] = None,
    cond: Annotated[
        list[str] | None,
        typer.Option("--cond", help="Named registered condition (repeatable; AND-combined)."),
    ] = None,
    match_cel: Annotated[
        str | None,
        typer.Option("--match-cel", help="Layer-2 CEL expression (requires the cel extra)."),
    ] = None,
    match_jmespath: Annotated[
        str | None,
        typer.Option("--match-jmespath", help="Layer-2 JMESPath expression (requires the jmespath extra)."),
    ] = None,
    repo: Annotated[
        str | None,
        typer.Option("--repo", help="owner/repo to scope the subscription (GitHub paths only)."),
    ] = None,
    source: Annotated[
        str | None,
        typer.Option("--source", help="Restrict to one event source. Default: all."),
    ] = None,
    timeout: Annotated[
        str,
        typer.Option(
            "--timeout",
            help=(
                "once mode: overall deadline. --loop: idle timeout (exit 124 if no new match for "
                "this long). Bare number = seconds; s/m/h/d suffixes accepted."
            ),
        ),
    ] = "300s",
    loop: Annotated[
        bool,
        typer.Option("--loop/--once", help="Keep reacting to every match (default: run once and exit)."),
    ] = False,
    restart: Annotated[
        bool,
        typer.Option(
            "--restart",
            help="With --loop: terminate the still-running command (SIGTERM then SIGKILL) before each new match.",
        ),
    ] = False,
    stop_timeout: Annotated[
        str,
        typer.Option("--stop-timeout", help="With --restart: grace period before SIGKILL (default 10s)."),
    ] = _DEFAULT_STOP_TIMEOUT,
) -> None:
    """Block until a predicate matches an event, then run a command.

    The command and its arguments follow a ``--`` separator, e.g.
    ``waitbus on --source pytest --match 'fields.event_type="pytest_session"' -- ./deploy.sh``.
    """
    command = list(ctx.args)
    if not command:
        _exit_with_error(
            "waitbus on requires a command after `--`",
            hint="Example: waitbus on --source pytest --match 'fields.event_type=\"pytest_session\"' -- ./deploy.sh",
            code=EXIT_STARTUP,
        )
    if restart and not loop:
        _exit_with_error(
            "--restart only applies with --loop",
            hint="Add --loop, or drop --restart (once mode runs the command exactly once).",
            code=EXIT_STARTUP,
        )
    if stop_timeout != _DEFAULT_STOP_TIMEOUT and not restart:
        # --stop-timeout only governs the restart SIGTERM->SIGKILL grace; note the
        # no-op rather than silently ignoring it (mirrors wait._resolve_repo's
        # ignored-`--repo` note). Not an error -- the run proceeds.
        print(
            "waitbus on: --stop-timeout has no effect without --restart (ignored)",
            file=sys.stderr,
            flush=True,
        )

    deadline_seconds = _parse_timeout_or_exit("--timeout", timeout)
    stop_timeout_seconds = _parse_timeout_or_exit("--stop-timeout", stop_timeout)

    composed, effective_source = _build_predicate(
        sha=sha,
        match=match,
        cond=cond,
        match_cel=match_cel,
        match_jmespath=match_jmespath,
        source=source,
        verb="on",
    )
    effective_repo = _resolve_repo(repo=repo, effective_source=effective_source, sha=sha)

    sub = _open_acked_subscriber(effective_repo)
    code = _supervise(
        sub,
        composed,
        command,
        loop=loop,
        restart=restart,
        deadline_seconds=deadline_seconds,
        stop_timeout_seconds=stop_timeout_seconds,
    )
    raise typer.Exit(code)


def main(argv: list[str] | None = None) -> int:
    """Entry point for ``waitbus on``."""
    return run_typer_app(_app, argv)
