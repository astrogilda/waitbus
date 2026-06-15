"""Soak orchestrator entry-point: subscriber thread + main loop + arg parsing.

Imports all four sibling modules (``_context``, ``_emit``, ``_verdict``,
``_suspend``) and ties them together.  Owns ``_SubscriberThread``,
``_parse_duration``, the periodic-sampler dispatcher, the inner
``_run_soak_step`` loop iteration, the argparse builder, and ``main``.
"""

from __future__ import annotations

import argparse
import contextlib
import gc
import json
import select
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from benchmarks._harness import (
    HdrRecorder,
    replay_corpus,
    spawn_waitbus_daemon,
    terminate_daemon_group,
    wait_for_socket,
)
from scripts.soak._context import (
    _FAST_FAULT_INJECTIONS,
    _GC_SAMPLE_INTERVAL_SEC,
    _LOG_SIZE_SAMPLE_INTERVAL_SEC,
    _P99_SAMPLE_INTERVAL_SEC,
    _STANDARD_FAULT_INJECTIONS,
    _STANDARD_SUSPEND_CYCLES,
    FaultInjectionRecord,
    SuspendCycle,
    _SoakAccumulators,
    _SoakContext,
    _SoakState,
)
from scripts.soak._drain_smoke import run_drain_path_smoke
from scripts.soak._emit import _SOURCES, NS_PER_SECOND, _emit_corpus_event, _emit_one
from scripts.soak._fault_injection import run_fault_injection_pass
from scripts.soak._suspend import _isolated_waitbus_dirs, _run_suspend_cycle
from scripts.soak._verdict import (
    _append_progress,
    _collect_sample_or_partial,
    _compute_drain_smoke_failure_doc,
    _compute_verdict_doc,
    _count_close_reasons,
    _stderr_sample_line,
    _write_verdict,
)
from waitbus._broadcast_sub import SubscriberHandle, open_subscriber
from waitbus._frame import sync_read_frame


def _parse_duration(spec: str) -> float:
    """Parse ``60s`` / ``8h`` / ``24h`` style duration into seconds.

    Refuses bare integers so a caller misreading the doc as "give it
    a number of seconds" hits a clear error rather than a silently
    wrong unit.
    """
    spec = spec.strip().lower()
    if spec.endswith("s"):
        return float(spec[:-1])
    if spec.endswith("m"):
        return float(spec[:-1]) * 60.0
    if spec.endswith("h"):
        return float(spec[:-1]) * 3600.0
    raise ValueError(f"duration must end in s/m/h, got {spec!r}")


class _SubscriberThread:
    """Background thread reading frames off the broadcast socket and recording arrival latency.

    Composition over inheritance, deliberately: this class *owns* a
    ``threading.Thread`` rather than subclassing it. ``threading.Thread``
    reserves a growing set of private attributes that ``Thread.join``
    relies on -- ``_stop`` on Python 3.11, and ``_handle`` (a
    ``_thread._ThreadHandle``) added in the 3.13 threading rework. A
    subclass that assigns any of those names shadows the superclass's
    own and breaks ``join`` (the 3.13 ``_handle`` collision raised
    ``AttributeError: 'SubscriberHandle' object has no attribute 'join'``
    on shutdown). Holding a private ``threading.Thread`` instead means
    our attribute namespace can never collide with CPython's, now or as
    future versions reserve more names. The stop signal lives in
    ``_shutdown_event``.

    The recorded latency is ``time.time_ns() - frame.received_at`` --
    the producer (waitbus emit) stamps ``received_at`` at insert time, so
    this is wall-clock event-to-arrival latency. HdrHistogram capture
    is lock-free at the per-record call site; the snapshot path takes
    a lock so a concurrent reader and the recording loop cannot race
    on the histogram's internal counters.
    """

    def __init__(self, socket_path: str) -> None:
        self._socket_path = socket_path
        self._recorder = HdrRecorder()
        self._lock = threading.Lock()
        self._shutdown_event = threading.Event()
        self._handle: SubscriberHandle | None = None
        self._frames_seen = 0
        self._startup_error: BaseException | None = None
        self._started_event = threading.Event()
        self._thread = threading.Thread(target=self._run, name="waitbus-soak-subscriber", daemon=True)

    def start(self) -> None:
        """Start the background thread (delegates to the owned Thread)."""
        self._thread.start()

    def _run(self) -> None:
        """Open subscriber, then loop reading frames and recording latency until stopped."""
        try:
            self._handle = open_subscriber(socket_path=self._socket_path)
        except BaseException as exc:
            self._startup_error = exc
            self._started_event.set()
            return
        self._started_event.set()
        sock = self._handle.sock
        # The select-loop pattern from await_predicate, but recording
        # arrival latency instead of dispatching to a decide() callback.
        # A short select budget (0.5 s) lets the shutdown_event poll
        # cheaply between reads.
        try:
            while not self._shutdown_event.is_set():
                ready, _, _ = select.select([sock], [], [], 0.5)
                if not ready:
                    continue
                try:
                    data = sync_read_frame(sock)
                except (ConnectionError, OSError):
                    return
                if data is None:
                    return
                arrival_ns = time.time_ns()
                try:
                    frame: dict[str, Any] = json.loads(data.decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                if frame.get("kind") == "daemon_heartbeat":
                    continue
                received_at = frame.get("received_at")
                if not isinstance(received_at, int):
                    continue
                latency_ns = arrival_ns - received_at
                if latency_ns <= 0:
                    # Clock skew or instrumentation glitch: drop the
                    # sample rather than feeding a non-positive latency
                    # into the histogram (which would clamp_low it).
                    continue
                with self._lock:
                    self._recorder.record(latency_ns)
                    self._frames_seen += 1
        finally:
            with contextlib.suppress(OSError):
                sock.close()

    def wait_started(self, timeout: float) -> None:
        """Block until ``_run`` has opened the subscriber. Raises on startup error."""
        if not self._started_event.wait(timeout=timeout):
            raise RuntimeError(f"subscriber thread did not start within {timeout}s")
        if self._startup_error is not None:
            raise RuntimeError("subscriber thread crashed during startup") from self._startup_error

    def snapshot_p99_ns(self) -> float:
        """Return the current p99 latency in nanoseconds; 0.0 when no samples yet."""
        with self._lock:
            if self._recorder.count == 0:
                return 0.0
            return float(self._recorder.value_at_percentile_fraction(0.99))

    @property
    def frames_seen(self) -> int:
        """Total non-heartbeat frames recorded into the histogram (thread-safe read)."""
        with self._lock:
            return self._frames_seen

    def stop(self, timeout: float = 5.0) -> None:
        """Signal the loop to exit and join the thread."""
        self._shutdown_event.set()
        self._thread.join(timeout=timeout)


def _run_periodic_samplers(
    ctx: _SoakContext,
    state: _SoakState,
    accums: _SoakAccumulators,
    *,
    now: float,
) -> None:
    """Run the four sub-cadence samplers when their next-sample times have elapsed.

    Collapses the four duplicated ``if now >= state.next_X_sample:`` blocks
    into a single call so the pattern cannot drift out of sync.  Each
    block updates ``state.next_X_sample`` and appends to the appropriate
    accumulator list.
    """
    # p99 drift sampling (rolling subscriber-thread histogram snapshot).
    if now >= state.next_p99_sample:
        p99_ns = ctx.subscriber.snapshot_p99_ns()
        if p99_ns > 0:
            accums.p99_samples.append((time.time_ns(), p99_ns))
        state.next_p99_sample += _P99_SAMPLE_INTERVAL_SEC

    # GC sampling (uncollectable + cumulative collected count).
    if now >= state.next_gc_sample:
        stats = gc.get_stats()
        # gc.get_stats() is documented to always return list[dict[str, int]]
        # with both keys present; direct indexing is safe here.
        uncollectable = sum(g["uncollectable"] for g in stats)
        collected = sum(g["collected"] for g in stats)
        accums.gc_samples.append((time.time_ns(), uncollectable, collected))
        state.next_gc_sample += _GC_SAMPLE_INTERVAL_SEC

    # Log-size sampling (the soak's own progress JSONL is the
    # log we control end-to-end; the broadcast daemon's
    # stderr is suppressed in spawn_waitbus_daemon so a separate
    # broadcast.log path is not available without modifying
    # the harness primitive). A 1 MiB/hr ceiling on the
    # progress JSONL catches runaway sampling / a writer leak.
    if now >= state.next_log_sample:
        try:
            size = ctx.progress_path.stat().st_size
        except FileNotFoundError:
            size = 0
        accums.log_size_samples.append((time.time_ns(), size))
        state.next_log_sample += _LOG_SIZE_SAMPLE_INTERVAL_SEC


def _advance_emit(
    ctx: _SoakContext,
    state: _SoakState,
    accums: _SoakAccumulators,
    *,
    emit_interval: float,
) -> None:
    """Emit one event and advance ``state.next_emit`` and ``state.i``.

    Handles corpus replay (with optional Hawkes-paced timing), corpus
    exhaustion fallback to synthetic, and the synthetic-only path.
    Appends to ``accums.source_counts`` in place; mutates ``state`` scalars.
    """
    if ctx.corpus_iter is not None and not state.corpus_exhausted:
        try:
            parsed_event = next(ctx.corpus_iter)
        except StopIteration:
            state.corpus_exhausted = True
            emitted_source = _emit_one(ctx.db_path, state.i)
            state.next_emit += emit_interval
        else:
            inter_ns, emitted_source = _emit_corpus_event(
                ctx.db_path, parsed_event, state.i, state=state, accums=accums
            )
            if ctx.args.preserve_timing and inter_ns > 0:
                # Hawkes-derived pacing: use the corpus's
                # own inter-arrival timing rather than the
                # fixed --rate cadence.
                state.next_emit += inter_ns / NS_PER_SECOND
            else:
                if ctx.args.preserve_timing and not state.preserve_warned:
                    sys.stderr.write(
                        "[soak] --preserve-timing requested but corpus event "
                        f"{state.i} has inter_arrival_ns={inter_ns}; falling back to "
                        "--rate cadence for this and subsequent events. "
                        "(Warning emitted once per soak run.)\n",
                    )
                    state.preserve_warned = True
                state.next_emit += emit_interval
    else:
        emitted_source = _emit_one(ctx.db_path, state.i)
        state.next_emit += emit_interval
    # ``emitted_source`` is guaranteed in ``_SOURCES`` by the boundary
    # check in ``_emit_corpus_event`` and the ``_SOURCES``-only paths in
    # ``_emit_one``; direct increment is safe.
    accums.source_counts[emitted_source] += 1
    state.i += 1


def _maybe_dispatch_fault_injection(
    ctx: _SoakContext,
    accums: _SoakAccumulators,
    remaining: list[FaultInjectionRecord],
    *,
    offset_sec: float,
) -> None:
    """Fire the next scheduled fault-injection probe if its offset has elapsed.

    Pops the due record from ``remaining`` (mutated in place, mirroring the
    suspend-cycle dispatch) and runs its probe, appending the outcome to
    ``accums.fault_injection_outcomes``. A no-op when nothing is due. Kept
    out of ``main``'s loop body so the orchestrator stays below the
    ``scripts/`` D-grade complexity ratchet.
    """
    if remaining and offset_sec >= remaining[0].offset_sec:
        scenario = remaining.pop(0)
        run_fault_injection_pass(
            axis=scenario.axis,
            socket_path=ctx.socket_path,
            offset_sec=offset_sec,
            outcomes=accums.fault_injection_outcomes,
        )


def _run_soak_step(
    ctx: _SoakContext,
    state: _SoakState,
    accums: _SoakAccumulators,
    *,
    now: float,
    offset_sec: float,
) -> bool:
    """Execute one iteration of the main soak loop body.

    Handles emit cadence, periodic sample collection, p99/GC/log-size
    sub-cadence sampling, and checkpoint writes.  Mutates ``state`` in
    place for all scalar timing and counter fields; appends to the
    accumulator lists via their references.

    Returns ``daemon_alive``: ``False`` means the daemon process is
    gone; the caller should set ``is_partial=True`` and break.
    """
    emit_interval = 1.0 / max(ctx.args.rate, 0.001)

    if now >= state.next_emit:
        _advance_emit(ctx, state, accums, emit_interval=emit_interval)

    if now >= state.next_sample:
        sample = _collect_sample_or_partial(
            ctx.proc,
            ctx.db_path,
            offset_sec=offset_sec,
            kind=f"offset {time.monotonic() - ctx.start_monotonic:.1f}s",
            progress_path=ctx.progress_path,
        )
        if sample is None:
            return False
        accums.rss_samples.append(sample)
        _append_progress(
            ctx.progress_fh,
            {
                "kind": "periodic",
                "ts_ns": sample.ts_ns,
                "offset_sec": offset_sec,
                "rss_bytes": sample.rss_bytes,
                "fd_count": sample.fd_count,
                "wal_bytes": sample.wal_bytes,
                "events_emitted_so_far": state.i,
            },
        )
        _stderr_sample_line(sample, offset_sec=offset_sec, kind="periodic")
        state.next_sample += ctx.args.sample_interval

        # Checkpoint the verdict-in-progress every N samples
        # so a crash leaves an inspectable partial.
        if (n := ctx.args.checkpoint_interval_samples) > 0 and len(accums.rss_samples) % n == 0:
            partial_doc = _compute_verdict_doc(
                ctx,
                accums,
                ended_at_ns=time.time_ns(),
                events_emitted=state.i,
                is_partial=True,
            )
            _write_verdict(ctx.args.output, partial_doc)

    _run_periodic_samplers(ctx, state, accums, now=now)

    # Brief sleep so the loop is not a tight spin between
    # emit/sample cadences.
    time.sleep(min(0.01, max(0.0, state.next_emit - time.monotonic())))
    return True


def _build_parser() -> argparse.ArgumentParser:
    """Build and return the argument parser for the soak orchestrator.

    Extracted from ``main()`` so the parser is independently testable and
    the main-function body can focus on orchestration logic rather than
    argument-definition boilerplate.
    """
    parser = argparse.ArgumentParser(
        description="24-hour mixed-source soak orchestrator.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--duration",
        type=str,
        default="24h",
        help="total soak duration (e.g. 60s, 8h, 24h). Default 24h.",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=5.0,
        help="emit rate in events/sec (default 5).",
    )
    parser.add_argument(
        "--sample-interval",
        type=float,
        default=60.0,
        help="seconds between monitor samples (default 60).",
    )
    parser.add_argument(
        "--inject-suspend-cycles",
        choices=["none", "standard"],
        default="none",
        help=(
            "Insert SIGSTOP/SIGCONT cycles in the realism window. 'standard' adds one 30-min cycle + six 5-min cycles."
        ),
    )
    parser.add_argument(
        "--inject-fault-scenarios",
        choices=["none", "standard", "fast"],
        default="none",
        help=(
            "Run subscriber-lifecycle fault-injection probes (token reject, "
            "version reject, replay-lag eviction). 'standard' schedules them "
            "at 2h, 4h, 6h between the suspend cycles. 'fast' fires every "
            "probe in the first 30 seconds for a sub-minute local smoke run."
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("soak-verdict.json"),
        help="path to write the verdict JSON (default ./soak-verdict.json).",
    )
    parser.add_argument(
        "--progress-jsonl",
        type=Path,
        default=None,
        help=(
            "Path to append a per-sample JSONL log (one record per "
            "Sample, written immediately on capture). Default: the "
            "output path with a '.progress.jsonl' suffix. Use "
            "'tail -F' on this file to see the soak in real time."
        ),
    )
    parser.add_argument(
        "--corpus",
        type=Path,
        default=None,
        help=(
            "Replay events from a gzipped JSONL corpus (output of "
            "`python -m benchmarks.gen_corpus`). When set, the emit "
            "loop draws from the corpus and falls back to synthetic "
            "round-robin once the corpus is exhausted."
        ),
    )
    parser.add_argument(
        "--preserve-timing",
        action="store_true",
        help=(
            "Used with --corpus: pace emits by the corpus events' "
            "`inter_arrival_ns` field (Hawkes-calibrated burst timing) "
            "instead of the fixed --rate cadence."
        ),
    )
    parser.add_argument(
        "--skip-drain-smoke",
        action="store_true",
        help=(
            "Skip the drain-path smoke pre-phase. By default, before the "
            "measured soak, a throwaway low-heartbeat daemon is driven through "
            "every subscriber-lifecycle drain path (token/version reject, "
            "replay-lag and heartbeat-lag eviction); a failure aborts before "
            "the measured run. Use only for debugging the measured loop in "
            "isolation."
        ),
    )
    parser.add_argument(
        "--checkpoint-interval-samples",
        type=int,
        default=10,
        help=(
            "Re-compute and rewrite the verdict JSON every N samples "
            "(default 10). Lets the operator inspect a partial "
            "verdict mid-run; a crash leaves an interpretable "
            "snapshot rather than nothing."
        ),
    )
    return parser


def _run_drain_smoke_gate(args: argparse.Namespace, *, total_seconds: float) -> tuple[Any, int | None]:
    """Run the drain-path smoke pre-phase against a throwaway daemon.

    Exercises every subscriber-lifecycle drain path (token/version reject,
    replay-lag and heartbeat-lag eviction) before the measured soak. The
    throwaway daemon never touches the measured state, so its aggressive
    heartbeat cannot contaminate the RSS/p99 measurements.

    Returns ``(result, early_exit_code)``. ``early_exit_code`` is 1 when the
    gate FAILED -- a failure verdict has already been written to
    ``args.output`` and the caller must return it without starting the
    measured soak; otherwise None. ``result`` is the ``DrainSmokeResult`` (or
    None when ``--skip-drain-smoke`` was passed) to thread into the measured
    run's verdict.
    """
    if args.skip_drain_smoke:
        return None, None
    smoke_started_ns = time.time_ns()
    print("[soak] running drain-path smoke pre-phase (throwaway daemon)...", file=sys.stderr)
    drain_smoke = run_drain_path_smoke()
    for verdict in drain_smoke.verdicts:
        print(
            f"[soak] drain-smoke {verdict.signal}: {'PASS' if verdict.passed else 'FAIL'} -- {verdict.detail}",
            file=sys.stderr,
        )
    if not drain_smoke.passed:
        failure_doc = _compute_drain_smoke_failure_doc(
            drain_smoke,
            started_at_ns=smoke_started_ns,
            ended_at_ns=time.time_ns(),
            duration_sec=total_seconds,
            emit_rate_hz=args.rate,
            sample_interval_sec=args.sample_interval,
        )
        _write_verdict(args.output, failure_doc)
        print(
            f"[soak] drain-path smoke FAILED; wrote {args.output} and aborting before the measured soak",
            file=sys.stderr,
        )
        return drain_smoke, 1
    print("[soak] drain-path smoke passed; starting the measured soak", file=sys.stderr)
    return drain_smoke, None


def main(argv: list[str] | None = None) -> int:
    """Orchestrate the soak run from argument parsing through verdict write."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    if sys.platform != "linux":
        print("error: soak is Linux-only (reads /proc/<pid>/status)", file=sys.stderr)
        return 2

    total_seconds = _parse_duration(args.duration)

    # Drain-path smoke pre-phase gate. A failed pre-phase has already written
    # a failure verdict and returns exit code 1 here -- the measured soak must
    # not start. ``drain_smoke`` (None when skipped) threads into the measured
    # run's verdict for closure.
    drain_smoke, early_rc = _run_drain_smoke_gate(args, total_seconds=total_seconds)
    if early_rc is not None:
        return early_rc

    suspend_cycles: tuple[SuspendCycle, ...] = (
        _STANDARD_SUSPEND_CYCLES if args.inject_suspend_cycles == "standard" else ()
    )
    fault_scenarios: tuple[FaultInjectionRecord, ...] = {
        "none": (),
        "standard": _STANDARD_FAULT_INJECTIONS,
        "fast": _FAST_FAULT_INJECTIONS,
    }[args.inject_fault_scenarios]

    progress_path: Path = (
        args.progress_jsonl
        if args.progress_jsonl is not None
        else args.output.with_suffix(args.output.suffix + ".progress.jsonl")
    )
    # Truncate any prior progress log so a re-run does not silently
    # appear to start at sample N.
    progress_path.write_text("", encoding="utf-8")
    print(
        f"[soak] streaming progress to {progress_path}; tail -F to follow",
        file=sys.stderr,
    )

    started_at_ns = time.time_ns()
    accums = _SoakAccumulators(
        rss_samples=[],
        p99_samples=[],
        gc_samples=[],
        log_size_samples=[],
        source_counts=dict.fromkeys(_SOURCES, 0),
        suspend_outcomes=[],
        suspend_verdicts=[],
        fault_injection_outcomes=[],
    )
    subscriber: _SubscriberThread | None = None
    proc: subprocess.Popen[bytes] | None = None
    ctx: _SoakContext | None = None
    state: _SoakState | None = None
    is_partial: bool = False
    final_close_reasons: dict[str, int] = {}

    with tempfile.TemporaryDirectory(prefix="waitbus-soak-") as tmp_str:
        tmp_dir = Path(tmp_str)
        state_dir = tmp_dir / "state"
        runtime_dir = tmp_dir / "runtime"
        state_dir.mkdir()
        runtime_dir.mkdir()

        daemon_stderr_path = tmp_dir / "daemon-stderr.log"
        with _isolated_waitbus_dirs(state_dir, runtime_dir):
            proc = spawn_waitbus_daemon(state_dir, runtime_dir, stderr_path=daemon_stderr_path)
            # The try/finally MUST wrap every operation after the spawn so a
            # raise in ``wait_for_socket`` (startup timeout, cold-disk
            # schema migration stall) does not orphan ``proc`` past the
            # TemporaryDirectory teardown.
            with progress_path.open("a", encoding="utf-8") as progress_fh:
                # Any raise inside this try (wait_for_socket, thread start,
                # _SubscriberThread.wait_started, the run loop) propagates
                # through the bare ``finally`` below and out of ``main()``
                # without entering the post-with verdict-write at the end
                # of this function -- ctx/state/is_partial are unreachable
                # on the raise path so no UnboundLocalError can occur.
                # Verdict-write is reached only on normal-completion or
                # ``break`` paths, both of which bind every name first.
                try:
                    socket_path = runtime_dir / "broadcast.sock"
                    wait_for_socket(socket_path)
                    db_path = state_dir / "github.db"
                    # Pre-sleep a brief moment so the daemon has finished schema
                    # init by the time the first emit fires; the broadcast serve
                    # entry does ensure_schema synchronously, so the wait_for_socket
                    # is normally sufficient, but a 0.5-s breather keeps the
                    # FIRST emit from racing the schema migration on cold disks.
                    time.sleep(0.5)

                    # Start the subscriber thread BEFORE the first emit so the
                    # broadcast daemon's emit-loop has a live subscriber when
                    # the first event hits the socket.
                    subscriber = _SubscriberThread(socket_path=str(socket_path))
                    subscriber.start()
                    subscriber.wait_started(timeout=5.0)

                    t0 = time.monotonic()
                    soak_deadline = t0 + total_seconds
                    start_monotonic = t0
                    state = _SoakState(
                        i=0,
                        next_emit=t0,
                        next_sample=t0,
                        next_p99_sample=t0 + _P99_SAMPLE_INTERVAL_SEC,
                        next_gc_sample=t0 + _GC_SAMPLE_INTERVAL_SEC,
                        next_log_sample=t0 + _LOG_SIZE_SAMPLE_INTERVAL_SEC,
                        corpus_exhausted=False,
                        preserve_warned=False,
                    )
                    remaining_cycles = list(suspend_cycles)
                    # Corpus replayer (lazy iterator; exhausting falls back to synthetic).
                    corpus_iter: Iterator[dict[str, Any] | None] | None
                    if args.corpus is None:
                        corpus_iter = None
                    elif not args.corpus.exists():
                        print(
                            f"[soak] --corpus path not found ({args.corpus}); falling back to synthetic emit",
                            file=sys.stderr,
                        )
                        corpus_iter = None
                    else:
                        corpus_iter = replay_corpus(args.corpus)

                    ctx = _SoakContext(
                        proc=proc,
                        db_path=db_path,
                        progress_path=progress_path,
                        socket_path=socket_path,
                        daemon_stderr_path=daemon_stderr_path,
                        configured_fault_axes=frozenset(r.axis for r in fault_scenarios),
                        args=args,
                        start_monotonic=start_monotonic,
                        started_at_ns=started_at_ns,
                        total_seconds=total_seconds,
                        corpus_iter=corpus_iter,
                        subscriber=subscriber,
                        sample_interval_sec=args.sample_interval,
                        progress_fh=progress_fh,
                        drain_smoke=drain_smoke,
                    )

                    remaining_fault_scenarios = list(fault_scenarios)

                    while time.monotonic() < soak_deadline:
                        now = time.monotonic()
                        offset_sec = now - start_monotonic

                        # Suspend-cycle dispatch.
                        if remaining_cycles and offset_sec >= remaining_cycles[0].offset_sec:
                            cycle = remaining_cycles.pop(0)
                            daemon_alive = _run_suspend_cycle(
                                ctx,
                                state,
                                accums,
                                cycle=cycle,
                                offset_sec=offset_sec,
                                emit_count=state.i,
                            )
                            if not daemon_alive:
                                is_partial = True
                                break

                        # Fault-injection probe dispatch.
                        _maybe_dispatch_fault_injection(ctx, accums, remaining_fault_scenarios, offset_sec=offset_sec)

                        daemon_alive = _run_soak_step(
                            ctx,
                            state,
                            accums,
                            now=now,
                            offset_sec=offset_sec,
                        )
                        if not daemon_alive:
                            is_partial = True
                            break
                finally:
                    # Stop the subscriber thread BEFORE killing the daemon: a
                    # daemon-side close would otherwise race the subscriber's
                    # blocking select and surface as a stderr OSError noise line.
                    if subscriber is not None:
                        subscriber.stop(timeout=5.0)
                    # Terminate the daemon's whole process group. Pairs with
                    # ``start_new_session=True`` in spawn_waitbus_daemon.
                    terminate_daemon_group(proc)
                    # Capture the close-reason tally NOW, while the daemon's
                    # stderr file still exists: the TemporaryDirectory below is
                    # deleted on block exit, before the end-of-run verdict is
                    # computed, so reading it there would silently yield {}.
                    final_close_reasons = _count_close_reasons(daemon_stderr_path)

    ended_at_ns = time.time_ns()

    # ctx/state are always bound when control reaches here because the only
    # exits from the inner try are: (a) normal loop exit (both bound at the
    # ``ctx = _SoakContext(...)`` and ``state = _SoakState(...)`` lines), or
    # (b) raise that propagates out of main() before reaching this point.
    assert ctx is not None and state is not None  # narrow for type-checker
    verdict_doc = _compute_verdict_doc(
        ctx,
        accums,
        ended_at_ns=ended_at_ns,
        events_emitted=state.i,
        is_partial=is_partial,
        subscriber_close_reasons=final_close_reasons,
    )
    _write_verdict(args.output, verdict_doc)
    print(
        f"[soak] wrote {args.output}; overall_passed={verdict_doc.overall_passed} "
        f"({len(accums.rss_samples)} samples, {state.i} events emitted, progress in {progress_path})",
        file=sys.stderr,
    )
    return 0 if verdict_doc.overall_passed else 1
