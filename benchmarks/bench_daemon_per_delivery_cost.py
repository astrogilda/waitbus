"""Measure the daemon's irreducible CPU cost per delivered (event x subscriber) pair.

This is a small open-loop calibration microbench. It exists to put an
EMPIRICAL number under the per-event equivalence budget that
``bench_multistream_proof`` declares as constants
(``_EQUIVALENCE_PER_EVENT_PER_SUB_UTIME_NS`` /
``_EQUIVALENCE_PER_EVENT_PER_SUB_SCHEDSTAT_NS``). Those constants were
analyst-chosen; this bench measures the daemon-side fan-out cost
directly so the budget can be set (or sanity-checked) against a real
floor rather than a guess.

Method
------
Spawn a real ``waitbus broadcast serve`` daemon in a tmp state/runtime
dir, pinned to a dedicated core set (mirroring the daemon-spawn + pin
pattern in ``bench_multistream_proof._run_bench``). Attach ``M`` plain
in-process subscriber threads, each parked in
``waitbus.subscribe`` on a scope-owner predicate. Then emit ``N``
events at a controlled rate via ``waitbus.emit``; every event
matches every subscriber, so the daemon performs ``N * M`` fan-out
socket writes. Sample the daemon's per-process utime (``/proc/<pid>/stat``)
and aggregated per-TID schedstat run-time (``/proc/<pid>/task/*/schedstat``)
across the burst and divide by ``N * M``.

This is a FLOOR, not the loaded figure: there is no producer swarm, no
LLM, no agent pool -- just raw emits to parked subscribers. The
``bench_multistream_proof`` baseline reads ~183 us daemon CPU per
delivery under contention; the number here is the irreducible cost of
broadcasting alone, the lower bound that interprets the loaded figure.

The bench discards a warmup window and reports the median per-delivery
utime-ns and schedstat-ns over several repetitions. It deliberately
does NOT carry the full pilot / Mann-Whitney machinery of the
production bench -- it is a calibration tool, kept simple.

Linux-only by design (it parses ``/proc`` and uses
``os.sched_setaffinity``); on a host that cannot pin cores (a
restrictive cgroup), pass ``--allow-unpinned`` and treat the resulting
number as noisier.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from collections.abc import Callable
from pathlib import Path

import msgspec

from benchmarks._bench_anchor import (
    SEED_EVENT_TYPE,
    SEED_SOURCE,
    emit_anchor_event,
)
from benchmarks._bench_preflight import compute_orchestrator_and_daemon_cores
from benchmarks._bench_shared import (
    read_daemon_cpu_ns,
    read_daemon_schedstat,
    schedstat_substrate_available,
)
from waitbus._log import structured

_logger = logging.getLogger("waitbus.bench.daemon_per_delivery_cost")

_BENCH_NAME = "bench_daemon_per_delivery_cost"

# Default sweep / burst knobs. N=2000 emits per window keeps each
# window short (seconds) while giving the daemon enough fan-out work
# that the per-delivery quotient is stable against the ~1-jiffy utime
# quantization floor. The subscriber sweep mirrors the production
# bench's M=3 subscriber count plus the M=1 single-subscriber floor.
_DEFAULT_N = 2000
_DEFAULT_SUBSCRIBERS: tuple[int, ...] = (1, 3)

# Controlled emit rate. The bench is open-loop on the producer side: a
# fixed inter-emit interval keeps the daemon's doorbell-thread from
# coalescing every wake into one batch (which would understate the
# per-event cost) while staying well below saturation so the number is
# a floor, not a contention figure.
_DEFAULT_EMIT_RATE_HZ = 500.0

# Repetition / warmup shape. The first window is discarded (cold caches,
# first-touch page faults, JIT-free but allocator-warm). The remaining
# windows feed the median.
_DEFAULT_REPEATS = 5
_WARMUP_WINDOWS = 1

# Daemon-ready socket poll budget.
_DAEMON_READY_TIMEOUT_SEC = 20.0

# Per-subscriber drain join budget at teardown. A parked subscriber
# thread is blocked in ``select``; closing the daemon (which we do by
# terminating it) drops the connection and unblocks the drain so the
# thread returns. The join budget bounds a torn-read edge case.
_SUBSCRIBER_JOIN_TIMEOUT_SEC = 5.0

# Settle window after the emit burst so the daemon finishes draining
# every queued fan-out write before we read its post-burst CPU sample.
# Without it the post-sample races the daemon's in-flight writes and
# undercounts the cost.
_DRAIN_SETTLE_SEC = 0.5


class _WindowSample(msgspec.Struct, frozen=True, kw_only=True):
    """Daemon CPU deltas measured across one emit burst of ``n * m`` deliveries."""

    n_events: int
    m_subscribers: int
    deliveries: int
    utime_delta_ns: int
    stime_delta_ns: int
    schedstat_run_delta_ns: int
    schedstat_pcount_delta: int

    @property
    def per_delivery_utime_ns(self) -> float:
        return self.utime_delta_ns / self.deliveries

    @property
    def per_delivery_stime_ns(self) -> float:
        return self.stime_delta_ns / self.deliveries

    @property
    def per_delivery_schedstat_ns(self) -> float:
        return self.schedstat_run_delta_ns / self.deliveries


class _SubscriberResult(msgspec.Struct, kw_only=True):
    """Mutable per-subscriber drain counter, read after teardown."""

    received: int = 0
    error: str | None = None


def per_delivery_cost(
    *,
    cpu_delta_ns: int,
    n_events: int,
    m_subscribers: int,
) -> float:
    """Return the per-delivery cost = ``cpu_delta_ns / (n_events * m_subscribers)``.

    The pure arithmetic kernel of the bench, factored out so the test
    suite can exercise it on synthetic deltas without spawning a daemon.

    ``n_events`` and ``m_subscribers`` must both be positive; a daemon
    that delivered zero (event x subscriber) pairs cannot yield a
    per-delivery cost and the caller has a configuration bug, so this
    raises rather than returning ``inf`` / ``nan``.
    """
    if n_events <= 0:
        raise ValueError(f"n_events must be > 0, got {n_events}")
    if m_subscribers <= 0:
        raise ValueError(f"m_subscribers must be > 0, got {m_subscribers}")
    return cpu_delta_ns / (n_events * m_subscribers)


def median_per_delivery(samples: list[_WindowSample], *, accessor: Callable[[_WindowSample], float]) -> float:
    """Return the median over each sample's per-delivery value.

    ``accessor`` selects which per-delivery column to summarise
    (utime / stime / schedstat). Returns ``0.0`` on an empty list so a
    degenerate run (every window discarded) records a sentinel rather
    than raising inside the verdict builder.
    """
    if not samples:
        return 0.0
    return statistics.median(accessor(s) for s in samples)


def _wait_for_daemon_socket(socket_path: Path) -> None:
    """Block until the daemon's AF_UNIX socket appears, or raise."""
    deadline = time.monotonic() + _DAEMON_READY_TIMEOUT_SEC
    while time.monotonic() < deadline:
        if socket_path.exists():
            return
        time.sleep(0.05)
    raise RuntimeError(f"daemon did not bind {socket_path} within {_DAEMON_READY_TIMEOUT_SEC}s")


def _drain_subscriber(
    *,
    result: _SubscriberResult,
    match: list[str],
    since: str,
    socket_path: str,
    ready_barrier: threading.Barrier,
) -> None:
    """Park in ``subscribe`` and count every delivered frame.

    Runs on a dedicated thread. Trips ``ready_barrier`` once the
    subscriber socket is open and the engine is registered, so the
    orchestrator does not start emitting before every subscriber is
    parked. The generator drains until the daemon closes the connection
    (orchestrator terminates the daemon at teardown), then returns.
    """
    from waitbus import subscribe

    try:
        stream = subscribe(match, source=None, since=since, socket_path=socket_path)
    except Exception as exc:
        result.error = f"{exc.__class__.__name__}: {exc}"
        # The barrier was never tripped by this party; aborting it wakes
        # every other waiter (including the orchestrator) with
        # ``BrokenBarrierError`` rather than wedging on the missing party.
        ready_barrier.abort()
        return
    try:
        # ``open_subscriber`` has run by the time the generator is
        # constructed; priming with ``next`` would block on the first
        # frame, so we trip the barrier right after construction and
        # accept a sub-millisecond register-vs-emit race (the ``since``
        # replay cursor covers any frame that lands in that gap).
        ready_barrier.wait()
        for _frame in stream:
            result.received += 1
    except threading.BrokenBarrierError:
        # Another subscriber aborted the barrier; this window is being
        # abandoned. Close the stream and return quietly.
        stream.close()
    except Exception as exc:
        result.error = f"{exc.__class__.__name__}: {exc}"
        stream.close()


def _emit_burst(
    *,
    n_events: int,
    seed_scope_id: str,
    db_path: Path,
    doorbell_path: Path,
    emit_rate_hz: float,
) -> None:
    """Emit ``n_events`` scope-owned events at a fixed open-loop rate.

    Each event carries the bare ``seed_scope_id`` as ``owner`` so it
    matches every subscriber's ``fields.owner="<scope>"`` predicate.
    The inter-emit interval is held fixed (open-loop) regardless of how
    long a given ``emit`` took, so a slow emit does not shrink the
    workload and hide cost.
    """
    from waitbus._emit import emit
    from waitbus._types import EventInsert

    interval_ns = round(1e9 / emit_rate_hz) if emit_rate_hz > 0 else 0
    t0 = time.monotonic_ns()
    for i in range(n_events):
        if interval_ns:
            target = t0 + i * interval_ns
            now = time.monotonic_ns()
            if now < target:
                time.sleep((target - now) / 1e9)
        emit(
            EventInsert(
                delivery_id=f"bench-per-delivery:{seed_scope_id}:{i}:{uuid.uuid4().hex[:8]}",
                source=SEED_SOURCE,
                event_type=SEED_EVENT_TYPE,
                owner=seed_scope_id,
                repo="waitbus/per-delivery-cost-bench",
                received_at=time.time_ns(),
                payload_json='{"kind": "per_delivery_seed"}',
                ingest_method="bench-per-delivery",
            ),
            db_path=db_path,
            doorbell_path=doorbell_path,
        )


def _measure_window(
    *,
    daemon_pid: int,
    n_events: int,
    m_subscribers: int,
    db_path: Path,
    doorbell_path: Path,
    socket_path: Path,
    emit_rate_hz: float,
) -> _WindowSample:
    """Run one emit burst against ``m`` parked subscribers; return the daemon deltas.

    Spawns ``m`` subscriber threads, waits for all to park, samples the
    daemon's CPU counters, emits the burst, lets the daemon drain, then
    samples again. The subscriber threads are torn down by the caller
    (they unblock when the daemon's connection drops at daemon
    teardown); within a window they stay parked, so the only daemon work
    measured is the fan-out of this window's emits.
    """
    # Emit a fresh anchor so each subscriber's ``since`` cursor opens
    # just before this window's emits -- the daemon's seq-replay covers
    # any frame that lands in the register-vs-emit gap.
    seed_scope_id = f"per-delivery-{uuid.uuid4().hex[:12]}"
    anchor_id = emit_anchor_event(
        seed_scope_id=seed_scope_id,
        db_path=db_path,
        doorbell_path=doorbell_path,
        repo="waitbus/per-delivery-cost-bench",
        ingest_method="bench-per-delivery",
        delivery_id_prefix="bench-per-delivery-anchor",
    )

    match = [f'fields.owner="{seed_scope_id}"']
    results: list[_SubscriberResult] = [_SubscriberResult() for _ in range(m_subscribers)]
    # +1 party for the orchestrator: it trips the barrier once every
    # subscriber has parked, then proceeds to emit.
    ready_barrier = threading.Barrier(m_subscribers + 1)
    threads: list[threading.Thread] = []
    for sub_result in results:
        thread = threading.Thread(
            target=_drain_subscriber,
            kwargs={
                "result": sub_result,
                "match": match,
                "since": anchor_id,
                "socket_path": str(socket_path),
                "ready_barrier": ready_barrier,
            },
            daemon=True,
        )
        thread.start()
        threads.append(thread)

    # Wait for every subscriber to register before emitting. A
    # subscriber that failed during construction aborts the barrier;
    # surface that as a window failure rather than measuring a window
    # with fewer subscribers than requested.
    try:
        ready_barrier.wait()
    except threading.BrokenBarrierError as exc:
        errors = [r.error for r in results if r.error is not None]
        raise RuntimeError(f"subscriber registration failed for m={m_subscribers}: {errors}") from exc

    utime_before, stime_before = read_daemon_cpu_ns(daemon_pid)
    sched_before = read_daemon_schedstat(daemon_pid)

    _emit_burst(
        n_events=n_events,
        seed_scope_id=seed_scope_id,
        db_path=db_path,
        doorbell_path=doorbell_path,
        emit_rate_hz=emit_rate_hz,
    )

    # Let the daemon finish draining every queued fan-out write before
    # the post-burst sample.
    time.sleep(_DRAIN_SETTLE_SEC)

    utime_after, stime_after = read_daemon_cpu_ns(daemon_pid)
    sched_after = read_daemon_schedstat(daemon_pid)

    deliveries = n_events * m_subscribers
    return _WindowSample(
        n_events=n_events,
        m_subscribers=m_subscribers,
        deliveries=deliveries,
        utime_delta_ns=utime_after - utime_before,
        stime_delta_ns=stime_after - stime_before,
        schedstat_run_delta_ns=sched_after.run_time_ns - sched_before.run_time_ns,
        schedstat_pcount_delta=sched_after.pcount - sched_before.pcount,
    )


def _spawn_daemon(
    *,
    state_dir: Path,
    runtime_dir: Path,
    daemon_cores: set[int] | None,
) -> tuple[subprocess.Popen[bytes], Path, Path, Path]:
    """Spawn ``waitbus broadcast serve`` in the tmp dirs; pin if requested.

    Mirrors the daemon-spawn + ``preexec_fn`` affinity pin in
    ``bench_multistream_proof._run_bench``. Returns the process plus the
    resolved socket / doorbell / db paths.
    """
    waitbus_path = shutil.which("waitbus") or "waitbus"
    daemon_env = os.environ.copy()
    daemon_env["WAITBUS_STATE_DIR"] = str(state_dir)
    daemon_env["WAITBUS_RUNTIME_DIR"] = str(runtime_dir)
    daemon_env["WAITBUS_HEARTBEAT_SEC"] = "3600"
    socket_path = runtime_dir / "broadcast.sock"
    doorbell_path = runtime_dir / "doorbell.sock"
    db_path = state_dir / "github.db"

    daemon_preexec_fn: Callable[[], None] | None = None
    if daemon_cores is not None:
        pinned_cores = frozenset(daemon_cores)

        def _pin_daemon_to_cores() -> None:
            os.sched_setaffinity(0, pinned_cores)

        daemon_preexec_fn = _pin_daemon_to_cores

    proc = subprocess.Popen(
        [waitbus_path, "broadcast", "serve"],
        env=daemon_env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        preexec_fn=daemon_preexec_fn,
    )
    return proc, socket_path, doorbell_path, db_path


def run_calibration(
    *,
    n_events: int,
    subscriber_sweep: tuple[int, ...],
    emit_rate_hz: float,
    repeats: int,
    allow_unpinned: bool,
) -> dict[str, object]:
    """Run the full calibration sweep; return the verdict dict.

    For each subscriber count ``m`` in ``subscriber_sweep`` runs
    ``repeats`` windows (the first ``_WARMUP_WINDOWS`` discarded) and
    records the median per-delivery utime-ns / stime-ns / schedstat-ns.
    """
    if not sys.platform.startswith("linux"):
        raise RuntimeError(f"bench requires Linux for /proc CPU sampling; sys.platform={sys.platform!r}")

    schedstat_available = schedstat_substrate_available()

    orchestrator_cores: set[int] | None = None
    daemon_cores: set[int] | None = None
    pinned = False
    if not allow_unpinned:
        try:
            orchestrator_cores, daemon_cores = compute_orchestrator_and_daemon_cores()
            os.sched_setaffinity(0, orchestrator_cores)
            pinned = True
        except Exception as exc:
            structured(
                _logger,
                logging.WARNING,
                "bench_per_delivery_pin_failed",
                error=f"{exc.__class__.__name__}: {exc}",
            )
            orchestrator_cores = None
            daemon_cores = None

    tmp_dir = Path(tempfile.mkdtemp(prefix="waitbus-bench-per-delivery-"))
    state_dir = tmp_dir / "state"
    runtime_dir = tmp_dir / "runtime"
    state_dir.mkdir(parents=True, exist_ok=True)
    runtime_dir.mkdir(parents=True, exist_ok=True)

    daemon_proc, socket_path, doorbell_path, db_path = _spawn_daemon(
        state_dir=state_dir,
        runtime_dir=runtime_dir,
        daemon_cores=daemon_cores,
    )
    daemon_pid = daemon_proc.pid

    # Verify the pin actually took. A ``preexec_fn`` can silently fail
    # under a restrictive cgroup; record what the daemon ACTUALLY
    # inherited rather than what we intended.
    daemon_affinity_actual: list[int] | None = None
    if daemon_cores is not None:
        actual = os.sched_getaffinity(daemon_pid)
        daemon_affinity_actual = sorted(actual)
        if actual != daemon_cores:
            structured(
                _logger,
                logging.WARNING,
                "bench_per_delivery_daemon_pin_mismatch",
                expected=sorted(daemon_cores),
                actual=daemon_affinity_actual,
            )
            pinned = False

    per_m_results: list[dict[str, object]] = []
    try:
        _wait_for_daemon_socket(socket_path)

        for m_subscribers in subscriber_sweep:
            samples: list[_WindowSample] = []
            for window_idx in range(repeats):
                sample = _measure_window(
                    daemon_pid=daemon_pid,
                    n_events=n_events,
                    m_subscribers=m_subscribers,
                    db_path=db_path,
                    doorbell_path=doorbell_path,
                    socket_path=socket_path,
                    emit_rate_hz=emit_rate_hz,
                )
                discarded = window_idx < _WARMUP_WINDOWS
                structured(
                    _logger,
                    logging.INFO,
                    "bench_per_delivery_window",
                    m_subscribers=m_subscribers,
                    window=window_idx,
                    discarded=discarded,
                    deliveries=sample.deliveries,
                    utime_delta_ns=sample.utime_delta_ns,
                    schedstat_run_delta_ns=sample.schedstat_run_delta_ns,
                    per_delivery_utime_ns=round(sample.per_delivery_utime_ns, 2),
                    per_delivery_schedstat_ns=round(sample.per_delivery_schedstat_ns, 2),
                )
                if not discarded:
                    samples.append(sample)

            per_m_results.append(
                {
                    "m_subscribers": m_subscribers,
                    "n_events_per_window": n_events,
                    "windows_kept": len(samples),
                    "median_per_delivery_utime_ns": round(
                        median_per_delivery(samples, accessor=lambda s: s.per_delivery_utime_ns), 3
                    ),
                    "median_per_delivery_stime_ns": round(
                        median_per_delivery(samples, accessor=lambda s: s.per_delivery_stime_ns), 3
                    ),
                    "median_per_delivery_schedstat_ns": round(
                        median_per_delivery(samples, accessor=lambda s: s.per_delivery_schedstat_ns), 3
                    ),
                }
            )
    finally:
        daemon_proc.terminate()
        try:
            daemon_proc.wait(timeout=_SUBSCRIBER_JOIN_TIMEOUT_SEC)
        except subprocess.TimeoutExpired:
            daemon_proc.kill()

    return {
        "bench": _BENCH_NAME,
        "linux": True,
        "pinned": pinned,
        "orchestrator_cores": sorted(orchestrator_cores) if orchestrator_cores is not None else None,
        "daemon_cores_intended": sorted(daemon_cores) if daemon_cores is not None else None,
        "daemon_affinity_actual": daemon_affinity_actual,
        "schedstat_substrate_available": schedstat_available,
        "emit_rate_hz": emit_rate_hz,
        "repeats": repeats,
        "warmup_windows": _WARMUP_WINDOWS,
        "results": per_m_results,
    }


def main(argv: list[str] | None = None) -> int:
    """CLI entry: run the calibration sweep and print / write the verdict."""
    parser = argparse.ArgumentParser(
        prog=_BENCH_NAME,
        description="Measure the daemon's irreducible CPU cost per (event x subscriber) delivery.",
    )
    parser.add_argument(
        "--n",
        type=int,
        default=_DEFAULT_N,
        help=f"events emitted per window (default {_DEFAULT_N})",
    )
    parser.add_argument(
        "--subscribers",
        type=int,
        nargs="+",
        default=list(_DEFAULT_SUBSCRIBERS),
        help=f"subscriber-count sweep (default {' '.join(str(m) for m in _DEFAULT_SUBSCRIBERS)})",
    )
    parser.add_argument(
        "--emit-rate-hz",
        type=float,
        default=_DEFAULT_EMIT_RATE_HZ,
        help=f"open-loop emit rate (default {_DEFAULT_EMIT_RATE_HZ})",
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=_DEFAULT_REPEATS,
        help=f"windows per subscriber count, first {_WARMUP_WINDOWS} discarded (default {_DEFAULT_REPEATS})",
    )
    parser.add_argument(
        "--allow-unpinned",
        action="store_true",
        help="skip the orchestrator/daemon core pin (noisier; for hosts whose cgroup forbids affinity)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="write the verdict JSON to this path (default: stdout only)",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stderr)

    verdict = run_calibration(
        n_events=args.n,
        subscriber_sweep=tuple(args.subscribers),
        emit_rate_hz=args.emit_rate_hz,
        repeats=args.repeats,
        allow_unpinned=args.allow_unpinned,
    )

    rendered = json.dumps(verdict, indent=2, sort_keys=True)
    print(rendered)
    if args.output is not None:
        args.output.write_text(rendered + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
