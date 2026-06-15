"""Event-driven file-change latency via ``inotifywait``.

Measures the wall-clock interval from a workload thread's ``os.utime()``
call to the moment the main thread reads the corresponding line from
``inotifywait``'s stdout.  Unlike the ``bench_polling_baseline_fs``
bench (which polls ``os.stat()`` on a fixed 1-second interval), this
bench uses the kernel's inotify mechanism via the ``inotifywait`` binary
from ``inotify-tools``.

Platform note
-------------
``inotifywait`` is Linux-only (it wraps Linux's ``inotify(7)`` kernel
interface).  On macOS the bench exits cleanly with exit code 0 and a
diagnostic message.  On Linux without ``inotifywait`` installed the
bench also exits cleanly with a remediation message.

Latency interpretation
----------------------
Because inotify is event-driven rather than polled, the expected
latency is **microseconds to low milliseconds** — orders of magnitude
below the 1-second floor seen in the ``os.stat()`` polling baselines.
This bench exists to document that waitbus's filesystem source has a
comparable event-delivery latency to raw inotify.  The value-add waitbus
provides over inotifywait is **feature-set** (multi-source predicate
waits, durable subscribers, MCP egress), not latency.

Measurement definition
----------------------
- ``t_inject_ns``: recorded by the workload thread immediately before
  it calls ``os.utime(watched_path, None)``.
- ``t_observe_ns``: recorded by the main thread immediately after it
  reads a matching line from ``inotifywait``'s stdout.
- Latency = ``t_observe_ns - t_inject_ns``.

Per-iteration protocol
----------------------
1. Workload thread waits for ``_next_event``.
2. Main thread signals ``_next_event``; workload thread sleeps a random
   delay in ``[0, 5 ms]`` (a realistic scheduling jitter floor; there
   is no polling-interval floor), calls ``os.utime``, records
   ``t_inject_ns``, and puts it on ``inject_queue``.
3. Main thread reads lines from ``inotifywait``'s stdout until it sees
   a line containing the watched file's name, records ``t_observe_ns``.
4. Latency is recorded; next iteration begins.

Sample posture
--------------
N=500 + 50 warmup.  Wall-clock ~30 s per phase (no polling-interval
floor; inotify events arrive within a few milliseconds).
``--smoke`` runs N=50 + 5 warmup.

Invocation
----------
::

    # Smoke (~3 seconds, N=50)
    uv run python -m benchmarks.bench_polling_baseline_fs_inotifywait --smoke

    # Production baseline (~1 minute, two phases)
    taskset -c 2,3 uv run python -m benchmarks.bench_polling_baseline_fs_inotifywait \\
        --output benchmarks/baselines/polling_baseline_fs_inotifywait.json

    # Regression check in CI
    uv run python -m benchmarks.bench_polling_baseline_fs_inotifywait --check-regression
"""

from __future__ import annotations

import argparse
import contextlib
import io
import os
import queue
import random
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Final

from benchmarks._bench_shared import CANONICAL_RNG_SEED, RNG_GC_XOR_MASK

from ._harness import (
    HdrRecorder,
    check_regression,
    collect_result,
    environment_report,
    gc_disabled,
    print_percentile_summary,
    resolve_output_path,
    write_result,
)

_BENCH_NAME = "polling_baseline_fs_inotifywait"
_DEFAULT_N = 500
_DEFAULT_WARMUP = 50
_SMOKE_N = 50
_SMOKE_WARMUP = 5
# Small jitter window: no polling-interval floor; this simulates realistic
# scheduling variability between the workload trigger and the utime call.
_INJECT_JITTER_S = 0.005
_BASELINE_PATH = Path(__file__).resolve().parent / "baselines" / f"{_BENCH_NAME}.json"
_RESULTS_DIR = Path(__file__).resolve().parent / "results"

_INJECT_QUEUE_TIMEOUT_SEC: Final[float] = 5.0
"""Timeout for ``inject_queue.get()`` when the main loop retrieves the
workload thread's recorded ``t_inject_ns`` (seconds).

5 s is well above the per-iteration upper bound (inotify event + inject
jitter = at most ``_INJECT_JITTER_S + kernel scheduling overhead``);
a timeout here indicates the workload thread has stalled or died.
"""

_WORKER_JOIN_TIMEOUT_SEC: Final[float] = 5.0
"""Timeout passed to ``worker.join()`` during loop teardown (seconds).

The workload thread is a daemon thread and will be reaped by the
interpreter on process exit; the join timeout prevents an abnormally
long bench teardown from masking the real measurement result.
"""

_INOTIFY_SETUP_TIMEOUT_SEC: Final[float] = 5.0
"""Maximum time to wait for ``inotifywait`` to emit "Watches established."
on stderr before the bench aborts (seconds).

inotifywait typically establishes the watch in under 50 ms; 5 s covers
a momentarily loaded kernel without indefinitely blocking the bench.
"""

_INOTIFY_READLINE_MIN_SEC: Final[float] = 0.1
"""Minimum time budget passed to ``_readline_with_timeout`` when
draining inotifywait's stderr during the setup handshake (seconds).

Keeps the timeout from collapsing to zero as the setup deadline
approaches and prevents a busy-poll on the last few milliseconds.
"""

_INOTIFY_ITER_DEADLINE_SEC: Final[float] = 5.0
"""Per-iteration deadline for inotifywait to produce a matching ATTRIB
line (seconds).

The inject jitter is at most ``_INJECT_JITTER_S`` (5 ms) plus kernel
scheduling overhead; a 5-second wait indicates a real malfunction
(e.g. the watched directory was removed or inotifywait stalled).
"""


class _WorkloadThread(threading.Thread):
    """Background thread that injects mtime changes at known times.

    Each iteration: waits for the main thread's ``request_next()`` signal,
    sleeps a random jitter in ``[0, _INJECT_JITTER_S]``, calls
    ``os.utime(path, None)``, records ``t_inject_ns``, and puts it on
    ``inject_queue``.

    The small jitter prevents a perfectly synchronous inject that would
    make the latency measurement unrealistically tight (the inotify event
    and the time.time_ns() call would be back-to-back with no scheduling
    variation).
    """

    def __init__(
        self,
        *,
        watched_path: Path,
        rng_seed: int,
        n_total: int,
    ) -> None:
        super().__init__(name="waitbus-bench-workload-inotifywait", daemon=True)
        self._path = watched_path
        self._rng = random.Random(rng_seed)
        self._n_total = n_total
        self._next_event = threading.Event()
        self._stop_event = threading.Event()
        self.inject_queue: queue.Queue[int] = queue.Queue()

    def request_next(self) -> None:
        """Signal the workload thread to proceed with the next inject."""
        self._next_event.set()

    def stop(self) -> None:
        """Signal the workload thread to exit at the next opportunity."""
        self._stop_event.set()
        self._next_event.set()  # unblock if waiting

    def run(self) -> None:
        for _ in range(self._n_total):
            if self._stop_event.is_set():
                return
            self._next_event.wait()
            self._next_event.clear()
            if self._stop_event.is_set():
                return
            jitter = self._rng.uniform(0.0, _INJECT_JITTER_S)
            if self._stop_event.wait(jitter):
                return
            os.utime(str(self._path), None)
            t_inject_ns = time.time_ns()
            self.inject_queue.put(t_inject_ns)


def _readline_with_timeout(stdout: io.BufferedReader, timeout_s: float) -> bytes:
    """Read one line from ``stdout`` with a wall-clock timeout.

    Spawns a daemon thread to perform the blocking ``readline()`` call
    and waits on a :class:`threading.Event` for up to ``timeout_s``
    seconds.  Returns the raw bytes line, or an empty ``b""`` on
    timeout.

    Using a helper thread is necessary because ``io.BufferedReader``
    does not expose a non-blocking readline; ``select()`` works on the
    underlying file descriptor for byte availability but not for full
    lines.  The daemon thread is intentionally leaked on timeout --
    it will unblock when the next byte arrives or when the process
    exits.
    """
    result: list[bytes] = []
    done = threading.Event()

    def _reader() -> None:
        result.append(stdout.readline())
        done.set()

    threading.Thread(target=_reader, daemon=True).start()
    done.wait(timeout=timeout_s)
    return result[0] if result else b""


def _run_loop(
    *,
    watched_path: Path,
    watched_dir: Path,
    n: int,
    warmup: int,
    rng_seed: int,
    hdr: HdrRecorder,
) -> None:
    """Run one bench loop using a fresh ``inotifywait`` process.

    A single ``inotifywait -m -e attrib <dir>`` process watches
    ``watched_dir`` for the duration of the loop. The ``attrib``
    event matches what ``os.utime(path, None)`` fires (IN_ATTRIB --
    mtime change); ``-e modify`` would NOT fire on utime alone
    because IN_MODIFY only fires on actual content writes.  Each iteration
    reads output lines until one names ``watched_path``; the main
    thread records ``t_observe_ns`` at that moment.

    A per-iteration 5-second deadline guards against inotifywait
    silently stalling (e.g. if the watched directory is removed).
    The deadline is generous: the inject jitter is at most
    ``_INJECT_JITTER_S`` (5 ms) plus kernel scheduling overhead; a
    5-second wait indicates a real malfunction.
    """
    n_total = n + warmup
    watched_filename = watched_path.name

    # ``inotifywait -m`` writes its setup confirmation to stderr, NOT
    # stdout (the canonical "Watches established." line). Capture
    # stderr so we can wait for that line synchronously before
    # starting the workload thread; without this synchronization the
    # workload's first ``os.utime`` can fire BEFORE the kernel has
    # registered the watch, and the corresponding MODIFY event is
    # never emitted.
    proc = subprocess.Popen(
        ["inotifywait", "-m", "-e", "attrib", str(watched_dir)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout: io.BufferedReader = proc.stdout  # type: ignore[assignment]
    stderr: io.BufferedReader = proc.stderr  # type: ignore[assignment]

    # Drain stderr until "Watches established." appears or the
    # process dies. inotifywait emits this exactly once on startup.
    setup_deadline = time.monotonic() + _INOTIFY_SETUP_TIMEOUT_SEC
    while time.monotonic() < setup_deadline:
        raw = _readline_with_timeout(stderr, max(_INOTIFY_READLINE_MIN_SEC, setup_deadline - time.monotonic()))
        if not raw:
            continue
        if b"Watches established" in raw:
            break
    else:
        proc.terminate()
        raise RuntimeError(f"inotifywait did not establish watches within {_INOTIFY_SETUP_TIMEOUT_SEC:.0f}s")

    # After setup, stderr is no longer interesting; close-drain it in
    # a daemon thread so the kernel doesn't backpressure inotifywait
    # if it writes warnings later.
    def _drain_stderr() -> None:
        with contextlib.suppress(Exception):
            for _ in iter(stderr.readline, b""):
                pass

    threading.Thread(target=_drain_stderr, name="waitbus-bench-inot-stderr", daemon=True).start()

    worker = _WorkloadThread(
        watched_path=watched_path,
        rng_seed=rng_seed,
        n_total=n_total,
    )
    worker.start()
    try:
        for i in range(n_total):
            worker.request_next()

            # Read lines until one matches the watched file.
            # inotifywait -m output format:
            #   <dir>/ ATTRIB <filename>
            deadline = time.monotonic() + _INOTIFY_ITER_DEADLINE_SEC
            t_observe_ns = 0
            matched = False
            while not matched:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RuntimeError(
                        f"iteration {i}: inotifywait produced no matching line within "
                        f"{_INOTIFY_ITER_DEADLINE_SEC:.0f} s"
                    )
                raw = _readline_with_timeout(stdout, remaining)
                if not raw:
                    raise RuntimeError(f"iteration {i}: inotifywait readline timed out or stdout closed")
                t_observe_ns = time.time_ns()
                line = raw.decode(errors="replace").rstrip("\n")
                if watched_filename in line and "ATTRIB" in line:
                    matched = True

            t_inject_ns = worker.inject_queue.get(timeout=_INJECT_QUEUE_TIMEOUT_SEC)
            latency = t_observe_ns - t_inject_ns
            if i >= warmup:
                hdr.record(latency)
    finally:
        worker.stop()
        worker.join(timeout=_WORKER_JOIN_TIMEOUT_SEC)
        proc.terminate()
        proc.wait(timeout=_INOTIFY_SETUP_TIMEOUT_SEC)


def main(argv: list[str] | None = None) -> int:
    """Entry point for the inotifywait event-driven latency bench."""
    if sys.platform == "darwin":
        print(
            "inotifywait is Linux-only (from inotify-tools); skipping on darwin",
            file=sys.stderr,
        )
        return 0

    inotifywait_bin = shutil.which("inotifywait")
    if inotifywait_bin is None:
        print(
            f"[{_BENCH_NAME}] inotifywait binary not found; install inotify-tools (apt install inotify-tools)",
            file=sys.stderr,
        )
        return 0

    parser = argparse.ArgumentParser(
        description="Measure event-driven file-change latency via inotifywait.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--n",
        type=int,
        default=_DEFAULT_N,
        help="number of measurement samples (default: 500)",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=_DEFAULT_WARMUP,
        help="number of leading samples to discard (default: 50)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=CANONICAL_RNG_SEED,
        help="RNG seed for inject-jitter randomization (default: 0xC1B5).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help=(
            "path to write the result JSON "
            "(default: benchmarks/results/polling_baseline_fs_inotifywait_<host>_<ts>.json)"
        ),
    )
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="quick run: N=50, warmup=5, no regression check.",
    )
    parser.add_argument(
        "--check-regression",
        action="store_true",
        help=(
            "after the run, compare p99 (gc-enabled) against "
            f"{_BASELINE_PATH.relative_to(_BASELINE_PATH.parent.parent)}; "
            "exit non-zero on >25% regression."
        ),
    )
    parser.add_argument(
        "--no-gc-off",
        action="store_true",
        help="skip the gc-disabled companion run.",
    )
    args = parser.parse_args(argv)

    n = _SMOKE_N if args.smoke else args.n
    warmup = _SMOKE_WARMUP if args.smoke else args.warmup

    env = environment_report()
    print(
        f"[{_BENCH_NAME}] n={n} warmup={warmup} inject_jitter={_INJECT_JITTER_S * 1000:.1f}ms",
        file=sys.stderr,
    )
    print(
        f"[{_BENCH_NAME}] NOTE: inotifywait is event-driven; expected latency is "
        "microseconds to low milliseconds (no polling-interval floor). "
        "waitbus's value-add over inotifywait is feature-set, not latency.",
        file=sys.stderr,
    )

    started_at_ns = time.time_ns()
    hdr_main = HdrRecorder()
    hdr_gc_off: HdrRecorder | None = None if args.no_gc_off else HdrRecorder()

    with tempfile.TemporaryDirectory(prefix="waitbus-bench-inotifywait-") as tmp_str:
        tmp_dir = Path(tmp_str)
        watched_path = tmp_dir / "watched.txt"
        # Create the file before inotifywait starts watching.
        watched_path.write_text("", encoding="utf-8")

        print(f"[{_BENCH_NAME}] gc-on", file=sys.stderr)
        _run_loop(
            watched_path=watched_path,
            watched_dir=tmp_dir,
            n=n,
            warmup=warmup,
            rng_seed=args.seed,
            hdr=hdr_main,
        )

        if hdr_gc_off is not None:
            print(f"[{_BENCH_NAME}] gc-off", file=sys.stderr)
            with gc_disabled():
                _run_loop(
                    watched_path=watched_path,
                    watched_dir=tmp_dir,
                    n=n,
                    warmup=warmup,
                    rng_seed=args.seed ^ RNG_GC_XOR_MASK,
                    hdr=hdr_gc_off,
                )

    ended_at_ns = time.time_ns()

    result = collect_result(
        bench_name=_BENCH_NAME,
        started_at_ns=started_at_ns,
        ended_at_ns=ended_at_ns,
        n_warmup_discarded=warmup,
        rate_hz=0.0,  # event-driven; no polling interval
        hdr_main=hdr_main,
        hdr_gc_off=hdr_gc_off,
        environment=env,
        extra={
            "smoke": args.smoke,
            "inject_jitter_s": _INJECT_JITTER_S,
            "method": "inotifywait -m -e attrib",
            "inotifywait_path": inotifywait_bin,
            "scope_note": (
                "event-driven baseline; no waitbus daemon involved; "
                "no polling-interval floor; "
                "waitbus value-add over inotifywait is feature-set not latency"
            ),
        },
    )

    output_path = resolve_output_path(_BENCH_NAME, _RESULTS_DIR, args.output, env)

    write_result(result, output_path)
    print(f"[{_BENCH_NAME}] wrote {output_path}", file=sys.stderr)

    print_percentile_summary(result, bench_name=_BENCH_NAME)

    if args.check_regression and not args.smoke:
        ok, msg = check_regression(result, _BASELINE_PATH)
        print(f"[{_BENCH_NAME}] regression-check: {msg}", file=sys.stderr)
        if not ok:
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
