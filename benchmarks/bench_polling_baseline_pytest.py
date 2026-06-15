"""Polling-cycle latency for tailing and parsing report.xml.

Measures the wall-clock interval a polling agent would experience when
it detects a pytest run completion by watching ``report.xml``'s mtime.
The bench is the *counterfactual* to waitbus's TTFAE: instead of a push
notification, the agent polls ``os.stat(report.xml).st_mtime_ns`` at a
fixed 1-second interval and observes the change.

Measurement definition
----------------------
- ``t_inject_ns``: recorded by the workload thread immediately before
  it calls ``os.utime(report_path)`` to update the file's mtime.
- ``t_observe_ns``: recorded by the polling loop immediately after it
  detects ``st_mtime_ns != prev_mtime_ns``.
- Latency = ``t_observe_ns - t_inject_ns``.

The polling interval is the floor on latency. At 1-second poll
interval, the expected p50 is ~500 ms and p99 is ~990 ms. The
distribution is approximately uniform over [0, poll_interval].

Per-iteration protocol
----------------------
1. Reset: record current mtime as ``prev_mtime_ns``.
2. Workload thread waits a random delay in ``[0, poll_interval)``
   then calls ``os.utime(report_path, None)`` and records
   ``t_inject_ns``.
3. Polling loop checks ``os.stat(report_path).st_mtime_ns`` every
   1.0 s; when it observes a change it records ``t_observe_ns``.
4. Latency is recorded; next iteration begins.

Sample posture
--------------
N=500 + 50 warmup. Wall-clock ~250 s = ~4 minutes per phase.
``--smoke`` runs N=50 + 5 warmup.

Invocation
----------
::

    # Smoke (~25 seconds)
    uv run python -m benchmarks.bench_polling_baseline_pytest --smoke

    # Production baseline (~8 minutes, two phases)
    taskset -c 2,3 uv run python -m benchmarks.bench_polling_baseline_pytest \\
        --output benchmarks/baselines/polling_baseline_pytest.json

    # Regression check in CI
    uv run python -m benchmarks.bench_polling_baseline_pytest --check-regression
"""

from __future__ import annotations

import argparse
import queue
import random
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

_BENCH_NAME = "polling_baseline_pytest"
_DEFAULT_N = 500
_DEFAULT_WARMUP = 50
_SMOKE_N = 50
_SMOKE_WARMUP = 5
_POLL_INTERVAL_S = 1.0
_BASELINE_PATH = Path(__file__).resolve().parent / "baselines" / f"{_BENCH_NAME}.json"
_RESULTS_DIR = Path(__file__).resolve().parent / "results"

_INJECT_QUEUE_TIMEOUT_SEC: Final[float] = 5.0
"""Timeout for ``inject_queue.get()`` when the main loop retrieves the
workload thread's recorded ``t_inject_ns`` (seconds).

5 s is well above the per-iteration upper bound (inject delay + one
poll interval = at most ``_POLL_INTERVAL_S + poll overhead``); a
timeout here indicates the workload thread has stalled or died.
"""

_WORKER_JOIN_TIMEOUT_SEC: Final[float] = 5.0
"""Timeout passed to ``worker.join()`` during loop teardown (seconds).

The workload thread is a daemon thread and will be reaped by the
interpreter on process exit; the join timeout prevents an abnormally
long bench teardown from masking the real measurement result.
"""


class _WorkloadThread(threading.Thread):
    """Background thread that injects mtime changes at random intervals.

    Each iteration: waits a random delay in ``[0, poll_interval)``
    then calls ``os.utime(path, None)`` to bump the mtime and records
    ``t_inject_ns`` into ``inject_queue``.

    After the iteration the thread blocks on ``_next_event`` until the
    main thread calls :meth:`request_next` to start the next inject.
    This coordination prevents the workload thread from racing ahead
    of the polling loop and losing samples.
    """

    def __init__(
        self,
        *,
        report_path: Path,
        poll_interval_s: float,
        rng_seed: int,
        n_total: int,
    ) -> None:
        super().__init__(name="waitbus-bench-workload-pytest", daemon=True)
        self._path = report_path
        self._poll_interval_s = poll_interval_s
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
            # Wait for the main thread's permission to inject.
            self._next_event.wait()
            self._next_event.clear()
            if self._stop_event.is_set():
                return
            # Random delay within one poll interval.
            delay = self._rng.uniform(0.0, self._poll_interval_s)
            if self._stop_event.wait(delay):
                return
            # Bump mtime and record inject time.
            self._path.touch()
            t_inject_ns = time.time_ns()
            self.inject_queue.put(t_inject_ns)


def _run_loop(
    *,
    report_path: Path,
    n: int,
    warmup: int,
    rng_seed: int,
    hdr: HdrRecorder,
) -> None:
    """Run one bench loop and record into ``hdr`` after warmup discard."""
    n_total = n + warmup
    worker = _WorkloadThread(
        report_path=report_path,
        poll_interval_s=_POLL_INTERVAL_S,
        rng_seed=rng_seed,
        n_total=n_total,
    )
    worker.start()
    try:
        for i in range(n_total):
            # Snapshot the current mtime before unblocking the workload.
            prev_mtime_ns = report_path.stat().st_mtime_ns
            worker.request_next()

            # Polling loop: check every poll_interval_s.
            while True:
                time.sleep(_POLL_INTERVAL_S)
                current_mtime_ns = report_path.stat().st_mtime_ns
                t_observe_ns = time.time_ns()
                if current_mtime_ns != prev_mtime_ns:
                    break

            t_inject_ns = worker.inject_queue.get(timeout=_INJECT_QUEUE_TIMEOUT_SEC)
            latency = t_observe_ns - t_inject_ns
            if i >= warmup:
                hdr.record(latency)
    finally:
        worker.stop()
        worker.join(timeout=_WORKER_JOIN_TIMEOUT_SEC)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Measure polling-cycle latency for report.xml mtime polling.",
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
        help="RNG seed for inject-delay randomization (default: 0xC1B5).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help=("path to write the result JSON (default: benchmarks/results/polling_baseline_pytest_<host>_<ts>.json)"),
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
        f"[{_BENCH_NAME}] n={n} warmup={warmup} poll_interval={_POLL_INTERVAL_S}s",
        file=sys.stderr,
    )

    started_at_ns = time.time_ns()
    hdr_main = HdrRecorder()
    hdr_gc_off: HdrRecorder | None = None if args.no_gc_off else HdrRecorder()

    with tempfile.TemporaryDirectory(prefix="waitbus-bench-polling-pytest-") as tmp_str:
        tmp_dir = Path(tmp_str)
        report_path = tmp_dir / "report.xml"
        # Create the file so stat() works before the first inject.
        report_path.write_text("<testsuites/>", encoding="utf-8")

        print(f"[{_BENCH_NAME}] gc-on", file=sys.stderr)
        _run_loop(
            report_path=report_path,
            n=n,
            warmup=warmup,
            rng_seed=args.seed,
            hdr=hdr_main,
        )

        if hdr_gc_off is not None:
            print(f"[{_BENCH_NAME}] gc-off", file=sys.stderr)
            with gc_disabled():
                _run_loop(
                    report_path=report_path,
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
        rate_hz=0.0,  # not open-loop scheduled; wall-clock is poll_interval-bound
        hdr_main=hdr_main,
        hdr_gc_off=hdr_gc_off,
        environment=env,
        extra={
            "smoke": args.smoke,
            "poll_interval_s": _POLL_INTERVAL_S,
            "method": "os.stat(report.xml).st_mtime_ns",
            "scope_note": ("polling counterfactual; no waitbus daemon involved; latency floor is poll_interval_s"),
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
