"""Notify-to-wake doorbell micro-bench (regression-gate only).

Measures the kernel-side wake roundtrip of waitbus's doorbell primitive:
``_doorbell.ring()`` writes one byte to the AF_UNIX SOCK_STREAM
listener; the daemon's accept-thread reads the byte and (on Linux)
forwards it to an ``eventfd``; the asyncio loop's reader notices
the readable eventfd and wakes. No SQLite, no subscriber, no
broadcast frame -- pure doorbell mechanism.

Scope and purpose
-----------------
This is the smallest bench in the suite. Its **value is
regression-gating**: a 10x spike in p50 means the kernel or a
dependency change broke the doorbell hot path, and that regression
would otherwise hide behind the SQLite + broadcast costs that
dominate the higher-level TTFAE benches. **NOT a headline number**;
operator-facing documentation does not cite it.

What is measured
----------------
- t=0: :func:`time.monotonic_ns` immediately before ``_doorbell.ring()``.
- t=end: :func:`time.monotonic_ns` immediately after :func:`select.select`
  returns with the doorbell fd readable.
- Recorded: ``t_end - t0``.

On Linux the doorbell fd is an ``eventfd`` driven by a
daemon-managed accept-thread (we recreate that thread here); on
macOS the doorbell fd is the listener socket itself and
``accept_one()`` runs inline from the readable callback. The bench
faithfully reconstructs the daemon's hot path on whichever
platform it runs on.

Sample posture
--------------
N=5000 + 500 warmup at 1 kHz scheduler rate. Wall-clock ~5.5 s per
phase x 2 phases (gc-enabled + gc-disabled) ~= 11 s total.
``--smoke`` runs N=100 + 10 warmup.

The harness's open-loop scheduler is used even for this micro-bench
so that an iteration that takes longer than the inter-iteration
interval (e.g. a transient page-cache miss) does NOT delay the next
sample's ``t_intended`` -- this is the same CO-aware discipline the
TTFAE benches use.

Invocation
----------
::

    # Smoke (~0.5 seconds)
    uv run python -m benchmarks.bench_notify_to_wake --smoke

    # Production baseline (taskset, ~12 s)
    taskset -c 2,3 uv run python -m benchmarks.bench_notify_to_wake \\
        --output benchmarks/baselines/notify_to_wake.json

    # PR regression gate (CI uses this)
    uv run python -m benchmarks.bench_notify_to_wake --check-regression
"""

from __future__ import annotations

import argparse
import contextlib
import os
import select
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Final

from waitbus import _doorbell

from ._harness import (
    HdrRecorder,
    OpenLoopScheduler,
    check_regression,
    collect_result,
    environment_report,
    gc_disabled,
    resolve_output_path,
    write_result,
)

_BENCH_NAME = "notify_to_wake"
_DEFAULT_N = 5000
_DEFAULT_WARMUP = 500
_DEFAULT_RATE_HZ = 1000.0
_SMOKE_N = 100
_SMOKE_WARMUP = 10
_BASELINE_PATH = Path(__file__).resolve().parent / "baselines" / f"{_BENCH_NAME}.json"
_RESULTS_DIR = Path(__file__).resolve().parent / "results"

_LISTENER_SELECT_TIMEOUT_SEC: Final[float] = 0.1
"""Timeout for the ``select.select`` call in the Linux accept-loop
background thread (seconds).

0.1 s keeps the accept-thread responsive to ``stop_flag`` without
spinning; the thread checks ``stop_flag.is_set()`` after each
timeout. On macOS the accept-loop is not used; the main bench loop
calls ``accept_one()`` inline.
"""


@contextlib.contextmanager
def _doorbell_context(tmp_dir: Path) -> Any:
    """Stand up a Doorbell against ``tmp_dir`` with an accept-thread.

    Mirrors the daemon-side hot path:
    - Linux: a background accept-thread pulls bytes off the listener
      socket and forwards them into the eventfd, so ``select`` on
      ``Doorbell.fd`` (the eventfd) wakes when at least one ring is
      pending.
    - macOS: ``Doorbell.fd`` IS the listener socket, so ``select``
      on it wakes when a writer connects; the bench's main loop
      calls ``accept_one()`` inline after the wake to consume the
      byte (same shape as the daemon's macOS code path).

    Yields the live :class:`_doorbell.Doorbell` instance plus a
    ``platform`` string telling the bench which wake mechanism to use.
    The ``_doorbell.doorbell_socket`` module-level binding is
    patched so :func:`_doorbell.ring` finds the bench's socket;
    full restoration on exit.
    """
    ds_path = tmp_dir / "doorbell.sock"
    saved = _doorbell.doorbell_socket  # type: ignore[attr-defined]
    _doorbell.doorbell_socket = lambda: ds_path  # type: ignore[attr-defined]
    doorbell = _doorbell.Doorbell.open(ds_path)

    stop_flag = threading.Event()
    accept_thread: threading.Thread | None = None

    if sys.platform == "linux":

        def _accept_loop() -> None:
            """Linux accept-thread: pull bytes off listener, write to eventfd."""
            listener_fd = doorbell.listener_fd
            while not stop_flag.is_set():
                readable, _, _ = select.select([listener_fd], [], [], _LISTENER_SELECT_TIMEOUT_SEC)
                if readable:
                    doorbell.accept_one()

        accept_thread = threading.Thread(target=_accept_loop, name="bench-doorbell-accept", daemon=True)
        accept_thread.start()

    try:
        yield doorbell, sys.platform
    finally:
        stop_flag.set()
        if accept_thread is not None:
            accept_thread.join(timeout=2.0)
        doorbell.close()
        _doorbell.doorbell_socket = saved  # type: ignore[attr-defined]


def _run_loop(
    *,
    doorbell: _doorbell.Doorbell,
    platform: str,
    n: int,
    warmup: int,
    rate_hz: float,
    hdr: HdrRecorder,
) -> None:
    """Run one bench loop. Records into ``hdr`` after warmup discard.

    Per iteration:

    1. Sleep until the open-loop scheduler's next ``t_intended``.
    2. Capture ``t0 = time.monotonic_ns()``.
    3. ``_doorbell.ring()`` -- writes one byte to the listener socket.
    4. ``select.select([fd], [], [])`` -- block on the doorbell fd
       becoming readable. On Linux this is the eventfd (fed by the
       accept-thread); on macOS this is the listener fd itself.
    5. Capture ``t_end = time.monotonic_ns()``.
    6. Drain the wake state (macOS: ``accept_one`` to read the byte;
       both platforms: ``doorbell.drain()`` to clear the counter).
    7. If ``i >= warmup``: ``hdr.record(t_end - t0)``.

    All timing is :func:`time.monotonic_ns` -- intra-process, immune
    to NTP adjustments, and the appropriate clock for sub-microsecond
    measurements.
    """
    sched = OpenLoopScheduler(rate_hz=rate_hz, n=n + warmup)
    fd = doorbell.fd

    for i, t_intended_ns in enumerate(sched):
        now_ns = time.monotonic_ns()
        if now_ns < t_intended_ns:
            time.sleep((t_intended_ns - now_ns) / 1e9)

        t0 = time.monotonic_ns()
        _doorbell.ring()
        # The doorbell fd becomes readable either via:
        #   * eventfd_write from the accept-thread (Linux)
        #   * connection arrival on the listener (macOS)
        # In both cases select() unblocks once the wake is observable.
        select.select([fd], [], [])
        t_end = time.monotonic_ns()

        if platform == "darwin":
            # macOS: select returned with the listener fd readable;
            # consume the byte inline as the daemon's macOS path does.
            doorbell.accept_one()
        # On both platforms, drain the wake state so the next
        # iteration's select doesn't return immediately on stale
        # state.
        doorbell.drain()

        if i >= warmup:
            hdr.record(t_end - t0)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Notify-to-wake micro-bench (regression-gate only).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--n",
        type=int,
        default=_DEFAULT_N,
        help="number of measurement samples (default: 5000)",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=_DEFAULT_WARMUP,
        help="number of leading samples to discard (default: 500)",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=_DEFAULT_RATE_HZ,
        help="open-loop rate in Hz (default: 1000.0)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="path to write the result JSON (default: benchmarks/results/notify_to_wake_<host>_<ts>.json)",
    )
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="quick run: N=100, warmup=10, no regression check.",
    )
    parser.add_argument(
        "--check-regression",
        action="store_true",
        help=(
            f"after the run, compare p99 (gc-enabled) against "
            f"{_BASELINE_PATH.relative_to(_BASELINE_PATH.parent.parent)}; exit non-zero on >25% regression."
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
    rate_hz: float = args.rate

    env = environment_report()
    print(f"[{_BENCH_NAME}] n={n} warmup={warmup} rate={rate_hz} Hz", file=sys.stderr)

    started_at_ns = time.time_ns()
    hdr_main = HdrRecorder()
    hdr_gc_off: HdrRecorder | None = None if args.no_gc_off else HdrRecorder()

    with tempfile.TemporaryDirectory(prefix="waitbus-bench-doorbell-") as tmp_str:
        tmp_dir = Path(tmp_str)
        with _doorbell_context(tmp_dir) as (doorbell, platform):
            print(f"[{_BENCH_NAME}] gc-on", file=sys.stderr)
            _run_loop(
                doorbell=doorbell,
                platform=platform,
                n=n,
                warmup=warmup,
                rate_hz=rate_hz,
                hdr=hdr_main,
            )

            if hdr_gc_off is not None:
                print(f"[{_BENCH_NAME}] gc-off", file=sys.stderr)
                with gc_disabled():
                    _run_loop(
                        doorbell=doorbell,
                        platform=platform,
                        n=n,
                        warmup=warmup,
                        rate_hz=rate_hz,
                        hdr=hdr_gc_off,
                    )

    ended_at_ns = time.time_ns()

    result = collect_result(
        bench_name=_BENCH_NAME,
        started_at_ns=started_at_ns,
        ended_at_ns=ended_at_ns,
        n_warmup_discarded=warmup,
        rate_hz=rate_hz,
        hdr_main=hdr_main,
        hdr_gc_off=hdr_gc_off,
        environment=env,
        extra={"smoke": args.smoke, "doorbell_mechanism": "eventfd" if sys.platform == "linux" else "af_unix_listener"},
    )

    output_path = resolve_output_path(_BENCH_NAME, _RESULTS_DIR, args.output, env)

    write_result(result, output_path)
    print(f"[{_BENCH_NAME}] wrote {output_path}", file=sys.stderr)

    # Notify-to-wake is sub-microsecond on tuned Linux; format in
    # microseconds rather than milliseconds for readable output.
    p50 = result.percentiles_gc_enabled["p50"]
    p90 = result.percentiles_gc_enabled["p90"]
    p99 = result.percentiles_gc_enabled["p99"]
    print(
        f"[{_BENCH_NAME}] gc-enabled  p50={p50.value_ns / 1e3:8.2f} us  "
        f"p90={p90.value_ns / 1e3:8.2f} us  "
        f"p99={p99.value_ns / 1e3:8.2f} us  "
        f"(n={result.n_samples})",
        file=sys.stderr,
    )
    if result.percentiles_gc_disabled is not None:
        g50 = result.percentiles_gc_disabled["p50"]
        g99 = result.percentiles_gc_disabled["p99"]
        print(
            f"[{_BENCH_NAME}] gc-disabled p50={g50.value_ns / 1e3:8.2f} us  p99={g99.value_ns / 1e3:8.2f} us",
            file=sys.stderr,
        )

    if args.check_regression and not args.smoke:
        ok, msg = check_regression(result, _BASELINE_PATH)
        print(f"[{_BENCH_NAME}] regression-check: {msg}", file=sys.stderr)
        if not ok:
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())


# Silence unused-import for Linux-only ``os`` references in the
# (currently unused) Linux-specific reading path; future macOS-vs-Linux
# divergence will use this.
_ = os
