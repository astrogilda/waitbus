"""TTFAE for the fs source.

Measures the wall-clock interval from "fs source decides to emit"
(t=0) to subscriber ``recv()`` (t=end). The whole emit-to-recv hot
path is exercised: ``emit_batch`` -> SQLite insert + commit ->
doorbell ring -> broadcaster pickup -> SELECT -> AF_UNIX frame
send -> subscriber ``sync_read_frame``.

What this bench is and is NOT
-----------------------------
The waitbus fs source comprises two parts:

1. **watchdog detection** -- a background thread observing inotify
   (Linux) / kqueue (BSD/macOS) / FSEvents (macOS) events. When a
   file close-write fires (the canonical "save is complete"
   signal), the handler decides to emit.
2. **waitbus emit path** -- the handler calls
   :func:`waitbus._emit.emit_batch` with a
   ``source="fs"`` :class:`EventInsert`. The rest of the
   path is identical to every other source.

This bench measures **part 2 only.** Part 1's detection latency is
bounded below by the kernel's inotify delivery latency (typically
under 100us on Linux) and varies by filesystem and load; including
it would convolve waitbus's emit-path cost with a kernel-level
externality, which is the same reason ``bench_ttfae_github.py``
excludes the GitHub-to-listener network leg.

The per-source comparison matrix that cites this bench makes the
exclusion explicit, and notes that the watchdog-detection vs
``inotifywait`` comparison runs in
``bench_polling_baseline_fs_inotifywait.py``.

t=0 / t=end
-----------
- t=0: :func:`time.time_ns` immediately before
  :func:`emit_batch` is called with a synthetic fs-shape
  :class:`EventInsert`. This is the moment the fs source decides
  to emit, just as the watchdog handler would.
- t=end: :func:`time.time_ns` immediately after
  :func:`sync_read_frame` returns the corresponding broadcast
  frame.

Per-iteration uniqueness
------------------------
``delivery_id`` is ``fs:bench-{i}`` to guarantee non-collision
across iterations (the natural ``path + mtime_ns`` shape isn't
needed here; the bench builds synthetic events, not real file
events).

Sample posture
--------------
N=5000 + 500 warmup at 100 Hz scheduler. Same as
``bench_ttfae_pytest.py``. Wall-clock approximately 1-2 minutes per
phase x 2 = 2-4 minutes total.

Invocation
----------
::

    # Smoke (~30 seconds)
    uv run python -m benchmarks.bench_ttfae_fs --smoke

    # Production baseline
    taskset -c 2,3 uv run python -m benchmarks.bench_ttfae_fs \\
        --output benchmarks/baselines/ttfae_fs.json
"""

from __future__ import annotations

import argparse
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import msgspec

from waitbus import _emit as emit_mod
from waitbus._broadcast_sub import open_subscriber
from waitbus._frame import sync_read_frame
from waitbus._types import EventInsert

from ._harness import (
    HdrRecorder,
    OpenLoopScheduler,
    check_regression,
    collect_result,
    daemon_context,
    environment_report,
    gc_disabled,
    print_percentile_summary,
    resolve_output_path,
    write_result,
)

_BENCH_NAME = "ttfae_fs"
_DEFAULT_N = 5000
_DEFAULT_WARMUP = 500
_DEFAULT_RATE_HZ = 100.0
_SMOKE_N = 100
_SMOKE_WARMUP = 10
_BASELINE_PATH = Path(__file__).resolve().parent / "baselines" / f"{_BENCH_NAME}.json"
_RESULTS_DIR = Path(__file__).resolve().parent / "results"


def _build_fs_event(i: int, *, watched_dir: Path) -> EventInsert:
    """Build one synthetic fs-shape EventInsert for iteration ``i``.

    Mirrors the shape :class:`waitbus.sources.fs_watch._Handler._emit`
    produces from a real close-write event: ``source="fs"``,
    ``event_type="fs_change"``, ``ingest_method="fs_watch"``,
    payload carries the path and the (synthetic) mtime_ns.

    delivery_id includes ``time.time_ns()`` so the natural key is
    unique across all phases of a single bench run. Using a plain
    ``f"fs:bench-{i}"`` would collide between gc-on run
    and gc-off run because ``i`` resets per phase; the
    SQLite ``INSERT OR IGNORE`` would then no-op the second-phase
    emits, suppress the doorbell ring, and hang ``sync_read_frame``
    indefinitely. The pytest bench escapes this trap because
    :class:`_Recorder` regenerates ``_session_id`` via
    ``time.time_ns()`` at every recorder construction; we apply the
    same pattern here at the ``EventInsert`` level.
    """
    path = watched_dir / f"bench_{i}.txt"
    now_ns = time.time_ns()
    payload: dict[str, Any] = {
        "path": str(path),
        "mtime_ns": now_ns,
        "event_kind": "closed",
    }
    return EventInsert(
        delivery_id=f"fs:bench-{i}-{now_ns}",
        source="fs",
        event_type="fs_change",
        owner="bench",
        repo="ttfae-fs",
        received_at=now_ns,
        payload_json=msgspec.json.encode(payload).decode(),
        ingest_method="fs_watch",
        status="completed",
        conclusion="success",
    )


def _run_loop(
    *,
    handle_db: Path,
    watched_dir: Path,
    sub_sock: Any,
    n: int,
    warmup: int,
    rate_hz: float,
    hdr: HdrRecorder,
) -> None:
    """Run one bench loop and record into ``hdr`` after warmup discard."""
    sched = OpenLoopScheduler(rate_hz=rate_hz, n=n + warmup)

    for i, t_intended_ns in enumerate(sched):
        now_ns = time.monotonic_ns()
        if now_ns < t_intended_ns:
            time.sleep((t_intended_ns - now_ns) / 1e9)

        event = _build_fs_event(i, watched_dir=watched_dir)
        t0 = time.time_ns()
        emit_mod.emit_batch([event], db_path=handle_db)
        frame = sync_read_frame(sub_sock)
        t_recv = time.time_ns()
        if frame is None:
            raise RuntimeError(f"iteration {i}: subscriber socket closed mid-bench (daemon died?)")
        if i >= warmup:
            hdr.record(t_recv - t0)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Measure TTFAE for the fs source.", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--n", type=int, default=_DEFAULT_N, help="number of measurement samples (default: 5000)")
    parser.add_argument(
        "--warmup", type=int, default=_DEFAULT_WARMUP, help="number of leading samples to discard (default: 500)"
    )
    parser.add_argument("--rate", type=float, default=_DEFAULT_RATE_HZ, help="open-loop rate in Hz (default: 100.0)")
    parser.add_argument("--output", type=Path, default=None, help="path to write the result JSON")
    parser.add_argument("--smoke", action="store_true", help="quick run: N=100, warmup=10, no regression check.")
    parser.add_argument(
        "--check-regression",
        action="store_true",
        help=(
            f"after the run, compare p99 (gc-enabled) against "
            f"{_BASELINE_PATH.relative_to(_BASELINE_PATH.parent.parent)}; exit non-zero on >25% regression."
        ),
    )
    parser.add_argument("--no-gc-off", action="store_true", help="skip the gc-disabled companion run.")
    args = parser.parse_args(argv)

    n = _SMOKE_N if args.smoke else args.n
    warmup = _SMOKE_WARMUP if args.smoke else args.warmup
    rate_hz: float = args.rate

    env = environment_report()
    print(f"[{_BENCH_NAME}] n={n} warmup={warmup} rate={rate_hz} Hz", file=sys.stderr)

    started_at_ns = time.time_ns()
    hdr_main = HdrRecorder()
    hdr_gc_off: HdrRecorder | None = None if args.no_gc_off else HdrRecorder()

    with tempfile.TemporaryDirectory(prefix="waitbus-bench-fs-") as tmp_str:
        tmp_dir = Path(tmp_str)
        watched_dir = tmp_dir / "watched"
        watched_dir.mkdir()
        with daemon_context(tmp_dir) as daemon:
            subscriber = open_subscriber(socket_path=str(daemon.broadcast_socket_path))
            try:
                print(f"[{_BENCH_NAME}] gc-on", file=sys.stderr)
                _run_loop(
                    handle_db=daemon.db_path,
                    watched_dir=watched_dir,
                    sub_sock=subscriber.sock,
                    n=n,
                    warmup=warmup,
                    rate_hz=rate_hz,
                    hdr=hdr_main,
                )
                if hdr_gc_off is not None:
                    print(f"[{_BENCH_NAME}] gc-off", file=sys.stderr)
                    with gc_disabled():
                        _run_loop(
                            handle_db=daemon.db_path,
                            watched_dir=watched_dir,
                            sub_sock=subscriber.sock,
                            n=n,
                            warmup=warmup,
                            rate_hz=rate_hz,
                            hdr=hdr_gc_off,
                        )
            finally:
                subscriber.sock.close()

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
        extra={
            "smoke": args.smoke,
            "scope_note": "measures emit-to-recv; excludes watchdog kernel detection latency",
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
