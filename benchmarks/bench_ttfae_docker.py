"""TTFAE for the docker source (synthetic-emit variant).

Measures the wall-clock interval from "emit decides to send a
docker-shape event" (t=0) to the moment a subscriber's ``recv()``
returns the corresponding broadcast frame (t=end). The whole
emit-to-recv hot path is exercised: ``emit_batch`` -> SQLite insert
+ commit -> doorbell ring -> broadcaster pickup -> SELECT -> AF_UNIX
frame send -> subscriber ``sync_read_frame``.

What this bench is and is NOT
-----------------------------
The waitbus docker source comprises two parts:

1. **Engine-event observation** -- a background thread streaming the
   Docker Engine ``GET /events`` HTTP/1.1 endpoint over
   ``/var/run/docker.sock``. When a container ``die``/``stop``/``kill``
   event arrives, ``docker_watch._build_event`` constructs an
   ``EventInsert`` with the engine's ``timeNano`` timestamp as
   ``received_at``.
2. **waitbus emit path** -- the watcher calls
   :func:`waitbus._emit.emit_batch` with the constructed
   ``EventInsert``. The rest of the path is identical to every other
   source.

This bench measures **part 2 only.** Part 1's observation latency is
bounded by Docker's engine-event delivery cadence (the engine writes
events asynchronously after container state transitions) and varies
by daemon load; including it would convolve waitbus's emit-path cost
with an external daemon externality. This is the same reason
``bench_ttfae_fs.py`` excludes watchdog inotify-event detection
latency.

The synthetic-emit approach also avoids the ~200 ms per-iteration
overhead of spawning ``docker run --rm alpine sh -c "exit 0"``
(N=5000 x 200 ms = 17 min/phase). A future ``bench_docker_watch_detection.py``
can measure the engine-event-stream-to-docker_watch observation latency
end-to-end; that bench would need the real Docker workload and would
use the ``timeNano`` field extracted from the subscriber frame's
payload to anchor t=0.

t=0 / t=end
-----------
- t=0: :func:`time.time_ns` immediately before
  :func:`emit_batch` is called with a synthetic docker-shape
  :class:`EventInsert`. This is the moment the docker source would
  decide to emit, just as ``docker_watch._build_event`` would.
- t=end: :func:`time.time_ns` immediately after
  :func:`sync_read_frame` returns the corresponding broadcast frame.

Docker daemon gate
------------------
At startup the bench runs ``docker info`` once. If the Docker daemon
is unreachable (``docker`` not installed, daemon stopped), the bench
exits 0 with a clear stderr message and does NOT count as a failure.
CI runners with Docker available execute the bench normally; macOS
dev workstations without Docker skip it cleanly.

Per-iteration uniqueness
------------------------
``delivery_id`` is ``docker:bench-{i}-{time_ns}`` so the natural key
is unique across both phases of a run. A plain ``docker:bench-{i}``
would collide between gc-on run and gc-off run
because ``i`` resets per phase; the SQLite ``INSERT OR IGNORE`` would
then no-op the second-phase emits, suppress the doorbell ring, and
hang ``sync_read_frame`` indefinitely. The ``time.time_ns()`` suffix
makes the key unique across phases without coupling uniqueness to
clock granularity.

Sample posture
--------------
Default N=500 + 50 warmup at 100 Hz open-loop scheduler. N=5000
gives Wilson Score 95% CI on p99 of roughly +/-0.27 percentile
points but would take ~8 min/phase; N=500 gives p99 ±~1pp CI, which
is sufficient for regression-gating. Use ``--n 5000`` for the
release-tag baseline capture.

Invocation
----------
::

    # Smoke (~10 seconds)
    uv run python -m benchmarks.bench_ttfae_docker --smoke

    # Production baseline
    taskset -c 2,3 uv run python -m benchmarks.bench_ttfae_docker \\
        --n 5000 --output benchmarks/baselines/ttfae_docker.json

    # Regression check in CI
    uv run python -m benchmarks.bench_ttfae_docker --check-regression
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Final

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

_BENCH_NAME = "ttfae_docker"
_DEFAULT_N = 500
_DEFAULT_WARMUP = 50
_DEFAULT_RATE_HZ = 100.0
_SMOKE_N = 100
_SMOKE_WARMUP = 10
_BASELINE_PATH = Path(__file__).resolve().parent / "baselines" / f"{_BENCH_NAME}.json"
_RESULTS_DIR = Path(__file__).resolve().parent / "results"

_DOCKER_CALL_TIMEOUT_SEC: Final[int] = 10
"""Timeout for the ``docker info`` availability probe (seconds).

10 s is a conservative upper bound for a round-trip to the local
Docker daemon; exceeding it indicates the daemon is unresponsive and
the bench should exit cleanly rather than hang.
"""


def _check_docker_available() -> bool:
    """Return True if the Docker daemon is reachable, False otherwise.

    Runs ``docker info`` once with a 10-second timeout. Exits cleanly
    (returns False) if the ``docker`` binary is absent, the daemon is
    stopped, or the process times out.
    """
    try:
        result = subprocess.run(
            ["docker", "info"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=_DOCKER_CALL_TIMEOUT_SEC,
        )
        return result.returncode == 0
    except FileNotFoundError:
        return False
    except subprocess.TimeoutExpired:
        return False


def _build_docker_event(i: int) -> EventInsert:
    """Build one synthetic docker-shape EventInsert for iteration ``i``.

    Mirrors the shape :func:`waitbus.sources.docker_watch._build_event`
    produces from a real container ``die`` event:
    ``source="docker"``, ``event_type="docker_container"``,
    ``ingest_method="docker_events"``, ``conclusion="success"`` (exit
    code 0), payload carrying a container-exit Engine message shape.

    ``delivery_id`` includes ``time.time_ns()`` for per-iteration
    uniqueness across both phases of a bench run (see module docstring).
    """
    now_ns = time.time_ns()
    container_id = f"bench{i:06d}a1b2c3d4e5f6"
    container_name = f"bench_container_{i}"
    payload: dict[str, Any] = {
        "Type": "container",
        "Action": "die",
        "Actor": {
            "ID": container_id,
            "Attributes": {
                "name": container_name,
                "image": "alpine",
                "exitCode": "0",
            },
        },
        "time": now_ns // 1_000_000_000,
        "timeNano": now_ns,
    }
    return EventInsert(
        delivery_id=f"docker:{container_id}:die:{now_ns}",
        source="docker",
        event_type="docker_container",
        owner="bench",
        repo="ttfae-docker",
        received_at=now_ns,
        payload_json=msgspec.json.encode(payload).decode(),
        ingest_method="docker_events",
        status="completed",
        conclusion="success",
        workflow_name=container_name,
    )


def _run_loop(
    *,
    handle_db: Path,
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

        event = _build_docker_event(i)
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
        description="Measure TTFAE for the docker source (synthetic-emit variant).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--n",
        type=int,
        default=_DEFAULT_N,
        help="number of measurement samples (default: 500; use 5000 for release-tag baseline)",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=_DEFAULT_WARMUP,
        help="number of leading samples to discard (default: 50)",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=_DEFAULT_RATE_HZ,
        help="open-loop rate in Hz (default: 100.0)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="path to write the result JSON",
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

    if not _check_docker_available():
        print(
            f"[{_BENCH_NAME}] docker daemon unreachable; skipping (this is a clean skip, not a failure)",
            file=sys.stderr,
        )
        return 0

    n = _SMOKE_N if args.smoke else args.n
    warmup = _SMOKE_WARMUP if args.smoke else args.warmup
    rate_hz: float = args.rate

    env = environment_report()
    print(f"[{_BENCH_NAME}] n={n} warmup={warmup} rate={rate_hz} Hz", file=sys.stderr)

    started_at_ns = time.time_ns()
    hdr_main = HdrRecorder()
    hdr_gc_off: HdrRecorder | None = None if args.no_gc_off else HdrRecorder()

    with tempfile.TemporaryDirectory(prefix="waitbus-bench-docker-") as tmp_str:
        tmp_dir = Path(tmp_str)
        with daemon_context(tmp_dir) as daemon:
            subscriber = open_subscriber(socket_path=str(daemon.broadcast_socket_path))
            try:
                print(f"[{_BENCH_NAME}] gc-on", file=sys.stderr)
                _run_loop(
                    handle_db=daemon.db_path,
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
            "scope_note": (
                "measures emit-to-recv for synthetic docker-shape events; "
                "excludes docker-watch engine-event-stream observation latency"
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
