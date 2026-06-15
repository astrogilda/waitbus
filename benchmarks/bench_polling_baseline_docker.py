"""Polling-cycle latency for ``docker ps -a`` container-status polling.

Measures the wall-clock interval a polling agent would experience when
it detects that a Docker container has exited by calling
``docker ps -a --filter id=<id> --format {{.Status}}`` on a 1-second
interval.

This bench is the *counterfactual* to waitbus's Docker TTFAE: instead of
a push notification, the agent polls the Docker daemon at a fixed
interval and waits to observe ``Exited`` in the status string.

Measurement definition
----------------------
- ``t_inject_ns``: recorded immediately before ``docker run`` is
  launched.  The container runs ``sleep 1.5`` and exits after roughly
  1.5 s.  The inject time is the moment the workload begins; the state
  change is the container exit at ``t_inject_ns + ~1.5 s``.
- ``t_observe_ns``: recorded immediately after the polling loop
  observes ``"Exited"`` in the ``docker ps`` output.
- Latency = ``t_observe_ns - t_inject_ns``.

This is a *total wait time* (container runtime + polling-cycle
overshoot), not a pure polling-interval floor.  The p99 captures the
worst-case wait across both the 1.5 s container startup+exit time and
the polling-cycle alignment.

Docker availability gate
------------------------
The bench calls ``docker info`` at startup.  If the Docker daemon is
absent or down, the bench prints a clear message to stderr and exits
with code 0 (analogous to ``pytest.skip``).  CI runners have Docker by
default; local workstations without Docker see a clean skip.

Wall-clock note
---------------
Each iteration takes approximately 1.5 s (container run) + up to 1.0 s
(polling overshoot) = ~2-3 s per iteration.  N=500 + warmup 20 = ~520
iterations = roughly 18-25 minutes per phase x 2 phases.  This is
documented as a long-running bench.  For local iteration, reduce N via
``--n 50`` (~2 min).

Sample posture
--------------
N=500 + 20 warmup.  ``--smoke`` runs N=50 + 5 warmup.

Invocation
----------
::

    # Smoke (~3 minutes, N=50)
    uv run python -m benchmarks.bench_polling_baseline_docker --smoke

    # Production baseline (long-running; ~45 minutes total, two phases)
    taskset -c 2,3 uv run python -m benchmarks.bench_polling_baseline_docker \\
        --output benchmarks/baselines/polling_baseline_docker.json

    # Quick local iteration
    uv run python -m benchmarks.bench_polling_baseline_docker --n 50

    # Regression check in CI
    uv run python -m benchmarks.bench_polling_baseline_docker --check-regression
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path
from typing import Final

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

_BENCH_NAME = "polling_baseline_docker"
_DEFAULT_N = 500
_DEFAULT_WARMUP = 20
_SMOKE_N = 50
_SMOKE_WARMUP = 5
_POLL_INTERVAL_S = 1.0
_CONTAINER_SLEEP_S = 1.5
_BASELINE_PATH = Path(__file__).resolve().parent / "baselines" / f"{_BENCH_NAME}.json"
_RESULTS_DIR = Path(__file__).resolve().parent / "results"

_DOCKER_RUN_TIMEOUT_SEC: Final[int] = 10
"""Timeout for the ``docker run --rm -d alpine sh -c "sleep N"`` call
that starts the bench container (seconds).

10 s is generous for a pre-pulled alpine image; protects against a
hung Docker daemon without blocking the iteration loop.
"""

_DOCKER_WAIT_TIMEOUT_SEC: Final[int] = 30
"""Maximum time the ``_run_container`` call may block waiting for the
container id to be printed (seconds).

30 s is the broader outer guard; ``_DOCKER_RUN_TIMEOUT_SEC`` is the
tighter inner guard on the subprocess itself.
"""

_DOCKER_PS_TIMEOUT_SEC: Final[int] = 10
"""Timeout for each ``docker ps -a --filter id=<id>`` poll call
(seconds).

One poll round-trip to the local Docker daemon is typically <100 ms;
10 s caps pathological hangs without letting the poll loop spin
indefinitely.
"""

_DOCKER_RM_TIMEOUT_SEC: Final[int] = 10
"""Timeout for the best-effort ``docker rm -f <id>`` cleanup call at
the end of each iteration (seconds).

Failure here is non-fatal (the bench logs it and continues); the
timeout prevents a stuck rm from anchoring the next iteration.
"""

_PROGRESS_LOG_CADENCE: Final[int] = 50
"""Print a progress line to stderr every this many completed iterations.

50 keeps the log readable across the default N=500 run (10 lines per
phase) without flooding stderr on short smoke runs.
"""

_ESTIMATED_SEC_PER_ITER: Final[float] = 2.5
"""Rough wall-clock seconds per iteration used in the startup banner.

Each iteration takes ~1.5 s container sleep + up to 1 s polling
overshoot; 2.5 s is the mid-range estimate. The banner uses this to
compute the expected total minutes so operators can judge whether to
reduce ``--n`` for a local iteration.
"""


def _docker_available() -> bool:
    """Return True if ``docker info`` exits 0 (daemon is reachable)."""
    try:
        result = subprocess.run(
            ["docker", "info"],
            check=False,
            capture_output=True,
            timeout=_DOCKER_RUN_TIMEOUT_SEC,
        )
        return result.returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


def _run_container() -> str:
    """Launch a detached alpine container that sleeps for _CONTAINER_SLEEP_S.

    Returns the container ID (full 64-char hex string).
    """
    cmd = [
        "docker",
        "run",
        "--rm",
        "-d",
        "alpine",
        "sh",
        "-c",
        f"sleep {_CONTAINER_SLEEP_S}",
    ]
    out = subprocess.run(
        cmd,
        check=True,
        capture_output=True,
        text=True,
        timeout=_DOCKER_WAIT_TIMEOUT_SEC,
    )
    return out.stdout.strip()


def _poll_until_exited(container_id: str) -> int:
    """Poll ``docker ps -a`` every _POLL_INTERVAL_S until the container exits.

    Returns ``t_observe_ns`` (wall-clock at the moment the ``Exited``
    status is observed).
    """
    while True:
        time.sleep(_POLL_INTERVAL_S)
        result = subprocess.run(
            [
                "docker",
                "ps",
                "-a",
                "--filter",
                f"id={container_id}",
                "--format",
                "{{.Status}}",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=_DOCKER_PS_TIMEOUT_SEC,
        )
        t_observe_ns = time.time_ns()
        status = result.stdout.strip()
        if "Exited" in status:
            return t_observe_ns
        # Container still running or status line is empty (container
        # cleaned up before we polled -- treat as exited).
        if not status:
            return t_observe_ns


def _run_loop(
    *,
    n: int,
    warmup: int,
    hdr: HdrRecorder,
) -> None:
    """Run one bench loop and record into ``hdr`` after warmup discard."""
    n_total = n + warmup
    for i in range(n_total):
        container_id = _run_container()
        t_inject_ns = time.time_ns()

        t_observe_ns = _poll_until_exited(container_id)

        latency = t_observe_ns - t_inject_ns
        if i >= warmup:
            hdr.record(latency)

        # Best-effort cleanup: the container was started with --rm so
        # Docker removes it on exit.  If it is still present (e.g. the
        # daemon is slow), a cleanup rm keeps the next iteration clean.
        subprocess.run(
            ["docker", "rm", "-f", container_id],
            check=False,
            capture_output=True,
            timeout=_DOCKER_RM_TIMEOUT_SEC,
        )

        if (i + 1) % _PROGRESS_LOG_CADENCE == 0:
            print(
                f"[{_BENCH_NAME}]   iteration {i + 1}/{n_total} done",
                file=sys.stderr,
            )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Measure polling-cycle latency for docker ps container-status polling.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--n",
        type=int,
        default=_DEFAULT_N,
        help=(
            "number of measurement samples (default: 500). "
            "Each sample takes ~2-3 s (container runtime + polling). "
            "Reduce for local iteration (--n 50)."
        ),
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=_DEFAULT_WARMUP,
        help="number of leading samples to discard (default: 20)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help=("path to write the result JSON (default: benchmarks/results/polling_baseline_docker_<host>_<ts>.json)"),
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

    # Docker availability gate (CC-4).
    if not _docker_available():
        print(
            f"[{_BENCH_NAME}] docker daemon not reachable (docker info failed); skipping.",
            file=sys.stderr,
        )
        return 0

    n = _SMOKE_N if args.smoke else args.n
    warmup = _SMOKE_WARMUP if args.smoke else args.warmup

    env = environment_report()
    print(
        f"[{_BENCH_NAME}] n={n} warmup={warmup} poll_interval={_POLL_INTERVAL_S}s "
        f"container_sleep={_CONTAINER_SLEEP_S}s",
        file=sys.stderr,
    )
    print(
        f"[{_BENCH_NAME}] NOTE: each iteration ~2-3 s; "
        f"estimated wall-clock ~{((n + warmup) * _ESTIMATED_SEC_PER_ITER / 60):.0f} min per phase.",
        file=sys.stderr,
    )

    started_at_ns = time.time_ns()
    hdr_main = HdrRecorder()
    hdr_gc_off: HdrRecorder | None = None if args.no_gc_off else HdrRecorder()

    print(f"[{_BENCH_NAME}] gc-on", file=sys.stderr)
    _run_loop(n=n, warmup=warmup, hdr=hdr_main)

    if hdr_gc_off is not None:
        print(f"[{_BENCH_NAME}] gc-off", file=sys.stderr)
        with gc_disabled():
            _run_loop(n=n, warmup=warmup, hdr=hdr_gc_off)

    ended_at_ns = time.time_ns()

    result = collect_result(
        bench_name=_BENCH_NAME,
        started_at_ns=started_at_ns,
        ended_at_ns=ended_at_ns,
        n_warmup_discarded=warmup,
        rate_hz=0.0,  # not open-loop scheduled; wall-clock is container+poll-bound
        hdr_main=hdr_main,
        hdr_gc_off=hdr_gc_off,
        environment=env,
        extra={
            "smoke": args.smoke,
            "poll_interval_s": _POLL_INTERVAL_S,
            "container_sleep_s": _CONTAINER_SLEEP_S,
            "method": "docker ps -a --filter id=<id> --format {{.Status}}",
            "scope_note": (
                "polling counterfactual; no waitbus daemon involved; "
                "latency includes container runtime + polling-cycle overshoot"
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
