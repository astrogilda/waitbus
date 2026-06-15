"""TTFAE for the pytest source.

Measures the wall-clock interval from ``pytest_sessionfinish`` hook
entry (t=0) to the moment a subscriber's ``recv()`` returns the
corresponding broadcast frame (t=end). The whole hot path is
exercised: ``_Recorder._build`` -> ``emit_batch`` -> SQLite insert +
commit -> doorbell ring -> broadcaster pickup -> SELECT -> AF_UNIX
frame send -> subscriber ``sync_read_frame``.

In-process bench rationale
------------------------------------
The pytest source has exactly one ingress event: the
``pytest_sessionfinish`` hook fires once at the end of a session, the
recorder builds one ``EventInsert`` per recorded test outcome, and
``emit_batch`` ships them all in one commit + one doorbell ring. The
TTFAE definition (per ``BENCHMARKING.md``) is "time from source
ingress to subscriber recv"; for pytest the ingress is the moment the
hook is entered, so t=0 is captured immediately before the recorder's
``pytest_sessionfinish`` call.

The bench is in-process: it constructs a real :class:`_Recorder`
instance with one fake test result and invokes its hook directly.
Spawning a pytest subprocess per iteration would add ~500 ms of
interpreter startup overhead per sample (a ~42-minute tax on N=5000)
without changing what is being measured -- the recorder code path is
identical whether the hook is fired by pytest's own session-finish
or by a direct synchronous call from the bench.

Sample-size discipline
----------------------
Default N=5000 + 500 warmup samples discarded. This gives the Wilson
Score 95% CI on p99 a half-width of roughly +/-0.3 percentile points
(see ``BENCHMARKING.md``'s sample-size table). ``--n`` overrides;
``--smoke`` runs N=100 + 10 warmup with no regression check, for
local iteration.

Output
------
JSON via :func:`benchmarks._harness.write_result` at
``benchmarks/results/ttfae_pytest_{host}_{ts}.json``. With
``--check-regression``, compared against
``benchmarks/baselines/ttfae_pytest.json``; >25% degradation on p99
of the gc-enabled run is a hard fail.

Invocation
----------
::

    # Smoke (~30 seconds)
    uv run python -m benchmarks.bench_ttfae_pytest --smoke

    # Production baseline (under taskset, ~3 minutes at 100 Hz)
    taskset -c 2,3 uv run python -m benchmarks.bench_ttfae_pytest \\
        --n 5000 --warmup 500 --rate 100 \\
        --output benchmarks/baselines/ttfae_pytest.json

    # Regression check in CI
    uv run python -m benchmarks.bench_ttfae_pytest --check-regression
"""

from __future__ import annotations

import argparse
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, cast

from waitbus._broadcast_sub import open_subscriber
from waitbus._frame import sync_read_frame
from waitbus.sources.pytest_emit import _Recorder

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

_BENCH_NAME = "ttfae_pytest"
_DEFAULT_N = 5000
_DEFAULT_WARMUP = 500
_DEFAULT_RATE_HZ = 100.0
_SMOKE_N = 100
_SMOKE_WARMUP = 10
_BASELINE_PATH = Path(__file__).resolve().parent / "baselines" / f"{_BENCH_NAME}.json"
_RESULTS_DIR = Path(__file__).resolve().parent / "results"


class _FakePytestConfig:
    """Minimal stand-in for :class:`pytest.Config`.

    The recorder only consults two surfaces on its config:
    ``hasattr(config, "workerinput")`` (xdist worker detection) and
    ``config.getoption("numprocesses", None)`` (xdist controller
    detection). This object satisfies both: no ``workerinput``
    attribute, ``getoption`` always returns the supplied default.
    The result is :meth:`_Recorder._is_xdist_controller` returns
    ``False`` and the emit proceeds.

    Using a concrete shim (not :func:`unittest.mock.MagicMock`)
    keeps the bench's call surface explicit and avoids the magic-mock
    pitfall where an unexpected attribute access silently succeeds
    and masks a real coupling change.
    """

    def getoption(self, name: str, default: Any = None) -> Any:
        """Return ``default`` for any option the recorder might query.

        The recorder asks for ``"numprocesses"`` only; any other
        access is returned with ``default`` too, defensively.
        """
        return default


def _run_loop(
    *,
    handle_db: Path,
    sub_sock: Any,
    n: int,
    warmup: int,
    rate_hz: float,
    hdr: HdrRecorder,
) -> None:
    """Run one bench loop and record into ``hdr``.

    Discards the first ``warmup`` samples. Runs N total iterations.
    The caller decides whether to invoke this inside a
    :func:`gc_disabled` block for the gc-off companion run.
    """
    fake_config = cast("Any", _FakePytestConfig())
    sched = OpenLoopScheduler(rate_hz=rate_hz, n=n + warmup)

    # The delivery_id natural key (per `_Recorder._build` at
    # ``pytest_emit.py:244-246``) is ``pytest:<session_id>:<nodeid>:<outcome>``.
    # SQLite's ``INSERT OR IGNORE`` against this UNIQUE key silently
    # no-ops a duplicate, which would block the subscriber's
    # ``sync_read_frame`` forever (the broadcaster doesn't ring on a
    # zero-row commit). Two protections are stacked:
    #   1. Recorder ``_session_id`` uses ``time.time_ns()``-pid -- at
    #      100 Hz iterations are 10 ms apart so collisions are
    #      vanishingly unlikely, but ``time.time_ns()`` granularity
    #      on some kernels is coarser than 1 ns and a future
    #      higher-rate variant (notify-to-wake at micro-second
    #      cadence) could hit duplicates.
    #   2. ``nodeid`` carries the iteration index, guaranteeing
    #      uniqueness independent of clock granularity. This is the
    #      load-bearing protection.

    for i, t_intended_ns in enumerate(sched):
        # Open-loop sleep: if behind schedule, do NOT shrink the
        # workload. Record the sample regardless; the tail reflects
        # reality.
        now_ns = time.monotonic_ns()
        if now_ns < t_intended_ns:
            time.sleep((t_intended_ns - now_ns) / 1e9)

        recorder = _Recorder(
            db_path=handle_db,
            owner="bench",
            repo="ttfae-pytest",
            config=fake_config,
        )
        # Reach in once to set the result list -- the recorder's
        # public surface is the pytest hook protocol; for a bench
        # this is the cleanest faithful invocation. Per-iteration
        # nodeid guarantees a unique delivery_id (see above).
        recorder._results = [(f"bench/test_{i}", "passed", 1_000_000)]

        t0 = time.time_ns()
        recorder.pytest_sessionfinish(exitstatus=0)
        frame = sync_read_frame(sub_sock)
        t_recv = time.time_ns()

        if frame is None:
            raise RuntimeError(f"iteration {i}: subscriber socket closed mid-bench (daemon died?)")

        if i >= warmup:
            hdr.record(t_recv - t0)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Measure TTFAE for the pytest source.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--n", type=int, default=_DEFAULT_N, help="number of measurement samples (default: 5000)")
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
        help="open-loop rate in Hz (default: 100.0)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help=("path to write the result JSON (default: benchmarks/results/ttfae_pytest_<host>_<ts>.json)"),
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
            "after the run, compare p99 (gc-enabled) against "
            f"{_BASELINE_PATH.relative_to(_BASELINE_PATH.parent.parent)}; "
            "exit non-zero on >25% regression."
        ),
    )
    parser.add_argument(
        "--no-gc-off",
        action="store_true",
        help=(
            "skip the gc-disabled companion run. NOT recommended for "
            "baselines; useful when iterating on the bench script itself."
        ),
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

    with tempfile.TemporaryDirectory(prefix="waitbus-bench-") as tmp_str:
        tmp_dir = Path(tmp_str)
        with daemon_context(tmp_dir) as daemon:
            subscriber = open_subscriber(socket_path=str(daemon.broadcast_socket_path))
            try:
                # GC-enabled run (representative of production).
                print(f"[{_BENCH_NAME}] gc-on", file=sys.stderr)
                _run_loop(
                    handle_db=daemon.db_path,
                    sub_sock=subscriber.sock,
                    n=n,
                    warmup=warmup,
                    rate_hz=rate_hz,
                    hdr=hdr_main,
                )

                # GC-disabled companion (algorithmic-cost figure).
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
        extra={"smoke": args.smoke},
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
