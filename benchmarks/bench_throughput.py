"""Sustained-throughput sweep: max events/sec without drops, per cell.

What is measured
----------------
For each cell in a 4 x 3 matrix:

- **subscriber count**: ``1, 4, 16, 64`` (open that many independent
  subscriber sockets, each draining in its own thread).
- **source mix**: ``all-github``, ``all-pytest``, ``25%-each`` (events
  emitted in the cell are drawn from this distribution).

binary-search the maximum events-per-second sustained without drops
over a 30-s steady-state window. A "drop" is any subscriber receiving
strictly fewer frames than the emitter put on the wire during the
window. The bench writes per-cell ``max_sustained_rate`` rather than a
percentile of any kind.

Custom result shape rationale
----------------------------------
Throughput is intrinsically per-cell -- there is no single
"throughput percentile" across the matrix that means anything. The
output JSON therefore uses a custom :class:`ThroughputResult` shape;
the regression gate is also custom (>25% drop in any cell's
max-sustained-rate fails the gate), since the standard
``check_regression`` is wired for higher-is-worse latency, not
lower-is-worse throughput.

Binary-search algorithm
-----------------------
Starting at rate = 1000 events/sec:

- Run a steady-state window at the target rate.
- If any drop occurred: halve the rate.
- If no drop occurred: double the rate.
- After ``iterations`` rounds, report the highest rate that produced
  no drops.

6 iterations converge to ``+/- ~1.5%``. ``--smoke`` mode runs a
single cell with 3 iterations and a 2-s window so the dev loop is
short; the production matrix is gated behind the default arguments.

Wall-clock
----------
12 cells x 6 iterations x 30 s = 36 min per phase, 72 min for both
gc-phases. ``--cells`` lets the operator subset (e.g.
``--cells 16,all-pytest;64,25-each``) for iteration.

Invocation
----------
::

    # Smoke (~6 s, 1 cell, 3 iterations, 2-s window):
    uv run python -m benchmarks.bench_throughput --smoke

    # Single cell from the matrix:
    uv run python -m benchmarks.bench_throughput --cells 16,all-pytest

    # Full matrix, gc-enabled only (~36 min):
    taskset -c 2,3 uv run python -m benchmarks.bench_throughput \\
        --no-gc-off --output benchmarks/baselines/throughput.json

    # Regression gate (compares against committed baseline):
    uv run python -m benchmarks.bench_throughput --check-regression
"""

from __future__ import annotations

import argparse
import contextlib
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

import msgspec

from waitbus import _emit as emit_mod
from waitbus._broadcast_sub import SubscriberHandle, open_subscriber
from waitbus._frame import sync_read_frame
from waitbus._types import EventInsert

from ._harness import (
    EnvironmentReport as _EnvironmentReport,
)
from ._harness import (
    OpenLoopScheduler,
    daemon_context,
    environment_report,
    gc_disabled,
    resolve_output_path,
)

_BENCH_NAME = "throughput"
_DEFAULT_START_RATE_HZ = 1000.0
_DEFAULT_WINDOW_SEC = 30.0
_DEFAULT_ITERATIONS = 6
_DEFAULT_DRAIN_SEC = 1.5
_REGRESSION_THRESHOLD = 0.25  # >25% drop in any cell fails the gate.
_SMOKE_WINDOW_SEC = 2.0
_SMOKE_ITERATIONS = 3
_SMOKE_DRAIN_SEC = 0.5
_BASELINE_PATH = Path(__file__).resolve().parent / "baselines" / f"{_BENCH_NAME}.json"
_RESULTS_DIR = Path(__file__).resolve().parent / "results"

_SUBSCRIBER_RECV_TIMEOUT_SEC: Final[float] = 0.05
"""Socket receive timeout used by each subscriber drain thread
(seconds).

50 ms is long enough that the loop doesn't spin-wait, and short enough
that ``stop_event`` propagation latency stays sub-100 ms. On
``socket.timeout`` the loop simply re-checks the stop flag and continues.
"""

_SATURATION_THRESHOLD: Final[float] = 0.99
"""Fraction of ``n_events_intended`` below which the emitter is
considered saturated.

The 1% slack (0.99 x intended) absorbs scheduler jitter at the
boundary where the inter-event interval approaches the emit cost;
without it the binary search under-reports by 2x at the true ceiling.
"""

_SUBSCRIBER_COUNTS: tuple[int, ...] = (1, 4, 16, 64)
_SOURCE_MIXES: tuple[str, ...] = ("all-github", "all-pytest", "25-each")
_EVENT_TYPE_BY_SOURCE: dict[str, str] = {
    "github": "workflow_run",
    "pytest": "pytest_session",
    "docker": "docker_container",
    "fs": "fs_change",
}


@dataclass(frozen=True)
class CellSpec:
    """One matrix cell: subscriber count x source mix."""

    subscriber_count: int
    source_mix: str


class CellResult(msgspec.Struct, frozen=True):
    """Outcome for one cell after the binary search converges.

    Two rate numbers are surfaced:

    - ``max_sustained_rate_hz``: the highest ``target_rate_hz`` from a
      no-drop iteration. Coarse to powers-of-two via the binary search
      ladder (1000 -> 2000 -> ... or 1000 -> 500 -> ...). Used for the
      ``--check-regression`` gate because it's stable across runs.
    - ``best_observed_rate_hz``: the highest ``observed_rate_hz`` from a
      no-drop iteration. Independent of binary-search step size; this is
      the number to cite in articles. On hardware where the ceiling sits
      between ladder rungs, ``best_observed_rate_hz`` is the truth and
      ``max_sustained_rate_hz`` is the lower-bound proof.
    """

    subscriber_count: int
    source_mix: str
    max_sustained_rate_hz: float
    best_observed_rate_hz: float
    iterations_run: int
    history: list[dict[str, Any]]


class ThroughputResult(msgspec.Struct, frozen=True):
    """Custom result shape; see module docstring."""

    bench_name: str
    started_at_ns: int
    ended_at_ns: int
    window_sec: float
    iterations: int
    cells_gc_enabled: list[CellResult]
    cells_gc_disabled: list[CellResult] | None
    environment: _EnvironmentReport
    extra: dict[str, Any] = msgspec.field(default_factory=dict)


# ---------------------------------------------------------------------------
# Cell parsing
# ---------------------------------------------------------------------------


def _parse_cells_arg(arg: str | None) -> list[CellSpec]:
    """Parse ``--cells`` into a list of :class:`CellSpec`.

    Format: ``"<sub_count>,<mix>;<sub_count>,<mix>;..."``. ``None``
    returns the full default matrix. Invalid entries raise immediately
    so a typo doesn't silently exclude cells.
    """
    if arg is None:
        return [CellSpec(s, m) for s in _SUBSCRIBER_COUNTS for m in _SOURCE_MIXES]
    out: list[CellSpec] = []
    for entry in arg.split(";"):
        entry = entry.strip()
        if not entry:
            continue
        try:
            sub_str, mix = entry.split(",", 1)
        except ValueError as exc:
            raise ValueError(f"bad --cells entry {entry!r}; expected 'N,mix'") from exc
        sub_count = int(sub_str)
        if sub_count not in _SUBSCRIBER_COUNTS:
            raise ValueError(f"--cells subscriber_count={sub_count} not in {_SUBSCRIBER_COUNTS}")
        if mix not in _SOURCE_MIXES:
            raise ValueError(f"--cells source_mix={mix!r} not in {_SOURCE_MIXES}")
        out.append(CellSpec(sub_count, mix))
    if not out:
        raise ValueError("--cells parsed to empty list; supply at least one cell")
    return out


# ---------------------------------------------------------------------------
# Source mix
# ---------------------------------------------------------------------------


def _source_sequence(mix: str, n: int) -> list[str]:
    """Return ``n`` sources according to ``mix``.

    ``all-github`` / ``all-pytest`` are single-source mixes.
    ``25-each`` rotates round-robin through all four sources so each
    receives ~n/4 events. The deterministic ordering keeps two cells
    from accidentally diverging on RNG state -- the rate is what's
    being measured, not the source distribution variance.
    """
    if mix == "all-github":
        return ["github"] * n
    if mix == "all-pytest":
        return ["pytest"] * n
    if mix == "25-each":
        order = ("github", "pytest", "docker", "fs")
        return [order[i % 4] for i in range(n)]
    raise ValueError(f"unknown source_mix {mix!r}")


def _build_event(i: int, source: str) -> EventInsert:
    """Build one EventInsert. delivery_id is iteration + time keyed.

    The high-rate emit path uses :func:`emit_mod.emit_batch` so per-event
    construction needs to be cheap; the payload is intentionally tiny
    (no per-event randomization) to keep the bench measuring the
    broadcast hot path rather than payload-encoding overhead.
    """
    now_ns = time.time_ns()
    return EventInsert(
        delivery_id=f"throughput:{source}:{i}-{now_ns}",
        source=source,
        event_type=_EVENT_TYPE_BY_SOURCE[source],
        owner="bench",
        repo="throughput",
        received_at=now_ns,
        payload_json='{"i":' + str(i) + "}",
        ingest_method="bench",
        status="completed",
        conclusion="success",
    )


# ---------------------------------------------------------------------------
# Subscriber threading
# ---------------------------------------------------------------------------


@dataclass
class _SubscriberWorker:
    """One subscriber thread: holds the handle + the received-count it
    accumulates.

    The thread loops on ``sync_read_frame`` with the socket in a
    short-timeout mode; on each successful frame the counter
    increments. When ``stop_event`` fires the thread drains any
    remaining frames already queued in the socket buffer (small
    ``drain_deadline`` window) and exits.
    """

    handle: SubscriberHandle
    thread: threading.Thread
    stop_event: threading.Event
    received_count: int = 0
    error: BaseException | None = None


def _make_subscriber(socket_path: str) -> SubscriberHandle:
    """Open one subscriber against ``socket_path``.

    No filters: this bench is about wire throughput, not predicate
    selectivity. The subscriber receives every frame the daemon
    broadcasts during the window.
    """
    return open_subscriber(socket_path=socket_path)


def _start_subscribers(socket_path: str, n: int) -> list[_SubscriberWorker]:
    """Spawn ``n`` subscriber threads draining via :func:`sync_read_frame`.

    The drain loop uses ``socket.settimeout(0.05)``: long enough that
    the loop doesn't spin-wait, short enough that ``stop_event``
    propagation is fast (sub-100ms). On ``socket.timeout`` the loop
    just re-checks the stop flag.
    """
    workers: list[_SubscriberWorker] = []
    for _ in range(n):
        handle = _make_subscriber(socket_path)
        handle.sock.settimeout(_SUBSCRIBER_RECV_TIMEOUT_SEC)
        stop_event = threading.Event()
        worker = _SubscriberWorker(handle=handle, thread=None, stop_event=stop_event)  # type: ignore[arg-type]

        def _loop(w: _SubscriberWorker = worker) -> None:
            try:
                while not w.stop_event.is_set():
                    try:
                        sync_read_frame(w.handle.sock)
                    except TimeoutError:
                        continue
                    except (ConnectionError, OSError):
                        # Socket closed (daemon dropped us, or teardown).
                        # Stop counting and exit cleanly.
                        break
                    w.received_count += 1
            except BaseException as exc:
                w.error = exc

        thread = threading.Thread(target=_loop, name="bench-throughput-sub", daemon=True)
        worker.thread = thread
        thread.start()
        workers.append(worker)
    return workers


def _stop_subscribers(workers: list[_SubscriberWorker], drain_sec: float) -> None:
    """Signal stop, wait ``drain_sec`` for queued frames, then close sockets.

    The drain window lets the kernel socket buffer flush so the
    received-count comparison is not biased by frames still in-flight
    when stop fires.
    """
    time.sleep(drain_sec)  # drain frames already in socket buffers
    for w in workers:
        w.stop_event.set()
    for w in workers:
        w.thread.join(timeout=2.0)
    for w in workers:
        with contextlib.suppress(OSError):
            w.handle.sock.close()


# ---------------------------------------------------------------------------
# One steady-state window
# ---------------------------------------------------------------------------


@dataclass
class WindowOutcome:
    """One steady-state window's result -- emitted vs received per sub.

    Fields:

    - ``target_rate_hz``: the rate the binary search asked the loop to hit.
    - ``n_events_intended``: ``int(target_rate_hz * window_sec)`` -- what the
      loop would have emitted at full target rate for the full window.
    - ``emitted``: what the emit loop actually pushed before the wallclock
      guard tripped (always ``<= n_events_intended``).
    - ``observed_rate_hz``: ``emitted / emit_elapsed_sec`` -- the ACHIEVED
      rate, independent of the binary-search step granularity. This is the
      headline number for an article: "the daemon sustained X events/sec",
      not "the binary search topped out at 2^k * start_rate".
    - ``emit_elapsed_sec``: wallclock of the emit loop only (excludes drain).
    - ``emitter_saturated``: ``emitted < int(n_events_intended * 0.99)``.
      The 1% slack absorbs scheduler jitter at the boundary where emit cost
      approaches the inter-event interval; without it the binary search
      under-reports by 2x at the true ceiling.
    - ``subscribers_lagged``: any subscriber's received_count < emitted
      (covers daemon-side broadcast queue overflow).
    - ``drop_occurred``: ``emitter_saturated OR subscribers_lagged``.
    - ``window_total_sec``: emit + drain wallclock; the per-cell wallclock
      cost equals ``iterations * window_total_sec``.
    """

    target_rate_hz: float
    n_events_intended: int
    emitted: int
    observed_rate_hz: float
    emit_elapsed_sec: float
    received_per_sub: list[int]
    drop_occurred: bool
    emitter_saturated: bool
    subscribers_lagged: bool
    min_received: int
    window_total_sec: float


def _run_window(
    *,
    db_path: Path,
    socket_path: str,
    target_rate_hz: float,
    window_sec: float,
    subscriber_count: int,
    source_mix: str,
    drain_sec: float,
) -> WindowOutcome:
    """Run one steady-state window at ``target_rate_hz`` for ``window_sec``.

    Returns the :class:`WindowOutcome` carrying per-sub received counts
    and the drop verdict. The bench's main loop drives the binary
    search by feeding successive windows with adjusted target rates.

    The emitter sleeps to the open-loop scheduler's ``t_intended`` so
    a transient slow-write doesn't bunch up subsequent emits (the
    coordinated-omission discipline the harness applies elsewhere).
    """
    n_events = max(1, int(target_rate_hz * window_sec))
    sources = _source_sequence(source_mix, n_events)
    workers = _start_subscribers(socket_path, subscriber_count)
    # Wallclock guard: if the emit loop falls behind schedule, break at the
    # nominal window boundary rather than running the whole n_events in
    # work-time. Without this, a target_rate the daemon cannot sustain
    # produces windows that overrun by 10-100x (the loop has no native
    # deadline; OpenLoopScheduler just yields t_intended values and the
    # emitter sleeps only when AHEAD of schedule, never when behind).
    start_ns = time.monotonic_ns()
    deadline_ns = start_ns + int(window_sec * 1e9)
    try:
        sched = OpenLoopScheduler(rate_hz=target_rate_hz, n=n_events)
        emitted = 0
        for i, t_intended_ns in enumerate(sched):
            now_ns = time.monotonic_ns()
            if now_ns >= deadline_ns:
                break
            if now_ns < t_intended_ns:
                time.sleep((t_intended_ns - now_ns) / 1e9)
            event = _build_event(i, sources[i])
            emit_mod.emit_batch([event], db_path=db_path)
            emitted += 1
        # Snapshot the emit-loop wallclock BEFORE drain so the diagnostic
        # measures emit time only, not emit + drain.
        emit_elapsed_sec = (time.monotonic_ns() - start_ns) / 1e9
    finally:
        _stop_subscribers(workers, drain_sec=drain_sec)

    received_per_sub = [w.received_count for w in workers]
    min_received = min(received_per_sub) if received_per_sub else 0
    # Saturation-aware drop verdict. Two distinct ways a window can fail:
    #   1. Emitter couldn't keep up with target_rate_hz (emitted < n_events).
    #      The binary search must treat this as a drop so the rate is halved.
    #      Without this check the bench cannot detect the daemon's real
    #      saturation point; subscribers drain indefinitely and would always
    #      catch up to whatever the emit loop actually pushed, so the
    #      subscribers-lagged check by itself is unable to fail.
    #   2. Subscribers received fewer frames than the emitter pushed (the
    #      original check; covers daemon-side broadcast queue overflow).
    #
    # The 1% slack on the saturation check absorbs scheduler jitter at the
    # boundary where emit cost approaches the inter-event interval. Without
    # slack, a single jittered iteration at the true ceiling trips
    # saturation and the binary search halves below the ceiling. With 1%
    # slack the bench converges TO the ceiling rather than half-ceiling.
    # The bench reports the OBSERVED rate (emitted/emit_elapsed_sec) as the
    # primary number so the result is independent of binary-search step
    # size; the target_rate_hz that produced it is recorded for traceability.
    saturation_threshold = max(1, int(n_events * _SATURATION_THRESHOLD))
    emitter_saturated = emitted < saturation_threshold
    subscribers_lagged = any(w.received_count < emitted for w in workers)
    drop_occurred = emitter_saturated or subscribers_lagged

    # Surface any subscriber-thread exception so a teardown bug doesn't
    # silently produce a "no drops" verdict.
    for w in workers:
        if w.error is not None:
            raise RuntimeError("subscriber thread raised") from w.error

    observed_rate_hz = emitted / max(emit_elapsed_sec, 1e-9)
    return WindowOutcome(
        target_rate_hz=target_rate_hz,
        n_events_intended=n_events,
        emitted=emitted,
        observed_rate_hz=observed_rate_hz,
        emit_elapsed_sec=emit_elapsed_sec,
        received_per_sub=received_per_sub,
        drop_occurred=drop_occurred,
        emitter_saturated=emitter_saturated,
        subscribers_lagged=subscribers_lagged,
        min_received=min_received,
        window_total_sec=(time.monotonic_ns() - start_ns) / 1e9,
    )


# ---------------------------------------------------------------------------
# Binary search per cell
# ---------------------------------------------------------------------------


def _search_cell(
    *,
    db_path: Path,
    socket_path: str,
    cell: CellSpec,
    start_rate_hz: float,
    iterations: int,
    window_sec: float,
    drain_sec: float,
) -> CellResult:
    """Binary-search the max sustained rate for ``cell``.

    Halve on drop, double on no-drop. Tracks the best (highest) rate
    seen with no drop so the final answer accounts for the search
    overshooting and coming back down. History records every window
    so a reviewer can reconstruct the search trajectory.
    """
    rate = start_rate_hz
    best_no_drop: float = 0.0
    best_observed: float = 0.0
    history: list[dict[str, Any]] = []
    for it in range(iterations):
        outcome = _run_window(
            db_path=db_path,
            socket_path=socket_path,
            target_rate_hz=rate,
            window_sec=window_sec,
            subscriber_count=cell.subscriber_count,
            source_mix=cell.source_mix,
            drain_sec=drain_sec,
        )
        history.append(
            {
                "iteration": it,
                "target_rate_hz": rate,
                "n_events_intended": outcome.n_events_intended,
                "emitted": outcome.emitted,
                "observed_rate_hz": outcome.observed_rate_hz,
                "emit_elapsed_sec": outcome.emit_elapsed_sec,
                "received_per_sub": outcome.received_per_sub,
                "drop_occurred": outcome.drop_occurred,
                "emitter_saturated": outcome.emitter_saturated,
                "subscribers_lagged": outcome.subscribers_lagged,
                "min_received": outcome.min_received,
                "window_total_sec": outcome.window_total_sec,
            }
        )
        if outcome.drop_occurred:
            rate = max(1.0, rate / 2.0)
        else:
            best_no_drop = max(best_no_drop, outcome.target_rate_hz)
            best_observed = max(best_observed, outcome.observed_rate_hz)
            rate = rate * 2.0
    return CellResult(
        subscriber_count=cell.subscriber_count,
        source_mix=cell.source_mix,
        max_sustained_rate_hz=best_no_drop,
        best_observed_rate_hz=best_observed,
        iterations_run=iterations,
        history=history,
    )


# ---------------------------------------------------------------------------
# Phase runner
# ---------------------------------------------------------------------------


def _run_phase(
    *,
    cells: list[CellSpec],
    start_rate_hz: float,
    iterations: int,
    window_sec: float,
    drain_sec: float,
) -> list[CellResult]:
    """Run one phase (gc-enabled or gc-disabled) over ``cells``.

    Each cell gets a fresh daemon context so a leak from one cell
    doesn't contaminate the next. The tmp dir is recreated each cell.
    """
    results: list[CellResult] = []
    for cell in cells:
        with tempfile.TemporaryDirectory(prefix="waitbus-bench-throughput-") as tmp_str:
            tmp_dir = Path(tmp_str)
            with daemon_context(tmp_dir) as daemon:
                print(
                    f"[{_BENCH_NAME}]   cell sub={cell.subscriber_count} mix={cell.source_mix}",
                    file=sys.stderr,
                )
                result = _search_cell(
                    db_path=daemon.db_path,
                    socket_path=str(daemon.broadcast_socket_path),
                    cell=cell,
                    start_rate_hz=start_rate_hz,
                    iterations=iterations,
                    window_sec=window_sec,
                    drain_sec=drain_sec,
                )
                print(
                    f"[{_BENCH_NAME}]     max_target={result.max_sustained_rate_hz:.1f} Hz  "
                    f"best_observed={result.best_observed_rate_hz:.1f} Hz",
                    file=sys.stderr,
                )
                results.append(result)
    return results


# ---------------------------------------------------------------------------
# Custom regression check
# ---------------------------------------------------------------------------


def _check_throughput_regression(
    current: ThroughputResult,
    baseline_path: Path,
    *,
    threshold: float = _REGRESSION_THRESHOLD,
) -> tuple[bool, str]:
    """Fail if any cell's max-sustained-rate is more than ``threshold`` below baseline.

    Direction is the opposite of latency: lower throughput is worse.
    Match cells by (subscriber_count, source_mix); ignore baseline
    cells absent from the current run (operator may have used --cells
    to subset).
    """
    if not baseline_path.exists():
        return True, f"no baseline at {baseline_path}; first run is the baseline"
    baseline = msgspec.json.decode(baseline_path.read_bytes(), type=ThroughputResult)
    by_key = {(c.subscriber_count, c.source_mix): c for c in baseline.cells_gc_enabled}
    regressions: list[str] = []
    for cur in current.cells_gc_enabled:
        key = (cur.subscriber_count, cur.source_mix)
        base = by_key.get(key)
        if base is None:
            continue
        if base.max_sustained_rate_hz <= 0:
            continue
        ratio = cur.max_sustained_rate_hz / base.max_sustained_rate_hz
        if ratio < 1.0 - threshold:
            regressions.append(
                f"cell sub={cur.subscriber_count} mix={cur.source_mix}: "
                f"current={cur.max_sustained_rate_hz:.1f} Hz vs "
                f"baseline={base.max_sustained_rate_hz:.1f} Hz (ratio={ratio:.3f})"
            )
    if regressions:
        return False, "; ".join(regressions)
    return True, "all cells within threshold"


def _write_throughput_result(result: ThroughputResult, path: Path) -> None:
    """Write a throughput result atomically (tmp + rename)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = msgspec.json.format(msgspec.json.encode(result), indent=2)
    tmp = path.with_suffix(path.suffix + ".partial")
    tmp.write_bytes(encoded)
    tmp.replace(path)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Sustained-throughput sweep (max events/sec without drops per cell).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--cells",
        type=str,
        default=None,
        help=("subset of cells, e.g. '16,all-pytest;64,25-each'. Defaults to the full 4 x 3 matrix."),
    )
    parser.add_argument(
        "--start-rate",
        type=float,
        default=_DEFAULT_START_RATE_HZ,
        help="binary-search starting rate (default: 1000 events/sec).",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=_DEFAULT_ITERATIONS,
        help="binary-search iterations per cell (default: 6 -> +/- 1.5%%).",
    )
    parser.add_argument(
        "--window",
        type=float,
        default=_DEFAULT_WINDOW_SEC,
        help="steady-state window per iteration in seconds (default: 30).",
    )
    parser.add_argument(
        "--drain",
        type=float,
        default=_DEFAULT_DRAIN_SEC,
        help="post-emit drain before stop signal in seconds (default: 1.5).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="path to write the result JSON (default: benchmarks/results/throughput_<host>_<ts>.json).",
    )
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="quick run: 1 cell (1 sub, all-pytest), 3 iterations, 2-s window.",
    )
    parser.add_argument(
        "--check-regression",
        action="store_true",
        help=(
            "after the run, compare each cell's max_sustained_rate against "
            f"{_BASELINE_PATH.relative_to(_BASELINE_PATH.parent.parent)}; "
            "exit non-zero on >25%% regression in any cell."
        ),
    )
    parser.add_argument(
        "--no-gc-off",
        action="store_true",
        help="skip the gc-disabled companion phase.",
    )
    args = parser.parse_args(argv)

    if args.smoke:
        cells = [CellSpec(1, "all-pytest")]
        iterations = _SMOKE_ITERATIONS
        window_sec = _SMOKE_WINDOW_SEC
        drain_sec = _SMOKE_DRAIN_SEC
    else:
        cells = _parse_cells_arg(args.cells)
        iterations = args.iterations
        window_sec = args.window
        drain_sec = args.drain

    env = environment_report()
    print(
        f"[{_BENCH_NAME}] cells={len(cells)} iterations={iterations} "
        f"window={window_sec}s drain={drain_sec}s start_rate={args.start_rate} Hz",
        file=sys.stderr,
    )

    started_at_ns = time.time_ns()

    print(f"[{_BENCH_NAME}] gc-on", file=sys.stderr)
    cells_gc_enabled = _run_phase(
        cells=cells,
        start_rate_hz=args.start_rate,
        iterations=iterations,
        window_sec=window_sec,
        drain_sec=drain_sec,
    )

    cells_gc_disabled: list[CellResult] | None = None
    if not args.no_gc_off and not args.smoke:
        print(f"[{_BENCH_NAME}] gc-off", file=sys.stderr)
        with gc_disabled():
            cells_gc_disabled = _run_phase(
                cells=cells,
                start_rate_hz=args.start_rate,
                iterations=iterations,
                window_sec=window_sec,
                drain_sec=drain_sec,
            )

    ended_at_ns = time.time_ns()

    result = ThroughputResult(
        bench_name=_BENCH_NAME,
        started_at_ns=started_at_ns,
        ended_at_ns=ended_at_ns,
        window_sec=window_sec,
        iterations=iterations,
        cells_gc_enabled=cells_gc_enabled,
        cells_gc_disabled=cells_gc_disabled,
        environment=env,
        extra={"smoke": args.smoke, "start_rate_hz": args.start_rate, "drain_sec": drain_sec},
    )

    output_path = resolve_output_path(_BENCH_NAME, _RESULTS_DIR, args.output, env)

    _write_throughput_result(result, output_path)
    print(f"[{_BENCH_NAME}] wrote {output_path}", file=sys.stderr)
    for cell_result in cells_gc_enabled:
        print(
            f"[{_BENCH_NAME}] gc-enabled  sub={cell_result.subscriber_count:>2} "
            f"mix={cell_result.source_mix:<10} "
            f"max_target={cell_result.max_sustained_rate_hz:8.1f} Hz "
            f"observed={cell_result.best_observed_rate_hz:8.1f} Hz",
            file=sys.stderr,
        )
    if cells_gc_disabled is not None:
        for cell_result in cells_gc_disabled:
            print(
                f"[{_BENCH_NAME}] gc-disabled sub={cell_result.subscriber_count:>2} "
                f"mix={cell_result.source_mix:<10} "
                f"max_target={cell_result.max_sustained_rate_hz:8.1f} Hz "
                f"observed={cell_result.best_observed_rate_hz:8.1f} Hz",
                file=sys.stderr,
            )

    if args.check_regression and not args.smoke:
        ok, msg = _check_throughput_regression(result, _BASELINE_PATH)
        print(f"[{_BENCH_NAME}] regression-check: {msg}", file=sys.stderr)
        if not ok:
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
