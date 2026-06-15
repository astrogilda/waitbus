"""TTFAE-first-match: per-source latency via the predicate-wait engine.

Measures source-ingress -> ``await_predicate`` return for a
single-source predicate, attributed by which source matched. The
plan's spec is N=5000 disjoint wait calls split ~1250 per source
across {github, pytest, docker, fs}; the result JSON carries both
the aggregate p50/p90/p99 and a per-source breakdown.

Predicate-engine peer bench
--------------
The four per-source TTFAE benches (``bench_ttfae_{github,pytest,
docker,fs}.py``) measure source-ingress -> ``sync_read_frame``.
This bench is the predicate-engine peer: same emit-to-recv path
but with the production ``await_predicate`` consuming the frame
and applying a predicate. The expected delta vs the raw
``sync_read_frame`` benches is the predicate engine's per-frame
cost, already measured at ~600 ns by
``bench_predicate_eval_latency_multi``. If this bench reads
materially HIGHER than the raw benches by anything more than that
delta, the predicate engine has acquired hidden overhead that
``bench_predicate_eval_latency_multi`` does not catch (e.g.
allocation per match, log per match, lock per match).

Per-source attribution
----------------------
Each iteration rotates the source it emits and the corresponding
single-source predicate -- ``source="github"``, then
``source="pytest"``, etc. Sample size: ~1250 per source at the
default N=5000. The result's ``extra`` field carries a per-source
``percentiles_gc_enabled`` block alongside the aggregate.

Same delivery_id-collision protection as ``bench_ttfae_fs``:
delivery_id includes ``time.time_ns()`` so the gc-on and gc-off runs do
not collide.

Same heartbeat-leak protection as every other broadcast-using
bench: ``daemon_context`` sets ``WAITBUS_HEARTBEAT_SEC=3600``
so no heartbeat frame can be returned by ``await_predicate``
before the matching event arrives.

Invocation
----------
::

    # Smoke (~30 seconds)
    uv run python -m benchmarks.bench_ttfae_first_match --smoke

    # Production baseline
    taskset -c 2,3 uv run python -m benchmarks.bench_ttfae_first_match \\
        --output benchmarks/baselines/ttfae_first_match.json
"""

from __future__ import annotations

import argparse
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Final

import msgspec

from waitbus import _emit as emit_mod
from waitbus._broadcast_sub import (
    FrameDecision,
    SubscriberHandle,
    await_predicate,
    open_subscriber,
)
from waitbus._frame import sync_read_frame
from waitbus._predicate import parse_match
from waitbus._types import EventInsert

from ._harness import (
    HdrRecorder,
    OpenLoopScheduler,
    check_regression,
    collect_result,
    daemon_context,
    environment_report,
    gc_disabled,
    resolve_output_path,
    write_result,
)

_BENCH_NAME = "ttfae_first_match"
_DEFAULT_N = 5000
_DEFAULT_WARMUP = 500
_DEFAULT_RATE_HZ = 100.0
_SMOKE_N = 100
_SMOKE_WARMUP = 10
_BASELINE_PATH = Path(__file__).resolve().parent / "baselines" / f"{_BENCH_NAME}.json"
_RESULTS_DIR = Path(__file__).resolve().parent / "results"

_DEADLINE_SECONDS: Final[float] = 5.0
"""Per-iteration deadline passed to ``await_predicate`` (seconds).

5 s is well above the expected emit-to-match latency (sub-10 ms at
100 Hz) but short enough that a missed iteration surfaces within the
bench run rather than blocking indefinitely.
"""

_SUBSCRIBER_SOCK_TIMEOUT_SEC: Final[float] = 5.0
"""Socket timeout set on the subscriber handle during the warmup
handshake (seconds).

The warmup handshake emits one event and reads it back via
``sync_read_frame``; 5 s is a conservative bound that covers a slow
daemon startup without hanging indefinitely.
"""

_SOURCES: tuple[str, ...] = (
    "github",
    "pytest",
    "docker",
    "fs",
)
_EVENT_TYPE_BY_SOURCE: dict[str, str] = {
    "github": "workflow_run",
    "pytest": "pytest_session",
    "docker": "docker_container",
    "fs": "fs_change",
}


def _build_event(i: int, source: str) -> EventInsert:
    """Build one per-source EventInsert. delivery_id is iteration- and time-keyed."""
    now_ns = time.time_ns()
    payload: dict[str, Any] = {"i": i, "source": source}
    return EventInsert(
        delivery_id=f"first_match:{source}:{i}-{now_ns}",
        source=source,
        event_type=_EVENT_TYPE_BY_SOURCE[source],
        owner="bench",
        repo="ttfae-first-match",
        received_at=now_ns,
        payload_json=msgspec.json.encode(payload).decode(),
        ingest_method="bench",
        status="completed",
        conclusion="success",
    )


def _run_loop(
    *,
    handle_db: Path,
    sub: SubscriberHandle,
    n: int,
    warmup: int,
    rate_hz: float,
    hdr_aggregate: HdrRecorder,
    hdr_per_source: dict[str, HdrRecorder],
) -> None:
    """Run one bench loop. Records into aggregate + per-source HdrRecorders.

    Rotates through the four sources round-robin (``i % 4``) so each
    source receives ``n // 4`` samples after warmup discard.

    Uses one :class:`SubscriberHandle` across all iterations. Opening
    a fresh subscriber per iteration would race the daemon's
    subscribe-registration against the same-iteration's emit -- the
    daemon needs a few hundred microseconds to finish accepting the
    subscribe frame and adding the subscriber to its broadcast set;
    a same-iteration emit can fire before that, and the broadcast
    fan-out misses the not-yet-registered subscriber, hanging
    ``await_predicate`` until the 5s deadline. Reusing one subscriber
    eliminates that race after the first warmup iteration.

    Each iteration emits exactly one matching frame and
    ``await_predicate`` consumes exactly one frame on match, so the
    socket recv buffer is empty between iterations -- no frame leaks
    across iterations, even with reuse.
    """
    sched = OpenLoopScheduler(rate_hz=rate_hz, n=n + warmup)

    for i, t_intended_ns in enumerate(sched):
        now_ns = time.monotonic_ns()
        if now_ns < t_intended_ns:
            time.sleep((t_intended_ns - now_ns) / 1e9)

        source = _SOURCES[i % len(_SOURCES)]
        # The broadcast frame nests the source under ``fields`` (see
        # ``broadcast._row_to_frame``); top-level keys are id/kind/
        # owner/repo/event_type/received_at/delivery_id/summary/
        # fields. A naive ``source="x"`` predicate would dig for
        # top-level ``source`` (which is absent) and never match.
        pred = parse_match([f'fields.source="{source}"'])
        # Bind ``pred`` via default argument so the closure does not
        # capture the loop variable (ruff B023). The default is
        # evaluated at def time, locking in this iteration's
        # predicate object.

        def _decide(frame: dict[str, Any], pred: Any = pred) -> FrameDecision:
            if pred(frame):
                return FrameDecision.MATCHED
            return FrameDecision.CONTINUE

        event = _build_event(i, source)
        t0 = time.time_ns()
        emit_mod.emit_batch([event], db_path=handle_db)
        outcome = await_predicate(sub, decide=_decide, deadline_seconds=_DEADLINE_SECONDS)
        t_end = time.time_ns()

        if not outcome.matched:
            raise RuntimeError(
                f"iteration {i} ({source}): wait did not match within {_DEADLINE_SECONDS:.0f}s: {outcome}"
            )

        if i >= warmup:
            latency = t_end - t0
            hdr_aggregate.record(latency)
            hdr_per_source[source].record(latency)


def _do_warmup_handshake(*, handle_db: Path, sub: SubscriberHandle) -> None:
    """Emit one synthetic event and consume its frame off the subscriber.

    Without a token, :func:`open_subscriber` returns immediately
    after ``sendall`` of the subscribe envelope; the daemon may
    still be processing the registration when the bench's first
    emit fires, causing the broadcast to miss the not-yet-
    registered subscriber. This handshake forces a confirmed
    end-to-end roundtrip before any timed sample, eliminating the
    race.

    Reads via :func:`sync_read_frame` directly rather than
    :func:`await_predicate` so the warm-up does not depend on
    predicate-engine semantics (the timed loop relies on
    ``await_predicate`` working; the warm-up only needs to confirm
    one frame can reach the subscriber). The deadline is enforced
    by a socket timeout set on ``sub.sock`` and restored afterward.
    """
    warmup_event = EventInsert(
        delivery_id=f"first_match:warmup-{time.time_ns()}",
        source="github",
        event_type="workflow_run",
        owner="bench",
        repo="warmup",
        received_at=time.time_ns(),
        payload_json='{"warmup": true}',
        ingest_method="bench",
        status="completed",
        conclusion="success",
    )
    emit_mod.emit_batch([warmup_event], db_path=handle_db)
    saved_timeout = sub.sock.gettimeout()
    sub.sock.settimeout(_SUBSCRIBER_SOCK_TIMEOUT_SEC)
    try:
        frame = sync_read_frame(sub.sock)
        if frame is None:
            raise RuntimeError("warmup handshake: subscriber socket closed (daemon died?)")
    finally:
        sub.sock.settimeout(saved_timeout)


def _format_percentiles(hdr: HdrRecorder) -> dict[str, Any]:
    """Render p50/p90/p99 from one HdrRecorder for JSON output."""
    out: dict[str, Any] = {}
    for p, key in ((0.50, "p50"), (0.90, "p90"), (0.99, "p99")):
        stats = hdr.percentile_with_ci(p)
        out[key] = {
            "value_ns": int(stats["value_ns"]),
            "ci_low_ns": int(stats["ci_low_ns"]),
            "ci_high_ns": int(stats["ci_high_ns"]),
        }
    out["n"] = hdr.count
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Per-source TTFAE via await_predicate (first-match).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--n", type=int, default=_DEFAULT_N, help="number of measurement samples (default: 5000)")
    parser.add_argument(
        "--warmup", type=int, default=_DEFAULT_WARMUP, help="number of leading samples to discard (default: 500)"
    )
    parser.add_argument("--rate", type=float, default=_DEFAULT_RATE_HZ, help="open-loop rate in Hz (default: 100.0)")
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--smoke", action="store_true", help="quick run: N=100, warmup=10, no regression check.")
    parser.add_argument(
        "--check-regression",
        action="store_true",
        help=(
            f"after the run, compare aggregate p99 (gc-enabled) against "
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
    per_source_main: dict[str, HdrRecorder] = {s: HdrRecorder() for s in _SOURCES}
    hdr_gc_off: HdrRecorder | None = None if args.no_gc_off else HdrRecorder()
    per_source_gc_off: dict[str, HdrRecorder] | None = None if args.no_gc_off else {s: HdrRecorder() for s in _SOURCES}

    with tempfile.TemporaryDirectory(prefix="waitbus-bench-first-match-") as tmp_str:
        tmp_dir = Path(tmp_str)
        with daemon_context(tmp_dir) as daemon:
            sub = open_subscriber(socket_path=str(daemon.broadcast_socket_path))
            try:
                # Warm-up handshake: without a token, ``open_subscriber``
                # returns immediately after ``sendall`` of the subscribe
                # frame -- the daemon may still be processing the
                # registration when the first emit fires, causing the
                # first broadcast to miss this subscriber. Emit one
                # synthetic event and consume the corresponding frame
                # via the same ``await_predicate`` path the bench uses
                # so the subscriber is guaranteed registered before any
                # timed sample is taken. Discarded; not recorded.
                _do_warmup_handshake(handle_db=daemon.db_path, sub=sub)

                print(f"[{_BENCH_NAME}] gc-on", file=sys.stderr)
                _run_loop(
                    handle_db=daemon.db_path,
                    sub=sub,
                    n=n,
                    warmup=warmup,
                    rate_hz=rate_hz,
                    hdr_aggregate=hdr_main,
                    hdr_per_source=per_source_main,
                )

                if hdr_gc_off is not None and per_source_gc_off is not None:
                    print(f"[{_BENCH_NAME}] gc-off", file=sys.stderr)
                    with gc_disabled():
                        _run_loop(
                            handle_db=daemon.db_path,
                            sub=sub,
                            n=n,
                            warmup=warmup,
                            rate_hz=rate_hz,
                            hdr_aggregate=hdr_gc_off,
                            hdr_per_source=per_source_gc_off,
                        )
            finally:
                sub.sock.close()

    ended_at_ns = time.time_ns()

    per_source_extra: dict[str, Any] = {
        "per_source_gc_enabled": {s: _format_percentiles(per_source_main[s]) for s in _SOURCES},
    }
    if per_source_gc_off is not None:
        per_source_extra["per_source_gc_disabled"] = {s: _format_percentiles(per_source_gc_off[s]) for s in _SOURCES}

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
            "scope_note": "single subscriber reused across iterations; await_predicate consumes one frame per match",
            **per_source_extra,
        },
    )

    output_path = resolve_output_path(_BENCH_NAME, _RESULTS_DIR, args.output, env)

    write_result(result, output_path)
    print(f"[{_BENCH_NAME}] wrote {output_path}", file=sys.stderr)

    p50 = result.percentiles_gc_enabled["p50"]
    p99 = result.percentiles_gc_enabled["p99"]
    print(
        f"[{_BENCH_NAME}] aggregate gc-enabled  p50={p50.value_ns / 1e6:8.3f} ms  "
        f"p99={p99.value_ns / 1e6:8.3f} ms  (n={result.n_samples})",
        file=sys.stderr,
    )
    for s in _SOURCES:
        rec = per_source_main[s]
        if rec.count == 0:
            continue
        sp50 = rec.value_at_percentile_fraction(0.50)
        sp99 = rec.value_at_percentile_fraction(0.99)
        print(
            f"[{_BENCH_NAME}]   {s:7s} p50={sp50 / 1e6:8.3f} ms  p99={sp99 / 1e6:8.3f} ms  (n={rec.count})",
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
