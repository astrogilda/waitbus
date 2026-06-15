"""Cross-source predicate-wait latency under mixed load.

This is the waitbus v0.5.0 marketing number for the source-agnostic
predicate engine: "if you wait on a mixed-source predicate while
many events stream in, what's the wait-to-match latency you see?"

The benchmark measures:

- **Predicate.** Each wait call constructs a randomized 3-way OR
  over a random 3-subset of ``{github, pytest, docker, fs}``. The
  predicate matches frames whose ``fields.owner`` equals the
  per-iteration marker ``"bench-match"`` AND whose ``fields.source``
  is one of the chosen subset. Background-load events use the
  marker ``"bench-bg"`` and are deliberately NOT matched.
- **Background load.** A daemon thread emits non-matching events at
  5 events/sec aggregate, distributed 50%/20%/20%/10% across
  github/pytest/docker/fs (the corpus-distribution share the
  captured-corpus replayer will eventually use; synthesised here
  because the corpus is not yet captured).
- **Matching event.** Per iteration, exactly one matching event is
  injected at ``t_inject = t_start + 50ms`` from a thread, with
  ``fields.source`` set to a randomly chosen member of the
  iteration's predicate subset. The bench measures
  ``t_response - t_inject`` -- the moment from when the matching
  event hits the daemon's listener to the moment
  ``await_predicate`` returns ``MATCHED``.
- **Aggregate + per-source.** Result carries the aggregate
  ``p50/p90/p99`` plus a per-matching-source breakdown so a
  reviewer can see whether one source dominates (it shouldn't --
  all four sources go through the same broadcast + predicate
  path).

Sample posture
--------------
N=1000 wait calls + 100 warmup (smaller than the 5000-sample TTFAE
benches because each iteration is wall-clock-bound by the 50 ms
inject delay plus broadcast roundtrip; 1000 samples at ~80 ms
per call = ~80 s per phase). Wilson Score CI on p99 at N=1000
gives ~+/-0.6 percentile points -- defensible but wider than the
N=5000 TTFAE benches. ``BENCHMARKING.md`` records this trade-off
in its sample-size table.

Invocation
----------
::

    # Smoke (~10 seconds, N=50)
    uv run python -m benchmarks.bench_predicate_wait_under_mixed_load --smoke

    # Production baseline
    taskset -c 2,3 uv run python -m benchmarks.bench_predicate_wait_under_mixed_load \\
        --output benchmarks/baselines/predicate_wait_under_mixed_load.json
"""

from __future__ import annotations

import argparse
import contextlib
import random
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Final

import msgspec

from benchmarks._bench_shared import CANONICAL_RNG_SEED
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
    check_regression,
    collect_result,
    daemon_context,
    environment_report,
    gc_disabled,
    resolve_output_path,
    write_result,
)

_BENCH_NAME = "predicate_wait_under_mixed_load"
_DEFAULT_N = 1000
_DEFAULT_WARMUP = 100
_SMOKE_N = 50
_SMOKE_WARMUP = 5
_INJECT_DELAY_NS = 50_000_000  # 50 ms; matches implementation notes
_BG_RATE_HZ = 5.0  # aggregate background events/sec
_DEADLINE_SECONDS = 5.0
_BASELINE_PATH = Path(__file__).resolve().parent / "baselines" / f"{_BENCH_NAME}.json"
_RESULTS_DIR = Path(__file__).resolve().parent / "results"

_PREDICATE_SUBSET_SIZE: Final[int] = 3
"""Number of sources drawn per iteration to form the OR-over-source
predicate subset.

3 of 4 available sources means 25% of background events are from the
non-matched source, exercising the predicate's short-circuit path
while keeping the match probability per-source at 1/3 of emitted
matching events.
"""

_SUBSCRIBER_SOCK_TIMEOUT_SEC: Final[float] = 5.0
"""Socket timeout set on the subscriber handle during the warmup
handshake (seconds).

The warmup handshake emits one synthetic event and reads it back via
``sync_read_frame``; 5 s is a conservative bound that covers a slow
daemon startup without hanging indefinitely if the daemon died.
"""

_SOURCES: tuple[str, ...] = (
    "github",
    "pytest",
    "docker",
    "fs",
)
# Corpus-distribution-shaped probabilities for background-load
# source selection. github:50%, pytest:20%, docker:20%, fs:10% --
# mirrors the captured-corpus distribution from the event replay
# baseline data.
_BG_SOURCE_WEIGHTS: tuple[tuple[str, float], ...] = (
    ("github", 0.50),
    ("pytest", 0.20),
    ("docker", 0.20),
    ("fs", 0.10),
)
_EVENT_TYPE_BY_SOURCE: dict[str, str] = {
    "github": "workflow_run",
    "pytest": "pytest_session",
    "docker": "docker_container",
    "fs": "fs_change",
}
_BENCH_OWNER_MATCH = "bench-match"
_BENCH_OWNER_BG = "bench-bg"
_BENCH_REPO = "predicate-wait-mixed"


def _weighted_choice(rng: random.Random, weights: tuple[tuple[str, float], ...]) -> str:
    """Sample one source from the weighted distribution."""
    r = rng.random()
    accumulated = 0.0
    for source, weight in weights:
        accumulated += weight
        if r < accumulated:
            return source
    return weights[-1][0]


def _build_event(*, source: str, owner: str, seq: int) -> EventInsert:
    """Build one EventInsert with a per-call unique delivery_id."""
    now_ns = time.time_ns()
    payload: dict[str, Any] = {"seq": seq, "owner": owner, "source": source}
    return EventInsert(
        delivery_id=f"{owner}:{source}:{seq}-{now_ns}",
        source=source,
        event_type=_EVENT_TYPE_BY_SOURCE[source],
        owner=owner,
        repo=_BENCH_REPO,
        received_at=now_ns,
        payload_json=msgspec.json.encode(payload).decode(),
        ingest_method="bench",
        status="completed",
        conclusion="success",
    )


class _BackgroundEmitter(threading.Thread):
    """Emits non-matching background events at a steady rate.

    Runs until :meth:`stop` is called. The rate is enforced via a
    monotonic-clock-paced sleep so it does not skew if the daemon
    momentarily lags. Source per emission is drawn from
    :data:`_BG_SOURCE_WEIGHTS` so the load distribution mirrors the
    corpus's intended capture share.

    The events carry ``owner="bench-bg"`` so the foreground
    predicate (which always pins ``fields.owner="bench-match"``)
    cannot match them by accident.
    """

    def __init__(self, *, db_path: Path, rate_hz: float, rng_seed: int) -> None:
        super().__init__(name="waitbus-bench-bg-emitter", daemon=True)
        self._db_path = db_path
        self._interval_s = 1.0 / rate_hz
        self._stop_event = threading.Event()
        self._rng = random.Random(rng_seed)
        self._seq = 0
        self._emitted = 0

    @property
    def emitted_count(self) -> int:
        return self._emitted

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        next_at = time.monotonic()
        while not self._stop_event.is_set():
            now = time.monotonic()
            # Use Event.wait so stop() short-circuits the sleep if
            # the bench is shutting down. Returns True when stop was
            # signalled; we exit cleanly.
            if now < next_at and self._stop_event.wait(next_at - now):
                return
            source = _weighted_choice(self._rng, _BG_SOURCE_WEIGHTS)
            event = _build_event(source=source, owner=_BENCH_OWNER_BG, seq=self._seq)
            self._seq += 1
            # Best-effort: a transient SQLite BUSY or socket error
            # on the background path is acceptable; the foreground
            # path's measurement is what matters. The background
            # thread MUST NOT crash the bench.
            with contextlib.suppress(Exception):
                emit_mod.emit_batch([event], db_path=self._db_path)
                self._emitted += 1
            next_at += self._interval_s


def _do_warmup_handshake(*, db_path: Path, sub: SubscriberHandle) -> None:
    """Emit one synthetic event and consume its frame off the subscriber.

    Same race-elimination handshake as ``bench_ttfae_first_match``.
    """
    warmup = _build_event(source="github", owner="bench-warmup", seq=0)
    emit_mod.emit_batch([warmup], db_path=db_path)
    saved = sub.sock.gettimeout()
    sub.sock.settimeout(_SUBSCRIBER_SOCK_TIMEOUT_SEC)
    try:
        frame = sync_read_frame(sub.sock)
        if frame is None:
            raise RuntimeError("warmup handshake: subscriber socket closed (daemon died?)")
    finally:
        sub.sock.settimeout(saved)


def _run_loop(
    *,
    db_path: Path,
    sub: SubscriberHandle,
    n: int,
    warmup: int,
    rng: random.Random,
    hdr_aggregate: HdrRecorder,
    hdr_per_source: dict[str, HdrRecorder],
) -> None:
    """Run one bench loop. Records into aggregate + per-source HdrRecorders.

    Each iteration:

    1. Pick a random 3-subset of sources for the predicate.
    2. Pick which source will produce the matching event.
    3. Build a predicate matching ``fields.owner="bench-match"``
       AND ``fields.source in <chosen subset>``.
    4. Start a thread that, after a 50ms sleep, emits the matching
       event (capturing ``t_inject`` immediately before emit).
    5. Call ``await_predicate(sub, decide, deadline=5s)``.
    6. Record ``t_response - t_inject`` (after warmup discard).
    """
    n_total = n + warmup

    for i in range(n_total):
        subset = tuple(rng.sample(_SOURCES, _PREDICATE_SUBSET_SIZE))
        match_source = rng.choice(subset)

        # Build the OR-over-source-subset predicate, AND'd with the
        # owner pin. parse_match's semantics: AND across distinct
        # keys, OR within a repeated key.
        match_specs = [f'fields.owner="{_BENCH_OWNER_MATCH}"']
        match_specs.extend(f'fields.source="{s}"' for s in subset)
        pred = parse_match(match_specs)

        match_event = _build_event(source=match_source, owner=_BENCH_OWNER_MATCH, seq=i)

        # Injector thread captures t_inject at the moment of
        # emit_batch and stores it in a single-cell list for the
        # main thread to read after await_predicate returns. Bind
        # via default arg so the closure does not capture loop vars.
        t_inject_holder: list[int] = []

        def _inject(
            event: EventInsert = match_event,
            holder: list[int] = t_inject_holder,
        ) -> None:
            time.sleep(_INJECT_DELAY_NS / 1e9)
            holder.append(time.time_ns())
            # If the daemon dies mid-bench, await_predicate surfaces
            # that via outcome.peer_closed. Don't propagate from the
            # injector thread.
            with contextlib.suppress(Exception):
                emit_mod.emit_batch([event], db_path=db_path)

        def _decide(frame: dict[str, Any], pred: Any = pred) -> FrameDecision:
            if pred(frame):
                return FrameDecision.MATCHED
            return FrameDecision.CONTINUE

        injector = threading.Thread(target=_inject, name=f"waitbus-bench-inject-{i}", daemon=True)
        injector.start()
        outcome = await_predicate(sub, decide=_decide, deadline_seconds=_DEADLINE_SECONDS)
        t_response = time.time_ns()
        injector.join(timeout=1.0)

        if not outcome.matched:
            raise RuntimeError(f"iteration {i} (match={match_source}, subset={list(subset)}): {outcome}")
        if not t_inject_holder:
            raise RuntimeError(f"iteration {i}: injector thread did not record t_inject")

        if i >= warmup:
            latency = t_response - t_inject_holder[0]
            hdr_aggregate.record(latency)
            hdr_per_source[match_source].record(latency)


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
        description="Headline cross-source predicate-wait latency under mixed load.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--n", type=int, default=_DEFAULT_N, help="number of measurement samples (default: 1000)")
    parser.add_argument(
        "--warmup", type=int, default=_DEFAULT_WARMUP, help="number of leading samples to discard (default: 100)"
    )
    parser.add_argument(
        "--bg-rate", type=float, default=_BG_RATE_HZ, help=f"background-load aggregate rate Hz (default: {_BG_RATE_HZ})"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=CANONICAL_RNG_SEED,
        help="RNG seed for predicate-subset / match-source randomization.",
    )
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--smoke", action="store_true", help="quick run: N=50, warmup=5, no regression check.")
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
    bg_rate: float = args.bg_rate

    env = environment_report()
    print(f"[{_BENCH_NAME}] n={n} warmup={warmup} bg_rate={bg_rate} Hz", file=sys.stderr)

    started_at_ns = time.time_ns()
    hdr_main = HdrRecorder()
    per_source_main: dict[str, HdrRecorder] = {s: HdrRecorder() for s in _SOURCES}
    hdr_gc_off: HdrRecorder | None = None if args.no_gc_off else HdrRecorder()
    per_source_gc_off: dict[str, HdrRecorder] | None = None if args.no_gc_off else {s: HdrRecorder() for s in _SOURCES}

    bg_emitted_total = 0

    with tempfile.TemporaryDirectory(prefix="waitbus-bench-mixed-load-") as tmp_str:
        tmp_dir = Path(tmp_str)
        with daemon_context(tmp_dir) as daemon:
            sub = open_subscriber(socket_path=str(daemon.broadcast_socket_path))
            bg = _BackgroundEmitter(db_path=daemon.db_path, rate_hz=bg_rate, rng_seed=args.seed)
            try:
                _do_warmup_handshake(db_path=daemon.db_path, sub=sub)
                bg.start()

                rng_main = random.Random(args.seed)
                print(f"[{_BENCH_NAME}] gc-on", file=sys.stderr)
                _run_loop(
                    db_path=daemon.db_path,
                    sub=sub,
                    n=n,
                    warmup=warmup,
                    rng=rng_main,
                    hdr_aggregate=hdr_main,
                    hdr_per_source=per_source_main,
                )

                if hdr_gc_off is not None and per_source_gc_off is not None:
                    print(f"[{_BENCH_NAME}] gc-off", file=sys.stderr)
                    rng_gc = random.Random(args.seed ^ 0xDEAD)
                    with gc_disabled():
                        _run_loop(
                            db_path=daemon.db_path,
                            sub=sub,
                            n=n,
                            warmup=warmup,
                            rng=rng_gc,
                            hdr_aggregate=hdr_gc_off,
                            hdr_per_source=per_source_gc_off,
                        )

                bg_emitted_total = bg.emitted_count
            finally:
                bg.stop()
                bg.join(timeout=2.0)
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
        rate_hz=0.0,  # bench rate is bench-defined (50ms inject + 5s deadline), not OpenLoopScheduler-paced.
        hdr_main=hdr_main,
        hdr_gc_off=hdr_gc_off,
        environment=env,
        extra={
            "smoke": args.smoke,
            "background_rate_hz": bg_rate,
            "background_events_total": bg_emitted_total,
            "inject_delay_ns": _INJECT_DELAY_NS,
            "predicate_subset_size": _PREDICATE_SUBSET_SIZE,
            **per_source_extra,
        },
    )

    output_path = resolve_output_path(_BENCH_NAME, _RESULTS_DIR, args.output, env)

    write_result(result, output_path)
    print(f"[{_BENCH_NAME}] wrote {output_path}", file=sys.stderr)

    p50 = result.percentiles_gc_enabled["p50"]
    p90 = result.percentiles_gc_enabled["p90"]
    p99 = result.percentiles_gc_enabled["p99"]
    print(
        f"[{_BENCH_NAME}] aggregate gc-enabled  p50={p50.value_ns / 1e6:8.3f} ms  "
        f"p90={p90.value_ns / 1e6:8.3f} ms  "
        f"p99={p99.value_ns / 1e6:8.3f} ms  (n={result.n_samples}; bg={bg_emitted_total})",
        file=sys.stderr,
    )
    for s in _SOURCES:
        rec = per_source_main[s]
        if rec.count == 0:
            continue
        sp50 = rec.value_at_percentile_fraction(0.50)
        sp99 = rec.value_at_percentile_fraction(0.99)
        print(
            f"[{_BENCH_NAME}]   match={s:7s} p50={sp50 / 1e6:8.3f} ms  p99={sp99 / 1e6:8.3f} ms  (n={rec.count})",
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
