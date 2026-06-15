"""Polling counterfactual baseline for the GitHub source.

Measures the polling-cycle latency a polling agent would experience when
watching a GitHub Actions run status: how long from the moment a run's
state changes to the moment the agent's next poll observes the new state.

This is the comparison baseline the launch articles cite in the per-source
comparison matrix.  It is NOT a measurement of waitbus; waitbus is not involved.

What is measured
----------------
The bench emulates the behavior of ``gh run watch --interval 3`` (the
canonical agent-side polling tool, which defaults to a 3-second poll
interval) without invoking the ``gh`` CLI.  A Python polling loop queries
a local mock HTTP server every 3 seconds.  This avoids subprocess-startup
overhead and pins the interval exactly to 3.0 s, isolating the polling-
cycle latency signal from CLI startup noise.

The mock server serves the GitHub Actions REST endpoint shape at::

    GET /repos/<owner>/<repo>/actions/runs/<id>

It returns ``{"status": "in_progress"}`` until the bench triggers a state
flip, then returns ``{"status": "completed", "conclusion": "success"}``.

Per iteration:

1. Reset the mock server to ``in_progress``.
2. Start the 3-second polling loop in the foreground.
3. After a random delay drawn uniformly from [0, 3.0) seconds, the bench
   triggers the state flip and records ``t_inject_ns = time.time_ns()``.
4. The polling loop records ``t_observe_ns = time.time_ns()`` when it first
   sees the ``"completed"`` status.
5. Latency = ``t_observe_ns - t_inject_ns``.

Expected distribution
---------------------
With a 3 s polling interval and a state-flip that can occur at any point in
[0, 3) seconds, the latency is roughly uniform in [0, 3 s].  The theoretical
expectation is 1.5 s; p99 approaches 3 s.  This is the floor the polling
agent pays regardless of how fast the underlying API responds — even an
infinitely fast API cannot improve on the polling-interval ceiling.

Sample posture
--------------
N=500 + 50 warmup.  Each iteration waits up to one poll interval (3 s) after
the state-flip, so average wall-clock per iteration is ~1.5 s.  Expected
total: ~750 s ≈ 12 min per phase.  The two-phase (gc-enabled + gc-disabled)
total is ~24 min.  ``--smoke`` runs N=30 + 5 warmup (~1 min).

The ``OpenLoopScheduler`` is not used here because the per-iteration duration
is wall-clock-bound by the polling interval itself, not by a fixed rate.
The bench is inherently serial: one state-flip + one poll-cycle per iteration.

GC discipline
-------------
Despite the polling-interval ceiling dominating the latency (making GC pauses
irrelevant in practice), the bench still produces gc-enabled and gc-disabled
runs to follow the suite's standard two-phase structure and allow reviewers to
confirm that GC has no measurable effect.

Invocation
----------
::

    # Smoke (~2 minutes)
    uv run python -m benchmarks.bench_polling_baseline_github --smoke

    # Production baseline (~24 minutes)
    taskset -c 2,3 uv run python -m benchmarks.bench_polling_baseline_github \\
        --n 500 --warmup 50 \\
        --output benchmarks/baselines/polling_baseline_github.json

    # Regression check in CI
    uv run python -m benchmarks.bench_polling_baseline_github --check-regression
"""

from __future__ import annotations

import argparse
import http.client
import http.server
import json
import random
import sys
import threading
import time
from pathlib import Path
from typing import Any, Final

from benchmarks._bench_shared import CANONICAL_RNG_SEED, RNG_GC_XOR_MASK

from ._harness import (
    HdrRecorder,
    check_regression,
    collect_result,
    environment_report,
    gc_disabled,
    resolve_output_path,
    write_result,
)

_BENCH_NAME = "polling_baseline_github"
_DEFAULT_N = 500
_DEFAULT_WARMUP = 50
_SMOKE_N = 30
_SMOKE_WARMUP = 5
_POLL_INTERVAL_S: float = 3.0
_FAKE_OWNER = "bench-owner"
_FAKE_REPO = "bench-repo"
_FAKE_RUN_ID = "123456789"
_BASELINE_PATH = Path(__file__).resolve().parent / "baselines" / f"{_BENCH_NAME}.json"
_RESULTS_DIR = Path(__file__).resolve().parent / "results"

_POLL_TIMEOUT_MULTIPLIER: Final[int] = 4
"""Multiplier applied to ``_POLL_INTERVAL_S`` to derive the per-iteration
deadline for the polling loop.

Setting the deadline to 4 x the poll interval means a stalled iteration
will surface within ~12 s (4 x 3 s = 12 s) rather than blocking
indefinitely. The margin also accommodates a late flip-thread scheduling
delay without a spurious timeout.
"""

_HTTP_CONNECT_TIMEOUT_SEC: Final[float] = 5.0
"""Socket-level timeout for the HTTP connection to the mock GitHub API
server (seconds).

The mock server is on loopback; 5 s is a generous cap against an
unexpected scheduler stall during the TCP handshake. Exceeding this
timeout on loopback indicates a pathological system condition rather
than normal bench behaviour.
"""

_HTTP_TEARDOWN_TIMEOUT_SEC: Final[float] = 5.0
"""Timeout for ``server_thread.join()`` after ``server.shutdown()``
is called (seconds).

The mock server's ``serve_forever`` loop exits promptly on
``shutdown()``; 5 s prevents a hang from masking the bench result.
"""

# Maximum time to wait for a single poll cycle to observe the state flip.
# Set well above one poll interval to catch pathological hangs without
# letting a stalled iteration block the whole bench indefinitely.
_POLL_TIMEOUT_S: float = _POLL_INTERVAL_S * _POLL_TIMEOUT_MULTIPLIER


# ---------------------------------------------------------------------------
# Mock GitHub Actions HTTP server
# ---------------------------------------------------------------------------


class _MockGitHubHandler(http.server.BaseHTTPRequestHandler):
    """Serve a minimal GitHub Actions ``/runs/<id>`` endpoint.

    The handler reads ``server.completed`` (a threading.Event set by
    the bench's main thread) to decide which JSON body to return.
    Requests to any other path get a 404.
    """

    # Suppress the default per-request log lines; they would interleave
    # with the bench's own stderr progress output.
    def log_message(self, format: str, *args: Any) -> None:
        pass

    def do_GET(self) -> None:
        expected = f"/repos/{_FAKE_OWNER}/{_FAKE_REPO}/actions/runs/{_FAKE_RUN_ID}"
        if self.path != expected:
            self.send_response(404)
            self.end_headers()
            return

        # The server object is the _MockGitHubServer instance; it carries
        # the completed Event.
        server: _MockGitHubServer = self.server  # type: ignore[assignment]
        if server.completed.is_set():
            body = json.dumps({"status": "completed", "conclusion": "success"}).encode()
        else:
            body = json.dumps({"status": "in_progress"}).encode()

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class _MockGitHubServer(http.server.HTTPServer):
    """HTTPServer subclass that carries the per-iteration state flag.

    ``completed`` is reset to a fresh ``threading.Event()`` at the start
    of each iteration (in-progress state) and set by the bench's main
    thread to trigger the state flip.
    """

    def __init__(self) -> None:
        # Bind to an ephemeral port on loopback.
        super().__init__(("127.0.0.1", 0), _MockGitHubHandler)
        self.completed: threading.Event = threading.Event()

    @property
    def port(self) -> int:
        port = self.server_address[1]
        assert isinstance(port, int)
        return port

    def reset(self) -> None:
        """Transition to in_progress for the next iteration."""
        self.completed.clear()

    def flip(self) -> None:
        """Transition to completed; the next poll will observe the change."""
        self.completed.set()


# ---------------------------------------------------------------------------
# Per-iteration polling loop
# ---------------------------------------------------------------------------


def _poll_until_completed(
    *,
    host: str,
    port: int,
    poll_interval_s: float,
    deadline_s: float,
) -> int | None:
    """Poll the mock endpoint until it returns ``"completed"`` or we time out.

    Returns the ``time.time_ns()`` value captured immediately after the
    HTTP response that first shows ``"completed"``, or ``None`` if the
    deadline expires without observing the completion.

    Uses :mod:`http.client` (stdlib) to avoid pulling in ``requests``
    for a one-field JSON body.

    The poll-interval sleep uses ``time.monotonic()`` for pacing so that
    a slow HTTP round-trip does not compress the next interval.  The
    effect on latency measurement is negligible (round-trips to loopback
    are <<1 ms) but the approach is more faithful to how a real polling
    agent would behave.
    """
    path = f"/repos/{_FAKE_OWNER}/{_FAKE_REPO}/actions/runs/{_FAKE_RUN_ID}"
    deadline_mono = time.monotonic() + deadline_s
    next_poll_mono = time.monotonic()

    while time.monotonic() < deadline_mono:
        # Pace the poll to the intended interval using the monotonic clock
        # so we don't accumulate drift from slow iterations.
        now_mono = time.monotonic()
        if now_mono < next_poll_mono:
            remaining = next_poll_mono - now_mono
            time.sleep(remaining)

        try:
            conn = http.client.HTTPConnection(host, port, timeout=_HTTP_CONNECT_TIMEOUT_SEC)
            conn.request("GET", path)
            resp = conn.getresponse()
            body = resp.read()
            conn.close()
        except OSError:
            # Transient connect failure on loopback is unexpected but
            # should not crash the bench; treat it as a missed poll.
            next_poll_mono += poll_interval_s
            continue

        t_observe = time.time_ns()

        try:
            data: dict[str, Any] = json.loads(body)
        except (ValueError, UnicodeDecodeError):
            next_poll_mono += poll_interval_s
            continue

        if data.get("status") == "completed":
            return t_observe

        next_poll_mono += poll_interval_s

    return None


# ---------------------------------------------------------------------------
# State-flip injector
# ---------------------------------------------------------------------------


def _schedule_flip(
    *,
    server: _MockGitHubServer,
    delay_s: float,
    t_inject_holder: list[int],
) -> None:
    """Sleep for ``delay_s`` then flip the server state and record t_inject.

    Runs in a daemon thread so the bench's foreground polling loop runs
    concurrently.  The t_inject timestamp is recorded immediately before
    ``server.flip()`` so ``t_observe - t_inject`` represents the true
    observation delay and not any additional flip-to-handler latency.
    """
    time.sleep(delay_s)
    t_inject_holder.append(time.time_ns())
    server.flip()


# ---------------------------------------------------------------------------
# Core measurement loop
# ---------------------------------------------------------------------------


def _run_loop(
    *,
    server: _MockGitHubServer,
    n: int,
    warmup: int,
    rng: random.Random,
    hdr: HdrRecorder,
) -> None:
    """Run one phase of the bench and record into ``hdr``.

    Each iteration:

    1. Reset the mock server to in_progress.
    2. Spawn a daemon thread to flip the state after a random delay in
       [0, ``_POLL_INTERVAL_S``).
    3. Run the polling loop in the foreground until it observes
       ``"completed"`` or times out.
    4. Record ``t_observe - t_inject`` (after warmup discard).

    The random flip delay is drawn from ``[0, _POLL_INTERVAL_S)`` so
    the resulting latency distribution approximates Uniform(0, 3 s) —
    the theoretical polling-interval floor.
    """
    n_total = n + warmup
    host = "127.0.0.1"
    port = server.port

    for i in range(n_total):
        server.reset()
        t_inject_holder: list[int] = []

        delay_s = rng.uniform(0.0, _POLL_INTERVAL_S)
        flipper = threading.Thread(
            target=_schedule_flip,
            kwargs={"server": server, "delay_s": delay_s, "t_inject_holder": t_inject_holder},
            name=f"waitbus-bench-flip-{i}",
            daemon=True,
        )
        flipper.start()

        t_observe = _poll_until_completed(
            host=host,
            port=port,
            poll_interval_s=_POLL_INTERVAL_S,
            deadline_s=_POLL_TIMEOUT_S,
        )

        flipper.join(timeout=_POLL_INTERVAL_S + 1.0)

        if t_observe is None:
            raise RuntimeError(
                f"iteration {i}: polling loop timed out after {_POLL_TIMEOUT_S} s "
                "without observing 'completed' — mock server or flip thread may have stalled."
            )
        if not t_inject_holder:
            raise RuntimeError(
                f"iteration {i}: flip thread did not record t_inject — thread may have been preempted before the flip."
            )

        if i >= warmup:
            latency_ns = t_observe - t_inject_holder[0]
            # A negative latency indicates the poll that returned
            # "completed" was already in flight when the flip occurred
            # (the poll started before the flip but the response arrived
            # after).  This is a legitimate sub-zero-latency race; clamp
            # to 1 ns so HdrRecorder accepts it.  The clamp count in the
            # result JSON will reveal if this race is frequent.
            hdr.record(max(1, latency_ns))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Polling-cycle latency baseline for the GitHub source (gh run watch --interval 3 emulation).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--n",
        type=int,
        default=_DEFAULT_N,
        help=f"number of measurement samples (default: {_DEFAULT_N})",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=_DEFAULT_WARMUP,
        help=f"number of leading samples to discard (default: {_DEFAULT_WARMUP})",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=CANONICAL_RNG_SEED,
        help="RNG seed for the state-flip delay distribution (default: 0xC1B5).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help=(f"path to write the result JSON (default: benchmarks/results/{_BENCH_NAME}_<host>_<ts>.json)"),
    )
    parser.add_argument(
        "--smoke",
        action="store_true",
        help=f"quick run: N={_SMOKE_N}, warmup={_SMOKE_WARMUP}, no regression check.",
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
            "skip the gc-disabled companion run.  NOT recommended for "
            "baselines; useful when iterating on the bench script itself."
        ),
    )
    args = parser.parse_args(argv)

    n = _SMOKE_N if args.smoke else args.n
    warmup = _SMOKE_WARMUP if args.smoke else args.warmup

    env = environment_report()
    print(
        f"[{_BENCH_NAME}] n={n} warmup={warmup} poll_interval={_POLL_INTERVAL_S}s",
        file=sys.stderr,
    )
    print(
        f"[{_BENCH_NAME}] expected wall-clock ~{(n + warmup) * _POLL_INTERVAL_S / 2:.0f}s per phase",
        file=sys.stderr,
    )

    # Start the mock server in a background daemon thread.
    server = _MockGitHubServer()
    server_thread = threading.Thread(
        target=server.serve_forever,
        name="waitbus-bench-mock-gh-api",
        daemon=True,
    )
    server_thread.start()
    mock_port = server.port
    print(f"[{_BENCH_NAME}] mock GitHub API listening on 127.0.0.1:{mock_port}", file=sys.stderr)

    started_at_ns = time.time_ns()
    hdr_main = HdrRecorder()
    hdr_gc_off: HdrRecorder | None = None if args.no_gc_off else HdrRecorder()

    try:
        rng_main = random.Random(args.seed)
        print(f"[{_BENCH_NAME}] gc-on", file=sys.stderr)
        _run_loop(
            server=server,
            n=n,
            warmup=warmup,
            rng=rng_main,
            hdr=hdr_main,
        )

        if hdr_gc_off is not None:
            rng_gc = random.Random(args.seed ^ RNG_GC_XOR_MASK)
            print(f"[{_BENCH_NAME}] gc-off", file=sys.stderr)
            with gc_disabled():
                _run_loop(
                    server=server,
                    n=n,
                    warmup=warmup,
                    rng=rng_gc,
                    hdr=hdr_gc_off,
                )
    finally:
        server.shutdown()
        server_thread.join(timeout=_HTTP_TEARDOWN_TIMEOUT_SEC)

    ended_at_ns = time.time_ns()

    result = collect_result(
        bench_name=_BENCH_NAME,
        started_at_ns=started_at_ns,
        ended_at_ns=ended_at_ns,
        n_warmup_discarded=warmup,
        rate_hz=0.0,  # not rate-driven; each iteration is wall-clock-bound by the poll interval
        hdr_main=hdr_main,
        hdr_gc_off=hdr_gc_off,
        environment=env,
        extra={
            "smoke": args.smoke,
            "gh_polling_interval_s": _POLL_INTERVAL_S,
            "mock_port": mock_port,
            "scope_note": ("emulates gh run watch --interval 3 polling cycle; no GitHub API or gh CLI invoked"),
        },
    )

    output_path = resolve_output_path(_BENCH_NAME, _RESULTS_DIR, args.output, env)

    write_result(result, output_path)
    print(f"[{_BENCH_NAME}] wrote {output_path}", file=sys.stderr)

    p50 = result.percentiles_gc_enabled["p50"]
    p90 = result.percentiles_gc_enabled["p90"]
    p99 = result.percentiles_gc_enabled["p99"]
    print(
        f"[{_BENCH_NAME}] gc-enabled  p50={p50.value_ns / 1e9:6.3f} s  "
        f"p90={p90.value_ns / 1e9:6.3f} s  "
        f"p99={p99.value_ns / 1e9:6.3f} s  "
        f"(n={result.n_samples})",
        file=sys.stderr,
    )
    if result.percentiles_gc_disabled is not None:
        g50 = result.percentiles_gc_disabled["p50"]
        g99 = result.percentiles_gc_disabled["p99"]
        print(
            f"[{_BENCH_NAME}] gc-disabled p50={g50.value_ns / 1e9:6.3f} s  p99={g99.value_ns / 1e9:6.3f} s",
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
