"""TTFAE for the GitHub webhook source.

Measures the wall-clock interval from webhook listener entry (t=0) to
the moment a subscriber's ``recv()`` returns the corresponding broadcast
frame (t=end). The full hot path is exercised: ``WebhookHandler.do_POST``
-> HMAC verify -> JSON parse -> ``insert_event`` -> SQLite insert + commit
-> doorbell ring -> broadcaster pickup -> SELECT -> AF_UNIX frame send ->
subscriber ``sync_read_frame``.

t=0 capture mechanism
---------------------
``WebhookHandler.do_POST`` runs in the listener's HTTP server thread.
The bench monkey-patches the method at startup so that
:func:`benchmarks._harness.capture_t0` is called with the request's
``X-GitHub-Delivery`` header value immediately before the original
``do_POST`` body executes. This makes t=0 the first instruction of
listener entry rather than the POST client's send-complete, which is
the correct measurement boundary (listener entry, not the
GitHub-to-listener network leg).

The patch is installed once before the bench loop and restored
unconditionally in the ``finally`` block.

What this bench excludes
------------------------
The GitHub-to-listener network leg (the HTTPS POST from GitHub's servers
to the operator's host) is excluded by design. That leg is bounded by
the operator's inbound latency, not by waitbus internals, and including
it would convolve a deployment-specific externality with the waitbus
emit-path cost. This exclusion is identical in spirit to
``bench_ttfae_fs.py`` excluding the kernel's inotify detection latency.
The ``scope_note`` field in the result JSON makes the exclusion explicit
for readers of the baseline file.

Workload
--------
Each iteration synthesises a minimal ``workflow_run`` webhook payload and
POSTs it to the listener's loopback HTTP endpoint over a blocking
``http.client.HTTPConnection``. A unique ``X-GitHub-Delivery`` UUID is
generated per iteration to satisfy SQLite's ``INSERT OR IGNORE`` duplicate
guard (a duplicate would suppress the doorbell ring, causing
``sync_read_frame`` to block indefinitely).

The HMAC-SHA256 signature is computed from the daemon's shared secret so
the listener accepts every request. A ``workflow_run`` event type is used
because the listener's ``_handle_github`` only stores rows for
``workflow_run`` and ``workflow_job``; other event types yield an
``ignored`` 200 response without a DB write, which would silently hang
``sync_read_frame``.

Server setup
------------
Two servers are required:

1. **Broadcast daemon** -- spun up via :func:`benchmarks._harness.daemon_context`
   against a temporary DB + sockets. Picks up events written by the listener
   and fans them out to subscribers.

2. **Webhook listener** -- an ``http.server.HTTPServer`` on an ephemeral
   port, dispatching to ``waitbus.listener.WebhookHandler``. The
   listener writes events to the same SQLite DB the daemon watches. Its
   ``db_path`` module-level callable and its ``WebhookHandler.secret``
   class attribute are redirected to the daemon's tmpdir values at setup
   time and restored on teardown.

Sample posture
--------------
Default N=5000 + 500 warmup at 100 Hz. Same as ``bench_ttfae_pytest.py``.
Wall-clock approximately 50 seconds per phase x 2 = ~2 minutes total.

Output
------
JSON via :func:`benchmarks._harness.write_result` at
``benchmarks/results/ttfae_github_{host}_{ts}.json``. With
``--check-regression``, compared against
``benchmarks/baselines/ttfae_github.json``; >25% degradation on p99 of
the gc-enabled run is a hard fail.

Invocation
----------
::

    # Smoke (~30 seconds)
    uv run python -m benchmarks.bench_ttfae_github --smoke

    # Production baseline (under taskset, ~2 minutes at 100 Hz)
    taskset -c 2,3 uv run python -m benchmarks.bench_ttfae_github \\
        --n 5000 --warmup 500 --rate 100 \\
        --output benchmarks/baselines/ttfae_github.json

    # Regression check in CI
    uv run python -m benchmarks.bench_ttfae_github --check-regression
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import hmac as hmac_mod
import http.client
import json
import sys
import tempfile
import threading
import time
import uuid
from collections.abc import Iterator
from http.server import HTTPServer
from pathlib import Path
from typing import Any

import msgspec

import waitbus.listener as listener_mod
from waitbus._broadcast_sub import open_subscriber
from waitbus._frame import sync_read_frame

from ._harness import (
    HdrRecorder,
    OpenLoopScheduler,
    capture_t0,
    check_regression,
    collect_result,
    consume_t0,
    daemon_context,
    environment_report,
    gc_disabled,
    print_percentile_summary,
    resolve_output_path,
    write_result,
)

_BENCH_NAME = "ttfae_github"
_DEFAULT_N = 5000
_DEFAULT_WARMUP = 500
_DEFAULT_RATE_HZ = 100.0
_SMOKE_N = 100
_SMOKE_WARMUP = 10
_BASELINE_PATH = Path(__file__).resolve().parent / "baselines" / f"{_BENCH_NAME}.json"
_RESULTS_DIR = Path(__file__).resolve().parent / "results"

# Loopback host for the listener's HTTPServer.
_LISTENER_HOST = "127.0.0.1"

# Synthetic owner / repo embedded in every bench webhook payload.
_BENCH_OWNER = "bench"
_BENCH_REPO = "ttfae-github"


def _make_secret() -> bytes:
    """Return a fixed bench-local HMAC secret.

    Using a fixed secret (rather than generating one per bench run)
    keeps the bench deterministic; there is no security requirement on
    this secret because the listener is bound to 127.0.0.1 only, the
    HTTPServer is ephemeral, and the DB is in a tmpdir that is cleaned
    up on exit.
    """
    return b"bench-github-secret-ttfae"


def _sign_body(secret: bytes, body: bytes) -> str:
    """Return an ``X-Hub-Signature-256`` header value for ``body``."""
    digest = hmac_mod.new(secret, body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def _build_payload(delivery_id: str) -> bytes:
    """Build a minimal valid ``workflow_run`` webhook payload as JSON bytes.

    The listener's ``_handle_github`` only stores events for
    ``workflow_run`` and ``workflow_job``; any other ``X-GitHub-Event``
    yields an ``ignored`` 200 response with no DB write, which would
    block ``sync_read_frame`` indefinitely. This payload satisfies the
    minimal repository/workflow_run structure the listener extracts
    fields from; field values are arbitrary because the bench measures
    latency, not payload diversity.

    The ``delivery_id`` is embedded in the payload solely for
    traceability; the listener ignores ``payload_json`` contents beyond
    the HMAC and JSON-parse checks.
    """
    payload: dict[str, Any] = {
        "action": "completed",
        "delivery": delivery_id,
        "repository": {
            "name": _BENCH_REPO,
            "owner": {"login": _BENCH_OWNER},
        },
        "workflow_run": {
            "id": 1,
            "name": "bench-workflow",
            "head_branch": "main",
            "head_sha": "abc123",
            "status": "completed",
            "conclusion": "success",
        },
    }
    return json.dumps(payload).encode("utf-8")


def _post_webhook(
    conn: http.client.HTTPConnection,
    *,
    body: bytes,
    secret: bytes,
    delivery_id: str,
) -> None:
    """Send one webhook POST; raise on non-200 response.

    Reuses the caller's persistent ``HTTPConnection`` to avoid the
    TCP handshake overhead that would be included in the measurement
    if a new connection were opened per iteration. The listener's
    ``WebhookHandler`` accepts keep-alive by default via
    ``BaseHTTPRequestHandler``'s HTTP/1.1 support.
    """
    sig = _sign_body(secret, body)
    conn.request(
        "POST",
        "/webhook",
        body=body,
        headers={
            "Content-Type": "application/json",
            "Content-Length": str(len(body)),
            "X-GitHub-Event": "workflow_run",
            "X-GitHub-Delivery": delivery_id,
            "X-Hub-Signature-256": sig,
        },
    )
    resp = conn.getresponse()
    # Drain the response body so the connection is ready for the next
    # request (HTTP/1.1 keep-alive requires the previous response to be
    # fully consumed before the next request).
    resp.read()
    if resp.status != 200:
        raise RuntimeError(f"listener returned HTTP {resp.status} for delivery {delivery_id!r}")


def _run_loop(
    *,
    listener_port: int,
    secret: bytes,
    sub_sock: Any,
    n: int,
    warmup: int,
    rate_hz: float,
    hdr: HdrRecorder,
) -> None:
    """Run one bench loop and record into ``hdr`` after warmup discard.

    Opens a persistent HTTP/1.1 keep-alive connection to the listener
    at the start of the loop and reuses it for all iterations. The
    connection teardown happens after the last iteration so the
    connection-close RTT is not included in any measurement sample.

    Per-iteration delivery_id uniqueness is guaranteed by
    ``uuid.uuid4().hex`` so SQLite's ``INSERT OR IGNORE`` never no-ops
    a duplicate and the doorbell always rings.
    """
    sched = OpenLoopScheduler(rate_hz=rate_hz, n=n + warmup)
    conn = http.client.HTTPConnection(_LISTENER_HOST, listener_port)
    try:
        for i, t_intended_ns in enumerate(sched):
            now_ns = time.monotonic_ns()
            if now_ns < t_intended_ns:
                time.sleep((t_intended_ns - now_ns) / 1e9)

            delivery_id = uuid.uuid4().hex
            body = _build_payload(delivery_id)

            # POST the webhook. The monkey-patched do_POST records t=0
            # (via capture_t0) immediately on entry, before HMAC verify
            # or DB work. The POST call does not return until the handler
            # has responded, so by the time we call sync_read_frame the
            # DB insert is either complete or in flight.
            _post_webhook(conn, body=body, secret=secret, delivery_id=delivery_id)

            # Skip any control frames (daemon heartbeats / subscribe acks)
            # that interleave between the POST and our event's broadcast
            # frame. They carry no ``delivery_id`` and must not be counted
            # as a sample -- a heartbeat landing mid-run is exactly what
            # raised ``KeyError: 'delivery_id'`` before this guard existed.
            while True:
                frame: bytes | None = sync_read_frame(sub_sock)
                t_recv = time.time_ns()
                if frame is None:
                    raise RuntimeError(f"iteration {i}: subscriber socket closed mid-bench (daemon died?)")
                # Decode the broadcast frame's top-level ``delivery_id``
                # to look up the t=0 timestamp recorded by the patched
                # listener. The broadcast-projection shape is a dict with
                # keys id/kind/owner/repo/event_type/received_at/
                # delivery_id/summary/fields (see broadcast._row_to_frame);
                # it is NOT the EventInsert/Event database row shape, so
                # we decode as a plain dict, not via decode_event.
                frame_dict: dict[str, Any] = msgspec.json.decode(frame, type=dict)
                if "delivery_id" in frame_dict:
                    break
            t0 = consume_t0(str(frame_dict["delivery_id"]))

            if i >= warmup:
                hdr.record(t_recv - t0)
    finally:
        conn.close()


@contextlib.contextmanager
def _listener_context(daemon: Any, secret: bytes) -> Iterator[int]:
    """Set up the webhook listener HTTPServer + monkey-patch teardown.

    Redirects ``listener_mod.db_path`` at the daemon's tmpdir DB so
    ``insert_event`` writes land where the daemon is watching. Patches
    the ``WebhookHandler`` HMAC secret to the bench secret. Starts an
    ``HTTPServer`` on an ephemeral port served by a daemon thread.
    Yields the listener port. Restores every patched binding (db_path,
    secret, am_secret) on exit and shuts the HTTP server + thread down
    cleanly.

    ``db_path`` is imported from ``._paths`` into the listener module's
    global namespace; it is not declared in the module stub, so the
    direct attribute accesses carry type: ignore.
    """
    _original_db_path = listener_mod.db_path  # type: ignore[attr-defined]
    listener_mod.db_path = lambda: daemon.db_path  # type: ignore[attr-defined]

    _original_secret = listener_mod.WebhookHandler.secret
    _original_am_secret = listener_mod.WebhookHandler.am_secret
    listener_mod.WebhookHandler.secret = secret
    listener_mod.WebhookHandler.am_secret = None

    server = HTTPServer((_LISTENER_HOST, 0), listener_mod.WebhookHandler)
    listener_port: int = server.server_address[1]
    listener_thread = threading.Thread(target=server.serve_forever, daemon=True)
    listener_thread.start()

    try:
        yield listener_port
    finally:
        server.shutdown()
        server.server_close()
        listener_thread.join(timeout=2.0)
        listener_mod.WebhookHandler.secret = _original_secret
        listener_mod.WebhookHandler.am_secret = _original_am_secret
        listener_mod.db_path = _original_db_path  # type: ignore[attr-defined]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Measure TTFAE for the GitHub webhook source.",
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
        help="open-loop rate in Hz (default: 100.0)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help=("path to write the result JSON (default: benchmarks/results/ttfae_github_<host>_<ts>.json)"),
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

    secret = _make_secret()

    # Install the monkey-patch before any server starts so that even if
    # the first request arrives before the bench loop body runs, the
    # capture is already in place.
    _orig_do_post = listener_mod.WebhookHandler.do_POST

    def _patched_do_post(self: listener_mod.WebhookHandler) -> None:
        delivery_id = self.headers.get("X-GitHub-Delivery", "")
        if delivery_id:
            capture_t0(delivery_id)
        return _orig_do_post(self)

    listener_mod.WebhookHandler.do_POST = _patched_do_post  # type: ignore[method-assign]

    started_at_ns = time.time_ns()
    hdr_main = HdrRecorder()
    hdr_gc_off: HdrRecorder | None = None if args.no_gc_off else HdrRecorder()

    # Outer try/finally: restore the monkey-patch unconditionally even if
    # the daemon context or listener setup raises.
    try:
        with (
            tempfile.TemporaryDirectory(prefix="waitbus-bench-gh-") as tmp_str,
            daemon_context(Path(tmp_str)) as daemon,
            _listener_context(daemon, secret) as listener_port,
        ):
            subscriber = open_subscriber(socket_path=str(daemon.broadcast_socket_path))
            try:
                print(
                    f"[{_BENCH_NAME}] listener on port {listener_port}",
                    file=sys.stderr,
                )
                print(
                    f"[{_BENCH_NAME}] gc-on",
                    file=sys.stderr,
                )
                _run_loop(
                    listener_port=listener_port,
                    secret=secret,
                    sub_sock=subscriber.sock,
                    n=n,
                    warmup=warmup,
                    rate_hz=rate_hz,
                    hdr=hdr_main,
                )

                if hdr_gc_off is not None:
                    print(
                        f"[{_BENCH_NAME}] gc-off",
                        file=sys.stderr,
                    )
                    with gc_disabled():
                        _run_loop(
                            listener_port=listener_port,
                            secret=secret,
                            sub_sock=subscriber.sock,
                            n=n,
                            warmup=warmup,
                            rate_hz=rate_hz,
                            hdr=hdr_gc_off,
                        )
            finally:
                subscriber.sock.close()

    finally:
        listener_mod.WebhookHandler.do_POST = _orig_do_post  # type: ignore[method-assign]

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
            "scope_note": ("t=0 is listener entry (WebhookHandler.do_POST), not the GitHub-to-listener network leg"),
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
