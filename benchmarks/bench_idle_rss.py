"""Idle resident-set-size comparison: waitbus broadcast daemon vs pinned nats-server.

What is measured
----------------
- **waitbus**: ``waitbus broadcast serve`` spawned as a subprocess against a
  tmp ``WAITBUS_STATE_DIR`` + ``WAITBUS_RUNTIME_DIR``. No events
  are emitted; no subscribers connect. The daemon sits idle for 60 s.
  Every 5 s the bench reads ``/proc/<PID>/status`` ``VmRSS``. The last
  10 samples (post-startup-spike window) are recorded.
- **nats-server**: ``docker run --rm -d <pinned-sha256> -m 8222`` with
  JetStream off (the default ``nats-server`` invocation matches waitbus's
  in-memory broadcast scope; turning JetStream on would compare an
  apples-to-oranges feature set). 60 s idle; ``docker stats --no-stream``
  every 5 s reads the container's RSS. Last 10 samples recorded.

The pinned digest comes from ``benchmarks/baselines/pinned_images.json``
populated by ``benchmarks/setup/pin_nats_image.sh``. If that file is
absent the bench exits with a remediation message naming the setup
script. Pinning the image guarantees a byte-reproducible reference: a
future operator (or a CI runner six months later) re-measures against
the exact same nats-server binary, not whatever ``nats:2.10-alpine``
floats to at that moment.

HdrRecorder for an RSS bench
----------------------------
The harness's :class:`HdrRecorder` is built for nanosecond latencies but
nothing in its internals cares about units -- it stores positive
integers and computes Wilson-rank percentiles. Recording RSS bytes
gives us median + p99 of the post-spike steady-state window for free,
and the existing ``--check-regression`` gate (>25% jump in p99)
transfers cleanly: a 25% RSS regression on the idle daemon is a real
operational signal.

The schema's field is named ``value_ns`` for the latency benches, but
the JSON consumers (CI gate, baseline-comparison tools) treat it as
opaque. The bench docstring + the ``extra.unit`` field disambiguate.

What is in ``extra``
--------------------
- ``unit``: ``"bytes"`` (vs ``"nanoseconds"`` for every other bench).
- ``nats_rss_samples_bytes``: list of 10 post-spike RSS samples from
  nats-server, plus ``nats_rss_median_bytes`` and ``nats_rss_p99_bytes``.
- ``nats_pinned_digest``: the ``image@sha256:...`` reference used.
- ``waitbus_pid``: PID of the subprocess sampled (for forensic
  reproducibility if a sample looks odd).
- ``warmup_window_sec``: seconds of startup-spike samples discarded.

Sample posture
--------------
12 samples per side (2 warmup + 10 measurement), 5 s apart. Wall-clock
~140 s total (60 s waitbus idle + 60 s nats-server idle + ~20 s
setup/teardown). ``--smoke`` shrinks each side's idle window to 15 s
(2 warmup + 1 measurement sample) for a ~30-s smoke; smoke mode does
not write a result baseline since 1-sample percentiles are meaningless.

Linux-only
----------
``/proc/<PID>/status`` is Linux-specific. The bench exits with a clear
message on macOS rather than silently producing meaningless numbers.

Invocation
----------
::

    # One-time setup (after a fresh clone or to refresh the nats pin):
    bash benchmarks/setup/pin_nats_image.sh

    # Smoke (~30 s)
    uv run python -m benchmarks.bench_idle_rss --smoke

    # Production baseline (~140 s)
    taskset -c 2,3 uv run python -m benchmarks.bench_idle_rss \\
        --output benchmarks/baselines/idle_rss.json

    # CI regression gate
    uv run python -m benchmarks.bench_idle_rss --check-regression
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import Final

from ._harness import (
    HdrRecorder,
    check_regression,
    collect_result,
    environment_report,
    nats_pinned_image,
    resolve_output_path,
    spawn_waitbus_daemon,
    wait_for_socket,
    write_result,
)

_BENCH_NAME = "idle_rss"
_DEFAULT_IDLE_SEC = 60.0
_DEFAULT_SAMPLE_INTERVAL_SEC = 5.0
_DEFAULT_WARMUP_SAMPLES = 2
_DEFAULT_MEASURED_SAMPLES = 10
_SMOKE_IDLE_SEC = 15.0
_SMOKE_WARMUP_SAMPLES = 2
_SMOKE_MEASURED_SAMPLES = 1
_BASELINE_PATH = Path(__file__).resolve().parent / "baselines" / f"{_BENCH_NAME}.json"
_RESULTS_DIR = Path(__file__).resolve().parent / "results"

_DOCKER_STATS_TIMEOUT_SEC: Final[float] = 15.0
"""Timeout for a single ``docker stats --no-stream`` call (seconds).

One-shot read; 15 s is generous relative to the typical sub-second
loopback latency but protects against a hung Docker daemon without
blocking the sample loop indefinitely.
"""

_DOCKER_RUN_TIMEOUT_SEC: Final[float] = 30.0
"""Timeout for ``docker run --rm -d <image>`` to return a container id.

30 s covers a slow image-pull on first run; on a primed host the call
typically completes in under 1 s.
"""

_DOCKER_STOP_TIMEOUT_SEC: Final[float] = 15.0
"""Timeout for ``docker stop <container>`` during teardown.

The nats container's SIGTERM handler shuts down in under 1 s; 15 s
gives headroom for a momentarily loaded Docker daemon without letting
a teardown race block the bench result path.
"""

_NATS_TERM_TIMEOUT_SEC: Final[float] = 5.0
"""Timeout passed to ``proc.wait()`` after SIGTERM to the waitbus
broadcast daemon.

5 s is sufficient for a clean SIGTERM handler; on expiry the bench
escalates to SIGKILL and calls ``proc.wait()`` a second time with the
same budget.
"""

_BROADCAST_SOCKET_WAIT_SEC: Final[float] = 15.0
"""Maximum time to wait for the broadcast daemon's AF_UNIX socket to
appear after spawn.

The daemon typically starts in under 500 ms on a warm filesystem;
15 s guards against a stall in an I/O-saturated CI environment.
"""

_P99_FRACTION: Final[float] = 0.99
"""Percentile fraction used to compute the p99 RSS value for both the
waitbus and nats sample paths.

Kept as a named constant so a future change (e.g. tightening to p95)
is a one-line edit rather than a search-and-replace across multiple
call sites.
"""


def _read_vmrss_bytes(pid: int) -> int:
    """Return ``VmRSS`` of ``pid`` in bytes, parsed from ``/proc/<pid>/status``.

    ``VmRSS`` is reported as ``<N> kB`` in the status file; the cast to
    bytes is done here so callers see one consistent unit across the
    waitbus and nats sample paths.
    """
    status_path = Path(f"/proc/{pid}/status")
    text = status_path.read_text(encoding="utf-8")
    for line in text.splitlines():
        if line.startswith("VmRSS:"):
            # Format: ``VmRSS:\t   42068 kB``
            parts = line.split()
            kb = int(parts[1])
            return kb * 1024
    raise RuntimeError(f"VmRSS not found in {status_path}")


def _read_container_rss_bytes(container_id: str) -> int:
    """Return container RSS in bytes via ``docker stats --no-stream``.

    The ``MEM USAGE / LIMIT`` column reports the cgroup memory.usage
    counter which on a fresh container is dominated by RSS. Using
    ``--no-stream`` forces a one-shot read; ``--format '{{.MemUsage}}'``
    keeps parsing trivial.
    """
    out = subprocess.check_output(
        ["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", container_id],
        text=True,
        timeout=_DOCKER_STATS_TIMEOUT_SEC,
    ).strip()
    # Format: ``42.13MiB / 7.674GiB`` -- the left side is what we want.
    used_str = out.split("/")[0].strip()
    return _parse_docker_size(used_str)


def _parse_docker_size(s: str) -> int:
    """Parse a docker-stats human-readable size (e.g. ``42.13MiB``) to bytes.

    docker uses IEC binary prefixes (KiB/MiB/GiB) for the cgroup memory
    counter. Other prefixes (B, kB without ``i``) appear for some
    network/IO columns; we cover the four that mem-usage emits in
    practice and raise on anything unexpected so a future docker output
    change surfaces rather than silently mis-parses.
    """
    suffixes = {
        "B": 1,
        "KiB": 1024,
        "MiB": 1024 * 1024,
        "GiB": 1024 * 1024 * 1024,
    }
    for suffix in ("GiB", "MiB", "KiB", "B"):
        if s.endswith(suffix):
            num = float(s[: -len(suffix)])
            return int(num * suffixes[suffix])
    raise RuntimeError(f"unrecognised docker-stats size: {s!r}")


def _sample_loop(
    label: str,
    *,
    read_fn: Callable[[], int],
    warmup_samples: int,
    measured_samples: int,
    sample_interval_sec: float,
) -> list[int]:
    """Sample ``read_fn`` ``warmup_samples + measured_samples`` times at ``sample_interval_sec`` cadence.

    Returns the measured samples (warmup discarded). The function name
    ``read_fn`` and the bytes returned are RSS-shaped; the same shape
    drives both the waitbus and nats sample paths.
    """
    samples: list[int] = []
    total = warmup_samples + measured_samples
    for i in range(total):
        # Sleep first so the very first read happens after the startup
        # spike has had time to settle. The bench's warmup-discard
        # window covers any residual spike from samples 0 and 1.
        time.sleep(sample_interval_sec)
        rss = read_fn()
        if i >= warmup_samples:
            samples.append(rss)
        print(
            f"[{_BENCH_NAME}]   {label} sample {i + 1}/{total}: rss={rss / (1024 * 1024):.2f} MiB"
            + ("" if i >= warmup_samples else " (warmup)"),
            file=sys.stderr,
        )
    return samples


def _stop_subprocess(proc: subprocess.Popen[bytes]) -> None:
    """Send SIGTERM, wait, escalate to SIGKILL if needed.

    The broadcast daemon's SIGTERM handler shuts down cleanly. The
    SIGKILL escalation defends against a future regression where
    shutdown hangs; in that case we'd rather end the bench than have
    a stray subprocess linger between runs.
    """
    proc.terminate()
    try:
        proc.wait(timeout=_NATS_TERM_TIMEOUT_SEC)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=_NATS_TERM_TIMEOUT_SEC)


def _run_nats_container(image_ref: str) -> str:
    """Run nats-server in a detached container and return the container ID.

    ``-m 8222`` enables the http-monitoring port (waitbus has no
    equivalent; matching the typical operator-facing invocation makes
    the RSS comparison representative). JetStream is not enabled --
    nats-server's default mode is in-memory pub-sub, which matches the
    waitbus broadcast scope. Enabling JetStream would compare an
    apples-to-oranges feature set; ``BENCHMARKING.md`` documents this.
    """
    cid = subprocess.check_output(
        ["docker", "run", "--rm", "-d", image_ref, "-m", "8222"],
        text=True,
        timeout=_DOCKER_RUN_TIMEOUT_SEC,
    ).strip()
    return cid


def _stop_container(container_id: str) -> None:
    """Stop the nats container; ignore stop-failures (``--rm`` cleans up)."""
    subprocess.run(
        ["docker", "stop", container_id],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=_DOCKER_STOP_TIMEOUT_SEC,
        check=False,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Idle RSS comparison: waitbus broadcast daemon vs pinned nats-server.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--idle-sec",
        type=float,
        default=_DEFAULT_IDLE_SEC,
        help="seconds to keep each daemon idle before sampling completes (default: 60).",
    )
    parser.add_argument(
        "--sample-interval",
        type=float,
        default=_DEFAULT_SAMPLE_INTERVAL_SEC,
        help="seconds between RSS samples (default: 5).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="path to write the result JSON (default: benchmarks/results/idle_rss_<host>_<ts>.json).",
    )
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="quick run: 15-s idle window, 1 measured sample per side.",
    )
    parser.add_argument(
        "--check-regression",
        action="store_true",
        help=(
            "after the run, compare p99 (gc-enabled) RSS against "
            f"{_BASELINE_PATH.relative_to(_BASELINE_PATH.parent.parent)}; "
            "exit non-zero on >25% regression."
        ),
    )
    parser.add_argument(
        "--skip-nats",
        action="store_true",
        help="skip the nats-server comparison (e.g. when docker is unavailable). Records waitbus-only.",
    )
    args = parser.parse_args(argv)

    if sys.platform != "linux":
        print(
            f"[{_BENCH_NAME}] error: this bench is Linux-only (reads /proc/<pid>/status). Skipping.",
            file=sys.stderr,
        )
        return 2

    if args.smoke:
        idle_sec = _SMOKE_IDLE_SEC
        warmup_samples = _SMOKE_WARMUP_SAMPLES
        measured_samples = _SMOKE_MEASURED_SAMPLES
    else:
        idle_sec = args.idle_sec
        warmup_samples = _DEFAULT_WARMUP_SAMPLES
        measured_samples = _DEFAULT_MEASURED_SAMPLES

    sample_interval = args.sample_interval
    total_samples = warmup_samples + measured_samples
    expected_window_sec = total_samples * sample_interval
    if expected_window_sec > idle_sec + sample_interval:
        # Defensive: if the operator passes --idle-sec smaller than the
        # sampling schedule needs, surface the inconsistency rather than
        # silently producing fewer samples than the schema records.
        print(
            f"[{_BENCH_NAME}] error: idle-sec={idle_sec}s too small for "
            f"{total_samples} samples at {sample_interval}s interval "
            f"(needs >= {total_samples * sample_interval}s).",
            file=sys.stderr,
        )
        return 2

    env = environment_report()
    print(
        f"[{_BENCH_NAME}] idle_sec={idle_sec} sample_interval={sample_interval}s "
        f"warmup={warmup_samples} measured={measured_samples}",
        file=sys.stderr,
    )

    started_at_ns = time.time_ns()

    # ----------------- waitbus side -----------------
    with tempfile.TemporaryDirectory(prefix="waitbus-bench-idle-rss-") as tmp_str:
        tmp_dir = Path(tmp_str)
        state_dir = tmp_dir / "state"
        runtime_dir = tmp_dir / "runtime"
        state_dir.mkdir()
        runtime_dir.mkdir()

        print(f"[{_BENCH_NAME}] waitbus-daemon", file=sys.stderr)
        proc = spawn_waitbus_daemon(state_dir, runtime_dir)
        try:
            wait_for_socket(runtime_dir / "broadcast.sock", timeout_seconds=_BROADCAST_SOCKET_WAIT_SEC)
            waitbus_pid = proc.pid
            waitbus_samples = _sample_loop(
                "waitbus",
                read_fn=lambda: _read_vmrss_bytes(waitbus_pid),
                warmup_samples=warmup_samples,
                measured_samples=measured_samples,
                sample_interval_sec=sample_interval,
            )
        finally:
            _stop_subprocess(proc)

    # ----------------- nats side -----------------
    nats_samples: list[int] = []
    nats_digest: str | None = None
    if not args.skip_nats:
        if shutil.which("docker") is None:
            print(
                f"[{_BENCH_NAME}] docker not on PATH; recording waitbus-only "
                "(re-run with docker available for the nats-server comparison).",
                file=sys.stderr,
            )
        else:
            try:
                nats_digest = nats_pinned_image()
            except FileNotFoundError as exc:
                print(f"[{_BENCH_NAME}] {exc}", file=sys.stderr)
                return 2
            print(
                f"[{_BENCH_NAME}] nats-server (pinned: {nats_digest})",
                file=sys.stderr,
            )
            cid = _run_nats_container(nats_digest)
            try:
                nats_samples = _sample_loop(
                    "nats",
                    read_fn=lambda: _read_container_rss_bytes(cid),
                    warmup_samples=warmup_samples,
                    measured_samples=measured_samples,
                    sample_interval_sec=sample_interval,
                )
            finally:
                _stop_container(cid)

    ended_at_ns = time.time_ns()

    # Record waitbus samples into HdrRecorder. We treat ``value_ns`` as a
    # unit-agnostic positive-integer field; the docstring and the
    # ``extra.unit`` flag disambiguate. The harness's percentile +
    # regression machinery works because Wilson-rank percentiles are
    # unit-blind.
    hdr_main = HdrRecorder()
    for sample in waitbus_samples:
        hdr_main.record(sample)

    nats_median_bytes: int | None = None
    nats_p99_bytes: int | None = None
    if nats_samples:
        # Match the waitbus side's percentile shape via a second HdrRecorder
        # for the nats samples; this keeps the percentile estimator
        # consistent and lets the bench report nats numbers in the same
        # form (median + p99) without hand-rolling a sort.
        hdr_nats = HdrRecorder()
        for sample in nats_samples:
            hdr_nats.record(sample)
        nats_median_bytes = int(hdr_nats.percentile_with_ci(0.50)["value_ns"])
        nats_p99_bytes = int(hdr_nats.percentile_with_ci(_P99_FRACTION)["value_ns"])

    extra: dict[str, object] = {
        "smoke": args.smoke,
        "unit": "bytes",
        "warmup_window_sec": warmup_samples * sample_interval,
        "waitbus_samples_bytes": waitbus_samples,
        "nats_samples_bytes": nats_samples,
        "nats_pinned_digest": nats_digest,
    }
    if nats_median_bytes is not None:
        extra["nats_rss_median_bytes"] = nats_median_bytes
        extra["nats_rss_p99_bytes"] = nats_p99_bytes

    # ``rate_hz`` is meaningless for an RSS bench; record the sample
    # cadence so the field is not silently zero and a downstream
    # reader can distinguish "I forgot to set this" from "this is the
    # cadence".
    result = collect_result(
        bench_name=_BENCH_NAME,
        started_at_ns=started_at_ns,
        ended_at_ns=ended_at_ns,
        n_warmup_discarded=warmup_samples,
        rate_hz=1.0 / sample_interval,
        hdr_main=hdr_main,
        hdr_gc_off=None,
        environment=env,
        extra=extra,
    )

    output_path = resolve_output_path(_BENCH_NAME, _RESULTS_DIR, args.output, env)
    write_result(result, output_path)
    print(f"[{_BENCH_NAME}] wrote {output_path}", file=sys.stderr)

    waitbus_p50 = result.percentiles_gc_enabled["p50"].value_ns
    waitbus_p99 = result.percentiles_gc_enabled["p99"].value_ns
    print(
        f"[{_BENCH_NAME}] waitbus RSS  p50={waitbus_p50 / (1024 * 1024):7.2f} MiB  "
        f"p99={waitbus_p99 / (1024 * 1024):7.2f} MiB  (n={result.n_samples})",
        file=sys.stderr,
    )
    if nats_median_bytes is not None and nats_p99_bytes is not None:
        print(
            f"[{_BENCH_NAME}] nats  RSS  p50={nats_median_bytes / (1024 * 1024):7.2f} MiB  "
            f"p99={nats_p99_bytes / (1024 * 1024):7.2f} MiB  (n={len(nats_samples)})  "
            f"image={nats_digest}",
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
