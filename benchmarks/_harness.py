"""Shared harness for the waitbus benchmark suite.

Provides the load-bearing primitives every bench in this directory
must use:

- :class:`OpenLoopScheduler` -- the Coordinated-Omission-aware
  scheduler (Gil Tene). Yields ``t_intended_ns`` at a fixed rate;
  callers record ``t_response - t_intended`` (NOT
  ``t_response - t_actual_dispatch``). Closed-loop bench scripts that
  iterate ``for _ in range(n): send(); recv()`` are forbidden.
- :class:`HdrRecorder` -- a thin wrapper around ``hdrh.HdrHistogram``
  with the project's canonical recording posture (precision = 3
  significant digits; range 1 ns - 60 s) and a Wilson-rank-binomial
  CI on every reported percentile's order-statistic rank position.
- :func:`environment_report` -- collects CPU governor, ASLR setting,
  taskset mask, hostname, Python and waitbus version; warns loudly
  (but non-fatally) if reproducibility-relevant knobs are not set.
- :func:`gc_disabled` -- context manager. Every bench must report
  both a gc-enabled run AND a gc-disabled companion run.
- :func:`daemon_context` -- spins up the broadcast daemon against a
  tmp DB + sockets in a background asyncio loop running inside a
  daemon thread. Mirrors ``tests/conftest.py::running_daemon`` (and
  ``broadcast_paths``) but does NOT depend on pytest. Bench scripts
  invoke it as a regular ``with`` block.
- :class:`BenchResult` / :func:`write_result` / :func:`check_regression` -
  the canonical JSON output shape and the >25%-regression gate.

The harness is callable from outside pytest. The
soak orchestrator reuses :class:`HdrRecorder` and
:func:`environment_report`.

Clock posture
-------------

Cross-process timing uses :func:`time.time_ns` (wall clock). NTP
adjustments over the 5000-sample bench window are negligible relative
to the latencies being measured (single-digit ms at worst); the
trade-off is acceptable for cross-process comparability between the
event producer (which records t=0) and the subscriber thread (which
records t=end). Within-process timing uses :func:`time.monotonic_ns`.

Reproducibility recipe
----------------------

Per ``BENCHMARKING.md``: pin CPU 2-3 via ``taskset -c 2,3``; set
governor to ``performance`` via ``sudo cpupower frequency-set -g
performance``; disable ASLR via ``echo 0 | sudo tee
/proc/sys/kernel/randomize_va_space``. :func:`environment_report`
warns at startup when any of these is missing; the result JSON
records the warnings so reviewers see which knobs were and were not
set.

Methodology document
--------------------

All Coordinated Omission citations, sample-size derivations, and the
"no substantive change in 14 days before a release" rule lives in
``benchmarks/BENCHMARKING.md``. This file is the executable shape;
that file is the methodology.
"""

from __future__ import annotations

import asyncio
import contextlib
import gc
import gzip
import math
import os
import platform
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Generator, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any, Final, cast

import msgspec

from waitbus import _config, _db, broadcast

# 1.96 is the standard 95% normal-distribution quantile; we use it in
# the Wilson-score formula for binomial proportion CIs (see
# https://en.wikipedia.org/wiki/Binomial_proportion_confidence_interval).
# Bench scripts that want a different confidence level construct their
# own Wilson call with the appropriate z; the harness default is 95%.
_Z_95: float = 1.959963984540054

# HdrHistogram parameters: 1 ns floor, 60 s ceiling, 3 significant
# digits. Matches the JVM HdrHistogram defaults the wider ecosystem
# uses, and the 1-ns floor is required because some bench paths
# (notify-to-wake in-process micro) can hit <1 us p50.
_HDR_MIN_NS: int = 1
_HDR_MAX_NS: int = 60_000_000_000
_HDR_SIG_DIGITS: int = 3

# ---------------------------------------------------------------------------
# Named timing / threshold constants (promotes every bare numeric literal
# that appears at a call site to a single named, documented source of truth).
# ---------------------------------------------------------------------------

_LATE_SAMPLE_MULTIPLIER: Final[int] = 10
"""Multiplier applied to the scheduler interval to define the "very late"
dispatch threshold.  A sample is considered very late when the actual dispatch
timestamp trails the intended timestamp by more than
``_LATE_SAMPLE_MULTIPLIER * interval_ns``.  Bench scripts MAY log a warning
when this threshold is crossed but must NOT discard the sample -- the purpose
of the open-loop scheduler is to record the full tail, not to mask overload.
The factor of 10 matches the wrk2 / HdrHistogram ecosystem convention for
"meaningful overload" rather than normal scheduling jitter (which stays within
1-2x)."""

_DEFAULT_SOCKET_WAIT_SEC: Final[float] = 5.0
"""Default maximum wall-clock seconds that :func:`wait_for_socket` will poll
before raising :class:`RuntimeError`.  Five seconds is generous for a daemon
whose socket typically appears within a few hundred milliseconds on an idle
host; the ceiling absorbs heavy CI load without making a genuine bind-failure
appear to hang indefinitely."""

_SOCKET_POLL_INTERVAL_SEC: Final[float] = 0.02
"""Sleep interval (seconds) between successive filesystem-existence checks
inside :func:`wait_for_socket`.  Twenty milliseconds keeps CPU usage
negligible while keeping the observed startup latency within one polling
cycle (~20 ms) of the true bind time."""

_DEFAULT_HEARTBEAT_SEC: Final[float] = 3600.0
"""Default ``WAITBUS_HEARTBEAT_SEC`` override injected into the daemon when
running under :func:`daemon_context`.  One hour ensures the broadcaster does
NOT emit ``daemon_heartbeat`` frames inside the bench's sample window: a
heartbeat frame arriving between a bench's emit and its subscriber recv would
be returned by ``sync_read_frame`` before the actual event frame, silently
spiking the measured latency to one heartbeat interval.  Tests use a short
heartbeat on purpose (to exercise the code path); benches must NOT inherit
that."""

_DAEMON_HANDSHAKE_TIMEOUT_SEC: Final[float] = 5.0
"""Timeout in seconds used at three points during daemon startup inside
:func:`daemon_context`: waiting for the background thread's ``started_event``,
waiting for the ``daemon.stop()`` coroutine future, and joining the background
thread on teardown.  Five seconds is the same headroom used for socket-wait so
a slow CI host receives consistent treatment across all startup/teardown
gates."""

_DEFAULT_REGRESSION_THRESHOLD: Final[float] = 0.25
"""Fractional p99 degradation budget for :func:`check_regression`.  A current
p99 more than 25 % above the committed baseline triggers a regression failure.
The figure matches the >25 % rule documented in ``BENCHMARKING.md``; it is
intentionally coarse so normal run-to-run variance and minor environmental
differences do not generate false positives in CI."""

_DAEMON_TERM_TIMEOUT_SEC: Final[float] = 10.0
"""Seconds :func:`terminate_daemon_group` waits after SIGTERM before
escalating to SIGKILL.  Ten seconds is generous enough for the daemon to
complete any in-flight SQLite write and close its sockets cleanly; it is
enforced by ``proc.wait(timeout=...)`` so the harness never blocks the
teardown path indefinitely."""

_DAEMON_KILL_TIMEOUT_SEC: Final[float] = 5.0
"""Seconds :func:`terminate_daemon_group` waits after SIGKILL before giving
up.  A process that does not die within five seconds of SIGKILL indicates a
kernel-level problem (uninterruptible sleep, zombie) rather than a normal
slow shutdown; the harness surfaces the ``TimeoutExpired`` so the bench fails
visibly rather than hanging."""


# ---------------------------------------------------------------------------
# Open-loop scheduler (Coordinated-Omission-aware)
# ---------------------------------------------------------------------------


class OpenLoopScheduler:
    """Yield t_intended_ns timestamps at a fixed rate.

    The Gil Tene fix for Coordinated Omission. Each ``next()`` call
    returns ``t0 + i / rate_hz`` regardless of how long previous
    iterations actually took. If iteration N was slow, iteration N+1
    is NOT delayed -- its ``t_intended`` was already set at scheduler
    construction. The caller measures ``t_response - t_intended``;
    when the system is overloaded the scheduler does NOT shrink the
    workload to hide it (which is exactly what closed-loop
    ``for _ in range(N): send(); recv()`` would do).

    The reference is ``giltene/wrk2`` (the canonical open-loop
    benchmark tool) and the "How NOT to Measure Latency" Strange Loop
    talk. See ``BENCHMARKING.md`` for the full citation block.

    Usage::

        sched = OpenLoopScheduler(rate_hz=100.0, n=5000)
        for t_intended_ns in sched:
            now = time.monotonic_ns()
            if now < t_intended_ns:
                time.sleep((t_intended_ns - now) / 1e9)
            elif now - t_intended_ns > sched.late_threshold_ns:
                # We are catching up from an overload; do NOT discard
                # the sample. Record it; the tail will reflect reality.
                pass
            t0 = time.time_ns()
            ...  # the bench's actual work
            t_end = time.time_ns()
            hdr.record(t_end - t0)
    """

    def __init__(self, rate_hz: float, n: int) -> None:
        if rate_hz <= 0:
            raise ValueError(f"rate_hz must be > 0, got {rate_hz}")
        if n <= 0:
            raise ValueError(f"n must be > 0, got {n}")
        self._rate_hz = rate_hz
        self._n = n
        # interval is the floating-point ideal; we accumulate the
        # exact intended time as an int (ns) to avoid drift over long
        # runs.
        self._interval_ns = round(1e9 / rate_hz)
        self._t0_ns: int | None = None
        # A sample is "very late" when t_actual_dispatch is more than
        # 10x the interval behind t_intended. Bench scripts MAY use
        # this to log a warning (not to discard samples).
        self.late_threshold_ns = _LATE_SAMPLE_MULTIPLIER * self._interval_ns

    @property
    def rate_hz(self) -> float:
        return self._rate_hz

    @property
    def n(self) -> int:
        return self._n

    @property
    def interval_ns(self) -> int:
        return self._interval_ns

    def __iter__(self) -> Iterator[int]:
        # Anchor on monotonic_ns at first __iter__ call so the bench's
        # iteration #0 starts immediately, not delayed by warmup work.
        self._t0_ns = time.monotonic_ns()
        for i in range(self._n):
            assert self._t0_ns is not None  # for mypy
            yield self._t0_ns + i * self._interval_ns


# ---------------------------------------------------------------------------
# Wilson Score rank-binomial CI on percentile order statistics
# ---------------------------------------------------------------------------


def wilson_rank_ci(n: int, p: float, *, z: float = _Z_95) -> tuple[float, float]:
    """Wilson Score CI on the rank-position of the p-th percentile.

    Returns a pair ``(p_low, p_high)`` of effective percentiles in
    [0, 1]. The caller looks up the latency value at each via
    :meth:`HdrRecorder.value_at_percentile_fraction`. For example, for
    p=0.99 and n=5000 the CI typically spans roughly (0.987, 0.992),
    which maps to (ranks 4935, 4960) in the sorted sample. The
    corresponding latency range is reported alongside the point
    estimate.

    Wilson Score is preferred over Wald for proportions close to 1
    (Wikipedia, ``Binomial_proportion_confidence_interval``); the
    canonical p99 / p99.9 cases live in exactly that regime, so the
    bench reports Wilson by default.

    Args:
        n: sample size.
        p: target percentile as a fraction in (0, 1).
        z: normal-distribution quantile for the desired confidence.
            Default 1.96 (95%).

    Returns:
        ``(p_low, p_high)`` -- the lower and upper Wilson Score bounds
        on the percentile proportion. Both are clipped to [0, 1].
    """
    if not (0 < p < 1):
        raise ValueError(f"p must be in (0,1), got {p}")
    if n <= 0:
        raise ValueError(f"n must be > 0, got {n}")
    z2 = z * z
    denom = 1.0 + z2 / n
    centre = (p + z2 / (2.0 * n)) / denom
    half_width = (z * math.sqrt(p * (1.0 - p) / n + z2 / (4.0 * n * n))) / denom
    return max(0.0, centre - half_width), min(1.0, centre + half_width)


# ---------------------------------------------------------------------------
# HdrHistogram wrapper
# ---------------------------------------------------------------------------


class HdrRecorder:
    """Record latencies and report percentiles with Wilson-rank CIs.

    Thin wrapper around ``hdrh.histogram.HdrHistogram``. The default
    range (1 ns - 60 s) and precision (3 sig digits) match the
    JVM-ecosystem HdrHistogram defaults and are sufficient for every
    waitbus bench: notify-to-wake hits low microseconds; TTFAE under
    real load can stretch to seconds; nothing in the bench suite
    rationally exceeds 60 s.

    The ``hdrh`` dependency is in the ``bench`` optional-deps group;
    a clean install of waitbus does not pull it. Bench scripts import
    this module only when running benches.
    """

    def __init__(
        self,
        *,
        min_ns: int = _HDR_MIN_NS,
        max_ns: int = _HDR_MAX_NS,
        sig_digits: int = _HDR_SIG_DIGITS,
    ) -> None:
        # Lazy import: the `hdrhistogram` PyPI package (which installs
        # the `hdrh` module) lives in the bench optional-deps group,
        # so importing at module top would force every type-check run
        # to require it. Lazy import limits the cost to bench
        # invocations.
        from hdrh.histogram import HdrHistogram

        self._h = HdrHistogram(min_ns, max_ns, sig_digits)
        self._min_ns = min_ns
        self._max_ns = max_ns
        self._sig_digits = sig_digits
        # Clamp telemetry. A nonzero ``clamp_high`` means at least one
        # sample exceeded the recorder's ceiling and was recorded at
        # the ceiling instead -- the reported p99 in that case is a
        # LOWER BOUND on the true p99, not an exact value. The result
        # JSON surfaces these so reviewers can see whether clamping
        # was active.
        self.clamp_low: int = 0
        self.clamp_high: int = 0

    def record(self, latency_ns: int) -> None:
        """Record one observation. Out-of-range values are clamped.

        Clamping (rather than rejecting) matches HdrHistogram's
        documented behaviour: a value below the floor is recorded at
        the floor; a value above the ceiling is recorded at the
        ceiling. :attr:`clamp_low` / :attr:`clamp_high` count each
        side independently so :func:`collect_result` can surface them
        in the result JSON; reviewers can then tell whether the
        reported p99 is exact or a lower-bound saturation reading.
        """
        # ``hdrh`` rejects values outside the range; clamp explicitly
        # so a stray timestamp glitch (e.g. NTP step) does not abort
        # a 5000-iteration run. Surface the clamp count rather than
        # silently absorbing it.
        if latency_ns < self._min_ns:
            self.clamp_low += 1
            self._h.record_value(self._min_ns)
        elif latency_ns > self._max_ns:
            self.clamp_high += 1
            self._h.record_value(self._max_ns)
        else:
            self._h.record_value(latency_ns)

    @property
    def count(self) -> int:
        """Total observations recorded (post-clamp)."""
        return int(self._h.get_total_count())

    def encode_b64(self) -> str:
        """Lossless base64 (HdrHistogram V2) encoding of the full distribution.

        Lets any percentile be reconstructed offline from the committed
        result, so the summary percentiles are not the only record of
        the run's shape.
        """
        # ``hdrh`` carries no stubs (ignore_missing_imports), so ``encode()``
        # is typed Any; the cast pins the documented bytes->ascii-str result.
        return cast(str, self._h.encode().decode("ascii"))

    def value_at_percentile_fraction(self, p: float) -> int:
        """Look up the latency at the given percentile fraction (0 < p < 1).

        ``hdrh.get_value_at_percentile`` takes the percentile as a
        number in [0, 100]; this wrapper takes a fraction in [0, 1]
        because that is the natural pair with :func:`wilson_rank_ci`.
        """
        return int(self._h.get_value_at_percentile(100.0 * p))

    def percentile_with_ci(self, p: float, *, z: float = _Z_95) -> dict[str, int | float]:
        """Report ``p``-percentile + Wilson Score CI on its rank.

        The point estimate is the latency at the empirical p-rank.
        The CI is the latency range corresponding to the Wilson Score
        bounds on the percentile's rank position. The bench writes one
        such dict per reported percentile (p50/p90/p99) into the
        result JSON.
        """
        p_low, p_high = wilson_rank_ci(self.count, p, z=z)
        # Edge case: p_low can clip to 0.0 for very small n; we map
        # 0.0 to the smallest non-zero percentile lookup the
        # histogram supports (rank 1, i.e. 1/n).
        p_low_eff = max(p_low, 1.0 / max(self.count, 1))
        p_high_eff = min(p_high, 1.0 - 1.0 / max(self.count, 1))
        return {
            "value_ns": self.value_at_percentile_fraction(p),
            "ci_low_ns": self.value_at_percentile_fraction(p_low_eff),
            "ci_high_ns": self.value_at_percentile_fraction(p_high_eff),
            "ci_z": z,
            "p_low_eff": p_low_eff,
            "p_high_eff": p_high_eff,
        }


# ---------------------------------------------------------------------------
# Environment report
# ---------------------------------------------------------------------------


class EnvironmentReport(msgspec.Struct, frozen=True):
    """Snapshot of reproducibility-relevant environment knobs.

    Captured at bench startup. Written into the result JSON so a
    reviewer can see which knobs were and were not set for a given
    run. The fields with ``None`` mean "not available on this
    platform" (e.g. cpufreq governor is Linux-only).
    """

    hostname: str
    python_version: str
    platform: str
    cpu_model: str | None
    cpu_governor: str | None
    aslr_disabled: bool | None
    taskset_mask: str | None
    waitbus_version: str
    warnings: tuple[str, ...]
    # The daemon process's CPU affinity mask AFTER spawn. ``None`` when
    # the bench does not spawn its own daemon (e.g. a passing-by harness
    # consumer), or when affinity-pinning is disabled (the legacy
    # unpinned dev path on a non-Linux host). When the bench enforces
    # the affinity recipe, this records the EXACT mask the daemon
    # actually inherited so a downstream reader can confirm the
    # orchestrator's intent matched reality. Filled via
    # ``msgspec.structs.replace`` after the daemon Popen returns.
    daemon_taskset_mask: str | None = None
    # NUMA node count + topology descriptor. ``None`` on non-Linux or
    # when ``/sys/devices/system/node`` is absent. On a multi-node
    # host, daemon-on-node-1 + orchestrator-on-node-0 suffers a
    # QPI/UPI round-trip per cross-process emit; the bench prints a
    # warning recommending ``numactl --cpubind=N --membind=N``.
    numa_node_count: int | None = None
    numa_topology: str | None = None


def _read_first_line(path: str) -> str | None:
    """Read the first line of a /proc or /sys file; return None on absence."""
    try:
        with Path(path).open("r", encoding="utf-8") as fh:
            return fh.readline().strip()
    except (OSError, ValueError):
        return None


def _detect_cpu_model() -> str | None:
    """Best-effort CPU model name (Linux /proc/cpuinfo; macOS sysctl)."""
    if sys.platform == "linux":
        try:
            content = Path("/proc/cpuinfo").read_text(encoding="utf-8", errors="replace")
        except OSError:
            return None
        for line in content.splitlines():
            if line.startswith("model name") and ":" in line:
                return line.split(":", 1)[1].strip()
        return None
    if sys.platform == "darwin":
        try:
            out = subprocess.run(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                check=False,
                capture_output=True,
                text=True,
                timeout=2,
            )
            return out.stdout.strip() or None
        except (OSError, subprocess.SubprocessError):
            return None
    return None


def _detect_numa_topology() -> tuple[int | None, str | None]:
    """Probe ``/sys/devices/system/node`` for NUMA node count + topology.

    Returns ``(node_count, topology_str)`` on Linux; ``(None, None)``
    on non-Linux or when the sysfs entry is absent. The topology
    string is a comma-joined list of node directory names (e.g.
    ``"node0,node1"``) for forensic context.
    """
    if sys.platform != "linux":
        return None, None
    node_dir = Path("/sys/devices/system/node")
    if not node_dir.is_dir():
        return None, None
    try:
        nodes = sorted(p.name for p in node_dir.glob("node[0-9]*"))
    except OSError:
        return None, None
    if not nodes:
        return None, None
    return len(nodes), ",".join(nodes)


def _detect_taskset_mask() -> str | None:
    """Read the bench process's CPU affinity mask if Linux exposes it.

    Returns the mask as a hex string (matching ``taskset -p`` output)
    or ``None`` on non-Linux. A return of ``"ff"`` typically indicates
    no CPU pin; the warning machinery flags that.
    """
    if sys.platform != "linux" or not hasattr(os, "sched_getaffinity"):
        return None
    try:
        cpus = sorted(os.sched_getaffinity(0))
    except OSError:
        return None
    return ",".join(str(c) for c in cpus)


def _waitbus_version() -> str:
    """Best-effort waitbus version string (importlib.metadata or ``unknown``)."""
    try:
        from importlib.metadata import PackageNotFoundError, version

        return version("waitbus")
    except (ImportError, PackageNotFoundError):
        return "unknown"


def environment_report() -> EnvironmentReport:
    """Collect environment knobs; warn loudly on missing reproducibility knobs.

    Warnings are printed to stderr at startup. They are NOT fatal --
    the bench still runs -- but they ARE recorded in the result JSON
    so reviewers can see which knobs were not set. Per
    ``BENCHMARKING.md``, the canonical reproducibility recipe is
    ``taskset -c 2,3`` + ``cpupower -g performance`` + ASLR-off;
    runs missing any of those are usable for development but should
    not be the run that gets committed as a baseline.
    """
    warnings: list[str] = []

    cpu_governor = _read_first_line("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
    if cpu_governor is not None and cpu_governor != "performance":
        warnings.append(
            f"cpu governor is {cpu_governor!r}, not 'performance'; "
            "set via `sudo cpupower frequency-set -g performance` for stable measurements."
        )

    aslr_raw = _read_first_line("/proc/sys/kernel/randomize_va_space")
    aslr_disabled: bool | None
    if aslr_raw is None:
        aslr_disabled = None  # macOS / non-Linux: setting unavailable
    else:
        aslr_disabled = aslr_raw == "0"
        if not aslr_disabled:
            warnings.append(
                f"ASLR is enabled (randomize_va_space={aslr_raw!r}); "
                "disable for canonical baselines via "
                "`echo 0 | sudo tee /proc/sys/kernel/randomize_va_space`."
            )

    taskset_mask = _detect_taskset_mask()
    if sys.platform == "linux" and taskset_mask is not None and "," in taskset_mask:
        # If the affinity covers more than 2 CPUs, warn -- the canonical
        # recipe pins to 2 isolated cores.
        cpu_count = len(taskset_mask.split(","))
        if cpu_count > 2:
            warnings.append(
                f"CPU affinity covers {cpu_count} cores; "
                "pin to 2 isolated cores via `taskset -c 2,3 python -m benchmarks.bench_*` "
                "for canonical baselines."
            )

    numa_node_count, numa_topology = _detect_numa_topology()
    if numa_node_count is not None and numa_node_count > 1:
        warnings.append(
            f"Multi-NUMA host detected ({numa_node_count} nodes); "
            "cross-process emits suffer a QPI/UPI round-trip on this layout. "
            "Re-run under `numactl --cpubind=N --membind=N` for a canonical baseline."
        )

    if warnings:
        print("=" * 78, file=sys.stderr)
        print("BENCH WARNING: reproducibility-relevant knobs are not set:", file=sys.stderr)
        for w in warnings:
            print(f"  - {w}", file=sys.stderr)
        print(
            "Results recorded in this state are usable for development; "
            "they should NOT be committed as `benchmarks/baselines/main.json`.",
            file=sys.stderr,
        )
        print("=" * 78, file=sys.stderr)

    return EnvironmentReport(
        hostname=platform.node(),
        python_version=sys.version.split()[0],
        platform=f"{platform.system()} {platform.release()}",
        cpu_model=_detect_cpu_model(),
        cpu_governor=cpu_governor,
        aslr_disabled=aslr_disabled,
        taskset_mask=taskset_mask,
        waitbus_version=_waitbus_version(),
        warnings=tuple(warnings),
        numa_node_count=numa_node_count,
        numa_topology=numa_topology,
    )


# ---------------------------------------------------------------------------
# GC discipline
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def gc_disabled() -> Generator[None, None, None]:
    """Disable the cyclic GC for the duration of the block.

    Bench scripts run their main measurement loop twice: once with
    GC enabled (representative of production) and once with GC
    disabled (algorithmic-cost figure that strips out the rare large
    GC pause). The result JSON carries both numbers under
    ``percentiles_gc_enabled`` and ``percentiles_gc_disabled``.

    Restoration is unconditional even on exception -- a bench that
    crashes mid-loop with GC disabled would otherwise leave the rest
    of the process running without it.
    """
    was_enabled = gc.isenabled()
    gc.disable()
    try:
        yield
    finally:
        if was_enabled:
            gc.enable()


# ---------------------------------------------------------------------------
# Daemon context (parallel of tests/conftest.py::running_daemon)
# ---------------------------------------------------------------------------


@dataclass
class DaemonHandle:
    """Owned references for a running daemon under a bench context.

    ``daemon`` is the live :class:`broadcast.Broadcast` instance.
    ``db_path`` / ``broadcast_socket_path`` / ``doorbell_socket_path``
    are the tmpdir-anchored paths the daemon is bound to. Bench
    scripts construct events with ``db_path=db_path`` and subscribers
    with ``socket_path=str(broadcast_socket_path)``.
    """

    daemon: broadcast.Broadcast
    db_path: Path
    broadcast_socket_path: Path
    doorbell_socket_path: Path
    loop: asyncio.AbstractEventLoop
    thread: threading.Thread
    _restore: list[tuple[Any, str, Any]] = field(default_factory=list)
    _saved_heartbeat: str | None = None


def wait_for_socket(path: Path, *, timeout_seconds: float = _DEFAULT_SOCKET_WAIT_SEC) -> None:
    """Block until ``path`` exists, up to ``timeout_seconds``. Raise on timeout.

    Used by bench scripts and the soak orchestrator to wait for the
    broadcast daemon's AF_UNIX socket to appear before issuing any
    subscriber connections or RSS samples.

    Args:
        path: filesystem path to the socket file.
        timeout_seconds: maximum seconds to wait (default 5.0).

    Raises:
        RuntimeError: the socket did not appear within ``timeout_seconds``.
    """
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if path.exists():
            return
        time.sleep(_SOCKET_POLL_INTERVAL_SEC)
    raise RuntimeError(f"daemon failed to bind socket at {path} within {timeout_seconds}s")


@contextlib.contextmanager
def daemon_context(
    tmp_dir: Path,
    *,
    heartbeat_sec: float = _DEFAULT_HEARTBEAT_SEC,
) -> Generator[DaemonHandle, None, None]:
    """Spin up a broadcast daemon against tmp paths in a background thread.

    This is the non-pytest equivalent of
    ``tests/conftest.py::running_daemon`` + ``broadcast_paths``. The
    daemon runs in its own asyncio event loop on a daemon thread;
    the bench scripts that use it remain synchronous (their socket
    I/O uses the blocking ``sync_read_frame`` from
    :mod:`waitbus._frame`).

    The same three module-level path factories that
    ``broadcast_paths`` patches are patched here, with full
    restoration on exit so a process running several benches in
    sequence stays clean.

    Args:
        tmp_dir: directory to anchor the DB and socket paths in.
            Must already exist; the caller (typically a bench
            script's main) owns its lifecycle.
        heartbeat_sec: override for ``WAITBUS_HEARTBEAT_SEC``.
            Defaults to 3600s (1 hour) so the broadcaster does NOT
            emit ``daemon_heartbeat`` frames inside the bench's
            sample window. A heartbeat frame arriving between a
            bench's emit and its subscriber recv would be returned
            by ``sync_read_frame`` before the actual event frame,
            silently spiking the measured latency to one heartbeat
            interval. ``running_daemon`` in tests uses 0.2s on
            purpose (to exercise heartbeat code paths); benches
            must NOT inherit that.

    Yields:
        A :class:`DaemonHandle` with the daemon, the paths, and the
        background-thread / loop references. The bench should NOT
        manually stop the daemon; the context manager handles
        teardown.
    """
    db_path = tmp_dir / "events.db"
    bs_path = tmp_dir / "broadcast.sock"
    ds_path = tmp_dir / "doorbell.sock"

    # Save originals so the patches do not leak between benches in the
    # same process. ``broadcast_socket`` / ``doorbell_socket`` are
    # imported into both modules from ``_paths`` but not re-exported via
    # ``__all__``, so mypy --strict considers them attr-undefined. The
    # ``# type: ignore[attr-defined]`` follows the same pattern
    # ``tests/conftest.py`` relies on via ``monkeypatch.setattr`` -- a
    # documented runtime contract that the static checker cannot see
    # because Python's import system rebinds the name at module scope.
    restore: list[tuple[Any, str, Any]] = [
        (broadcast, "broadcast_socket", broadcast.broadcast_socket),  # type: ignore[attr-defined]
        (broadcast, "doorbell_socket", broadcast.doorbell_socket),  # type: ignore[attr-defined]
        (_db._doorbell, "doorbell_socket", _db._doorbell.doorbell_socket),  # type: ignore[attr-defined]
    ]
    saved_heartbeat = os.environ.get("WAITBUS_HEARTBEAT_SEC")

    # Apply patches via direct assignment (same pattern as the save).
    broadcast.broadcast_socket = lambda: bs_path  # type: ignore[attr-defined]
    broadcast.doorbell_socket = lambda: ds_path  # type: ignore[attr-defined]
    _db._doorbell.doorbell_socket = lambda: ds_path  # type: ignore[attr-defined]
    os.environ["WAITBUS_HEARTBEAT_SEC"] = str(heartbeat_sec)
    _config._reset_for_test()

    daemon = broadcast.Broadcast(db_path=str(db_path))
    loop = asyncio.new_event_loop()

    # ``Broadcast.run()`` installs SIGINT/SIGTERM handlers via
    # ``loop.add_signal_handler``, which is a main-thread-only API on
    # CPython (it calls ``signal.set_wakeup_fd`` under the hood). The
    # bench runs the daemon on a background thread, so the handler
    # install would raise and leave ``Broadcast.run()`` half-set-up.
    # No-op the API on the thread loop: signal-based shutdown is
    # irrelevant here because :meth:`Broadcast.stop` is invoked
    # explicitly from the main thread via
    # :func:`asyncio.run_coroutine_threadsafe`.
    def _noop_add(*_args: object, **_kwargs: object) -> None:
        return

    def _noop_remove(*_args: object, **_kwargs: object) -> bool:
        # ``remove_signal_handler`` returns bool (True iff a handler
        # was removed) per the asyncio production contract. Return
        # False because the no-op ``add_signal_handler`` never
        # registered one.
        return False

    loop.add_signal_handler = _noop_add  # type: ignore[method-assign]
    loop.remove_signal_handler = _noop_remove  # type: ignore[method-assign]

    started_event = threading.Event()
    task_holder: dict[str, asyncio.Task[int]] = {}
    runner_error: dict[str, BaseException] = {}

    def _runner() -> None:
        try:
            asyncio.set_event_loop(loop)
            task_holder["task"] = loop.create_task(daemon.run())
            started_event.set()
            # Run forever until loop.stop() is scheduled from the main
            # thread; daemon.run() awaiting its internal stop_event will
            # complete first and the loop will then unwind naturally.
            loop.run_forever()
        except BaseException as exc:
            runner_error["exc"] = exc
            started_event.set()  # unblock the main thread so it can surface the error

    thread = threading.Thread(target=_runner, name="waitbus-bench-daemon", daemon=True)
    thread.start()
    if not started_event.wait(timeout=_DAEMON_HANDSHAKE_TIMEOUT_SEC):
        raise RuntimeError("daemon thread did not signal startup within 5s")
    if "exc" in runner_error:
        # Re-raise the original error with context rather than the
        # downstream "socket failed to bind" symptom.
        raise RuntimeError("daemon thread crashed during startup") from runner_error["exc"]
    wait_for_socket(bs_path)

    handle = DaemonHandle(
        daemon=daemon,
        db_path=db_path,
        broadcast_socket_path=bs_path,
        doorbell_socket_path=ds_path,
        loop=loop,
        thread=thread,
        _restore=restore,
        _saved_heartbeat=saved_heartbeat,
    )

    try:
        yield handle
    finally:
        # Stop the daemon via the public stop event, NOT cancellation
        # (the same pattern conftest.py uses for the same reason: a
        # cancelled run() leaves accept tasks half-cleaned, surfacing
        # as PytestUnraisableExceptionWarning the next time anything
        # GCs).
        with contextlib.suppress(Exception):
            future = asyncio.run_coroutine_threadsafe(daemon.stop(), loop)
            future.result(timeout=_DAEMON_HANDSHAKE_TIMEOUT_SEC)
        with contextlib.suppress(Exception):
            loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=_DAEMON_HANDSHAKE_TIMEOUT_SEC)
        with contextlib.suppress(Exception):
            loop.close()
        # Restore patches unconditionally.
        for module, attr, original in restore:
            setattr(module, attr, original)
        if saved_heartbeat is None:
            os.environ.pop("WAITBUS_HEARTBEAT_SEC", None)
        else:
            os.environ["WAITBUS_HEARTBEAT_SEC"] = saved_heartbeat
        _config._reset_for_test()


# ---------------------------------------------------------------------------
# Result JSON shape + regression gate
# ---------------------------------------------------------------------------


class PercentilePoint(msgspec.Struct, frozen=True):
    """One reported percentile with its Wilson-rank CI."""

    value_ns: int
    ci_low_ns: int
    ci_high_ns: int
    ci_z: float
    p_low_eff: float
    p_high_eff: float


class ClampStats(msgspec.Struct, frozen=True):
    """Out-of-range sample counts (low-clamp + high-clamp).

    A nonzero ``high`` count means at least one sample exceeded the
    recorder's ceiling and was recorded at the ceiling instead. In
    that case the reported p99 (and any higher percentile that falls
    in the clamped tail) is a **lower bound** on the true latency,
    not an exact value. Reviewers reading the result JSON should
    treat a high clamp count as a flag to widen the histogram range
    and re-measure.
    """

    low: int
    high: int


class BenchResult(msgspec.Struct, frozen=True):
    """Canonical JSON shape for a bench run.

    ``percentiles_gc_enabled`` and ``percentiles_gc_disabled`` carry
    the same percentile keys (a ``p10``-through-``p99`` grid by
    default; ``p50`` / ``p90`` / ``p99`` are always present). The
    ``--check-regression`` gate compares only ``p99`` of the
    gc-enabled side (representative-of-production).

    ``histogram_b64_gc_enabled`` / ``histogram_b64_gc_disabled`` carry
    the lossless base64 HdrHistogram (V2) encoding of the full
    distribution, so any percentile can be reconstructed offline from
    the committed result -- the summary percentiles above are a
    convenience, not the only record. Optional (``None`` on results
    captured before the field existed).

    ``clamp_stats_gc_enabled`` and ``clamp_stats_gc_disabled`` report
    how many samples were clamped to the recorder's floor / ceiling.
    A non-zero ``high`` in either side invalidates the upper-tail
    percentile readings (the actual values exceeded the ceiling); the
    regression gate does NOT automatically fail on clamps, so
    reviewers must inspect manually.
    """

    bench_name: str
    waitbus_version: str
    started_at_ns: int
    ended_at_ns: int
    n_samples: int
    n_warmup_discarded: int
    rate_hz: float
    percentiles_gc_enabled: dict[str, PercentilePoint]
    percentiles_gc_disabled: dict[str, PercentilePoint] | None
    clamp_stats_gc_enabled: ClampStats
    clamp_stats_gc_disabled: ClampStats | None
    environment: EnvironmentReport
    extra: dict[str, Any] = msgspec.field(default_factory=dict)
    histogram_b64_gc_enabled: str | None = None
    histogram_b64_gc_disabled: str | None = None


def collect_result(
    *,
    bench_name: str,
    started_at_ns: int,
    ended_at_ns: int,
    n_warmup_discarded: int,
    rate_hz: float,
    hdr_main: HdrRecorder,
    hdr_gc_off: HdrRecorder | None,
    environment: EnvironmentReport,
    extra: dict[str, Any] | None = None,
    percentiles: tuple[float, ...] = (0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99),
) -> BenchResult:
    """Assemble the JSON-ready result from a finished bench run.

    Args:
        bench_name: short identifier (e.g. ``"ttfae_pytest"``).
        started_at_ns / ended_at_ns: bench wall-clock bracket
            (``time.time_ns()`` values).
        n_warmup_discarded: count of samples discarded as warmup.
            Recorded so a reviewer can see the discard.
        rate_hz: target rate from :class:`OpenLoopScheduler`.
        hdr_main: gc-enabled :class:`HdrRecorder`.
        hdr_gc_off: optional gc-disabled companion recorder.
        environment: from :func:`environment_report`.
        extra: bench-specific extension dict (e.g. for
            :file:`bench_idle_rss.py` to record the nats-server
            comparison number). Defaults to empty.
        percentiles: percentile fractions to report. Default is a
            ``p10``-through-``p99`` grid so the committed result carries
            the distribution shape, not just the tail. p99.9 is
            NOT in the default; see the sample-size
            section of BENCHMARKING.md for why we stop at p99 at
            n=5000 (a defensible p99.9 CI needs ~15k samples).
    """

    def _block(rec: HdrRecorder) -> dict[str, PercentilePoint]:
        out: dict[str, PercentilePoint] = {}
        for p in percentiles:
            stats = rec.percentile_with_ci(p)
            out[f"p{int(p * 100)}" if p * 100 == int(p * 100) else f"p{p * 100:g}"] = PercentilePoint(
                value_ns=int(stats["value_ns"]),
                ci_low_ns=int(stats["ci_low_ns"]),
                ci_high_ns=int(stats["ci_high_ns"]),
                ci_z=float(stats["ci_z"]),
                p_low_eff=float(stats["p_low_eff"]),
                p_high_eff=float(stats["p_high_eff"]),
            )
        return out

    return BenchResult(
        bench_name=bench_name,
        waitbus_version=_waitbus_version(),
        started_at_ns=started_at_ns,
        ended_at_ns=ended_at_ns,
        n_samples=hdr_main.count,
        n_warmup_discarded=n_warmup_discarded,
        rate_hz=rate_hz,
        percentiles_gc_enabled=_block(hdr_main),
        percentiles_gc_disabled=_block(hdr_gc_off) if hdr_gc_off is not None else None,
        clamp_stats_gc_enabled=ClampStats(low=hdr_main.clamp_low, high=hdr_main.clamp_high),
        clamp_stats_gc_disabled=(
            ClampStats(low=hdr_gc_off.clamp_low, high=hdr_gc_off.clamp_high) if hdr_gc_off is not None else None
        ),
        environment=environment,
        extra=extra or {},
        histogram_b64_gc_enabled=hdr_main.encode_b64(),
        histogram_b64_gc_disabled=hdr_gc_off.encode_b64() if hdr_gc_off is not None else None,
    )


def write_result(result: BenchResult, path: Path) -> None:
    """Write the result to ``path`` as pretty-printed JSON.

    Atomic via tmp-rename to defend against a crash mid-write
    leaving a partial file that a CI step would then read.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = msgspec.json.format(msgspec.json.encode(result), indent=2)
    tmp = path.with_suffix(path.suffix + ".partial")
    tmp.write_bytes(encoded)
    tmp.replace(path)


def check_regression(
    result: BenchResult,
    baseline_path: Path,
    *,
    threshold: float = _DEFAULT_REGRESSION_THRESHOLD,
) -> tuple[bool, str]:
    """Compare ``result`` against the committed baseline.

    Returns ``(ok, message)``. ``ok=False`` means the current result
    is more than ``threshold`` (default 25%) worse than the baseline
    on p99 of the gc-enabled side. Bench scripts use this for
    ``--check-regression`` mode and exit non-zero when ``ok`` is
    False.

    If the baseline file does not exist, returns ``(True, "no
    baseline file committed yet")`` -- the first run on a new
    bench script is by definition the baseline. Bench scripts treat
    this as a success in CI but should fail a separate "did the
    baseline get committed?" check in the next CI run.
    """
    if not baseline_path.exists():
        return True, f"no baseline at {baseline_path}; first run is the baseline"
    raw = baseline_path.read_bytes()
    baseline = msgspec.json.decode(raw, type=BenchResult)
    if baseline.bench_name != result.bench_name:
        return (
            False,
            f"baseline bench_name {baseline.bench_name!r} != current {result.bench_name!r}",
        )
    baseline_p99 = baseline.percentiles_gc_enabled["p99"].value_ns
    current_p99 = result.percentiles_gc_enabled["p99"].value_ns
    if baseline_p99 <= 0:
        return False, f"baseline p99 is {baseline_p99} (invalid)"
    ratio = current_p99 / baseline_p99
    if ratio > 1.0 + threshold:
        return (
            False,
            (
                f"p99 regression: current={current_p99} ns vs baseline={baseline_p99} ns "
                f"(ratio={ratio:.3f}; threshold=+{threshold * 100:.0f}%)"
            ),
        )
    return (
        True,
        f"p99 ok: current={current_p99} ns vs baseline={baseline_p99} ns (ratio={ratio:.3f})",
    )


# ---------------------------------------------------------------------------
# Waitbus daemon subprocess spawn helper
# ---------------------------------------------------------------------------


def spawn_waitbus_daemon(
    state_dir: Path,
    runtime_dir: Path,
    *,
    stderr_path: Path | None = None,
    heartbeat_sec: float | None = None,
) -> subprocess.Popen[bytes]:
    """Spawn ``waitbus broadcast serve`` as a subprocess with isolated state.

    Inherits the caller's PATH/PYTHONPATH (picks up the uv venv in dev)
    and points the daemon at the supplied tmp directories via
    ``WAITBUS_STATE_DIR`` / ``WAITBUS_RUNTIME_DIR`` so the invocation
    never touches the operator's real waitbus state.

    ``WAITBUS_HEARTBEAT_SEC`` defaults to 3600 so the broadcaster does
    not emit ``daemon_heartbeat`` rows during the measurement window
    (those rows wake the broadcaster, which would disturb RSS and timing
    measurements). Pass ``heartbeat_sec`` to override it -- the soak's
    drain-path smoke pre-phase uses an aggressive sub-second heartbeat
    against a throwaway daemon to drive the ``heartbeat_lag`` eviction
    path, which cannot fire under the 3600-second measurement default.

    The subprocess's stdout is always suppressed. Its stderr is
    suppressed by default (``stderr_path=None``); when ``stderr_path`` is
    supplied, the daemon's structured JSON log stream is appended there
    instead, so a caller (the soak harness) can parse ``subscriber_closed``
    and other structured events after the run. The daemon configures
    ``format="%(message)s"`` to stderr, so each captured line is pure
    JSON. The bench or soak script is responsible for cleaning up the
    process (SIGTERM + wait, escalating to SIGKILL on timeout).

    Args:
        state_dir: directory for the SQLite DB. Must already exist.
        runtime_dir: directory for AF_UNIX sockets. Must already exist.
        stderr_path: when set, append the daemon's stderr to this file
            (one persistent fd for the daemon's lifetime; does not affect
            the daemon's own /proc/<pid>/fd count, which the soak monitors).
        heartbeat_sec: when set, override the daemon's heartbeat interval
            (default 3600). Used by the drain-path smoke to fire heartbeats
            frequently against a throwaway daemon.

    Returns:
        The live :class:`subprocess.Popen` handle.
    """
    env = os.environ.copy()
    env["WAITBUS_STATE_DIR"] = str(state_dir)
    env["WAITBUS_RUNTIME_DIR"] = str(runtime_dir)
    env["WAITBUS_HEARTBEAT_SEC"] = "3600" if heartbeat_sec is None else str(float(heartbeat_sec))
    # Orphan-leak guard via an inherited pipe. The daemon is detached
    # into its own session (start_new_session=True below) so we can group-kill
    # it, with cleanup in a finally (terminate_daemon_group). A SIGKILL'd /
    # crashed harness bypasses that finally and would orphan the session-
    # detached daemon forever. We hand the daemon the READ end of a pipe and
    # keep the WRITE end open for its lifetime: when this process dies by any
    # means, the kernel closes the write end and the daemon's read end hits EOF.
    # PEP 446 makes Python-created fds non-inheritable by default, so the read
    # end is marked inheritable and passed via pass_fds; the write end stays
    # CLOEXEC so no sibling subprocess of the harness inherits it.
    death_r, death_w = os.pipe()
    os.set_inheritable(death_r, True)
    env["WAITBUS_DEATH_FD"] = str(death_r)
    stderr_target: int | IO[bytes] = subprocess.DEVNULL
    stderr_fh: IO[bytes] | None = None
    if stderr_path is not None:
        stderr_fh = stderr_path.open("ab")
        stderr_target = stderr_fh
    # start_new_session=True places the daemon in its own process group
    # so a caller can SIGTERM the whole group via os.killpg(os.getpgid(pid)),
    # catching the grandchild Python process that ``uv run`` exec-chains
    # to. Without this, a terminate()/kill() on the outer Popen PID
    # leaves the grandchild as an orphan -- the failure mode fault-injection
    # testing surfaced when the daemon was SIGKILL'd mid-soak.
    try:
        proc = subprocess.Popen(
            ["uv", "run", "waitbus", "broadcast", "serve"],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=stderr_target,
            start_new_session=True,
            pass_fds=(death_r,),
        )
    finally:
        # Popen dup'd both the stderr and the death-read fd into the child; the
        # parent's copies are no longer needed. Closing them keeps the parent's
        # fd table flat (the soak monitors the DAEMON's fd count, not the
        # parent's) and avoids a GC-time finalizer warning when the caller's tmp
        # dir is torn down. The death-WRITE end stays open below.
        os.close(death_r)
        if stderr_fh is not None:
            stderr_fh.close()
    # Stash the write end on the handle so it lives exactly as long as the
    # caller holds the Popen object. Holding it open is the liveness signal;
    # terminate_daemon_group closes it explicitly, and a crashed harness closes
    # it implicitly on process exit -- both make the daemon's read end hit EOF.
    proc._waitbus_death_w = death_w  # type: ignore[attr-defined]
    return proc


def terminate_daemon_group(
    proc: subprocess.Popen[bytes],
    *,
    term_timeout: float = _DAEMON_TERM_TIMEOUT_SEC,
    kill_timeout: float = _DAEMON_KILL_TIMEOUT_SEC,
) -> None:
    """Terminate a daemon spawned by :func:`spawn_waitbus_daemon` and its group.

    Pairs with ``start_new_session=True``: SIGTERMs the whole process group
    (catching the grandchild Python process that ``uv run`` exec-chains to),
    waits ``term_timeout`` for a clean exit, then escalates to SIGKILL on the
    group and waits ``kill_timeout``. All process-lookup races are suppressed
    so a daemon that already exited is a no-op. Also closes the spawner-death
    pipe's write end (the daemon's EOF liveness signal) so the parent's fd table
    stays flat after the explicit teardown.
    """
    try:
        pgid: int | None = os.getpgid(proc.pid)
    except ProcessLookupError:
        pgid = None
    if pgid is not None:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(pgid, signal.SIGTERM)
    try:
        proc.wait(timeout=term_timeout)
    except subprocess.TimeoutExpired:
        if pgid is not None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(pgid, signal.SIGKILL)
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=kill_timeout)
    death_w = getattr(proc, "_waitbus_death_w", None)
    if death_w is not None:
        with contextlib.suppress(OSError):
            os.close(death_w)
        proc._waitbus_death_w = None  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Output-path resolution helper
# ---------------------------------------------------------------------------


def resolve_output_path(
    bench_name: str,
    results_dir: Path,
    output_arg: Path | None,
    env: EnvironmentReport,
) -> Path:
    """Return the output path for a bench result JSON file.

    If ``output_arg`` is not None (the operator supplied ``--output``),
    return it directly. Otherwise construct a timestamped default under
    ``results_dir`` as ``<bench_name>_<host>_<YYYYMMDDTHHmmSS>.json``.

    The hostname component uses ``env.hostname`` (already captured by
    :func:`environment_report`) with dots replaced by underscores so the
    filename is shell-safe. An empty hostname falls back to ``"unknown"``.

    Args:
        bench_name: short bench identifier (matches ``_BENCH_NAME``).
        results_dir: directory for auto-generated result files.
        output_arg: value of the ``--output`` CLI argument, or None.
        env: environment report captured at bench startup.

    Returns:
        Resolved :class:`Path` for the result JSON.
    """
    if output_arg is not None:
        return output_arg
    ts = time.strftime("%Y%m%dT%H%M%S")
    host = env.hostname.replace(".", "_") or "unknown"
    return results_dir / f"{bench_name}_{host}_{ts}.json"


def print_percentile_summary(
    result: BenchResult,
    *,
    bench_name: str,
    file: IO[str] | None = None,
) -> None:
    """Print gc-enabled and gc-disabled percentile summary lines to stderr.

    Prints one line for the gc-enabled run (p50/p90/p99 in ms plus
    sample count) and, when the result carries a gc-disabled companion
    run, a second line (p50/p99 in ms). Both lines go to ``sys.stderr``
    by default; pass a real file object to capture output in tests.

    Args:
        result: finished bench result from :func:`collect_result`.
        bench_name: short bench identifier (matches ``_BENCH_NAME``).
        file: output file; defaults to ``sys.stderr``.
    """
    out: IO[str] = file if file is not None else sys.stderr
    p50 = result.percentiles_gc_enabled["p50"]
    p90 = result.percentiles_gc_enabled["p90"]
    p99 = result.percentiles_gc_enabled["p99"]
    print(
        f"[{bench_name}] gc-enabled  p50={p50.value_ns / 1e6:8.3f} ms  "
        f"p90={p90.value_ns / 1e6:8.3f} ms  "
        f"p99={p99.value_ns / 1e6:8.3f} ms  "
        f"(n={result.n_samples})",
        file=out,
    )
    if result.percentiles_gc_disabled is not None:
        g50 = result.percentiles_gc_disabled["p50"]
        g99 = result.percentiles_gc_disabled["p99"]
        print(
            f"[{bench_name}] gc-disabled p50={g50.value_ns / 1e6:8.3f} ms  p99={g99.value_ns / 1e6:8.3f} ms",
            file=out,
        )


# ---------------------------------------------------------------------------
# Public re-exports kept narrow on purpose; bench scripts import what they need.
# ---------------------------------------------------------------------------

__all__ = [
    "BenchResult",
    "ClampStats",
    "DaemonHandle",
    "EnvironmentReport",
    "HdrRecorder",
    "OpenLoopScheduler",
    "PercentilePoint",
    "capture_t0",
    "check_regression",
    "collect_result",
    "consume_t0",
    "daemon_context",
    "environment_report",
    "gc_disabled",
    "nats_pinned_image",
    "print_percentile_summary",
    "resolve_output_path",
    "spawn_waitbus_daemon",
    "wait_for_socket",
    "wilson_rank_ci",
    "write_result",
]


# ---------------------------------------------------------------------------
# Cross-thread t=0 capture (used by bench_ttfae_github)
# ---------------------------------------------------------------------------

#: Process-wide map from a delivery-key to the timestamp captured at the
#: source's ingress. The github TTFAE bench monkey-patches the
#: webhook listener to write into this map keyed by ``X-GitHub-Delivery``
#: on every incoming request; the bench's main loop reads back via
#: :func:`consume_t0` when the corresponding subscriber frame arrives.
#: Lives in the harness so any future "instrument ingress timestamp"
#: bench reuses the same primitive instead of re-deriving thread-safe
#: storage.
_T0_CAPTURE: dict[str, int] = {}
_T0_CAPTURE_LOCK = threading.Lock()


def capture_t0(key: str) -> None:
    """Record the current wall-clock time keyed by ``key``.

    Called from the producer side (the monkey-patched ingress
    handler). Uses :func:`time.time_ns` because the consumer side
    runs in a different thread/process and we need a comparable
    cross-process clock. The bench's
    :class:`OpenLoopScheduler` is the only intra-process timing
    primitive; ingress measurement is cross-process by definition.
    """
    with _T0_CAPTURE_LOCK:
        _T0_CAPTURE[key] = time.time_ns()


def consume_t0(key: str) -> int:
    """Pop and return the recorded ``t=0`` for ``key``.

    Called from the consumer side once the bench has identified
    which delivery this frame corresponds to. Raises
    :class:`KeyError` if the producer never recorded a t=0 for
    this key -- a real bug (the producer is supposed to record
    before triggering the send), not a noise we should silently
    swallow.
    """
    with _T0_CAPTURE_LOCK:
        return _T0_CAPTURE.pop(key)


# ---------------------------------------------------------------------------
# Pinned nats-server image lookup (used by bench_idle_rss)
# ---------------------------------------------------------------------------

#: Location of the committed digest pin file. Set up by
#: ``benchmarks/setup/pin_nats_image.sh`` and read at bench startup.
_PINNED_IMAGES_PATH = Path(__file__).resolve().parent / "baselines" / "pinned_images.json"


def nats_pinned_image() -> str:
    """Return the pinned nats-server Docker image reference.

    Reads from ``benchmarks/baselines/pinned_images.json``. The
    file is created/refreshed by
    ``benchmarks/setup/pin_nats_image.sh`` (one-time setup the
    bench operator runs before the first idle-RSS measurement,
    and any time they want to refresh the pin to a newer NATS
    minor/patch release).

    Returns:
        A ``image@sha256:digest`` reference suitable for
        ``docker run``. The bench passes this to ``docker run``
        verbatim so the comparison is byte-reproducible.

    Raises:
        FileNotFoundError: the pin file is absent (setup script
            was never run). The remediation message names the
            setup script explicitly.
        KeyError: the pin file exists but does not contain a
            ``nats`` entry -- an inconsistent setup state worth
            surfacing rather than silently degrading.
    """
    if not _PINNED_IMAGES_PATH.exists():
        raise FileNotFoundError(
            f"pinned-image manifest absent at {_PINNED_IMAGES_PATH}; "
            "run `bash benchmarks/setup/pin_nats_image.sh` to populate it."
        )
    raw = _PINNED_IMAGES_PATH.read_text(encoding="utf-8")
    pins = msgspec.json.decode(raw, type=dict[str, str])
    try:
        return pins["nats"]
    except KeyError as exc:
        raise KeyError(
            f"no 'nats' pin in {_PINNED_IMAGES_PATH}; re-run `bash benchmarks/setup/pin_nats_image.sh`."
        ) from exc


# ---------------------------------------------------------------------------
# Corpus replay (consumed by scripts/soak/_main.py --corpus and ad-hoc bench paths)
# ---------------------------------------------------------------------------


def replay_corpus(path: Path) -> Iterator[dict[str, Any] | None]:
    """Yield one parsed event dict per line from a gzipped corpus file.

    Corpus shape is the output of ``python -m benchmarks.gen_corpus``:
    gzipped UTF-8 JSONL with one event per line, trailing newline.
    Each well-formed line yields the ``json.loads`` result. Lines that
    fail ``json.loads`` yield ``None`` so the caller can register the
    parse-failure (count + one-time warning) without re-decoding.

    Yielding ``None`` matches the waitbus ``T | None`` sentinel idiom
    (sibling: ``_collect_sample_or_partial``). The soak consumer
    (``_emit_corpus_event``) already pattern-matches ``event is None``
    to ``accums.corpus_decode_fallthroughs`` + ``state.json_decode_warned``;
    the original ``continue`` form made that counter structurally
    unreachable.

    Raises:
        FileNotFoundError: if ``path`` does not exist.
    """
    import json as _json

    if not path.exists():
        raise FileNotFoundError(f"corpus file not found at {path}")
    with gzip.open(path, "rb") as fh:
        for raw_line in fh:
            stripped = raw_line.rstrip(b"\n")
            if not stripped:
                continue
            try:
                yield _json.loads(stripped)
            except _json.JSONDecodeError:
                yield None
