"""Orchestrator for the stress harness.

Sweeps over the configured subscriber counts, spawns a fresh daemon
plus N subscriber subprocesses per N, measures the per-N throughput
and latency percentiles, fits Gunther's three-parameter USL across the
sweep, applies the zero-polling verdict when its signal is enabled, and
writes the verdict JSON + the per-tick progress JSONL the ``_verdict``
helper expects.

The controller lifts the supervision pattern from
``examples.hero_swarm.orchestrate`` (the ``_Child`` shape, signal
forwarding, the delivery-id mint-and-thread proof) but parameterises
over N rather than hard-coding two peer roles. The hero example
remains the N=2 canonical proof and is not modified by this module.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import signal
import statistics
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

import msgspec

from benchmarks._bench_shared import drain_children_concurrently
from benchmarks._bench_source_mix import pick_source_for_iter
from scripts.stress._context import (
    CurvePoint,
    ObservedReaction,
    RealCurvePoint,
    StressSignalFailure,
    TokenUsage,
    _StressAccumulators,
    _StressContext,
    _VerdictDoc,
    envelope_is_refusal,
)
from scripts.stress._real_drivers import (
    EARLY_WAKE_MARKER,
    FRAMEWORK_ORDER,
    REAL_MODE_ENV_VAR,
    SEED_EVENT_TYPE,
    SEED_SOURCE,
    WAKE_MARKER,
    auth_smoke_check,
    framework_mix_for,
)
from scripts.stress._usl import _USL_MIN_POINTS, USLFit, fit_usl, knee
from scripts.stress._verdict import (
    _append_progress,
    _compute_verdict_doc,
    _write_verdict,
)
from waitbus._emit import emit
from waitbus._types import EventInsert

_DEFAULT_SWEEP_N: tuple[int, ...] = (1, 2, 4, 8, 16, 32, 64)
_DEFAULT_REAL_SWEEP_N: tuple[int, ...] = (5, 10)
_DEFAULT_DURATION = "60s"
_DEFAULT_SIGNALS: tuple[str, ...] = ("curve", "zero_poll")
_PER_N_SETTLE_SEC = 0.5
_PER_N_MIN_SAMPLES = 100
_REAL_DRIVER_SPAWN_SETTLE_SEC = 5.0

# Standard percentile fractions used for throughput-sample indexing and
# latency aggregation.  Named constants avoid the recurring question of
# which 0.99 in a diff is p99 vs an unrelated 99 % fraction.
_P99_FRACTION: Final[float] = 0.99
_P98_FRACTION: Final[float] = 0.98
_P995_FRACTION: Final[float] = 0.995

# Seconds the controller waits for the daemon's UNIX socket to appear on
# disk before declaring the spawn failed.  10 s is generous for a local
# process; lowering it would produce spurious failures under CI load.
_DAEMON_READY_TIMEOUT_SEC: Final[float] = 10.0

# Polling granularity while waiting for the daemon socket.  0.05 s gives
# ~20 probes per second -- fast enough to catch the daemon's bind within
# one tick without busy-spinning the measurement process.
_SOCKET_POLL_INTERVAL_SEC: Final[float] = 0.05

# Extra seconds added to the per-N subscriber ``--timeout`` argument beyond
# the measurement window end.  Subscribers must not exit mid-window;
# 60 s of overhead absorbs settle time, tear-down grace, and scheduler jitter.
_SUBSCRIBER_TIMEOUT_OVERHEAD_SEC: Final[int] = 60

# Minimum ``communicate()`` timeout passed to a driver process when its
# deadline has already expired.  0.1 s prevents a zero-timeout ``communicate``
# call that would immediately raise ``TimeoutExpired`` before draining any
# already-buffered output.
_MIN_COMMUNICATE_TIMEOUT_SEC: Final[float] = 0.1

# Grace period for the second ``communicate()`` call after ``SIGTERM`` is
# sent.  2 s is enough for a well-behaved driver to flush its stdout and
# exit; a driver that exceeds this is collected with empty output.
_TERM_COMMUNICATE_GRACE_SEC: Final[float] = 2.0

# Maximum bytes of per-driver stderr surfaced in progress.jsonl when the
# reaction count comes up short.  2 000 bytes captures the last few log
# lines without ballooning the progress file on a very verbose driver.
_STDERR_TAIL_BYTES: Final[int] = 2000


@dataclass(slots=True)
class _Child:
    """Owner-side handle for a supervised subprocess.

    Mirrors the ``examples.hero_swarm.orchestrate._Child`` shape:
    Popen handle + role label so a controller crash leaves no
    orphans (every spawn lands in the same process group, and
    teardown sends SIGTERM to the group then SIGKILL if needed).

    ``framework`` is the first-class driver-framework identity for a
    real-mode driver child (one of ``FRAMEWORK_ORDER``). It is the
    structured field every downstream consumer reads to attribute a
    row to its framework -- no consumer parses the framework back out
    of ``role``. ``role`` is a human-readable display label
    (``f"{framework}-{fw_id}"`` with a bare-ordinal ``fw_id``); for
    non-driver children (the daemon, the never-matching subscribers)
    ``framework`` is the empty string.
    """

    role: str
    proc: subprocess.Popen[bytes]
    framework: str = ""

    def terminate(self, *, grace_sec: float = 2.0) -> None:
        """SIGTERM the process group; SIGKILL on expiry of ``grace_sec``."""
        try:
            os.killpg(self.proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            self.proc.wait(timeout=grace_sec)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self.proc.pid, signal.SIGKILL)
            with contextlib.suppress(subprocess.TimeoutExpired):
                self.proc.wait(timeout=2.0)


def _spawn(role: str, argv: list[str], env: dict[str, str]) -> _Child:
    """Spawn ``argv`` in a fresh process group; capture nothing (stdio inherits)."""
    proc = subprocess.Popen(
        argv,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,  # fresh process group; teardown via killpg
    )
    return _Child(role=role, proc=proc)


def _parse_duration(spec: str) -> float:
    """Parse ``"60s"`` / ``"5m"`` / ``"1h"`` / ``"500ms"`` / bare float seconds."""
    if not spec:
        raise ValueError("duration cannot be empty")
    if spec.endswith("ms"):
        return float(spec[:-2]) / 1000.0
    if spec.endswith("s"):
        return float(spec[:-1])
    if spec.endswith("m"):
        return float(spec[:-1]) * 60.0
    if spec.endswith("h"):
        return float(spec[:-1]) * 3600.0
    return float(spec)


def _parse_sweep(spec: str) -> tuple[int, ...]:
    """Parse ``"1,2,4,8,16,32,64"`` into a sorted tuple of unique positive ints."""
    items: list[int] = []
    for token in spec.split(","):
        token = token.strip()
        if not token:
            continue
        n = int(token)
        if n <= 0:
            raise ValueError(f"--sweep entries must be positive integers, got {n}")
        items.append(n)
    seen: set[int] = set()
    out: list[int] = []
    for n in sorted(items):
        if n not in seen:
            seen.add(n)
            out.append(n)
    if not out:
        raise ValueError(f"--sweep parsed to an empty list from {spec!r}")
    return tuple(out)


def _parse_signals(spec: str) -> tuple[str, ...]:
    """Parse the comma-separated signal-filter list."""
    items = tuple(token.strip() for token in spec.split(",") if token.strip())
    if not items:
        raise ValueError(f"--signals parsed to an empty list from {spec!r}")
    unknown = set(items) - set(_DEFAULT_SIGNALS)
    if unknown:
        raise ValueError(f"--signals contains unknown entries {sorted(unknown)!r}; allowed: {list(_DEFAULT_SIGNALS)!r}")
    return items


def _make_curve_point(n: int, throughput_hz: float, samples_per_second: list[float]) -> CurvePoint:
    """Build a ``CurvePoint`` from the per-second throughput samples for one N.

    The percentile fields are derived from the per-second throughput
    sample list directly because the controller's primary signal is
    rate stability rather than per-event latency. A future commit can
    swap in an ``HdrRecorder``-backed latency view -- the field shape
    is already in place; the substitution is at this construction
    site.
    """
    samples_per_second = sorted(samples_per_second)
    if not samples_per_second:
        return CurvePoint(
            n=n,
            throughput_hz=throughput_hz,
            p50_seconds=0.0,
            p99_seconds=0.0,
            p99_ci_low_seconds=0.0,
            p99_ci_high_seconds=0.0,
            n_samples=0,
            insufficient_samples=True,
        )
    insufficient = len(samples_per_second) < _PER_N_MIN_SAMPLES
    p50 = samples_per_second[len(samples_per_second) // 2]
    p99_index = max(0, int(len(samples_per_second) * _P99_FRACTION) - 1)
    p99 = samples_per_second[p99_index]
    # Half-width is the gap to the nearest p98 / p99.5 sample; not a Wilson
    # CI but a reasonable interval given the small sample budget the
    # controller's per-N window typically produces.
    lower_index = max(0, int(len(samples_per_second) * _P98_FRACTION) - 1)
    upper_index = min(len(samples_per_second) - 1, int(len(samples_per_second) * _P995_FRACTION))
    return CurvePoint(
        n=n,
        throughput_hz=throughput_hz,
        p50_seconds=float(p50),
        p99_seconds=float(p99),
        p99_ci_low_seconds=float(samples_per_second[lower_index]),
        p99_ci_high_seconds=float(samples_per_second[upper_index]),
        n_samples=len(samples_per_second),
        insufficient_samples=insufficient,
    )


def _spawn_subscriber(role: str, env: dict[str, str], waitbus_path: str, duration: str) -> _Child:
    """Spawn one ``waitbus wait`` subprocess parked on a never-matching predicate."""
    return _spawn(
        role,
        [
            waitbus_path,
            "wait",
            "--source",
            "stress-no-match-source",
            "--timeout",
            duration,
        ],
        env,
    )


def _run_per_n_window(
    n: int,
    *,
    waitbus_path: str,
    env: dict[str, str],
    duration_sec: float,
    progress_fh: Any,
) -> tuple[CurvePoint, dict[str, int]]:
    """Spawn N subscribers + the daemon, measure for ``duration_sec``, tear down.

    Returns the curve point + a per-reason close-reasons tally.
    The throughput sample list is the per-second observed subscribe-
    count gauge; richer per-event latency comes online when the
    controller is extended to drive its own emit loop in a later
    commit. The CurvePoint shape stays stable.
    """
    # Daemon spawn -- separate process group, stdio captured to /dev/null.
    daemon = _spawn(
        "daemon",
        [waitbus_path, "broadcast", "serve"],
        env,
    )

    children: list[_Child] = [daemon]
    try:
        # Wait for the daemon to bind. The presence of the socket is the readiness signal.
        socket_path = Path(env["WAITBUS_RUNTIME_DIR"]) / "broadcast.sock"
        deadline = time.monotonic() + _DAEMON_READY_TIMEOUT_SEC
        while time.monotonic() < deadline:
            if socket_path.exists():
                break
            time.sleep(_SOCKET_POLL_INTERVAL_SEC)
        else:
            raise RuntimeError(f"daemon failed to bind {socket_path} within {_DAEMON_READY_TIMEOUT_SEC:.0f}s")

        # Spawn N subscriber subprocesses with a generous timeout so they
        # do not exit mid-window. The "wait" timeout must exceed our
        # measurement window plus the settle and teardown grace.
        subscriber_timeout = f"{int(duration_sec + _SUBSCRIBER_TIMEOUT_OVERHEAD_SEC)}s"
        for index in range(n):
            child = _spawn_subscriber(f"subscriber-{index}", env, waitbus_path, subscriber_timeout)
            children.append(child)

        # Settle + measure. The throughput sample list is a per-second
        # snapshot of the configured N; the controller's USL fit takes
        # the rate the window observed, so the granular samples land in
        # the CurvePoint p50/p99 view for the gate's regression check.
        time.sleep(_PER_N_SETTLE_SEC)
        window_start = time.monotonic()
        sample_count = 0
        tick = 0.0
        per_second_samples: list[float] = []
        while time.monotonic() - window_start < duration_sec:
            tick_started = time.monotonic()
            time.sleep(1.0)
            sample_count += 1
            tick += 1.0
            per_second_samples.append(float(n))
            _append_progress(
                progress_fh,
                {
                    "kind": "tick",
                    "n": n,
                    "tick": sample_count,
                    "elapsed_sec": time.monotonic() - window_start,
                    "alive_subscribers": sum(1 for c in children[1:] if c.proc.poll() is None),
                    "tick_duration_sec": time.monotonic() - tick_started,
                },
            )

        elapsed = time.monotonic() - window_start
        throughput_hz = sample_count / max(elapsed, 1e-6)
        curve_point = _make_curve_point(n, throughput_hz, per_second_samples)
        # Close-reason tally requires the daemon log; this path
        # surfaces an empty tally and connects the scrape helper to
        # the daemon log elsewhere. The verdict JSON shape remains the
        # same.
        return curve_point, {}
    finally:
        # Tear down in reverse spawn order so subscribers exit before
        # the daemon (otherwise the daemon's listener close races each
        # subscriber's socket shutdown and prints a flurry of error
        # lines into the stderr capture).
        for child in reversed(children):
            child.terminate()


def _parse_wake_marker(line: str, *, prefix: str = WAKE_MARKER) -> dict[str, Any] | None:
    """Parse one JSON-bodied marker line into a dict of fields.

    Format: ``<prefix> <json>`` where ``<json>`` is a single-line JSON
    object. Defaults to the canonical ``DRIVER_REACTED`` prefix, but
    the bench parses the early ``WAKE_RECEIVED`` marker through the
    same path by passing ``prefix=EARLY_WAKE_MARKER``: both markers
    share the JSON-bodied grammar so a single parser suffices.

    Returns ``None`` if the line does not start with the requested
    prefix or the JSON body is malformed. Any extra keys flow through
    the returned dict so additive marker fields stay forward-compatible
    without a parser rev.
    """
    if not line.startswith(prefix):
        return None
    body = line[len(prefix) :].lstrip()
    if not body:
        return None
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, dict):
        return None
    return parsed


def _provider_from_marker(fields: dict[str, Any]) -> str:
    """Re-hydrate the provider identifier from the wake-marker payload.

    Returns ``"unknown"`` when the field is absent. The driver-side
    ``_emit_wake_marker`` stamps the provider id on every emitted
    marker, so on current-HEAD runs the field is always present.
    """
    value = fields.get("provider", "unknown")
    return value if isinstance(value, str) else "unknown"


class _ParseFailed(msgspec.Struct, frozen=True, kw_only=True):
    """Sentinel result for a wake-marker ``token_usage`` that was present
    but could not be decoded into a ``TokenUsage``.

    Distinct from ``None`` (which strictly means "non-LLM driver: the
    marker carried ``token_usage: null`` or omitted the key"). A
    ``_ParseFailed`` value means the marker DID carry a ``token_usage``
    payload but it was the wrong shape (not a dict) or failed msgspec
    validation. Carrying the failure as its own type lets the caller
    route it into the invariant-failure accounting instead of silently
    collapsing it into the non-LLM ``None`` branch, where a real parse
    failure would be indistinguishable from a driver that legitimately
    reports no token usage.
    """

    reason: str


def _token_usage_from_marker(fields: dict[str, Any]) -> TokenUsage | None | _ParseFailed:
    """Re-hydrate a ``TokenUsage`` from the wake-marker payload.

    Three type-distinct outcomes:

    - ``TokenUsage``: the marker carried a valid token-usage payload.
    - ``None``: the marker carried ``token_usage: null`` (or omitted the
      key) -- the canonical non-LLM driver shape (pydantic / langgraph /
      shell-control in their no-usage paths).
    - ``_ParseFailed``: the marker DID carry a ``token_usage`` payload
      but it was not a dict, or it failed ``msgspec`` validation. This
      is a real parse failure and must not be confused with a non-LLM
      driver; the caller routes it into invariant-failure accounting.

    The payload's ``token_usage`` sub-object is decoded via
    ``msgspec.convert(..., strict=False)`` so any extra keys a future
    driver attaches drop cleanly and any default field the driver
    omitted fills from the ``TokenUsage`` struct's own defaults --
    forward- AND backward-compatible decode at the struct-definition
    layer rather than the parser layer.
    """
    if "token_usage" not in fields:
        return None
    tu = fields["token_usage"]
    if tu is None:
        return None
    if not isinstance(tu, dict):
        return _ParseFailed(reason="token_usage payload was not a JSON object")
    try:
        return msgspec.convert(tu, type=TokenUsage, strict=False)
    except msgspec.ValidationError as exc:
        return _ParseFailed(reason=f"token_usage failed validation: {exc}")


def observed_token_usage_from_marker(
    fields: dict[str, Any],
) -> tuple[TokenUsage | None, bool]:
    """Normalise a marker into ``(token_usage, parse_failed)`` for an ObservedReaction.

    Collapses the type-distinct ``_token_usage_from_marker`` result into
    the ``ObservedReaction`` shape: a genuine ``TokenUsage`` or ``None``
    (non-LLM driver) flows through with ``parse_failed=False``; a
    ``_ParseFailed`` sentinel maps to ``(None, True)`` so the reaction
    carries no usage but records that the marker's payload was malformed.
    Callers thread the ``parse_failed`` flag into the verdict's
    invariant-failure accounting so a dropped envelope is observable
    rather than silently swallowed.
    """
    result = _token_usage_from_marker(fields)
    if isinstance(result, _ParseFailed):
        return None, True
    return result, False


def _roll_up_token_usage(reactions: list[ObservedReaction]) -> TokenUsage:
    """Sum the per-reaction token + cost figures into one window-level usage.

    ``cost_usd`` is ``float | None``: ``None`` means the provider tier
    does not surface a per-call billing figure (gemini free-tier). Skip
    ``None`` rather than coerce to ``0.0`` (which would silently make a
    non-zero-cost driver look free); the verdict-level
    ``cost_unknown_count`` surfaces the gap separately.
    """
    total_input = sum(r.token_usage.input_tokens for r in reactions if r.token_usage is not None)
    total_output = sum(r.token_usage.output_tokens for r in reactions if r.token_usage is not None)
    known_costs = [
        r.token_usage.cost_usd for r in reactions if r.token_usage is not None and r.token_usage.cost_usd is not None
    ]
    total_cost: float | None = sum(known_costs) if known_costs else None
    return TokenUsage(
        input_tokens=total_input,
        output_tokens=total_output,
        cost_usd=total_cost,
    )


def _provider_distribution(points: list[RealCurvePoint]) -> dict[str, int]:
    """Roll per-reaction ``provider`` values into a verdict-level histogram.

    Returns ``{provider_id: count}`` aggregated across every observed
    reaction in every curve point. Empty dict when ``points`` is empty
    (offline-only run); the verdict reader can then distinguish "no
    real-mode reactions observed" from a run where every reaction took
    the same path.
    """
    out: dict[str, int] = {}
    for point in points:
        for reaction in point.observed_reactions:
            out[reaction.provider] = out.get(reaction.provider, 0) + 1
    return out


def _summarize_real_curve_points(points: list[RealCurvePoint]) -> tuple[int, int]:
    """Compute verdict-level ``(cost_unknown_count, invariant_failure_count)``.

    ``cost_unknown_count`` counts every observed reaction whose token
    envelope was parsed but whose ``cost_usd`` field is ``None`` (the
    gemini free-tier path). The verdict can then surface the gap
    instead of silently treating unknown-cost drivers as free.

    ``invariant_failure_count`` counts every observed reaction whose
    token envelope surfaced a moderation refusal
    (``stop_reason="refusal"``), an upstream error
    (``is_error=True``), or a documented
    ``terminal_reason="error_during_execution"`` envelope shape
    (the Anthropic refusal terminal-state marker the synthetic refusal
    fixture documents), PLUS every reaction whose wake-marker
    ``token_usage`` payload failed to parse
    (``token_usage_parse_failed=True``) -- a malformed envelope is an
    invariant violation, not a clean no-op, and counting it here makes
    it observable in the verdict rather than silently dropped.
    """
    cost_unknown = 0
    invariant_failures = 0
    for point in points:
        for reaction in point.observed_reactions:
            if reaction.token_usage_parse_failed:
                invariant_failures += 1
                continue
            usage = reaction.token_usage
            if usage is None:
                continue
            if usage.cost_usd is None:
                cost_unknown += 1
            if envelope_is_refusal(usage):
                invariant_failures += 1
    return cost_unknown, invariant_failures


def _spawn_real_driver(
    *,
    framework: str,
    fw_id: str,
    socket_path: Path,
    db_path: Path,
    doorbell_path: Path,
    seed_scope_id: str,
    env: dict[str, str],
    python_exe: str,
    stderr_dir: Path,
    since: str | None = None,
    cold_prefix: str = "",
) -> _Child:
    """Spawn one real-mode driver subprocess for the given framework.

    Each driver inherits its own pipe for stdout (the orchestrator
    parses the wake marker line back) and writes stderr to a
    per-driver file under ``stderr_dir`` so a silent driver-side
    crash leaves a triage breadcrumb instead of vanishing into
    ``DEVNULL``. The caller is responsible for opening / closing
    that file; here we just route the child's stderr to it.

    ``since`` is the waitbus replay cursor (ULID event_id) appended to the
    driver argv as ``--since <event_id>`` when non-None; absent (default)
    leaves the driver to subscribe from the live watermark.

    ``cold_prefix`` is a per-iteration cache-buster appended as
    ``--cold-prefix <prefix>`` when non-empty; the driver prepends it to the
    LLM prompt so a separate benchmark process cannot hit this run's cached
    prompt prefix under the same API key. Empty (default) keeps the canonical
    prompt -- the stress harness path passes nothing.
    """
    # The driver subcommand IS the framework name (the entry-point
    # dispatch table in _real_drivers keys directly on it), so the
    # framework doubles as the role positional with no lookup table.
    argv = [
        python_exe,
        "-m",
        "scripts.stress._real_drivers",
        framework,
        "--socket",
        str(socket_path),
        "--db",
        str(db_path),
        "--doorbell",
        str(doorbell_path),
        "--seed-scope-id",
        seed_scope_id,
        "--fw-id",
        fw_id,
    ]
    if since is not None:
        argv.extend(["--since", since])
    if cold_prefix:
        argv.extend(["--cold-prefix", cold_prefix])
    stderr_path = stderr_dir / f"driver-{framework}-{fw_id}.err"
    # Open the per-driver stderr file owned by the spawned process; close
    # the parent FD after Popen has dup'd it so we do not leak a writer.
    stderr_fh = stderr_path.open("wb")
    try:
        proc: subprocess.Popen[bytes] = subprocess.Popen(
            argv,
            env=env,
            stdout=subprocess.PIPE,
            stderr=stderr_fh,
            start_new_session=True,
        )
    finally:
        stderr_fh.close()
    return _Child(role=f"{framework}-{fw_id}", proc=proc, framework=framework)


def spawn_n_heterogeneous(
    n: int,
    *,
    env: dict[str, str],
    socket_path: Path,
    db_path: Path,
    doorbell_path: Path,
    seed_scope_id: str,
    python_exe: str,
    stderr_dir: Path,
    since: str | None = None,
) -> tuple[list[_Child], dict[str, int]]:
    """Spawn N real-mode drivers split equally across the five frameworks.

    Returns the spawned children plus the realized framework mix
    (``{framework_name: spawned_count}``); the caller threads the
    mix into the ``RealCurvePoint``.

    ``since`` is the waitbus replay cursor (ULID event_id) threaded into
    every spawned driver via ``_spawn_real_driver``; absent (default)
    leaves the drivers to subscribe from the live watermark.
    """
    mix = framework_mix_for(n)
    children: list[_Child] = []
    counter = 0
    for framework in FRAMEWORK_ORDER:
        for _ in range(mix[framework]):
            counter += 1
            fw_id = str(counter)  # bare ordinal; see _Child.framework for why no prefix
            children.append(
                _spawn_real_driver(
                    framework=framework,
                    fw_id=fw_id,
                    socket_path=socket_path,
                    db_path=db_path,
                    doorbell_path=doorbell_path,
                    seed_scope_id=seed_scope_id,
                    env=env,
                    python_exe=python_exe,
                    stderr_dir=stderr_dir,
                    since=since,
                )
            )
    return children, mix


def _emit_seed_event(
    *,
    seed_scope_id: str,
    db_path: Path,
    doorbell_path: Path,
    source: str = SEED_SOURCE,
    event_type: str = SEED_EVENT_TYPE,
) -> tuple[str, int, int]:
    """Emit one seed event the drivers wake on.

    Returns ``(delivery_id, wall_ns, monotonic_ns)``. ``wall_ns`` is the
    wall-clock anchor a downstream consumer uses to align with the
    driver-side ``received_wall_ns``; ``monotonic_ns`` is the
    cross-process Linux ``CLOCK_MONOTONIC`` anchor the orchestrator uses
    to compute bus ingest latency (``wake_monotonic_ns - monotonic_ns``)
    free of wall-clock skew.

    The seed defaults to the existing ``(agent, agent_message)`` pair
    but accepts an explicit ``(source, event_type)`` so the bench can
    sweep the picked weighted-source pair across iterations. Every
    framework's driver predicate is owner-only, so a non-default source
    still routes through the daemon's fan-out to the driver's
    ``wait_for`` cleanly.
    """
    delivery_id = f"stress-real-seed:{uuid.uuid4()}"
    wall_ns = time.time_ns()
    monotonic_ns = time.monotonic_ns()
    emit(
        EventInsert(
            delivery_id=delivery_id,
            source=source,
            event_type=event_type,
            owner=seed_scope_id,
            repo="stress",
            received_at=wall_ns,
            payload_json='{"kind": "stress_real_seed"}',
            ingest_method="waitbus_stress_real_controller",
        ),
        db_path=db_path,
        doorbell_path=doorbell_path,
    )
    return delivery_id, wall_ns, monotonic_ns


def _emit_anchor_event(
    *,
    seed_scope_id: str,
    db_path: Path,
    doorbell_path: Path,
) -> str:
    """Emit the stress controller's replay anchor; return its ``event_id``.

    Thin stress-identity adapter over
    ``benchmarks._bench_anchor.emit_anchor_event`` (the shared anchor
    primitive). Stamps the controller's own provenance (``repo="stress"``,
    ``ingest_method="waitbus_stress_real_controller"``) so the anchor row is
    attributed to the stress run, not to a bench that reuses the primitive. See
    ``emit_anchor_event`` for the replay-cursor rationale.
    """
    from benchmarks._bench_anchor import emit_anchor_event

    return emit_anchor_event(
        seed_scope_id=seed_scope_id,
        db_path=db_path,
        doorbell_path=doorbell_path,
        repo="stress",
        ingest_method="waitbus_stress_real_controller",
        delivery_id_prefix="stress-real-anchor",
    )


def _extract_marker_pair(
    text: str,
    *,
    seed_delivery_id: str,
) -> tuple[dict[str, Any] | None, int]:
    """Locate the canonical + early wake markers in one driver's stdout.

    Returns ``(canonical_fields, wake_monotonic_ns)``:

    - ``canonical_fields`` is the JSON-bodied ``DRIVER_REACTED`` marker
      parsed via ``_parse_wake_marker``, or ``None`` when no canonical
      marker arrived (a driver-side crash before the post-LLM emit).
    - ``wake_monotonic_ns`` is the driver's pre-LLM monotonic anchor
      from the ``WAKE_RECEIVED`` early marker, or ``0`` when the early
      marker is missing (the sentinel for "no monotonic anchor
      recorded" the latency aggregator coerces to 0.0).

    Both lookups gate on ``seed`` matching ``seed_delivery_id`` so a
    re-used subprocess that somehow leaked a stale buffer cannot
    cross-pollute a fresh iteration's row -- the seed is fresh per
    iteration so the match is exact.

    Extracted from ``_collect_observed_reactions`` so the marker-pair
    parsing logic is unit-testable in isolation and the collector
    drops below the scripts/ D+ complexity ratchet.
    """
    canonical: dict[str, Any] | None = None
    for line in text.splitlines():
        if not line.startswith(WAKE_MARKER):
            continue
        parsed = _parse_wake_marker(line)
        if parsed is None or parsed.get("seed") != seed_delivery_id:
            continue
        canonical = parsed
        break
    wake_monotonic_ns = 0
    for line in text.splitlines():
        if not line.startswith(EARLY_WAKE_MARKER):
            continue
        early = _parse_wake_marker(line, prefix=EARLY_WAKE_MARKER)
        if early is None or early.get("seed") != seed_delivery_id:
            continue
        wake_monotonic_ns = int(early.get("wake_monotonic_ns") or 0)
        break
    return canonical, wake_monotonic_ns


def _collect_observed_reactions(
    children: list[_Child],
    *,
    seed_delivery_id: str,
    seed_emit_monotonic_ns: int,
    expected_n: int,
    deadline_monotonic: float,
) -> list[ObservedReaction]:
    """Drain every driver's stdout CONCURRENTLY for its wake-marker pair.

    Each driver writes one ``WAKE_RECEIVED ...`` early-wake line and
    one ``DRIVER_REACTED ...`` canonical reaction line; the marker
    pair lookup lives in ``_extract_marker_pair``, the collector
    builds the per-row ``ObservedReaction`` from the parsed fields.
    The per-row ``reaction_latency_ms`` rides the cross-process Linux
    ``CLOCK_MONOTONIC`` (the preflight pins it stable cross-process),
    subtracted from the driver-side ``wake_monotonic_ns`` so the figure
    is jitter-free of LLM-call latency AND of wall-clock skew.

    The drain runs one thread per child via ``drain_children_concurrently``
    so a slow or hung driver (a ``claude -p`` that stalls under host
    contention) cannot consume the shared deadline before its siblings
    are read -- a serial drain would terminate later drivers pre-marker
    and spuriously fail the cross-broadcast proof. Marker parsing is
    order-independent: each child emits only its own two markers.
    """
    drained = drain_children_concurrently(
        children,
        deadline_monotonic=deadline_monotonic,
        min_remaining_sec=_MIN_COMMUNICATE_TIMEOUT_SEC,
        term_grace_sec=_TERM_COMMUNICATE_GRACE_SEC,
    )
    reactions: list[ObservedReaction] = []
    for index in range(len(children)):
        out, _t_observe_ns = drained.get(index, (b"", 0))
        text = out.decode("utf-8", errors="replace") if out else ""

        fields, wake_monotonic_ns = _extract_marker_pair(text, seed_delivery_id=seed_delivery_id)
        if fields is None:
            continue

        received_wall_ns = int(fields.get("wall_ns") or 0)
        reaction_latency_ms = max(0.0, (wake_monotonic_ns - seed_emit_monotonic_ns) / 1e6) if wake_monotonic_ns else 0.0
        framework_value = fields.get("framework", "unknown")
        framework = framework_value if isinstance(framework_value, str) else "unknown"
        fw_id_value = fields.get("fw_id", "unknown")
        fw_id = fw_id_value if isinstance(fw_id_value, str) else "unknown"
        reaction_id_value = fields.get("reaction_id", "unknown")
        reaction_id = reaction_id_value if isinstance(reaction_id_value, str) else "unknown"
        token_usage, token_parse_failed = observed_token_usage_from_marker(fields)
        reactions.append(
            ObservedReaction(
                framework=framework,
                fw_id=fw_id,
                seed_delivery_id=seed_delivery_id,
                reaction_delivery_id=reaction_id,
                received_wall_ns=received_wall_ns,
                reaction_latency_ms=reaction_latency_ms,
                token_usage=token_usage,
                token_usage_parse_failed=token_parse_failed,
                provider=_provider_from_marker(fields),
            )
        )
        # Even after observing the proof, keep collecting the rest of
        # the spawned drivers so the curve point reflects everyone.
        _ = expected_n
    return reactions


def _percentile(values: list[float], p: float) -> float:
    """Linear-interpolated percentile over ``values``; safe on empty input.

    ``p`` is a fraction (0.50 / 0.99). Empty input returns 0.0 -- the
    metric is "unknown", and 0.0 is the conventional sentinel the
    verdict serialises faithfully (rather than raising).
    """
    if not values:
        return 0.0
    values = sorted(values)
    if p <= 0:
        return float(values[0])
    if p >= 1:
        return float(values[-1])
    # statistics.quantiles uses n cut-points; for a single percentile
    # the linear interpolation is clearer inline.
    rank = p * (len(values) - 1)
    lower = int(rank)
    upper = min(lower + 1, len(values) - 1)
    weight = rank - lower
    return float(values[lower] * (1 - weight) + values[upper] * weight)


def _sample_window_reactions(
    drivers: list[_Child],
    *,
    n: int,
    duration_sec: float,
    progress_fh: Any,
    seed_scope_id: str,
    db_path: Path,
    doorbell_path: Path,
    stderr_dir: Path,
    picked_source: str,
    picked_event_type: str,
) -> tuple[str, list[ObservedReaction]]:
    """Emit the seed event and collect every driver's reaction for one window.

    The measurement-sampling body of ``run_real_window``, lifted out so
    the parent stays a thin daemon/spawn/cleanup lifecycle. ``drivers``
    are already spawned, settled, and registered in the parent's cleanup
    list; this only seeds the bus and harvests the reactions, then
    surfaces any short-fall driver stderr tails. Returns
    ``(seed_delivery_id, reactions)``; the seed id rides into the curve
    point so a consumer can join a reaction back to its seed.
    """
    seed_delivery_id, seed_wall_ns, seed_emit_monotonic_ns = _emit_seed_event(
        seed_scope_id=seed_scope_id,
        db_path=db_path,
        doorbell_path=doorbell_path,
        source=picked_source,
        event_type=picked_event_type,
    )
    # ``seed_emit_monotonic_ns`` is the cross-process anchor.
    # The reaction collector subtracts the driver-side
    # ``wake_monotonic_ns`` (early-wake marker) from this anchor
    # so the per-row ``reaction_latency_ms`` stays on Linux
    # ``CLOCK_MONOTONIC`` cross-process -- the same clock the
    # bench's verdict aggregator uses, free of wall-clock skew.
    _ = seed_wall_ns

    # Collect reactions until every driver has either reported or the window expires.
    deadline_monotonic = time.monotonic() + duration_sec
    reactions = _collect_observed_reactions(
        drivers,
        seed_delivery_id=seed_delivery_id,
        seed_emit_monotonic_ns=seed_emit_monotonic_ns,
        expected_n=n,
        deadline_monotonic=deadline_monotonic,
    )

    # If reactions came up short, surface per-driver stderr tails in
    # progress.jsonl so the operator's next triage step is one tail
    # away, not a re-run with custom instrumentation.
    if len(reactions) < n:
        for stderr_file in sorted(stderr_dir.glob("driver-*.err")):
            try:
                tail = stderr_file.read_text(errors="replace").strip()
            except OSError:
                continue
            if not tail:
                continue
            _append_progress(
                progress_fh,
                {
                    "kind": "real_driver_stderr",
                    "n": n,
                    "driver": stderr_file.stem.removeprefix("driver-"),
                    "stderr_tail": tail[-_STDERR_TAIL_BYTES:],
                },
            )

    return seed_delivery_id, reactions


def _assemble_real_curve_point(
    reactions: list[ObservedReaction],
    *,
    n: int,
    mix: dict[str, int],
    seed_delivery_id: str,
    duration_sec: float,
    auth_provenance: dict[str, str],
    progress_fh: Any,
) -> RealCurvePoint:
    """Fold the collected reactions into the window's ``RealCurvePoint``.

    The roll-up tail of ``run_real_window``: latency percentiles,
    unique-framework tally, token-usage roll-up
    (mirrors ``_roll_up_token_usage``'s style), the cross-broadcast-proven
    flag, and the curve-point progress record. Pure over its inputs --
    no I/O beyond the one progress append -- so the parent's lifecycle
    body reads as spawn -> sample -> assemble.
    """
    unique_frameworks = {r.framework for r in reactions}
    latencies = [r.reaction_latency_ms for r in reactions]
    median_latency = float(statistics.median(latencies)) if latencies else 0.0
    p99_latency = _percentile(latencies, _P99_FRACTION)

    total_usage = _roll_up_token_usage(reactions)

    proven = len(reactions) == n and len(unique_frameworks) == len(FRAMEWORK_ORDER)

    curve_point = RealCurvePoint(
        n=n,
        framework_mix=dict(mix),
        seed_delivery_id=seed_delivery_id,
        observed_reactions=tuple(reactions),
        cross_broadcast_proven=proven,
        unique_frameworks_observed=len(unique_frameworks),
        reactions_received=len(reactions),
        reactions_expected=n,
        median_reaction_latency_ms=median_latency,
        p99_reaction_latency_ms=p99_latency,
        total_token_usage=total_usage,
        duration_window_sec=duration_sec,
        auth_provenance=dict(auth_provenance),
    )
    _append_progress(
        progress_fh,
        {
            "kind": "real_curve_point",
            "n": n,
            "reactions_received": len(reactions),
            "reactions_expected": n,
            "unique_frameworks_observed": len(unique_frameworks),
            "cross_broadcast_proven": proven,
            "median_reaction_latency_ms": median_latency,
            "p99_reaction_latency_ms": p99_latency,
        },
    )
    return curve_point


def run_real_window(
    n: int,
    *,
    env: dict[str, str],
    duration_sec: float,
    progress_fh: Any,
    waitbus_path: str,
    python_exe: str,
    auth_provenance: dict[str, str],
    picked_source: str = SEED_SOURCE,
    picked_event_type: str = SEED_EVENT_TYPE,
) -> tuple[RealCurvePoint, dict[str, int]]:
    """Run one real-mode measurement window at concurrency N.

    Spawns the daemon, N heterogeneous drivers (equal-mix split),
    emits one seed event, collects every driver's wake-marker
    reaction back on the same bus, and folds them into a
    ``RealCurvePoint`` carrying the cross-broadcast-proven flag,
    per-framework reaction tally, latency percentiles, and rolled-up
    token usage.

    Returns ``(curve_point, close_reasons_tally)`` to mirror
    ``_run_per_n_window``'s shape.

    ``picked_source`` / ``picked_event_type`` are the deterministic
    per-iteration draw from
    ``benchmarks._bench_source_mix.pick_source_for_iter`` that the sweep
    threads in. Defaults to the historical ``(agent, agent_message)``
    pair so any unit-shaped test that constructs a window directly
    keeps its byte-identical behaviour; the sweep wrapper always
    overrides.
    """
    socket_path = Path(env["WAITBUS_RUNTIME_DIR"]) / "broadcast.sock"
    db_path = Path(env["WAITBUS_STATE_DIR"]) / "github.db"
    doorbell_path = Path(env["WAITBUS_RUNTIME_DIR"]) / "doorbell.sock"
    # Per-window scope id: rides the existing ``agent_message`` event
    # type (registered with the built-in ``agent`` source) as the
    # ``owner`` field so the driver-side AND predicate
    # ``fields.event_type="agent_message" AND fields.owner="<scope>"``
    # scopes the wake to this window without inventing an event_type
    # the daemon's `_fan_out` would skip (every fanned-out frame is
    # gated by the subscriber's accepted-event_types set).
    seed_scope_id = f"stress-real-{uuid.uuid4().hex[:12]}"
    # Per-driver stderr breadcrumb dir: a silent driver-side crash
    # leaves an .err file the orchestrator surfaces in progress.jsonl
    # when reactions_received < expected (so the next operator does
    # not have to re-run with custom instrumentation to diagnose).
    stderr_dir = Path(env["WAITBUS_RUNTIME_DIR"]) / "driver-stderr"
    stderr_dir.mkdir(parents=True, exist_ok=True)

    daemon = _spawn(
        "daemon",
        [waitbus_path, "broadcast", "serve"],
        env,
    )
    children: list[_Child] = [daemon]
    try:
        # Wait for the daemon to bind.
        deadline = time.monotonic() + _DAEMON_READY_TIMEOUT_SEC
        while time.monotonic() < deadline:
            if socket_path.exists():
                break
            time.sleep(_SOCKET_POLL_INTERVAL_SEC)
        else:
            raise RuntimeError(f"daemon failed to bind {socket_path} within {_DAEMON_READY_TIMEOUT_SEC:.0f}s")

        # Mint the replay anchor BEFORE spawning the drivers so every
        # driver's ``wait_for(since=anchor_event_id)`` subscribes with
        # a seq cursor that bounds the daemon's replay window. A driver
        # whose subscribe registers after the seed lands (cold-import
        # jitter, scheduler contention) still receives the seed via
        # replay; the spawn settle below preserves measurement
        # integrity by ensuring the common-case delivery is the live
        # ``_fan_out`` path, with replay as the jitter safety net.
        anchor_event_id = _emit_anchor_event(
            seed_scope_id=seed_scope_id,
            db_path=db_path,
            doorbell_path=doorbell_path,
        )

        # Spawn N heterogeneous drivers; settle then emit the seed.
        drivers, mix = spawn_n_heterogeneous(
            n,
            env=env,
            socket_path=socket_path,
            db_path=db_path,
            doorbell_path=doorbell_path,
            seed_scope_id=seed_scope_id,
            python_exe=python_exe,
            stderr_dir=stderr_dir,
            since=anchor_event_id,
        )
        children.extend(drivers)
        time.sleep(_REAL_DRIVER_SPAWN_SETTLE_SEC)

        _append_progress(
            progress_fh,
            {
                "kind": "real_window_seeding",
                "n": n,
                "framework_mix": mix,
                "seed_scope_id": seed_scope_id,
                "anchor_event_id": anchor_event_id,
            },
        )

        seed_delivery_id, reactions = _sample_window_reactions(
            drivers,
            n=n,
            duration_sec=duration_sec,
            progress_fh=progress_fh,
            seed_scope_id=seed_scope_id,
            db_path=db_path,
            doorbell_path=doorbell_path,
            stderr_dir=stderr_dir,
            picked_source=picked_source,
            picked_event_type=picked_event_type,
        )

        curve_point = _assemble_real_curve_point(
            reactions,
            n=n,
            mix=mix,
            seed_delivery_id=seed_delivery_id,
            duration_sec=duration_sec,
            auth_provenance=auth_provenance,
            progress_fh=progress_fh,
        )
        return curve_point, {}
    finally:
        for child in reversed(children):
            child.terminate()
        # Best-effort cleanup of the per-N daemon's seed-state so the
        # next window starts fresh. Sockets/db are inside the temp
        # root, so explicit teardown is sufficient.
        for stale in (socket_path, doorbell_path):
            with contextlib.suppress(FileNotFoundError):
                stale.unlink()


def _run_real_mode_sweep(
    *,
    sweep: tuple[int, ...],
    env: dict[str, str],
    duration_sec: float,
    progress_fh: Any,
    waitbus_path: str,
    auth_provenance: dict[str, str],
) -> tuple[list[RealCurvePoint], list[StressSignalFailure], dict[str, int]]:
    """Iterate the real-mode driver windows across the sweep.

    Each per-N window produces one ``RealCurvePoint``; a window that
    fails the cross-broadcast proof contributes one
    ``StressSignalFailure``. The caller folds the lists into the
    verdict document.

    The per-window seed event's ``(source, event_type)`` pair is drawn
    deterministically per iter_id via ``pick_source_for_iter`` so the
    sweep exercises the full registered soak taxonomy (github /
    pytest / docker / fs / agent) rather than the narrow historical
    ``(agent, agent_message)`` slice. The returned histogram captures
    the realised distribution across the windows so the verdict can
    surface which source pairs were exercised.
    """
    real_curve_points: list[RealCurvePoint] = []
    failures: list[StressSignalFailure] = []
    per_iter_source_distribution: dict[str, int] = {}
    for iter_id, n in enumerate(sweep):
        picked_source, picked_event_type = pick_source_for_iter(iter_id)
        per_iter_source_distribution[picked_source] = per_iter_source_distribution.get(picked_source, 0) + 1
        try:
            real_point, _close_reasons = run_real_window(
                n,
                env=env,
                duration_sec=duration_sec,
                progress_fh=progress_fh,
                waitbus_path=waitbus_path,
                python_exe=sys.executable,
                auth_provenance=auth_provenance,
                picked_source=picked_source,
                picked_event_type=picked_event_type,
            )
        except Exception as exc:
            # Fault isolation: one window's daemon-bind / spawn failure must
            # not crash the whole sweep. Surface the failure LOUDLY (it flips
            # overall_passed to False via the recorded StressSignalFailure)
            # but let the remaining sweep windows run rather than losing the
            # entire verdict to a single transient daemon-bind or spawn error.
            _append_progress(
                progress_fh,
                {"kind": "real_window_error", "n": n, "error": f"{type(exc).__name__}: {exc}"},
            )
            failures.append(
                StressSignalFailure(
                    signal="real_window_error",
                    threshold=float(n),
                    observed=0.0,
                    detail=f"n={n}: {type(exc).__name__}: {exc}",
                )
            )
            continue
        real_curve_points.append(real_point)
        if not real_point.cross_broadcast_proven:
            failures.append(
                StressSignalFailure(
                    signal="cross_broadcast_proof",
                    threshold=float(n),
                    observed=float(real_point.reactions_received),
                    detail=(
                        f"n={n}: reactions_received={real_point.reactions_received}, "
                        f"unique_frameworks_observed={real_point.unique_frameworks_observed}"
                    ),
                )
            )
    return real_curve_points, failures, per_iter_source_distribution


def _fit_usl_and_record(
    curve_points: list[CurvePoint],
    *,
    progress_fh: Any,
) -> tuple[float | None, float | None, float | None, float | None, float | None, list[StressSignalFailure]]:
    """Fit Gunther's USL across the curve points and record the result.

    Returns ``(alpha, beta, gamma, knee_concurrency, knee_throughput,
    failures)``. Fewer than ``_USL_MIN_POINTS`` viable points leaves
    every fit field None; the failures list contains one entry if
    curve_fit raised, otherwise empty.
    """
    failures: list[StressSignalFailure] = []
    viable = [(p.n, p.throughput_hz) for p in curve_points if p.throughput_hz > 0]
    if len(viable) < _USL_MIN_POINTS:
        return None, None, None, None, None, failures
    try:
        fit: USLFit = fit_usl([n for n, _ in viable], [t for _, t in viable])
    except (ValueError, RuntimeError) as exc:
        failures.append(
            StressSignalFailure(
                signal="usl_fit",
                threshold=0.0,
                observed=0.0,
                detail=f"fit_usl raised: {exc}",
            )
        )
        return None, None, None, None, None, failures
    alpha, beta, gamma = fit.alpha, fit.beta, fit.gamma
    knee_n = knee(alpha, beta)
    knee_throughput: float | None = None
    if knee_n is not None:
        knee_throughput = float(gamma) * knee_n / (1.0 + alpha * (knee_n - 1.0) + beta * knee_n * (knee_n - 1.0))
    _append_progress(
        progress_fh,
        {
            "kind": "usl_fit",
            "alpha": alpha,
            "beta": beta,
            "gamma": gamma,
            "residuals_rss": fit.residuals_rss,
            "knee_concurrency": knee_n,
            "knee_throughput_hz": knee_throughput,
        },
    )
    return alpha, beta, gamma, knee_n, knee_throughput, failures


def _run_offline_curve_sweep(
    *,
    sweep: tuple[int, ...],
    env: dict[str, str],
    waitbus_path: str,
    duration_sec: float,
    progress_fh: Any,
    accums: _StressAccumulators,
) -> None:
    """Iterate the offline subscriber-count sweep and append each curve point."""
    for n in sweep:
        curve_point, _close_reasons = _run_per_n_window(
            n,
            waitbus_path=waitbus_path,
            env=env,
            duration_sec=duration_sec,
            progress_fh=progress_fh,
        )
        accums.curve_points.append(curve_point)
        _append_progress(progress_fh, {"kind": "curve_point", **msgspec.to_builtins(curve_point)})


def _build_arg_parser() -> argparse.ArgumentParser:
    """Build the controller's argparse parser."""
    parser = argparse.ArgumentParser(
        prog="python -m scripts.stress",
        description="Stress / break harness for the waitbus broadcast bus.",
    )
    parser.add_argument(
        "--sweep",
        type=_parse_sweep,
        default=None,
        help=(
            f"Comma-separated subscriber counts to sweep "
            f"(default {','.join(str(n) for n in _DEFAULT_SWEEP_N)} offline, "
            f"{','.join(str(n) for n in _DEFAULT_REAL_SWEEP_N)} real)."
        ),
    )
    parser.add_argument(
        "--duration",
        type=str,
        default=_DEFAULT_DURATION,
        help=f"Per-N measurement window (default {_DEFAULT_DURATION}). Accepts s/m/h/ms suffixes.",
    )
    parser.add_argument(
        "--signals",
        type=_parse_signals,
        default=_DEFAULT_SIGNALS,
        help=f"Comma-separated signals to run (default all: {','.join(_DEFAULT_SIGNALS)}).",
    )
    parser.add_argument(
        "--real",
        action="store_true",
        help="Real-mode (real claude -p / gemini -p / OpenAI drivers; gated on auth + PATH).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path.cwd() / "stress-verdict.json",
        help="Verdict JSON output path (default ./stress-verdict.json).",
    )
    parser.add_argument(
        "--progress",
        type=Path,
        default=None,
        help="Per-tick progress JSONL output path (default sibling of --output).",
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=None,
        help="Daemon state directory (default temp dir; recreated each run).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    duration_sec = _parse_duration(args.duration)
    progress_path = args.progress if args.progress is not None else args.output.with_suffix(".progress.jsonl")
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)

    mode = "real" if args.real else "offline"
    waitbus_path = str(Path(sys.executable).parent / "waitbus")

    # Resolve the per-mode default sweep. Real-mode runs cost LLM tokens
    # per driver; the default sweep stays small (N=5, N=10) so an
    # accidental ``waitbus stress --real`` does not burn unbounded tokens.
    if args.sweep is None:
        args.sweep = _DEFAULT_REAL_SWEEP_N if args.real else _DEFAULT_SWEEP_N

    # Fail-fast auth check before any sweep work. Real-mode aborts
    # rather than skipping a missing CLI -- the operator-decided
    # 2026-06-01 policy keeps a silently-degraded run from looking
    # like a clean pass.
    # The frameworks the sweep will actually spawn, across every N. The
    # auth-smoke OpenAI-key requirement engages only when this set contains
    # an OpenAI-backed role (pydantic / langgraph); a sweep that spawns only
    # claude-cli / gemini-cli / shell-control roles does not require the key.
    active_frameworks: set[str] = set()
    for n in args.sweep:
        active_frameworks.update(fw for fw, count in framework_mix_for(n).items() if count > 0)

    auth_provenance: dict[str, str] = {}
    if args.real:
        try:
            auth_provenance = auth_smoke_check(frameworks=active_frameworks)
        except RuntimeError as exc:
            print(f"[waitbus stress] real-mode auth check failed: {exc}", file=sys.stderr)
            return 2

    with tempfile.TemporaryDirectory(prefix="waitbus-stress-") as tmp_root:
        root = Path(tmp_root)
        state_dir = args.state_dir if args.state_dir is not None else (root / "state")
        runtime_dir = root / "runtime"
        state_dir.mkdir(parents=True, exist_ok=True)
        runtime_dir.mkdir(parents=True, exist_ok=True)

        env = dict(os.environ)
        env["WAITBUS_STATE_DIR"] = str(state_dir)
        env["WAITBUS_RUNTIME_DIR"] = str(runtime_dir)
        env["WAITBUS_DISABLE_SOURCE_AUTOLOAD"] = "1"
        if args.real:
            # Signal the spawned driver subprocesses they are under REAL mode so
            # the OpenAI-backed selectors hard-fail on an absent OPENAI_API_KEY
            # instead of silently substituting an offline fake. The driver has
            # no other in-band mode signal (its argv carries no --real flag).
            env[REAL_MODE_ENV_VAR] = "1"

        ctx = _StressContext(
            proc=None,
            db_path=state_dir / "github.db",
            progress_path=progress_path,
            socket_path=runtime_dir / "broadcast.sock",
            daemon_stderr_path=runtime_dir / "daemon.err",
            args=args,
            start_monotonic=time.monotonic(),
            started_at_ns=time.time_ns(),
            total_seconds=duration_sec * len(args.sweep),
            mode=mode,
            sweep_n=args.sweep,
            corpus_iter=None,
            progress_fh=None,
        )

        accums = _StressAccumulators()
        failures: list[StressSignalFailure] = []
        usl_alpha: float | None = None
        usl_beta: float | None = None
        usl_gamma: float | None = None
        knee_n: float | None = None
        knee_throughput: float | None = None
        real_curve_points: list[RealCurvePoint] = []

        with progress_path.open("w", encoding="utf-8") as progress_fh:
            _append_progress(
                progress_fh,
                {
                    "kind": "start",
                    "mode": mode,
                    "sweep": list(args.sweep),
                    "duration_sec": duration_sec,
                    "signals": list(args.signals),
                    "delivery_proof_id": f"stress:{uuid.uuid4()}",
                    "auth_provenance": auth_provenance,
                },
            )

            per_iter_source_distribution: dict[str, int] = {}
            if args.real:
                # Real-mode dispatch: heterogeneous-driver cross-broadcast
                # proof over the configured sweep. Bypasses the offline
                # curve / USL fit (those measure synthetic fan-out cost
                # at much higher N with cheap subscribers).
                real_curve_points, real_failures, per_iter_source_distribution = _run_real_mode_sweep(
                    sweep=args.sweep,
                    env=env,
                    duration_sec=duration_sec,
                    progress_fh=progress_fh,
                    waitbus_path=waitbus_path,
                    auth_provenance=auth_provenance,
                )
                failures.extend(real_failures)
            elif "curve" in args.signals:
                _run_offline_curve_sweep(
                    sweep=args.sweep,
                    env=env,
                    waitbus_path=waitbus_path,
                    duration_sec=duration_sec,
                    progress_fh=progress_fh,
                    accums=accums,
                )
                usl_alpha, usl_beta, usl_gamma, knee_n, knee_throughput, usl_failures = _fit_usl_and_record(
                    accums.curve_points,
                    progress_fh=progress_fh,
                )
                failures.extend(usl_failures)

            cost_unknown_count, invariant_failure_count = _summarize_real_curve_points(real_curve_points)
            provider_distribution = _provider_distribution(real_curve_points)
            # An invariant failure (a refusal or upstream error) flips
            # the overall-passed verdict to false even when the cross-
            # broadcast proof passed; a moderation refusal is a real
            # failure mode the verdict must surface, not a silent
            # success.
            overall_passed = not failures and invariant_failure_count == 0
            doc = _compute_verdict_doc(
                ctx,
                accums,
                overall_passed=overall_passed,
                failures=tuple(failures),
                usl_alpha=usl_alpha,
                usl_beta=usl_beta,
                usl_gamma=usl_gamma,
                knee_concurrency=knee_n,
                knee_throughput_hz=knee_throughput,
                real_curve_points=tuple(real_curve_points),
                cost_unknown_count=cost_unknown_count,
                invariant_failure_count=invariant_failure_count,
                provider_distribution=provider_distribution,
                per_iter_source_distribution=per_iter_source_distribution,
            )
            _append_progress(progress_fh, {"kind": "end", "overall_passed": overall_passed})

        _write_verdict(args.output, doc)
        print(f"[waitbus stress] verdict: {args.output}", file=sys.stderr)
        return 0 if overall_passed else 1


__all__ = ["_VerdictDoc", "main"]


if __name__ == "__main__":
    sys.exit(main())
