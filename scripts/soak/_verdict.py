"""Soak verdict computation: integrity check + sample-write + JSON output.

Imports ``_context`` for the structs and ``_emit`` for the default
source-mix constant.  Owns ``_collect_sample_or_partial`` so the
suspend-cycle module can call it without creating a cycle back through
the orchestrator.
"""

from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any

import msgspec

from scripts.soak._context import (
    SoakSignalFailure,
    _SoakAccumulators,
    _SoakContext,
    _VerdictDoc,
)
from scripts.soak._emit import _DEFAULT_SOURCE_MIX
from scripts.soak._fault_injection import fault_injection_coverage_threshold
from scripts.soak_monitor import (
    Sample,
    ThresholdVerdict,
    collect_sample,
    fd_growth_threshold,
    gc_threshold,
    log_size_threshold,
    p99_drift_threshold,
    per_source_share_threshold,
    rss_slope_threshold,
    wal_size_threshold,
)


def _check_integrity(db_path: Path) -> tuple[bool, str]:
    """Return ``(ok, reason)`` after running ``PRAGMA integrity_check``.

    Opens a read-only connection (``mode=ro``, ``uri=True``) so the
    integrity check does not race the live daemon's writes. Any
    sqlite3 error (lock contention, missing file, malformed schema)
    surfaces as ``(False, reason)`` so the suspend-recovery verdict
    fails loud and the suspend-outcome record carries an operator-
    readable diagnostic string.

    Reason strings:
    - ``"ok"`` -- integrity check passed
    - ``"missing"`` -- ``db_path`` does not exist
    - ``"locked"`` -- sqlite3 lock or I/O error
    - ``"integrity_check returned <value>"`` -- unexpected PRAGMA result
    """
    if not db_path.exists():
        return False, "missing"
    try:
        uri = f"file:{db_path}?mode=ro"
        with sqlite3.connect(uri, uri=True, timeout=5.0) as conn:
            row = conn.execute("PRAGMA integrity_check").fetchone()
    except sqlite3.Error:
        return False, "locked"
    if row is None:
        return False, "integrity_check returned None"
    if row[0] == "ok":
        return True, "ok"
    return False, f"integrity_check returned {row[0]!r}"


def _count_close_reasons(stderr_path: Path) -> dict[str, int]:
    """Tally ``subscriber_closed`` events by reason from the daemon's stderr log.

    The broadcast daemon configures ``format="%(message)s"`` to stderr and
    every structured event is one compact JSON object per line, so each
    line parses directly. Lines that are not JSON (or carry no
    ``subscriber_closed`` event) are skipped. A missing file (daemon never
    closed a subscriber, or stderr capture was disabled) yields an empty
    tally rather than an error. This surfaces the daemon-internal
    close-reason vocabulary (``lag_limit_exceeded``, ``heartbeat_lag``,
    ``replay_lag_limit_exceeded``, ``replay_db_error``, ``shutdown``,
    ``subscribe_ack_send_failed``) in the verdict for operator inspection,
    matching the ``waitbus_subscriber_evicted_total{reason}`` metric the
    daemon increments at the same site.
    """
    counts: dict[str, int] = {}
    try:
        text = stderr_path.read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return counts
    for line in text.splitlines():
        line = line.strip()
        if not line or '"subscriber_closed"' not in line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if record.get("event") != "subscriber_closed":
            continue
        reason = record.get("reason")
        if isinstance(reason, str):
            counts[reason] = counts.get(reason, 0) + 1
    return counts


def _append_progress(fh: Any, record: dict[str, Any]) -> None:
    """Append one JSON record + newline to the long-lived progress file handle.

    Each record carries enough context (kind, ts, raw counters) for a
    crash-recovery tool to reconstruct the soak's trajectory from the
    JSONL alone.  The long-lived FD with explicit ``flush()`` per record
    eliminates the open/close I/O cost that accumulates over a 24-hour
    run (one open+close per sample would add measurable overhead at the
    default 60 s cadence, and is unnecessary given the FD is held for
    the lifetime of the soak).  A ``tail -F`` on the file sees each
    sample the moment it is captured because the explicit flush writes
    the kernel buffer immediately.
    """
    fh.write(json.dumps(record) + "\n")
    fh.flush()


def _stderr_sample_line(sample: Sample, *, offset_sec: float, kind: str) -> None:
    """Print one human-readable line per sample so an operator tailing
    stderr can see drift in real time.

    Format prioritises eye-scanning over machine-parsing (the JSONL
    log is for parsing). Hours / MiB units make a 24-hour run
    legible at a glance.
    """
    print(
        f"[soak] t={offset_sec / 3600:7.3f}h  rss={sample.rss_bytes / (1024 * 1024):7.2f} MiB"
        f"  fd={sample.fd_count:4d}  wal={sample.wal_bytes / (1024 * 1024):6.2f} MiB"
        f"  ({kind})",
        file=sys.stderr,
        flush=True,
    )


def _collect_sample_or_partial(
    proc: subprocess.Popen[bytes],
    db_path: Path,
    *,
    offset_sec: float,
    kind: str,
    progress_path: Path,
) -> Sample | None:
    """Collect one monitor sample, returning ``None`` if the daemon has vanished.

    The ``None`` return signals daemon death: the caller should set
    ``is_partial=True`` and break the main loop.  Prints an operator-
    readable ``[soak] daemon process gone...`` line on the ``None`` path.
    """
    try:
        return collect_sample(pid=proc.pid, db_path=db_path)
    except (FileNotFoundError, RuntimeError) as exc:
        print(
            f"[soak] daemon process gone at {kind} (offset {offset_sec:.1f}s; {exc}); writing partial verdict",
            file=sys.stderr,
        )
        return None


def _compute_verdict_doc(
    ctx: _SoakContext,
    accums: _SoakAccumulators,
    *,
    ended_at_ns: int,
    events_emitted: int,
    is_partial: bool,
    subscriber_close_reasons: dict[str, int] | None = None,
) -> _VerdictDoc:
    """Build the typed verdict document from current samples.

    Reused by checkpoint writes (mid-run) and end-of-run.  The
    ``is_partial`` flag marks checkpoints written mid-run so a
    consumer of the JSON (an operator inspecting a partial, a crash
    forensic tool) can tell the difference between "soak finished
    cleanly" and "soak crashed at sample N, this is what we had".

    Eight verdict signals are invoked per the soak design decision:
    RSS slope, FD growth, WAL size, suspend recovery (one per cycle),
    p99 drift, GC, log size, and per-source share. The per-source-
    share check is evaluated against the full-soak source counter
    (not a rolling 30-min window) at end-of-run; checkpoint partials
    surface the same counter for inspection.

    ``subscriber_close_reasons`` overrides the daemon stderr tally when
    provided. The final verdict MUST pass it: ``_count_close_reasons``
    reads ``ctx.daemon_stderr_path``, which lives under the soak's
    ``TemporaryDirectory`` and is deleted before the end-of-run verdict is
    computed -- reading it there would silently yield an empty tally. The
    caller captures it while the file still exists (right after daemon
    teardown). Checkpoint partials run inside the temp-dir block and pass
    ``None``, reading the live file directly.
    """
    samples = accums.rss_samples
    suspend_verdicts = accums.suspend_verdicts
    suspend_recovery_overall = ThresholdVerdict(
        "suspend_recovery",
        all(v.passed for v in suspend_verdicts) if suspend_verdicts else True,
        f"{len(suspend_verdicts)} cycles; {sum(1 for v in suspend_verdicts if not v.passed)} failed"
        if suspend_verdicts
        else "no suspend cycles configured; skipping",
    )
    verdicts: list[ThresholdVerdict] = [
        rss_slope_threshold(samples),
        fd_growth_threshold(samples),
        wal_size_threshold(samples),
        suspend_recovery_overall,
        p99_drift_threshold(accums.p99_samples),
        gc_threshold(accums.gc_samples),
        log_size_threshold(accums.log_size_samples),
        per_source_share_threshold(accums.source_counts, _DEFAULT_SOURCE_MIX),
        fault_injection_coverage_threshold(accums.fault_injection_outcomes, ctx.configured_fault_axes),
    ]
    # Fold the drain-path smoke pre-phase verdicts into the signal list so a
    # mid-run checkpoint and the final verdict both surface them. The
    # pre-phase already passed (a failure aborts before the measured run), so
    # this is a closure record; the gate itself ran before this point.
    drain_smoke = ctx.drain_smoke
    if drain_smoke is not None:
        verdicts.extend(drain_smoke.verdicts)
    overall_passed = all(v.passed for v in verdicts)
    failures = tuple(
        SoakSignalFailure(
            signal=v.signal,
            threshold=0.0,  # threshold value not directly exposed by ThresholdVerdict; use 0.0 as sentinel
            observed=0.0,  # observed value not directly exposed; consumers use detail string
            detail=v.detail,
        )
        for v in verdicts
        if not v.passed
    )
    return _VerdictDoc(
        started_at_ns=ctx.started_at_ns,
        ended_at_ns=ended_at_ns,
        duration_sec=ctx.total_seconds,
        emit_rate_hz=ctx.args.rate,
        sample_interval_sec=ctx.sample_interval_sec,
        n_samples=len(samples),
        events_emitted=events_emitted,
        is_partial=is_partial,
        verdicts=[{"signal": v.signal, "passed": v.passed, "detail": v.detail} for v in verdicts],
        suspend_cycles=accums.suspend_outcomes,
        suspend_recovery_per_cycle=[
            {"signal": v.signal, "passed": v.passed, "detail": v.detail} for v in suspend_verdicts
        ],
        p99_samples_ns=[{"ts_ns": ts, "p99_ns": v} for ts, v in accums.p99_samples],
        gc_samples=[
            {"ts_ns": ts, "uncollectable": uncoll, "collected_cumulative": collected}
            for ts, uncoll, collected in accums.gc_samples
        ],
        log_size_samples=[{"ts_ns": ts, "size_bytes": size} for ts, size in accums.log_size_samples],
        source_counts=dict(accums.source_counts),
        samples=[
            {
                "ts_ns": s.ts_ns,
                "rss_bytes": s.rss_bytes,
                "fd_count": s.fd_count,
                "wal_bytes": s.wal_bytes,
            }
            for s in samples
        ],
        overall_passed=overall_passed,
        failures=failures,
        fault_injection_outcomes=list(accums.fault_injection_outcomes),
        subscriber_close_reasons=(
            subscriber_close_reasons
            if subscriber_close_reasons is not None
            else _count_close_reasons(ctx.daemon_stderr_path)
        ),
        drain_smoke_outcomes=list(drain_smoke.outcomes) if drain_smoke is not None else [],
        drain_smoke_close_reasons=dict(drain_smoke.close_reasons) if drain_smoke is not None else {},
    )


def _compute_drain_smoke_failure_doc(
    drain_smoke: Any,
    *,
    started_at_ns: int,
    ended_at_ns: int,
    duration_sec: float,
    emit_rate_hz: float,
    sample_interval_sec: float,
) -> _VerdictDoc:
    """Build a verdict doc for a FAILED drain-path smoke pre-phase.

    The measured soak never started, so there are no samples; ``is_partial``
    marks the early abort and ``overall_passed`` is False. The pre-phase's
    own verdicts (coverage + close-reason consistency) are the only signals,
    and the probe outcomes / close reasons are carried for forensics.
    """
    verdicts = list(drain_smoke.verdicts)
    failures = tuple(
        SoakSignalFailure(signal=v.signal, threshold=0.0, observed=0.0, detail=v.detail)
        for v in verdicts
        if not v.passed
    )
    return _VerdictDoc(
        started_at_ns=started_at_ns,
        ended_at_ns=ended_at_ns,
        duration_sec=duration_sec,
        emit_rate_hz=emit_rate_hz,
        sample_interval_sec=sample_interval_sec,
        n_samples=0,
        events_emitted=0,
        is_partial=True,
        verdicts=[{"signal": v.signal, "passed": v.passed, "detail": v.detail} for v in verdicts],
        suspend_cycles=[],
        suspend_recovery_per_cycle=[],
        p99_samples_ns=[],
        gc_samples=[],
        log_size_samples=[],
        source_counts={},
        samples=[],
        overall_passed=False,
        failures=failures,
        fault_injection_outcomes=[],
        subscriber_close_reasons={},
        drain_smoke_outcomes=list(drain_smoke.outcomes),
        drain_smoke_close_reasons=dict(drain_smoke.close_reasons),
    )


def _write_verdict(path: Path, doc: _VerdictDoc) -> None:
    """Atomic write of the verdict JSON via tmp + rename."""
    tmp = path.with_suffix(path.suffix + ".partial")
    tmp.write_text(json.dumps(msgspec.to_builtins(doc), indent=2), encoding="utf-8")
    tmp.replace(path)
