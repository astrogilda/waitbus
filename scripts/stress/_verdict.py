"""Stress verdict computation: atomic write + JSONL progress + aggregator.

Lifts the same shape conventions used by ``scripts.soak._verdict`` --
atomic tmp + rename for the verdict JSON, long-lived FD + flush per
record for the progress JSONL -- so a downstream operator can drive
the stress verdict file with the same tooling that already consumes
soak verdicts.

Imports ``_context`` for the structs and reads ``_StressAccumulators``
to fold the per-N curve, fault outcomes, and signal observations into
the typed verdict document.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import msgspec

from scripts.stress._context import (
    RealCurvePoint,
    StressSignalFailure,
    _StressAccumulators,
    _StressContext,
    _VerdictDoc,
)


def _append_progress(fh: Any, record: dict[str, Any]) -> None:
    """Append one JSON record + newline to the long-lived progress file handle.

    Each record carries enough context (kind, ts, raw counters) for a
    crash-recovery tool to reconstruct the stress run's trajectory from
    the JSONL alone. The long-lived FD with explicit ``flush()`` per
    record eliminates the open/close I/O cost that accumulates over a
    long sweep (one open+close per N would add measurable overhead at
    moderate sweep depths and is unnecessary given the FD is held for
    the lifetime of the controller). A ``tail -F`` on the file sees
    each record the moment it is captured because the explicit flush
    writes the kernel buffer immediately.
    """
    fh.write(json.dumps(record) + "\n")
    fh.flush()


def _write_verdict(path: Path, doc: _VerdictDoc) -> None:
    """Atomic write of the verdict JSON via tmp + rename.

    Identical discipline to ``scripts.soak._verdict._write_verdict``:
    a partial-rename window cannot leave a half-written verdict on disk
    for a downstream consumer (CI gate, ``waitbus stress --json`` pipe
    consumer, post-mortem operator) to misinterpret.
    """
    tmp = path.with_suffix(path.suffix + ".partial")
    tmp.write_text(json.dumps(msgspec.to_builtins(doc), indent=2), encoding="utf-8")
    tmp.replace(path)


def _compute_verdict_doc(
    ctx: _StressContext,
    accums: _StressAccumulators,
    *,
    overall_passed: bool,
    failures: tuple[StressSignalFailure, ...] = (),
    usl_alpha: float | None = None,
    usl_beta: float | None = None,
    usl_gamma: float | None = None,
    knee_concurrency: float | None = None,
    knee_throughput_hz: float | None = None,
    zero_polling_verdict: dict[str, Any] | None = None,
    hdr_dump_path: str | None = None,
    real_curve_points: tuple[RealCurvePoint, ...] = (),
    cost_unknown_count: int = 0,
    invariant_failure_count: int = 0,
    provider_distribution: dict[str, int] | None = None,
    per_iter_source_distribution: dict[str, int] | None = None,
) -> _VerdictDoc:
    """Fold ``ctx`` + ``accums`` + caller-provided signal verdicts into a typed doc.

    The caller (``_controller.main``) computes the signal-level
    verdicts via the modules that produce them (the test harness for
    zero-polling, ``_usl`` for the curve fit) and passes them in by
    name. Field names map
    1:1 to ``_VerdictDoc`` fields, so a future field addition in
    ``_context`` is a one-line addition here too -- no parsing.

    ``cost_unknown_count`` / ``invariant_failure_count`` are the
    real-mode-only summary fields the controller derives from the
    per-window observed reactions; default to 0 so the offline-mode
    caller path can omit them.

    ``per_iter_source_distribution`` is the histogram of
    ``(source, event_type)`` draws across the sweep's real-mode
    windows; empty dict on offline-only runs.
    """
    ended_at_ns = time.time_ns()
    duration_sec = (ended_at_ns - ctx.started_at_ns) / 1e9
    return _VerdictDoc(
        started_at_ns=ctx.started_at_ns,
        ended_at_ns=ended_at_ns,
        duration_sec=duration_sec,
        mode=ctx.mode,
        overall_passed=overall_passed,
        failures=failures,
        curve=tuple(accums.curve_points),
        usl_alpha=usl_alpha,
        usl_beta=usl_beta,
        usl_gamma=usl_gamma,
        knee_concurrency=knee_concurrency,
        knee_throughput_hz=knee_throughput_hz,
        zero_polling_verdict=zero_polling_verdict,
        subscriber_close_reasons=dict(accums.subscriber_close_reasons),
        hdr_dump_path=hdr_dump_path,
        real_curve_points=real_curve_points,
        cost_unknown_count=cost_unknown_count,
        invariant_failure_count=invariant_failure_count,
        provider_distribution=dict(provider_distribution) if provider_distribution else {},
        per_iter_source_distribution=dict(per_iter_source_distribution) if per_iter_source_distribution else {},
    )
