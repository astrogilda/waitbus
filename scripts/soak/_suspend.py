"""Soak SIGSTOP/SIGCONT suspend-cycle injection + isolated env context manager.

Imports ``_context`` for the structs and ``_verdict`` for the sampling
helpers + integrity check.
"""

from __future__ import annotations

import contextlib
import os
import signal
import time
import unittest.mock
from collections.abc import Iterator
from pathlib import Path

from scripts.soak._context import (
    SuspendCycle,
    _SoakAccumulators,
    _SoakContext,
    _SoakState,
)
from scripts.soak._verdict import (
    _append_progress,
    _check_integrity,
    _collect_sample_or_partial,
    _stderr_sample_line,
)
from scripts.soak_monitor import suspend_recovery_threshold


@contextlib.contextmanager
def _isolated_waitbus_dirs(
    state_dir: Path,
    runtime_dir: Path,
) -> Iterator[None]:
    """Context manager: align this process's path-resolution with the spawned daemon.

    Sets ``WAITBUS_STATE_DIR`` / ``WAITBUS_RUNTIME_DIR`` env vars
    for the duration of the ``with`` block and restores the prior values
    (or removes the keys) on exit, even if the body raises.

    ``emit_batch`` passes ``db_path`` explicitly, but ``_doorbell.ring()``
    resolves the doorbell socket from ``_paths.runtime_dir()`` with no
    override seam.  Without these env vars, every emit's ring would hit
    the operator's real ``$XDG_RUNTIME_DIR/waitbus/doorbell.sock`` (or
    nothing), the soak's daemon would never wake, and the subscriber
    would observe zero frames -- silently neutralising the p99-drift
    and suspend-recovery verdict signals.

    (``_paths`` factories do not cache, so no companion
    ``_cache_clear()`` invocation is needed; env mutation takes effect
    on the next call.)
    """
    with unittest.mock.patch.dict(
        os.environ,
        {
            "WAITBUS_STATE_DIR": str(state_dir),
            "WAITBUS_RUNTIME_DIR": str(runtime_dir),
        },
        clear=False,
    ):
        yield


def _run_suspend_cycle(
    ctx: _SoakContext,
    state: _SoakState,
    accums: _SoakAccumulators,
    *,
    cycle: SuspendCycle,
    offset_sec: float,
    emit_count: int,
) -> bool:
    """Run one SIGSTOP/SIGCONT suspend cycle.

    Returns ``True`` if the daemon survived the cycle, ``False`` if
    the daemon process is gone (the caller should set
    ``is_partial=True`` and break the main while-loop).
    """
    del state  # signature uniformity with _run_soak_step; state not used here
    pre_p99_ns = ctx.subscriber.snapshot_p99_ns()
    pre_frames_seen = ctx.subscriber.frames_seen
    pre_emit_count = emit_count
    pre_sample = _collect_sample_or_partial(
        ctx.proc, ctx.db_path, offset_sec=offset_sec, kind="suspend pre-sample", progress_path=ctx.progress_path
    )
    if pre_sample is None:
        return False
    _append_progress(
        ctx.progress_fh,
        {
            "kind": "pre_suspend",
            "ts_ns": pre_sample.ts_ns,
            "offset_sec": offset_sec,
            "rss_bytes": pre_sample.rss_bytes,
            "fd_count": pre_sample.fd_count,
            "wal_bytes": pre_sample.wal_bytes,
            "cycle_duration_sec": cycle.duration_sec,
        },
    )
    _stderr_sample_line(pre_sample, offset_sec=offset_sec, kind="pre_suspend")
    ctx.proc.send_signal(signal.SIGSTOP)
    time.sleep(cycle.duration_sec)
    ctx.proc.send_signal(signal.SIGCONT)
    # Brief drain so the daemon catches up on the
    # backlog before sampling. 2 seconds is enough on
    # the synthetic-emitter rate; the real-replayer
    # variant may need more and operators can edit.
    time.sleep(2.0)
    post_sample = _collect_sample_or_partial(
        ctx.proc,
        ctx.db_path,
        offset_sec=offset_sec + cycle.duration_sec + 2.0,
        kind="suspend post-sample",
        progress_path=ctx.progress_path,
    )
    if post_sample is None:
        return False
    _append_progress(
        ctx.progress_fh,
        {
            "kind": "post_suspend",
            "ts_ns": post_sample.ts_ns,
            "offset_sec": offset_sec + cycle.duration_sec + 2.0,
            "rss_bytes": post_sample.rss_bytes,
            "fd_count": post_sample.fd_count,
            "wal_bytes": post_sample.wal_bytes,
        },
    )
    _stderr_sample_line(
        post_sample,
        offset_sec=offset_sec + cycle.duration_sec + 2.0,
        kind="post_suspend",
    )
    post_p99_ns = ctx.subscriber.snapshot_p99_ns()
    post_frames_seen = ctx.subscriber.frames_seen
    integrity_ok, integrity_reason = _check_integrity(ctx.db_path)
    # Events emitted DURING the SIGSTOP+drain window are an
    # expected data-loss class (documented in SOAK_TEST.md);
    # what we care about is whether every event emitted
    # since pre_emit_count reached the subscriber by the
    # time we sampled post_frames_seen. Approximation:
    # events_lost = (emits_since_pre) - (frames_since_pre).
    # A short positive value reflects pipeline depth +
    # in-flight frames at the drain boundary; a sustained
    # large value reflects real loss.
    emits_since_pre = emit_count - pre_emit_count
    frames_since_pre = post_frames_seen - pre_frames_seen
    events_lost = max(0, emits_since_pre - frames_since_pre)
    cycle_verdict = suspend_recovery_threshold(
        pre_suspend_p99_ns=pre_p99_ns,
        post_suspend_p99_ns=post_p99_ns,
        integrity_ok=integrity_ok,
        events_lost_post_resume=events_lost,
    )
    accums.suspend_verdicts.append(cycle_verdict)
    accums.suspend_outcomes.append(
        {
            "offset_sec": cycle.offset_sec,
            "duration_sec": cycle.duration_sec,
            "pre_rss_bytes": pre_sample.rss_bytes,
            "post_rss_bytes": post_sample.rss_bytes,
            "pre_p99_ns": pre_p99_ns,
            "post_p99_ns": post_p99_ns,
            "integrity_ok": integrity_ok,
            "integrity_reason": integrity_reason,
            "events_lost_post_resume": events_lost,
            "verdict_passed": cycle_verdict.passed,
            "verdict_detail": cycle_verdict.detail,
        }
    )
    return True
