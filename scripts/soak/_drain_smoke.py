"""Drain-path smoke pre-phase: exercise every subscriber-lifecycle drain
path against a throwaway low-heartbeat daemon before the measured soak.

The measured soak pins ``WAITBUS_HEARTBEAT_SEC`` to 3600 (via
``spawn_waitbus_daemon``) so heartbeat frames do not disturb its RSS/p99
measurements, which means the ``heartbeat_lag`` eviction path cannot fire
against the measured daemon. This pre-phase spins up a throwaway daemon
with an aggressive sub-second heartbeat, seeds a backlog so the replay
probe has rows to saturate, drives all four wire probes (incl. a real
``heartbeat_lag`` eviction), verifies coverage AND close-reason
consistency, then tears the daemon down, leaving the measured soak
unaffected.

A failed pre-phase aborts the soak before the measured run starts -- this
is the "smoke must pass before the long soak" gate, self-contained in a
single ``scripts.soak`` invocation.
"""

from __future__ import annotations

import dataclasses
import tempfile
import time
from pathlib import Path
from typing import Any

from benchmarks._harness import (
    spawn_waitbus_daemon,
    terminate_daemon_group,
    wait_for_socket,
)
from scripts.soak._emit import _build_event_insert, _pick_weighted_source
from scripts.soak._fault_injection import (
    fault_injection_close_reason_consistency_threshold,
    fault_injection_coverage_threshold,
    run_fault_injection_pass,
)
from scripts.soak._suspend import _isolated_waitbus_dirs
from scripts.soak._verdict import _count_close_reasons
from scripts.soak_monitor import ThresholdVerdict
from waitbus import _emit as emit_mod

# All four subscriber-lifecycle drain paths reachable from the wire. Order
# is fast-probes-first; heartbeat_lag last because it polls the longest.
_DRAIN_SMOKE_AXES: tuple[str, ...] = (
    "version_reject",
    "token_reject",
    "replay_lag_eviction",
    "heartbeat_lag",
)

# Aggressive heartbeat so the heartbeat_lag probe's starved buffer fills
# and trips LAG_LIMIT within seconds even on kernels with a larger
# ``net.core.rmem_min`` (more heartbeats needed to fill). Safe here: the
# daemon is throwaway, so frequent heartbeats cannot contaminate any
# measurement.
_DRAIN_SMOKE_HEARTBEAT_SEC = 0.01

# Seed more than REPLAY_LIMIT (500) rows so the replay_lag_eviction probe's
# full-history replay saturates its starved receive buffer and trips the
# lag counter before the replay drains.
_DRAIN_SMOKE_SEED_EVENTS = 600


@dataclasses.dataclass(frozen=True)
class DrainSmokeResult:
    """Outcome of the drain-path smoke pre-phase.

    ``passed`` is the AND of every verdict. ``outcomes`` are the raw
    per-probe :class:`FaultInjectionOutcome` dicts; ``close_reasons`` is the
    throwaway daemon's ``subscriber_closed`` reason tally. ``verdicts`` carry
    the coverage + close-reason-consistency :class:`ThresholdVerdict`s, which
    the soak folds into the final verdict JSON's signal list.
    """

    passed: bool
    outcomes: list[dict[str, Any]]
    close_reasons: dict[str, int]
    verdicts: list[ThresholdVerdict]


def _seed_backlog(db_path: Path, n_events: int) -> None:
    """Emit ``n_events`` synthetic events in one batch so the replay probe has
    a backlog to saturate. A single ``emit_batch`` (one transaction, one
    doorbell ring) keeps seeding fast -- per-event emits cost ~10 s for 600
    rows, which would dominate the pre-phase wall-clock.
    """
    events = [
        _build_event_insert(
            _pick_weighted_source(i),
            delivery_id=f"drain-smoke:{i}",
            ingest_method="drain_smoke",
        )
        for i in range(n_events)
    ]
    emit_mod.emit_batch(events, db_path=db_path)


def _run_probes_against(socket_path: Path) -> list[dict[str, Any]]:
    """Run every drain-path probe once against ``socket_path``."""
    outcomes: list[dict[str, Any]] = []
    for axis in _DRAIN_SMOKE_AXES:
        run_fault_injection_pass(axis=axis, socket_path=socket_path, offset_sec=0.0, outcomes=outcomes)
    return outcomes


def run_drain_path_smoke(
    *,
    heartbeat_sec: float = _DRAIN_SMOKE_HEARTBEAT_SEC,
    seed_events: int = _DRAIN_SMOKE_SEED_EVENTS,
) -> DrainSmokeResult:
    """Run the drain-path smoke against a throwaway daemon and return the result.

    Spins up an isolated low-heartbeat daemon, seeds a backlog, drives all
    four probes, tallies the daemon's close reasons, evaluates the coverage
    and close-reason-consistency thresholds, and tears the daemon down. Never
    touches the measured soak's state.
    """
    outcomes: list[dict[str, Any]] = []
    close_reasons: dict[str, int] = {}
    with tempfile.TemporaryDirectory(prefix="waitbus-drain-smoke-") as tmp_str:
        tmp_dir = Path(tmp_str)
        state_dir = tmp_dir / "state"
        runtime_dir = tmp_dir / "runtime"
        state_dir.mkdir()
        runtime_dir.mkdir()
        stderr_path = tmp_dir / "daemon-stderr.log"
        with _isolated_waitbus_dirs(state_dir, runtime_dir):
            proc = spawn_waitbus_daemon(state_dir, runtime_dir, stderr_path=stderr_path, heartbeat_sec=heartbeat_sec)
            try:
                socket_path = runtime_dir / "broadcast.sock"
                wait_for_socket(socket_path)
                db_path = state_dir / "github.db"
                # Brief breather so the daemon's synchronous schema init has
                # settled before the first emit (mirrors the measured soak).
                time.sleep(0.5)
                _seed_backlog(db_path, seed_events)
                time.sleep(0.5)
                outcomes = _run_probes_against(socket_path)
            finally:
                terminate_daemon_group(proc)
        close_reasons = _count_close_reasons(stderr_path)

    expected_axes = frozenset(_DRAIN_SMOKE_AXES)
    # Prefix the signal names so they do not collide with the measured run's
    # own ``fault_injection_coverage`` signal when both are folded into the
    # final verdict's signal list.
    verdicts = [
        dataclasses.replace(
            fault_injection_coverage_threshold(outcomes, expected_axes),
            signal="drain_smoke_coverage",
        ),
        dataclasses.replace(
            fault_injection_close_reason_consistency_threshold(outcomes, close_reasons),
            signal="drain_smoke_close_reason_consistency",
        ),
    ]
    return DrainSmokeResult(
        passed=all(v.passed for v in verdicts),
        outcomes=outcomes,
        close_reasons=close_reasons,
        verdicts=verdicts,
    )
