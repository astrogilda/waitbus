"""Gating tests for ``benchmarks._bench_ci_producer_swarm.CiProducerSwarm``.

The swarm spawns producer THREADS that emit owner-scoped events via
``waitbus._emit.emit``. Full end-to-end emit verification
requires a live daemon and lives in the bench's integration tests;
this module covers the no-daemon construction + validation contract.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from benchmarks._bench_ci_producer_swarm import CiProducerSwarm


def _has_waitbus() -> bool:
    return shutil.which("waitbus") is not None


_REQUIRES_DAEMON = pytest.mark.skipif(
    not _has_waitbus() or not sys.platform.startswith("linux"),
    reason="CiProducerSwarm emit verification requires Linux + waitbus on PATH",
)


def test_producer_count_zero_is_no_op(tmp_path: Path) -> None:
    """producer_count=0 enters/exits cleanly and fires() returns without spawning threads."""
    db_path = tmp_path / "db"
    doorbell_path = tmp_path / "doorbell.sock"
    with CiProducerSwarm(
        producer_count=0,
        aggregate_rate_hz=200.0,
        run_duration_sec=1.0,
        seed_scope_id="bench-noop",
        db_path=db_path,
        doorbell_path=doorbell_path,
    ) as swarm:
        swarm.fire()  # No-op; no daemon required.
        assert swarm.emit_count == 0
        assert swarm.error_count == 0
        assert swarm.late_count == 0
        assert swarm.attrition_detected is False


def test_rate_zero_is_no_op(tmp_path: Path) -> None:
    """aggregate_rate_hz=0 is a no-op (idle producer)."""
    db_path = tmp_path / "db"
    doorbell_path = tmp_path / "doorbell.sock"
    with CiProducerSwarm(
        producer_count=5,
        aggregate_rate_hz=0.0,
        run_duration_sec=1.0,
        seed_scope_id="bench-rate-zero",
        db_path=db_path,
        doorbell_path=doorbell_path,
    ) as swarm:
        swarm.fire()
        assert swarm.emit_count == 0


def test_negative_producer_count_raises_at_init(tmp_path: Path) -> None:
    """Negative producer_count is rejected at construction."""
    db_path = tmp_path / "db"
    doorbell_path = tmp_path / "doorbell.sock"
    with pytest.raises(ValueError, match="producer_count must be >= 0"):
        CiProducerSwarm(
            producer_count=-1,
            aggregate_rate_hz=200.0,
            run_duration_sec=1.0,
            seed_scope_id="bench-bad",
            db_path=db_path,
            doorbell_path=doorbell_path,
        )


def test_negative_rate_raises_at_init(tmp_path: Path) -> None:
    """Negative aggregate_rate_hz is rejected at construction."""
    db_path = tmp_path / "db"
    doorbell_path = tmp_path / "doorbell.sock"
    with pytest.raises(ValueError, match="aggregate_rate_hz must be >= 0"):
        CiProducerSwarm(
            producer_count=5,
            aggregate_rate_hz=-1.0,
            run_duration_sec=1.0,
            seed_scope_id="bench-bad",
            db_path=db_path,
            doorbell_path=doorbell_path,
        )


def test_zero_duration_raises_at_init(tmp_path: Path) -> None:
    """run_duration_sec must be strictly positive."""
    db_path = tmp_path / "db"
    doorbell_path = tmp_path / "doorbell.sock"
    with pytest.raises(ValueError, match="run_duration_sec must be > 0"):
        CiProducerSwarm(
            producer_count=5,
            aggregate_rate_hz=200.0,
            run_duration_sec=0.0,
            seed_scope_id="bench-bad",
            db_path=db_path,
            doorbell_path=doorbell_path,
        )


@pytest.fixture()
def daemon(tmp_path: Path) -> Iterator[dict[str, Any]]:
    """Spawn a fresh waitbus broadcast daemon for the duration of the test."""
    waitbus_path = shutil.which("waitbus")
    if waitbus_path is None:
        pytest.skip("waitbus not on PATH")
    state_dir = tmp_path / "state"
    runtime_dir = tmp_path / "runtime"
    state_dir.mkdir()
    runtime_dir.mkdir()
    env = os.environ.copy()
    env["WAITBUS_STATE_DIR"] = str(state_dir)
    env["WAITBUS_RUNTIME_DIR"] = str(runtime_dir)
    env["WAITBUS_HEARTBEAT_SEC"] = "3600"
    proc = subprocess.Popen(
        [waitbus_path, "broadcast", "serve"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    socket_path = runtime_dir / "broadcast.sock"
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        if socket_path.exists():
            break
        time.sleep(0.05)
    if not socket_path.exists():
        proc.terminate()
        proc.wait(timeout=5.0)
        pytest.fail(f"daemon never created socket at {socket_path}")
    try:
        yield {
            "db_path": state_dir / "github.db",
            "doorbell_path": runtime_dir / "doorbell.sock",
            "runtime_dir": runtime_dir,
        }
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2.0)


@_REQUIRES_DAEMON
def test_emit_count_matches_realized_rate(daemon: dict[str, Any]) -> None:
    """5 producers at 20 Hz aggregate for 1 second emit roughly 20 events."""
    with CiProducerSwarm(
        producer_count=5,
        aggregate_rate_hz=20.0,
        run_duration_sec=1.0,
        seed_scope_id="bench-emit-count",
        db_path=daemon["db_path"],
        doorbell_path=daemon["doorbell_path"],
    ) as swarm:
        swarm.fire()
    # Allow a wide tolerance for scheduler jitter at low rate; the
    # OpenLoopScheduler is exact in the long run but a 1-second
    # sample is short. Expect at least 10 (50% of nominal).
    assert swarm.emit_count >= 10, f"realized rate too low; got {swarm.emit_count} emits"
    assert swarm.error_count == 0
    assert swarm.attrition_detected is False


def test_per_emit_errors_do_not_trip_attrition(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Per-emit broadcast failures increment error_count but never attrition.

    A producer whose ``emit`` raises (simulated daemon backpressure) keeps
    firing and finishes alive; attrition tracks thread mortality, not the
    per-emit error rate. This exercises the real producer loop -- no daemon
    needed because ``emit`` is monkeypatched to raise before any socket I/O.
    """

    def _raise_emit(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("simulated broadcast backpressure")

    monkeypatch.setattr("waitbus._emit.emit", _raise_emit)
    with CiProducerSwarm(
        producer_count=3,
        aggregate_rate_hz=30.0,
        run_duration_sec=0.1,
        seed_scope_id="bench-emit-errors",
        db_path=tmp_path / "db",
        doorbell_path=tmp_path / "doorbell.sock",
    ) as swarm:
        swarm.fire()
    assert swarm.error_count >= 1
    assert swarm.emit_count == 0
    assert swarm.attrition_detected is False


def test_thread_death_trips_attrition(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An uncaught exception in the producer loop counts as attrition.

    Sabotaging ``pick_source_for_iter`` (called outside the per-emit
    try/except) kills every producer thread on its first tick; each death
    increments ``_attrition_count``.
    """

    def _raise_pick(_iter_id: int) -> tuple[str, str]:
        raise RuntimeError("sabotaged source pick")

    monkeypatch.setattr("benchmarks._bench_ci_producer_swarm.pick_source_for_iter", _raise_pick)
    with CiProducerSwarm(
        producer_count=4,
        aggregate_rate_hz=80.0,
        run_duration_sec=0.1,
        seed_scope_id="bench-thread-death",
        db_path=tmp_path / "db",
        doorbell_path=tmp_path / "doorbell.sock",
    ) as swarm:
        swarm.fire()
    assert swarm.attrition_detected is True
    assert swarm._attrition_count == 4
    assert swarm.emit_count == 0


def test_scheduler_init_failure_trips_attrition(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A scheduler that fails to construct counts the thread as attrition.

    The init-failure path is distinct from per-emit errors: it increments
    only ``_attrition_count`` (the thread produced nothing), never
    ``error_count``.
    """

    def _raise_sched(*_args: Any, **_kwargs: Any) -> Any:
        raise ValueError("sabotaged scheduler init")

    monkeypatch.setattr("benchmarks._bench_ci_producer_swarm.OpenLoopScheduler", _raise_sched)
    with CiProducerSwarm(
        producer_count=2,
        aggregate_rate_hz=40.0,
        run_duration_sec=0.1,
        seed_scope_id="bench-init-fail",
        db_path=tmp_path / "db",
        doorbell_path=tmp_path / "doorbell.sock",
    ) as swarm:
        swarm.fire()
    assert swarm._attrition_count == 2
    assert swarm.emit_count == 0
    assert swarm.error_count == 0
