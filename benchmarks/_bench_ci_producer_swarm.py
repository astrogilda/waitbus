"""Synthetic CI-event producer swarm for bench_multistream_proof's loaded arm.

Spawns N producer threads that emit owner-scoped events into the daemon
at a fixed aggregate rate via ``OpenLoopScheduler``. Each producer draws
its per-emit ``(source, event_type)`` from the same weighted source
taxonomy the soak harness uses
(``benchmarks._bench_source_mix.pick_source_for_iter``), so the fan-out
predicate-match cost is exercised across the full registered source set.

The loaded arm pairs this producer swarm with the LLM-agent subscribers
spawned by ``_bench_llm_agent_pool``: producers publish synthetic CI-like
emits, subscribers park in ``wait_for`` and react, and the bench measures
daemon CPU during the window. Producers carry no LLM cost -- they are pure
publishers writing ``EventInsert`` rows. The aggregate rate is the
operator-chosen loaded-arm throughput knob (``--producer-event-rate-hz``).
"""

from __future__ import annotations

import contextlib
import threading
import time
import uuid
from pathlib import Path
from types import TracebackType
from typing import Self

from benchmarks._bench_source_mix import pick_source_for_iter
from benchmarks._harness import OpenLoopScheduler


class CiProducerSwarm:
    """Manage N CI-event producer threads as a unit.

    Usage::

        with CiProducerSwarm(
            producer_count=50,
            aggregate_rate_hz=200.0,
            run_duration_sec=1.0,
            seed_scope_id="bench-multistream-abc123",
            db_path=db_path,
            doorbell_path=doorbell_path,
        ) as swarm:
            swarm.fire()             # blocking; emits for run_duration_sec
            print(swarm.emit_count) # total events emitted
            print(swarm.error_count)

    ``producer_count == 0`` is a no-op (idle arm enters/exits cleanly
    without spawning any threads). The threaded model fits the bench's
    sync structure -- each producer is one Python thread firing
    ``emit(EventInsert(...))`` against the daemon's AF_UNIX socket;
    GIL release happens during the socket write so multiple producers
    progress in parallel.
    """

    def __init__(
        self,
        *,
        producer_count: int,
        aggregate_rate_hz: float,
        run_duration_sec: float,
        seed_scope_id: str,
        db_path: Path,
        doorbell_path: Path,
        iter_id_base: int = 0,
    ) -> None:
        if producer_count < 0:
            raise ValueError(f"producer_count must be >= 0, got {producer_count}")
        if aggregate_rate_hz < 0:
            raise ValueError(f"aggregate_rate_hz must be >= 0, got {aggregate_rate_hz}")
        if run_duration_sec <= 0:
            raise ValueError(f"run_duration_sec must be > 0, got {run_duration_sec}")
        self.producer_count = producer_count
        self.aggregate_rate_hz = aggregate_rate_hz
        self.run_duration_sec = run_duration_sec
        self.seed_scope_id = seed_scope_id
        self.db_path = db_path
        self.doorbell_path = doorbell_path
        self.iter_id_base = iter_id_base
        self._threads: list[threading.Thread] = []
        self._stop_flag = threading.Event()
        self._emit_counter_lock = threading.Lock()
        self._emit_count = 0
        self._error_count = 0
        self._late_count = 0
        # Per-thread mortality counter; see ``attrition_detected``.
        self._attrition_count = 0

    @property
    def emit_count(self) -> int:
        with self._emit_counter_lock:
            return self._emit_count

    @property
    def error_count(self) -> int:
        with self._emit_counter_lock:
            return self._error_count

    @property
    def late_count(self) -> int:
        """Emits where t_actual_dispatch > t_intended + late_threshold."""
        with self._emit_counter_lock:
            return self._late_count

    @property
    def attrition_detected(self) -> bool:
        """True when at least one producer thread exited early.

        Attrition tracks thread mortality: a producer that fails to start
        (scheduler init error) or dies on an uncaught exception mid-run
        increments ``_attrition_count``. Per-emit broadcast errors are
        recoverable backpressure -- the daemon returning EAGAIN/EPIPE
        under load is part of what the loaded arm measures against -- and
        are counted separately in ``error_count``; they never trip
        attrition. A loaded window with attrition under-generated its
        intended load and is rejected by the bench.
        """
        with self._emit_counter_lock:
            return self._attrition_count > 0

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _tb: TracebackType | None,
    ) -> None:
        self._stop_flag.set()
        for t in self._threads:
            with contextlib.suppress(RuntimeError):
                t.join(timeout=5.0)
        self._threads.clear()

    def fire(self) -> None:
        """Spawn ``producer_count`` threads and block until ``run_duration_sec`` elapses.

        Each thread runs an ``OpenLoopScheduler`` capped at the
        per-producer slice of the aggregate rate. Returns once every
        thread has joined; ``emit_count`` reflects the realized count.
        On ``producer_count == 0`` this is a no-op.
        """
        if self.producer_count == 0 or self.aggregate_rate_hz == 0:
            return
        per_producer_hz = self.aggregate_rate_hz / self.producer_count
        # Cap per-producer scheduler n at the worst-case wall window so
        # a slow producer cannot accidentally emit past run_duration.
        per_producer_n = max(1, int(self.run_duration_sec * per_producer_hz * 1.5))
        self._stop_flag.clear()
        for i in range(self.producer_count):
            t = threading.Thread(
                target=self._producer_loop,
                args=(i, per_producer_hz, per_producer_n),
                name=f"bench-multistream-producer-{i:03d}",
                daemon=True,
            )
            self._threads.append(t)
            t.start()
        deadline = time.monotonic() + self.run_duration_sec
        while time.monotonic() < deadline and any(t.is_alive() for t in self._threads):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(0.05, remaining))
        self._stop_flag.set()
        for t in self._threads:
            t.join(timeout=2.0)

    def _producer_loop(self, producer_idx: int, per_hz: float, per_n: int) -> None:
        # Lazy-import the emit primitive so the module stays importable
        # without the daemon-side closure.
        from waitbus._emit import emit
        from waitbus._types import EventInsert

        try:
            sched = OpenLoopScheduler(rate_hz=per_hz, n=per_n)
        except ValueError:
            # Scheduler init failed: this thread produces nothing -> attrition.
            with self._emit_counter_lock:
                self._attrition_count += 1
            return

        try:
            for tick, t_intended_ns in enumerate(sched):
                if self._stop_flag.is_set():
                    return
                now_ns = time.monotonic_ns()
                if now_ns < t_intended_ns:
                    slack = (t_intended_ns - now_ns) / 1e9
                    if slack > 0:
                        time.sleep(min(slack, 0.5))
                elif now_ns - t_intended_ns > sched.late_threshold_ns:
                    with self._emit_counter_lock:
                        self._late_count += 1
                iter_id = self.iter_id_base + producer_idx * per_n + tick
                picked_source, picked_event_type = pick_source_for_iter(iter_id)
                delivery_id = f"bench-multistream-producer:{producer_idx}:{tick}:{uuid.uuid4()}"
                try:
                    emit(
                        EventInsert(
                            delivery_id=delivery_id,
                            source=picked_source,
                            event_type=picked_event_type,
                            owner=self.seed_scope_id,
                            repo="bench",
                            received_at=time.time_ns(),
                            payload_json='{"kind": "bench_multistream_producer_emit"}',
                            ingest_method="bench_multistream_ci_producer_swarm",
                        ),
                        db_path=self.db_path,
                        doorbell_path=self.doorbell_path,
                    )
                    with self._emit_counter_lock:
                        self._emit_count += 1
                except Exception:
                    # Per-emit backpressure (EAGAIN/EPIPE): recoverable,
                    # keep firing. Surfaced via error_count, not attrition.
                    with self._emit_counter_lock:
                        self._error_count += 1
        except Exception:
            # Producer died mid-run -> thread mortality (attrition).
            with self._emit_counter_lock:
                self._attrition_count += 1


__all__ = ["CiProducerSwarm"]
