"""Concurrent per-source emitters for the stress harness.

The five-source taxonomy (``SOAK_SOURCE_REGISTRY`` in
``benchmarks._source_taxonomy``) carries each source's
``default_mix_share`` -- the fraction of synthetic traffic to route to
that source. The stress harness fans these shares out as one emitter
thread per source so the daemon's fan-out path sees realistic
cross-source interleaving (one github_workflow_run jostling with one
agent_message jostling with one fs_change) rather than a single
weighted-pick loop that hides cross-source ordering effects.

Each emitter thread paces itself with an open-loop scheduler from
``benchmarks._harness`` so coordinated omission cannot creep in
through closed-loop "send -> wait -> send" sequencing (the canonical
VL-11 trap). Every emission writes to both the daemon's events
table via ``scripts.soak._emit._build_event_insert`` + the shared
``waitbus._db.emit_batch`` AND the controller's ``EmitLedger``
so the correctness diff in ``_ledger.diff_ledgers`` has a complete
record on disk even if a controller crash interrupts the daemon
read-side.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from benchmarks._source_taxonomy import SOAK_SOURCE_REGISTRY
from scripts.soak._emit import _DEFAULT_SOURCE_MIX, _EVENT_TYPES, _build_event_insert
from scripts.stress._ledger import EmitLedger
from waitbus import _emit as emit_mod

# Maximum seconds ``_EmitterHandle.stop`` / ``stop_concurrent_emitters`` will
# wait for an emitter thread to join before giving up.  5 s is well above the
# worst-case inter-emit sleep the open-loop scheduler produces; a thread that
# does not exit within this budget is almost certainly stuck on I/O, and the
# caller's harness will surface the hung thread through its own watchdog.
_DEFAULT_JOIN_TIMEOUT_SEC: Final[float] = 5.0

# An emitter callable receives the per-iteration index. The production
# emitter is ``_emit_one_source(db_path, ledger, source, ...)``; tests
# can pass a no-op or a counter to exercise the loop's pacing without
# touching sqlite.
EmitterFn = Callable[[int], None]


def _emit_one_source(
    db_path: Path,
    ledger: EmitLedger,
    source: str,
    index: int,
) -> None:
    """Emit one synthetic event of the named source into ``db_path``.

    Mirrors the field shape of ``scripts.soak._emit._emit_one`` so a
    downstream consumer (the daemon, the MCP read path, an operator
    grep) cannot tell the stress emissions apart from soak emissions
    by structural inspection -- only the ``ingest_method="stress"``
    tag and the ``delivery_id`` namespace distinguish them. Records
    the emission into the shared ``EmitLedger`` for the correctness
    diff.
    """
    delivery_id = f"stress:{source}:{index}-{time.time_ns()}"
    event_type = _EVENT_TYPES.get(source, "unknown")
    emit_mod.emit_batch(
        [_build_event_insert(source, delivery_id=delivery_id, ingest_method="stress")],
        db_path=db_path,
    )
    ledger.record(delivery_id=delivery_id, source=source, event_type=event_type)


@dataclass(slots=True)
class _EmitterHandle:
    """Owner-side handle bundling the thread + the stop event for one source."""

    source: str
    thread: threading.Thread
    stop_event: threading.Event

    def stop(self, *, join_timeout: float = _DEFAULT_JOIN_TIMEOUT_SEC) -> None:
        """Signal the emitter to stop and join its thread.

        ``join_timeout`` is the maximum wait before the test/harness
        gives up; in production paths the inter-emit sleep is well
        below 1 s so a ``_DEFAULT_JOIN_TIMEOUT_SEC`` budget catches
        stuck emitters loudly.
        """
        self.stop_event.set()
        self.thread.join(timeout=join_timeout)


def _emit_loop(
    *,
    rate_hz: float,
    emitter: EmitterFn,
    stop_event: threading.Event,
) -> None:
    """Open-loop emitter loop for one source.

    Schedules each emission at a planned tick computed from ``rate_hz``;
    never sleeps relative to the prior emit's actual completion (which
    would re-introduce coordinated omission per Brooker / wrk2 / Tene).
    The planned-tick deadline is held in monotonic time so wall-clock
    jitter (NTP step, suspend / resume) does not silently corrupt the
    inter-arrival distribution.

    ``emitter`` is the per-tick callable that produces one event; the
    loop only owns the timing discipline. Decoupling the timing from
    the I/O lets tests pin the loop's pacing without touching the
    daemon path.
    """
    if rate_hz <= 0:
        return
    period = 1.0 / rate_hz
    next_tick = time.monotonic()
    index = 0
    while not stop_event.is_set():
        now = time.monotonic()
        if now < next_tick:
            stop_event.wait(timeout=next_tick - now)
            continue
        emitter(index)
        index += 1
        next_tick += period


def start_concurrent_emitters(
    *,
    db_path: Path,
    ledger: EmitLedger,
    total_rate_hz: float,
    source_mix: Mapping[str, float] | None = None,
) -> list[_EmitterHandle]:
    """Spin one emitter thread per source whose share is non-zero.

    The total ``total_rate_hz`` is split across the five sources by
    ``default_mix_share`` (or by the operator's ``source_mix``
    override). A source with share 0 produces no thread, so the
    operator can run a github-only stress probe by passing
    ``{"github": 1.0}`` without paying the cost of four idle threads.
    """
    mix = dict(source_mix) if source_mix is not None else dict(_DEFAULT_SOURCE_MIX)
    handles: list[_EmitterHandle] = []
    for spec in SOAK_SOURCE_REGISTRY:
        share = mix.get(spec.name, 0.0)
        if share <= 0.0:
            continue
        rate = total_rate_hz * share
        stop_event = threading.Event()
        source = spec.name

        def emit_one(index: int, src: str = source) -> None:
            _emit_one_source(db_path, ledger, src, index)

        thread = threading.Thread(
            target=_emit_loop,
            kwargs={
                "rate_hz": rate,
                "emitter": emit_one,
                "stop_event": stop_event,
            },
            name=f"stress-emit-{source}",
            daemon=True,
        )
        thread.start()
        handles.append(_EmitterHandle(source=source, thread=thread, stop_event=stop_event))
    return handles


def stop_concurrent_emitters(handles: list[_EmitterHandle], *, join_timeout: float = _DEFAULT_JOIN_TIMEOUT_SEC) -> None:
    """Signal every emitter to stop and join its thread."""
    for handle in handles:
        handle.stop(join_timeout=join_timeout)
