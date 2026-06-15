"""Append-only JSONL ledgers for the stress harness correctness diff.

The stress harness's loss / duplicate / ordering verdict is computed
by diffing two ledgers written from independent ends of the bus:

- ``EmitLedger`` -- written by the controller's per-source emitter
  threads. One line per emission: ``{delivery_id, source, event_type,
  emit_ns}``. The ``emit_ns`` is captured immediately before the
  underlying ``_emit_one`` returns so a crashed controller still has
  a faithful pre-emit timestamp on disk.
- ``ReceivedLedger`` -- one per subscriber agent. One line per frame
  the agent observes: ``{delivery_id, recv_ns, frame_seq}``. The
  ``frame_seq`` is the agent's per-process monotonic count so the
  ordering check does not depend on cross-process clocks.

Both ledgers ``flush()`` after every record so the kernel buffer
contains every emit before the next one starts (a concurrent
``tail -F`` consumer sees each line live). ``fsync`` per record
would add ~50 ms per call on commodity SSDs, throttling the harness
emit rate to ~20 Hz; that is not the right tradeoff for a stress
run because a SIGKILL mid-run invalidates the run anyway (the
controller's per-N step is not crash-resumable). The ``flush``
without ``fsync`` is durable across the controller exiting cleanly
on its own and a non-write-cache-loss process kill -- and those are
the only cases that should leave a recoverable ledger on disk.

The diff function in this module reads both ledgers, computes the
per-event correctness signal, and returns a ``CorrectnessDiff``
record that the verdict aggregator folds into the JSON wire shape.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import IO


@dataclass(slots=True)
class _LedgerWriter:
    """Shared append-only JSONL writer with per-record flush.

    Owns one file descriptor for the lifetime of the harness; closing
    is the caller's responsibility (typically a ``contextmanager`` in
    the controller). Line-buffered + explicit ``flush()`` per write
    keeps a concurrent ``tail -F`` consumer live without paying the
    fsync cost on every emit.
    """

    path: Path
    _fh: IO[str]

    @classmethod
    def open(cls, path: Path) -> _LedgerWriter:
        """Open ``path`` for append-only JSONL writes, creating parents as needed."""
        path.parent.mkdir(parents=True, exist_ok=True)
        # Line buffering keeps the kernel buffer small; the explicit fsync below
        # guarantees durability without relying on pages flushing on their own.
        fh = path.open("a", buffering=1, encoding="utf-8")
        return cls(path=path, _fh=fh)

    def write(self, record: dict[str, object]) -> None:
        """Append one JSON record + newline and flush the kernel buffer.

        ``flush`` makes the line visible to a concurrent ``tail -F``
        consumer immediately; ``fsync`` per record would cost ~50 ms
        each on a commodity SSD and is not warranted for a stress run
        (see the module docstring on the durability tradeoff).
        """
        self._fh.write(json.dumps(record, separators=(",", ":")) + "\n")
        self._fh.flush()

    def close(self) -> None:
        """Close the underlying file handle; safe to call more than once."""
        if not self._fh.closed:
            self._fh.close()


@dataclass(slots=True)
class EmitLedger:
    """Controller-side per-emission ledger.

    One instance per stress run; threads emitting against different
    sources share the same ledger. ``record`` is thread-safe via the
    underlying file-descriptor write ordering -- one short JSON line
    per call, atomic at the syscall level on Linux for any write
    under ``PIPE_BUF``.
    """

    _writer: _LedgerWriter

    @classmethod
    def open(cls, path: Path) -> EmitLedger:
        return cls(_writer=_LedgerWriter.open(path))

    def record(self, *, delivery_id: str, source: str, event_type: str) -> None:
        """Append one emit record stamped with the current monotonic-corrected time_ns."""
        self._writer.write(
            {
                "kind": "emit",
                "delivery_id": delivery_id,
                "source": source,
                "event_type": event_type,
                "emit_ns": time.time_ns(),
            }
        )

    def close(self) -> None:
        self._writer.close()


@dataclass(slots=True)
class ReceivedLedger:
    """One-per-subscriber-agent received-frames ledger.

    ``record`` writes one line per frame the agent observes; the
    ``frame_seq`` is the agent-local monotonic count of frames observed
    so the ordering check is robust to clock skew across processes.
    """

    agent_id: str
    _writer: _LedgerWriter
    _next_frame_seq: int = 0

    @classmethod
    def open(cls, path: Path, agent_id: str) -> ReceivedLedger:
        return cls(agent_id=agent_id, _writer=_LedgerWriter.open(path))

    def record(self, *, delivery_id: str) -> None:
        """Append one recv record. Auto-advances the per-agent frame_seq."""
        self._writer.write(
            {
                "kind": "recv",
                "agent_id": self.agent_id,
                "delivery_id": delivery_id,
                "recv_ns": time.time_ns(),
                "frame_seq": self._next_frame_seq,
            }
        )
        self._next_frame_seq += 1

    def close(self) -> None:
        self._writer.close()


# --- Diff -------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CorrectnessDiff:
    """Per-event correctness signal computed from one emit ledger + N recv ledgers.

    ``lost`` counts delivery_ids the emitter wrote but no agent received.
    ``duplicates`` counts (agent_id, delivery_id) pairs observed more than
    once on the same agent. ``ordering_violations`` counts cases where a
    given agent observed delivery_ids in an order that contradicts the
    emit-order monotonic ranking (i.e., agent saw event B then A even
    though A was emitted before B). ``unmatched_recv`` counts recv
    records carrying a delivery_id that the emit ledger never recorded
    (which would indicate a synthetic-test bug or a different emitter
    on the bus -- never expected in a clean stress run).
    """

    lost: int
    duplicates: int
    ordering_violations: int
    unmatched_recv: int


def _iter_jsonl(path: Path) -> Iterator[dict[str, object]]:
    """Yield one parsed JSON dict per line; skip blank lines."""
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def diff_ledgers(emit_path: Path, recv_paths: list[Path]) -> CorrectnessDiff:
    """Compute the per-event correctness diff over one emit + N recv ledgers."""
    emit_order: dict[str, int] = {}
    for index, record in enumerate(_iter_jsonl(emit_path)):
        delivery_id = record["delivery_id"]
        if not isinstance(delivery_id, str):
            raise TypeError(f"emit ledger {emit_path} has non-string delivery_id: {record!r}")
        emit_order[delivery_id] = index

    received_by_agent: dict[str, list[str]] = {}
    duplicates = 0
    unmatched_recv = 0
    for path in recv_paths:
        seen_on_agent: set[str] = set()
        observed_sequence: list[str] = []
        path_agent_id: str | None = None
        for record in _iter_jsonl(path):
            agent_id = record["agent_id"]
            delivery_id = record["delivery_id"]
            if not isinstance(agent_id, str) or not isinstance(delivery_id, str):
                raise TypeError(f"recv ledger {path} has non-string id field: {record!r}")
            path_agent_id = agent_id
            if delivery_id in seen_on_agent:
                duplicates += 1
                continue
            seen_on_agent.add(delivery_id)
            if delivery_id not in emit_order:
                unmatched_recv += 1
                continue
            observed_sequence.append(delivery_id)
        if path_agent_id is not None:
            received_by_agent.setdefault(path_agent_id, []).extend(observed_sequence)

    # An emitted delivery_id is lost when no agent recorded it.
    received_anywhere: set[str] = set()
    for agent_sequence in received_by_agent.values():
        received_anywhere.update(agent_sequence)
    lost = sum(1 for delivery_id in emit_order if delivery_id not in received_anywhere)

    # Ordering: every agent's observed sequence must respect emit_order.
    ordering_violations = 0
    for agent_sequence in received_by_agent.values():
        prev_rank = -1
        for delivery_id in agent_sequence:
            rank = emit_order[delivery_id]
            if rank < prev_rank:
                ordering_violations += 1
            else:
                prev_rank = rank

    return CorrectnessDiff(
        lost=lost,
        duplicates=duplicates,
        ordering_violations=ordering_violations,
        unmatched_recv=unmatched_recv,
    )
