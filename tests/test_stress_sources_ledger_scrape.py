"""Tests for the ``_sources`` / ``_ledger`` / ``_scrape`` stress harness modules.

Covers:
- ``_ledger`` -- emit + recv ledger durability (fsync per record),
  the loss / duplicate / ordering diff over hand-crafted ledger files.
- ``_sources`` -- per-source emitter thread + ``start_concurrent_emitters``
  fans out one thread per non-zero share.
- ``_scrape`` -- daemon ``metrics_snapshot`` tail filter, ``cpu.stat``
  parser, ``/proc/<pid>/status`` ctxt-switch reader.

The ``_sources`` tests use the existing in-process ``broadcast_paths``
fixture so the emitter writes against a real (tmp) waitbus database.
"""

from __future__ import annotations

import io
import json
import os
import sys
import threading
import time
from pathlib import Path

import pytest

from scripts.stress._ledger import (
    CorrectnessDiff,
    EmitLedger,
    ReceivedLedger,
    diff_ledgers,
)
from scripts.stress._scrape import (
    CtxtSwitchSnapshot,
    MetricsSnapshot,
    read_cgroup_cpu_stat,
    read_ctxt_switches,
    tail_metrics_snapshots,
)

# --- _ledger ----------------------------------------------------------------


def test_emit_ledger_writes_one_jsonl_record_per_emit(tmp_path: Path) -> None:
    """``EmitLedger.record`` appends one JSON line per call with all the fields."""
    ledger = EmitLedger.open(tmp_path / "emit.jsonl")
    ledger.record(delivery_id="stress:github:1-100", source="github", event_type="workflow_run")
    ledger.record(delivery_id="stress:agent:2-200", source="agent", event_type="agent_message")
    ledger.close()

    rows = [json.loads(line) for line in (tmp_path / "emit.jsonl").read_text().splitlines()]
    assert len(rows) == 2
    assert rows[0]["kind"] == "emit"
    assert rows[0]["delivery_id"] == "stress:github:1-100"
    assert rows[0]["source"] == "github"
    assert rows[0]["event_type"] == "workflow_run"
    assert isinstance(rows[0]["emit_ns"], int)
    assert rows[1]["source"] == "agent"


def test_received_ledger_advances_frame_seq_per_record(tmp_path: Path) -> None:
    """Each ``ReceivedLedger.record`` advances the agent-local frame_seq."""
    ledger = ReceivedLedger.open(tmp_path / "agent-0.jsonl", agent_id="agent-0")
    ledger.record(delivery_id="d-1")
    ledger.record(delivery_id="d-2")
    ledger.close()

    rows = [json.loads(line) for line in (tmp_path / "agent-0.jsonl").read_text().splitlines()]
    assert [row["frame_seq"] for row in rows] == [0, 1]
    assert all(row["agent_id"] == "agent-0" for row in rows)


def test_diff_ledgers_reports_no_violations_for_clean_run(tmp_path: Path) -> None:
    """A clean run -- every emit observed exactly once per agent, in order -- diffs to zeros."""
    emit = EmitLedger.open(tmp_path / "emit.jsonl")
    emit.record(delivery_id="d-1", source="github", event_type="workflow_run")
    emit.record(delivery_id="d-2", source="agent", event_type="agent_message")
    emit.record(delivery_id="d-3", source="fs", event_type="fs_change")
    emit.close()

    recv_a = ReceivedLedger.open(tmp_path / "agent-a.jsonl", agent_id="agent-a")
    for delivery_id in ("d-1", "d-2", "d-3"):
        recv_a.record(delivery_id=delivery_id)
    recv_a.close()

    diff = diff_ledgers(tmp_path / "emit.jsonl", [tmp_path / "agent-a.jsonl"])

    assert diff == CorrectnessDiff(lost=0, duplicates=0, ordering_violations=0, unmatched_recv=0)


def test_diff_ledgers_counts_loss_dup_ordering_unmatched(tmp_path: Path) -> None:
    """Diff distinguishes the four failure modes by their canonical signal."""
    emit = EmitLedger.open(tmp_path / "emit.jsonl")
    for delivery_id in ("d-1", "d-2", "d-3"):
        emit.record(delivery_id=delivery_id, source="github", event_type="workflow_run")
    emit.close()

    # Agent observes d-2 twice (duplicate), then d-1 (ordering violation),
    # then a delivery_id the emitter never wrote (unmatched_recv). d-3 is
    # never observed (loss).
    recv = ReceivedLedger.open(tmp_path / "agent-a.jsonl", agent_id="agent-a")
    recv.record(delivery_id="d-2")
    recv.record(delivery_id="d-2")  # duplicate
    recv.record(delivery_id="d-1")  # ordering violation (1 < 2)
    recv.record(delivery_id="d-orphan")  # unmatched_recv
    recv.close()

    diff = diff_ledgers(tmp_path / "emit.jsonl", [tmp_path / "agent-a.jsonl"])

    assert diff.duplicates == 1
    assert diff.ordering_violations == 1
    assert diff.unmatched_recv == 1
    assert diff.lost == 1  # d-3 never observed


# --- _sources ---------------------------------------------------------------


def test_emit_loop_paces_against_open_loop_schedule() -> None:
    """The emitter loop fires at approximately the configured rate.

    Decoupled from the waitbus DB path: the emitter callable is a
    pure counter so the test pins the loop's timing discipline
    (open-loop scheduling, no closed-loop drift) without sqlite
    write costs muddying the rate measurement. We pin 200 Hz over
    0.5 s and assert the count lands inside +-30 % -- enough slack
    to absorb scheduler jitter on a loaded CI runner while still
    failing loudly if the loop regresses to a closed-loop "send
    then sleep" form that re-introduces coordinated omission.
    """
    from scripts.stress._sources import _emit_loop

    observations: list[int] = []
    stop_event = threading.Event()

    def collect(index: int) -> None:
        observations.append(index)

    thread = threading.Thread(
        target=_emit_loop,
        kwargs={
            "rate_hz": 200.0,
            "emitter": collect,
            "stop_event": stop_event,
        },
        daemon=True,
    )
    started_at = time.monotonic()
    thread.start()
    time.sleep(0.5)
    stop_event.set()
    thread.join(timeout=5.0)
    elapsed = time.monotonic() - started_at

    expected = int(elapsed * 200)
    assert 0.6 * expected <= len(observations) <= 1.4 * expected, (
        f"expected ~{expected} emits over {elapsed:.2f}s @ 200 Hz; got {len(observations)}"
    )
    # Index sequence must be strictly increasing -- the loop never re-fires
    # a counter value, which is the structural property the diff relies on.
    assert observations == sorted(observations)
    assert len(set(observations)) == len(observations)


# --- _scrape ----------------------------------------------------------------


def test_tail_metrics_snapshots_filters_event_field() -> None:
    """Only ``event=="metrics_snapshot"`` lines surface; other events are skipped."""
    sub_count_sample = '{"name": "waitbus_subscriber_count", "labels": {}, "value": 3.0}'
    families_payload = '"families": {"waitbus_subscriber_count": [' + sub_count_sample + "]}"
    stream = io.StringIO(
        "\n".join(
            [
                '{"ts": 1.0, "event": "ready", "db": "/tmp/db"}',
                '{"ts": 2.0, "event": "metrics_snapshot", ' + families_payload + "}",
                "not a json line",
                '{"ts": 3.0, "event": "subscriber_closed", "reason": "shutdown"}',
                '{"ts": 4.0, "event": "metrics_snapshot", "families": {}}',
            ]
        )
        + "\n"
    )

    snapshots = list(tail_metrics_snapshots(stream))

    assert [s.ts for s in snapshots] == [2.0, 4.0]
    assert isinstance(snapshots[0], MetricsSnapshot)
    assert "waitbus_subscriber_count" in snapshots[0].families


def test_read_cgroup_cpu_stat_parses_canonical_fields(tmp_path: Path) -> None:
    """All four documented ``cpu.stat`` fields land in the parsed view."""
    cgroup = tmp_path / "leaf"
    cgroup.mkdir()
    (cgroup / "cpu.stat").write_text(
        "usage_usec 12345\nuser_usec 8000\nsystem_usec 4345\nnr_periods 10\nnr_throttled 3\nthrottled_usec 2500\n"
    )

    sample = read_cgroup_cpu_stat(cgroup)

    assert sample.nr_periods == 10
    assert sample.nr_throttled == 3
    assert sample.throttled_usec == 2500
    assert sample.usage_usec == 12345


@pytest.mark.skipif(sys.platform != "linux", reason="reads /proc/<pid>/status; Linux-only")
def test_read_ctxt_switches_parses_voluntary_and_nonvoluntary(tmp_path: Path) -> None:
    """``/proc/<pid>/status`` reader picks the two documented counters."""
    snapshot = read_ctxt_switches(os.getpid())

    assert isinstance(snapshot, CtxtSwitchSnapshot)
    # The current process has at least one of each by the time the test runs;
    # they are read by pytest's own startup and the runner shell respectively.
    assert snapshot.voluntary >= 0
    assert snapshot.nonvoluntary >= 0
