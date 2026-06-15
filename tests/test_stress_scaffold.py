"""Tests for the stress scaffold (``scripts.stress._context`` + ``_verdict``).

Covers the round-trip discipline that downstream commits depend on:

- ``_VerdictDoc`` serialises through ``msgspec.to_builtins`` to a
  JSON-encodable structure and round-trips bit-stable on
  ``msgspec.json.encode`` / ``msgspec.json.decode``.
- ``_write_verdict`` is atomic (tmp + rename) and never leaves the
  ``.partial`` artefact behind.
- ``_append_progress`` flushes after every record so a concurrent
  ``tail -F`` consumer sees each line immediately.
- The accumulator's mutable lists can be promoted into the immutable
  ``_VerdictDoc.curve`` tuple without surprise sharing.
"""

from __future__ import annotations

import argparse
import io
import json
import time
from pathlib import Path

import msgspec

from scripts.stress._context import (
    CurvePoint,
    StressSignalFailure,
    _StressAccumulators,
    _StressContext,
    _StressState,
    _VerdictDoc,
)
from scripts.stress._verdict import (
    _append_progress,
    _compute_verdict_doc,
    _write_verdict,
)

# --- _VerdictDoc shape ------------------------------------------------------


def test_verdict_doc_round_trips_through_msgspec_to_builtins() -> None:
    """``msgspec.to_builtins(doc)`` -> ``json.dumps`` is the wire-contract path.

    Downstream CI gates parse the verdict JSON; any field that fails
    ``msgspec.to_builtins`` would silently break the gate at the
    serialise step. Exercising the full field surface (failures,
    curve, signal verdicts, close-reason tally) catches drift loudly.
    """
    doc = _VerdictDoc(
        started_at_ns=1_700_000_000_000_000_000,
        ended_at_ns=1_700_000_005_000_000_000,
        duration_sec=5.0,
        mode="offline",
        overall_passed=True,
        failures=(
            StressSignalFailure(
                signal="curve_p99_regression",
                threshold=0.05,
                observed=0.012,
                detail="within tolerance",
            ),
        ),
        curve=(
            CurvePoint(
                n=1,
                throughput_hz=12345.6,
                p50_seconds=0.0002,
                p99_seconds=0.001,
                p99_ci_low_seconds=0.0009,
                p99_ci_high_seconds=0.0011,
                n_samples=10_000,
            ),
        ),
        usl_alpha=0.05,
        usl_beta=0.001,
        usl_gamma=12345.6,
        knee_concurrency=22.4,
        knee_throughput_hz=98_765.0,
        zero_polling_verdict={"syscall_count": 0},
        subscriber_close_reasons={"lag_limit_exceeded": 2},
    )

    encoded = msgspec.json.encode(doc)
    decoded = msgspec.json.decode(encoded, type=_VerdictDoc)
    assert decoded == doc

    builtins_dict = msgspec.to_builtins(doc)
    json_body = json.dumps(builtins_dict)
    json_dict = json.loads(json_body)
    assert json_dict["mode"] == "offline"
    assert json_dict["curve"][0]["n"] == 1
    assert json_dict["failures"][0]["signal"] == "curve_p99_regression"


def test_verdict_doc_optional_signal_fields_default_to_none() -> None:
    """A minimal verdict (no curve probe) is well-formed.

    The user-facing ``waitbus stress --signals zero_poll`` invocation
    runs only the zero-polling assertion and leaves the curve fields
    ``None``. The verdict JSON must still parse cleanly so the
    downstream CI gate can read it.
    """
    doc = _VerdictDoc(
        started_at_ns=0,
        ended_at_ns=1_000_000_000,
        duration_sec=1.0,
        mode="offline",
        overall_passed=True,
    )

    assert doc.curve == ()
    assert doc.failures == ()
    assert doc.usl_alpha is None
    assert doc.zero_polling_verdict is None

    # The wire shape stays JSON-encodable even with every optional ``None``.
    json.dumps(msgspec.to_builtins(doc))


# --- _write_verdict atomicity ------------------------------------------------


def test_write_verdict_writes_atomically_via_partial_rename(tmp_path: Path) -> None:
    """``_write_verdict`` lands the target path via ``.partial`` rename.

    The partial file must not survive the call so a recovery tool
    cannot mistake a half-written rename for a stale partial.
    """
    target = tmp_path / "verdict.json"
    doc = _VerdictDoc(
        started_at_ns=0,
        ended_at_ns=1_000_000_000,
        duration_sec=1.0,
        mode="offline",
        overall_passed=True,
    )

    _write_verdict(target, doc)

    assert target.exists()
    assert not target.with_suffix(".json.partial").exists()
    payload = json.loads(target.read_text())
    assert payload["mode"] == "offline"
    assert payload["overall_passed"] is True


# --- _append_progress flush semantics ---------------------------------------


def test_append_progress_flushes_after_each_record() -> None:
    """``_append_progress`` flushes per record so a tail consumer sees lines live."""

    class _CountingBuffer(io.StringIO):
        flush_count = 0

        def flush(self) -> None:
            type(self).flush_count += 1
            super().flush()

    buf = _CountingBuffer()
    _append_progress(buf, {"kind": "tick", "n": 1})
    _append_progress(buf, {"kind": "tick", "n": 2})

    assert _CountingBuffer.flush_count == 2
    lines = buf.getvalue().splitlines()
    assert [json.loads(line)["n"] for line in lines] == [1, 2]


# --- _compute_verdict_doc folds ctx + accums --------------------------------


def test_compute_verdict_doc_promotes_curve_point_list_into_tuple(tmp_path: Path) -> None:
    """Accumulator-side ``list[CurvePoint]`` -> verdict-side ``tuple[CurvePoint, ...]``."""
    ctx = _StressContext(
        proc=None,
        db_path=tmp_path / "db.sqlite",
        progress_path=tmp_path / "progress.jsonl",
        socket_path=tmp_path / "broadcast.sock",
        daemon_stderr_path=tmp_path / "daemon.err",
        args=argparse.Namespace(duration="5s"),
        start_monotonic=time.monotonic(),
        started_at_ns=time.time_ns(),
        total_seconds=5.0,
        mode="offline",
        sweep_n=(1, 2, 4),
        corpus_iter=None,
        progress_fh=io.StringIO(),
    )
    accums = _StressAccumulators(
        curve_points=[
            CurvePoint(
                n=1,
                throughput_hz=10.0,
                p50_seconds=0.001,
                p99_seconds=0.002,
                p99_ci_low_seconds=0.0019,
                p99_ci_high_seconds=0.0021,
                n_samples=20_000,
            ),
        ],
    )

    doc = _compute_verdict_doc(ctx, accums, overall_passed=True)

    assert isinstance(doc.curve, tuple)
    assert len(doc.curve) == 1
    assert doc.curve[0].n == 1
    assert doc.mode == "offline"


def test_stress_state_dataclass_is_assignment_friendly() -> None:
    """``_StressState`` fields are mutable; the controller advances them per N."""
    state = _StressState()
    assert state.current_n_index == 0

    state.current_n_index = 3
    state.next_scrape_monotonic = 1.5
    assert state.current_n_index == 3
    assert state.next_scrape_monotonic == 1.5
