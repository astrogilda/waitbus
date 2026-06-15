"""Property-based tests for the corpus-replay contract and Hawkes-median pin.

Inoculation tests for two classes of latent boundary defect:

1. ``replay_corpus`` ↔ ``_emit_corpus_event`` contract: the producer
   yields ``dict[str, Any] | None``; the consumer pattern-matches
   ``event is None`` to increment ``accums.corpus_decode_fallthroughs``.
   Properties asserted: the consumer never raises on arbitrary
   dict-or-None inputs, at most one ``emit_batch`` row lands per call,
   and the fallthrough counter increments iff the input is ``None``.

2. ``_select_burst_lifetime`` estimator stability: returns a valid
   central tendency in ``[min(q1), max(q1)]``, is deterministic, and
   is type-stable. The estimator-choice (``statistics.median`` vs the
   prior upper-middle-index form) does diverge on inputs like
   ``[0, 2, 2, 2, 2, 2, 2, 2]`` (50%+ beta drift); the empirical
   byte-stability of ``benchmarks/data/gh_distributions.toml`` (verified
   by ``--check`` at every CI gate) is the proof that real GHALogs
   data does not hit the pathological case.
"""

from __future__ import annotations

import contextlib
import gzip
import sqlite3
import statistics
from pathlib import Path
from typing import Any, cast

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from benchmarks._harness import replay_corpus
from scripts.derive_gh_distributions import _select_burst_lifetime

# ---------------------------------------------------------------------------
# Corpus contract: replay_corpus ↔ _emit_corpus_event
# ---------------------------------------------------------------------------


def test_replay_corpus_yields_none_on_malformed_line(tmp_path: Path) -> None:
    """Malformed JSON lines yield ``None`` (not silently skipped)."""
    corpus = tmp_path / "corpus.jsonl.gz"
    payload = b'{"delivery_id": "ok-1"}\nnot-json\n{"delivery_id": "ok-2"}\n'
    with gzip.open(corpus, "wb") as fh:
        fh.write(payload)

    yielded = list(replay_corpus(corpus))

    assert len(yielded) == 3
    assert yielded[0] == {"delivery_id": "ok-1"}
    assert yielded[1] is None
    assert yielded[2] == {"delivery_id": "ok-2"}


def test_replay_corpus_skips_empty_lines_yields_no_sentinel(tmp_path: Path) -> None:
    """Blank lines are dropped before parsing; no ``None`` sentinel emitted."""
    corpus = tmp_path / "corpus.jsonl.gz"
    payload = b'{"x": 1}\n\n\n{"x": 2}\n'
    with gzip.open(corpus, "wb") as fh:
        fh.write(payload)

    yielded = list(replay_corpus(corpus))
    assert yielded == [{"x": 1}, {"x": 2}]


# Hypothesis strategy informed by the actual corpus shape the consumer
# expects (per ``scripts/soak.py:_emit_corpus_event``).  Generates the
# malformed/edge inputs the validator must reject: missing
# ``delivery_id``, non-string ``source``, non-int ``inter_arrival_ns``,
# empty-dict, plus the ``None`` sentinel.
_event_strategy = st.one_of(
    st.none(),
    st.fixed_dictionaries(
        {},
        optional={
            "source": st.one_of(
                st.text(min_size=0, max_size=32),
                st.sampled_from(["pytest_session", "docker_container", "fs_change", "github_workflow_run"]),
                st.none(),
            ),
            "delivery_id": st.one_of(st.text(min_size=0, max_size=64), st.none(), st.just("")),
            "owner": st.text(min_size=0, max_size=32),
            "repo": st.text(min_size=0, max_size=32),
            "ingest_method": st.text(min_size=0, max_size=16),
            "payload": st.dictionaries(
                st.text(min_size=1, max_size=8), st.integers() | st.text(max_size=16), max_size=4
            ),
            "inter_arrival_ns": st.one_of(
                st.integers(min_value=0, max_value=10_000_000_000),
                st.text(max_size=8),
                st.none(),
                st.floats(allow_nan=True, allow_infinity=True),
            ),
        },
    ),
)


@given(events=st.lists(_event_strategy, min_size=1, max_size=8))
@settings(
    max_examples=120,
    # No wall-clock deadline: the property does real sqlite I/O per example,
    # so a loaded serial-coverage run can take seconds on the first call and
    # milliseconds on the re-run — Hypothesis then reports FlakyFailure on
    # timing alone. Correctness here is the never-raises/counter property,
    # not latency.
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_emit_corpus_event_never_raises_counter_iff_none(
    tmp_path_factory: pytest.TempPathFactory,
    events: list[dict[str, Any] | None],
) -> None:
    """``_emit_corpus_event`` is total over ``dict[str, Any] | None``.

    Properties:
    - Never raises.
    - Increments ``accums.corpus_decode_fallthroughs`` exactly once iff
      the event is ``None``.
    - At most one row lands per call (DB layer's INSERT OR IGNORE may
      drop a duplicate-delivery_id event silently; that dedup-drop class
      is distinct from the JSON-parse-failure class tested here).
    - Returns ``(int, str)`` with non-negative inter-arrival and a
      known soak-source.
    """
    # Lazy import avoids module-level import-time work on collection.
    from scripts.soak._emit import _emit_corpus_event
    from waitbus import _db

    tmp = tmp_path_factory.mktemp("corpus_property")
    db_path = tmp / "events.db"
    _db.ensure_schema(db_path)

    accums = _build_accums()
    state = _build_state()
    none_count_before = accums.corpus_decode_fallthroughs

    none_seen = 0
    from scripts.soak._emit import _SOURCES

    for i, event in enumerate(events):
        before_rows = _count_rows(db_path)
        before_falls = accums.corpus_decode_fallthroughs
        inter_ns, source = _emit_corpus_event(db_path, event, i, state=state, accums=accums)
        after_rows = _count_rows(db_path)
        after_falls = accums.corpus_decode_fallthroughs

        # At most one row per call. INSERT OR IGNORE at the DB layer may
        # drop a row when two events share a delivery_id; that silent
        # dedup-drop class is distinct from the JSON-parse-failure class.
        # The corpus-contract invariant tested here is about JSON-parse failures only.
        assert after_rows in (before_rows, before_rows + 1)
        # Inter-arrival is non-negative.
        assert inter_ns >= 0
        # Source is a known soak-source.
        assert source in _SOURCES
        # Parse-failure counter increments iff event is None.
        if event is None:
            assert after_falls == before_falls + 1
            none_seen += 1
        else:
            assert after_falls == before_falls

    assert accums.corpus_decode_fallthroughs == none_count_before + none_seen


# ---------------------------------------------------------------------------
# Hawkes median estimator: stability + byte-stability of the committed TOML
# ---------------------------------------------------------------------------


@given(
    sample=st.lists(
        # Right-skewed integer samples mirroring CI inter-arrival shape:
        # heavy clustering at small values (seconds), occasional larger gaps.
        st.integers(min_value=0, max_value=300),
        min_size=4,
        max_size=400,
    ),
)
@settings(max_examples=300, deadline=500)
def test_select_burst_lifetime_returns_valid_central_tendency(sample: list[int]) -> None:
    """Pin the estimator property: returns a valid central tendency of the q1 slice.

    The "median formula change silently shifts the TOML" concern is real
    only if `_select_burst_lifetime` is an UNSTABLE estimator. This
    property asserts it IS stable in the senses that matter:

    1. Returns a value in `[min(q1), max(q1)]` (or None for zero-median).
    2. Returns the same value on repeated calls (deterministic).
    3. Returns a `float | None` (type-stable).

    The choice of `statistics.median` (Type-7) over `q1[len(q1)//2]`
    (upper-middle index) was made per the function's docstring; the
    formulas DO diverge on inputs like `[0, 2, 2, 2, 2, 2, 2, 2]` (50%+
    beta drift). The byte-stability of the committed `gh_distributions.toml`
    is the empirical proof that real GHALogs data does not hit the
    pathological case — verified by `--check` at every CI gate and by
    `--derive --include-ghalogs` at each release cut. If a future Zenodo
    data shape DOES hit the pathological case, `--check` trips the gate
    before the TOML can silently change.
    """
    sorted_inter = sorted(sample)
    q1_slice = sorted_inter[: max(len(sorted_inter) // 4, 1)]
    if not q1_slice:
        pytest.skip("empty quartile slice not exercised")

    result = _select_burst_lifetime(sample)

    # Deterministic.
    assert _select_burst_lifetime(sample) == result

    # Returns None iff the median is non-positive.
    median_burst = statistics.median(q1_slice)
    if median_burst <= 0:
        assert result is None
        return

    # Type-stable: float.
    assert isinstance(result, float)
    # Within the slice's range.
    assert min(q1_slice) <= result <= max(q1_slice)


def test_select_burst_lifetime_returns_none_on_zero_median() -> None:
    """``_select_burst_lifetime`` returns ``None`` when the burst median is non-positive."""
    assert _select_burst_lifetime([0, 0, 0, 0]) is None
    assert _select_burst_lifetime([0]) is None


def test_select_burst_lifetime_returns_positive_float_on_realistic_input() -> None:
    """A realistic CI inter-arrival sample yields a positive float median."""
    sample = [1, 1, 2, 2, 3, 5, 8, 13, 21, 34, 55, 89]
    result = _select_burst_lifetime(sample)
    assert result is not None
    assert result > 0
    assert isinstance(result, float)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_accums() -> Any:
    """Construct a fresh ``_SoakAccumulators`` instance for the property test."""
    from scripts.soak._context import _SoakAccumulators

    return _SoakAccumulators(
        rss_samples=[],
        p99_samples=[],
        gc_samples=[],
        log_size_samples=[],
        source_counts={},
        suspend_outcomes=[],
        suspend_verdicts=[],
    )


def _build_state() -> Any:
    """Construct a fresh ``_SoakState`` for the property test."""
    from scripts.soak._context import _SoakState

    return _SoakState(
        i=0,
        next_emit=0.0,
        next_sample=0.0,
        next_p99_sample=0.0,
        next_gc_sample=0.0,
        next_log_sample=0.0,
        corpus_exhausted=False,
        preserve_warned=False,
    )


def _count_rows(db_path: Path) -> int:
    """Return the row count in the events table.

    Uses ``contextlib.closing`` because ``with sqlite3.connect(...) as conn``
    is the COMMIT context (rolls back on exception, commits on success) —
    it does NOT close the connection. The pytest ``_force_gc_after_test``
    autouse fixture's ``gc.collect()`` would otherwise finalize the
    leaked connections and raise ``PytestUnraisableExceptionWarning``.
    """
    with contextlib.closing(sqlite3.connect(str(db_path))) as conn:
        return cast(int, conn.execute("SELECT COUNT(*) FROM events").fetchone()[0])
