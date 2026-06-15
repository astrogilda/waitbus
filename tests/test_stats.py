"""Contract tests for ``waitbus stats`` (per-source counterfactual model).

Covers the five contracts the per-source refactor pins:

1. Per-source measurement reconciles exactly with a seeded events DB
   (events_observed[s] matches COUNT(*) WHERE source = s for each s).
2. Per-source modelled_savings_tokens equals events_observed * per_poll_tokens
   for that source, and aggregate_modelled_savings_tokens equals the
   deterministic sum across sources.
3. The output is tripartite: MEASURED / ESTIMATED / COMPUTED banners
   in that order, in both text and JSON.
4. There is no "you saved $X" headline anywhere in the output (the
   aggregate prints last, never as a hero, and "savings"-shaped marketing
   strings never appear).
5. The report is strictly read-only: the DB file's mtime and row count
   are unchanged after a run.

The model lives in ``waitbus.stats`` so all tests here drive that
module directly.  CLI-layer tests (typer wiring, env-var validation) live
in ``tests/test_cli_stats.py``.
"""

from __future__ import annotations

import contextlib
import json
import sqlite3
from pathlib import Path

import pytest

from waitbus import _db, stats

_BASE_NS = 1_500_000_000_000_000_000  # well above NS_RECEIVED_AT_MIN


def _default_costs() -> dict[str, int]:
    """The four per-source defaults as a fresh dict (so tests can mutate)."""
    return {
        "github": stats.DEFAULT_POLL_COST_GITHUB,
        "pytest": stats.DEFAULT_POLL_COST_PYTEST,
        "docker": stats.DEFAULT_POLL_COST_DOCKER,
        "fs": stats.DEFAULT_POLL_COST_FS,
    }


def _seed(db_path: Path, rows: list[tuple[str, str, str, int]]) -> None:
    """Insert (delivery_id, source, event_type, received_at_ns) rows.

    Bypasses ``insert_event`` (no doorbell ring / magnitude guard needed
    here — the model only reads the table back).
    """
    _db.ensure_schema(db_path)
    with contextlib.closing(sqlite3.connect(str(db_path))) as conn:
        conn.executemany(
            """
            INSERT INTO events (
                delivery_id, source, event_type, owner, repo,
                received_at, payload_json, ingest_method, event_id
            ) VALUES (?, ?, ?, 'astro', 'csb', ?, '{}', 'webhook', ?)
            """,
            [(did, src, et, ts, f"01HV000000000000000000000{i}") for i, (did, src, et, ts) in enumerate(rows, start=1)],
        )
        conn.commit()


def _request(
    db: Path,
    *,
    as_json: bool = False,
    costs: dict[str, int] | None = None,
) -> stats.StatsRequest:
    return stats.StatsRequest(
        db_path=db,
        poll_interval_seconds=stats.DEFAULT_POLL_INTERVAL_SECONDS,
        per_source_token_costs=costs if costs is not None else _default_costs(),
        as_json=as_json,
    )


# --- measurement reconciliation -----------------------------------------------


def test_measured_reconciles_with_seeded_db(tmp_path: Path) -> None:
    """by_source counts match COUNT(*) per source; by_event_type matches per type."""
    db = tmp_path / "events.db"
    _seed(
        db,
        [
            ("a1", "github", "workflow_run", _BASE_NS + 1),
            ("a2", "github", "workflow_run", _BASE_NS + 2),
            ("a3", "github", "workflow_job", _BASE_NS + 3),
            ("a4", "pytest", "pytest_session", _BASE_NS + 4),
            ("a5", "docker", "docker_container", _BASE_NS + 5),
            ("a6", "docker", "docker_container", _BASE_NS + 6),
            ("a7", "fs", "fs_change", _BASE_NS + 7),
        ],
    )
    with _db.connect(db, readonly=True) as conn:
        facts = stats._measure(conn)
    assert facts.total_events == 7
    assert facts.by_source == {"github": 3, "docker": 2, "pytest": 1, "fs": 1}
    assert facts.by_event_type == {
        "workflow_run": 2,
        "docker_container": 2,
        "workflow_job": 1,
        "pytest_session": 1,
        "fs_change": 1,
    }
    assert facts.span_seconds is not None
    assert facts.span_seconds > 0


def test_single_event_has_no_span(tmp_path: Path) -> None:
    db = tmp_path / "events.db"
    _seed(db, [("only", "github", "workflow_run", _BASE_NS + 1)])
    with _db.connect(db, readonly=True) as conn:
        facts = stats._measure(conn)
    assert facts.total_events == 1
    assert facts.span_seconds is None  # one event = no span


# --- per-source estimate ------------------------------------------------------


def test_estimate_per_source_reconciles_with_measured(tmp_path: Path) -> None:
    """events_observed[s] == count[s]; modelled_savings == count * cost per source."""
    db = tmp_path / "events.db"
    _seed(
        db,
        [
            ("g1", "github", "workflow_run", _BASE_NS + 1),
            ("g2", "github", "workflow_run", _BASE_NS + 2),
            ("p1", "pytest", "pytest_session", _BASE_NS + 3),
            ("d1", "docker", "docker_container", _BASE_NS + 4),
            ("d2", "docker", "docker_container", _BASE_NS + 5),
            ("d3", "docker", "docker_container", _BASE_NS + 6),
        ],
    )
    with _db.connect(db, readonly=True) as conn:
        facts = stats._measure(conn)

    req = _request(db)
    est = stats._estimate(facts, req)

    by_source = {row.source: row for row in est.per_source}
    assert by_source["github"].events_observed == 2
    assert by_source["github"].per_poll_tokens == stats.DEFAULT_POLL_COST_GITHUB
    assert by_source["github"].polls_avoided == 2
    assert by_source["github"].modelled_savings_tokens == 2 * stats.DEFAULT_POLL_COST_GITHUB

    assert by_source["pytest"].events_observed == 1
    assert by_source["pytest"].modelled_savings_tokens == 1 * stats.DEFAULT_POLL_COST_PYTEST

    assert by_source["docker"].events_observed == 3
    assert by_source["docker"].modelled_savings_tokens == 3 * stats.DEFAULT_POLL_COST_DOCKER

    assert by_source["fs"].events_observed == 0
    assert by_source["fs"].modelled_savings_tokens == 0


def test_aggregate_equals_sum_of_per_source(tmp_path: Path) -> None:
    """The invariant: aggregate is the deterministic sum across sources."""
    db = tmp_path / "events.db"
    _seed(
        db,
        [
            ("g1", "github", "workflow_run", _BASE_NS + 1),
            ("p1", "pytest", "pytest_session", _BASE_NS + 2),
            ("d1", "docker", "docker_container", _BASE_NS + 3),
            ("f1", "fs", "fs_change", _BASE_NS + 4),
        ],
    )
    with _db.connect(db, readonly=True) as conn:
        facts = stats._measure(conn)
    est = stats._estimate(facts, _request(db))

    expected_total_polls = sum(r.polls_avoided for r in est.per_source)
    expected_total_tokens = sum(r.modelled_savings_tokens for r in est.per_source)
    assert est.aggregate_polls_avoided == expected_total_polls == 4
    assert est.aggregate_modelled_savings_tokens == expected_total_tokens


def test_per_source_token_cost_override(tmp_path: Path) -> None:
    """A caller can override any source's per-poll cost; the rest stay default."""
    db = tmp_path / "events.db"
    _seed(
        db,
        [
            ("p1", "pytest", "pytest_session", _BASE_NS + 1),
            ("p2", "pytest", "pytest_session", _BASE_NS + 2),
        ],
    )
    with _db.connect(db, readonly=True) as conn:
        facts = stats._measure(conn)

    costs = _default_costs()
    costs["pytest"] = 500  # operator simulating traceback-heavy run
    est = stats._estimate(facts, _request(db, costs=costs))

    pytest_row = next(r for r in est.per_source if r.source == "pytest")
    assert pytest_row.per_poll_tokens == 500
    assert pytest_row.modelled_savings_tokens == 1000  # 2 events x 500


# --- output shape -------------------------------------------------------------


def test_text_output_has_three_banners_in_order(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """MEASURED before ESTIMATED before COMPUTED, all three present."""
    db = tmp_path / "events.db"
    _seed(db, [("g1", "github", "workflow_run", _BASE_NS + 1)])
    stats.run_stats(_request(db))
    out = capsys.readouterr().out.lower()

    measured_at = out.index("=== measured")
    estimated_at = out.index("=== estimated")
    computed_at = out.index("=== computed")
    assert measured_at < estimated_at < computed_at


def test_text_output_prints_per_source_rows(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """All four known sources appear in ESTIMATED and COMPUTED, including zero-event sources."""
    db = tmp_path / "events.db"
    _seed(db, [("g1", "github", "workflow_run", _BASE_NS + 1)])
    stats.run_stats(_request(db))
    out = capsys.readouterr().out

    # ESTIMATED block lists per-source costs for all four sources.
    for source in ("github", "pytest", "docker", "fs"):
        assert f"{source}: per_poll_tokens=" in out

    # COMPUTED block has a per-source modelled_savings line for each.
    for source in ("github", "pytest", "docker", "fs"):
        assert f"{source}: events_observed=" in out


def test_no_bare_savings_headline(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """The aggregate prints last; no marketing-shaped 'saves $X' phrasing."""
    db = tmp_path / "events.db"
    _seed(db, [("g1", "github", "workflow_run", _BASE_NS + 1)])
    stats.run_stats(_request(db))
    out = capsys.readouterr().out.lower()

    # No marketing strings.
    for forbidden in ("saved $", "saves $", "waitbus saved", "waitbus saves", "you saved"):
        assert forbidden not in out

    # The aggregate appears only AFTER every per-source COMPUTED row.
    last_per_source_idx = out.rindex("modelled_savings_tokens=")
    aggregate_idx = out.index("aggregate_modelled_savings_tokens:")
    assert last_per_source_idx < aggregate_idx


def test_json_output_separates_three_blocks(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db = tmp_path / "events.db"
    _seed(
        db,
        [
            ("g1", "github", "workflow_run", _BASE_NS + 1),
            ("p1", "pytest", "pytest_session", _BASE_NS + 2),
        ],
    )
    stats.run_stats(_request(db, as_json=True))
    out = capsys.readouterr().out
    payload = json.loads(out)

    assert set(payload.keys()) == {"measured", "estimated", "computed"}

    # MEASURED block carries by_source + caveats verbatim.
    assert payload["measured"]["by_source"] == {"github": 1, "pytest": 1}
    assert any("not a single counter" in c.lower() for c in payload["measured"]["caveats"])

    # ESTIMATED block has per-source costs.
    est_sources = {row["source"]: row for row in payload["estimated"]["per_source"]}
    assert est_sources["github"]["per_poll_tokens"] == stats.DEFAULT_POLL_COST_GITHUB

    # COMPUTED block: per-source product + aggregate sum.
    computed = payload["computed"]
    computed_sources = {row["source"]: row for row in computed["per_source"]}
    assert computed_sources["github"]["modelled_savings_tokens"] == stats.DEFAULT_POLL_COST_GITHUB
    assert computed_sources["pytest"]["modelled_savings_tokens"] == stats.DEFAULT_POLL_COST_PYTEST
    assert computed["aggregate_modelled_savings_tokens"] == (
        stats.DEFAULT_POLL_COST_GITHUB + stats.DEFAULT_POLL_COST_PYTEST
    )


def test_json_aggregate_equals_sum_invariant(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """JSON output preserves the aggregate-equals-sum invariant."""
    db = tmp_path / "events.db"
    _seed(
        db,
        [
            ("g1", "github", "workflow_run", _BASE_NS + 1),
            ("g2", "github", "workflow_run", _BASE_NS + 2),
            ("d1", "docker", "docker_container", _BASE_NS + 3),
        ],
    )
    stats.run_stats(_request(db, as_json=True))
    payload = json.loads(capsys.readouterr().out)
    computed = payload["computed"]
    expected = sum(row["modelled_savings_tokens"] for row in computed["per_source"])
    assert computed["aggregate_modelled_savings_tokens"] == expected


def test_caveats_state_delivered_and_uptime_are_not_counters(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "events.db"
    _seed(db, [("g1", "github", "workflow_run", _BASE_NS + 1)])
    stats.run_stats(_request(db))
    out = capsys.readouterr().out
    assert "events_delivered is NOT a single counter" in out
    assert "subscription_uptime is NOT a counter" in out
    assert "waitbus_db_dedup_ignored_total is NOT recoverable" in out


# --- read-only invariants -----------------------------------------------------


def test_run_is_read_only(tmp_path: Path) -> None:
    """Row count and mtime are unchanged after run_stats."""
    db = tmp_path / "events.db"
    _seed(db, [("g1", "github", "workflow_run", _BASE_NS + 1)])
    mtime_before = db.stat().st_mtime_ns
    with contextlib.closing(sqlite3.connect(str(db))) as conn:
        count_before = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]

    stats.run_stats(_request(db))

    mtime_after = db.stat().st_mtime_ns
    with contextlib.closing(sqlite3.connect(str(db))) as conn:
        count_after = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    assert mtime_after == mtime_before
    assert count_after == count_before


def test_write_attempt_through_readonly_conn_fails(tmp_path: Path) -> None:
    """The read-only connection refuses INSERTs at the SQLite layer."""
    db = tmp_path / "events.db"
    _seed(db, [("g1", "github", "workflow_run", _BASE_NS + 1)])
    with _db.connect(db, readonly=True) as conn, pytest.raises(sqlite3.OperationalError):
        conn.execute(
            "INSERT INTO events ("
            "delivery_id, source, event_type, owner, repo, received_at, "
            "payload_json, ingest_method, event_id) VALUES "
            "('x','github','workflow_run','o','r',?,'{}','webhook','01HVZ0000000000000000000ZZ')",
            (_BASE_NS + 99,),
        )


# --- error / edge cases -------------------------------------------------------


def test_missing_db_returns_2_with_hint(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc = stats.run_stats(_request(tmp_path / "does-not-exist.db"))
    assert rc == 2
    assert "waitbus init" in capsys.readouterr().err


def test_empty_store_succeeds(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Empty events table: total_events=0, every per-source row is zero."""
    db = tmp_path / "events.db"
    _db.ensure_schema(db)
    rc = stats.run_stats(_request(db))
    assert rc == 0
    out = capsys.readouterr().out
    assert "total_events: 0" in out
    assert "aggregate_modelled_savings_tokens: 0" in out


def test_cli_entry_rejects_missing_source_cost() -> None:
    """cli_entry raises ValueError if a known source's cost is missing.

    Defensive: the CLI layer is responsible for resolving every source's
    cost (via env var or default); the typed entry refuses a half-built
    dict rather than silently defaulting.
    """
    incomplete = _default_costs()
    del incomplete["fs"]
    with pytest.raises(ValueError, match="missing required keys"):
        stats.cli_entry(
            poll_interval_seconds=stats.DEFAULT_POLL_INTERVAL_SECONDS,
            per_source_token_costs=incomplete,
            as_json=False,
            db_path=None,
        )


# CLI-layer tests (typer wiring, env-var validation) live in
# tests/test_cli_stats.py so model tests and CLI tests are in separate files.


def test_single_event_text_output_shows_no_span(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Single-event DB: text output prints 'single_event (no span)' for span_seconds."""
    db = tmp_path / "events.db"
    _seed(db, [("only", "github", "workflow_run", _BASE_NS + 1)])
    rc = stats.run_stats(_request(db))
    assert rc == 0
    out = capsys.readouterr().out
    assert "single_event (no span)" in out


def test_sqlite_error_returns_2_with_message(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """A corrupt / non-SQLite file triggers the OperationalError handler (rc=2)."""
    db = tmp_path / "corrupt.db"
    db.write_bytes(b"not a sqlite database\n")
    rc = stats.run_stats(_request(db))
    assert rc == 2
    assert "sqlite error" in capsys.readouterr().err
