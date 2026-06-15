"""CLI-layer tests for ``waitbus stats``.

Covers the typer wiring: env-var overrides, invalid env-var rejection
(non-integer and negative values), and a smoke round-trip through
``cli.app``.  Model-layer contracts live in ``tests/test_stats.py``.
"""

from __future__ import annotations

import contextlib
import json
import sqlite3
from pathlib import Path

import pytest
from typer.testing import CliRunner

from waitbus import _db, cli, stats

_BASE_NS = 1_500_000_000_000_000_000  # well above NS_RECEIVED_AT_MIN


def _seed(db_path: Path, rows: list[tuple[str, str, str, int]]) -> None:
    """Insert (delivery_id, source, event_type, received_at_ns) rows."""
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


# --- typer wiring smoke tests ------------------------------------------------


def test_stats_json_emits_three_banner_shape(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`waitbus stats --json` exits 0 and emits the measured/estimated/computed banners."""
    db = tmp_path / "events.db"
    _seed(db, [("g1", "github", "workflow_run", _BASE_NS + 1)])
    runner = CliRunner()
    result = runner.invoke(cli.app, ["stats", "--json", "--db", str(db)])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert set(payload.keys()) == {"measured", "estimated", "computed"}
    # The single github event survives the round-trip.
    by_source = {row["source"]: row for row in payload["computed"]["per_source"]}
    assert by_source["github"]["events_observed"] == 1


def test_cli_stats_env_var_overrides_per_source_cost(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """$WAITBUS_POLL_COST_PYTEST overrides the pytest default; others stay default."""
    db = tmp_path / "events.db"
    _seed(
        db,
        [("p1", "pytest", "pytest_session", _BASE_NS + 1)],
    )
    monkeypatch.setenv("WAITBUS_POLL_COST_PYTEST", "999")
    runner = CliRunner()
    result = runner.invoke(cli.app, ["stats", "--json", "--db", str(db)])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    by_source = {row["source"]: row for row in payload["computed"]["per_source"]}
    assert by_source["pytest"]["per_poll_tokens"] == 999
    assert by_source["pytest"]["modelled_savings_tokens"] == 999
    assert by_source["github"]["per_poll_tokens"] == stats.DEFAULT_POLL_COST_GITHUB


# --- $WAITBUS_POLL_COST_<SOURCE> validation ------------------------------------
#
# Each env var must reject non-integer values (exit 2, typer error message)
# and negative integers (exit 2, typer error message).  Covers all four
# sources: GITHUB, PYTEST, DOCKER, FS.


@pytest.mark.parametrize(
    "env_name",
    [
        "WAITBUS_POLL_COST_GITHUB",
        "WAITBUS_POLL_COST_PYTEST",
        "WAITBUS_POLL_COST_DOCKER",
        "WAITBUS_POLL_COST_FS",
    ],
)
def test_cli_stats_invalid_non_int_cost_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, env_name: str
) -> None:
    """$WAITBUS_POLL_COST_<SOURCE>=abc raises typer.BadParameter (exit 2)."""
    db = tmp_path / "events.db"
    _db.ensure_schema(db)
    monkeypatch.setenv(env_name, "abc")
    runner = CliRunner()
    result = runner.invoke(cli.app, ["stats", "--db", str(db)])
    assert result.exit_code == 2, result.output
    assert env_name in result.output
    assert "not an integer" in result.output


@pytest.mark.parametrize(
    "env_name",
    [
        "WAITBUS_POLL_COST_GITHUB",
        "WAITBUS_POLL_COST_PYTEST",
        "WAITBUS_POLL_COST_DOCKER",
        "WAITBUS_POLL_COST_FS",
    ],
)
def test_cli_stats_negative_int_cost_rejected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, env_name: str) -> None:
    """$WAITBUS_POLL_COST_<SOURCE>=-1 raises typer.BadParameter (exit 2)."""
    db = tmp_path / "events.db"
    _db.ensure_schema(db)
    monkeypatch.setenv(env_name, "-1")
    runner = CliRunner()
    result = runner.invoke(cli.app, ["stats", "--db", str(db)])
    assert result.exit_code == 2, result.output
    assert env_name in result.output
    assert "negative" in result.output
