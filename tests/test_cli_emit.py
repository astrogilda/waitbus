"""Tests for the ``waitbus emit`` typer command wrapper.

Exercises the real root-app argv surface through CliRunner: the happy
insert path, the idempotent re-emit no-op, the CloudEvents output
format, and every exit-2 input-coercion branch (bad ``--received-at``,
unknown ``--source``, unreadable ``@file`` payload). The doorbell ring
inside ``insert_event`` is best-effort, so no daemon is needed; the
``broadcast_paths`` fixture redirects the doorbell socket away from any
real daemon on the host.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest
from click.testing import Result
from typer.testing import CliRunner

from waitbus import _db
from waitbus.cli.main import app

runner = CliRunner()


@pytest.fixture
def emit_db(broadcast_paths: dict[str, Path]) -> Path:
    """Schema-applied per-test DB with the doorbell redirected off-host."""
    db = broadcast_paths["db"]
    _db.ensure_schema(db)
    return db


def _emit_args(db: Path, **overrides: str) -> list[str]:
    """Build a full ``emit`` argv with sane defaults, applying overrides."""
    options = {
        "--delivery-id": "cli-test:1",
        "--source": "pytest",
        "--event-type": "pytest_session",
        "--owner": "local",
        "--repo": "demo",
        "--received-at": str(int(time.time())),
        "--payload-json": "{}",
        "--ingest-method": "manual",
        "--db": str(db),
    }
    for flag, value in overrides.items():
        options[f"--{flag.replace('_', '-')}"] = value
    args = ["emit"]
    for flag, value in options.items():
        args.extend([flag, value])
    return args


def _all_output(result: Result) -> str:
    """stdout + stderr regardless of the CliRunner mixing mode."""
    return result.stdout + str(result.stderr or "")


def test_emit_inserts_a_new_row(emit_db: Path) -> None:
    """A fresh delivery id inserts and reports inserted=true with the row."""
    result = runner.invoke(app, _emit_args(emit_db))
    assert result.exit_code == 0, _all_output(result)
    body = json.loads(result.stdout)
    assert body["inserted"] is True
    assert body["event"]["delivery_id"] == "cli-test:1"


def test_emit_same_delivery_id_is_idempotent_noop(emit_db: Path) -> None:
    """Re-emitting the same delivery id exits 0, inserted=false, stderr notes the no-op."""
    first = runner.invoke(app, _emit_args(emit_db))
    assert first.exit_code == 0, _all_output(first)
    second = runner.invoke(app, _emit_args(emit_db))
    assert second.exit_code == 0, _all_output(second)
    body = json.loads(second.stdout)
    assert body["inserted"] is False
    assert "idempotent no-op" in _all_output(second)


def test_emit_cloudevent_format_prints_envelope_without_inserted(emit_db: Path) -> None:
    """--format cloudevent prints the v1.0 envelope; the inserted bit is absent."""
    result = runner.invoke(app, _emit_args(emit_db, format="cloudevent"))
    assert result.exit_code == 0, _all_output(result)
    body = json.loads(result.stdout)
    assert body["specversion"] == "1.0"
    assert "inserted" not in body


def test_emit_bad_received_at_exits_2(emit_db: Path) -> None:
    """A --received-at that is neither a number nor a timestamp exits 2."""
    result = runner.invoke(app, _emit_args(emit_db, received_at="nope"))
    assert result.exit_code == 2
    assert "invalid input" in _all_output(result)


def test_emit_unknown_source_exits_2(emit_db: Path) -> None:
    """An unregistered --source is rejected with the accepted-values list."""
    result = runner.invoke(app, _emit_args(emit_db, source="carrier-pigeon"))
    assert result.exit_code == 2
    assert "unknown --source" in _all_output(result)


def test_emit_unreadable_payload_file_exits_2(emit_db: Path) -> None:
    """An @file payload pointing at a nonexistent path exits 2 (OSError branch)."""
    result = runner.invoke(app, _emit_args(emit_db, payload_json="@/nonexistent/payload.json"))
    assert result.exit_code == 2
    assert "invalid input" in _all_output(result)


def test_emit_payload_from_file(emit_db: Path, tmp_path: Path) -> None:
    """An @file payload is read from disk and stored verbatim."""
    payload_file = tmp_path / "payload.json"
    payload_file.write_text('{"k": "v"}', encoding="utf-8")
    result = runner.invoke(app, _emit_args(emit_db, payload_json=f"@{payload_file}"))
    assert result.exit_code == 0, _all_output(result)
    body = json.loads(result.stdout)
    assert body["inserted"] is True
    assert json.loads(body["event"]["payload_json"]) == {"k": "v"}
