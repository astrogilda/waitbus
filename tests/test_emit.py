"""Tests for waitbus.emit: the public local-emit ingress seam.

Covers the explicit ``delivery_id`` idempotency contract, the
``received_at`` unit normalisation + rejection, the CloudEvents
projection of an emitted row, and the daemon-independence property (a
single-user workstation can emit with no broadcast daemon running — the
``etag-poll`` oneshot precedent).

The broadcast doorbell ring is patched to a no-op in every test: emit
must not *require* a listening daemon (a missed ring is a bounded
delivery delay, not an error), and patching it keeps the unit tests
from depending on a live AF_UNIX socket.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from waitbus import _db
from waitbus import _emit as emit_mod
from waitbus._cloudevents import to_cloudevent
from waitbus._types import Event, EventInsert


@pytest.fixture(autouse=True)
def _silence_doorbell(monkeypatch: pytest.MonkeyPatch) -> None:
    """No daemon in unit tests — emit must not depend on a live doorbell."""
    monkeypatch.setattr(_db._doorbell, "ring", lambda _path=None: None)


@pytest.fixture
def db(tmp_path: Path) -> Path:
    """Per-test SQLite DB with the canonical schema applied."""
    path = tmp_path / "events.db"
    _db.ensure_schema(path)
    return path


def _insert(
    *,
    delivery_id: str = "d-1",
    received_at: int = 1_700_000_000_000_000_000,
    source: str = "pytest",
) -> EventInsert:
    return EventInsert(
        delivery_id=delivery_id,
        source=source,
        event_type="workflow_run",
        owner="acme",
        repo="widgets",
        received_at=received_at,
        payload_json='{"k":"v"}',
        ingest_method="manual",
    )


# --- round-trip -------------------------------------------------------------


def test_emit_round_trips_into_events_row(db: Path) -> None:
    """emit() persists a row readable back with the right source/fields and
    a generated (caller-never-supplied) ULID event_id."""
    result = emit_mod.emit(_insert(source="docker"), db_path=db)

    assert result.inserted is True
    assert isinstance(result.event, Event)
    assert result.event.source == "docker"
    assert result.event.owner == "acme"
    assert result.event.repo == "widgets"
    assert result.event.delivery_id == "d-1"
    # event_id is generated internally (ULID, 26 chars), never supplied.
    assert result.event.event_id
    assert len(result.event.event_id) == 26

    with _db.connect(db, readonly=True) as conn:
        row = conn.execute("SELECT source, owner, repo, event_id FROM events WHERE delivery_id = 'd-1'").fetchone()
    assert row == ("docker", "acme", "widgets", result.event.event_id)


# --- idempotency: duplicate delivery_id is a no-op --------------------------


def test_duplicate_delivery_id_is_idempotent_noop(db: Path) -> None:
    """Re-emitting the same delivery_id does not write a second row and
    reports inserted=False with the pre-existing canonical row."""
    first = emit_mod.emit(_insert(delivery_id="dup"), db_path=db)
    second = emit_mod.emit(_insert(delivery_id="dup"), db_path=db)

    assert first.inserted is True
    assert second.inserted is False
    # The no-op returns the row that WON (the first insert's event_id),
    # not a phantom from the second call.
    assert second.event.event_id == first.event.event_id

    with _db.connect(db, readonly=True) as conn:
        count = conn.execute("SELECT COUNT(*) FROM events WHERE delivery_id = 'dup'").fetchone()[0]
    assert count == 1


# --- received_at unit conversion + rejection --------------------------------


def test_received_at_seconds_scaled_to_nanoseconds() -> None:
    ns = emit_mod._resolve_received_at_ns("1763337600")
    assert ns == 1763337600 * 1_000_000_000


def test_received_at_fractional_seconds_scaled() -> None:
    ns = emit_mod._resolve_received_at_ns("1763337600.5")
    assert ns == int(1763337600.5 * 1_000_000_000)


def test_received_at_nanoseconds_passed_through() -> None:
    ns = emit_mod._resolve_received_at_ns("1763337600000000000")
    assert ns == 1763337600000000000


def test_received_at_iso8601_z_parsed_as_utc() -> None:
    ns = emit_mod._resolve_received_at_ns("2026-05-17T12:00:00Z")
    expected = int(datetime(2026, 5, 17, 12, 0, 0, tzinfo=UTC).timestamp() * 1_000_000_000)
    assert ns == expected


def test_received_at_naive_iso_interpreted_as_utc() -> None:
    ns = emit_mod._resolve_received_at_ns("2026-05-17T12:00:00")
    expected = int(datetime(2026, 5, 17, 12, 0, 0, tzinfo=UTC).timestamp() * 1_000_000_000)
    assert ns == expected


def test_received_at_garbage_rejected() -> None:
    with pytest.raises(ValueError):
        emit_mod._resolve_received_at_ns("not-a-time")


@pytest.mark.parametrize("text", ["inf", "-inf", "Inf", "nan", "NaN"])
def test_received_at_non_finite_rejected_as_valueerror(text: str) -> None:
    """``float("inf")`` succeeds, so the parser must reject non-finite
    values explicitly: otherwise ``int(inf)`` raises ``OverflowError``,
    which is not ``ValueError`` and would escape the CLI handler as
    exit 1 + traceback instead of the documented exit 2."""
    with pytest.raises(ValueError, match="finite"):
        emit_mod._resolve_received_at_ns(text)


def test_received_at_iso_z_case_insensitive() -> None:
    """RFC3339 §5.6 makes the offset designator case-insensitive: ``Z``
    and ``z`` must round-trip to the same epoch-ns value."""
    upper = emit_mod._resolve_received_at_ns("2026-05-17T12:00:00Z")
    lower = emit_mod._resolve_received_at_ns("2026-05-17T12:00:00z")
    assert upper == lower


def test_emit_rejects_sub_ns_magnitude_received_at(db: Path) -> None:
    """A seconds-magnitude value reaching insert_event (the single ns-floor
    source of truth) is rejected, not silently persisted."""
    with pytest.raises(ValueError, match="epoch nanoseconds"):
        emit_mod.emit(_insert(received_at=1_763_337_600), db_path=db)


# --- CloudEvents projection of an emitted row -------------------------------


def test_cloudevent_projection_of_emitted_row(db: Path) -> None:
    ce = emit_mod.emit_cloudevent(_insert(delivery_id="ce-1", source="pytest"), db_path=db)

    assert ce.specversion == "1.0"
    assert ce.source == "urn:waitbus:source:pytest"
    assert ce.type == "workflow_run"
    assert ce.datacontenttype == "application/json"
    assert ce.time.endswith("Z")
    # id is the generated ULID; data is the lossless remainder.
    assert len(ce.id) == 26
    assert ce.data["delivery_id"] == "ce-1"

    # Projection is consistent with the stored Event.
    stored = emit_mod.emit(_insert(delivery_id="ce-1", source="pytest"), db_path=db).event
    assert to_cloudevent(stored).id == ce.id


# --- daemon-independence (read-only-safe / no daemon required to emit) -------


def test_emit_works_with_no_daemon_running(db: Path) -> None:
    """No listener/broadcast/etag-poll daemon is started anywhere in this
    test; emit still durably commits the row. The doorbell ring is a
    fire-and-forget no-op here (patched), proving emit does not block on
    or require a live daemon — the etag-poll oneshot precedent."""
    result = emit_mod.emit(_insert(delivery_id="solo"), db_path=db)
    assert result.inserted is True

    # The row is durably committed and visible to a fresh independent
    # connection with zero daemon involvement.
    with _db.connect(db, readonly=True) as conn:
        got = conn.execute("SELECT delivery_id FROM events WHERE delivery_id = 'solo'").fetchone()
    assert got == ("solo",)


# --- CLI adapter ------------------------------------------------------------


def test_cli_entry_round_trip_json(db: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc = emit_mod.cli_entry(
        delivery_id="cli-1",
        source="PYTEST",  # enum NAME accepted
        event_type="workflow_run",
        owner="acme",
        repo="widgets",
        received_at="2026-05-17T12:00:00Z",
        payload_json='{"hello":"world"}',
        ingest_method="manual",
        output_format="json",
        db_path=db,
    )
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["inserted"] is True
    assert out["event"]["source"] == "pytest"
    assert out["event"]["delivery_id"] == "cli-1"


def test_cli_entry_cloudevent_format(db: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc = emit_mod.cli_entry(
        delivery_id="cli-ce",
        source="github",
        event_type="workflow_job",
        owner="acme",
        repo="widgets",
        received_at="1763337600",  # seconds
        payload_json='{"job":1}',
        ingest_method="manual",
        output_format="cloudevent",
        db_path=db,
    )
    assert rc == 0
    envelope = json.loads(capsys.readouterr().out)
    assert envelope["specversion"] == "1.0"
    assert envelope["source"] == "urn:waitbus:source:github"
    assert "inserted" not in envelope


def test_cli_entry_payload_from_stdin(
    db: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """'-' reads stdin verbatim into payload_json."""
    import io

    monkeypatch.setattr("sys.stdin", io.StringIO('{"from":"stdin"}'))
    rc = emit_mod.cli_entry(
        delivery_id="cli-stdin",
        source="pytest",
        event_type="workflow_run",
        owner="acme",
        repo="widgets",
        received_at="1763337600",
        payload_json="-",
        ingest_method="manual",
        output_format="json",
        db_path=db,
    )
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert json.loads(out["event"]["payload_json"]) == {"from": "stdin"}


def test_cli_entry_payload_at_file(db: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    body = tmp_path / "body.json"
    body.write_text('{"from":"file"}', encoding="utf-8")
    rc = emit_mod.cli_entry(
        delivery_id="cli-file",
        source="fs",
        event_type="workflow_run",
        owner="acme",
        repo="widgets",
        received_at="1763337600000000000",
        payload_json=f"@{body}",
        ingest_method="manual",
        output_format="json",
        db_path=db,
    )
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert json.loads(out["event"]["payload_json"]) == {"from": "file"}


def test_cli_entry_duplicate_reports_noop(db: Path, capsys: pytest.CaptureFixture[str]) -> None:
    kw = dict(
        delivery_id="cli-dup",
        source="pytest",
        event_type="workflow_run",
        owner="acme",
        repo="widgets",
        received_at="1763337600",
        payload_json="{}",
        ingest_method="manual",
        output_format="json",
        db_path=db,
    )
    assert emit_mod.cli_entry(**kw) == 0  # type: ignore[arg-type]
    capsys.readouterr()
    assert emit_mod.cli_entry(**kw) == 0  # type: ignore[arg-type]
    captured = capsys.readouterr()
    assert json.loads(captured.out)["inserted"] is False
    assert "idempotent no-op" in captured.err


def test_cli_entry_bad_source_exits_2(db: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc = emit_mod.cli_entry(
        delivery_id="x",
        source="jenkins",  # not a known source
        event_type="workflow_run",
        owner="acme",
        repo="widgets",
        received_at="1763337600",
        payload_json="{}",
        ingest_method="manual",
        output_format="json",
        db_path=db,
    )
    assert rc == 2
    assert "unknown --source" in capsys.readouterr().err


def test_cli_entry_bad_received_at_exits_2(db: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc = emit_mod.cli_entry(
        delivery_id="x",
        source="pytest",
        event_type="workflow_run",
        owner="acme",
        repo="widgets",
        received_at="yesterday",
        payload_json="{}",
        ingest_method="manual",
        output_format="json",
        db_path=db,
    )
    assert rc == 2
    assert "invalid input" in capsys.readouterr().err


def test_cli_entry_inf_received_at_exits_2_not_traceback(db: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Public-API contract: a non-finite ``--received-at`` must exit 2
    (the documented "invalid input" path), NOT raise an uncaught
    ``OverflowError`` and exit 1 + traceback."""
    rc = emit_mod.cli_entry(
        delivery_id="x-inf",
        source="pytest",
        event_type="workflow_run",
        owner="acme",
        repo="widgets",
        received_at="inf",
        payload_json="{}",
        ingest_method="manual",
        output_format="json",
        db_path=db,
    )
    assert rc == 2
    assert "invalid input" in capsys.readouterr().err
