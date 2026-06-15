"""Tests for the soak verdict's daemon-stderr close-reason tally.

``_count_close_reasons`` parses the broadcast daemon's captured stderr
(pure JSON lines, ``format="%(message)s"``) and tallies
``subscriber_closed`` events by reason. This surfaces the daemon-internal
close-reason vocabulary in the soak verdict, 
"""

from __future__ import annotations

import json
from pathlib import Path

from scripts.soak._verdict import _count_close_reasons


def _write_lines(path: Path, records: list[dict[str, object]]) -> None:
    path.write_text(
        "\n".join(json.dumps(r, separators=(",", ":")) for r in records) + "\n",
        encoding="utf-8",
    )


def test_count_close_reasons_tallies_by_reason(tmp_path: Path) -> None:
    """Each ``subscriber_closed`` event increments its reason's count."""
    log = tmp_path / "daemon-stderr.log"
    _write_lines(
        log,
        [
            {"ts": 1.0, "event": "subscriber_closed", "fd": 7, "reason": "lag_limit_exceeded"},
            {"ts": 2.0, "event": "subscriber_closed", "fd": 8, "reason": "lag_limit_exceeded"},
            {"ts": 3.0, "event": "subscriber_closed", "fd": 9, "reason": "replay_db_error"},
            {"ts": 4.0, "event": "subscriber_closed", "fd": 10, "reason": "shutdown"},
        ],
    )
    counts = _count_close_reasons(log)
    assert counts == {"lag_limit_exceeded": 2, "replay_db_error": 1, "shutdown": 1}


def test_count_close_reasons_ignores_other_events_and_noise(tmp_path: Path) -> None:
    """Non-subscriber_closed events, blank lines, and non-JSON lines are skipped."""
    log = tmp_path / "daemon-stderr.log"
    log.write_text(
        "\n".join(
            [
                json.dumps({"ts": 1.0, "event": "subscribe_bad_proto", "proto": 9999}),
                "",
                "not json at all",
                json.dumps({"ts": 2.0, "event": "subscriber_closed", "fd": 7, "reason": "heartbeat_lag"}),
                json.dumps({"ts": 3.0, "event": "daemon_started"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    counts = _count_close_reasons(log)
    assert counts == {"heartbeat_lag": 1}


def test_count_close_reasons_missing_file_returns_empty(tmp_path: Path) -> None:
    """A missing stderr log (capture disabled, daemon never closed a subscriber) yields {}."""
    assert _count_close_reasons(tmp_path / "does-not-exist.log") == {}


def test_count_close_reasons_skips_event_without_string_reason(tmp_path: Path) -> None:
    """A subscriber_closed record missing a string reason is skipped, not crashed on."""
    log = tmp_path / "daemon-stderr.log"
    _write_lines(
        log,
        [
            {"ts": 1.0, "event": "subscriber_closed", "fd": 7},  # no reason key
            {"ts": 2.0, "event": "subscriber_closed", "fd": 8, "reason": None},
            {"ts": 3.0, "event": "subscriber_closed", "fd": 9, "reason": "shutdown"},
        ],
    )
    assert _count_close_reasons(log) == {"shutdown": 1}
