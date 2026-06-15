"""Cross-source composition suite for ``waitbus wait --all-of / --first-of``.

Mirrors the ``test_waitbus_wait_universal.py`` fixtures + helpers: a real
broadcast daemon (``running_daemon``), the wait CLI on a thread bound to the
test socket, deterministic registration / join barriers. Also covers the
startup-error matrix in-process (exit 2, no daemon needed) and the outcome
dispatcher's signal / connection-loss arms with fabricated outcomes.
"""

from __future__ import annotations

import contextlib
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
import typer

from tests._daemon_helpers import (
    await_subscribers as _await_subscribers,
)
from tests._daemon_helpers import (
    await_thread as _await_thread,
)
from waitbus import _compose, _predicate, broadcast, wait
from waitbus._broadcast_sub import SubscriberHandle, WaitOutcome, open_subscriber

pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="broadcast daemon SO_PEERCRED check is Linux-only",
)


# --- helpers (mirror test_waitbus_wait_universal.py shape) --------------------


def _insert(db: Path, delivery_id: str, **overrides: Any) -> None:
    """Insert one event row. Defaults to a docker container frame."""
    import sqlite3

    from waitbus import _db
    from waitbus._types import EventInsert

    defaults: dict[str, Any] = {
        "source": "docker",
        "event_type": "docker_container",
        "owner": "local",
        "repo": "docker",
        "received_at": time.time_ns(),
        "payload_json": "{}",
        "ingest_method": "watcher",
    }
    defaults.update(overrides)
    with contextlib.closing(sqlite3.connect(str(db))) as conn:
        _db.insert_event(conn, EventInsert(delivery_id=delivery_id, **defaults))


def _run_wait(paths: dict[str, Path], argv: list[str]) -> tuple[threading.Thread, list[int]]:
    """Start ``wait.main(argv)`` in a daemon thread bound to the test socket."""
    original_open = open_subscriber

    def patched_open(**kwargs: Any) -> SubscriberHandle:
        kwargs["socket_path"] = str(paths["broadcast"])
        return original_open(**kwargs)

    rc_holder: list[int] = []

    def run() -> None:
        with patch.object(wait, "open_subscriber", patched_open):
            rc_holder.append(wait.main(argv))

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return t, rc_holder


_PYTEST_CLAUSE = 'pytest:fields.event_type="pytest_session"'
_DOCKER_CLAUSE = 'docker:fields.event_type="docker_container"'
_GITHUB_CLAUSE = 'github:fields.conclusion="success"'


# --- --all-of: sticky conjunction across sources ------------------------------


@pytest.mark.asyncio
async def test_all_of_two_sources_in_order_exits_zero(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """Two clauses satisfied by two DIFFERENT events (pytest then docker)."""
    daemon, paths = running_daemon
    t, rc = _run_wait(
        paths,
        ["--all-of", _PYTEST_CLAUSE, "--all-of", _DOCKER_CLAUSE, "--timeout", "3s"],
    )
    await _await_subscribers(daemon)
    _insert(paths["db"], "c-allof-a1", source="pytest", event_type="pytest_session", repo="pytest")
    _insert(paths["db"], "c-allof-a2", source="docker", event_type="docker_container")
    await _await_thread(t)
    assert not t.is_alive(), "composed wait hung"
    assert rc == [0]


@pytest.mark.asyncio
async def test_all_of_two_sources_reversed_order_exits_zero(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """Sticky satisfaction is order-independent: docker then pytest also wakes."""
    daemon, paths = running_daemon
    t, rc = _run_wait(
        paths,
        ["--all-of", _PYTEST_CLAUSE, "--all-of", _DOCKER_CLAUSE, "--timeout", "3s"],
    )
    await _await_subscribers(daemon)
    _insert(paths["db"], "c-allof-b1", source="docker", event_type="docker_container")
    _insert(paths["db"], "c-allof-b2", source="pytest", event_type="pytest_session", repo="pytest")
    await _await_thread(t)
    assert not t.is_alive()
    assert rc == [0]


@pytest.mark.asyncio
async def test_all_of_with_outstanding_clause_times_out_124(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """One clause satisfied, the other never fires: exit 124 naming the
    outstanding clause."""
    daemon, paths = running_daemon
    t, rc = _run_wait(
        paths,
        ["--all-of", _PYTEST_CLAUSE, "--all-of", _DOCKER_CLAUSE, "--timeout", "1s"],
    )
    await _await_subscribers(daemon)
    _insert(paths["db"], "c-allof-c1", source="docker", event_type="docker_container")
    await _await_thread(t, timeout=4.0)
    assert not t.is_alive()
    assert rc == [124]
    err = capsys.readouterr().err
    assert "outstanding clauses" in err
    # The outstanding clause is named verbatim as typed, not lowered.
    assert _PYTEST_CLAUSE in err
    assert "fields.source=" not in err


@pytest.mark.asyncio
async def test_all_of_single_frame_satisfying_both_clauses_exits_zero(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """One frame may flip several clauses at once."""
    daemon, paths = running_daemon
    t, rc = _run_wait(
        paths,
        [
            "--all-of",
            _DOCKER_CLAUSE,
            "--all-of",
            'docker:fields.conclusion="success"',
            "--timeout",
            "3s",
        ],
    )
    await _await_subscribers(daemon)
    _insert(
        paths["db"],
        "c-allof-d1",
        source="docker",
        event_type="docker_container",
        conclusion="success",
    )
    await _await_thread(t)
    assert not t.is_alive()
    assert rc == [0]


# --- --first-of: single-event disjunction across sources ----------------------


@pytest.mark.asyncio
async def test_first_of_returns_on_the_earlier_clause(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """docker|github race where only the docker event arrives: exit 0."""
    daemon, paths = running_daemon
    t, rc = _run_wait(
        paths,
        ["--first-of", _DOCKER_CLAUSE, "--first-of", _GITHUB_CLAUSE, "--timeout", "3s"],
    )
    await _await_subscribers(daemon)
    _insert(paths["db"], "c-firstof-a1", source="docker", event_type="docker_container")
    await _await_thread(t)
    assert not t.is_alive()
    assert rc == [0]


@pytest.mark.asyncio
async def test_first_of_no_matching_event_times_out_124(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
    capsys: pytest.CaptureFixture[str],
) -> None:
    daemon, paths = running_daemon
    t, rc = _run_wait(
        paths,
        ["--first-of", _PYTEST_CLAUSE, "--first-of", _GITHUB_CLAUSE, "--timeout", "1s"],
    )
    await _await_subscribers(daemon)
    _insert(paths["db"], "c-firstof-b1", source="docker", event_type="docker_container")
    await _await_thread(t, timeout=4.0)
    assert not t.is_alive()
    assert rc == [124]
    assert "clauses:" in capsys.readouterr().err


# --- regression: single-source path is untouched ------------------------------


@pytest.mark.asyncio
async def test_legacy_single_source_invocation_still_exits_zero(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """The pre-composition invocation shape resolves exactly as before."""
    daemon, paths = running_daemon
    t, rc = _run_wait(
        paths,
        [
            "--source",
            "docker",
            "--match",
            'fields.event_type="docker_container"',
            "--timeout",
            "3s",
        ],
    )
    await _await_subscribers(daemon)
    _insert(paths["db"], "c-legacy-1", source="docker", event_type="docker_container")
    await _await_thread(t)
    assert not t.is_alive()
    assert rc == [0]


# --- startup errors (exit 2, no daemon needed) --------------------------------


def test_all_of_and_first_of_together_is_startup_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = wait.main(["--all-of", _PYTEST_CLAUSE, "--first-of", _DOCKER_CLAUSE, "--timeout", "1s"])
    assert rc == 2
    assert "mutually exclusive" in capsys.readouterr().err


def test_all_of_with_source_is_startup_error(capsys: pytest.CaptureFixture[str]) -> None:
    rc = wait.main(["--all-of", _PYTEST_CLAUSE, "--source", "docker", "--timeout", "1s"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "--all-of cannot be combined with" in err
    assert "--source" in err


def test_first_of_with_repo_is_startup_error(capsys: pytest.CaptureFixture[str]) -> None:
    rc = wait.main(["--first-of", _GITHUB_CLAUSE, "--repo", "o/r", "--timeout", "1s"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "--first-of cannot be combined with" in err
    assert "--repo" in err


def test_all_of_with_sha_is_startup_error(capsys: pytest.CaptureFixture[str]) -> None:
    rc = wait.main(["--all-of", _GITHUB_CLAUSE, "--sha", "abc1234", "--timeout", "1s"])
    assert rc == 2
    assert "--sha" in capsys.readouterr().err


def test_malformed_clause_is_startup_error(capsys: pytest.CaptureFixture[str]) -> None:
    """A clause with no source prefix exits 2 naming the clause verbatim."""
    rc = wait.main(["--all-of", 'fields.x="a:b"', "--timeout", "1s"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "invalid --all-of clause" in err
    assert "clause source must match" in err


def test_clause_without_colon_is_startup_error(capsys: pytest.CaptureFixture[str]) -> None:
    rc = wait.main(["--first-of", "bare_word", "--timeout", "1s"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "invalid --first-of clause" in err
    assert "source:key=json_literal" in err


# --- subprocess-level startup errors (installed CLI entry, no daemon) ---------

_WAITBUS_BIN = Path(sys.executable).parent / "waitbus"


@pytest.mark.skipif(not _WAITBUS_BIN.exists(), reason="waitbus console script not installed")
def test_cli_entry_rejects_conflicting_compose_flags() -> None:
    """Drive the installed `waitbus wait` shim: validation runs before any
    daemon contact, so the exit-2 path needs no daemon."""
    proc = subprocess.run(
        [str(_WAITBUS_BIN), "wait", "--all-of", _PYTEST_CLAUSE, "--first-of", _DOCKER_CLAUSE],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert proc.returncode == 2
    assert "mutually exclusive" in proc.stderr


@pytest.mark.skipif(not _WAITBUS_BIN.exists(), reason="waitbus console script not installed")
def test_cli_entry_rejects_malformed_clause() -> None:
    proc = subprocess.run(
        [str(_WAITBUS_BIN), "wait", "--all-of", "bare_word"],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert proc.returncode == 2
    assert "invalid --all-of clause" in proc.stderr


# --- outcome dispatcher arms not reachable from the daemon harness ------------


def _composed_dispatch(outcome: WaitOutcome, *, conjunction: bool = True) -> int:
    clauses = [
        _compose.clause_predicate(_compose.parse_clause(_PYTEST_CLAUSE)),
        _compose.clause_predicate(_compose.parse_clause(_DOCKER_CLAUSE)),
    ]
    tracker = _compose.AllOfTracker(clauses)
    with pytest.raises(typer.Exit) as excinfo:
        wait._dispatch_composed_outcome(
            outcome=outcome,
            conjunction=conjunction,
            timeout="1s",
            tracker=tracker,
            clauses=clauses,
            matched_source={"source": "docker"},
        )
    return int(excinfo.value.exit_code)


def _outcome(**kw: bool) -> WaitOutcome:
    base: dict[str, bool] = {
        "matched": False,
        "timed_out": False,
        "cancelled": False,
        "peer_closed": False,
        "framing_error": False,
    }
    base.update(kw)
    return WaitOutcome(**base)


def test_composed_dispatch_cancelled_exits_130(capsys: pytest.CaptureFixture[str]) -> None:
    assert _composed_dispatch(_outcome(cancelled=True)) == 130
    assert "wait interrupted" in capsys.readouterr().err


def test_composed_dispatch_conjunction_timeout_names_clauses_verbatim(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The timeout message lists outstanding clauses as the operator typed
    them (source:key=json_literal), never the lowered predicate text."""
    assert _composed_dispatch(_outcome(timed_out=True)) == 124
    err = capsys.readouterr().err
    assert "outstanding clauses" in err
    assert _PYTEST_CLAUSE in err
    assert _DOCKER_CLAUSE in err
    assert "fields.source=" not in err


def test_composed_dispatch_disjunction_timeout_names_clauses_verbatim(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert _composed_dispatch(_outcome(timed_out=True), conjunction=False) == 124
    err = capsys.readouterr().err
    assert "clauses:" in err
    assert _PYTEST_CLAUSE in err
    assert _DOCKER_CLAUSE in err
    assert "fields.source=" not in err


def test_composed_dispatch_peer_closed_exits_2(capsys: pytest.CaptureFixture[str]) -> None:
    assert _composed_dispatch(_outcome(peer_closed=True)) == 2
    assert "broadcast connection closed before a match" in capsys.readouterr().err


def test_composed_dispatch_disjunction_match_names_source(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert _composed_dispatch(_outcome(matched=True), conjunction=False) == 0
    assert "matched on source=docker" in capsys.readouterr().err


def test_composed_dispatch_conjunction_match_reports_all_satisfied(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert _composed_dispatch(_outcome(matched=True)) == 0
    assert "all clauses satisfied" in capsys.readouterr().err


def test_build_composed_decide_skips_frames_without_fields() -> None:
    """A truncated frame (no fields dict) never matches, mirroring the
    single-source decide closure."""
    clauses = [_compose.clause_predicate(_compose.parse_clause(_DOCKER_CLAUSE))]
    tracker = _compose.AllOfTracker(clauses)
    decide = wait._build_composed_decide(clauses, True, tracker, {})
    from waitbus._broadcast_sub import FrameDecision

    assert decide({"kind": "event"}) is FrameDecision.CONTINUE
    assert tracker.outstanding != ()


def test_compose_any_used_by_disjunction_decide() -> None:
    """The disjunction decide closure matches the first frame ANY clause accepts."""
    clauses = [
        _compose.clause_predicate(_compose.parse_clause(_DOCKER_CLAUSE)),
        _compose.clause_predicate(_compose.parse_clause(_PYTEST_CLAUSE)),
    ]
    tracker = _compose.AllOfTracker(clauses)
    matched_source: dict[str, str | None] = {}
    decide = wait._build_composed_decide(clauses, False, tracker, matched_source)
    from waitbus._broadcast_sub import FrameDecision

    frame = {"fields": {"source": "pytest", "event_type": "pytest_session"}}
    assert decide(frame) is FrameDecision.MATCHED
    assert matched_source["source"] == "pytest"
    # `_predicate.compose_any` is the combinator behind the disjunction form.
    assert _predicate.compose_any(*clauses)(frame) is True
