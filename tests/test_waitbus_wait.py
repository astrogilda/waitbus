"""Contract + exit-matrix tests for ``waitbus wait``.

Covers, against a live in-process broadcast daemon (the ``running_daemon``
conftest fixture):

* the full 8-value GitHub ``conclusion`` -> exit-code matrix
* timeout -> 124 (coreutils convention)
* SIGINT -> 130 with clean teardown and no spurious match
* ``--repo`` git-remote default (detect_repo) + the never-silently-``*`` rule
* ``--no-exit-status`` opt-out
* duration parsing (unit suffixes)

Linux-only: the broadcast daemon's SO_PEERCRED check is Linux-only.
"""

from __future__ import annotations

import asyncio
import contextlib
import select
import socket
import sys
import threading
import time
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from tests._daemon_helpers import (
    await_subscribers as _await_subscribers,
)
from tests._daemon_helpers import (
    await_thread as _await_thread,
)
from waitbus import broadcast, wait
from waitbus._broadcast_sub import (
    BroadcastConnectionError,
    SubscriberHandle,
    open_subscriber,
)
from waitbus._terminal import (
    FAILURE_CONCLUSIONS,
    NON_TERMINAL_CONCLUSIONS,
    SUCCESS_CONCLUSION,
)

pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="broadcast daemon SO_PEERCRED check is Linux-only",
)


# --- helpers -------------------------------------------------------------


def _insert(db: Path, delivery_id: str, **overrides: Any) -> None:
    """Insert one event row (mirrors the conftest helper in test_broadcast_sub)."""
    import sqlite3

    from waitbus import _db
    from waitbus._types import EventInsert

    defaults: dict[str, Any] = {
        "source": "github",
        "event_type": "workflow_job",
        "owner": "test-owner",
        "repo": "test-repo",
        "received_at": time.time_ns(),
        "payload_json": "{}",
        "ingest_method": "webhook",
        "run_id": 1,
        "workflow_name": "Tests",
        "head_branch": "main",
        "head_sha": "deadbeef",
        "status": "completed",
        "conclusion": "success",
        "job_id": 1,
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


# --- the 8-value conclusion -> exit matrix -------------------------------

# GitHub's `conclusion` field has exactly these eight values. The mapping
# is asserted against the canonical _terminal frozensets (the single
# source of truth) to guarantee the test and the implementation cannot
# drift apart.
_TERMINAL_EXIT = {
    "success": 0,
    "failure": 1,
    "cancelled": 1,
    "timed_out": 1,
}
_NON_TERMINAL = ("skipped", "neutral", "action_required", "stale")


def test_matrix_covers_all_eight_github_conclusions() -> None:
    """The test matrix is exhaustive over GitHub's real 8 conclusions and
    matches the canonical _terminal bucketing (no parallel map)."""
    all_eight = set(_TERMINAL_EXIT) | set(_NON_TERMINAL)
    assert all_eight == ({SUCCESS_CONCLUSION} | FAILURE_CONCLUSIONS | NON_TERMINAL_CONCLUSIONS)
    assert len(all_eight) == 8


@pytest.mark.asyncio
@pytest.mark.parametrize(("conclusion", "expected_rc"), sorted(_TERMINAL_EXIT.items()))
async def test_terminal_conclusion_drives_exit(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
    conclusion: str,
    expected_rc: int,
) -> None:
    """Each terminal conclusion exits with its bucketed code."""
    daemon, paths = running_daemon
    t, rc = _run_wait(
        paths,
        ["--sha", "deadbeef", "--repo", "test-owner/test-repo", "--timeout", "3s"],
    )
    await _await_subscribers(daemon)
    _insert(paths["db"], f"d-{conclusion}", conclusion=conclusion)
    await _await_thread(t)
    assert not t.is_alive(), f"wait hung on conclusion={conclusion}"
    assert rc == [expected_rc]


@pytest.mark.asyncio
@pytest.mark.parametrize("conclusion", _NON_TERMINAL)
async def test_non_terminal_conclusion_keeps_waiting_then_times_out(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
    conclusion: str,
) -> None:
    """skipped/neutral/action_required/stale do NOT terminate the wait;
    with no subsequent terminal frame the wait expires -> 124."""
    daemon, paths = running_daemon
    t, rc = _run_wait(
        paths,
        ["--sha", "deadbeef", "--repo", "test-owner/test-repo", "--timeout", "1s"],
    )
    await _await_subscribers(daemon)
    _insert(paths["db"], f"d-nonterm-{conclusion}", conclusion=conclusion)
    await _await_thread(t, timeout=4.0)
    assert not t.is_alive()
    assert rc == [124], f"{conclusion} should be non-terminal -> timeout 124"


@pytest.mark.asyncio
async def test_non_terminal_then_terminal_resolves(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """A non-terminal frame followed by a terminal one resolves on the
    terminal frame (the wait survived the non-terminal one)."""
    daemon, paths = running_daemon
    t, rc = _run_wait(
        paths,
        ["--sha", "deadbeef", "--repo", "test-owner/test-repo", "--timeout", "3s"],
    )
    await _await_subscribers(daemon)
    _insert(paths["db"], "d-skip", conclusion="skipped")
    await asyncio.sleep(0.2)
    _insert(paths["db"], "d-fail", conclusion="failure")
    await _await_thread(t)
    assert not t.is_alive()
    assert rc == [1]


# --- timeout -> 124 ------------------------------------------------------


@pytest.mark.asyncio
async def test_timeout_exits_124(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """No matching frame within --timeout -> exit 124 (coreutils)."""
    _daemon, paths = running_daemon
    t, rc = _run_wait(
        paths,
        ["--sha", "fade123", "--repo", "test-owner/test-repo", "--timeout", "1s"],
    )
    await _await_thread(t, timeout=4.0)
    assert not t.is_alive()
    assert rc == [124]


@pytest.mark.asyncio
async def test_wrong_sha_times_out(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """A terminal frame for a DIFFERENT sha must not satisfy the wait."""
    daemon, paths = running_daemon
    t, rc = _run_wait(
        paths,
        ["--sha", "abc1234", "--repo", "test-owner/test-repo", "--timeout", "1s"],
    )
    await _await_subscribers(daemon)
    _insert(paths["db"], "d-other", head_sha="deadbeefcafe", conclusion="failure")
    await _await_thread(t, timeout=4.0)
    assert not t.is_alive()
    assert rc == [124], "wrong-sha frame must not match"


@pytest.mark.asyncio
async def test_short_sha_prefix_matches_full_head_sha(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """A 7-char `--sha` prefix resolves against the full 40-char head_sha
    GitHub stores (git-style abbreviation)."""
    daemon, paths = running_daemon
    full_sha = "abc1234def5678901234567890abcdef12345678"
    t, rc = _run_wait(
        paths,
        ["--sha", full_sha[:7], "--repo", "test-owner/test-repo", "--timeout", "3s"],
    )
    await _await_subscribers(daemon)
    _insert(paths["db"], "d-prefix", head_sha=full_sha, conclusion="success")
    await _await_thread(t)
    assert not t.is_alive(), "wait hung on a valid short-SHA prefix"
    assert rc == [0]


@pytest.mark.asyncio
async def test_sha_prefix_is_bounded_not_substring(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """The prefix anchors at the START: a head_sha that merely contains the
    prefix mid-string (not as a prefix) must NOT match."""
    daemon, paths = running_daemon
    t, rc = _run_wait(
        paths,
        ["--sha", "abc1234", "--repo", "test-owner/test-repo", "--timeout", "1s"],
    )
    await _await_subscribers(daemon)
    # head_sha contains "abc1234" at offset 3, not as a prefix.
    _insert(paths["db"], "d-mid", head_sha="999abc1234def", conclusion="success")
    await _await_thread(t, timeout=4.0)
    assert not t.is_alive()
    assert rc == [124], "mid-string occurrence must not satisfy a prefix match"


def test_too_short_sha_is_startup_error(capsys: pytest.CaptureFixture[str]) -> None:
    """A sub-7-char `--sha` is a startup error (2), never a silent never-match."""
    rc = wait.main(["--sha", "abc12", "--repo", "o/r", "--timeout", "1s"])
    assert rc == 2
    assert "at least 7 hex" in capsys.readouterr().err


def test_non_hex_sha_is_startup_error(capsys: pytest.CaptureFixture[str]) -> None:
    """A non-hex `--sha` is a startup error (2)."""
    rc = wait.main(["--sha", "nothex1", "--repo", "o/r", "--timeout", "1s"])
    assert rc == 2
    assert "hexadecimal" in capsys.readouterr().err


# --- SIGINT -> 130 -------------------------------------------------------


@pytest.mark.asyncio
async def test_sigint_exits_130_clean_no_spurious_match(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """KeyboardInterrupt during the wait -> exit 130, clean teardown,
    NO spurious terminal match (the predicate never fired)."""
    _daemon, paths = running_daemon

    original_open = open_subscriber

    def patched_open(**kwargs: Any) -> SubscriberHandle:
        kwargs["socket_path"] = str(paths["broadcast"])
        return original_open(**kwargs)

    rc_holder: list[int] = []

    def run() -> None:
        # await_predicate translates KeyboardInterrupt -> cancelled. We
        # raise it from inside select.select to model a real Ctrl-C
        # arriving mid-wait. _broadcast_sub does `import select`, so the
        # module object it holds IS the stdlib singleton; string-target
        # patching its `select` attribute is runtime-identical to reaching
        # through the module object, without the mypy attr-defined reach.
        real_select = select.select
        fired = {"n": 0}

        def boom(*a: Any, **k: Any) -> Any:
            fired["n"] += 1
            if fired["n"] >= 2:
                raise KeyboardInterrupt
            return real_select(*a, **k)

        with (
            patch.object(wait, "open_subscriber", patched_open),
            patch("waitbus._broadcast_sub.select.select", boom),
        ):
            rc_holder.append(
                wait.main(
                    [
                        "--sha",
                        "deadbeef",
                        "--repo",
                        "test-owner/test-repo",
                        "--timeout",
                        "10s",
                    ]
                )
            )

    t = threading.Thread(target=run, daemon=True)
    t.start()
    await _await_thread(t, timeout=4.0)
    assert not t.is_alive(), "wait did not exit on SIGINT"
    assert rc_holder == [130]


# --- --repo git-remote default ------------------------------------------


def test_repo_defaults_from_git_remote() -> None:
    """When --repo is omitted, wait derives owner/repo from detect_repo
    (the existing git-remote autodetect). It must never silently use '*'.

    No ``running_daemon`` fixture: the wait verb errors out at the
    ``BroadcastConnectionError`` raised by the mocked ``open_subscriber``
    before any socket I/O happens.
    """
    captured: dict[str, Any] = {}

    def fake_open(**kwargs: Any) -> socket.socket:
        captured.update(kwargs)
        raise BroadcastConnectionError("stop here", "no daemon")

    with (
        patch.object(wait, "detect_repo", lambda: ("auto-owner", "auto-repo")),
        patch.object(wait, "open_subscriber", fake_open),
    ):
        rc = wait.main(["--sha", "abc1234", "--timeout", "1s"])

    assert rc == 2  # BroadcastConnectionError -> startup failure
    assert captured["filters"] == ["auto-owner/auto-repo"]
    assert captured["filters"] != ["*"]


def test_repo_default_missing_remote_is_startup_error() -> None:
    """No --repo and detect_repo() returns None -> startup error (2),
    NEVER a silent wildcard subscription."""
    with patch.object(wait, "detect_repo", lambda: None):
        rc = wait.main(["--sha", "abc1234", "--timeout", "1s"])
    assert rc == 2


def test_explicit_repo_overrides_detect() -> None:
    """An explicit --repo is used verbatim and detect_repo is not consulted."""
    captured: dict[str, Any] = {}

    def fake_open(**kwargs: Any) -> socket.socket:
        captured.update(kwargs)
        raise BroadcastConnectionError("stop", "no daemon")

    def boom_detect() -> tuple[str, str] | None:  # pragma: no cover
        raise AssertionError("detect_repo must not be called when --repo given")

    with patch.object(wait, "detect_repo", boom_detect), patch.object(wait, "open_subscriber", fake_open):
        rc = wait.main(["--sha", "abc1234", "--repo", "o/r", "--timeout", "1s"])
    assert rc == 2
    assert captured["filters"] == ["o/r"]


def test_malformed_repo_is_startup_error() -> None:
    """A non owner/repo --repo value is a startup error (2)."""
    rc = wait.main(["--sha", "abc1234", "--repo", "not-a-repo", "--timeout", "1s"])
    assert rc == 2


# --- --no-exit-status opt-out -------------------------------------------


@pytest.mark.asyncio
async def test_no_exit_status_failure_returns_zero(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """--no-exit-status: a terminal FAILURE still exits 0 (opt-out of the
    conclusion-driven exit code) while timeout/SIGINT are unaffected."""
    daemon, paths = running_daemon
    t, rc = _run_wait(
        paths,
        [
            "--sha",
            "deadbeef",
            "--repo",
            "test-owner/test-repo",
            "--timeout",
            "3s",
            "--no-exit-status",
        ],
    )
    await _await_subscribers(daemon)
    _insert(paths["db"], "d-noexit", conclusion="failure")
    await _await_thread(t)
    assert not t.is_alive()
    assert rc == [0]


# --- duration parsing ----------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "seconds"),
    [
        ("30", 30.0),
        ("30s", 30.0),
        ("5m", 300.0),
        ("2h", 7200.0),
        ("1d", 86400.0),
        ("1.5m", 90.0),
    ],
)
def test_parse_duration_units(raw: str, seconds: float) -> None:
    from waitbus._duration import parse_duration

    assert parse_duration(raw) == seconds


@pytest.mark.parametrize("bad", ["", "0", "-5", "abc", "10x", "  "])
def test_parse_duration_rejects_bad(bad: str) -> None:
    from waitbus._duration import parse_duration

    with pytest.raises(ValueError):
        parse_duration(bad)


def test_invalid_timeout_is_startup_error() -> None:
    rc = wait.main(["--sha", "abc1234", "--repo", "o/r", "--timeout", "nope"])
    assert rc == 2


def test_exit_code_if_terminal_buckets() -> None:
    """_exit_code_if_terminal delegates to the canonical _terminal frozensets."""
    assert wait._exit_code_if_terminal("success") == 0
    for c in FAILURE_CONCLUSIONS:
        assert wait._exit_code_if_terminal(c) == 1
    for c in NON_TERMINAL_CONCLUSIONS:
        assert wait._exit_code_if_terminal(c) is None
    assert wait._exit_code_if_terminal(None) is None
    assert wait._exit_code_if_terminal("") is None
