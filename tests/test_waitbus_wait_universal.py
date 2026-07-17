"""Universal-source regression suite for ``waitbus wait``.

``waitbus wait`` must resolve on
non-GitHub frames (docker / pytest / fs / alertmanager) when given a
matching ``--match`` predicate. Mirrors the test_waitbus_wait.py
fixtures + helpers; new behaviour only.
"""

from __future__ import annotations

import contextlib
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
from waitbus._broadcast_sub import SubscriberHandle, open_subscriber

pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="broadcast daemon SO_PEERCRED check is Linux-only",
)


# --- helpers (mirror test_waitbus_wait.py shape) ------------------------------


def _insert(db: Path, delivery_id: str, **overrides: Any) -> None:
    """Insert one event row. Defaults to a docker container_exit frame."""
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


# --- non-GitHub source matches: the load-bearing regression -----------------


@pytest.mark.asyncio
async def test_docker_match_resolves_exit_zero(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """`waitbus wait --source docker --match fields.event_type=container_exit`
    resolves on a matching docker frame, exit 0 (no conclusion vocabulary
    for non-GitHub sources)."""
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
    _insert(paths["db"], "d-docker-1", source="docker", event_type="docker_container")
    await _await_thread(t)
    assert not t.is_alive(), "wait hung on docker frame"
    assert rc == [0]


@pytest.mark.asyncio
async def test_pytest_match_resolves_exit_zero(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """`waitbus wait --source pytest --match fields.event_type=pytest_session`
    resolves on a matching pytest frame."""
    daemon, paths = running_daemon
    t, rc = _run_wait(
        paths,
        [
            "--source",
            "pytest",
            "--match",
            'fields.event_type="pytest_session"',
            "--timeout",
            "3s",
        ],
    )
    await _await_subscribers(daemon)
    _insert(
        paths["db"],
        "d-pytest-1",
        source="pytest",
        event_type="pytest_session",
        owner="local",
        repo="pytest",
    )
    await _await_thread(t)
    assert not t.is_alive()
    assert rc == [0]


@pytest.mark.asyncio
async def test_fs_match_resolves_exit_zero(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """`waitbus wait --source fs --match fields.event_type=closed` resolves
    on a matching filesystem-watcher frame."""
    daemon, paths = running_daemon
    t, rc = _run_wait(
        paths,
        [
            "--source",
            "fs",
            "--match",
            'fields.event_type="fs_change"',
            "--timeout",
            "3s",
        ],
    )
    await _await_subscribers(daemon)
    _insert(
        paths["db"],
        "d-fs-1",
        source="fs",
        event_type="fs_change",
        owner="local",
        repo="fs",
    )
    await _await_thread(t)
    assert not t.is_alive()
    assert rc == [0]


# --- repo / detect_repo relaxation ------------------------------------------


def test_non_github_source_does_not_call_detect_repo(serve_dirs: dict[str, Path]) -> None:
    """`--source docker` (no --repo) must NOT consult detect_repo() --
    that was the third leg of the GitHub-only triple lockout."""
    called = {"n": 0}

    def fake_detect_repo() -> tuple[str, str] | None:
        called["n"] += 1
        return None

    # `serve_dirs` isolates WAITBUS_RUNTIME_DIR to an empty per-test dir so no
    # daemon socket exists: the wait fails cleanly at the open_subscriber step
    # (startup exit 2), NOT against whatever daemon the host happens to be
    # running at the default socket. detect_repo() must have already been
    # skipped before that failure.
    with patch.object(wait, "detect_repo", fake_detect_repo):
        rc = wait.main(
            [
                "--source",
                "docker",
                "--match",
                'fields.action="die"',
                "--timeout",
                "0.1s",
            ]
        )
    # We expect EITHER startup-2 (no daemon) OR timeout-124 (if a daemon
    # happens to be running). Either way: detect_repo() was NOT called.
    assert called["n"] == 0, "detect_repo must NOT run for non-GitHub source"
    assert rc in (2, 124)


def test_repo_with_non_github_source_warns_and_ignores(
    serve_dirs: dict[str, Path],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--source docker --repo o/r`: --repo is ignored with a stderr note.

    `serve_dirs` isolates WAITBUS_RUNTIME_DIR to an empty per-test dir so the
    wait fails cleanly at open_subscriber (startup exit 2) rather than hitting a
    daemon the host happens to be running at the default socket.
    """
    rc = wait.main(
        [
            "--source",
            "docker",
            "--repo",
            "owner/repo",
            "--match",
            'fields.action="die"',
            "--timeout",
            "0.1s",
        ]
    )
    err = capsys.readouterr().err
    assert "--repo 'owner/repo' ignored" in err
    assert "repo filter is GitHub-only" in err
    # The wait still proceeds (rc 2 if no daemon, 124 if daemon running).
    assert rc in (2, 124)


# --- predicate composition --------------------------------------------------


@pytest.mark.asyncio
async def test_or_within_repeated_key(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """Two --match flags with the same key match EITHER value.

    Uses ``fields.conclusion`` (a real multi-value column) so the OR test
    doesn't have to invent an event_type the daemon would reject as
    unsupported.
    """
    daemon, paths = running_daemon
    t, rc = _run_wait(
        paths,
        [
            "--source",
            "docker",
            "--match",
            'fields.conclusion="success"',
            "--match",
            'fields.conclusion="failure"',
            "--timeout",
            "3s",
        ],
    )
    await _await_subscribers(daemon)
    # Insert a docker frame with conclusion="failure" -- the SECOND
    # alternative; predicate must still match (OR-within-key).
    _insert(
        paths["db"],
        "d-or-1",
        source="docker",
        event_type="docker_container",
        conclusion="failure",
    )
    await _await_thread(t)
    assert not t.is_alive()
    assert rc == [0]


@pytest.mark.asyncio
async def test_and_across_keys_requires_both(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """--match across distinct keys AND-combines; partial match must NOT match."""
    daemon, paths = running_daemon
    t, rc = _run_wait(
        paths,
        [
            "--source",
            "docker",
            "--match",
            'fields.event_type="docker_container"',
            "--match",
            'fields.workflow_name="api"',
            "--timeout",
            "1s",
        ],
    )
    await _await_subscribers(daemon)
    # Insert a frame matching only the first key -- must timeout, NOT match.
    _insert(
        paths["db"],
        "d-partial",
        source="docker",
        event_type="docker_container",
        workflow_name="other",
    )
    await _await_thread(t, timeout=4.0)
    assert not t.is_alive()
    assert rc == [124]


# --- startup errors ---------------------------------------------------------


def test_no_predicate_is_startup_error(capsys: pytest.CaptureFixture[str]) -> None:
    """`waitbus wait --timeout 1s` (no predicate at all) exits 2 with hint."""
    rc = wait.main(["--timeout", "1s"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "at least one of --sha / --match / --cond" in err


def test_malformed_match_is_startup_error(capsys: pytest.CaptureFixture[str]) -> None:
    """A --match spec without `=` exits 2 quoting the offending spec."""
    rc = wait.main(["--source", "docker", "--match", "bare_word", "--timeout", "1s"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "must be key=json_literal" in err
    assert "'bare_word'" in err


def test_match_cel_without_extra_is_startup_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--match-cel without the cel extra installed exits 2 with install hint."""
    rc = wait.main(
        [
            "--source",
            "github",
            "--repo",
            "o/r",
            "--match-cel",
            "fields.x > 5",
            "--timeout",
            "1s",
        ]
    )
    assert rc == 2
    err = capsys.readouterr().err
    assert "to use --match-cel, install waitbus[cel]" in err


def test_match_jmespath_without_extra_is_startup_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--match-jmespath without the jmespath extra installed exits 2 with hint."""
    rc = wait.main(
        [
            "--source",
            "github",
            "--repo",
            "o/r",
            "--match-jmespath",
            "fields.x",
            "--timeout",
            "1s",
        ]
    )
    assert rc == 2
    err = capsys.readouterr().err
    assert "to use --match-jmespath, install waitbus[jmespath]" in err


def test_sha_with_conflicting_source_is_startup_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--sha X --source docker`: the sugar implies GitHub; conflict rejected."""
    rc = wait.main(
        [
            "--sha",
            "abc1234",
            "--source",
            "docker",
            "--timeout",
            "1s",
        ]
    )
    assert rc == 2
    err = capsys.readouterr().err
    assert "implies --source github" in err


def test_unknown_cond_is_startup_error(capsys: pytest.CaptureFixture[str]) -> None:
    """`--cond no-such-name` exits 2 with the registered-names hint."""
    rc = wait.main(
        [
            "--source",
            "docker",
            "--cond",
            "no-such-condition",
            "--timeout",
            "1s",
        ]
    )
    assert rc == 2
    err = capsys.readouterr().err
    assert "no-such-condition" in err
    assert "registered:" in err


# --- --sha / exact-match agreement at full SHA length -----------------------


@pytest.mark.asyncio
async def test_sha_sugar_equivalent_to_match_head_sha(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """At FULL SHA length, `--sha deadbeef` and the exact
    `--source github --match fields.head_sha="deadbeef"` produce identical
    exit outcomes, because a full-length prefix spans the whole stored SHA.
    (They diverge for a shorter `--sha`, which prefix-matches -- covered by
    test_short_sha_prefix_matches_full_head_sha in test_waitbus_wait.py.)

    Each path subscribes BEFORE the matching frame is inserted so the
    live-tail path delivers it (mirrors the other test_waitbus_wait.py
    cases; --since with a non-ULID cursor is rejected).
    """
    daemon, paths = running_daemon

    # Path A: --sha sugar. Start the wait, then insert a matching frame.
    t1, rc1 = _run_wait(
        paths,
        ["--sha", "deadbeef", "--repo", "test-owner/test-repo", "--timeout", "3s"],
    )
    await _await_subscribers(daemon)
    _insert(
        paths["db"],
        "d-sugar-eq-a",
        source="github",
        event_type="workflow_job",
        owner="test-owner",
        repo="test-repo",
        head_sha="deadbeef",
        conclusion="success",
        status="completed",
        run_id=1,
        workflow_name="Tests",
        head_branch="main",
        job_id=1,
    )
    await _await_thread(t1)
    assert rc1 == [0], f"--sha sugar should match terminal success, got rc={rc1}"

    # Path B: explicit --match (semantically equivalent).
    t2, rc2 = _run_wait(
        paths,
        [
            "--source",
            "github",
            "--repo",
            "test-owner/test-repo",
            "--match",
            'fields.head_sha="deadbeef"',
            "--timeout",
            "3s",
        ],
    )
    await _await_subscribers(daemon)
    _insert(
        paths["db"],
        "d-sugar-eq-b",
        source="github",
        event_type="workflow_job",
        owner="test-owner",
        repo="test-repo",
        head_sha="deadbeef",
        conclusion="success",
        status="completed",
        run_id=1,
        workflow_name="Tests",
        head_branch="main",
        job_id=1,
    )
    await _await_thread(t2)
    assert rc2 == [0], f"--match should match terminal success, got rc={rc2}"
    assert rc1 == rc2, "sugar and explicit --match diverged"
