"""Coverage for the pr_monitor wake loop, its exit-code matrix, and the
subscribe-reject regression.

The migration to ``open_subscriber`` + ``await_predicate`` left
``_pr_monitor_tick`` / ``main`` uncovered. These tests drive the loop with a
fake subscriber and a monkeypatched ``await_predicate`` so no live daemon is
needed. The token-reject case is the subscribe-rejection regression: the daemon's
``subscribe_rejected`` frame is read by ``await_predicate`` INSIDE the loop
(not by the subscriber factory), so the reject must surface as a clean
``return 2`` rather than an unhandled traceback escaping ``main``.
"""

from __future__ import annotations

import socket
import sqlite3
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from waitbus import pr_monitor
from waitbus._broadcast_sub import (
    BroadcastConnectionError,
    SubscriberHandle,
    SubscriberLaggedError,
    TokenRequiredError,
    WaitOutcome,
)


def _outcome(*, peer_closed: bool = False, framing_error: bool = False) -> WaitOutcome:
    """A wake/EOF outcome; ``timed_out`` is the inverse of ``peer_closed``."""
    return WaitOutcome(
        matched=False,
        timed_out=not peer_closed,
        cancelled=False,
        peer_closed=peer_closed,
        framing_error=framing_error,
    )


@pytest.fixture
def fake_sub() -> Iterator[SubscriberHandle]:
    """A SubscriberHandle wrapping one end of a real socketpair (never read —
    await_predicate is monkeypatched)."""
    left, right = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        yield SubscriberHandle(sock=left)
    finally:
        left.close()
        right.close()


def _call_tick(
    monkeypatch: pytest.MonkeyPatch,
    sub: SubscriberHandle,
    *,
    outcome: WaitOutcome,
    terminal: bool,
) -> int | None:
    """Invoke _pr_monitor_tick with await_predicate/tick stubbed to the given
    outcome + terminal verdict. sha_at is set to now so the sha-refresh arm is
    skipped (no subprocess)."""
    import time

    monkeypatch.setattr(pr_monitor, "await_predicate", lambda *a, **k: outcome)
    monkeypatch.setattr(pr_monitor, "tick", lambda *a, **k: terminal)
    conn = sqlite3.connect(":memory:")
    try:
        return pr_monitor._pr_monitor_tick(
            conn,
            sub,
            "o",
            "r",
            [1],
            {1: "abc"},
            {},
            {1: False},
            [time.time()],
            300,
            0,
        )
    finally:
        conn.close()


def test_tick_wake_and_continue_returns_none(monkeypatch: pytest.MonkeyPatch, fake_sub: SubscriberHandle) -> None:
    assert _call_tick(monkeypatch, fake_sub, outcome=_outcome(), terminal=False) is None


def test_tick_peer_closed_with_framing_error_returns_1(
    monkeypatch: pytest.MonkeyPatch, fake_sub: SubscriberHandle
) -> None:
    outcome = _outcome(peer_closed=True, framing_error=True)
    assert _call_tick(monkeypatch, fake_sub, outcome=outcome, terminal=False) == 1


def test_tick_peer_closed_clean_returns_0(monkeypatch: pytest.MonkeyPatch, fake_sub: SubscriberHandle) -> None:
    outcome = _outcome(peer_closed=True, framing_error=False)
    assert _call_tick(monkeypatch, fake_sub, outcome=outcome, terminal=False) == 0


def test_tick_deadline_then_terminal_returns_0(
    monkeypatch: pytest.MonkeyPatch, fake_sub: SubscriberHandle, capsys: pytest.CaptureFixture[str]
) -> None:
    assert _call_tick(monkeypatch, fake_sub, outcome=_outcome(), terminal=True) == 0
    assert "MONITOR_DONE" in capsys.readouterr().out


@pytest.mark.parametrize("reject_exc", [TokenRequiredError, SubscriberLaggedError])
def test_main_subscribe_reject_mid_loop_returns_2(
    monkeypatch: pytest.MonkeyPatch,
    tmp_db_path: Path,
    fake_sub: SubscriberHandle,
    capsys: pytest.CaptureFixture[str],
    reject_exc: type[BroadcastConnectionError],
) -> None:
    """subscribe-rejection regression: a daemon reject raised by await_predicate inside the
    loop is caught and turned into a clean ``return 2``, not a traceback."""

    def _factory(**_kwargs: Any) -> SubscriberHandle:
        return fake_sub

    def _raise(*_a: Any, **_k: Any) -> WaitOutcome:
        raise reject_exc("rejected by daemon", remediation="reconnect")

    monkeypatch.setattr(pr_monitor, "db_path", lambda: tmp_db_path)
    monkeypatch.setattr(pr_monitor, "head_sha", lambda _pr: "abc")
    monkeypatch.setattr(pr_monitor, "tick", lambda *a, **k: False)
    monkeypatch.setattr(pr_monitor, "await_predicate", _raise)

    rc = pr_monitor.main(["--owner", "o", "--repo", "r", "--pr", "1"], subscriber_factory=_factory)
    assert rc == 2
    assert "pr_monitor:" in capsys.readouterr().err


def test_head_sha_returns_stripped_oid(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("waitbus.pr_monitor.subprocess.check_output", lambda *a, **k: "deadbeef\n")
    assert pr_monitor.head_sha(7) == "deadbeef"


def test_head_sha_none_on_subprocess_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(*_a: Any, **_k: Any) -> str:
        raise OSError("gh missing")

    monkeypatch.setattr("waitbus.pr_monitor.subprocess.check_output", _boom)
    assert pr_monitor.head_sha(7) is None


def test_detect_repo_parses_origin(monkeypatch: pytest.MonkeyPatch) -> None:
    class _R:
        stdout = "git@github.com:o/r.git\n"

    monkeypatch.setattr("waitbus.pr_monitor.subprocess.run", lambda *a, **k: _R())
    assert pr_monitor.detect_repo() == ("o", "r")


def test_detect_repo_none_when_no_remote(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(*_a: Any, **_k: Any) -> Any:
        raise FileNotFoundError

    monkeypatch.setattr("waitbus.pr_monitor.subprocess.run", _boom)
    assert pr_monitor.detect_repo() is None


def test_resolve_owner_repo_falls_back_to_detect(monkeypatch: pytest.MonkeyPatch) -> None:
    import argparse

    monkeypatch.setattr(pr_monitor, "detect_repo", lambda: None)
    args = argparse.Namespace(owner=None, repo=None)
    assert pr_monitor._resolve_owner_repo(args) == 2


def test_tick_refreshes_head_sha_on_cadence(monkeypatch: pytest.MonkeyPatch, fake_sub: SubscriberHandle) -> None:
    """sha_at far in the past forces the refresh branch; head_sha is re-fetched."""
    calls: list[int] = []

    def _record_head_sha(pr: int) -> str:
        calls.append(pr)
        return "newsha"

    monkeypatch.setattr(pr_monitor, "head_sha", _record_head_sha)
    monkeypatch.setattr(pr_monitor, "await_predicate", lambda *a, **k: _outcome())
    monkeypatch.setattr(pr_monitor, "tick", lambda *a, **k: False)
    conn = sqlite3.connect(":memory:")
    pr_sha: dict[int, str | None] = {1: "old"}
    try:
        result = pr_monitor._pr_monitor_tick(conn, fake_sub, "o", "r", [1], pr_sha, {}, {1: False}, [0.0], 300, 0)
    finally:
        conn.close()
    assert result is None
    assert calls == [1] and pr_sha[1] == "newsha"


def test_main_keyboard_interrupt_returns_130(
    monkeypatch: pytest.MonkeyPatch,
    tmp_db_path: Path,
    fake_sub: SubscriberHandle,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def _factory(**_kwargs: Any) -> SubscriberHandle:
        return fake_sub

    def _interrupt(*_a: Any, **_k: Any) -> WaitOutcome:
        raise KeyboardInterrupt

    monkeypatch.setattr(pr_monitor, "db_path", lambda: tmp_db_path)
    monkeypatch.setattr(pr_monitor, "head_sha", lambda _pr: "abc")
    monkeypatch.setattr(pr_monitor, "tick", lambda *a, **k: False)
    monkeypatch.setattr(pr_monitor, "await_predicate", _interrupt)

    rc = pr_monitor.main(["--owner", "o", "--repo", "r", "--pr", "1"], subscriber_factory=_factory)
    assert rc == 130
    assert "MONITOR_INTERRUPTED" in capsys.readouterr().out
