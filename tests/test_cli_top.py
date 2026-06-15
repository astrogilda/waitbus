"""Tests for ``waitbus top`` -- live, collapsed, full-screen view of the event bus.

Covers the pure render helpers and collapse model (no daemon), the startup
guards, and an end-to-end bounded run against the in-process daemon proving the
collapse key folds same-entity frames into one row and the append-only non-TTY
fallback emits one line per frame. Linux-only: the daemon's SO_PEERCRED check is.
"""

from __future__ import annotations

import asyncio
import json
import socket
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
from typer.testing import CliRunner

from waitbus import _broadcast_sub, broadcast
from waitbus import top as top_mod
from waitbus._types import EventInsert
from waitbus.cli.main import app
from waitbus.sources._protocol import SourceSpec
from waitbus.sources._registry import _clear_for_test_isolation, is_known_source, register_source

pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="broadcast daemon SO_PEERCRED check is Linux-only",
)

runner = CliRunner()


@pytest.fixture(autouse=True)
def _isolate_registry() -> Iterator[None]:
    _clear_for_test_isolation()
    yield
    _clear_for_test_isolation()


def _gh_run_frame(run_id: int, conclusion: str | None, *, status: str = "completed") -> dict[str, object]:
    return {
        "event_id": f"e-{run_id}-{conclusion}",
        "event_type": "workflow_run",
        "owner": "acme",
        "repo": "acme/widgets",
        "summary": f"run {run_id} {conclusion or status}",
        "fields": {"source": "github", "run_id": run_id, "status": status, "conclusion": conclusion},
    }


# ---------------------------------------------------------------------------
# Pure render helpers + collapse model (no daemon)
# ---------------------------------------------------------------------------


def test_display_key_uses_entity_key_for_github_run() -> None:
    """A GitHub workflow_run collapses on its stable run-id entity key."""
    assert top_mod._display_key(_gh_run_frame(42, "success")) == ("github", "run", "42")


def test_display_key_folds_passthrough_by_source_type_repo() -> None:
    """A pass-through source (no entity key) folds by (source, event_type, repo)."""
    frame = {"event_type": "pytest_session", "repo": "acme/widgets", "fields": {"source": "pytest"}}
    assert top_mod._display_key(frame) == ("by-type", "pytest", "pytest_session", "acme/widgets")


def test_status_token_prefers_conclusion_then_status() -> None:
    assert top_mod._status_token({"fields": {"conclusion": "failure", "status": "completed"}}) == "failure"
    assert top_mod._status_token({"fields": {"status": "in_progress"}}) == "in_progress"
    assert top_mod._status_token({"fields": {}}) == "-"
    assert top_mod._status_token({"fields": None}) == "-"  # non-dict fields -> "-"


def test_model_collapses_same_entity_to_one_updating_row() -> None:
    """Two frames for the same run collapse to one row carrying the latest state."""
    model = top_mod._TopModel()
    model.update(_gh_run_frame(7, None, status="in_progress"))
    model.update(_gh_run_frame(7, "success"))
    lines = model.lines()
    assert len(lines) == 1  # collapsed, not appended
    assert "success" in lines[0]


def test_model_keeps_distinct_entities_separate_in_order() -> None:
    """Distinct entities each get their own row, in insertion order."""
    model = top_mod._TopModel()
    model.update(_gh_run_frame(1, "success"))
    model.update(_gh_run_frame(2, "failure"))
    lines = model.lines()
    assert len(lines) == 2
    assert "success" in lines[0]
    assert "failure" in lines[1]


def test_model_lines_truncate_to_width() -> None:
    """An over-wide row is truncated to the given width with an ellipsis."""
    model = top_mod._TopModel()
    model.update(_gh_run_frame(1, "success"))
    (line,) = model.lines(width=20)
    assert len(line) == 20
    assert line.endswith("…")


# ---------------------------------------------------------------------------
# Startup guards (no daemon)
# ---------------------------------------------------------------------------


def test_top_bad_timeout_is_error() -> None:
    result = runner.invoke(app, ["top", "--timeout", "nope"])
    assert result.exit_code == 2
    assert "invalid --timeout" in result.stdout + str(result.stderr or "")


def test_top_nonpositive_max_frames_is_error() -> None:
    result = runner.invoke(app, ["top", "--max-frames", "0"])
    assert result.exit_code == 2
    assert "--max-frames must be a positive integer" in result.stdout + str(result.stderr or "")


def test_top_exits_2_when_daemon_absent(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """`waitbus top` with no broadcast daemon reachable is a startup error (exit 2)."""
    monkeypatch.setattr(_broadcast_sub, "broadcast_socket", lambda: tmp_path / "nonexistent.sock")
    result = runner.invoke(app, ["top", "--max-frames", "1"])
    assert result.exit_code == 2


# ---------------------------------------------------------------------------
# End to end against the in-process daemon
# ---------------------------------------------------------------------------


def _emit(db_path: Path, event_type: str, source: str, **fields: object) -> None:
    from waitbus._emit import emit

    if source == "agent" and not is_known_source(source):
        register_source(SourceSpec(name="agent", event_types=("agent_claim", "agent_task_failed")))
    emit(
        EventInsert(
            delivery_id=f"top-test:{event_type}:{time.time_ns()}",
            source=source,
            event_type=event_type,
            owner="acme",
            repo="acme/widgets",
            received_at=time.time_ns(),
            payload_json=json.dumps(fields),
            ingest_method="test",
            **{k: v for k, v in fields.items() if k in ("run_id", "status", "conclusion")},  # type: ignore[arg-type]
        ),
        db_path=db_path,
    )


@pytest.mark.slow
@pytest.mark.asyncio
async def test_top_bounded_run_emits_rows_for_delivered_frames(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A bounded `waitbus top --max-frames N` (piped: append fallback) emits a row per frame.

    The CLI runs in an executor while the daemon serves; CliRunner captures a
    non-TTY stdout, so top takes the append-only path -- one ``_format_row`` line
    per delivered frame -- and exits 0 once N frames have been rendered.
    """
    _daemon, paths = running_daemon
    monkeypatch.setattr(_broadcast_sub, "broadcast_socket", lambda: paths["broadcast"])

    loop = asyncio.get_running_loop()
    invoke = loop.run_in_executor(None, lambda: runner.invoke(app, ["top", "--max-frames", "2", "--timeout", "10s"]))
    await asyncio.sleep(1.0)  # let the subscriber register
    await loop.run_in_executor(
        None, lambda: _emit(paths["db"], "workflow_run", "github", run_id=1, status="completed", conclusion="success")
    )
    await loop.run_in_executor(
        None, lambda: _emit(paths["db"], "workflow_run", "github", run_id=2, status="completed", conclusion="failure")
    )
    result = await asyncio.wait_for(invoke, timeout=10.0)

    assert result.exit_code == 0, f"exit {result.exit_code}\n{result.stdout}"
    rows = [ln for ln in result.stdout.splitlines() if "github/workflow_run" in ln]
    assert len(rows) == 2, f"expected one row per frame, got {rows}"
    # Colour is TTY-gated; captured stdout stays clean.
    assert "\x1b[" not in result.stdout


# ---------------------------------------------------------------------------
# Render path + _run branches (deterministic, no daemon)
# ---------------------------------------------------------------------------


def test_use_colour_false_off_tty(monkeypatch: pytest.MonkeyPatch) -> None:
    """use_colour is False when stdout is not a TTY (the captured-test case)."""
    from waitbus.cli import _shared

    monkeypatch.setattr(sys.stdout, "isatty", lambda: False, raising=False)
    assert _shared.use_colour() is False


def test_render_writes_header_rows_and_clear(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """_render emits the clear-home sequence, a header, and one line per row."""
    monkeypatch.setattr(top_mod, "use_colour", lambda: True)
    model = top_mod._TopModel()
    model.update(_gh_run_frame(1, "success"))
    model.update(_gh_run_frame(2, "failure"))
    top_mod._render(model, count=2)
    out = capsys.readouterr().out
    assert top_mod._CLEAR_HOME in out
    assert "waitbus top" in out
    assert "2 entities" in out
    assert "\x1b[" in out  # colour on
    assert out.count("github/workflow_run") == 2


def test_render_no_colour_omits_ansi_styling(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """With colour off, _render still clears+draws but emits no styling escape."""
    monkeypatch.setattr(top_mod, "use_colour", lambda: False)
    model = top_mod._TopModel()
    model.update(_gh_run_frame(1, "success"))
    top_mod._render(model, count=1)
    out = capsys.readouterr().out
    assert top_mod._CLEAR_HOME in out  # still clears + redraws
    assert "1 entity" in out  # singular
    assert "\x1b[1m" not in out  # no bold-style escape when colour is off


class _FakeTtyStdout:
    """A stdout stand-in that reports as a TTY and records writes."""

    def __init__(self) -> None:
        self.buffer: list[str] = []

    def isatty(self) -> bool:
        return True

    def write(self, s: str) -> int:
        self.buffer.append(s)
        return len(s)

    def flush(self) -> None:
        pass


def _patch_subscriber(
    monkeypatch: pytest.MonkeyPatch, outcome: object, frames: list[dict[str, object]]
) -> socket.socket:
    """Patch top's subscribe/ack/engine so _run drives ``frames`` then returns ``outcome``.

    Returns the server end of the socketpair; the caller closes it in a finally.
    """
    from waitbus._broadcast_sub import SubscriberHandle

    server, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    monkeypatch.setattr(top_mod, "open_subscriber", lambda **_k: SubscriberHandle(sock=client))
    monkeypatch.setattr(top_mod, "read_subscribe_ack", lambda _s: None)

    def _fake_await(sub: object, *, decide: object, deadline_seconds: object, idle_reset: object) -> object:
        for fr in frames:
            decide(fr)  # type: ignore[operator]
        return outcome

    monkeypatch.setattr(top_mod, "await_predicate", _fake_await)
    return server


def test_run_interactive_renders_and_restores_screen(monkeypatch: pytest.MonkeyPatch) -> None:
    """On a TTY, _run enters/leaves the alt-screen and renders each frame in place."""
    from waitbus._broadcast_sub import WaitOutcome

    fake = _FakeTtyStdout()
    monkeypatch.setattr(sys, "stdout", fake)
    matched = WaitOutcome(matched=True, timed_out=False, cancelled=False, peer_closed=False, framing_error=False)
    server = _patch_subscriber(monkeypatch, matched, [_gh_run_frame(1, "success"), _gh_run_frame(1, "failure")])
    try:
        rc = top_mod._run(deadline_seconds=None, max_frames=2)
    finally:
        server.close()

    joined = "".join(fake.buffer)
    assert rc == 0
    assert top_mod._ALT_SCREEN_ENTER in joined  # entered alt screen
    assert top_mod._ALT_SCREEN_LEAVE in joined  # restored on exit
    assert top_mod._CLEAR_HOME in joined  # redrew in place
    # Same run id collapsed to one row whose latest state is the failure.
    assert "failure" in joined


def test_run_returns_framing_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """A wire-framing error from the engine returns exit 1."""
    from waitbus._broadcast_sub import WaitOutcome

    fake = _FakeTtyStdout()
    monkeypatch.setattr(sys, "stdout", fake)
    framing = WaitOutcome(matched=False, timed_out=False, cancelled=False, peer_closed=True, framing_error=True)
    server = _patch_subscriber(monkeypatch, framing, [])
    try:
        assert top_mod._run(deadline_seconds=None, max_frames=None) == 1
    finally:
        server.close()


def test_top_exits_2_when_credentials_misconfigured(monkeypatch: pytest.MonkeyPatch) -> None:
    """A SecretNotConfigured from open_subscriber is a startup error (exit 2)."""
    from waitbus._secrets import SecretNotConfigured

    def _raise(**_k: object) -> None:
        raise SecretNotConfigured("token backend not configured")

    monkeypatch.setattr(top_mod, "open_subscriber", _raise)
    result = runner.invoke(app, ["top", "--max-frames", "1"])
    assert result.exit_code == 2


def test_top_exits_2_when_ack_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    """A subscribe rejection at the ack barrier surfaces as a startup error (exit 2)."""
    import socket

    from waitbus._broadcast_sub import BroadcastConnectionError, SubscriberHandle

    server, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    monkeypatch.setattr(top_mod, "open_subscriber", lambda **_k: SubscriberHandle(sock=client))

    def _reject(_s: object) -> None:
        raise BroadcastConnectionError("rejected", remediation="check token")

    monkeypatch.setattr(top_mod, "read_subscribe_ack", _reject)
    try:
        result = runner.invoke(app, ["top", "--max-frames", "1"])
        assert result.exit_code == 2
    finally:
        server.close()
