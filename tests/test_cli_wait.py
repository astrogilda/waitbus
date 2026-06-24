"""Tests for the ``waitbus wait`` typer command wrapper.

Covers the startup guards (no predicate, malformed --timeout, short
--sha prefix, unreachable daemon) and the match / timeout paths end to
end against the in-process broadcast daemon (the blessed
``running_daemon`` fixture). The blocking ``waitbus wait`` CLI and the
matching ``waitbus emit`` run in a thread executor while the daemon
serves, mirroring the ``waitbus on`` end-to-end tests. Linux-only: the
broadcast daemon's SO_PEERCRED check is Linux-only.
"""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

import pytest
from typer.testing import CliRunner, Result

from tests._daemon_helpers import await_subscribers
from waitbus import _broadcast_sub, broadcast
from waitbus.cli.main import app

pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="broadcast daemon SO_PEERCRED check is Linux-only",
)

runner = CliRunner()


def _all_output(result: Result) -> str:
    """stdout + stderr regardless of the CliRunner mixing mode."""
    return result.stdout + str(result.stderr or "")


# ---------------------------------------------------------------------------
# Startup guards (no daemon needed)
# ---------------------------------------------------------------------------


def test_wait_requires_a_predicate() -> None:
    """`waitbus wait` with no predicate input is a startup error (exit 2)."""
    result = runner.invoke(app, ["wait"])
    assert result.exit_code == 2
    assert "requires at least one of" in _all_output(result)


def test_wait_bad_timeout_is_error() -> None:
    """A malformed --timeout is a startup error (exit 2)."""
    result = runner.invoke(
        app,
        ["wait", "--source", "pytest", "--match", 'fields.event_type="pytest_session"', "--timeout", "nope"],
    )
    assert result.exit_code == 2
    assert "invalid --timeout" in _all_output(result)


def test_wait_short_sha_prefix_is_error() -> None:
    """A --sha shorter than the 7-hex-char git abbreviation floor exits 2."""
    result = runner.invoke(app, ["wait", "--sha", "abc12"])
    assert result.exit_code == 2


def test_wait_exits_2_when_daemon_absent(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """`waitbus wait` with no broadcast daemon reachable is a startup error (exit 2)."""
    monkeypatch.setattr(_broadcast_sub, "broadcast_socket", lambda: tmp_path / "nonexistent.sock")
    result = runner.invoke(
        app,
        ["wait", "--source", "pytest", "--match", 'fields.event_type="pytest_session"', "--timeout", "2s"],
    )
    assert result.exit_code == 2
    assert "broadcast" in _all_output(result).lower()


# ---------------------------------------------------------------------------
# End to end against the in-process daemon (running_daemon fixture)
# ---------------------------------------------------------------------------


def _cli_emit_agent_event(db_path: Path) -> None:
    """Emit one matching agent event through the CLI emit command.

    Uses its own CliRunner: instances are not documented thread-safe, so
    the executor thread must not share the module-level one while the
    blocking ``wait`` invocation is in flight.
    """
    result = CliRunner().invoke(
        app,
        [
            "emit",
            "--delivery-id",
            f"wait-test:{time.time_ns()}",
            "--source",
            "agent",
            "--event-type",
            "agent_task_failed",
            "--owner",
            "local",
            "--repo",
            "swarm",
            "--received-at",
            str(time.time_ns()),
            "--payload-json",
            '{"agent": "a1", "error": "x"}',
            "--ingest-method",
            "manual",
            "--db",
            str(db_path),
        ],
    )
    assert result.exit_code == 0, result.stdout + str(result.stderr or "")


@pytest.mark.slow
@pytest.mark.asyncio
async def test_wait_exits_0_on_matching_event(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A matching event emitted through the CLI releases the wait with exit 0.

    The daemon serves in the test loop; the blocking ``waitbus wait`` and the
    CLI emit run in a thread executor. The CLI's default subscriber socket is
    redirected to the daemon-under-test's socket.
    """
    daemon, paths = running_daemon
    monkeypatch.setattr(_broadcast_sub, "broadcast_socket", lambda: paths["broadcast"])

    loop = asyncio.get_running_loop()
    invoke = loop.run_in_executor(
        None,
        lambda: runner.invoke(
            app,
            [
                "wait",
                "--source",
                "agent",
                "--match",
                'fields.event_type="agent_task_failed"',
                "--timeout",
                "10s",
            ],
        ),
    )
    # Wait for the subscriber to register deterministically (the ack barrier
    # lives inside the executor thread) rather than a fixed wall-clock sleep.
    await await_subscribers(daemon, added=1)
    await loop.run_in_executor(None, lambda: _cli_emit_agent_event(paths["db"]))
    result = await asyncio.wait_for(invoke, timeout=10.0)

    assert result.exit_code == 0, f"expected 0 (match), got {result.exit_code}\n{_all_output(result)}"


@pytest.mark.slow
@pytest.mark.asyncio
async def test_wait_times_out_with_no_match(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No matching event before --timeout exits 124."""
    _daemon, paths = running_daemon
    monkeypatch.setattr(_broadcast_sub, "broadcast_socket", lambda: paths["broadcast"])
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(
        None,
        lambda: runner.invoke(
            app,
            [
                "wait",
                "--source",
                "agent",
                "--match",
                'fields.event_type="agent_task_failed"',
                "--timeout",
                "1s",
            ],
        ),
    )
    assert result.exit_code == 124
