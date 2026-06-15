"""Smoke test for ``waitbus swarm-demo``.

End-to-end: invoke the swarm-demo subcommand via typer's CliRunner, assert exit
code 0 within a generous timeout, and verify that BOTH coordination beats
matched on the real ``await_predicate`` engine (not a manufactured outcome). The
temporary state directory is owned by the command and cleaned up on exit; no
project-wide state is mutated.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from typer.testing import CliRunner

from waitbus.cli import swarm_demo as swarm_demo_mod
from waitbus.cli.main import app
from waitbus.sources._registry import _clear_for_test_isolation, is_known_source


@pytest.fixture(autouse=True)
def _isolate_registry() -> Iterator[None]:
    """Clear the process-singleton source registry around each test so the
    demo-scoped ``agent`` source never leaks between tests."""
    _clear_for_test_isolation()
    yield
    _clear_for_test_isolation()


@pytest.mark.slow
def test_swarm_demo_both_beats_match() -> None:
    """The end-to-end happy path: ``waitbus swarm-demo`` exits 0 and both beats
    deliver their synthesized agent event to a real waiter.

    The command allocates its own temporary XDG dirs internally and registers a
    demo-scoped ``agent`` source in-process, so this runs cleanly whether or not
    waitbus is installed on the workstation, without touching real state. The
    wall-clock budget inside the command is 60 s; we cap pytest at 90 s.
    """
    runner = CliRunner()
    result = runner.invoke(app, ["swarm-demo"], catch_exceptions=False)
    assert result.exit_code == 0, f"swarm-demo exited {result.exit_code}\nstdout:\n{result.stdout}"

    # Each beat renders exactly one [event] line, and only on a genuine
    # predicate match (the waiter is parked on the real await_predicate engine
    # fed the real --match string the banner shows). Two beats -> two events.
    event_lines = [line for line in result.stdout.splitlines() if line.startswith("[event] ")]
    assert len(event_lines) == 2, f"expected 2 [event] lines, got {len(event_lines)}\nstdout:\n{result.stdout}"

    joined = "\n".join(event_lines)
    assert "agent_claim" in joined  # beat 1: conflict avoidance
    assert "agent_task_failed" in joined  # beat 2: failure fan-out (load-bearing)
    # The failure payload (the traceback) rides through to the fixer.
    assert "AssertionError: parser.py:42" in joined

    # The narrated wait command shows the real waitbus wait shape with a real
    # --match predicate (fidelity: the displayed command IS the wait that ran).
    assert "waitbus wait --source agent --match" in result.stdout
    # The agents and events are synthesized in-process and the banner says so.
    assert "SYNTHESIZED in-process" in result.stdout
    # Coordination is same-machine, same-user; the demo must not imply isolation.
    assert "same machine, same user" in result.stdout.lower()
    # The closing onboarding hint leads with the one-command path.
    assert "uvx waitbus swarm-demo" in result.stdout
    # Colour is TTY-gated: a captured (non-TTY) stdout stays free of ANSI escapes.
    assert "\x1b[" not in result.stdout


# ---------------------------------------------------------------------------
# Unit coverage for the helpers the E2E path does not exercise on a non-TTY
# ---------------------------------------------------------------------------


def test_echo_colourizes_each_role_on_tty(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """When colour is enabled, _echo wraps each role line in ANSI escapes.

    The E2E test runs on a non-TTY (no escapes); this covers the styling branch
    by forcing use_colour True, for the three role prefixes.
    """
    monkeypatch.setattr(swarm_demo_mod, "use_colour", lambda: True)
    swarm_demo_mod._echo("[event] agent_claim agent-1 -> src/parser.py")
    swarm_demo_mod._echo("[agent-2] backing off")
    swarm_demo_mod._echo("[swarm-demo] one bus, zero polls")
    swarm_demo_mod._echo("")  # a prefix-less spacer line stays unstyled even with colour on
    lines = capsys.readouterr().out.splitlines()
    # Each role line is wrapped in ANSI escapes; the prefix-less spacer is not.
    assert all("\x1b[" in lines[i] for i in (0, 1, 2)), lines
    assert lines[3] == ""


def test_agent_source_is_a_first_class_builtin() -> None:
    """The swarm-demo emits against the first-class built-in ``agent`` source, so it
    needs no in-process registration step (the source is always known)."""
    assert is_known_source(swarm_demo_mod._AGENT_SOURCE)


def test_swarm_demo_does_not_mutate_path_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """The swarm-demo injects every daemon path explicitly and never touches the
    process-global path env vars.

    Stronger than the old restore test: the command binds its daemon to temp dirs
    by passing db / socket / doorbell paths directly to Broadcast and emit, so a
    pre-existing operator value is left byte-identical (no save/mutate/restore at
    all) and an in-process caller cannot be contaminated.
    """
    import os

    monkeypatch.setenv("WAITBUS_STATE_DIR", "/sentinel/prior/state")
    monkeypatch.setenv("WAITBUS_RUNTIME_DIR", "/sentinel/prior/runtime")
    runner = CliRunner()
    result = runner.invoke(app, ["swarm-demo"], catch_exceptions=False)
    assert result.exit_code == 0
    assert os.environ["WAITBUS_STATE_DIR"] == "/sentinel/prior/state"
    assert os.environ["WAITBUS_RUNTIME_DIR"] == "/sentinel/prior/runtime"
