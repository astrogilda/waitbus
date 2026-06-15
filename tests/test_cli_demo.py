"""Smoke test for ``waitbus demo``.

End-to-end: invoke the demo subcommand via typer's CliRunner, assert
exit code 0 within a generous timeout, and verify the stdout carries
one ``[event] ...`` line per built-in source (github, pytest, docker,
fs). The temporary state directory is owned by the command and cleaned
up on exit; no project-wide state is mutated.
"""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from waitbus.cli.main import app


@pytest.mark.slow
def test_demo_emits_one_event_per_source() -> None:
    """The end-to-end happy path: ``waitbus demo`` exits 0 and the
    subscriber tap renders one ``[event]`` line per built-in source.

    The demo allocates its own temporary XDG dirs internally and
    redirects the broadcast / doorbell path resolution to them, so this
    test runs cleanly against a workstation that has waitbus already
    installed (or not — the test does not touch the operator's real
    state). The wall-clock budget inside the demo is 60 s; we cap
    pytest at 90 s.
    """
    runner = CliRunner()
    result = runner.invoke(app, ["demo"], catch_exceptions=False)
    assert result.exit_code == 0, f"demo exited {result.exit_code}\nstdout:\n{result.stdout}"
    # One line per source. Banners are emitted to stdout via plain
    # print() calls; subscriber-rendered lines start with `[event] `.
    event_lines = [line for line in result.stdout.splitlines() if line.startswith("[event] ")]
    assert len(event_lines) == 4, f"expected 4 [event] lines, got {len(event_lines)}\nstdout:\n{result.stdout}"

    joined = "\n".join(event_lines)
    assert "github workflow_run" in joined
    assert "pytest_session" in joined
    assert "docker_container success container=demo-worker" in joined
    assert "fs_change modified demo.txt" in joined
    # The value-prop phase: the wait returns on the event, and the demo
    # makes the avoided polling cost concrete (gh run watch's 3s default).
    assert "waitbus wait returned the instant" in result.stdout
    assert "zero polls" in result.stdout
    # The displayed wait command must use only real flags (copy-paste-real);
    # --event-type is an emit flag, never a wait flag, and must not appear.
    assert "--event-type" not in result.stdout
    # Closing banner is a load-bearing user-onboarding hint; the demo
    # leads with the one-command uvx path.
    assert "uvx waitbus demo" in result.stdout
    # Colour is TTY-gated: a captured (non-TTY) stdout must stay free of
    # ANSI escapes so logs, pipes, and assertions see clean text.
    assert "\x1b[" not in result.stdout
