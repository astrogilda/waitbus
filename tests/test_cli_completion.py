"""Shell-completion smoke tests for the waitbus top-level CLI.

Verifies that every registered sub-command appears in shell completion output.
Typer auto-generates bash / zsh / fish completion infrastructure for every
registered sub-command of an app whose root has ``add_completion=True``
(see ``waitbus/cli/main.py``). The ``source`` and ``allowlist``
sub-apps themselves carry ``add_completion=False`` because completion is
emitted at the root only -- this test exercises the runtime completion
path and asserts both sub-apps' names are enumerated.

Completion architecture
-----------------------
``waitbus --show-completion <shell>`` emits a STATIC stub script (shell-
specific syntax) that calls ``waitbus`` at completion-time with the
``_TYPER_COMPLETE_ARGS`` / ``_WAITBUS_COMPLETE`` env-var protocol. The
verb names are resolved dynamically by that re-entry: the stub never
embeds them. So the smoke contract has two halves:

1. ``--show-completion`` succeeds and emits a non-empty stub mentioning
   ``waitbus`` (the program name to wire up).
2. Running waitbus with the click-completion env-var protocol enumerates
   the registered sub-commands -- including ``source`` and ``allowlist``.
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from typer.testing import CliRunner

from waitbus import cli

_WAITBUS_BIN = str(Path(sys.executable).parent / "waitbus")


# Env vars that click/typer's completion machinery reads at runtime. Once any
# of them is set, click flips into "completion mode" and parts of its internal
# state are not unconditionally reset by ``CliRunner.invoke``, so a prior test
# that exercised runtime completion can leak into a later test that exercises
# the static stub. Clear them before every test for ordering-independence.
_COMPLETION_ENV_VARS = (
    "_WAITBUS_COMPLETE",
    "_TYPER_COMPLETE_ARGS",
    "_TYPER_COMPLETE_TEST_DISABLE_SHELL_DETECTION",
    "COMP_WORDS",
    "COMP_CWORD",
)


def _run_waitbus_subprocess(
    args: list[str],
    shell: str | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run ``waitbus <args>`` in a subprocess with a sanitised env.

    The CliRunner shares typer/click module state across invocations --
    when an earlier test triggers click's completion mode (via the
    ``_WAITBUS_COMPLETE`` env-var protocol), the in-process state lingers
    in a way that breaks a subsequent ``--show-completion`` call. A
    subprocess gets fresh module state every time, which is the only
    way to fully isolate from sibling test pollution.

    By default typer's ``--show-completion`` is a NO-VALUE flag whose
    dialect is auto-detected by shellingham's /proc parent-process walk
    -- NOT ``$SHELL`` -- so it inherits whatever shell launched pytest
    (an ``sh`` ancestor makes it exit 1 with "Shell sh not supported").
    To pin the dialect deterministically, set ``shell=<name>``: the
    wrapper sets ``_TYPER_COMPLETE_TEST_DISABLE_SHELL_DETECTION`` (which
    turns ``--show-completion`` into a value-taking option in the child
    process) and passes the shell name as that value.
    """
    env = {k: v for k, v in os.environ.items() if k not in _COMPLETION_ENV_VARS}
    if shell is not None:
        env["_TYPER_COMPLETE_TEST_DISABLE_SHELL_DETECTION"] = "1"
        flag_at = args.index("--show-completion")
        args = [*args[: flag_at + 1], shell, *args[flag_at + 1 :]]
    return subprocess.run(
        [_WAITBUS_BIN, *args],
        capture_output=True,
        text=True,
        check=False,
        env=env,
        timeout=30.0,
    )


@pytest.fixture(autouse=True)
def _clear_completion_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Drop completion-mode env vars before every test in this module."""
    for name in _COMPLETION_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    yield


@pytest.mark.skipif(
    sys.platform == "darwin",
    reason="typer --show-completion exits non-zero under the macOS runner shell setup; "
    "root cause unconfirmed. Completion is exercised on Linux.",
)
@pytest.mark.parametrize("shell", ["bash", "zsh", "fish"])
def test_show_completion_emits_stub(shell: str) -> None:
    """``waitbus --show-completion`` pinned to each dialect emits a stub.

    The wrapper pins the dialect via typer's detection-disable env var
    (auto-detection walks the parent-process tree and would otherwise
    pick up the ambient pytest-launcher shell). The stub is a
    shell-grammar wrapper that re-invokes ``waitbus`` at
    completion-time; verb names appear only at the runtime-invocation
    layer (see the runtime test below).
    """
    result = _run_waitbus_subprocess(["--show-completion"], shell=shell)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "waitbus" in result.stdout, (
        f"shell={shell}: completion stub must wire up the waitbus binary; got: {result.stdout[:200]!r}"
    )


@pytest.mark.skipif(
    sys.platform == "darwin",
    reason="typer --show-completion exits non-zero under the macOS runner shell setup; "
    "root cause unconfirmed. Completion is exercised on Linux.",
)
def test_show_completion_default_shell_succeeds() -> None:
    """``waitbus --show-completion`` succeeds with a pinned dialect.

    Pins a deterministic shell via the wrapper's ``shell=`` argument so
    the test does not depend on the ambient process tree (pytest may be
    launched from a shell typer does not recognise, e.g. plain ``sh``).
    """
    result = _run_waitbus_subprocess(["--show-completion"], shell="bash")
    assert result.returncode == 0, result.stdout + result.stderr


# Runtime enumeration is asserted for bash + zsh only. The fish completion
# handler in click uses a different env protocol (descriptive-completion
# tokens emitted only when fish itself supplies command-line state via its
# own machinery) and cannot be exercised through CliRunner alone. The
# stub-emission test above already covers the fish-stub path; the
# enumeration contract is identical at the click layer for all three
# shells (same registered sub-commands), so bash + zsh coverage is
# sufficient to detect the regression we care about (a sub-app silently
# dropped from the root typer app).
@pytest.mark.parametrize("shell", ["bash", "zsh"])
def test_runtime_completion_enumerates_source_and_allowlist(shell: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Runtime completion (waitbus + click _WAITBUS_COMPLETE env protocol) lists sub-apps.

    Click's runtime completion (which typer wraps) is triggered by the
    ``_WAITBUS_COMPLETE=complete_<shell>`` env var with the partial command
    line passed via the ``_TYPER_COMPLETE_ARGS`` (typer) /
    ``COMP_WORDS`` + ``COMP_CWORD`` (bash) shape. We invoke the app at
    the top level (empty partial) and assert that both top-level
    sub-apps appear in the enumeration.
    """
    monkeypatch.setenv("_WAITBUS_COMPLETE", f"complete_{shell}")
    monkeypatch.setenv("_TYPER_COMPLETE_ARGS", "waitbus ")
    monkeypatch.setenv("COMP_WORDS", "waitbus ")
    monkeypatch.setenv("COMP_CWORD", "1")

    runner = CliRunner()
    result = runner.invoke(cli.app, [])

    for token in ("source", "allowlist"):
        assert token in result.output, (
            f"shell={shell}: expected sub-app name {token!r} in runtime completion enumeration; "
            f"exit_code={result.exit_code}, output(first 400)={result.output[:400]!r}"
        )
