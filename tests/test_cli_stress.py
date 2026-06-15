"""Tests for the ``waitbus stress`` typer command wrapper.

Exercises the real root-app argv surface through CliRunner: argv
pass-through to the stress controller, exit-code propagation for both
verdicts, the actionable message when the stress extra is missing, and
the re-raise of unrelated import failures. The controller itself is
covered by the stress-harness suites; these tests pin the wrapper's
invocation glue so the module can sit under the per-file coverage gate.
"""

from __future__ import annotations

import builtins
from typing import Any

import pytest
from typer.testing import CliRunner

from waitbus.cli.main import app

runner = CliRunner()


@pytest.fixture
def controller_calls(monkeypatch: pytest.MonkeyPatch) -> list[list[str]]:
    """Replace the stress controller's main with a recorder returning 0."""
    calls: list[list[str]] = []

    def _fake_main(args: list[str]) -> int:
        calls.append(args)
        return 0

    import scripts.stress._controller as controller

    monkeypatch.setattr(controller, "main", _fake_main)
    return calls


def test_stress_passes_extra_args_to_the_controller(controller_calls: list[list[str]]) -> None:
    result = runner.invoke(app, ["stress", "--sweep", "1,2", "--duration", "5s"])
    assert result.exit_code == 0, result.output
    assert controller_calls == [["--sweep", "1,2", "--duration", "5s"]]


def test_stress_propagates_controller_failure_exit_code(monkeypatch: pytest.MonkeyPatch) -> None:
    import scripts.stress._controller as controller

    monkeypatch.setattr(controller, "main", lambda args: 1)
    result = runner.invoke(app, ["stress"])
    assert result.exit_code == 1


def test_stress_missing_scipy_yields_install_hint(monkeypatch: pytest.MonkeyPatch) -> None:
    """An ImportError naming scipy becomes the install-the-extra message."""
    real_import = builtins.__import__

    def _no_scipy(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "scripts.stress._controller":
            raise ImportError("No module named 'scipy'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _no_scipy)
    result = runner.invoke(app, ["stress"])
    assert result.exit_code == 2
    assert "waitbus[stress]" in result.output


def test_stress_unrelated_import_error_is_reraised(monkeypatch: pytest.MonkeyPatch) -> None:
    """An import failure NOT caused by the missing extra must not be masked."""
    real_import = builtins.__import__

    def _broken_controller(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "scripts.stress._controller":
            raise ImportError("No module named 'made_up_dependency'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _broken_controller)
    result = runner.invoke(app, ["stress"])
    assert result.exit_code != 0
    assert isinstance(result.exception, ImportError)
    assert "made_up_dependency" in str(result.exception)
