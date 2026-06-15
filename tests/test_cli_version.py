"""Tests for --version flag across the sub-command tree."""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from waitbus.cli import app

runner = CliRunner()

_VERSION_PREFIX = "waitbus"


def _check_version(args: list[str]) -> None:
    """Assert --version at args prints 'waitbus <version>' and exits 0."""
    try:
        from importlib.metadata import version as pkg_version

        expected_version = pkg_version("waitbus")
    except Exception:
        pytest.skip("waitbus package metadata unavailable; run `uv sync` first")

    result = runner.invoke(app, args)
    assert result.exit_code == 0, f"--version at {args} returned exit {result.exit_code}: {result.output!r}"
    assert _VERSION_PREFIX in result.output, f"Expected '{_VERSION_PREFIX}' in output for {args}: {result.output!r}"
    assert expected_version in result.output, (
        f"Expected version {expected_version!r} in output for {args}: {result.output!r}"
    )


# ---------------------------------------------------------------------------
# root
# ---------------------------------------------------------------------------


def test_root_version_flag() -> None:
    _check_version(["--version"])


def test_root_version_short_flag() -> None:
    """Root also accepts -V."""
    try:
        from importlib.metadata import version as pkg_version

        expected_version = pkg_version("waitbus")
    except Exception:
        pytest.skip("waitbus package metadata unavailable")

    result = runner.invoke(app, ["-V"])
    assert result.exit_code == 0
    assert expected_version in result.output


# ---------------------------------------------------------------------------
# daemon sub-apps
# ---------------------------------------------------------------------------


def test_listener_version_flag() -> None:
    _check_version(["listener", "--version"])


def test_broadcast_version_flag() -> None:
    _check_version(["broadcast", "--version"])


def test_etag_poll_version_flag() -> None:
    _check_version(["etag-poll", "--version"])


def test_mcp_version_flag() -> None:
    _check_version(["mcp", "--version"])


def test_read_events_version_flag() -> None:
    _check_version(["read-events", "--version"])


def test_pr_monitor_version_flag() -> None:
    _check_version(["pr-monitor", "--version"])


def test_watchdog_check_version_flag() -> None:
    _check_version(["watchdog-check", "--version"])
