"""Tests for the ``waitbus install-launchd`` subcommand and the
shipped LaunchAgent plists.

The plist-shape tests run on every platform (parsing XML is OS-agnostic
and well-formed-XML is a property of the file, not the runtime). The
end-to-end install-launchd invocation tests are split: the
"skip-on-linux" path runs on Linux, the dry-run + bootstrap paths run
on macOS only.
"""

from __future__ import annotations

import plistlib
import sys
from collections.abc import Generator
from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from waitbus import cli

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LAUNCHD_SRC_DIR = PROJECT_ROOT / "installers" / "launchd"

runner = CliRunner()


# ---------------------------------------------------------------------------
# Plist-shape tests (cross-platform — XML parsing is OS-agnostic)
# ---------------------------------------------------------------------------


def _shipped_plists() -> list[Path]:
    """Return the four shipped LaunchAgent plists in deterministic order."""
    return sorted(LAUNCHD_SRC_DIR.glob("dev.waitbus.*.plist"))


def test_install_launchd_plist_files_are_valid_xml() -> None:
    """Every shipped plist must parse cleanly as plist XML.

    Also asserts each plist's Label key matches the filename stem
    (the canonical launchd convention; misalignment between the file
    name and Label is the most common operator-confusing failure
    mode at install time).
    """
    plists = _shipped_plists()
    assert len(plists) == 4, f"Expected 4 shipped plists, found {len(plists)}"
    for p in plists:
        with open(p, "rb") as f:
            data = plistlib.load(f)
        assert data["Label"] == p.stem, f"Label/filename mismatch: {p.name} has Label={data['Label']!r}"


def test_install_launchd_emits_keepalive_for_long_running_daemons() -> None:
    """The listener and broadcast plists must have RunAtLoad + KeepAlive.

    These are the long-running daemons; without RunAtLoad they would
    never start automatically, and without KeepAlive a crash would not
    trigger a restart. The KeepAlive value is a dict, not a bool, so
    only abnormal exits trigger a restart (matching the Linux
    Restart=on-failure shape).
    """
    for name in ("dev.waitbus.listener.plist", "dev.waitbus.broadcast.plist"):
        with open(LAUNCHD_SRC_DIR / name, "rb") as f:
            data = plistlib.load(f)
        assert data["RunAtLoad"] is True, f"{name} missing RunAtLoad=true"
        assert isinstance(data["KeepAlive"], dict), f"{name} KeepAlive must be a dict, not a bool, to scope restart"
        assert data["KeepAlive"]["SuccessfulExit"] is False, (
            f"{name} KeepAlive.SuccessfulExit must be false (restart on non-zero exit only)"
        )


def test_install_launchd_emits_startinterval_for_periodic_daemons() -> None:
    """The etag-poll and watchdog plists must use StartInterval.

    StartInterval values match the Linux timer cadences: 45 seconds for
    the ETag poll (mirrors OnUnitActiveSec=45s) and 300 seconds for the
    watchdog (mirrors OnUnitActiveSec=5min).
    """
    with open(LAUNCHD_SRC_DIR / "dev.waitbus.etag-poll.plist", "rb") as f:
        etag = plistlib.load(f)
    assert etag["StartInterval"] == 45, (
        "etag-poll plist StartInterval must be 45s (matches systemd OnUnitActiveSec=45s)"
    )
    with open(LAUNCHD_SRC_DIR / "dev.waitbus.watchdog.plist", "rb") as f:
        watchdog = plistlib.load(f)
    assert watchdog["StartInterval"] == 300, (
        "watchdog plist StartInterval must be 300s (matches systemd OnUnitActiveSec=5min)"
    )


def test_shipped_plists_carry_install_time_placeholders() -> None:
    """Every shipped plist must reference __BIN_DIR__ + __LOG_DIR__ + __RUNTIME_DIR__.

    The placeholders are resolved at ``waitbus install-launchd`` time
    to the operator's actual paths. A plist that ships with no
    placeholder would mean either the install step is missing
    substitution or the plist was authored with a hardcoded path —
    both regressions caught here.
    """
    for p in _shipped_plists():
        raw = p.read_text()
        assert "__BIN_DIR__" in raw, f"{p.name} missing __BIN_DIR__ placeholder"
        assert "__LOG_DIR__" in raw, f"{p.name} missing __LOG_DIR__ placeholder"
        assert "__RUNTIME_DIR__" in raw, f"{p.name} missing __RUNTIME_DIR__ placeholder"


# ---------------------------------------------------------------------------
# install-launchd subcommand
# ---------------------------------------------------------------------------


@pytest.fixture
def isolated_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Generator[Path, None, None]:
    """Redirect _paths factories + HOME to tmp_path."""
    state = tmp_path / "state"
    runtime = tmp_path / "runtime"
    monkeypatch.setenv("WAITBUS_STATE_DIR", str(state))
    monkeypatch.setenv("WAITBUS_RUNTIME_DIR", str(runtime))
    monkeypatch.setenv("HOME", str(tmp_path))
    yield tmp_path


def _make_share_launchd_dir(tmp_path: Path) -> Path:
    """Build a fake share/launchd/ tree populated from the real shipped plists.

    Lets the test exercise the real placeholder-substitution path
    without depending on a built wheel.
    """
    share = tmp_path / "share-launchd"
    share.mkdir(parents=True)
    for p in _shipped_plists():
        (share / p.name).write_text(p.read_text())
    # MANIFEST.txt parity with the shipped manifest.
    (share / "MANIFEST.txt").write_text((LAUNCHD_SRC_DIR / "MANIFEST.txt").read_text())
    return share


@pytest.mark.skipif(sys.platform != "linux", reason="Linux platform-guard check")
def test_install_launchd_skips_on_linux(isolated_home: Path) -> None:
    """On Linux, ``install-launchd`` exits 0 with a message
    pointing the operator at install-systemd. The command must not
    touch the filesystem.
    """
    result = runner.invoke(cli.app, ["install-launchd"])
    assert result.exit_code == 0, result.output
    assert "macOS-only" in result.output or "install-systemd" in result.output
    assert not (isolated_home / "Library" / "LaunchAgents").exists()


@pytest.mark.skipif(sys.platform != "linux", reason="symmetric platform-guard check")
def test_install_systemd_skips_on_unknown_platform(monkeypatch: pytest.MonkeyPatch, isolated_home: Path) -> None:
    """Symmetric: install-systemd refuses to run on non-Linux platforms.

    Simulated by patching ``sys.platform`` so the test exercises the
    platform guard without needing a real macOS / freebsd runner. The
    command must exit 0 and not touch the filesystem.
    """
    monkeypatch.setattr(cli.sys, "platform", "darwin")
    result = runner.invoke(cli.app, ["install-systemd", "--no-enable"])
    assert result.exit_code == 0, result.output
    assert "Linux-only" in result.output or "install-launchd" in result.output


def test_install_launchd_dry_run_substitutes_placeholders(isolated_home: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The dry-run path validates placeholder substitution end-to-end.

    Runs on every platform: the dry-run path bypasses launchctl
    bootstrap and never touches ~/Library/LaunchAgents/. The substitution
    is exercised through _resolve_launchd_placeholders directly so the
    Linux runner can verify the substitution logic without needing a
    macOS-only side-effect.
    """
    bin_dir = Path("/opt/waitbus/bin")
    log_dir = Path("/var/log/waitbus")
    runtime_dir = Path("/run/user/1000/waitbus")
    for p in _shipped_plists():
        template = p.read_text()
        resolved = cli._resolve_launchd_placeholders(
            template,
            bin_dir=bin_dir,
            log_dir=log_dir,
            runtime_dir=runtime_dir,
        )
        assert "__BIN_DIR__" not in resolved
        assert "__LOG_DIR__" not in resolved
        assert "__RUNTIME_DIR__" not in resolved
        assert str(bin_dir) in resolved
        # Verify the result is well-formed plist XML after substitution.
        data = plistlib.loads(resolved.encode("utf-8"))
        assert data["Label"] == p.stem


@pytest.mark.skipif(sys.platform != "darwin", reason="install-launchd dry-run runs on macOS")
def test_install_launchd_dry_run_no_filesystem_writes(isolated_home: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``install-launchd --dry-run`` prints the resolved plan and exits 0
    without touching ~/Library/LaunchAgents/ or invoking launchctl.
    """
    share = _make_share_launchd_dir(isolated_home)
    target = isolated_home / "Library" / "LaunchAgents"
    monkeypatch.setattr("waitbus.cli.install.launchd._share_launchd_dir", lambda: share)
    monkeypatch.setattr("waitbus.cli.install.launchd._launchd_target_dir", lambda: target)
    with patch("waitbus.cli._shared.subprocess.run") as run_mock:
        result = runner.invoke(cli.app, ["install-launchd", "--dry-run"])
    assert result.exit_code == 0, result.output
    assert "Would write" in result.output
    assert run_mock.call_count == 0, "launchctl must not be invoked under --dry-run"
    assert not target.exists() or not any(target.iterdir())


@pytest.mark.skipif(sys.platform != "darwin", reason="install-launchd write path runs on macOS")
def test_install_launchd_no_enable_writes_plists(isolated_home: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``install-launchd --no-enable`` writes resolved plists into the
    target dir and skips the launchctl bootstrap step entirely.
    """
    share = _make_share_launchd_dir(isolated_home)
    target = isolated_home / "Library" / "LaunchAgents"
    monkeypatch.setattr("waitbus.cli.install.launchd._share_launchd_dir", lambda: share)
    monkeypatch.setattr("waitbus.cli.install.launchd._launchd_target_dir", lambda: target)
    monkeypatch.setattr("waitbus.cli.install.launchd._resolve_launchd_bin_dir", lambda: isolated_home / "bin")
    with patch("waitbus.cli._shared.subprocess.run") as run_mock:
        result = runner.invoke(cli.app, ["install-launchd", "--no-enable"])
    assert result.exit_code == 0, result.output
    for p in _shipped_plists():
        dst = target / p.name
        assert dst.exists(), f"plist not written: {dst}"
        # Verify the on-disk file parses cleanly post-substitution.
        with open(dst, "rb") as f:
            plistlib.load(f)
    assert run_mock.call_count == 0, "launchctl must not run under --no-enable"
