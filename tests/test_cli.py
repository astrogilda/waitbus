"""Unit tests for the waitbus CLI surface (`waitbus.cli`).

Uses typer's CliRunner. Each test isolates HOME to a tmp_path so the
operator's real state directories, config files, and credential store
are never touched. ``install-credentials`` is tested by mocking
``shutil.which`` and ``subprocess.run`` so no actual systemd-creds
invocation occurs.
"""

from __future__ import annotations

import importlib
import os
import shutil
import sys
from collections.abc import Generator
from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from waitbus import cli

# mix_stderr=False would split stdout/stderr, but typer's CliRunner default
# already merges them so we can match against `result.stdout` for either
# stream. (Some click versions removed the kwarg; rely on the default.)
runner = CliRunner()


# --- fixtures --------------------------------------------------------------


@pytest.fixture
def isolated_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Generator[Path, None, None]:
    """Redirect all _paths factories to tmp_path via WAITBUS_*_DIR env overrides.

    The CLI calls db_path(), watched_repos(), etag_state(), etc. at runtime,
    and ``_paths`` factories re-read env on every call, so overriding the env
    vars is sufficient for full path isolation without monkeypatching
    module attributes.
    """
    state = tmp_path / ".local" / "state" / "waitbus"
    runtime = tmp_path / "run" / "waitbus"
    monkeypatch.setenv("WAITBUS_STATE_DIR", str(state))
    monkeypatch.setenv("WAITBUS_RUNTIME_DIR", str(runtime))
    yield tmp_path


# --- init ------------------------------------------------------------------


def test_version_flag_prints_package_version() -> None:
    """`waitbus --version` (and `-V`) prints the installed version string and exits 0.

    Skips cleanly if the package metadata isn't available (e.g., the
    contributor cloned the repo and ran `pytest` without first running
    `uv sync` / `pip install -e .`). The CLI itself handles this case
    by printing a fallback message, but the test only asserts the
    full-version path where metadata IS resolvable.
    """
    from importlib.metadata import PackageNotFoundError
    from importlib.metadata import version as pkg_version

    try:
        expected = pkg_version("waitbus")
    except PackageNotFoundError:
        pytest.skip("waitbus package metadata unavailable; run `uv sync` first")
    for flag in ("--version", "-V"):
        result = runner.invoke(cli.app, [flag])
        assert result.exit_code == 0, f"{flag} returned exit code {result.exit_code}: {result.stdout}"
        assert expected in result.stdout, f"{flag} did not include version {expected}: {result.stdout!r}"


def test_init_creates_state_dirs_and_scaffolds(isolated_home: Path) -> None:
    result = runner.invoke(cli.app, ["init"])
    assert result.exit_code == 0, result.stdout
    events = isolated_home / ".local" / "state" / "waitbus"
    assert events.is_dir()
    assert (events / "cursors").is_dir()
    assert (events / "watched_repos.txt").exists()
    assert (events / "etag_state.json").exists()
    assert (events / "github.db").exists()


def test_init_is_idempotent(isolated_home: Path) -> None:
    runner.invoke(cli.app, ["init"])
    result = runner.invoke(cli.app, ["init"])
    assert result.exit_code == 0
    assert "already present" in result.stdout


def test_init_dry_run_does_not_mutate_filesystem(isolated_home: Path) -> None:
    result = runner.invoke(cli.app, ["init", "--dry-run"])
    assert result.exit_code == 0
    events = isolated_home / ".local" / "state" / "waitbus"
    assert not events.exists(), "dry-run must not create state dirs"
    assert "Would create" in result.stdout


# --- install-credentials ---------------------------------------------------


def test_install_credentials_dry_run_prints_command_and_skips_invocation(
    isolated_home: Path,
) -> None:
    with (
        patch("waitbus.cli.shutil.which", return_value="/usr/bin/systemd-creds"),
        patch("waitbus.cli.subprocess.run") as run_mock,
    ):
        result = runner.invoke(
            cli.app,
            ["install-credentials", "github-webhook-secret", "--value", "abc123", "--dry-run"],
        )
    assert result.exit_code == 0, result.stdout
    assert "systemd-creds encrypt" in result.stdout
    assert "LoadCredentialEncrypted=github-webhook-secret:" in result.stdout
    assert run_mock.call_count == 0


def test_install_credentials_invokes_systemd_creds_encrypt(
    isolated_home: Path,
    tmp_path: Path,
) -> None:
    import subprocess as _subprocess

    credstore = tmp_path / "credstore.encrypted"
    credstore.mkdir()
    with (
        patch("waitbus.cli.shutil.which", return_value="/usr/bin/systemd-creds"),
        patch("waitbus.cli.subprocess.run") as run_mock,
    ):
        run_mock.return_value = _subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout="",
            stderr="",
        )
        result = runner.invoke(
            cli.app,
            ["install-credentials", "broadcast-token", "--value", "tok-xyz", "--credstore-dir", str(credstore)],
        )
    assert result.exit_code == 0, result.stdout
    assert run_mock.call_count == 1
    args = run_mock.call_args.args[0]
    assert args[0] == "systemd-creds"
    assert args[1] == "encrypt"
    assert args[2] == "--name=broadcast-token"
    assert args[-1].endswith("waitbus.broadcast-token.cred")
    assert run_mock.call_args.kwargs["input"] == "tok-xyz"


def test_install_credentials_rejects_value_and_file_together(isolated_home: Path) -> None:
    with patch("waitbus.cli.shutil.which", return_value="/usr/bin/systemd-creds"):
        result = runner.invoke(
            cli.app,
            ["install-credentials", "x", "--value", "v", "--file", "/dev/null"],
        )
    assert result.exit_code != 0
    assert "mutually exclusive" in result.output


def test_install_credentials_requires_systemd_creds_on_path(isolated_home: Path) -> None:
    with patch("waitbus.cli.shutil.which", return_value=None):
        result = runner.invoke(
            cli.app,
            ["install-credentials", "x", "--value", "v", "--dry-run"],
        )
    assert result.exit_code != 0
    assert "systemd-creds is not on PATH" in result.output


def test_install_credentials_rejects_empty_value(isolated_home: Path) -> None:
    with patch("waitbus.cli.shutil.which", return_value="/usr/bin/systemd-creds"):
        result = runner.invoke(
            cli.app,
            ["install-credentials", "x", "--value", "", "--dry-run"],
        )
    assert result.exit_code != 0
    assert "empty" in result.output


# --- install-systemd -------------------------------------------------------

# install-systemd refuses to run off Linux (it points operators at
# install-launchd); the macOS side is covered by
# tests/test_install_launchd.py's symmetric platform-guard test.
_INSTALL_SYSTEMD_LINUX_ONLY = pytest.mark.skipif(
    sys.platform != "linux",
    reason="install-systemd is Linux-only; install-launchd covers the macOS path",
)


def _make_share_dir(tmp_path: Path, units: list[str]) -> Path:
    share = tmp_path / "share-systemd-user"
    share.mkdir(parents=True)
    (share / "waitbus.MANIFEST.txt").write_text("\n".join(["# manifest", *units]) + "\n")
    for u in units:
        (share / u).write_text(f"[Unit]\nDescription={u}\n")
    return share


@_INSTALL_SYSTEMD_LINUX_ONLY
def test_install_systemd_copy_writes_units(isolated_home: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    units = ["waitbus-listener.service", "waitbus-watchdog.timer"]
    share = _make_share_dir(isolated_home, units)
    target = isolated_home / ".config" / "systemd" / "user"
    monkeypatch.setattr("waitbus.cli.install.systemd._share_systemd_user_dir", lambda: share)
    monkeypatch.setattr("waitbus.cli.install.systemd._systemd_user_target_dir", lambda: target)
    with patch("waitbus.cli.subprocess.run") as run:
        run.return_value = importlib.import_module("subprocess").CompletedProcess(
            args=[], returncode=0, stdout="", stderr=""
        )
        result = runner.invoke(cli.app, ["install-systemd", "--no-enable"])
    assert result.exit_code == 0, result.stdout
    for u in units:
        assert (target / u).exists()


@_INSTALL_SYSTEMD_LINUX_ONLY
def test_install_systemd_dry_run_does_not_copy(isolated_home: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    units = ["waitbus-listener.service"]
    share = _make_share_dir(isolated_home, units)
    target = isolated_home / ".config" / "systemd" / "user"
    monkeypatch.setattr("waitbus.cli.install.systemd._share_systemd_user_dir", lambda: share)
    monkeypatch.setattr("waitbus.cli.install.systemd._systemd_user_target_dir", lambda: target)
    result = runner.invoke(cli.app, ["install-systemd", "--dry-run", "--no-enable"])
    assert result.exit_code == 0, result.stdout
    assert "Would copy" in result.stdout
    assert not target.exists() or not any(target.iterdir())


@_INSTALL_SYSTEMD_LINUX_ONLY
def test_install_systemd_dry_run_with_sync_never_prompts(isolated_home: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """--dry-run + --sync prints the diff and exits 0 even with no TTY."""
    units = ["waitbus-listener.service"]
    share = _make_share_dir(isolated_home, units)
    target = isolated_home / ".config" / "systemd" / "user"
    target.mkdir(parents=True)
    # Plant an orphan unit for --sync to detect.
    (target / "waitbus-old.service").write_text("[Unit]\n")
    monkeypatch.setattr("waitbus.cli.install.systemd._share_systemd_user_dir", lambda: share)
    monkeypatch.setattr("waitbus.cli.install.systemd._systemd_user_target_dir", lambda: target)
    result = runner.invoke(
        cli.app,
        ["install-systemd", "--dry-run", "--sync", "--no-enable"],
        input="",  # no TTY input available
    )
    assert result.exit_code == 0, result.stdout
    assert "Would stop + disable + remove" in result.stdout
    # The orphan must STILL exist after a dry-run.
    assert (target / "waitbus-old.service").exists()


@_INSTALL_SYSTEMD_LINUX_ONLY
def test_install_systemd_sync_force_removes_orphans(isolated_home: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    units = ["waitbus-listener.service"]
    share = _make_share_dir(isolated_home, units)
    target = isolated_home / ".config" / "systemd" / "user"
    target.mkdir(parents=True)
    orphan = target / "waitbus-old.service"
    orphan.write_text("[Unit]\n")
    # Also plant a user-created file that should NOT be touched.
    user_file = target / "waitbus-notes.txt"
    user_file.write_text("not a unit\n")
    monkeypatch.setattr("waitbus.cli.install.systemd._share_systemd_user_dir", lambda: share)
    monkeypatch.setattr("waitbus.cli.install.systemd._systemd_user_target_dir", lambda: target)
    with patch("waitbus.cli.subprocess.run") as run:
        run.return_value = importlib.import_module("subprocess").CompletedProcess(
            args=[], returncode=0, stdout="", stderr=""
        )
        result = runner.invoke(
            cli.app,
            ["install-systemd", "--sync", "--force", "--no-enable"],
        )
    assert result.exit_code == 0, result.stdout
    assert not orphan.exists(), "orphan unit must be removed"
    assert user_file.exists(), "non-unit file must NOT be touched"


@_INSTALL_SYSTEMD_LINUX_ONLY
def test_install_systemd_missing_share_dir_exits_2(isolated_home: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    missing = isolated_home / "no-such-share"
    monkeypatch.setattr("waitbus.cli.install.systemd._share_systemd_user_dir", lambda: missing)
    monkeypatch.setattr("waitbus.cli.install.systemd._systemd_user_target_dir", lambda: isolated_home / "target")
    result = runner.invoke(cli.app, ["install-systemd", "--no-enable"])
    assert result.exit_code == 2
    # The "source dir does not exist" message goes to stderr via
    # typer.secho(err=True). Newer click versions split streams in
    # CliRunner; `result.output` is the merged view.
    assert "no-such-share" in result.output


@_INSTALL_SYSTEMD_LINUX_ONLY
def test_install_systemd_missing_manifest_exits_2(isolated_home: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    share = isolated_home / "share-empty"
    share.mkdir()
    monkeypatch.setattr("waitbus.cli.install.systemd._share_systemd_user_dir", lambda: share)
    monkeypatch.setattr("waitbus.cli.install.systemd._systemd_user_target_dir", lambda: isolated_home / "target")
    result = runner.invoke(cli.app, ["install-systemd", "--no-enable"])
    assert result.exit_code == 2


# --- doctor ----------------------------------------------------------------

# The doctor subcommand dispatches the process-supervisor section by
# platform (systemd on Linux, launchd on macOS). The existing tests fix
# the systemd branch; macOS coverage of the launchd branch lives in
# tests/test_install_launchd.py. Marking the two doctor tests Linux-only
# avoids a CI matrix split between the systemd and launchd output shapes.
_DOCTOR_LINUX_ONLY = pytest.mark.skipif(
    sys.platform != "linux",
    reason="doctor's systemd section is Linux-only; macOS exercises [launchd] instead",
)


@_DOCTOR_LINUX_ONLY
def test_doctor_exits_1_when_state_missing(isolated_home: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Fresh tmp HOME with nothing provisioned should surface issues."""
    monkeypatch.setattr("waitbus.cli._shared._share_systemd_user_dir", lambda: isolated_home / "no-share")
    monkeypatch.setattr("waitbus.cli._shared._systemd_user_target_dir", lambda: isolated_home / "no-target")
    with patch("waitbus.cli.shutil.which", return_value=None):
        result = runner.invoke(cli.app, ["doctor"])
    assert result.exit_code == 1
    # Must include sections for config, paths, binaries, credentials, systemd.
    assert "[config]" in result.stdout
    assert "[paths]" in result.stdout
    assert "[binaries]" in result.stdout
    assert "[credentials]" in result.stdout
    assert "[systemd]" in result.stdout


@_DOCTOR_LINUX_ONLY
@pytest.mark.skipif(
    os.geteuid() == 0,
    reason="root bypasses directory permissions; cannot simulate an unreadable credstore",
)
def test_doctor_handles_unreadable_credstore(
    isolated_home: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """doctor must not crash when the credstore dir is not operator-readable.

    The system credstore is root-0700 by design (systemd-creds), so the
    operator account cannot stat its entries. Python 3.12+ raises
    PermissionError from Path.is_file() there (3.11 returned False); doctor
    must report the credentials as indeterminate and keep going, not crash
    mid-run before the [systemd] section.
    """
    locked = tmp_path / "credstore.encrypted"
    locked.mkdir()
    os.chmod(locked, 0o000)  # no search permission: is_file() on a child raises PermissionError
    monkeypatch.setattr("waitbus.cli._shared.CREDSTORE_DIR", locked)
    monkeypatch.setattr("waitbus.cli._shared._share_systemd_user_dir", lambda: isolated_home / "no-share")
    monkeypatch.setattr("waitbus.cli._shared._systemd_user_target_dir", lambda: isolated_home / "no-target")
    try:
        with patch("waitbus.cli.shutil.which", return_value=None):
            result = runner.invoke(cli.app, ["doctor"])
    finally:
        os.chmod(locked, 0o755)  # restore so pytest's tmp cleanup can remove it
    assert not isinstance(result.exception, PermissionError), result.exception
    assert "[credentials]" in result.stdout
    assert "indeterminate" in result.stdout
    assert "[systemd]" in result.stdout  # reached past credentials without crashing


@_DOCTOR_LINUX_ONLY
def test_doctor_exits_0_when_everything_ok(isolated_home: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """All-green doctor on a fully-installed system."""
    units = ["waitbus-listener.service"]
    share = _make_share_dir(isolated_home, units)
    target = isolated_home / ".config" / "systemd" / "user"
    target.mkdir(parents=True)
    for u in units:
        shutil.copy2(share / u, target / u)
    # Bootstrap state dirs and DB.
    runner.invoke(cli.app, ["init"])
    monkeypatch.setattr("waitbus.cli._shared._share_systemd_user_dir", lambda: share)
    monkeypatch.setattr("waitbus.cli._shared._systemd_user_target_dir", lambda: target)
    with (
        patch("waitbus.cli.shutil.which", return_value="/usr/bin/systemd-creds"),
        patch("waitbus.cli.doctor._check_credentials", return_value=[]),
        patch("waitbus.cli.doctor._check_metrics_endpoint", return_value=[]),
        patch("waitbus.cli.doctor._check_config_validation", return_value=[]),
    ):
        result = runner.invoke(cli.app, ["doctor"])
    assert result.exit_code == 0, result.stdout
    assert "all checks passed" in result.stdout
