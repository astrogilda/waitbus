"""Tests for the one-shot legacy state migration in `waitbus init`.

`_migrate_legacy_state_if_needed` detects a populated legacy state
directory, stops the relevant daemons, and moves the tree onto the
platformdirs-resolved target. The helper is idempotent: a second run
sees no legacy dir and no-ops.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import typer

from waitbus import cli


def _seed_legacy(legacy_dir: Path) -> None:
    """Create a populated legacy event dir with the canonical github.db
    sentinel plus a representative scaffold file."""
    legacy_dir.mkdir(parents=True, exist_ok=True)
    (legacy_dir / "github.db").write_bytes(b"\x00" * 64)
    (legacy_dir / "watched_repos.txt").write_text("# example\n")
    cursors = legacy_dir / "cursors"
    cursors.mkdir()
    (cursors / "owner_repo.ulid").write_text("01HZ" + "Z" * 22)


def _patch_subprocess_run(monkeypatch: pytest.MonkeyPatch) -> list[list[str]]:
    """Replace `subprocess.run` inside cli with a recorder; return the
    captured argv lists. Returns a list of all invocations (one entry
    per call).
    """
    calls: list[list[str]] = []

    def _fake_run(args: list[str], *_args: object, **_kwargs: object) -> object:
        calls.append(list(args))

        class _Result:
            returncode = 0
            stdout = ""
            stderr = ""

        return _Result()

    monkeypatch.setattr(cli.subprocess, "run", _fake_run)
    return calls


def test_no_legacy_no_op(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """When the legacy dir does not exist, the helper returns cleanly
    and never invokes systemctl."""
    fake_home = tmp_path / "home"
    fake_home.mkdir()
    monkeypatch.setattr(Path, "home", lambda: fake_home)
    monkeypatch.setenv("WAITBUS_STATE_DIR", str(tmp_path / "state" / "waitbus"))
    calls = _patch_subprocess_run(monkeypatch)

    # Should not raise; should not invoke systemctl.
    cli._migrate_legacy_state_if_needed()

    assert calls == []
    # Target was not auto-created either; the helper is no-op on absence.
    assert not (tmp_path / "state" / "waitbus").exists()


def test_legacy_with_data_moves_atomically(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Populated legacy + empty target: helper stops daemons, runs
    shutil.move, then applies chmod 0700 on the new tree."""
    fake_home = tmp_path / "home"
    legacy = fake_home / ".claude" / "events"
    _seed_legacy(legacy)
    target = tmp_path / "state" / "waitbus"
    monkeypatch.setattr(Path, "home", lambda: fake_home)
    monkeypatch.setenv("WAITBUS_STATE_DIR", str(target))
    calls = _patch_subprocess_run(monkeypatch)

    cli._migrate_legacy_state_if_needed()

    # Legacy is gone; target now holds the moved tree.
    assert not legacy.exists()
    assert (target / "github.db").exists()
    assert (target / "watched_repos.txt").exists()
    assert (target / "cursors" / "owner_repo.ulid").exists()
    # 0700 perms applied to the new root.
    assert (target.stat().st_mode & 0o777) == 0o700
    # systemctl stop was invoked for each of the open-fd-holding units.
    stopped_units = {
        call[3].removesuffix(".service")  # ["systemctl", "--user", "stop", "<unit>.service"]
        for call in calls
        if call[:3] == ["systemctl", "--user", "stop"]
    }
    assert stopped_units == set(cli._MIGRATION_DAEMON_UNITS)


def test_both_populated_refuses(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """When both legacy and target are populated, the helper refuses
    to guess: it prints both paths to stderr and raises typer.Exit
    with a non-zero code; the legacy tree stays untouched."""
    fake_home = tmp_path / "home"
    legacy = fake_home / ".claude" / "events"
    _seed_legacy(legacy)
    target = tmp_path / "state" / "waitbus"
    target.mkdir(parents=True)
    (target / "github.db").write_bytes(b"\x01" * 32)
    monkeypatch.setattr(Path, "home", lambda: fake_home)
    monkeypatch.setenv("WAITBUS_STATE_DIR", str(target))
    _patch_subprocess_run(monkeypatch)

    with pytest.raises(typer.Exit) as excinfo:
        cli._migrate_legacy_state_if_needed()

    assert excinfo.value.exit_code != 0
    captured = capsys.readouterr()
    assert str(legacy) in captured.err
    assert str(target) in captured.err
    # Legacy untouched.
    assert (legacy / "github.db").exists()


def test_idempotent_second_call_is_noop(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """First call migrates; the second call sees no legacy and no-ops."""
    fake_home = tmp_path / "home"
    legacy = fake_home / ".claude" / "events"
    _seed_legacy(legacy)
    target = tmp_path / "state" / "waitbus"
    monkeypatch.setattr(Path, "home", lambda: fake_home)
    monkeypatch.setenv("WAITBUS_STATE_DIR", str(target))
    calls = _patch_subprocess_run(monkeypatch)

    # First call migrates.
    cli._migrate_legacy_state_if_needed()
    assert (target / "github.db").exists()
    first_call_count = len(calls)
    assert first_call_count > 0

    # Second call: legacy is gone, helper short-circuits.
    cli._migrate_legacy_state_if_needed()
    assert len(calls) == first_call_count  # no new systemctl calls
