"""`install-credentials` top-level command."""

from __future__ import annotations

import contextlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import typer

from ... import _secrets
from .._shared import KNOWN_CREDENTIALS, _read_credential_value

_KNOWN_CREDENTIAL_NAMES = ", ".join(name for name, _desc in KNOWN_CREDENTIALS)

#: Staging this credential brings the (opt-in) GitHub webhook listener online.
_LISTENER_TRIGGER_CREDENTIAL = "github-webhook-secret"


def _atomic_merge_write(path: Path, name: str, value: str) -> None:
    """Merge ``{name: value}`` into the JSON secrets file, atomically.

    Reads the existing object (empty when absent), updates the one key
    without clobbering siblings, writes a temp file whose mode is set to
    0600 BEFORE any payload is written, validates the serialized JSON,
    then ``os.replace`` swaps it in (POSIX-atomic, so a concurrent daemon
    never reads a torn file). The temp file lives in the same directory
    as the target so ``os.replace`` stays on one filesystem.
    """
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    existing: dict[str, object] = {}
    if path.exists():
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise typer.BadParameter(f"existing secrets file {path} is unreadable/corrupt: {exc}") from exc
        if not isinstance(loaded, dict):
            raise typer.BadParameter(f"existing secrets file {path} must contain a JSON object")
        existing = loaded
    existing[name] = value
    payload = json.dumps(existing, indent=2, sort_keys=True) + "\n"
    # Validate the serialized form round-trips before it touches the target.
    json.loads(payload)
    fd, tmp = tempfile.mkstemp(prefix=".secrets-", suffix=".tmp", dir=str(path.parent))
    try:
        # 0600 before the payload is written so the secret is never briefly
        # group/other-readable.
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(payload)
        os.replace(tmp, path)
    except OSError:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


def _enable_listener() -> None:
    """Bring the opt-in webhook listener online for this platform.

    Linux: ``systemctl --user enable --now waitbus-listener.service``.
    macOS: ``launchctl bootstrap gui/$UID <listener plist>`` (re-bootstraps
    the already-written plist). Both are best-effort: a failure prints a
    warning but does not fail the credential staging — the secret is on
    disk regardless and the operator can enable the unit manually.
    """
    if sys.platform == "linux":
        proc = subprocess.run(
            ["systemctl", "--user", "enable", "--now", "waitbus-listener.service"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if proc.returncode != 0:
            typer.secho(
                f"  warning: could not enable waitbus-listener.service: {proc.stderr.strip()}\n"
                "  Run `waitbus install-systemd` first, then "
                "`systemctl --user enable --now waitbus-listener.service`.",
                fg=typer.colors.YELLOW,
                err=True,
            )
        else:
            typer.echo("  Enabled + started: waitbus-listener.service")
        return
    if sys.platform == "darwin":
        from .._shared import _launchctl_bootstrap, _launchd_target_dir

        plist = _launchd_target_dir() / "dev.waitbus.listener.plist"
        if not plist.exists():
            typer.secho(
                f"  warning: listener plist not found at {plist}; "
                "run `waitbus install-launchd` first, then re-run this command.",
                fg=typer.colors.YELLOW,
                err=True,
            )
            return
        _launchctl_bootstrap(plist, dry_run=False)
        return
    typer.secho(
        f"  note: automatic listener enablement is unsupported on {sys.platform!r}; enable it manually.",
        fg=typer.colors.YELLOW,
        err=True,
    )


def install_credentials(
    name: str = typer.Argument(
        ...,
        help=(
            f"Credential name. Known names: {_KNOWN_CREDENTIAL_NAMES}. "
            "Unknown names are accepted (operators may add their own credentials)."
        ),
    ),
    source_file: Path | None = typer.Option(  # noqa: B008  (typer idiom)
        None,
        "--file",
        help="Read the credential value from this file. When omitted, the value is read from stdin.",
        exists=False,
        file_okay=True,
        dir_okay=False,
        readable=False,
    ),
    enable_listener: bool = typer.Option(
        True,
        "--enable-listener/--no-enable-listener",
        help=(
            "When staging github-webhook-secret, bring the opt-in webhook "
            "listener online (Linux: systemctl --user enable --now; macOS: "
            "launchctl bootstrap). Use --no-enable-listener to stage the "
            "secret without starting the listener."
        ),
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Print the destination path and (for the listener secret) the enablement command; write nothing.",
    ),
) -> None:
    """Stage a credential into the 0600 JSON secrets file for the daemons.

    The value is read from ``--file <path>`` or, when omitted, from stdin
    (so it never lands in shell history). It is merged into the JSON
    object at ``_paths.state_dir()/secrets.json`` without clobbering other
    keys; the file is written atomically with mode 0600.

    Staging ``github-webhook-secret`` also enables the (opt-in) webhook
    listener unit unless ``--no-enable-listener`` is passed.

    Rotation: re-run with the new value and restart the consuming unit(s)
    so the daemon re-reads the secrets file.
    """
    typer.echo(f"waitbus install-credentials {name} (dry-run={dry_run})")

    secrets_path = Path(_secrets.secrets_path())

    if dry_run:
        typer.echo(f"  Would write {name} into: {secrets_path} (mode 0600, merged)")
        if name == _LISTENER_TRIGGER_CREDENTIAL and enable_listener:
            if sys.platform == "linux":
                typer.echo("  Would run: systemctl --user enable --now waitbus-listener.service")
            elif sys.platform == "darwin":
                typer.echo("  Would run: launchctl bootstrap gui/$UID dev.waitbus.listener.plist")
        return

    plaintext = _read_credential_value(inline=None, source_file=source_file, name=name)
    # Trailing newline from a heredoc / file is a frequent footgun; strip it
    # so the stored value matches what the operator intended.
    plaintext = plaintext.rstrip("\r\n")
    if not plaintext:
        raise typer.BadParameter(f"credential value for {name} is empty")

    _atomic_merge_write(secrets_path, name, plaintext)
    typer.echo(f"  Wrote {name} into {secrets_path} (mode 0600)")

    if name == _LISTENER_TRIGGER_CREDENTIAL and enable_listener:
        _enable_listener()
