"""`install-credentials` top-level command."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import typer

from .._shared import CREDSTORE_DIR, KNOWN_CREDENTIALS, _read_credential_value

_KNOWN_CREDENTIAL_NAMES = ", ".join(name for name, _desc in KNOWN_CREDENTIALS)


def install_credentials(
    name: str = typer.Argument(
        ...,
        help=(
            f"Credential name. Known names: {_KNOWN_CREDENTIAL_NAMES}. "
            "Unknown names are accepted (operators may add their own credentials)."
        ),
    ),
    value: str | None = typer.Option(
        None,
        "--value",
        help="Inline credential value. Mutually exclusive with --file. "
        "Avoid on shared hosts: shell history retains the secret.",
    ),
    source_file: Path | None = typer.Option(  # noqa: B008  (typer idiom)
        None,
        "--file",
        help="Read the credential value from this file. Mutually exclusive with --value.",
        exists=False,
        file_okay=True,
        dir_okay=False,
        readable=False,
    ),
    credstore_dir: Path = typer.Option(  # noqa: B008  (typer idiom)
        CREDSTORE_DIR,
        "--credstore-dir",
        help=f"Directory where the encrypted credential is written. Defaults to {CREDSTORE_DIR}.",
        exists=False,
        file_okay=False,
        dir_okay=True,
        readable=False,
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Print the systemd-creds command and the LoadCredentialEncrypted= "
        "drop-in snippet; do not invoke systemd-creds or write any file.",
    ),
) -> None:
    """Encrypt a credential with systemd-creds and stage it for the daemon.

    Encrypts the value with ``systemd-creds encrypt --name=<name>`` and
    writes the ciphertext to ``<credstore-dir>/waitbus.<name>.cred``.
    Prints the ``LoadCredentialEncrypted=`` snippet to add to each unit
    drop-in that needs the credential.

    The value is read from one of: ``--value <inline>``, ``--file <path>``,
    or stdin (terminal hint shown when interactive).

    Rotation: re-run with the new value; systemd-creds will overwrite the
    ciphertext at the destination path. Restart the consuming unit(s) so
    the daemon re-reads ``$CREDENTIALS_DIRECTORY``.
    """
    typer.echo(f"waitbus install-credentials {name} (dry-run={dry_run})")

    if shutil.which("systemd-creds") is None:
        raise typer.BadParameter(
            "systemd-creds is not on PATH. Install systemd >= 250 (systemd-creds(1) ships with systemd itself)."
        )

    plaintext = _read_credential_value(inline=value, source_file=source_file, name=name)
    if not plaintext:
        raise typer.BadParameter(f"credential value for {name} is empty")

    dst = credstore_dir / f"waitbus.{name}.cred"
    cmd = ["systemd-creds", "encrypt", f"--name={name}", "-", str(dst)]
    typer.echo(f"  Encrypted output: {dst}")
    typer.echo(f"  Command: {' '.join(cmd)}")

    if dry_run:
        typer.echo("  (dry-run; skipping invocation)")
    else:
        if not credstore_dir.exists():
            raise typer.BadParameter(
                f"{credstore_dir} does not exist. Create it as root (mkdir -p, "
                f"chmod 700) before running install-credentials."
            )
        proc = subprocess.run(
            cmd,
            input=plaintext,
            capture_output=True,
            text=True,
            timeout=15,
        )
        if proc.returncode != 0:
            typer.secho(
                f"  systemd-creds encrypt failed: {proc.stderr.strip()}",
                fg=typer.colors.RED,
                err=True,
            )
            raise typer.Exit(code=2)
        typer.echo(f"  Wrote {dst}")

    typer.echo("")
    typer.echo("Add this line to each unit drop-in that needs the credential:")
    typer.echo(f"  LoadCredentialEncrypted={name}:{dst}")
