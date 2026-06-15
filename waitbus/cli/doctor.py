"""`doctor` top-level command — validate the current install."""

from __future__ import annotations

import sys

import typer

from ._shared import (
    _check_binaries,
    _check_config,
    _check_config_validation,
    _check_credentials,
    _check_launchd,
    _check_metrics_endpoint,
    _check_paths,
    _check_systemd,
)


def doctor() -> None:
    """Validate the current install. Exit 0 if everything's in sync;
    exit 1 if anything is off (so the command is usable in pre-commit
    hooks, shell-prompt indicators, or post-restart health checks).

    Distinct from `init --dry-run`: dry-run is a preview of an action
    and always returns 0; doctor is a sync-check across the live
    filesystem, credential store, and systemd state.
    """
    typer.echo("=== waitbus doctor ===")
    typer.echo("")

    issues: list[str] = []
    issues.extend(_check_config())
    issues.extend(_check_config_validation())
    issues.extend(_check_paths())
    issues.extend(_check_binaries())
    issues.extend(_check_credentials())
    issues.extend(_check_metrics_endpoint())
    # Process-supervisor check is platform-dispatched at the leaf: launchd on
    # macOS, systemd on Linux. Non-supported platforms (BSD, Windows under
    # WSL, ...) report the unsupported state and continue.
    if sys.platform == "darwin":
        issues.extend(_check_launchd())
    elif sys.platform == "linux":
        issues.extend(_check_systemd())
    else:
        typer.echo(f"[supervisor] (no supervisor check on {sys.platform!r})\n")

    if issues:
        typer.secho(f"=== {len(issues)} issue(s) ===", fg=typer.colors.YELLOW, err=True)
        for issue in issues:
            typer.echo(f"  - {issue}", err=True)
        raise typer.Exit(code=1)
    typer.secho("=== all checks passed ===", fg=typer.colors.GREEN)
