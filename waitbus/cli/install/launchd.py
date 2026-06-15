"""`install-launchd` top-level command (macOS)."""

from __future__ import annotations

import sys

import typer

from ... import _paths
from .._shared import (
    _apply_launchd_plist,
    _launchctl_bootstrap,
    _launchd_log_dir,
    _launchd_target_dir,
    _resolve_launchd_bin_dir,
    _share_launchd_dir,
    _sync_launchd_orphans,
)


def install_launchd(
    enable: bool = typer.Option(
        True,
        "--enable/--no-enable",
        help="After resolving placeholders and writing plists, "
        "`launchctl bootstrap gui/$UID` each one. Use --no-enable "
        "for a write-only install.",
    ),
    sync: bool = typer.Option(
        False,
        "--sync",
        help="Remove orphan plists — plists present in "
        "~/Library/LaunchAgents/ that match the dev.waitbus.* "
        "prefix but are NOT in this wheel's MANIFEST.txt. Boots "
        "them out before deletion. Requires --force OR a TTY to "
        "confirm.",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Skip the confirmation prompt for --sync orphan removal. Required when running non-interactively.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Print what would be done; do not write, bootstrap, or remove anything. Never prompts for confirmation.",
    ),
) -> None:
    """Install waitbus LaunchAgent plists to ~/Library/LaunchAgents/.

    Mirrors ``install-systemd`` for macOS. The shipped plists carry
    ``__BIN_DIR__`` / ``__LOG_DIR__`` / ``__RUNTIME_DIR__``
    placeholders that this command resolves to the operator's actual
    paths before writing into the LaunchAgents directory. Agents are
    loaded via ``launchctl bootstrap gui/$UID <plist>`` (the modern
    replacement for the deprecated ``launchctl load -w``).
    """
    if sys.platform != "darwin":
        typer.secho(
            "install-launchd is macOS-only. On Linux use `waitbus install-systemd` instead.",
            fg=typer.colors.YELLOW,
            err=True,
        )
        raise typer.Exit(code=0)

    share_dir = _share_launchd_dir()
    target_dir = _launchd_target_dir()
    log_dir = _launchd_log_dir()
    bin_dir = _resolve_launchd_bin_dir()
    runtime_dir = _paths.runtime_dir()

    typer.echo(f"waitbus install-launchd (dry-run={dry_run}, enable={enable})")
    typer.echo(f"  Source:    {share_dir}")
    typer.echo(f"  Target:    {target_dir}")
    typer.echo(f"  Bin dir:   {bin_dir}")
    typer.echo(f"  Log dir:   {log_dir}")
    typer.echo(f"  Runtime:   {runtime_dir}")

    if not share_dir.exists():
        typer.secho(
            f"ERROR: source dir does not exist: {share_dir}\n"
            "Is waitbus installed via pip / uv tool / pipx? "
            "If you're running from a checkout, run `uv pip install -e .` first.",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2)

    manifest_path = share_dir / "MANIFEST.txt"
    if not manifest_path.exists():
        typer.secho(
            f"ERROR: no plists found via MANIFEST at {manifest_path}",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2)
    plists = [
        line.strip()
        for line in manifest_path.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    typer.echo(f"  Plists to install: {len(plists)}")

    if not dry_run:
        target_dir.mkdir(parents=True, exist_ok=True)
        log_dir.mkdir(parents=True, exist_ok=True)

    for name in plists:
        src = share_dir / name
        dst = target_dir / name
        if not src.exists():
            typer.secho(f"  ERROR: declared plist missing in source: {src}", fg=typer.colors.RED, err=True)
            continue
        _apply_launchd_plist(
            src,
            dst,
            bin_dir=bin_dir,
            log_dir=log_dir,
            runtime_dir=runtime_dir,
            dry_run=dry_run,
        )

    if enable:
        for name in plists:
            _launchctl_bootstrap(target_dir / name, dry_run=dry_run)

    if sync:
        _sync_launchd_orphans(
            target_dir,
            set(plists),
            force=force,
            dry_run=dry_run,
        )

    typer.echo("")
    typer.echo("Done. Check status with:  launchctl print gui/$UID/dev.waitbus.listener")
