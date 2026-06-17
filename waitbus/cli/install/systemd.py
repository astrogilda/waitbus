"""`install-systemd` top-level command."""

from __future__ import annotations

import subprocess
import sys

import typer

from .._shared import (
    _apply_unit_change,
    _enable_units,
    _install_executable_stack_overrides,
    _interpreter_has_executable_stack,
    _read_manifest,
    _share_systemd_user_dir,
    _sync_orphans,
    _systemd_user_target_dir,
)


def install_systemd(
    enable: bool = typer.Option(
        True,
        "--enable/--no-enable",
        help="After copying, `systemctl --user enable --now` the units. Use --no-enable for a copy-only install.",
    ),
    sync: bool = typer.Option(
        False,
        "--sync",
        help="In addition to copying the current units, remove orphans — "
        "units present in ~/.config/systemd/user/ that match the "
        "waitbus-* prefix but are NOT in this wheel's MANIFEST.txt "
        "(e.g., units removed in a package upgrade). Stops and "
        "disables them before deletion. Requires --force OR a TTY "
        "to confirm.",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Skip the confirmation prompt for --sync orphan removal. "
        "Required when running non-interactively (e.g., from a "
        "post-install hook).",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Print what would be done; do not copy, enable, or remove anything. "
        "Never prompts for confirmation; exits 0 after printing the diff.",
    ),
) -> None:
    """Install waitbus systemd-user units to ~/.config/systemd/user/.

    Required when waitbus is installed via `uv tool install` or
    `pipx install` (those isolated prefixes are not on systemd's load
    path). For `pip install --user` this command is still safe to run
    — it idempotently mirrors the canonical units to the operator's
    own systemd-user dir, which guarantees `systemctl --user
    daemon-reload` sees them.
    """
    if sys.platform != "linux":
        typer.secho(
            "install-systemd is Linux-only. On macOS use `waitbus install-launchd` instead.",
            fg=typer.colors.YELLOW,
            err=True,
        )
        raise typer.Exit(code=0)
    share_dir = _share_systemd_user_dir()
    target_dir = _systemd_user_target_dir()

    typer.echo(f"waitbus install-systemd (dry-run={dry_run}, enable={enable})")
    typer.echo(f"  Source: {share_dir}")
    typer.echo(f"  Target: {target_dir}")

    if not share_dir.exists():
        typer.secho(
            f"ERROR: source dir does not exist: {share_dir}\n"
            "Is waitbus installed via pip / uv tool / pipx? "
            "If you're running from a checkout, run `uv pip install -e .` first.",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2)

    units = _read_manifest(share_dir)
    if not units:
        typer.secho(
            f"ERROR: no units found via MANIFEST at {share_dir}/waitbus.MANIFEST.txt",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2)

    typer.echo(f"  Units to install: {len(units)}")

    if not dry_run:
        target_dir.mkdir(parents=True, exist_ok=True)

    for unit in units:
        src = share_dir / unit
        dst = target_dir / unit
        if not src.exists():
            typer.secho(f"  ERROR: declared unit missing in source: {src}", fg=typer.colors.RED, err=True)
            continue
        _apply_unit_change("copy", src, dst, dry_run)

    # An interpreter with an executable stack (uv / pyenv standalone Python
    # builds ship without a non-executable GNU_STACK header) makes glibc
    # allocate writable-and-executable thread stacks, which the units'
    # MemoryDenyWriteExecute= setting blocks -- the daemons would fail to
    # create threads. Detect that and drop in an override so they run; keep
    # the protection for interpreters whose stack is non-executable.
    if _interpreter_has_executable_stack():
        overridden = _install_executable_stack_overrides(units, target_dir, dry_run=dry_run)
        if overridden:
            typer.secho(
                "  Note: this interpreter marks its stack executable, which is "
                "typical of uv and pyenv standalone Python builds. "
                "MemoryDenyWriteExecute has been disabled for the waitbus daemon "
                "units so they can create threads; run waitbus under a system "
                "Python with a non-executable stack to keep that protection.",
                fg=typer.colors.YELLOW,
                err=True,
            )

    # daemon-reload is cheap and always safe
    if dry_run:
        typer.echo("  Would run: systemctl --user daemon-reload")
    else:
        proc = subprocess.run(["systemctl", "--user", "daemon-reload"], capture_output=True, text=True, timeout=10)
        if proc.returncode != 0:
            typer.secho(f"  systemctl daemon-reload failed: {proc.stderr.strip()}", fg=typer.colors.YELLOW, err=True)
        else:
            typer.echo("  systemctl --user daemon-reload OK")

    if enable:
        _enable_units(units, dry_run=dry_run)

    if sync:
        _sync_orphans(target_dir, set(units), force=force, dry_run=dry_run)

    typer.echo("")
    typer.echo("Done. Check status with:  systemctl --user status waitbus-listener")
