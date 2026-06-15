"""Shared helpers, constants, and templates used across the cli/ package.

Holds the install-time helpers (path resolution, manifest reading, unit
mutation), the health-check helpers (binaries, paths, credentials,
systemd, launchd, config, /metrics), the launchd plist plumbing, the
legacy state-migration shim, and the credential-value resolver.
"""

from __future__ import annotations

import os
import re
import shutil
import stat
import subprocess
import sys
import sysconfig
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import NoReturn

import typer

from .. import _paths

# ---------------------------------------------------------------------------
# version callbacks
# ---------------------------------------------------------------------------


def _version_callback(value: bool) -> None:
    if value:
        from .._version import PACKAGE_VERSION

        if PACKAGE_VERSION == "0.0.0+unknown":
            typer.echo("waitbus (version unavailable — package metadata missing)")
        else:
            typer.echo(f"waitbus {PACKAGE_VERSION}")
        raise typer.Exit()


def _sub_version_callback(value: bool) -> None:
    if value:
        _version_callback(True)


# ---------------------------------------------------------------------------
# constants
# ---------------------------------------------------------------------------

REQUIRED_BINS: tuple[str, ...] = ("systemctl", "systemd-creds")
"""Binaries every waitbus workflow assumes are present on PATH.
`gh` is not required at install time — it is only needed by the
per-repo forwarder, which fails clearly when invoked without it.
``systemd-creds`` (shipped with systemd >= 250) is needed by
``waitbus install-credentials``."""

# Anchored regex used to identify wheel-owned units in
# ~/.config/systemd/user/. Replaces the prior fnmatch-style "waitbus-*"
# glob, which would have matched a user-created file named, e.g.,
# `waitbus-notes.txt` and counted it as an orphan.
_UNIT_NAME_RE = re.compile(r"^waitbus-[a-z][a-z0-9-]*\.(service|socket|timer)$")

WATCHED_REPOS_TEMPLATE = """\
# waitbus watched_repos.txt — one owner/repo slug per line.
# Lines starting with `#` are ignored. The ETag-poll loop walks this
# list every 45s and conditionally GETs api.github.com for fresh runs.
#
# Add a line per repo you want passive polling for (in addition to
# any webhook-driven repos you've configured via `gh webhook forward`
# or a GitHub repository webhook pointing at http://127.0.0.1:9000/webhook).
"""

ETAG_STATE_TEMPLATE = "{}\n"

CREDSTORE_DIR = Path("/etc/credstore.encrypted")
"""Host-wide encrypted credential store. systemd-creds(1) decrypts files
in this directory at unit-start time and exposes them as plaintext files
under ``$CREDENTIALS_DIRECTORY`` to the running service. The directory
is host-keyed via TPM2 or ``/var/lib/systemd/credential.secret``; an
attacker who lifts the encrypted blob off the disk image cannot decrypt
it on another machine."""

KNOWN_CREDENTIALS: tuple[tuple[str, str], ...] = (
    ("github-webhook-secret", "GitHub webhook HMAC secret (load-bearing for the listener)."),
    ("alertmanager-hmac", "Alertmanager / watchdog HMAC secret (optional)."),
    ("broadcast-token", "Broadcast subscribe-time bearer token (optional)."),
)
"""Credentials the daemon stack reads. The first element is the
credential name (used in ``LoadCredentialEncrypted=<name>:<path>`` and as
the filename under ``$CREDENTIALS_DIRECTORY``). New credentials must be
added here AND to the matching unit file's ``LoadCredentialEncrypted=``
line."""

LAUNCHD_LABEL_PREFIX = "dev.waitbus."
"""Reverse-DNS Label prefix used on every shipped LaunchAgent plist. The
`dev.` TLD is the launchd convention for development / unsigned tools."""

_LAUNCHD_PLIST_NAME_RE = re.compile(r"^dev\.waitbus\.[a-z][a-z0-9-]*\.plist$")
"""Anchored regex used to identify wheel-owned plists in
``~/Library/LaunchAgents/``. Mirrors ``_UNIT_NAME_RE`` for systemd."""


# Units that hold open file descriptors inside EVENTS_DIR (the SQLite DB,
# the doorbell socket). They must be stopped before `shutil.move` walks
# the tree, otherwise the daemons keep writing to the legacy inodes
# while their parent dir moves out from under them.
_MIGRATION_DAEMON_UNITS: tuple[str, ...] = (
    "waitbus-listener",
    "waitbus-broadcast",
    "waitbus-etag-poll",
)


# ---------------------------------------------------------------------------
# install-time helpers
# ---------------------------------------------------------------------------


def _share_systemd_user_dir() -> Path:
    """Resolve the wheel's `share/systemd/user/` install location for
    this Python interpreter. For `pip install --user` this is
    `~/.local/share/systemd/user/`. For `uv tool install` / `pipx
    install` it's the isolated tool prefix's `share/systemd/user/`,
    which is NOT on systemd's load path — `install-systemd` copies
    out of here to `~/.config/systemd/user/`."""
    data_root = Path(sysconfig.get_path("data"))
    return data_root / "share" / "systemd" / "user"


def _systemd_user_target_dir() -> Path:
    """Operator-visible systemd-user dir where `install-systemd`
    materialises the units. systemd ALWAYS reads from
    `~/.config/systemd/user/` per `systemd.unit(5)`."""
    return Path.home() / ".config" / "systemd" / "user"


def _read_manifest(share_dir: Path) -> list[str]:
    """Return the canonical unit filenames from
    `share/systemd/user/waitbus.MANIFEST.txt`. Used by
    `install-systemd` to find the set of units this wheel installs,
    and by `install-systemd --sync` to find orphans."""
    manifest_path = share_dir / "waitbus.MANIFEST.txt"
    if not manifest_path.exists():
        return []
    units = []
    for line in manifest_path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        units.append(stripped)
    return units


def _share_launchd_dir() -> Path:
    """Resolve the wheel's `share/launchd/` install location for this
    interpreter. Same shape as ``_share_systemd_user_dir`` — under `uv
    tool install` / `pipx install` it lands inside the isolated tool
    prefix, which is NOT on launchd's load path; ``install-launchd``
    copies out of here into ``~/Library/LaunchAgents/`` where launchd
    looks (see launchd.plist(5)).
    """
    data_root = Path(sysconfig.get_path("data"))
    return data_root / "share" / "launchd"


def _launchd_target_dir() -> Path:
    """Operator-visible LaunchAgents directory where ``install-launchd``
    materialises the resolved plists. Per ``launchd.plist(5)`` and
    Apple's developer documentation, gui/$UID agents load from
    ``~/Library/LaunchAgents/``."""
    return Path.home() / "Library" / "LaunchAgents"


def _launchd_log_dir() -> Path:
    """Default per-user log directory for waitbus LaunchAgents.

    macOS convention is ``~/Library/Logs/<app>/``; the directory is
    created at install time so the plists' StandardOutPath /
    StandardErrorPath open successfully on first launch.
    """
    return Path.home() / "Library" / "Logs" / "waitbus"


def _resolve_launchd_placeholders(template: str, *, bin_dir: Path, log_dir: Path, runtime_dir: Path) -> str:
    """Substitute ``__BIN_DIR__`` / ``__LOG_DIR__`` / ``__RUNTIME_DIR__``
    in a plist template. The placeholders are deliberately not real
    plist syntax so a partially-resolved file never validates as a
    well-formed LaunchAgent — that fails loud at install time rather
    than silently launching a process from the wrong path.
    """
    return (
        template.replace("__BIN_DIR__", str(bin_dir))
        .replace("__LOG_DIR__", str(log_dir))
        .replace("__RUNTIME_DIR__", str(runtime_dir))
    )


def _resolve_launchd_bin_dir() -> Path:
    """Return the directory where the waitbus console-script lives.

    Resolves via ``shutil.which`` first (handles uv tool install /
    pipx install / pip install --user uniformly). Falls back to
    ``sysconfig.get_path('scripts')`` when ``waitbus`` is not yet on
    PATH — useful for the test-fixture case where the install location
    is constructed but PATH has not been augmented.
    """
    found = shutil.which("waitbus")
    if found:
        return Path(found).parent
    return Path(sysconfig.get_path("scripts"))


def _apply_unit_change(
    action: str,
    src: Path,
    dst: Path,
    dry_run: bool,
) -> None:
    """Apply one filesystem mutation against a systemd unit.

    Args:
        action: "copy" (src -> dst), "remove" (dst), or "disable+remove"
            (systemctl disable --now <dst.name>; rm dst).
        src: source path for copy actions. Ignored for remove actions.
        dst: destination (for copy) or victim (for remove) path.
        dry_run: when True, prints the intended mutation without
            touching the filesystem or invoking systemctl.
    """
    if action == "copy":
        if dry_run:
            typer.echo(f"  Would copy: {src} -> {dst}")
            return
        shutil.copy2(src, dst)
        typer.echo(f"  Copied: {dst}")
        return
    if action == "disable+remove":
        if dry_run:
            typer.echo(f"  Would stop + disable + remove: {dst.name}")
            return
        subprocess.run(
            ["systemctl", "--user", "disable", "--now", dst.name],
            capture_output=True,
            text=True,
            timeout=15,
        )
        dst.unlink(missing_ok=True)
        typer.echo(f"  Removed: {dst}")
        return
    raise ValueError(f"unknown action: {action!r}")


def _enable_units(units: list[str], *, dry_run: bool) -> None:
    """Run `systemctl --user enable --now` for the units that declare [Install].

    Several waitbus units intentionally omit [Install] (e.g.
    waitbus-broadcast.service) because their lifecycle is driven by
    socket activation; enabling those would form boot-ordering cycles.
    The hard-coded set below names every unit that DOES declare
    [Install]; new units must be added here when introduced.
    """
    enableable = (
        "waitbus-listener.service",
        "waitbus-broadcast.socket",
        "waitbus-watchdog.timer",
        "waitbus-etag-poll.timer",
    )
    for unit in enableable:
        if unit not in units:
            continue
        if dry_run:
            typer.echo(f"  Would run: systemctl --user enable --now {unit}")
            continue
        proc = subprocess.run(
            ["systemctl", "--user", "enable", "--now", unit],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if proc.returncode != 0:
            typer.secho(
                f"  enable --now {unit} failed: {proc.stderr.strip()}",
                fg=typer.colors.YELLOW,
                err=True,
            )
        else:
            typer.echo(f"  Enabled + started: {unit}")


def _confirm_orphan_removal(*, force: bool) -> None:
    """Block on operator confirmation before removing orphan units.

    Returns silently when removal is approved (either via --force or
    the operator's "yes" at a TTY prompt). Raises typer.Exit otherwise.
    """
    if force:
        return
    if not sys.stdin.isatty():
        typer.secho(
            "  --sync requires --force when not running on a TTY. Re-run with --force to remove the orphans.",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=1)
    try:
        confirmed = typer.confirm("  Remove these orphan units?", default=False)
    except KeyboardInterrupt:
        typer.echo("  aborted by user", err=True)
        raise typer.Abort() from None
    if not confirmed:
        typer.echo("  --sync cancelled by user.")
        raise typer.Exit(code=0)


def _sync_orphans(
    target_dir: Path,
    canonical: set[str],
    *,
    force: bool,
    dry_run: bool,
) -> None:
    """Stop, disable, and remove units no longer in the wheel manifest.

    Orphans are identified via the anchored `_UNIT_NAME_RE` regex, NOT
    fnmatch — so an operator-created `waitbus-notes.txt` is left
    untouched. `--dry-run` prints the diff and exits 0 without prompting.
    """
    orphans = [
        p.name
        for p in target_dir.glob("waitbus-*")
        if p.is_file() and _UNIT_NAME_RE.match(p.name) and p.name not in canonical
    ]
    if not orphans:
        typer.echo("  --sync: no orphans to remove.")
        return
    typer.echo(f"  --sync: {len(orphans)} orphan unit(s) detected:")
    for name in orphans:
        typer.echo(f"    {target_dir / name}")
    if dry_run:
        for name in orphans:
            _apply_unit_change("disable+remove", target_dir / name, target_dir / name, dry_run=True)
        return
    _confirm_orphan_removal(force=force)
    for name in orphans:
        _apply_unit_change("disable+remove", target_dir / name, target_dir / name, dry_run=False)
    subprocess.run(["systemctl", "--user", "daemon-reload"], capture_output=True, text=True, timeout=10)


def _apply_launchd_plist(
    src: Path,
    dst: Path,
    *,
    bin_dir: Path,
    log_dir: Path,
    runtime_dir: Path,
    dry_run: bool,
) -> None:
    """Resolve placeholders in ``src`` and write the result to ``dst``.

    The plist is validated post-write by parsing it with
    ``plistlib.loads`` — a malformed substitution (operator-set path
    containing characters that break XML) fails loud at install time
    rather than at agent-load time. Validation is skipped under
    ``--dry-run``.
    """
    import plistlib  # stdlib, deferred so the CLI import stays cheap

    template = src.read_text()
    resolved = _resolve_launchd_placeholders(
        template,
        bin_dir=bin_dir,
        log_dir=log_dir,
        runtime_dir=runtime_dir,
    )
    if dry_run:
        typer.echo(f"  Would write: {dst}")
        # Validate the in-memory bytes even on dry-run so a placeholder
        # bug surfaces without touching the filesystem.
        try:
            plistlib.loads(resolved.encode("utf-8"))
        except Exception as exc:  # plistlib raises a tree of subclasses
            typer.secho(
                f"  ERROR: resolved plist would be malformed: {exc}",
                fg=typer.colors.RED,
                err=True,
            )
            raise typer.Exit(code=2) from exc
        return
    dst.write_text(resolved)
    try:
        plistlib.loads(resolved.encode("utf-8"))
    except Exception as exc:
        typer.secho(
            f"  ERROR: wrote malformed plist to {dst}: {exc}",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2) from exc
    typer.echo(f"  Wrote: {dst}")


def _launchctl_bootstrap(plist_path: Path, *, dry_run: bool) -> None:
    """Bootstrap a single LaunchAgent into the gui/$UID domain.

    Uses the modern ``launchctl bootstrap gui/$UID <plist>`` form, not
    the deprecated ``launchctl load -w``. If the agent is already
    loaded, ``bootout`` it first so the new plist contents take effect
    — re-bootstrapping a pre-existing label is a no-op on Apple's
    launchctl and silently ignores the new plist.
    """
    label = plist_path.stem  # filename without .plist
    target = f"gui/{os.getuid()}"
    if dry_run:
        typer.echo(f"  Would run: launchctl bootout {target}/{label} (if loaded)")
        typer.echo(f"  Would run: launchctl bootstrap {target} {plist_path}")
        return
    # Bootout the existing label if loaded (idempotent: a not-loaded
    # label exits non-zero but the next bootstrap will succeed).
    subprocess.run(
        ["launchctl", "bootout", f"{target}/{label}"],
        capture_output=True,
        text=True,
        timeout=15,
    )
    proc = subprocess.run(
        ["launchctl", "bootstrap", target, str(plist_path)],
        capture_output=True,
        text=True,
        timeout=15,
    )
    if proc.returncode != 0:
        typer.secho(
            f"  bootstrap {label} failed: {proc.stderr.strip() or proc.stdout.strip()}",
            fg=typer.colors.YELLOW,
            err=True,
        )
    else:
        typer.echo(f"  Bootstrapped: {label}")


def _sync_launchd_orphans(
    target_dir: Path,
    canonical: set[str],
    *,
    force: bool,
    dry_run: bool,
) -> None:
    """Bootout and remove plists no longer in the wheel manifest.

    Mirrors ``_sync_orphans`` for systemd: orphans are identified by
    the anchored ``_LAUNCHD_PLIST_NAME_RE`` regex so operator-created
    files in ``~/Library/LaunchAgents/`` are left untouched.
    """
    if not target_dir.exists():
        return
    orphans = [
        p.name
        for p in target_dir.glob("dev.waitbus.*")
        if p.is_file() and _LAUNCHD_PLIST_NAME_RE.match(p.name) and p.name not in canonical
    ]
    if not orphans:
        typer.echo("  --sync: no orphans to remove.")
        return
    typer.echo(f"  --sync: {len(orphans)} orphan plist(s) detected:")
    for name in orphans:
        typer.echo(f"    {target_dir / name}")
    if dry_run:
        for name in orphans:
            label = Path(name).stem
            typer.echo(f"  Would run: launchctl bootout gui/$UID/{label}")
            typer.echo(f"  Would remove: {target_dir / name}")
        return
    _confirm_orphan_removal(force=force)
    for name in orphans:
        plist_path = target_dir / name
        label = plist_path.stem
        subprocess.run(
            ["launchctl", "bootout", f"gui/{os.getuid()}/{label}"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        plist_path.unlink(missing_ok=True)
        typer.echo(f"  Removed: {plist_path}")


# ---------------------------------------------------------------------------
# health-check helpers (shared by init + doctor)
# ---------------------------------------------------------------------------


def _check_binaries() -> list[str]:
    """Return human-readable issue strings for every missing required binary."""
    issues: list[str] = []
    typer.echo("[binaries]")
    for binary in REQUIRED_BINS:
        if shutil.which(binary):
            typer.echo(f"  {binary:14} on PATH")
        else:
            typer.secho(f"  {binary:14} MISSING", fg=typer.colors.YELLOW, err=True)
            issues.append(f"binary {binary} not on PATH")
    typer.echo("")
    return issues


def _check_paths() -> list[str]:
    """Verify state directories and the SQLite DB are present."""
    issues: list[str] = []
    typer.echo("[paths]")
    for label, path in (
        ("EVENTS_DIR", _paths.state_dir()),
        ("CURSORS_DIR", _paths.cursors_dir()),
        ("db_path()", _paths.db_path()),
    ):
        if path.exists():
            typer.echo(f"  {label:11} present: {path}")
        else:
            typer.secho(f"  {label:11} MISSING: {path}", fg=typer.colors.YELLOW, err=True)
            issues.append(f"{label} missing — run `waitbus init`")
    typer.echo("")
    return issues


def _check_credentials() -> list[str]:
    """Verify the credential staging dir holds entries for the known names.

    Doctor runs as the operator, not as the daemon, so ``$CREDENTIALS_DIRECTORY``
    is not set; the check inspects the encrypted credential store on disk
    (``/etc/credstore.encrypted/waitbus.<name>.cred``) instead of
    decrypted values. Decrypt-time failures (TPM unsealing, host-key
    rotation) cannot be detected from the operator account.
    """
    issues: list[str] = []
    typer.echo("[credentials]")
    for name, _desc in KNOWN_CREDENTIALS:
        path = CREDSTORE_DIR / f"waitbus.{name}.cred"
        try:
            present = stat.S_ISREG(path.stat().st_mode)
        except FileNotFoundError:
            present = False
        except OSError as exc:
            # The system credstore is root-owned 0700 by design (systemd-creds),
            # so the operator account lacks search permission to stat individual
            # entries. Path.stat() raises PermissionError on every Python
            # version; Path.is_file() is the trap -- whether it swallows
            # the error and returns False or propagates it varies across
            # Python versions, so stat() explicitly and report the entry
            # as indeterminate rather than crashing doctor or
            # false-flagging the credential MISSING.
            typer.echo(f"  {name:32} indeterminate ({CREDSTORE_DIR} not operator-readable: {exc.strerror or exc})")
            continue
        if present:
            typer.echo(f"  {name:32} present at {path}")
        else:
            typer.secho(f"  {name:32} MISSING at {path}", fg=typer.colors.YELLOW, err=True)
            issues.append(f"credential {name} missing — run `waitbus install-credentials {name}`")
    typer.echo("")
    return issues


def _print_unit_diff(canonical: set[str], installed: set[str], target_dir: Path) -> None:
    """Print the per-unit installed/missing/orphan annotations."""
    for name in sorted(canonical):
        if name in installed:
            typer.echo(f"  {name:42} installed")
        else:
            typer.secho(f"  {name:42} MISSING in {target_dir}", fg=typer.colors.YELLOW, err=True)
    for name in sorted(installed - canonical):
        typer.secho(f"  {name:42} ORPHAN in {target_dir}", fg=typer.colors.YELLOW, err=True)


def _check_systemd() -> list[str]:
    """Verify shipped units are installed in ~/.config/systemd/user/."""
    typer.echo("[systemd]")
    share_dir = _share_systemd_user_dir()
    target_dir = _systemd_user_target_dir()
    canonical = set(_read_manifest(share_dir)) if share_dir.exists() else set()
    if not canonical:
        typer.echo(f"  (wheel's MANIFEST.txt not found at {share_dir}; install via `waitbus install-systemd`)")
        typer.echo("")
        return ["systemd units not installed — run `waitbus install-systemd`"]
    installed = {p.name for p in target_dir.glob("waitbus-*") if p.is_file() and _UNIT_NAME_RE.match(p.name)}
    _print_unit_diff(canonical, installed, target_dir)
    issues: list[str] = []
    missing = canonical - installed
    orphans = installed - canonical
    if missing:
        issues.append(f"systemd units missing ({len(missing)}) — run `waitbus install-systemd`")
    if orphans:
        issues.append(f"orphan systemd units ({len(orphans)}) — run `waitbus install-systemd --sync`")
    typer.echo("")
    return issues


def _check_launchd() -> list[str]:
    """Verify shipped plists are installed in ~/Library/LaunchAgents/."""
    typer.echo("[launchd]")
    share_dir = _share_launchd_dir()
    target_dir = _launchd_target_dir()
    manifest_path = share_dir / "MANIFEST.txt"
    if not manifest_path.exists():
        typer.echo(f"  (wheel's MANIFEST.txt not found at {manifest_path}; install via `waitbus install-launchd`)")
        typer.echo("")
        return ["launchd plists not installed — run `waitbus install-launchd`"]
    canonical = {
        line.strip()
        for line in manifest_path.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    }
    if not target_dir.exists():
        typer.secho(
            f"  LaunchAgents dir missing: {target_dir}",
            fg=typer.colors.YELLOW,
            err=True,
        )
        typer.echo("")
        return ["launchd plists not installed — run `waitbus install-launchd`"]
    installed = {
        p.name for p in target_dir.glob("dev.waitbus.*") if p.is_file() and _LAUNCHD_PLIST_NAME_RE.match(p.name)
    }
    _print_unit_diff(canonical, installed, target_dir)
    issues: list[str] = []
    missing = canonical - installed
    orphans = installed - canonical
    if missing:
        issues.append(f"launchd plists missing ({len(missing)}) — run `waitbus install-launchd`")
    if orphans:
        issues.append(f"orphan launchd plists ({len(orphans)}) — run `waitbus install-launchd --sync`")
    typer.echo("")
    return issues


def _check_config() -> list[str]:
    """Report the resolved config (config.toml path, prom_owner, prom_repo)."""
    from .. import _config, _paths

    cfg = _config.get_config()
    config_file = _paths.config_file()
    typer.echo("[config]")
    if config_file.exists():
        typer.echo(f"  Config file: {config_file}")
    else:
        typer.echo("  Config file: (not present — defaults apply)")
    typer.echo(f"  PROM_OWNER : {cfg.prom_owner}")
    typer.echo(f"  PROM_REPO  : {cfg.prom_repo}")
    typer.echo("")
    return []


def _check_metrics_endpoint() -> list[str]:
    """Probe the /metrics endpoint on the listener port."""
    import urllib.error
    import urllib.request

    from .. import _config

    cfg = _config.get_config()
    port = getattr(cfg, "listener_port", 9000)
    typer.echo("[metrics]")
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=2.0) as resp:
            if resp.status == 200:
                typer.echo("  /metrics reachable")
                typer.echo("")
                return []
            typer.secho(
                f"  /metrics returned HTTP {resp.status}",
                fg=typer.colors.YELLOW,
                err=True,
            )
            typer.echo("")
            return [f"/metrics returned HTTP {resp.status}"]
    except (urllib.error.URLError, OSError):
        typer.secho("  listener not serving", fg=typer.colors.YELLOW, err=True)
        typer.echo("")
        return ["listener not serving — run `waitbus listener serve`"]


def _check_config_validation() -> list[str]:
    """Validate config.toml via pydantic-settings."""
    from .. import _config

    typer.echo("[config-validation]")
    try:
        _config.CiStatusConfig()
        typer.echo("  config valid")
        typer.echo("")
        return []
    except Exception as exc:
        typer.secho(f"  config invalid: {exc}", fg=typer.colors.YELLOW, err=True)
        typer.echo("")
        return [f"config invalid: {exc}"]


# ---------------------------------------------------------------------------
# legacy state migration
# ---------------------------------------------------------------------------


def _migrate_legacy_state_if_needed() -> None:
    """Move legacy event data from the legacy state directory to the
    platformdirs state directory. No-op if there's no legacy data; refuses
    to clobber a populated new location. Idempotent: re-running `waitbus
    init` after a successful migration sees no legacy dir and proceeds normally.
    """
    legacy = Path.home() / ".claude" / "events"
    target = _paths.state_dir()
    if not (legacy.exists() and (legacy / "github.db").exists()):
        return
    if target.exists() and any(target.iterdir()):
        typer.secho(
            f"Both legacy {legacy} and target {target} contain data. Inspect both and remove one before re-running.",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2)
    # Stop daemons before moving their open files; capture_output to keep
    # the CLI surface quiet on a fresh machine where the units don't yet
    # exist (systemctl exits non-zero with a clear message but no actual
    # failure).
    for unit in _MIGRATION_DAEMON_UNITS:
        subprocess.run(
            ["systemctl", "--user", "stop", f"{unit}.service"],
            check=False,
            capture_output=True,
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(legacy), str(target))
    target.chmod(0o700)
    typer.echo(f"Migrated legacy event data: {legacy} -> {target}")


# ---------------------------------------------------------------------------
# credential value resolver
# ---------------------------------------------------------------------------


def _read_credential_value(
    *,
    inline: str | None,
    source_file: Path | None,
    name: str,
) -> str:
    """Resolve the credential's plaintext value from --value, --file, or stdin."""
    if inline is not None and source_file is not None:
        raise typer.BadParameter("--value and --file are mutually exclusive")
    if inline is not None:
        return inline
    if source_file is not None:
        try:
            return source_file.read_text(encoding="utf-8")
        except OSError as exc:
            raise typer.BadParameter(f"--file {source_file} unreadable: {exc}") from exc
    if sys.stdin.isatty():
        typer.echo(f"Reading {name} from stdin (Ctrl-D to end); use --value or --file to script.", err=True)
    return sys.stdin.read()


# ---------------------------------------------------------------------------
# colour policy
# ---------------------------------------------------------------------------


def use_colour() -> bool:
    """Colour only on a real TTY with NO_COLOR unset (https://no-color.org/).

    The single definition of the colour-output policy, shared by the demo,
    swarm-demo, and top renderers so the NO_COLOR / TTY contract lives in one place.
    """
    return sys.stdout.isatty() and os.environ.get("NO_COLOR") is None


# ---------------------------------------------------------------------------
# operator-facing error exit
# ---------------------------------------------------------------------------


def _render_table(
    rows: Sequence[Mapping[str, object]],
    columns: Sequence[str],
    *,
    use_colour: bool,
    row_style_fn: Callable[[Mapping[str, object]], str | None] | None = None,
) -> str:
    """Render ``rows`` as a fixed-width aligned table.

    Each row maps column-name to a value rendered via ``str()``. The
    column-width is the max of the column header length and the
    widest data value in that column.

    When ``use_colour`` is True the header is bolded. ``row_style_fn``
    is an optional per-row callback returning a typer colour name
    (``typer.colors.CYAN`` etc.) or ``None``; non-None values colour
    that row's full body. This keeps the renderer agnostic of
    per-verb colouring policy (``waitbus source list`` colours plugin
    rows; ``waitbus allowlist list`` colours nothing).

    Empty ``rows`` returns the empty string -- callers print their
    own "(no entries)" message rather than letting the renderer
    invent one.
    """
    if not rows:
        return ""

    widths: dict[str, int] = {col: len(col) for col in columns}
    for row in rows:
        for col in columns:
            widths[col] = max(widths[col], len(str(row.get(col, ""))))

    def _fmt(values: Sequence[str]) -> str:
        return "  ".join(v.ljust(widths[col]) for col, v in zip(columns, values, strict=True))

    lines: list[str] = []
    header = _fmt(list(columns))
    separator = "  ".join("-" * widths[col] for col in columns)
    lines.append(typer.style(header, bold=True) if use_colour else header)
    lines.append(separator)
    for row in rows:
        line = _fmt([str(row.get(col, "")) for col in columns])
        if use_colour and row_style_fn is not None:
            fg = row_style_fn(row)
            if fg is not None:
                line = typer.style(line, fg=fg)
        lines.append(line)
    return "\n".join(lines)


def _exit_with_error(message: str, *, hint: str | None = None, code: int = 2) -> NoReturn:
    """Print an operator-facing error (and optional hint) to stderr, then exit."""
    print(f"error: {message}", file=sys.stderr)
    if hint:
        print(f"hint: {hint}", file=sys.stderr)
    raise typer.Exit(code)


def _ensure_plugin_discovery_for_cli() -> None:
    """Populate the plugin registry in a fresh CLI process; warn on policy failures.

    The waitbus broadcast / listener daemons call
    :func:`~waitbus.sources.discover_plugins_once` at startup so
    the in-process registry knows about every installed plugin. CLI
    verbs run in a separate, short-lived Python process where that
    call has NOT yet happened -- without an explicit discovery call,
    ``known_sources()`` returns only the built-ins and plugin sources
    are invisible to ``waitbus source list``, ``source show``, and
    ``source verify``.

    This helper makes the discovery call idempotent across multiple
    verbs in the same process (``discover_plugins_once`` itself is a
    no-op on the second call). Policy failures
    (``PluginContractError`` / ``PluginShadowError`` /
    ``PluginVersionMismatchError`` /
    ``PluginDuplicateRegistrationError``) are aggregated into an
    ``ExceptionGroup`` by the registry; we catch the group, print a
    structured WARN per failure to stderr, and continue with the
    plugins that DID register. CLI verbs that operate purely on the
    registry view (``source list`` / ``show``) still surface the
    successfully-registered plugins; verify-shaped verbs treat the
    listed-but-not-registered case as "unknown source" per their own
    exit-code contract.

    Verification failures (``AttestationVerificationError``) are
    already caught by ``_verify_publisher`` inside the registry and
    surfaced via the per-plugin row's ``signature-status`` column;
    they do NOT propagate here.
    """
    from ..sources._registry import discover_plugins_once

    try:
        discover_plugins_once()
    except ExceptionGroup as group:
        print(
            f"warning: {len(group.exceptions)} plugin source(s) rejected during discovery:",
            file=sys.stderr,
        )
        for failure in group.exceptions:
            print(f"  - {type(failure).__name__}: {failure}", file=sys.stderr)
        print(
            "continuing with successfully-registered plugins; "
            "fix the underlying issues (or `waitbus allowlist remove <name>` "
            "for an intentional vendor change) before next daemon restart.",
            file=sys.stderr,
        )


# ---------------------------------------------------------------------------
# typer process-control adapter
# ---------------------------------------------------------------------------


def run_typer_app(app: typer.Typer, argv: list[str] | None) -> int:
    """Invoke a Typer app with standalone_mode=False and map exits to int.

    The subscriber CLIs (broadcast_tap, replay, wait) share this
    boilerplate to convert ``typer.Exit`` into a process return code.
    Factored here -- on the CLI side, next to ``_exit_with_error`` --
    so the engine module (``_broadcast_sub``) carries NO ``typer``
    surface and stays a pure typed-API module the open-core boundary
    keeps relay/account/network-free.

    With ``standalone_mode=False`` a ``typer.Exit`` bubbles up as a
    bare RuntimeError subclass (not SystemExit); the function returns 0
    for any non-integer result.
    """
    result = app(args=argv, standalone_mode=False)
    return int(result) if isinstance(result, int) else 0
