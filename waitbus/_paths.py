"""Path-resolution factories for the waitbus event store.

Resolution precedence (applies to all three directory roots):

1. Environment-variable override (must be absolute) — operator-controlled.
2. ``platformdirs`` default for the platform:
   - Linux (state): ``~/.local/state/waitbus/`` (honours ``XDG_STATE_HOME``).
   - Linux (runtime): ``/run/user/$UID/waitbus/`` (honours ``XDG_RUNTIME_DIR``).
   - Linux (config): ``~/.config/waitbus/`` (honours ``XDG_CONFIG_HOME``).
   - macOS (state): ``~/Library/Application Support/waitbus/``.
   - macOS (runtime): ``<tempfile.gettempdir()>/waitbus-<uid>/`` (macOS's
     ``user_runtime_dir`` is an evictable-cache path — wrong for sockets).
   - macOS (config): ``~/Library/Preferences/waitbus/``.

Env-override safety: relative paths are rejected immediately with a clear
``RuntimeError``. A relative override resolves against the daemon's working
directory, which differs per systemd unit or shell context — splitting writes
between daemons silently. Absolute path (starting with ``/`` or ``~``) required.

All path resolution re-reads the env vars on every call (no caching);
tests that need to override paths set the relevant ``WAITBUS_*_DIR`` env
var and the next call observes the change directly.

``ensure_state_dirs()`` creates the state, cursors, and runtime directories
with 0700 permissions. Call it once at daemon/CLI startup; library imports do
NOT trigger directory creation.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import platformdirs

_APPNAME = "waitbus"


def _validate_absolute(env_var: str, value: str) -> Path:
    """Coerce a WAITBUS_*_DIR env-override value to an absolute Path or raise.

    Relative env-override values are an operator footgun: they resolve
    against the daemon's working directory (which varies by systemd unit
    or shell context), causing the listener and broadcast to disagree on
    the directory location. Reject early with a clear error message.
    """
    raw = Path(value)
    # CPython >=3.11 raises RuntimeError("Could not determine home directory.")
    # from .expanduser() in two distinct failure modes that operators need to
    # distinguish:
    #
    #   * literal ``~`` or ``~/...`` with HOME unset (systemd user units
    #     without ``Environment=HOME=...``; macOS launchd contexts without a
    #     logged-in session)
    #   * ``~unknownuser`` referring to a passwd entry that does not exist
    #
    # Catch the RuntimeError and re-raise with the operator-targeted hint.
    try:
        expanded = raw.expanduser()
    except RuntimeError as exc:
        if value == "~" or value.startswith("~/"):
            raise RuntimeError(
                f"{env_var}={value!r}: HOME is unset so the leading '~' could "
                "not be expanded; pass a literal absolute path or set HOME in "
                "the unit environment."
            ) from exc
        raise RuntimeError(
            f"{env_var}={value!r}: user-prefix expansion failed (unknown user?); use a literal absolute path."
        ) from exc
    if not expanded.is_absolute():
        raise RuntimeError(
            f"{env_var}={value!r} must be an absolute path "
            "(starting with '/' or '~'); relative paths split writes "
            "between daemons that resolve them under different CWDs."
        )
    return expanded


def state_dir() -> Path:
    """Resolve the state dir (events DB, etag state, watched repos, cursors).

    Honors ``WAITBUS_STATE_DIR``; falls back to
    ``platformdirs.user_state_path`` on Linux or
    ``~/Library/Application Support/waitbus`` on macOS.
    """
    env = os.environ.get("WAITBUS_STATE_DIR")
    if env:
        return _validate_absolute("WAITBUS_STATE_DIR", env)
    return Path(platformdirs.user_state_path(_APPNAME, appauthor=False))


def runtime_dir() -> Path:
    """Resolve the runtime dir (AF_UNIX sockets: broadcast, doorbell).

    Honors ``WAITBUS_RUNTIME_DIR``; on Linux falls back to
    ``$XDG_RUNTIME_DIR`` via platformdirs; on macOS falls back to
    ``tempfile.gettempdir()/waitbus-<uid>`` since macOS's
    ``user_runtime_dir`` is an evictable cache unsuitable for sockets.
    """
    env = os.environ.get("WAITBUS_RUNTIME_DIR")
    if env:
        return _validate_absolute("WAITBUS_RUNTIME_DIR", env)
    if sys.platform == "darwin":
        return Path(tempfile.gettempdir()) / f"{_APPNAME}-{os.getuid()}"
    return Path(platformdirs.user_runtime_path(_APPNAME, appauthor=False))


def config_dir() -> Path:
    """Resolve the config dir (config.toml, mcp filter file).

    Honors ``WAITBUS_CONFIG_DIR``; falls back to
    ``platformdirs.user_config_path``.
    """
    env = os.environ.get("WAITBUS_CONFIG_DIR")
    if env:
        return _validate_absolute("WAITBUS_CONFIG_DIR", env)
    return Path(platformdirs.user_config_path(_APPNAME, appauthor=False))


# ---------------------------------------------------------------------------
# Public path helpers derived from the cached directory factories.
# Call these functions; do not read the module-level constants in new code.
# ---------------------------------------------------------------------------


def db_path() -> Path:
    """Absolute path to the SQLite event database."""
    return state_dir() / "github.db"


def resolve_db_path(override: Path | None) -> Path:
    """Return the explicit DB path, or the platform default when None."""
    return override if override is not None else db_path()


def watched_repos() -> Path:
    """Absolute path to the watched-repos manifest."""
    return state_dir() / "watched_repos.txt"


def etag_state() -> Path:
    """Absolute path to the etag-state JSON file."""
    return state_dir() / "etag_state.json"


def cursors_dir() -> Path:
    """Absolute path to the per-repo cursor directory."""
    return state_dir() / "cursors"


def broadcast_socket() -> Path:
    """Absolute path to the broadcast AF_UNIX socket."""
    return runtime_dir() / "broadcast.sock"


def doorbell_socket() -> Path:
    """Absolute path to the doorbell AF_UNIX socket."""
    return runtime_dir() / "doorbell.sock"


def config_file() -> Path:
    """Absolute path to the operator config.toml.

    All operator-edited settings live here; the legacy
    ``filters.json`` file is retired in favour of a ``[mcp]`` section
    in this TOML file.
    """
    return config_dir() / "config.toml"


def ensure_state_dirs() -> None:
    """Create state, cursors, and runtime directories with 0700 perms.

    Idempotent. Callers invoke this once at daemon/CLI startup before
    opening files inside the resolved dirs. Library imports do NOT
    trigger directory creation.

    ``Path.mkdir(mode=..., exist_ok=True)`` is a no-op on the mode when
    the directory already exists, so an explicit ``chmod`` follows each
    ``mkdir`` call. This guarantees 0700 even when the directories were
    created by an earlier process with a more permissive umask.
    """
    for path in (state_dir(), cursors_dir(), runtime_dir()):
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        path.chmod(0o700)
