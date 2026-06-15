"""Declarative TOML config + publisher-bound TOFU allowlist for plugin sources.

Two files live under ``$XDG_CONFIG_HOME/waitbus/`` (platformdirs-resolved
on macOS so the layout is conventional there too):

* ``config.toml`` — the operator's declared policy. Shape::

      [plugins]
      autoload = true        # bool, default true
      allow = []             # list[str]; if non-empty + autoload=false,
                             # only these names are loaded
      deny = []              # list[str]; always-applied blocklist

* ``plugins.allowlist.toml`` — the TOFU-pinned publisher bindings.
  Shape::

      [[source]]
      name = "circleci"
      publisher_kind = "GitHub"
      publisher_identity = "astrogilda/waitbus-circleci @ .github/workflows/release.yml"
      first_pinned_at = "2026-05-20T09:42:11Z"

Both files are optional. If absent, waitbus uses the default policy
(``autoload=true``, empty allow/deny, no publisher pins). The
emergency env override ``WAITBUS_DISABLE_SOURCE_AUTOLOAD=1`` takes
precedence over the config file.

The split is deliberate. ``config.toml`` is the operator's *declared*
policy (what they typed); ``plugins.allowlist.toml`` is the runtime-
learned *publisher bindings* (TOFU-recorded on first-install). Keeping
them separate avoids conflating operator intent with discovered runtime
state, mirroring how SSH separates ``ssh_config`` from ``known_hosts``.

Both files are TOML rather than rows in the waitbus SQLite store because
the allowlist is operator-auditable policy, not application state: ``cat
~/.config/waitbus/plugins.allowlist.toml`` MUST show what is trusted,
exactly as ``cat ~/.ssh/known_hosts`` does. SQLite blobs cannot be
meaningfully diffed or version-controlled.
"""

from __future__ import annotations

import contextlib
import fcntl
import os
import tempfile
import tomllib
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

import platformdirs

_CONFIG_FILENAME: Final[str] = "config.toml"
_ALLOWLIST_FILENAME: Final[str] = "plugins.allowlist.toml"
_ALLOWLIST_LOCK_FILENAME: Final[str] = ".plugins.allowlist.lock"
_DISABLE_AUTOLOAD_ENV: Final[str] = "WAITBUS_DISABLE_SOURCE_AUTOLOAD"
_ENABLED_PLUGINS_ENV: Final[str] = "WAITBUS_PLUGINS"


class AllowlistCorruptError(RuntimeError):
    """The publisher-pin allowlist file is unreadable or malformed.

    Raised by :func:`load_allowlist` when ``plugins.allowlist.toml``
    fails to parse, has the wrong shape, or cannot be opened. Callers
    inside the daemon registration path (`_enforce_tofu` in the
    registry) catch this and log a structured warning rather than
    propagating, so a single corrupt allowlist file does not kill
    daemon startup. Operators self-heal via ``waitbus allowlist repair``
    or by hand-editing the file. The typed exception lets the
    distinction between "file is corrupt" and "an arbitrary
    RuntimeError leaked from elsewhere" be preserved at the catch
    site.
    """


@dataclass(frozen=True)
class PluginPolicy:
    """Operator-declared plugin-load policy.

    Loaded from ``$XDG_CONFIG_HOME/waitbus/config.toml``'s ``[plugins]``
    table, with env-var overrides applied. Frozen so it can be passed
    around safely.

    Attributes:
        autoload: When True, waitbus walks the
            ``waitbus.sources.v1`` entry-point group at daemon startup
            and registers every discovered plugin (subject to ``deny``
            and to publisher-bound TOFU). When False, only names in
            ``allow`` are loaded (a strict allowlist mode).
        allow: When ``autoload=False``, only entry-point names in
            this list are loaded. Ignored when ``autoload=True``.
        deny: Always-applied blocklist. A name in ``deny`` is never
            loaded regardless of ``autoload`` or ``allow``.
    """

    autoload: bool = True
    allow: tuple[str, ...] = ()
    deny: tuple[str, ...] = ()


@dataclass(frozen=True)
class PublisherPin:
    """One TOFU-pinned publisher binding for a source name.

    A line in ``plugins.allowlist.toml``. On first install of a plugin
    that registers source name ``N``, waitbus records the plugin's
    verified PEP 740 publisher identity here. Subsequent installs
    (version upgrades, etc.) from the same publisher silently pass;
    a different publisher trying to register the same name hard-fails
    with :class:`~._protocol.PluginShadowError`.

    Attributes:
        name: The canonical source name (e.g. ``"circleci"``).
        publisher_kind: The Trusted-Publisher kind
            (``"GitHub"``, ``"GitLab"``, ``"Google"``, ``"ActiveState"``).
        publisher_identity: The canonical identity string -- e.g.
            ``"astrogilda/waitbus-circleci @ .github/workflows/release.yml"``.
        first_pinned_at: ISO 8601 UTC timestamp of the first
            successful registration. Stored for operator auditing
            (the equivalent of ``known_hosts``' "added by you on X").
    """

    name: str
    publisher_kind: str
    publisher_identity: str
    first_pinned_at: str


@dataclass(frozen=True)
class Allowlist:
    """The complete publisher-pin set, indexed by source name.

    Attributes:
        pins: Mapping from source name to its :class:`PublisherPin`.
    """

    pins: dict[str, PublisherPin] = field(default_factory=dict)

    def for_source(self, name: str) -> PublisherPin | None:
        """Return the pin for ``name`` if one exists, else ``None``."""
        return self.pins.get(name)


def config_dir() -> Path:
    """Return the waitbus config directory.

    Uses ``platformdirs`` so the layout matches the platform
    convention: ``~/.config/waitbus/`` on XDG-compliant systems
    (Linux), ``~/Library/Application Support/waitbus/`` on macOS. The
    directory is NOT created here -- callers that need to write
    (e.g., the allowlist updater) call :func:`ensure_config_dir`.
    """
    return Path(platformdirs.user_config_dir("waitbus", appauthor=False))


def ensure_config_dir() -> Path:
    """Create the waitbus config directory (idempotent) and return it."""
    path = config_dir()
    path.mkdir(parents=True, exist_ok=True)
    return path


def load_plugin_policy() -> PluginPolicy:
    """Load the operator's plugin policy from ``config.toml``.

    Returns the default policy (``autoload=True``, empty allow/deny)
    if the file does not exist or contains no ``[plugins]`` table.
    Applies env-var overrides: ``WAITBUS_DISABLE_SOURCE_AUTOLOAD=1``
    forces ``autoload=False``, and ``WAITBUS_PLUGINS=a,b,c`` overrides
    the allow list (the env-var form mirrors pytest's
    ``PYTEST_PLUGINS``).
    """
    config_path = config_dir() / _CONFIG_FILENAME
    autoload = True
    allow: tuple[str, ...] = ()
    deny: tuple[str, ...] = ()

    if config_path.exists():
        try:
            data = tomllib.loads(config_path.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError) as exc:
            raise RuntimeError(
                f"failed to parse {config_path}: {exc}; fix the file or remove it to use defaults"
            ) from exc
        plugins_section = data.get("plugins", {})
        if isinstance(plugins_section, dict):
            autoload = bool(plugins_section.get("autoload", True))
            raw_allow = plugins_section.get("allow", [])
            raw_deny = plugins_section.get("deny", [])
            if isinstance(raw_allow, list):
                allow = tuple(str(x) for x in raw_allow)
            if isinstance(raw_deny, list):
                deny = tuple(str(x) for x in raw_deny)

    if os.environ.get(_DISABLE_AUTOLOAD_ENV):
        autoload = False
    env_plugins = os.environ.get(_ENABLED_PLUGINS_ENV)
    if env_plugins is not None:
        allow = tuple(name.strip() for name in env_plugins.split(",") if name.strip())

    return PluginPolicy(autoload=autoload, allow=allow, deny=deny)


@contextmanager
def _allowlist_lock() -> Iterator[None]:
    """Hold an exclusive advisory lock on a sibling lockfile.

    Wraps the read-then-write critical section in
    :func:`append_publisher_pin` and :func:`remove_publisher_pin` so a
    concurrent ``waitbus allowlist add`` (operator CLI) and ``_enforce_tofu``
    (daemon registration path) cannot interleave their load + modify +
    write sequences and lose updates. The lock target is a sibling
    file ``.plugins.allowlist.lock`` rather than the allowlist itself
    because ``os.replace`` would invalidate a file lock held on the
    target path (the new file's i-node has no lock).

    POSIX advisory ``flock`` is process-scoped and released on close,
    so a daemon crash mid-write does not leak a lock file beyond
    process lifetime. The lockfile itself is created if absent (mode
    0600); long-lived readers (e.g., ``waitbus allowlist list``) take
    a shared lock to coexist with concurrent writers without blocking.
    """
    dir_path = ensure_config_dir()
    lock_path = dir_path / _ALLOWLIST_LOCK_FILENAME
    # ``os.O_CREAT | os.O_RDWR`` so we can open the file even if it
    # does not yet exist; ``0o600`` matches the conservative posture
    # of the allowlist itself.
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def load_allowlist() -> Allowlist:
    """Load the publisher-pin allowlist from ``plugins.allowlist.toml``.

    Returns an empty :class:`Allowlist` if the file does not exist
    (TOFU operates from a clean slate). Raises
    :class:`AllowlistCorruptError` on malformed TOML or unexpected
    shape so operators see allowlist corruption clearly; the
    `_enforce_tofu` path in the registry catches this and treats
    the situation as untrusted-all rather than killing the daemon.
    """
    path = config_dir() / _ALLOWLIST_FILENAME
    if not path.exists():
        return Allowlist(pins={})
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise AllowlistCorruptError(
            f"failed to parse {path}: {exc}; the publisher allowlist is corrupt. "
            "Inspect or rebuild via `waitbus allowlist repair`."
        ) from exc

    pins: dict[str, PublisherPin] = {}
    raw_sources = data.get("source", [])
    if not isinstance(raw_sources, list):
        raise AllowlistCorruptError(f"{path}: expected ``[[source]]`` array, got {type(raw_sources).__name__}")
    for raw in raw_sources:
        if not isinstance(raw, dict):
            continue
        name = str(raw.get("name", ""))
        if not name:
            continue
        pins[name] = PublisherPin(
            name=name,
            publisher_kind=str(raw.get("publisher_kind", "unknown")),
            publisher_identity=str(raw.get("publisher_identity", "unknown")),
            first_pinned_at=str(raw.get("first_pinned_at", "")),
        )
    return Allowlist(pins=pins)


def append_publisher_pin(name: str, publisher_kind: str, publisher_identity: str) -> PublisherPin:
    """Record a new TOFU pin for ``name``, persisting it atomically.

    Idempotent: if ``name`` is already pinned to the same
    ``(publisher_kind, publisher_identity)``, returns the existing
    pin unchanged. If pinned to a different publisher, raises
    ``ValueError`` (the caller is responsible for handling the
    typed shadow error -- this function is the persistence seam,
    not the policy seam).

    Holds an exclusive advisory file lock for the read + check + write
    sequence so a concurrent CLI ``allowlist add`` and a daemon
    ``_enforce_tofu`` writer cannot lose updates.
    """
    with _allowlist_lock():
        existing = load_allowlist()
        prior = existing.for_source(name)
        if prior is not None:
            if prior.publisher_kind == publisher_kind and prior.publisher_identity == publisher_identity:
                return prior
            raise ValueError(
                f"refusing to overwrite pin for {name!r}: "
                f"previously {prior.publisher_kind}:{prior.publisher_identity!r}, "
                f"now requested {publisher_kind}:{publisher_identity!r}. "
                "Run `waitbus allowlist remove` first if this rebinding is intentional."
            )

        new_pin = PublisherPin(
            name=name,
            publisher_kind=publisher_kind,
            publisher_identity=publisher_identity,
            first_pinned_at=datetime.now(UTC).isoformat(timespec="seconds"),
        )
        pins = dict(existing.pins)
        pins[name] = new_pin
        _write_allowlist(Allowlist(pins=pins))
        return new_pin


def remove_publisher_pin(name: str) -> bool:
    """Remove the TOFU pin for ``name``; return True if removed, False if absent.

    Persists the updated allowlist atomically via tempfile + fsync +
    ``os.replace`` (same path as :func:`append_publisher_pin`). Holds
    the exclusive advisory file lock for the read + write sequence.
    """
    with _allowlist_lock():
        existing = load_allowlist()
        if name not in existing.pins:
            return False
        pins = {k: v for k, v in existing.pins.items() if k != name}
        _write_allowlist(Allowlist(pins=pins))
        return True


def _render_allowlist(allowlist: Allowlist) -> str:
    """Render an :class:`Allowlist` to a TOML string.

    tomllib has no encoder (per the stdlib design); the
    allowlist file format is narrow enough that an explicit writer
    is clearer than pulling in `tomli_w` as a dependency. Extracted
    so :func:`_write_allowlist` and the ``allowlist repair`` verb
    can share the format.
    """
    lines: list[str] = [
        "# waitbus plugin publisher allowlist (TOFU-pinned).\n",
        "# Managed by `waitbus allowlist add|remove|list`. Operators MAY\n",
        "# edit by hand; the format is stable.\n\n",
    ]
    for name in sorted(allowlist.pins):
        pin = allowlist.pins[name]
        lines.append("[[source]]\n")
        lines.append(f'name = "{_escape(pin.name)}"\n')
        lines.append(f'publisher_kind = "{_escape(pin.publisher_kind)}"\n')
        lines.append(f'publisher_identity = "{_escape(pin.publisher_identity)}"\n')
        lines.append(f'first_pinned_at = "{_escape(pin.first_pinned_at)}"\n\n')
    return "".join(lines)


def _write_allowlist(allowlist: Allowlist) -> None:
    """Persist ``allowlist`` atomically with mode 0600 and crash durability.

    The atomic-write recipe:

    1. ``tempfile.NamedTemporaryFile`` in the same directory so the
       eventual ``os.replace`` is on the same filesystem (POSIX-atomic).
       The random suffix avoids the collision race the previous fixed
       ``.tmp`` suffix had under concurrent writers.
    2. ``os.fchmod`` to 0600 BEFORE the first byte is written. The
       prior implementation chmod'd AFTER ``write_text``, leaving a
       small umask-readable window during which a local non-root
       attacker could read the operator's publisher-pin contents.
    3. ``flush`` + ``os.fsync`` on the data fd, then ``os.replace`` for
       the rename, then ``os.fsync`` on the parent directory fd so the
       rename itself is crash-durable (ext4 data-ordered defaults do
       not durably commit a rename without a directory fsync). The
       allowlist is security-policy state where a partial-write on
       crash would mean the daemon re-loads garbage TOFU pins next
       boot; durability is worth the latency.
    4. ``try/finally`` cleanup unlinks the temp file on any failure
       path so a disk-full / permission error during write does not
       leave a stray tmp file in the operator's config directory.

    Callers are responsible for holding :func:`_allowlist_lock`
    around the surrounding load-then-write sequence.
    """
    dir_path = ensure_config_dir()
    target = dir_path / _ALLOWLIST_FILENAME
    content = _render_allowlist(allowlist).encode("utf-8")

    # ``mkstemp`` returns an open fd we own; we manage close + rename
    # explicitly because ``NamedTemporaryFile`` would close on context
    # exit before we can ``os.replace``. The random suffix avoids the
    # collision race the previous fixed ``.tmp`` suffix had under
    # concurrent writers.
    tmp_fd, tmp_name = tempfile.mkstemp(
        dir=str(dir_path),
        prefix=".plugins.allowlist.",
        suffix=".tmp",
    )
    tmp_path = Path(tmp_name)
    try:
        # fchmod BEFORE first write so the file is never world-readable.
        os.fchmod(tmp_fd, 0o600)
        with os.fdopen(tmp_fd, "wb", closefd=True) as tmp_file:
            tmp_fd = -1  # fdopen now owns the fd; do not double-close
            tmp_file.write(content)
            tmp_file.flush()
            os.fsync(tmp_file.fileno())
        os.replace(tmp_path, target)
        # Directory-fsync so the rename itself is crash-durable.
        dir_fd = os.open(dir_path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except BaseException:
        # Best-effort tmp cleanup; suppress OSError so the original
        # exception propagates unobscured.
        if tmp_fd >= 0:
            with contextlib.suppress(OSError):
                os.close(tmp_fd)
        with contextlib.suppress(OSError):
            tmp_path.unlink()
        raise


def _escape(value: str) -> str:
    """Escape a string for inclusion inside double-quoted TOML."""
    return value.replace("\\", "\\\\").replace('"', '\\"')


__all__ = [
    "Allowlist",
    "AllowlistCorruptError",
    "PluginPolicy",
    "PublisherPin",
    "append_publisher_pin",
    "config_dir",
    "ensure_config_dir",
    "load_allowlist",
    "load_plugin_policy",
    "remove_publisher_pin",
]
