"""Read-only secret loader backed by a single 0600 JSON file.

Secrets live in one JSON object at ``_paths.state_dir()/secrets.json``,
written atomically by ``waitbus install-credentials`` (mode 0600). The
read path is stdlib-only (``json.loads``): no external binary, no
``cryptography``/``cffi`` closure, no per-unit credential plumbing.

At-rest protection is delegated to host full-disk encryption
(FileVault / LUKS) plus UNIX discretionary access control: the file is
0600, readable only by the owning user, and a lifted disk image is
covered by the host FDE the operator already runs. See ``SECURITY.md``
for the full boundary statement.

Why one JSON file and not the keyring library: the keyring v25 closure
pulled in ``cryptography`` + ``cffi`` (native Rust + C) which cost
+21.6 MiB RSS at first secret read and ~175 ms cold-import latency. A
plain 0600 JSON read is lighter than either, not heavier.

The ``_load_secrets`` indirection is the one seam a future host-bound
backend (e.g. a TPM-sealed store for an always-on server) can re-enter
without changing ``get_secret`` or any of its callers.
"""

from __future__ import annotations

import json
import os
import stat
from functools import lru_cache
from typing import Any

from . import _paths


class SecretNotConfigured(RuntimeError):  # noqa: N818
    """Raised when the secrets file is present but unusable.

    Triggered by a corrupt/unreadable ``secrets.json`` or a file whose
    mode is not 0600. A simply-absent file is NOT an error — it returns
    ``None`` so the broadcast/wait path runs with no secrets at all.

    Operator fix: stage the secret via
    ``waitbus install-credentials <name>``, which writes the file
    atomically with the correct 0600 mode.
    """


def secrets_path() -> str:
    """Return the absolute path to the JSON secrets file."""
    return str(_paths.state_dir() / "secrets.json")


@lru_cache(maxsize=1)
def _load_secrets(path: str) -> dict[str, Any]:
    """Read and parse the secrets file once per path, then cache it.

    Returns an empty dict when the file is absent (secrets are optional).
    Raises ``SecretNotConfigured`` when the file exists but cannot be
    read, is not mode 0600, or does not parse to a JSON object.

    The cache means an operator rotating a secret must restart the daemon
    to pick up the new value — fail-loud, no silent stale auth — matching
    the prior backends' construction-time read semantics.
    """
    try:
        st = os.stat(path)
    except FileNotFoundError:
        return {}
    except OSError as exc:
        raise SecretNotConfigured(f"secrets file unreadable: {path} ({exc})") from exc
    mode = stat.S_IMODE(st.st_mode)
    if mode != 0o600:
        raise SecretNotConfigured(
            f"secrets file {path} has mode {mode:#o}, expected 0600; re-stage via `waitbus install-credentials <name>`."
        )
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except OSError as exc:
        raise SecretNotConfigured(f"secrets file unreadable: {path} ({exc})") from exc
    except json.JSONDecodeError as exc:
        raise SecretNotConfigured(f"secrets file {path} is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise SecretNotConfigured(f"secrets file {path} must contain a JSON object, got {type(data).__name__}")
    return data


def get_secret(name: str) -> str | None:
    """Read a credential from the JSON secrets file.

    Returns the credential value as a string, or ``None`` when the
    secrets file is absent or has no entry for ``name``. A simply-absent
    file is not an error — the broadcast/wait path runs with no secrets.

    Raises:
        SecretNotConfigured: the secrets file exists but cannot be read,
            is not mode 0600, or does not parse to a JSON object.
    """
    data = _load_secrets(secrets_path())
    value = data.get(name)
    if value is None:
        return None
    return str(value)


def _reset_cache_for_test() -> None:
    """Clear the secrets cache so a test can re-stage and re-read.

    Production code never calls this — a daemon restart is the rotation
    boundary. Tests that write a fresh ``secrets.json`` between reads use
    it to invalidate the per-path cache.
    """
    _load_secrets.cache_clear()
