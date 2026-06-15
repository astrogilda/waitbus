"""Read-only secret loader with a pluggable backend.

Two backends are supported, selected by the
``WAITBUS_SECRETS_BACKEND`` environment variable:

- ``systemd-creds`` (default): credentials are delivered to the daemon
  by systemd via ``LoadCredentialEncrypted=<name>:<encrypted-path>``
  directives in each unit. systemd decrypts each credential to a tmpfs
  file under ``$CREDENTIALS_DIRECTORY`` (one file per credential, name =
  directive name, mode 0400, owned by the service user) before
  ``ExecStart`` runs. This module reads those files and returns the
  contents as Python strings. Writes happen exclusively via the
  ``waitbus install credentials`` subcommand, which shells out to
  ``systemd-creds encrypt``; the daemon itself never sees the key.

- ``age``: credentials are stored as age-encrypted files at
  ``$WAITBUS_AGE_CREDS_DIR/<name>.age`` and decrypted at read time by
  shelling out to the ``age`` binary with the identity file at
  ``$WAITBUS_AGE_IDENTITY``. This backend unblocks Docker, macOS, and
  other non-systemd deployments where ``LoadCredentialEncrypted=`` is
  unavailable. age is an optional external tool the operator installs,
  exactly parallel to systemd-creds being an external tool — the daemon
  stays stdlib-only on the secret-read path either way.

The ``WAITBUS_CREDS_DIR`` environment variable is a documented
test-only override for the systemd-creds backend that takes effect when
``CREDENTIALS_DIRECTORY`` is unset. It is intended for ``pytest`` and
local smoke runs only — never set it on a production daemon.

Why not the keyring library: the keyring v25 closure pulled in
``cryptography`` + ``cffi`` (native Rust + C) which cost +21.6 MiB RSS
at first secret read and ~175 ms cold-import latency. Both backends keep
the daemon stdlib-only on the secret-read path.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

_AGE_DECRYPT_TIMEOUT_SECONDS = 15


class SecretNotConfigured(RuntimeError):  # noqa: N818
    """Raised when a required credential is present but unreadable.

    Operator fix: store the secret via ``waitbus install credentials``
    (systemd-creds backend) or re-encrypt it with the configured age
    recipient (age backend), and ensure the daemon can reach it.
    """


def _get_secret_systemd_creds(name: str) -> str | None:
    creds_dir = os.environ.get("CREDENTIALS_DIRECTORY") or os.environ.get("WAITBUS_CREDS_DIR")
    if not creds_dir:
        return None
    path = Path(creds_dir) / name
    if not path.is_file():
        return None
    try:
        return path.read_text(encoding="utf-8").rstrip("\r\n")
    except OSError as exc:
        raise SecretNotConfigured(
            f"credential file unreadable: {path} ({exc})",
        ) from exc


def _get_secret_age(name: str) -> str | None:
    creds_dir = os.environ.get("WAITBUS_AGE_CREDS_DIR")
    identity = os.environ.get("WAITBUS_AGE_IDENTITY")
    if not creds_dir or not identity:
        return None
    path = Path(creds_dir) / f"{name}.age"
    if not path.is_file():
        return None
    age_bin = shutil.which("age")
    if age_bin is None:
        raise SecretNotConfigured(
            "WAITBUS_SECRETS_BACKEND=age but the age binary is not on "
            "PATH. Install age (https://github.com/FiloSottile/age) or "
            "switch WAITBUS_SECRETS_BACKEND back to systemd-creds."
        )
    try:
        proc = subprocess.run(
            [age_bin, "--decrypt", "--identity", identity, str(path)],
            capture_output=True,
            timeout=_AGE_DECRYPT_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise SecretNotConfigured(
            f"age decrypt failed for {path}: {exc}",
        ) from exc
    if proc.returncode != 0:
        stderr = proc.stderr.decode("utf-8", "replace").strip()
        raise SecretNotConfigured(
            f"age decrypt exited {proc.returncode} for {path}: {stderr}",
        )
    return proc.stdout.decode("utf-8").rstrip("\r\n")


def get_secret(name: str) -> str | None:
    """Read a credential via the configured secret backend.

    Returns the credential value (UTF-8-decoded, trailing newline
    stripped) or ``None`` when the credential is not configured on this
    host.

    Backend selection: ``WAITBUS_SECRETS_BACKEND`` is ``systemd-creds``
    (default) or ``age``. An unknown value raises ``SecretNotConfigured``
    rather than silently falling back, so a misconfigured deployment
    fails loud instead of running without an expected credential.

    Raises:
        SecretNotConfigured: the credential exists but cannot be read or
            decrypted (binary blob, permission denial, missing age
            binary, wrong age identity), or the backend name is unknown.
    """
    backend = os.environ.get("WAITBUS_SECRETS_BACKEND", "systemd-creds")
    if backend == "systemd-creds":
        return _get_secret_systemd_creds(name)
    if backend == "age":
        return _get_secret_age(name)
    raise SecretNotConfigured(f"unknown WAITBUS_SECRETS_BACKEND={backend!r}; expected 'systemd-creds' or 'age'")
