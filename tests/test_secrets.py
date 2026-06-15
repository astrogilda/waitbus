"""Unit tests for the systemd-creds-backed secret loader.

End-to-end exercises the credential-read path by populating a real
directory on disk and pointing the loader at it via the
``WAITBUS_CREDS_DIR`` override (production daemons use
``$CREDENTIALS_DIRECTORY``, set by systemd). No mocking of the I/O
boundary — the test asserts behaviour against actual filesystem state.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from waitbus import _secrets


def _populate(creds_dir: Path, name: str, value: str) -> None:
    """Write a credential file the way systemd would expose it."""
    (creds_dir / name).write_text(value, encoding="utf-8")


# --- get_secret -------------------------------------------------------------


def test_get_secret_returns_value_when_file_present(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WAITBUS_CREDS_DIR", str(tmp_path))
    _populate(tmp_path, "github-webhook-secret", "abc123")
    assert _secrets.get_secret("github-webhook-secret") == "abc123"


def test_get_secret_strips_trailing_newline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """systemd-creds encrypt followed by manual heredoc often appends \\n."""
    monkeypatch.setenv("WAITBUS_CREDS_DIR", str(tmp_path))
    _populate(tmp_path, "broadcast-token", "tok-value\n")
    assert _secrets.get_secret("broadcast-token") == "tok-value"


def test_get_secret_returns_none_when_credentials_directory_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CREDENTIALS_DIRECTORY", raising=False)
    monkeypatch.delenv("WAITBUS_CREDS_DIR", raising=False)
    assert _secrets.get_secret("anything") is None


def test_get_secret_returns_none_when_file_absent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WAITBUS_CREDS_DIR", str(tmp_path))
    assert _secrets.get_secret("missing") is None


def test_get_secret_prefers_systemd_credentials_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CREDENTIALS_DIRECTORY wins over WAITBUS_CREDS_DIR when both set."""
    systemd_dir = tmp_path / "systemd"
    fallback_dir = tmp_path / "fallback"
    systemd_dir.mkdir()
    fallback_dir.mkdir()
    _populate(systemd_dir, "tok", "from-systemd")
    _populate(fallback_dir, "tok", "from-fallback")
    monkeypatch.setenv("CREDENTIALS_DIRECTORY", str(systemd_dir))
    monkeypatch.setenv("WAITBUS_CREDS_DIR", str(fallback_dir))
    assert _secrets.get_secret("tok") == "from-systemd"


def test_get_secret_ignores_subdirectories(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A name that resolves to a directory (not a regular file) returns None."""
    monkeypatch.setenv("WAITBUS_CREDS_DIR", str(tmp_path))
    (tmp_path / "github-webhook-secret").mkdir()
    assert _secrets.get_secret("github-webhook-secret") is None


def test_get_secret_raises_secret_not_configured_on_oserror(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """read_text raising OSError surfaces as SecretNotConfigured."""
    creds_dir = tmp_path / "creds"
    creds_dir.mkdir()
    target = creds_dir / "foo"
    target.write_text("x")

    monkeypatch.setenv("WAITBUS_CREDS_DIR", str(creds_dir))

    real_read_text = Path.read_text

    def _boom(self: Path, *args: object, **kwargs: object) -> str:
        if self == target:
            raise OSError("permission denied")
        return real_read_text(self, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(Path, "read_text", _boom)

    with pytest.raises(_secrets.SecretNotConfigured, match="credential file unreadable"):
        _secrets.get_secret("foo")


# --- backend selection ------------------------------------------------------


def test_unknown_backend_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unrecognised backend fails loud rather than silently returning None."""
    monkeypatch.setenv("WAITBUS_SECRETS_BACKEND", "vault")
    with pytest.raises(_secrets.SecretNotConfigured, match="unknown WAITBUS_SECRETS_BACKEND"):
        _secrets.get_secret("anything")


# --- age backend ------------------------------------------------------------


def _install_fake_age(bin_dir: Path, monkeypatch: pytest.MonkeyPatch, *, body: str) -> None:
    """Put a fake `age` on PATH that emits `body` for `age --decrypt ...`."""
    bin_dir.mkdir(parents=True, exist_ok=True)
    fake = bin_dir / "age"
    # The real age writes plaintext to stdout for --decrypt; mirror that.
    # echo appends a trailing newline, exercising the rstrip path too.
    fake.write_text(
        f"#!/bin/sh\necho {body!r}\n",
        encoding="utf-8",
    )
    fake.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}:{__import__('os').environ['PATH']}")


def test_age_backend_decrypts_via_age_binary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    creds_dir = tmp_path / "age-creds"
    creds_dir.mkdir()
    (creds_dir / "broadcast-token.age").write_bytes(b"<age-ciphertext>")
    identity = tmp_path / "key.txt"
    identity.write_text("AGE-SECRET-KEY-TEST", encoding="utf-8")

    monkeypatch.setenv("WAITBUS_SECRETS_BACKEND", "age")
    monkeypatch.setenv("WAITBUS_AGE_CREDS_DIR", str(creds_dir))
    monkeypatch.setenv("WAITBUS_AGE_IDENTITY", str(identity))
    _install_fake_age(tmp_path / "bin", monkeypatch, body="decrypted-secret")

    assert _secrets.get_secret("broadcast-token") == "decrypted-secret"


def test_age_backend_returns_none_when_env_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WAITBUS_SECRETS_BACKEND", "age")
    monkeypatch.delenv("WAITBUS_AGE_CREDS_DIR", raising=False)
    monkeypatch.delenv("WAITBUS_AGE_IDENTITY", raising=False)
    assert _secrets.get_secret("anything") is None


def test_age_backend_raises_when_age_binary_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    creds_dir = tmp_path / "age-creds"
    creds_dir.mkdir()
    (creds_dir / "tok.age").write_bytes(b"<ct>")
    identity = tmp_path / "id.txt"
    identity.write_text("k", encoding="utf-8")

    monkeypatch.setenv("WAITBUS_SECRETS_BACKEND", "age")
    monkeypatch.setenv("WAITBUS_AGE_CREDS_DIR", str(creds_dir))
    monkeypatch.setenv("WAITBUS_AGE_IDENTITY", str(identity))
    monkeypatch.setenv("PATH", str(tmp_path / "empty-bin"))

    with pytest.raises(_secrets.SecretNotConfigured, match="age binary is not on"):
        _secrets.get_secret("tok")


def test_age_backend_raises_on_nonzero_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    creds_dir = tmp_path / "age-creds"
    creds_dir.mkdir()
    (creds_dir / "tok.age").write_bytes(b"<ct>")
    identity = tmp_path / "id.txt"
    identity.write_text("k", encoding="utf-8")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake = bin_dir / "age"
    fake.write_text(
        "#!/bin/sh\necho 'bad identity' >&2\nexit 1\n",
        encoding="utf-8",
    )
    fake.chmod(0o755)

    monkeypatch.setenv("WAITBUS_SECRETS_BACKEND", "age")
    monkeypatch.setenv("WAITBUS_AGE_CREDS_DIR", str(creds_dir))
    monkeypatch.setenv("WAITBUS_AGE_IDENTITY", str(identity))
    import os as _os

    monkeypatch.setenv("PATH", f"{bin_dir}:{_os.environ['PATH']}")

    with pytest.raises(_secrets.SecretNotConfigured, match="age decrypt exited 1"):
        _secrets.get_secret("tok")
