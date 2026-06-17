"""Unit tests for the 0600-JSON-file secret loader.

End-to-end exercises the read path against real filesystem state: a real
``secrets.json`` written under a per-test state dir (via the
``WAITBUS_STATE_DIR`` env override). No mocking of the I/O boundary — the
test asserts behaviour against actual files and modes.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from waitbus import _secrets


def _write_secrets(state_dir: Path, data: dict[str, str], *, mode: int = 0o600) -> Path:
    """Write a ``secrets.json`` under ``state_dir`` with the given mode."""
    state_dir.mkdir(parents=True, exist_ok=True)
    path = state_dir / "secrets.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    path.chmod(mode)
    return path


@pytest.fixture(autouse=True)
def _isolate_secrets(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Point the state dir at a per-test tmp dir and clear the read cache."""
    state = tmp_path / "state"
    state.mkdir()
    monkeypatch.setenv("WAITBUS_STATE_DIR", str(state))
    _secrets._reset_cache_for_test()
    yield state
    _secrets._reset_cache_for_test()


# --- get_secret -------------------------------------------------------------


def test_get_secret_returns_value_when_present(_isolate_secrets: Path) -> None:
    _write_secrets(_isolate_secrets, {"github-webhook-secret": "abc123"})
    assert _secrets.get_secret("github-webhook-secret") == "abc123"


def test_get_secret_returns_none_when_file_absent(_isolate_secrets: Path) -> None:
    """An absent secrets file is not an error — secrets are optional."""
    assert _secrets.get_secret("anything") is None


def test_get_secret_returns_none_when_key_absent(_isolate_secrets: Path) -> None:
    _write_secrets(_isolate_secrets, {"other-key": "x"})
    assert _secrets.get_secret("github-webhook-secret") is None


def test_get_secret_reads_one_of_several_keys(_isolate_secrets: Path) -> None:
    _write_secrets(
        _isolate_secrets,
        {"github-webhook-secret": "gh", "alertmanager-hmac": "am"},
    )
    assert _secrets.get_secret("github-webhook-secret") == "gh"
    assert _secrets.get_secret("alertmanager-hmac") == "am"


def test_get_secret_coerces_non_string_value_to_string(_isolate_secrets: Path) -> None:
    """A JSON number value is returned as its string form, not None."""
    _isolate_secrets.mkdir(parents=True, exist_ok=True)
    path = _isolate_secrets / "secrets.json"
    path.write_text('{"numeric": 12345}', encoding="utf-8")
    path.chmod(0o600)
    assert _secrets.get_secret("numeric") == "12345"


# --- error conditions -------------------------------------------------------


def test_get_secret_raises_on_wrong_mode(_isolate_secrets: Path) -> None:
    """A secrets file that is not 0600 fails loud rather than reading."""
    _write_secrets(_isolate_secrets, {"k": "v"}, mode=0o644)
    _secrets._reset_cache_for_test()
    with pytest.raises(_secrets.SecretNotConfigured, match=r"mode .* expected 0600"):
        _secrets.get_secret("k")


def test_get_secret_raises_on_corrupt_json(_isolate_secrets: Path) -> None:
    _isolate_secrets.mkdir(parents=True, exist_ok=True)
    path = _isolate_secrets / "secrets.json"
    path.write_text("{not valid json", encoding="utf-8")
    path.chmod(0o600)
    with pytest.raises(_secrets.SecretNotConfigured, match="not valid JSON"):
        _secrets.get_secret("k")


def test_get_secret_raises_when_not_a_json_object(_isolate_secrets: Path) -> None:
    _isolate_secrets.mkdir(parents=True, exist_ok=True)
    path = _isolate_secrets / "secrets.json"
    path.write_text("[1, 2, 3]", encoding="utf-8")
    path.chmod(0o600)
    with pytest.raises(_secrets.SecretNotConfigured, match="must contain a JSON object"):
        _secrets.get_secret("k")


def test_get_secret_raises_on_unreadable_file(_isolate_secrets: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An OSError on open surfaces as SecretNotConfigured, not a bare crash."""
    path = _write_secrets(_isolate_secrets, {"k": "v"})
    _secrets._reset_cache_for_test()

    real_open = open

    def _boom(file: object, *args: object, **kwargs: object) -> object:
        if str(file) == str(path):
            raise OSError("permission denied")
        return real_open(file, *args, **kwargs)  # type: ignore[call-overload]

    monkeypatch.setattr("builtins.open", _boom)
    with pytest.raises(_secrets.SecretNotConfigured, match="secrets file unreadable"):
        _secrets.get_secret("k")


# --- caching ----------------------------------------------------------------


def test_get_secret_caches_until_reset(_isolate_secrets: Path) -> None:
    """The file is read once; a later write is not seen until the cache clears.

    This is the rotation-requires-restart contract: a daemon caches the
    secrets at startup and does not pick up an on-disk edit silently.
    """
    _write_secrets(_isolate_secrets, {"k": "first"})
    assert _secrets.get_secret("k") == "first"
    _write_secrets(_isolate_secrets, {"k": "second"})
    # Still the cached value (no reset).
    assert _secrets.get_secret("k") == "first"
    _secrets._reset_cache_for_test()
    assert _secrets.get_secret("k") == "second"


def test_secrets_path_is_under_state_dir(_isolate_secrets: Path) -> None:
    assert _secrets.secrets_path() == str(_isolate_secrets / "secrets.json")
    assert os.path.basename(_secrets.secrets_path()) == "secrets.json"
