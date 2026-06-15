"""Boundary-path tests for the listener's credential loaders.

Covers the three small helpers at the top of ``waitbus/listener.py``
that wrap ``_secrets.get_secret`` with daemon-lifecycle semantics:

* ``_lookup_secret`` — absorbs ``SecretNotConfigured`` + empty-string into
  ``None`` so callers see a uniform sentinel.
* ``load_secret`` — fail-fast wrapper that exits 2 with a stderr message
  naming the missing credential and the remediation command.
* ``load_secret_optional`` — pass-through wrapper for non-load-bearing
  ingress paths; returns ``None`` without exiting.

These three functions sit on the boundary between credential infrastructure
and the listener's startup wiring.
"""

from __future__ import annotations

import sys
from io import StringIO
from unittest.mock import patch

import pytest

from waitbus import _secrets, listener


def test_lookup_secret_returns_none_when_not_configured() -> None:
    """``SecretNotConfigured`` from the credential backend collapses to None."""
    with patch.object(_secrets, "get_secret", side_effect=_secrets.SecretNotConfigured("absent")):
        assert listener._lookup_secret("missing-creds") is None


def test_lookup_secret_returns_none_for_empty_value() -> None:
    """An empty-string credential is treated as missing, not as a valid value."""
    with patch.object(_secrets, "get_secret", return_value=""):
        assert listener._lookup_secret("empty-creds") is None


def test_lookup_secret_returns_bytes_for_present_value() -> None:
    """A non-empty credential is encoded to UTF-8 bytes."""
    with patch.object(_secrets, "get_secret", return_value="hunter2"):
        assert listener._lookup_secret("present-creds") == b"hunter2"


def test_load_secret_exits_2_when_missing() -> None:
    """``load_secret`` calls ``sys.exit(2)`` with a remediation message on absence."""
    captured = StringIO()
    with (
        patch.object(_secrets, "get_secret", side_effect=_secrets.SecretNotConfigured("absent")),
        patch.object(sys, "stderr", captured),
        pytest.raises(SystemExit) as exc_info,
    ):
        listener.load_secret("github-webhook-hmac")
    assert exc_info.value.code == 2
    stderr_text = captured.getvalue()
    assert "github-webhook-hmac" in stderr_text
    assert "waitbus install-credentials" in stderr_text


def test_load_secret_returns_bytes_when_present() -> None:
    """``load_secret`` returns the UTF-8 bytes for a present credential."""
    with patch.object(_secrets, "get_secret", return_value="hunter2"):
        assert listener.load_secret("github-webhook-hmac") == b"hunter2"


def test_load_secret_optional_returns_none_when_missing() -> None:
    """``load_secret_optional`` returns ``None`` without exiting on absence."""
    with patch.object(_secrets, "get_secret", side_effect=_secrets.SecretNotConfigured("absent")):
        assert listener.load_secret_optional("alertmanager-hmac") is None


def test_load_secret_optional_returns_bytes_when_present() -> None:
    """``load_secret_optional`` matches ``load_secret`` on the happy path."""
    with patch.object(_secrets, "get_secret", return_value="webhook-secret"):
        assert listener.load_secret_optional("alertmanager-hmac") == b"webhook-secret"
