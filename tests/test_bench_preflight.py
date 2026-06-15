"""Unit tests for ``benchmarks._bench_preflight``.

Covers the version-band parser, the keyring lookup wrapper, the
``--temperature`` / ``--seed`` CLI gates, and the end-to-end
``run_preflight_assertions`` happy + failure paths.

Network-free: every external probe is patched. The real OpenAI keyring
lookup is exercised by the bench's caller (live preflight); these tests
pin the helper contracts.
"""

from __future__ import annotations

import sys
from typing import Any
from unittest.mock import patch

import pytest

from benchmarks._bench_preflight import (
    PreflightError,
    _check_cli_no_temperature_or_seed,
    _load_canonical_versions,
    _version_in_band,
    read_openai_key_from_keyring,
    run_preflight_assertions,
)

# ---------------------------------------------------------------------
# Version-band gate (packaging.specifiers.SpecifierSet backed).
# ---------------------------------------------------------------------


def test_version_in_band_inside() -> None:
    assert _version_in_band("1.104.0", ">=1.104,<2.0")
    assert _version_in_band("1.2.2", ">=1.2,<2.0")
    assert _version_in_band("0.10.3", ">=0.10,<1.0")


def test_version_in_band_outside() -> None:
    assert not _version_in_band("0.9.0", ">=1.0,<2.0")
    assert not _version_in_band("2.0.0", ">=1.0,<2.0")
    assert not _version_in_band("1.0.0", ">=1.5,<2.0")


def test_version_in_band_le_operator_includes_boundary() -> None:
    """``<=`` admits the upper boundary that ``<`` would exclude."""
    assert _version_in_band("2.0", ">=1.0,<=2.0")
    assert not _version_in_band("2.0", ">=1.0,<2.0")


def test_version_in_band_eq_operator_pins_exact_version() -> None:
    """``==`` matches only the pinned version."""
    assert _version_in_band("1.5", "==1.5")
    assert not _version_in_band("1.5.1", "==1.5")


def test_version_in_band_reversed_band_admits_nothing() -> None:
    """A reversed band is an empty (unsatisfiable) set, not an error."""
    assert not _version_in_band("1.5", ">=2.0,<1.0")
    assert not _version_in_band("2.5", ">=2.0,<1.0")


def test_version_in_band_tolerates_internal_whitespace() -> None:
    """Stray whitespace inside the band is stripped before parsing."""
    assert _version_in_band("1.5.0", ">= 1.0 , < 2.0")
    assert not _version_in_band("2.5.0", ">= 1.0 , < 2.0")


def test_version_in_band_prerelease_inside_band_accepted() -> None:
    """A pre-release that lands inside the band passes (gate is on major.minor)."""
    assert _version_in_band("1.2.3rc1", ">=1.2,<2.0")
    assert _version_in_band("0.10.3.dev0", ">=0.10,<1.0")


def test_version_in_band_bare_gt_operator_is_valid() -> None:
    """A bare ``>`` is a legitimate PEP 440 operator and is honoured."""
    assert _version_in_band("1.5", ">1.0,<2.0")
    assert not _version_in_band("1.0", ">1.0,<2.0")


def test_version_in_band_malformed_band_raises() -> None:
    """A band with no recognisable operator fails the bench fast."""
    with pytest.raises(PreflightError, match="malformed"):
        _version_in_band("1.0", "1.0")


def test_version_in_band_unparseable_version_raises() -> None:
    """An installed version string that is not PEP 440 fails the bench fast."""
    with pytest.raises(PreflightError, match="unparseable"):
        _version_in_band("not-a-version", ">=1.0,<2.0")


# ---------------------------------------------------------------------
# Canonical versions file load.
# ---------------------------------------------------------------------


def test_load_canonical_versions_returns_band_and_observed() -> None:
    """The shipped CANONICAL_VERSIONS.toml loads and contains every required pin."""
    pins = _load_canonical_versions()
    required = {"pydantic-ai-slim", "langgraph", "langchain-core", "openai", "tiktoken", "msgspec", "hdrhistogram"}
    assert required.issubset(pins.keys())
    for pkg in required:
        assert "band" in pins[pkg]
        assert "observed" in pins[pkg]


# ---------------------------------------------------------------------
# Keyring lookup wrapper.
# ---------------------------------------------------------------------


def test_read_openai_key_from_keyring_returns_string_or_none() -> None:
    """The wrapper returns a str or None; never an empty string."""
    key = read_openai_key_from_keyring()
    if key is not None:
        assert isinstance(key, str)
        assert key  # non-empty


# ---------------------------------------------------------------------
# CLI gates.
# ---------------------------------------------------------------------


def test_check_cli_no_temperature_raises_when_flag_advertised() -> None:
    """A fake CLI whose --help contains ``--temperature`` triggers PreflightError."""

    class _Result:
        def __init__(self, stdout: str) -> None:
            self.stdout = stdout
            self.stderr = ""
            self.returncode = 0

    def fake_run(*args: Any, **kwargs: Any) -> _Result:
        return _Result("Usage: foo --temperature FLOAT [other options]")

    with (
        patch("benchmarks._bench_preflight.shutil.which", return_value="/usr/bin/foo"),
        patch("benchmarks._bench_preflight.subprocess.run", fake_run),
        pytest.raises(PreflightError, match="--temperature"),
    ):
        _check_cli_no_temperature_or_seed("foo")


def test_check_cli_no_seed_raises_when_flag_advertised() -> None:
    """``--seed`` in --help triggers PreflightError just like --temperature."""

    class _Result:
        def __init__(self, stdout: str) -> None:
            self.stdout = stdout
            self.stderr = ""
            self.returncode = 0

    def fake_run(*args: Any, **kwargs: Any) -> _Result:
        return _Result("Usage: foo --seed INT")

    with (
        patch("benchmarks._bench_preflight.shutil.which", return_value="/usr/bin/foo"),
        patch("benchmarks._bench_preflight.subprocess.run", fake_run),
        pytest.raises(PreflightError, match="--seed"),
    ):
        _check_cli_no_temperature_or_seed("foo")


def test_check_cli_passes_when_no_flag_advertised() -> None:
    """A clean CLI --help passes silently."""

    class _Result:
        def __init__(self) -> None:
            self.stdout = "Usage: foo [-p PROMPT] [--output-format json]"
            self.stderr = ""
            self.returncode = 0

    def fake_run(*args: Any, **kwargs: Any) -> _Result:
        return _Result()

    with (
        patch("benchmarks._bench_preflight.shutil.which", return_value="/usr/bin/foo"),
        patch("benchmarks._bench_preflight.subprocess.run", fake_run),
    ):
        # No raise = pass.
        _check_cli_no_temperature_or_seed("foo")


def test_check_cli_missing_binary_raises() -> None:
    """A missing binary raises PreflightError with a clear message."""
    with (
        patch("benchmarks._bench_preflight.shutil.which", return_value=None),
        pytest.raises(PreflightError, match="not found on PATH"),
    ):
        _check_cli_no_temperature_or_seed("nonexistent-cli-xyz")


# ---------------------------------------------------------------------
# End-to-end preflight with all CLI requirements off.
# ---------------------------------------------------------------------


def test_run_preflight_no_cli_no_openai_passes_on_linux() -> None:
    """With every gate disabled, preflight reduces to host + library checks.

    Exercises the happy-path on the current dev host: the canonical
    versions file matches the installed closure, the monotonic clock
    is stable, and the report carries ``openai_key_present=False``.
    """
    if not sys.platform.startswith("linux"):
        pytest.skip("Linux-only bench gate")
    report = run_preflight_assertions(
        bench_name="unit_test",
        require_openai=False,
        require_claude_cli=False,
        require_gemini_cli=False,
    )
    assert report.openai_key_present is False
    # Library version probes populated.
    assert report.openai_sdk_version is not None
    assert report.msgspec_version is not None


def test_run_preflight_non_linux_aborts(monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-Linux ``sys.platform`` raises PreflightError, never proceeds."""
    monkeypatch.setattr(sys, "platform", "darwin")
    with pytest.raises(PreflightError, match="Linux"):
        run_preflight_assertions(
            bench_name="unit_test",
            require_openai=False,
            require_claude_cli=False,
            require_gemini_cli=False,
        )


def test_run_preflight_aborts_when_openai_key_missing_and_required() -> None:
    """When require_openai=True and keyring lookup returns None, abort."""
    if not sys.platform.startswith("linux"):
        pytest.skip("Linux-only bench gate")
    with (
        patch("benchmarks._bench_preflight.read_openai_key_from_keyring", return_value=None),
        pytest.raises(PreflightError, match="OPENAI_API_KEY"),
    ):
        run_preflight_assertions(
            bench_name="unit_test",
            require_openai=True,
            require_claude_cli=False,
            require_gemini_cli=False,
        )


def test_run_preflight_returns_report_with_openai_key_present_bool() -> None:
    """When the keyring returns a key, the report's openai_key_present=True
    and the key VALUE never appears in any report field."""
    if not sys.platform.startswith("linux"):
        pytest.skip("Linux-only bench gate")
    fake_key = "sk-fake-test-key-do-not-use-1234567890"
    with patch("benchmarks._bench_preflight.read_openai_key_from_keyring", return_value=fake_key):
        report = run_preflight_assertions(
            bench_name="unit_test",
            require_openai=True,
            require_claude_cli=False,
            require_gemini_cli=False,
        )
    assert report.openai_key_present is True
    # The struct's encoded form must not contain the key value anywhere.
    import msgspec

    encoded = msgspec.json.encode(report).decode("utf-8")
    assert fake_key not in encoded
