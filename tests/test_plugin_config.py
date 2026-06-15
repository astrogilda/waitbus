"""Tests for the plugin policy and publisher-pin allowlist config module.

Covers load_plugin_policy() defaults and file/env-var overrides,
load_allowlist() parse and empty-file paths, and append_publisher_pin()
atomicity, idempotency, and conflict detection. Also covers remove_publisher_pin().
"""

from __future__ import annotations

import stat
from pathlib import Path

import pytest

from waitbus.sources._config import (
    Allowlist,
    AllowlistCorruptError,
    PluginPolicy,
    append_publisher_pin,
    config_dir,
    load_allowlist,
    load_plugin_policy,
    remove_publisher_pin,
)

# ---------------------------------------------------------------------------
# Autouse: redirect XDG_CONFIG_HOME for every test
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _isolated_config_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Redirect the waitbus config directory to tmp_path so real config is never touched."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    # Remove env overrides that could bleed from an outer environment.
    monkeypatch.delenv("WAITBUS_DISABLE_SOURCE_AUTOLOAD", raising=False)
    monkeypatch.delenv("WAITBUS_PLUGINS", raising=False)


# ---------------------------------------------------------------------------
# load_plugin_policy tests
# ---------------------------------------------------------------------------


def test_load_plugin_policy_returns_default_when_file_absent() -> None:
    """With no config.toml present, load_plugin_policy() returns the default policy."""
    policy = load_plugin_policy()
    assert policy == PluginPolicy(autoload=True, allow=(), deny=())


def test_load_plugin_policy_reads_config_toml(tmp_path: Path) -> None:
    """load_plugin_policy() parses [plugins] from config.toml correctly."""
    waitbus_dir = tmp_path / "waitbus"
    waitbus_dir.mkdir(parents=True)
    (waitbus_dir / "config.toml").write_text(
        '[plugins]\nautoload = false\nallow = ["a"]\ndeny = ["b"]\n',
        encoding="utf-8",
    )
    policy = load_plugin_policy()
    assert policy.autoload is False
    assert policy.allow == ("a",)
    assert policy.deny == ("b",)


def test_load_plugin_policy_env_override_disables_autoload(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """WAITBUS_DISABLE_SOURCE_AUTOLOAD=1 forces autoload=False regardless of config file."""
    waitbus_dir = tmp_path / "waitbus"
    waitbus_dir.mkdir(parents=True)
    (waitbus_dir / "config.toml").write_text("[plugins]\nautoload = true\n", encoding="utf-8")
    monkeypatch.setenv("WAITBUS_DISABLE_SOURCE_AUTOLOAD", "1")
    policy = load_plugin_policy()
    assert policy.autoload is False


def test_load_plugin_policy_env_override_sets_allow(monkeypatch: pytest.MonkeyPatch) -> None:
    """WAITBUS_PLUGINS=a,b,c overrides the allow list from the config file."""
    monkeypatch.setenv("WAITBUS_PLUGINS", "a,b,c")
    policy = load_plugin_policy()
    assert policy.allow == ("a", "b", "c")


# ---------------------------------------------------------------------------
# load_allowlist tests
# ---------------------------------------------------------------------------


def test_load_allowlist_returns_empty_when_file_absent() -> None:
    """load_allowlist() returns an empty Allowlist when no allowlist file exists."""
    result = load_allowlist()
    assert result == Allowlist(pins={})
    assert result.for_source("anything") is None


def test_load_allowlist_parses_array_of_tables(tmp_path: Path) -> None:
    """load_allowlist() correctly parses a [[source]] array with two entries."""
    waitbus_dir = tmp_path / "waitbus"
    waitbus_dir.mkdir(parents=True)
    (waitbus_dir / "plugins.allowlist.toml").write_text(
        "[[source]]\n"
        'name = "circleci"\n'
        'publisher_kind = "GitHub"\n'
        'publisher_identity = "org/waitbus-circleci @ .github/workflows/release.yml"\n'
        'first_pinned_at = "2026-05-01T10:00:00+00:00"\n\n'
        "[[source]]\n"
        'name = "jenkins"\n'
        'publisher_kind = "GitLab"\n'
        'publisher_identity = "gitlab:group/waitbus-jenkins @ .gitlab-ci.yml"\n'
        'first_pinned_at = "2026-05-02T11:00:00+00:00"\n',
        encoding="utf-8",
    )
    result = load_allowlist()
    assert "circleci" in result.pins
    assert "jenkins" in result.pins
    circleci_pin = result.for_source("circleci")
    assert circleci_pin is not None
    assert circleci_pin.publisher_kind == "GitHub"
    jenkins_pin = result.for_source("jenkins")
    assert jenkins_pin is not None
    assert jenkins_pin.publisher_kind == "GitLab"


# ---------------------------------------------------------------------------
# append_publisher_pin tests
# ---------------------------------------------------------------------------


def test_append_publisher_pin_persists_atomically(tmp_path: Path) -> None:
    """Two successive append_publisher_pin calls produce a file with both pins at mode 0600."""
    pin_a = append_publisher_pin(
        name="source_a",
        publisher_kind="GitHub",
        publisher_identity="org/repo-a @ .github/workflows/release.yml",
    )
    pin_b = append_publisher_pin(
        name="source_b",
        publisher_kind="GitHub",
        publisher_identity="org/repo-b @ .github/workflows/release.yml",
    )

    allowlist = load_allowlist()
    assert "source_a" in allowlist.pins
    assert "source_b" in allowlist.pins

    waitbus_dir = tmp_path / "waitbus"
    allowlist_file = waitbus_dir / "plugins.allowlist.toml"
    assert allowlist_file.exists()
    file_mode = stat.S_IMODE(allowlist_file.stat().st_mode)
    assert file_mode == 0o600

    assert pin_a.name == "source_a"
    assert pin_b.name == "source_b"


def test_append_publisher_pin_idempotent_same_publisher() -> None:
    """Calling append_publisher_pin twice with identical arguments returns the existing pin without error."""
    first = append_publisher_pin(
        name="idempotent_source",
        publisher_kind="GitHub",
        publisher_identity="org/repo @ .github/workflows/release.yml",
    )
    second = append_publisher_pin(
        name="idempotent_source",
        publisher_kind="GitHub",
        publisher_identity="org/repo @ .github/workflows/release.yml",
    )
    # Both return a valid pin; content must match.
    assert first.name == second.name
    assert first.publisher_identity == second.publisher_identity
    # Only one entry should appear in the file.
    allowlist = load_allowlist()
    assert len([p for p in allowlist.pins if p == "idempotent_source"]) == 1


def test_append_publisher_pin_raises_on_different_publisher_for_existing_name() -> None:
    """Attempting to rebind a pinned name to a different publisher raises ValueError."""
    append_publisher_pin(
        name="contested_source",
        publisher_kind="GitHub",
        publisher_identity="original/repo @ .github/workflows/release.yml",
    )
    with pytest.raises(ValueError, match="contested_source"):
        append_publisher_pin(
            name="contested_source",
            publisher_kind="GitHub",
            publisher_identity="attacker/repo @ .github/workflows/release.yml",
        )


# ---------------------------------------------------------------------------
# remove_publisher_pin tests
# ---------------------------------------------------------------------------


def test_remove_publisher_pin_removes_existing() -> None:
    """remove_publisher_pin() removes a pinned name; the name no longer appears in the allowlist."""
    append_publisher_pin(
        name="to_remove",
        publisher_kind="GitHub",
        publisher_identity="org/to-remove @ .github/workflows/release.yml",
    )
    removed = remove_publisher_pin("to_remove")
    assert removed is True

    allowlist = load_allowlist()
    assert allowlist.for_source("to_remove") is None


def test_remove_publisher_pin_returns_false_when_absent() -> None:
    """remove_publisher_pin() returns False when the name has no pin recorded."""
    result = remove_publisher_pin("never_pinned")
    assert result is False


# ---------------------------------------------------------------------------
# Malformed TOML error path
# ---------------------------------------------------------------------------


def test_load_allowlist_raises_on_malformed_toml(tmp_path: Path) -> None:
    """load_allowlist() raises AllowlistCorruptError on invalid TOML.

    The typed exception is a RuntimeError subclass so callers using the
    broad ``except RuntimeError`` still work; the typed class exists so
    the registry's ``_enforce_tofu`` path can catch corruption
    specifically without swallowing arbitrary RuntimeError leakage.
    """
    waitbus_dir = tmp_path / "waitbus"
    waitbus_dir.mkdir(parents=True)
    (waitbus_dir / "plugins.allowlist.toml").write_text(
        "[[source]\nname = !!!broken toml\n",
        encoding="utf-8",
    )
    with pytest.raises(AllowlistCorruptError, match=r"plugins\.allowlist\.toml"):
        load_allowlist()


def test_load_allowlist_raises_on_wrong_source_shape(tmp_path: Path) -> None:
    """load_allowlist() raises AllowlistCorruptError when [[source]] is not an array."""
    waitbus_dir = tmp_path / "waitbus"
    waitbus_dir.mkdir(parents=True)
    (waitbus_dir / "plugins.allowlist.toml").write_text(
        'source = "not an array"\n',
        encoding="utf-8",
    )
    with pytest.raises(AllowlistCorruptError, match=r"expected ``\[\[source\]\]`` array"):
        load_allowlist()


# ---------------------------------------------------------------------------
# Atomic-write, fchmod, fsync, flock invariants
# ---------------------------------------------------------------------------


def test_append_publisher_pin_file_mode_is_0600() -> None:
    """After a successful pin write the file mode is exactly 0600."""
    append_publisher_pin(
        name="mode_test",
        publisher_kind="GitHub",
        publisher_identity="org/repo @ wf.yml",
    )
    path = config_dir() / "plugins.allowlist.toml"
    assert path.exists()
    file_mode = stat.S_IMODE(path.stat().st_mode)
    assert file_mode == 0o600, f"expected 0600, got {file_mode:o}"


def test_write_allowlist_is_atomic_under_concurrent_writers() -> None:
    """N threads racing append_publisher_pin all succeed; no pins are lost.

    Without the advisory flock the read-then-write sequence inside
    append_publisher_pin would race -- two threads could both load an
    empty allowlist, both compute "no prior pin", both write, and the
    second write would overwrite the first (losing the first thread's
    pin). The flock serialises the load+write critical section so the
    final file contains every distinct pin.
    """
    import threading

    n = 8
    barrier = threading.Barrier(n)
    errors: list[BaseException] = []

    def _pin(idx: int) -> None:
        barrier.wait()
        try:
            append_publisher_pin(
                name=f"concurrent_{idx:02d}",
                publisher_kind="GitHub",
                publisher_identity=f"org/repo_{idx:02d} @ wf.yml",
            )
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=_pin, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"unexpected errors: {errors}"
    allowlist = load_allowlist()
    assert set(allowlist.pins) == {f"concurrent_{i:02d}" for i in range(n)}


def test_corrupt_allowlist_does_not_crash_register_plugin() -> None:
    """register_plugin survives a corrupt allowlist; _enforce_tofu logs WARN + treats as empty.

    Hard-failing on a corrupt allowlist would be a denial-of-service
    vector (one bad byte bricks the whole event bus). The trade-off is
    to log WARN + fall back to empty TOFU view so operators see the
    problem and can run `waitbus allowlist repair`.
    """
    from unittest.mock import MagicMock

    from waitbus.sources._protocol import SOURCE_PLUGIN_API_VERSION, SourceSpec
    from waitbus.sources._registry import (
        _clear_for_test_isolation,
        is_known_source,
        register_plugin,
    )

    _clear_for_test_isolation()
    try:
        waitbus_dir = config_dir()
        waitbus_dir.mkdir(parents=True, exist_ok=True)
        (waitbus_dir / "plugins.allowlist.toml").write_text(
            "this is not valid TOML [[[\n",
            encoding="utf-8",
        )

        class _Plug:
            def spec(self) -> SourceSpec:
                return SourceSpec(
                    name="ok_source",
                    event_types=("ok_event",),
                    api_version=SOURCE_PLUGIN_API_VERSION,
                )

        ep = MagicMock()
        ep.name = "ok_source"
        ep.value = "fake:ok_source"
        ep.dist = None

        spec = register_plugin(ep, _Plug())
        assert spec.name == "ok_source"
        assert is_known_source("ok_source")
    finally:
        _clear_for_test_isolation()
