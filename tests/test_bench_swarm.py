"""Unit tests for ``benchmarks._bench_swarm``.

The factory is tested via its spawn-argv shape (no real subprocess is
launched in unit tests). The load-bearing assertion is that
``OPENAI_API_KEY`` is propagated to OpenAI-needing drivers and STRIPPED
from the env of every other driver (ensures OPENAI_API_KEY does not
leak into shell-control or claude-cli stderr).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from benchmarks._bench_swarm import (
    _MAX_COLD_PREFIX_LEN,
    _build_env_for_framework,
    default_python_executable,
    spawn_n_heterogeneous,
)

# ---------------------------------------------------------------------
# Env build per framework.
# ---------------------------------------------------------------------


def test_build_env_forces_deterministic_python_knobs() -> None:
    """Every framework gets PYTHONHASHSEED / PYTHONUNBUFFERED / WAITBUS_BENCH_GC_OFF."""
    env = _build_env_for_framework(
        framework="shell-control",
        base_env={"PATH": "/usr/bin"},
        openai_api_key=None,
    )
    assert env["PYTHONHASHSEED"] == "0"
    assert env["PYTHONUNBUFFERED"] == "1"
    assert env["WAITBUS_BENCH_GC_OFF"] == "1"


def test_build_env_pydantic_gets_openai_key() -> None:
    """pydantic-ai needs OpenAI; the key lands in its env."""
    env = _build_env_for_framework(
        framework="pydantic",
        base_env={"PATH": "/usr/bin"},
        openai_api_key="sk-test-1234",
    )
    assert env["OPENAI_API_KEY"] == "sk-test-1234"


def test_build_env_langgraph_gets_openai_key() -> None:
    """langgraph wraps OpenAI under the hood; key lands in env."""
    env = _build_env_for_framework(
        framework="langgraph",
        base_env={"PATH": "/usr/bin"},
        openai_api_key="sk-test-1234",
    )
    assert env["OPENAI_API_KEY"] == "sk-test-1234"


def test_build_env_pydantic_without_key_omits_openai_key() -> None:
    """Pydantic spawn with no key: OPENAI_API_KEY absent in env."""
    env = _build_env_for_framework(
        framework="pydantic",
        base_env={"PATH": "/usr/bin"},
        openai_api_key=None,
    )
    assert "OPENAI_API_KEY" not in env


def test_build_env_no_legacy_flag_written() -> None:
    """Greenfield: the legacy WAITBUS_USE_REAL_OPENAI flag is never written.

    Drivers detect ``OPENAI_API_KEY`` directly; the bench factory must
    not write any opt-in toggle. Asserts the flag is absent from the
    spawn env for every framework, with and without a key.
    """
    for framework in ("pydantic", "langgraph", "claude-cli", "gemini-cli", "shell-control"):
        for key in (None, "sk-test-1234"):
            env = _build_env_for_framework(
                framework=framework,
                base_env={"PATH": "/usr/bin"},
                openai_api_key=key,
            )
            assert "WAITBUS_USE_REAL_OPENAI" not in env, (
                f"legacy flag leaked into env for framework={framework} key={key!r}"
            )


def test_build_env_openai_framework_sets_real_mode_flag_with_key() -> None:
    """Real-LLM mode (key non-None): pydantic/langgraph env carries REAL_MODE_ENV_VAR=1.

    The flag is the driver's only signal of the parent's real mode; without it
    the driver's OpenAI selector cannot tell real mode from offline and would
    silently fall back to a fake on an absent key.
    """
    from scripts.stress._real_drivers import REAL_MODE_ENV_VAR

    for framework in ("pydantic", "langgraph"):
        env = _build_env_for_framework(
            framework=framework,
            base_env={"PATH": "/usr/bin"},
            openai_api_key="sk-test-1234",
        )
        assert env.get(REAL_MODE_ENV_VAR) == "1", f"real-mode flag missing for {framework}"


def test_build_env_offline_path_omits_real_mode_flag() -> None:
    """Offline path (key None): REAL_MODE_ENV_VAR absent so the offline fake is legitimate."""
    from scripts.stress._real_drivers import REAL_MODE_ENV_VAR

    for framework in ("pydantic", "langgraph", "claude-cli", "gemini-cli", "shell-control"):
        env = _build_env_for_framework(
            framework=framework,
            base_env={"PATH": "/usr/bin", REAL_MODE_ENV_VAR: "1"},
            openai_api_key=None,
        )
        assert REAL_MODE_ENV_VAR not in env, f"stale real-mode flag leaked for {framework} on offline path"


def test_build_env_non_openai_framework_omits_real_mode_flag_even_with_key() -> None:
    """Non-OpenAI roles never carry REAL_MODE_ENV_VAR (they make no OpenAI call)."""
    from scripts.stress._real_drivers import REAL_MODE_ENV_VAR

    for framework in ("claude-cli", "gemini-cli", "shell-control"):
        env = _build_env_for_framework(
            framework=framework,
            base_env={"PATH": "/usr/bin", REAL_MODE_ENV_VAR: "1"},
            openai_api_key="sk-test-1234",
        )
        assert REAL_MODE_ENV_VAR not in env, f"real-mode flag wrongly set for non-OpenAI {framework}"


def test_build_env_shell_control_has_no_openai_key_even_if_in_base_env() -> None:
    """shell-control's env strips OPENAI_API_KEY (ensures no key leak)."""
    env = _build_env_for_framework(
        framework="shell-control",
        base_env={"PATH": "/usr/bin", "OPENAI_API_KEY": "sk-already-there"},
        openai_api_key="sk-test-1234",
    )
    assert "OPENAI_API_KEY" not in env


def test_build_env_claude_cli_strips_openai_key() -> None:
    """claude-cli does not need OpenAI; its env must NOT carry the key."""
    env = _build_env_for_framework(
        framework="claude-cli",
        base_env={"PATH": "/usr/bin", "OPENAI_API_KEY": "sk-already-there"},
        openai_api_key="sk-test-1234",
    )
    assert "OPENAI_API_KEY" not in env


def test_build_env_gemini_cli_strips_openai_key() -> None:
    """gemini-cli does not need OpenAI; its env must NOT carry the key."""
    env = _build_env_for_framework(
        framework="gemini-cli",
        base_env={"PATH": "/usr/bin", "OPENAI_API_KEY": "sk-already-there"},
        openai_api_key="sk-test-1234",
    )
    assert "OPENAI_API_KEY" not in env


# ---------------------------------------------------------------------
# spawn_n_heterogeneous — argv + env shape via Popen patch.
# ---------------------------------------------------------------------


@pytest.fixture
def spawn_capture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Patch subprocess.Popen to capture argv + env without launching a process."""
    captured: list[dict[str, Any]] = []

    class _FakeProc:
        def __init__(self, argv: list[str], env: dict[str, str]) -> None:
            self.argv = argv
            self.env = env
            self.pid = 99999
            self.returncode: int | None = 0

        def wait(self, timeout: float | None = None) -> int:
            return 0

        def communicate(self, timeout: float | None = None) -> tuple[bytes, bytes]:
            return b"", b""

        def terminate(self) -> None:
            return None

        def kill(self) -> None:
            return None

        def poll(self) -> int | None:
            return 0

    def fake_popen(argv: list[str], **kwargs: Any) -> _FakeProc:
        captured.append({"argv": list(argv), "env": dict(kwargs.get("env", {}))})
        return _FakeProc(argv, kwargs.get("env", {}))

    monkeypatch.setattr("benchmarks._bench_swarm.subprocess.Popen", fake_popen)
    return captured


def test_spawn_n_heterogeneous_unknown_framework_raises(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unknown framework"):
        spawn_n_heterogeneous(
            swarm_spec={"not-a-framework": 1},
            base_env={"PATH": "/usr/bin"},
            socket_path=tmp_path / "sock",
            db_path=tmp_path / "db",
            doorbell_path=tmp_path / "doorbell",
            seed_scope_id="test",
            python_exe="/usr/bin/python3",
            stderr_dir=tmp_path,
            openai_api_key=None,
        )


def test_spawn_n_heterogeneous_argv_contains_python_dash_u(spawn_capture: list[dict[str, Any]], tmp_path: Path) -> None:
    """Spawn argv includes the ``-u`` unbuffered flag."""
    spawn_n_heterogeneous(
        swarm_spec={"shell-control": 1},
        base_env={"PATH": "/usr/bin"},
        socket_path=tmp_path / "sock",
        db_path=tmp_path / "db",
        doorbell_path=tmp_path / "doorbell",
        seed_scope_id="test",
        python_exe="/usr/bin/python3",
        stderr_dir=tmp_path,
        openai_api_key=None,
    )
    assert len(spawn_capture) == 1
    argv = spawn_capture[0]["argv"]
    assert "-u" in argv
    # ``-u`` appears immediately after the Python executable so it
    # affects every subsequent module import.
    assert argv[0] == "/usr/bin/python3"
    assert argv[1] == "-u"


def test_spawn_n_heterogeneous_forwards_cold_prefix(
    spawn_capture: list[dict[str, Any]],
    tmp_path: Path,
) -> None:
    """Spawn argv includes ``--cold-prefix <value>`` so the driver subprocess
    can prepend it onto the LLM prompt."""
    spawn_n_heterogeneous(
        swarm_spec={"claude-cli": 1},
        base_env={"PATH": "/usr/bin"},
        socket_path=tmp_path / "sock",
        db_path=tmp_path / "db",
        doorbell_path=tmp_path / "doorbell",
        seed_scope_id="test",
        python_exe="/usr/bin/python3",
        stderr_dir=tmp_path,
        openai_api_key=None,
        cold_prefix="iter-prefix-42",
    )
    assert len(spawn_capture) == 1
    argv = spawn_capture[0]["argv"]
    assert "--cold-prefix" in argv
    idx = argv.index("--cold-prefix")
    assert argv[idx + 1] == "iter-prefix-42"


def test_spawn_rejects_oversized_cold_prefix(
    spawn_capture: list[dict[str, Any]],
    tmp_path: Path,
) -> None:
    """A cold_prefix at or above the cap raises before reaching Popen.

    The argv length floor (Linux MAX_ARG_STRLEN) is enforced at the
    factory boundary so an oversized cache-busting prefix fails fast with
    a clear message rather than as an opaque E2BIG from the spawn.
    """
    with pytest.raises(ValueError, match="cold_prefix"):
        spawn_n_heterogeneous(
            swarm_spec={"claude-cli": 1},
            base_env={"PATH": "/usr/bin"},
            socket_path=tmp_path / "sock",
            db_path=tmp_path / "db",
            doorbell_path=tmp_path / "doorbell",
            seed_scope_id="test",
            python_exe="/usr/bin/python3",
            stderr_dir=tmp_path,
            openai_api_key=None,
            cold_prefix="x" * _MAX_COLD_PREFIX_LEN,
        )
    assert spawn_capture == []


def test_spawn_n_heterogeneous_forwards_since_cursor(
    spawn_capture: list[dict[str, Any]],
    tmp_path: Path,
) -> None:
    """Spawn argv includes ``--since <event_id>`` when the orchestrator
    threads a replay cursor through ``spawn_n_heterogeneous``.

    The replay cursor lets the driver's ``wait_for`` subscribe with a
    ``since=`` window so a seed event emitted at or after the cursor is
    delivered regardless of subprocess start-up latency.
    """
    anchor_event_id = "01H8XYZA01H8XYZA01H8XYZA01"  # canonical ULID shape
    spawn_n_heterogeneous(
        swarm_spec={"claude-cli": 1},
        base_env={"PATH": "/usr/bin"},
        socket_path=tmp_path / "sock",
        db_path=tmp_path / "db",
        doorbell_path=tmp_path / "doorbell",
        seed_scope_id="test",
        python_exe="/usr/bin/python3",
        stderr_dir=tmp_path,
        openai_api_key=None,
        since=anchor_event_id,
    )
    assert len(spawn_capture) == 1
    argv = spawn_capture[0]["argv"]
    assert "--since" in argv
    idx = argv.index("--since")
    assert argv[idx + 1] == anchor_event_id


def test_spawn_n_heterogeneous_omits_since_when_default_none(
    spawn_capture: list[dict[str, Any]],
    tmp_path: Path,
) -> None:
    """Spawn argv omits the ``--since`` token entirely when no replay cursor
    is supplied.

    Pins the default-None contract: a call site that has not yet adopted
    the replay-cursor flow produces the same argv it did before the
    parameter landed -- subscribe-from-live remains the default behaviour.
    """
    spawn_n_heterogeneous(
        swarm_spec={"claude-cli": 1},
        base_env={"PATH": "/usr/bin"},
        socket_path=tmp_path / "sock",
        db_path=tmp_path / "db",
        doorbell_path=tmp_path / "doorbell",
        seed_scope_id="test",
        python_exe="/usr/bin/python3",
        stderr_dir=tmp_path,
        openai_api_key=None,
    )
    assert len(spawn_capture) == 1
    argv = spawn_capture[0]["argv"]
    assert "--since" not in argv


def test_spawn_n_heterogeneous_shell_control_env_has_no_openai_key(
    spawn_capture: list[dict[str, Any]],
    tmp_path: Path,
) -> None:
    """End-to-end: a shell-control spawn's env is missing OPENAI_API_KEY
    EVEN WHEN the base_env carries it (ensures OPENAI_API_KEY does not
    leak to non-OpenAI drivers)."""
    spawn_n_heterogeneous(
        swarm_spec={"shell-control": 1},
        base_env={"PATH": "/usr/bin", "OPENAI_API_KEY": "sk-real-key"},
        socket_path=tmp_path / "sock",
        db_path=tmp_path / "db",
        doorbell_path=tmp_path / "doorbell",
        seed_scope_id="test",
        python_exe="/usr/bin/python3",
        stderr_dir=tmp_path,
        openai_api_key="sk-real-key",
    )
    assert len(spawn_capture) == 1
    assert "OPENAI_API_KEY" not in spawn_capture[0]["env"]


def test_spawn_n_heterogeneous_pydantic_env_has_openai_key(
    spawn_capture: list[dict[str, Any]],
    tmp_path: Path,
) -> None:
    """End-to-end: a pydantic spawn's env carries the OpenAI key."""
    spawn_n_heterogeneous(
        swarm_spec={"pydantic": 1},
        base_env={"PATH": "/usr/bin"},
        socket_path=tmp_path / "sock",
        db_path=tmp_path / "db",
        doorbell_path=tmp_path / "doorbell",
        seed_scope_id="test",
        python_exe="/usr/bin/python3",
        stderr_dir=tmp_path,
        openai_api_key="sk-real-key",
    )
    assert len(spawn_capture) == 1
    assert spawn_capture[0]["env"]["OPENAI_API_KEY"] == "sk-real-key"


def test_spawn_n_heterogeneous_full_5_driver_arm_env_isolation(
    spawn_capture: list[dict[str, Any]],
    tmp_path: Path,
) -> None:
    """Five drivers across all frameworks: only pydantic + langgraph have the key."""
    spawn_n_heterogeneous(
        swarm_spec={
            "pydantic": 1,
            "langgraph": 1,
            "claude-cli": 1,
            "gemini-cli": 1,
            "shell-control": 1,
        },
        base_env={"PATH": "/usr/bin", "OPENAI_API_KEY": "sk-real-key"},
        socket_path=tmp_path / "sock",
        db_path=tmp_path / "db",
        doorbell_path=tmp_path / "doorbell",
        seed_scope_id="test",
        python_exe="/usr/bin/python3",
        stderr_dir=tmp_path,
        openai_api_key="sk-real-key",
    )
    assert len(spawn_capture) == 5
    # Reconstruct framework <-> env mapping from argv.
    for capture in spawn_capture:
        role_argv_index = capture["argv"].index("scripts.stress._real_drivers") + 1
        role = capture["argv"][role_argv_index]
        if role in {"pydantic", "langgraph"}:
            assert capture["env"]["OPENAI_API_KEY"] == "sk-real-key"
        else:
            assert "OPENAI_API_KEY" not in capture["env"]


def test_spawn_n_heterogeneous_role_carries_single_framework_prefix(
    spawn_capture: list[dict[str, Any]],
    tmp_path: Path,
) -> None:
    """Pin: the role label prefixes the framework exactly once.

    Regression guard for the doubled-prefix bug. ``fw_id`` is a bare
    ordinal carrying no framework token, so ``role`` (``f"{framework}-{fw_id}"``)
    and the ``--fw-id`` argv value never embed the framework twice, and
    the framework rides ``_Child.framework`` as a first-class field.
    """
    children = spawn_n_heterogeneous(
        swarm_spec={"claude-cli": 2, "shell-control": 1},
        base_env={"PATH": "/usr/bin"},
        socket_path=tmp_path / "sock",
        db_path=tmp_path / "db",
        doorbell_path=tmp_path / "doorbell",
        seed_scope_id="test",
        python_exe="/usr/bin/python3",
        stderr_dir=tmp_path,
        openai_api_key=None,
    )
    assert len(children) == 3
    for child, capture in zip(children, spawn_capture, strict=True):
        framework = child.framework
        # The framework rode as a first-class field, not parsed from role.
        assert framework in {"claude-cli", "shell-control"}
        # Single prefix: strip the framework prefix once and the remainder
        # must be a bare ordinal with no further framework token.
        assert child.role.startswith(f"{framework}-")
        fw_id = child.role[len(framework) + 1 :]
        assert fw_id.isdigit(), f"fw_id {fw_id!r} is not a bare ordinal; doubled prefix?"
        assert framework not in fw_id
        # The wire ``--fw-id`` value matches the bare ordinal (no doubling).
        argv = capture["argv"]
        wire_fw_id = argv[argv.index("--fw-id") + 1]
        assert wire_fw_id == fw_id
        assert framework not in wire_fw_id
        # The driver subcommand is the bare framework, not a doubled token.
        driver_subcmd = argv[argv.index("scripts.stress._real_drivers") + 1]
        assert driver_subcmd == framework


def test_spawn_n_heterogeneous_zero_count_silently_skipped(
    spawn_capture: list[dict[str, Any]],
    tmp_path: Path,
) -> None:
    """A framework with count=0 is silently skipped."""
    spawn_n_heterogeneous(
        swarm_spec={"pydantic": 0, "shell-control": 2},
        base_env={"PATH": "/usr/bin"},
        socket_path=tmp_path / "sock",
        db_path=tmp_path / "db",
        doorbell_path=tmp_path / "doorbell",
        seed_scope_id="test",
        python_exe="/usr/bin/python3",
        stderr_dir=tmp_path,
        openai_api_key=None,
    )
    assert len(spawn_capture) == 2


# ---------------------------------------------------------------------
# default_python_executable.
# ---------------------------------------------------------------------


def test_default_python_executable_returns_sys_executable() -> None:
    """The helper returns the current Python interpreter's absolute path."""
    import sys

    assert default_python_executable() == sys.executable
