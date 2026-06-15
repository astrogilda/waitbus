"""Gating tests for ``benchmarks._bench_llm_agent_pool.LlmAgentPool``.

Full end-to-end driver spawn requires a live waitbus daemon AND every
LLM CLI authenticated; those live in the bench's integration tests.
This module covers the construction + no-op contract.
"""

from __future__ import annotations

import io
import os
from pathlib import Path
from typing import Any

import pytest

from benchmarks._bench_llm_agent_pool import (
    LlmAgentPool,
    LlmAgentPoolError,
    LlmAgentPoolResults,
)


def _make_pool(tmp_path: Path, *, frameworks: tuple[str, ...]) -> LlmAgentPool:
    return LlmAgentPool(
        frameworks=frameworks,
        env=os.environ.copy(),
        socket_path=tmp_path / "broadcast.sock",
        db_path=tmp_path / "db",
        doorbell_path=tmp_path / "doorbell.sock",
        seed_scope_id="bench-test",
        python_exe="/usr/bin/python3",
        stderr_dir=tmp_path / "stderr",
    )


def test_empty_frameworks_tuple_is_no_op(tmp_path: Path) -> None:
    """Empty frameworks tuple: spawn / settle / collect all return cleanly."""
    pool = _make_pool(tmp_path, frameworks=())
    with pool:
        pool.spawn()
        pool.settle()
        results = pool.collect()
        assert isinstance(results, LlmAgentPoolResults)
        assert results.reacted_count == 0
        assert results.framework_mix == {}
        assert pool.attrition_detected is False
        assert pool.agent_count == 0


def test_invalid_framework_string_raises_at_init(tmp_path: Path) -> None:
    """Non-string or empty-string entries in frameworks are rejected at construction."""
    with pytest.raises(ValueError, match="frameworks must be a tuple"):
        LlmAgentPool(
            frameworks=("",),
            env=os.environ.copy(),
            socket_path=tmp_path,
            db_path=tmp_path,
            doorbell_path=tmp_path,
            seed_scope_id="x",
            python_exe="/usr/bin/python3",
            stderr_dir=tmp_path,
        )


def test_agent_count_property_derives_from_frameworks(tmp_path: Path) -> None:
    """agent_count is the length of the frameworks tuple."""
    pool = _make_pool(tmp_path, frameworks=("pydantic", "langgraph", "shell"))
    assert pool.agent_count == 3


def test_framework_mix_is_empty_before_spawn(tmp_path: Path) -> None:
    """framework_mix is an empty dict before spawn() is called."""
    pool = _make_pool(tmp_path, frameworks=("pydantic", "langgraph"))
    assert pool.framework_mix == {}


def test_collect_before_spawn_returns_empty(tmp_path: Path) -> None:
    """collect() is safe to call without spawn (returns degenerate results)."""
    pool = _make_pool(tmp_path, frameworks=("pydantic", "langgraph"))
    with pool:
        results = pool.collect()
        assert results.reacted_count == 0
        assert results.per_child == []


def test_teardown_is_idempotent(tmp_path: Path) -> None:
    """teardown() can be called twice without raising."""
    pool = _make_pool(tmp_path, frameworks=())
    with pool:
        pool.teardown()
        pool.teardown()  # second call is no-op


def test_settle_no_op_when_no_frameworks(tmp_path: Path) -> None:
    """settle() with empty frameworks returns immediately."""
    pool = _make_pool(tmp_path, frameworks=())
    with pool:
        pool.spawn()
        pool.settle(timeout_sec=0.05)


def test_pool_error_is_runtime_error_subclass() -> None:
    """LlmAgentPoolError is a RuntimeError subclass (catchable via the parent)."""
    assert issubclass(LlmAgentPoolError, RuntimeError)


@pytest.fixture
def pool_spawn_capture(monkeypatch: pytest.MonkeyPatch) -> list[list[str]]:
    """Patch the controller's ``subprocess.Popen`` so ``pool.spawn`` captures
    each driver argv without launching a real subprocess."""
    captured: list[list[str]] = []

    class _FakeProc:
        def __init__(self) -> None:
            self.pid = 99999
            self.returncode: int | None = 0
            self.stdout = io.BytesIO(b"")

        def poll(self) -> int | None:
            return 0

        def wait(self, timeout: float | None = None) -> int:
            return 0

        def communicate(self, timeout: float | None = None) -> tuple[bytes, bytes]:
            return b"", b""

        def terminate(self) -> None:
            return None

        def kill(self) -> None:
            return None

        def send_signal(self, _sig: int) -> None:
            return None

    def fake_popen(argv: list[str], **_kwargs: Any) -> _FakeProc:
        captured.append(list(argv))
        return _FakeProc()

    monkeypatch.setattr("scripts.stress._controller.subprocess.Popen", fake_popen)
    return captured


def test_pool_spawn_forwards_cold_prefix(pool_spawn_capture: list[list[str]], tmp_path: Path) -> None:
    """``pool.spawn(cold_prefix=...)`` threads ``--cold-prefix <value>`` into
    the driver argv so the spawned driver prepends it to the LLM prompt -- the
    per-run cold-cache salt the multistream bench depends on to keep
    ``cache_contaminated_count`` accurate across separate runs."""
    pool = _make_pool(tmp_path, frameworks=("claude-cli",))
    pool.spawn(cold_prefix="iter-prefix-42")
    try:
        assert len(pool_spawn_capture) == 1
        argv = pool_spawn_capture[0]
        assert "--cold-prefix" in argv
        assert argv[argv.index("--cold-prefix") + 1] == "iter-prefix-42"
    finally:
        pool.teardown()


def test_pool_spawn_omits_cold_prefix_when_empty(pool_spawn_capture: list[list[str]], tmp_path: Path) -> None:
    """The default empty cold_prefix emits no ``--cold-prefix`` flag, so the
    stress-harness path (which passes nothing) keeps the canonical prompt."""
    pool = _make_pool(tmp_path, frameworks=("claude-cli",))
    pool.spawn()
    try:
        assert len(pool_spawn_capture) == 1
        assert "--cold-prefix" not in pool_spawn_capture[0]
    finally:
        pool.teardown()
