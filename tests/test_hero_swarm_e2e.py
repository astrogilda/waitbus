"""CLAIM-2 proof: cross-harness failure broadcast on one local bus.

This is the load-bearing end-to-end test for the launch wedge: TWO genuinely
different agent frameworks -- Pydantic AI and LangGraph -- run as SEPARATE OS
processes, each parked (blocked, zero polling) in the waitbus SDK ``wait_for`` on
one real broadcast daemon. A third process (a Pydantic AI worker) FAILS and
emits one ``agent_task_failed`` event; the test asserts that BOTH peers, on two
DIFFERENT frameworks, observably wake on that single cross-process failure
broadcast.

Single-process tests cannot prove cross-harness failure broadcast. ``waitbus swarm-demo``
already proves the coordination primitive in-process; what it cannot prove is the
cross-harness case, because there is one process and one framework. This test exercises the
real ``subprocess`` topology -- real ``waitbus broadcast serve``, real ``waitbus top``,
and the two frameworks as distinct child processes over an AF_UNIX socket -- so a
green test means "framework A in process X woke framework B in process Y", which is
the wedge claim.

What the test does not use. The agents run with FAKE models (Pydantic AI
``TestModel`` / LangGraph ``FakeListChatModel``); no real LLMs, no network. The
failure event is INJECTED (a deterministic ``emit``, not a real crash). The
``agent`` source is a first-class built-in waitbus source, owning
``agent_message`` / ``agent_claim`` / ``agent_task_failed``; the worker emits
against it directly, with no in-process registration step.

Teardown. ``run_hero_demo`` supervises every child in its own process group and
tears them all down (``SIGTERM`` -> grace -> group ``SIGKILL``) on exit, so this
test leaves zero orphan processes and zero leaked sockets (it asserts the temp
runtime dir's sockets are gone after the run).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("pydantic_ai")
pytest.importorskip("langgraph")

from examples.hero_swarm.orchestrate import main, run_hero_demo

pytestmark = [
    pytest.mark.slow,
    pytest.mark.skipif(
        sys.platform != "linux",
        reason="broadcast daemon AF_UNIX SO_PEERCRED + own-group supervision are Linux-only",
    ),
]


def test_cross_harness_failure_broadcast(tmp_path: Path) -> None:
    """Two DIFFERENT frameworks (separate processes) wake on ONE peer's failure.

    Runs the full real-process demo: real broadcast daemon, two peer agents on
    two different frameworks parked in ``wait_for``, a ``waitbus top`` view, and a
    failing worker that emits one ``agent_task_failed`` event. Asserts both peers
    observably woke (the cross-harness proof) and that teardown left no sockets.
    """
    state_dir = tmp_path / "state"
    runtime_dir = tmp_path / "runtime"
    state_dir.mkdir(mode=0o700)
    runtime_dir.mkdir(mode=0o700)

    result = run_hero_demo(state_dir, runtime_dir)

    # Both DIFFERENT frameworks must have observably woken on the single failure
    # event -- this is the cross-harness claim, not just "one agent reacted".
    assert result.peer_woke.get("pydantic-ai") is True, (
        f"Pydantic AI peer did not wake on the cross-harness failure:\n{result.peer_output.get('pydantic-ai')!r}"
    )
    assert result.peer_woke.get("langgraph") is True, (
        f"LangGraph peer did not wake on the cross-harness failure:\n{result.peer_output.get('langgraph')!r}"
    )
    assert result.cross_harness_proven, "fewer than two distinct frameworks woke"

    # Each peer's wake marker must carry the EXACT delivery_id the worker emitted,
    # proving the specific event round-tripped across the process boundary to that
    # framework -- a full-line, event-identity match, not a substring coincidence.
    assert result.delivery_id, "orchestrator did not mint a delivery_id"
    for framework in ("pydantic-ai", "langgraph"):
        out = result.peer_output.get(framework) or ""
        prefix = f"HERO_PEER_WOKE framework={framework} "
        want = f"delivery={result.delivery_id}"
        assert any(line.startswith(prefix) and want in line.split() for line in out.splitlines()), (
            f"{framework} peer marker missing {want}:\n{out!r}"
        )

    # Deterministic teardown: the supervised children are gone, so their AF_UNIX
    # sockets must have been cleaned up (zero leaked sockets/processes).
    assert not (runtime_dir / "broadcast.sock").exists(), "broadcast socket leaked after teardown"
    assert not (runtime_dir / "doorbell.sock").exists(), "doorbell socket leaked after teardown"


def test_failing_worker_role_requires_explicit_paths() -> None:
    # The emitting worker MUST be handed the demo daemon's db / doorbell / event
    # id explicitly; argparse rejects a hand-run worker that omits them, so it can
    # never emit against the operator's real default database.
    for argv in (
        ["failing-worker", "--socket", "/tmp/x"],
        ["failing-worker", "--socket", "/tmp/x", "--db", "/tmp/d.sqlite"],
    ):
        with pytest.raises(SystemExit):
            main(argv)
