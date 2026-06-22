"""End-to-end wire test: a real MCP client drives ``waitbus mcp serve`` over stdio.

Regression guard for the serve-loop dispatch gap. The pull tools were
registered (``@server.list_tools`` / ``@server.call_tool``) and unit-tested at the
implementation layer (``_tool_*_impl``), but ``mcp.main_async`` drained the
incoming-request stream WITHOUT dispatching it -- it never called
``server.run`` / ``server._handle_message`` -- so no MCP client could call a tool
over the wire. ``initialize`` succeeded; every ``tools/list`` and ``tools/call``
hung. Impl-layer tests cannot catch this: they prove the function works, not that
a client can reach it. This test spawns the real server subprocess and asserts the
pull tools answer end to end.

Marked ``slow``: it forks a Python subprocess and runs the full MCP stdio
handshake. Every client call is wrapped in ``asyncio.wait_for`` so a regression
(the dispatch loop draining again) FAILS FAST instead of hanging the suite.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import pytest
from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from pydantic import AnyUrl

from waitbus import _db
from waitbus._mcp_subscriptions import URI_CURRENT

pytestmark = pytest.mark.slow

# Bootstrap the CLI without depending on the console-script location: run the
# typer app directly so `<python> -c <bootstrap> mcp serve` is the server.
_BOOTSTRAP = "from waitbus.cli.main import app; app()"

# Wire-call ceiling. A regressed (drain-only) serve loop never answers, so these
# calls would hang forever; wait_for turns that into a fast, clear failure.
_CALL_TIMEOUT_S = 20.0

_EXPECTED_TOOLS = {"query_ci", "get_event", "tail_events"}


def _seed_workflow_run(state_dir: Path) -> None:
    """Insert one terminal workflow_run so get_ci_status has a row to return."""
    db_path = state_dir / "github.db"
    _db.ensure_schema(db_path)
    with _db.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO events (delivery_id, source, event_type, owner, repo, "
            "run_id, status, conclusion, received_at, payload_json, "
            "ingest_method, job_id, parent_run_id, event_id) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "wire-e2e:1",
                "github_webhook",
                "workflow_run",
                "example-org",
                "waitbus",
                42,
                "completed",
                "success",
                1_700_000_000_000_000_000,
                "{}",
                "webhook",
                None,
                None,
                "01HZWIREE2E000000000000042",
            ),
        )
        conn.commit()


async def test_pull_tools_answer_over_stdio(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    runtime_dir = tmp_path / "runtime"
    state_dir.mkdir()
    runtime_dir.mkdir()
    _seed_workflow_run(state_dir)

    env = {
        **os.environ,
        "WAITBUS_STATE_DIR": str(state_dir),
        "WAITBUS_RUNTIME_DIR": str(runtime_dir),
        "WAITBUS_LOG_LEVEL": "ERROR",
    }
    params = StdioServerParameters(
        command=sys.executable,
        args=["-c", _BOOTSTRAP, "mcp", "serve"],
        env=env,
    )

    async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
        await asyncio.wait_for(session.initialize(), timeout=_CALL_TIMEOUT_S)

        tools = await asyncio.wait_for(session.list_tools(), timeout=_CALL_TIMEOUT_S)
        assert {t.name for t in tools.tools} >= _EXPECTED_TOOLS

        # tools/call dispatch, with real seeded data round-tripped.
        ci = await asyncio.wait_for(
            session.call_tool("query_ci", {"view": "status", "repo": "example-org/waitbus"}),
            timeout=_CALL_TIMEOUT_S,
        )
        assert ci.structuredContent is not None
        runs = ci.structuredContent["runs"]
        assert runs, "query_ci status returned no runs over the wire"
        assert runs[0]["run_id"] == 42
        assert runs[0]["conclusion"] == "success"

        # A second view proves dispatch is generic (not status-specific).
        # Every tool and the resources below ride the identical
        # _handle_message path, so two calls + a resource read fully
        # exercise the serve-loop dispatch the bug had severed.
        failed = await asyncio.wait_for(
            session.call_tool("query_ci", {"view": "failed_jobs"}),
            timeout=_CALL_TIMEOUT_S,
        )
        assert failed.structuredContent is not None
        assert "jobs" in failed.structuredContent

        # resources/read dispatch -- resources ride the same _handle_message path
        # and were equally unreachable over the wire before the fix.
        current = await asyncio.wait_for(
            session.read_resource(AnyUrl(URI_CURRENT)),
            timeout=_CALL_TIMEOUT_S,
        )
        assert current.contents, "waitbus://current returned no contents over the wire"
