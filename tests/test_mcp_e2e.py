"""End-to-end MCP server validation: synthetic broadcast → stdio notifications.

This test stands up an AF_UNIX SOCK_STREAM listener at the path the
MCP server expects (via ``WAITBUS_RUNTIME_DIR`` override), starts the
real ``waitbus-mcp`` console-script as a subprocess, drives a full
initialize handshake on stdin, lets the subscriber loop connect to the
synthetic broadcast socket, sends one length-prefix-framed frame, and
asserts that both notification frames appear on the server's stdout with
the exact expected payload shape.

This guards against the class of bugs that ``test_mcp.py``'s
helper-function tests can miss — anything that only manifests when the
actual SDK ``send_notification`` path is exercised against real stdio.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from waitbus._frame import encode_frame, sync_read_frame

pytestmark = [
    pytest.mark.slow,
    pytest.mark.skipif(
        sys.platform != "linux",
        reason="broadcast daemon AF_UNIX socket is Linux-only",
    ),
    pytest.mark.skipif(
        not shutil.which("waitbus"),
        reason="waitbus console-script not on PATH (install the wheel first)",
    ),
]


_SYNTHETIC_FRAME: dict[str, Any] = {
    "kind": "event",
    "event_id": "01HXSYNTHETIC2026ZZZZZZZZZZ",
    "event_type": "workflow_run",
    "owner": "synth-org",
    "repo": "synth-repo",
    "received_at": 1700000000,
    "delivery_id": "synth-001",
    "summary": "synthetic test event",
    "fields": {"run_id": 42, "conclusion": "success", "head_sha": "deadbeef"},
}


def _run_synthetic_broadcast(socket_path: Path, frame: dict[str, Any], ready: threading.Event) -> None:
    """Bind socket_path, accept one connection, send one length-prefix-framed frame, exit."""
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(str(socket_path))
    srv.listen(1)
    ready.set()

    conn, _ = srv.accept()
    conn.setblocking(True)
    sync_read_frame(conn)  # drain the subscribe envelope (length-prefix-framed)
    wire = encode_frame(json.dumps(frame, separators=(",", ":"), default=str).encode("utf-8"))
    conn.sendall(wire)
    time.sleep(0.3)  # let the MCP server emit before we close
    conn.close()
    srv.close()


def test_e2e_synthetic_broadcast_emits_notifications(tmp_path: Path) -> None:
    """Real stdio round-trip: frame in → two notification frames out."""
    runtime = tmp_path / "runtime"
    runtime.mkdir(mode=0o700)
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    socket_path = runtime / "broadcast.sock"

    ready = threading.Event()
    broadcast_thread = threading.Thread(
        target=_run_synthetic_broadcast,
        args=(socket_path, _SYNTHETIC_FRAME, ready),
        daemon=True,
    )
    broadcast_thread.start()
    assert ready.wait(timeout=5.0), "synthetic broadcast did not bind in time"

    env = os.environ.copy()
    env["WAITBUS_RUNTIME_DIR"] = str(runtime)
    env["WAITBUS_STATE_DIR"] = str(state)

    proc = subprocess.Popen(
        ["waitbus", "mcp", "serve"],
        env=env,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert proc.stdin is not None
        # Advertise the claude/channel experimental capability so the
        # server's capability gate (FIX-4) emits the channel notification;
        # a client that omits this correctly receives no channel traffic.
        proc.stdin.write(
            '{"jsonrpc":"2.0","id":1,"method":"initialize","params":'
            '{"protocolVersion":"2025-06-18",'
            '"capabilities":{"experimental":{"claude/channel":{}}},'
            '"clientInfo":{"name":"e2e-test","version":"0.0.1"}}}\n'
        )
        proc.stdin.write('{"jsonrpc":"2.0","method":"notifications/initialized"}\n')
        proc.stdin.flush()

        broadcast_thread.join(timeout=5.0)
        time.sleep(0.5)  # allow stdio flushes
        proc.terminate()
        try:
            stdout, _ = proc.communicate(timeout=5.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, _ = proc.communicate()
    finally:
        if proc.poll() is None:
            proc.kill()

    _assert_e2e_notifications(stdout)


def _assert_e2e_notifications(stdout: str) -> None:
    """Parse the MCP server's stdio and assert both notification kinds + their content.

    Extracted from the test body so the per-test cyclomatic complexity
    stays focused on the orchestration (broadcast + subprocess + stdin
    drive); the assertion ladder lives here. Asserts:

    * Stdout is a JSONL stream of MCP messages (one JSON object per line).
    * At least one ``notifications/claude/channel`` notification was emitted
      (the client advertised the claude/channel experimental capability).
    * The claude/channel content is wrapped in the waitbus untrusted-field
      envelope (defense against prompt injection through webhook-derived text).
    * The claude/channel meta carries the synthetic-event identifiers
      (repo, kind, id, run_id, conclusion).
    * NO ``notifications/resources/updated`` is emitted: the event URI is
      not subscribable (FIX-2) and the client never sent resources/subscribe,
      so the spec forbids any resources/updated for it.

    The synthetic-event identifiers are hard-coded against
    ``_SYNTHETIC_FRAME``; any drift in the frame's contents flows
    through both producers (the broadcast emitter) and this consumer
    (the assertion ladder), so the test stays a true round-trip
    rather than a self-consistency check.
    """
    messages = []
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            messages.append(json.loads(line))
        except json.JSONDecodeError:
            continue

    claude_notifs = [m for m in messages if m.get("method") == "notifications/claude/channel"]
    resource_notifs = [m for m in messages if m.get("method") == "notifications/resources/updated"]

    assert claude_notifs, f"no claude/channel notification emitted; got {messages!r}"
    # FIX-2: the event URI is not subscribable and the client never
    # subscribed, so zero resources/updated is the conformant outcome.
    assert not resource_notifs, f"unexpected resources/updated for an unsubscribed client; got {resource_notifs!r}"

    claude_params = claude_notifs[0]["params"]
    # webhook-derived summary is fenced as untrusted external data
    # before it reaches an agent (SEC: untrusted-field wrapping)
    assert claude_params["content"] == (
        '<waitbus:untrusted label="event-summary">synthetic test event</waitbus:untrusted>'
    )
    assert claude_params["meta"]["repo"] == "synth-org/synth-repo"
    assert claude_params["meta"]["kind"] == "workflow_run"
    assert claude_params["meta"]["id"] == "01HXSYNTHETIC2026ZZZZZZZZZZ"
    assert claude_params["meta"]["run_id"] == "42"
    assert claude_params["meta"]["conclusion"] == "success"
