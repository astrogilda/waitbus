"""Unit tests for the ``waitbus mcp serve`` server.

Behaviour covered:

- ``_build_frame_emissions`` shapes (one frame → one emission triple;
  heartbeats drop; truncated frames still emit).
- ``_emit_frame`` calls the channel emitter and the resource emitter for
  one non-heartbeat frame.
- The reconnect backoff sequence walks 1s, 2s, 4s, 8s, 16s, then
  saturates at 30s.
- ``emit_claude_channel`` rejects channel meta keys outside ``[a-zA-Z0-9_]+``
  and non-string meta values.
- ``build_initialization_options`` advertises the
  ``experimental.claude/channel`` capability.
- The SDK initialize handshake against an in-memory stream pair returns
  the expected server name, version, and experimental capability.
- The SDK rewrite emits notifications matching both the Tier 1
  wire-baseline fixture (regression against the prior hand-rolled wire
  layer) and the Tier 2 channels-reference fixture (spec anchor).
- ``mcp.info()`` returns the correct JSON-serialisable shape; constants
  alignment (``PROTOCOL_VERSION == PROTOCOL_VERSIONS_SUPPORTED[-1]``); and
  ``PROTOCOL_VERSIONS_SUPPORTED`` is never empty.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import anyio
import pytest

from waitbus import mcp as mcp_mod
from waitbus._version import PACKAGE_VERSION

# --- _build_frame_emissions ------------------------------------------------


def _fixture_frame() -> dict[str, Any]:
    # Frozen broadcast wire protocol v1: kind="event" (the data-plane
    # discriminator), event_id is the canonical identity, and event_type
    # carries the open-value-space event class.
    return {
        "kind": "event",
        "event_id": "01HXZZZZZZZZZZZZZZZZZZZZZZ",
        "event_type": "workflow_run",
        "owner": "test-org",
        "repo": "test-repo",
        "received_at": 1700000000,
        "delivery_id": "test-delivery",
        "summary": "test run",
        "fields": {},
    }


def test_notification_frame_shapes() -> None:
    """One broadcast frame -> one (content, meta, event_id) emission triple."""
    frame = _fixture_frame()
    emissions = mcp_mod._build_frame_emissions(frame)

    assert len(emissions) == 1
    content, meta, event_id = emissions[0]

    # webhook-derived summary is now fenced as untrusted external data
    # before it reaches an agent (SEC: untrusted-field wrapping)
    assert content == ('<waitbus:untrusted label="event-summary">test run</waitbus:untrusted>')
    assert meta["repo"] == "test-org/test-repo"
    assert meta["id"] == "01HXZZZZZZZZZZZZZZZZZZZZZZ"
    assert meta["kind"] == "workflow_run"
    assert meta["conclusion"] == "pending"
    assert event_id == "01HXZZZZZZZZZZZZZZZZZZZZZZ"


def test_expected_frame_json_fixture_matches_emitted_payload() -> None:
    """The on-disk fixture pins the full wire shape — method AND payload."""
    expected = json.loads((Path(__file__).parent / "data" / "expected_mcp_frame.json").read_text())

    emissions = mcp_mod._build_frame_emissions(_fixture_frame())
    assert len(emissions) == 1
    content, meta, event_id = emissions[0]

    expected_claude = expected["claude_channel"]
    assert expected_claude["method"] == "notifications/claude/channel"
    assert expected_claude["params"]["content"] == content
    assert expected_claude["params"]["meta"] == meta

    expected_resources = expected["resources_updated"]
    assert expected_resources["method"] == "notifications/resources/updated"
    assert expected_resources["params"] == {"uri": f"waitbus://event/{event_id}"}


def test_heartbeat_dropped() -> None:
    """daemon_heartbeat frames produce zero emissions."""
    heartbeat = {"kind": "daemon_heartbeat", "ts": 1700000000.0, "uptime_sec": 1.0}
    assert mcp_mod._build_frame_emissions(heartbeat) == []


def test_subscribe_ack_dropped() -> None:
    """subscribe_ack control frames produce zero emissions (v1 wire control plane)."""
    ack = {
        "kind": "subscribe_ack",
        "proto": 1,
        "caught_up_at": None,
        "heartbeat_sec": 15,
        "max_frame_bytes": 65536,
    }
    assert mcp_mod._build_frame_emissions(ack) == []


def test_truncated_frame_emits_with_prefix() -> None:
    """Truncated frames surface a stub so operators know an event existed."""
    frame = {
        "kind": "truncated",
        "event_id": "01HXTRUNCATEDIDXXXXXXXXXXXX",
        "reason": "payload exceeds MAX_FRAME_BYTES",
        "max_frame_bytes": 65536,
    }
    emissions = mcp_mod._build_frame_emissions(frame)
    assert len(emissions) == 1
    content, meta, event_id = emissions[0]
    assert content.startswith("[truncated]")
    assert meta["kind"] == "truncated"
    assert event_id == "01HXTRUNCATEDIDXXXXXXXXXXXX"


# --- emit_claude_channel meta-key contract ---------------------------------


@pytest.mark.asyncio
async def test_emit_claude_channel_rejects_hyphenated_meta_key() -> None:
    session = AsyncMock()
    with pytest.raises(ValueError, match="must match"):
        await mcp_mod.emit_claude_channel(session, "ok", meta={"bad-key": "v"})


@pytest.mark.asyncio
async def test_emit_claude_channel_rejects_dotted_meta_key() -> None:
    session = AsyncMock()
    with pytest.raises(ValueError, match="must match"):
        await mcp_mod.emit_claude_channel(session, "ok", meta={"a.b": "v"})


@pytest.mark.asyncio
async def test_emit_claude_channel_rejects_non_string_meta_value() -> None:
    session = AsyncMock()
    with pytest.raises(ValueError, match="must be a string"):
        await mcp_mod.emit_claude_channel(
            session,
            "ok",
            meta={"k": 1},  # type: ignore[dict-item]
        )


@pytest.mark.asyncio
async def test_emit_claude_channel_send_message_envelope_shape() -> None:
    """Channel emission produces a bare JSONRPCNotification with the open method string."""
    session = AsyncMock()
    await mcp_mod.emit_claude_channel(session, "hello", meta={"repo": "o/r"})

    assert session.send_message.await_count == 1
    (sent,) = session.send_message.await_args.args
    notification = sent.message
    dumped = notification.model_dump_json(by_alias=True, exclude_none=True)
    parsed = json.loads(dumped)
    assert parsed["jsonrpc"] == "2.0"
    assert parsed["method"] == "notifications/claude/channel"
    assert parsed["params"] == {"content": "hello", "meta": {"repo": "o/r"}}
    assert "id" not in parsed


@pytest.mark.asyncio
async def test_emit_resource_updated_uses_sdk_typed_helper() -> None:
    """Resource emission delegates to ServerSession.send_resource_updated."""
    session = AsyncMock()
    await mcp_mod.emit_resource_updated(session, "waitbus://event/01ABC")
    assert session.send_resource_updated.await_count == 1
    (sent_uri,) = session.send_resource_updated.await_args.args
    assert str(sent_uri) == "waitbus://event/01ABC"


# --- _emit_frame -----------------------------------------------------------


@pytest.mark.asyncio
async def test_emit_frame_sends_both_notifications() -> None:
    """_emit_frame calls emit_claude_channel then emit_resource_updated."""
    session = AsyncMock()
    await mcp_mod._emit_frame(session, _fixture_frame())

    assert session.send_message.await_count == 1
    assert session.send_resource_updated.await_count == 1

    (sent_msg,) = session.send_message.await_args.args
    dumped = json.loads(sent_msg.message.model_dump_json(by_alias=True, exclude_none=True))
    assert dumped["method"] == "notifications/claude/channel"
    assert dumped["params"]["content"] == ('<waitbus:untrusted label="event-summary">test run</waitbus:untrusted>')
    assert dumped["params"]["meta"]["repo"] == "test-org/test-repo"

    (sent_uri,) = session.send_resource_updated.await_args.args
    assert str(sent_uri) == "waitbus://event/01HXZZZZZZZZZZZZZZZZZZZZZZ"


@pytest.mark.asyncio
async def test_emit_frame_skips_heartbeat() -> None:
    """_emit_frame emits nothing for daemon_heartbeat frames."""
    session = AsyncMock()
    await mcp_mod._emit_frame(session, {"kind": "daemon_heartbeat", "ts": 1.0})
    assert session.send_message.await_count == 0
    assert session.send_resource_updated.await_count == 0


# --- _stream_events backoff progression ------------------------------------


@pytest.mark.asyncio
async def test_backoff_on_missing_socket_grows_to_cap(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Sleeps follow 1s, 2s, 4s, ..., capped at 30s."""
    missing = tmp_path / "absent.sock"
    monkeypatch.setattr("waitbus._paths.broadcast_socket", lambda: missing)

    sleeps: list[float] = []
    real_sleep = asyncio.sleep

    async def fake_sleep(duration: float) -> None:
        sleeps.append(duration)
        if len(sleeps) >= 8:
            raise asyncio.CancelledError
        await real_sleep(0)

    monkeypatch.setattr("waitbus.mcp.asyncio.sleep", fake_sleep)

    session = AsyncMock()

    with pytest.raises(asyncio.CancelledError):
        await mcp_mod._stream_events(session)

    # Expected sequence: 1, 2, 4, 8, 16, 30, 30, 30 (saturated at cap).
    assert sleeps[:5] == [1.0, 2.0, 4.0, 8.0, 16.0]
    assert sleeps[5] == 30.0
    assert all(s <= 30.0 for s in sleeps)
    assert sleeps[-1] == 30.0


# --- Server construction smoke --------------------------------------------


def test_build_server_carries_package_version() -> None:
    """build_server() pins the SDK Server's advertised version to PACKAGE_VERSION."""
    server = mcp_mod.build_server()
    assert server.name == "waitbus"
    assert server.version == PACKAGE_VERSION


def test_build_initialization_options_advertises_claude_channel() -> None:
    """The SDK InitializationOptions advertise the experimental.claude/channel block."""
    server = mcp_mod.build_server()
    opts = mcp_mod.build_initialization_options(server)
    assert opts.server_name == "waitbus"
    assert opts.server_version == PACKAGE_VERSION
    assert opts.capabilities.experimental == {"claude/channel": {}}


# --- _load_filters --------------------------------------------------------


def test_load_filters_defaults_when_missing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Missing operator config -> default subscribe envelope {filters: [*]}."""
    # Point WAITBUS_CONFIG_DIR at an empty tmp_path; no config.toml
    # exists there so the pydantic defaults apply.
    monkeypatch.setenv("WAITBUS_CONFIG_DIR", str(tmp_path))
    from waitbus import _config as _config_mod

    _config_mod._reset_for_test()
    assert mcp_mod._load_filters() == {"filters": ["*"]}


def test_load_filters_propagates_provided_fields(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Optional fields in [mcp] flow through to the subscribe envelope."""
    cfg = tmp_path / "config.toml"
    cfg.write_text(
        '[mcp]\nfilter = ["org/repo"]\nevent_types = ["workflow_run"]\nsince = "01HZSINCEIDXXXXXXXXXXXXXXX"\n'
    )
    monkeypatch.setenv("WAITBUS_CONFIG_DIR", str(tmp_path))
    from waitbus import _config as _config_mod

    _config_mod._reset_for_test()
    # Token lives in systemd-creds (not the operator-edited TOML), so it is
    # intentionally absent from the subscribe envelope here. broadcast.py's
    # _lookup_token() reads it via _secrets at daemon startup.
    assert mcp_mod._load_filters() == {
        "filters": ["org/repo"],
        "event_types": ["workflow_run"],
        "since": "01HZSINCEIDXXXXXXXXXXXXXXX",
    }


# --- SDK initialize handshake (in-memory) --------------------------------


@pytest.mark.asyncio
async def test_sdk_initialize_handshake_returns_expected_result() -> None:
    """Drive a ServerSession against an in-memory anyio stream pair.

    Issues an ``initialize`` request and asserts the SDK responds with the
    server name, version, and experimental capability we configure.
    """
    from mcp.server.session import ServerSession
    from mcp.shared.message import SessionMessage
    from mcp.types import (
        ClientCapabilities,
        Implementation,
        InitializeRequest,
        InitializeRequestParams,
        JSONRPCMessage,
        JSONRPCRequest,
    )

    server = mcp_mod.build_server()
    init_options = mcp_mod.build_initialization_options(server)

    # anyio in-memory pair: client → server (read) and server → client (write).
    client_to_server_send, client_to_server_recv = anyio.create_memory_object_stream[SessionMessage | Exception](10)
    server_to_client_send, server_to_client_recv = anyio.create_memory_object_stream[SessionMessage](10)

    _ = InitializeRequest  # used by the request below
    _ = InitializeRequestParams
    _ = Implementation
    _ = ClientCapabilities

    async with ServerSession(client_to_server_recv, server_to_client_send, init_options) as session:  # noqa: F841
        # Send an initialize request from the "client".
        init_req = JSONRPCRequest(
            jsonrpc="2.0",
            id=1,
            method="initialize",
            params={
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "test-client", "version": "0.0.1"},
            },
        )
        await client_to_server_send.send(SessionMessage(message=JSONRPCMessage(root=init_req)))

        # Read the InitializeResult response.
        with anyio.fail_after(2.0):
            response_msg = await server_to_client_recv.receive()

        # Close every memory-object stream so anyio releases the
        # underlying objects eagerly. Closing only the send side of one
        # pair leaves three half-open streams that Python 3.14's
        # ResourceWarning finalizer surfaces at GC time.
        await client_to_server_send.aclose()
        await client_to_server_recv.aclose()
        await server_to_client_send.aclose()
        await server_to_client_recv.aclose()

    dumped = json.loads(response_msg.message.model_dump_json(by_alias=True, exclude_none=True))
    assert dumped["jsonrpc"] == "2.0"
    assert dumped["id"] == 1
    result = dumped["result"]
    assert result["serverInfo"]["name"] == "waitbus"
    assert result["serverInfo"]["version"] == PACKAGE_VERSION
    assert "claude/channel" in result["capabilities"]["experimental"]


# --- Two-tier wire fixture conformance ------------------------------------


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    """Parse a fixture .jsonl file, stripping ``#`` comment lines and blanks."""
    frames: list[dict[str, Any]] = []
    for line in path.read_text().splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        frames.append(json.loads(s))
    return frames


@pytest.mark.asyncio
async def test_tier1_baseline_channel_envelope_matches_sdk_emission() -> None:
    """Tier 1: SDK channel-notification frame parses to the baseline envelope."""
    baseline = _load_jsonl(Path(__file__).parent / "data" / "mcp_wire_baseline.jsonl")
    # The middle line is the channel envelope (after the initialize result).
    expected = next(f for f in baseline if f.get("method") == "notifications/claude/channel")

    # Drive the real projection so the inputs are the genuine v1-wire
    # (content, meta) the production builder produces — a true round-trip,
    # not a hand-transcribed pair that could drift from _build_frame_emissions.
    emissions = mcp_mod._build_frame_emissions(_fixture_frame())
    assert len(emissions) == 1
    content, meta, _event_id = emissions[0]

    session = AsyncMock()
    await mcp_mod.emit_claude_channel(session, content, meta=meta)

    (sent,) = session.send_message.await_args.args
    emitted = json.loads(sent.message.model_dump_json(by_alias=True, exclude_none=True))
    assert emitted == expected


def test_tier2_spec_reference_channel_envelope_is_well_formed() -> None:
    """Tier 2: spec-reference channel envelope has the channels-reference shape."""
    spec_frames = _load_jsonl(Path(__file__).parent / "data" / "mcp_wire_spec_reference.jsonl")
    channel = next(f for f in spec_frames if f.get("method") == "notifications/claude/channel")
    assert channel["jsonrpc"] == "2.0"
    assert "id" not in channel  # notification, not request
    assert isinstance(channel["params"]["content"], str)
    # Every meta key must be identifier-form.
    import re

    for k in channel["params"]["meta"]:
        assert re.match(r"^[a-zA-Z0-9_]+$", k), f"meta key {k!r} violates contract"

    resource = next(f for f in spec_frames if f.get("method") == "notifications/resources/updated")
    assert resource["params"] == {"uri": "waitbus://event/01ABC"}


@pytest.mark.asyncio
async def test_tier2_spec_reference_channel_round_trip_matches_sdk_emission() -> None:
    """Tier 2: SDK emission of the spec-reference inputs matches the fixture frame."""
    spec_frames = _load_jsonl(Path(__file__).parent / "data" / "mcp_wire_spec_reference.jsonl")
    expected = next(f for f in spec_frames if f.get("method") == "notifications/claude/channel")

    session = AsyncMock()
    await mcp_mod.emit_claude_channel(
        session,
        expected["params"]["content"],
        meta=dict(expected["params"]["meta"]),
    )
    (sent,) = session.send_message.await_args.args
    emitted = json.loads(sent.message.model_dump_json(by_alias=True, exclude_none=True))
    assert emitted == expected


# --- Protocol version range advertisement ---------------------------------


def test_protocol_versions_supported_is_non_empty() -> None:
    """PROTOCOL_VERSIONS_SUPPORTED must never be emptied accidentally."""
    from waitbus._mcp_constants import PROTOCOL_VERSIONS_SUPPORTED

    assert len(PROTOCOL_VERSIONS_SUPPORTED) > 0, (
        "PROTOCOL_VERSIONS_SUPPORTED is empty; every waitbus install must "
        "support at least the version it was built against"
    )


def test_protocol_version_equals_last_supported() -> None:
    """PROTOCOL_VERSION is always PROTOCOL_VERSIONS_SUPPORTED[-1]."""
    from waitbus._mcp_constants import PROTOCOL_VERSION, PROTOCOL_VERSIONS_SUPPORTED

    assert PROTOCOL_VERSIONS_SUPPORTED[-1] == PROTOCOL_VERSION, (
        f"PROTOCOL_VERSION ({PROTOCOL_VERSION!r}) must equal "
        f"PROTOCOL_VERSIONS_SUPPORTED[-1] ({PROTOCOL_VERSIONS_SUPPORTED[-1]!r}); "
        "bump the tuple, not the scalar"
    )


def test_mcp_info_returns_expected_shape() -> None:
    """mcp.info() returns a dict with the required keys and correct types."""
    record = mcp_mod.info()

    assert record["name"] == "waitbus"
    assert record["version"] == PACKAGE_VERSION
    # protocolVersion must be the latest supported version.
    from waitbus._mcp_constants import PROTOCOL_VERSION, PROTOCOL_VERSIONS_SUPPORTED

    assert record["protocolVersion"] == PROTOCOL_VERSION
    # supportedProtocolVersions is a non-empty list of strings.
    supported = record["supportedProtocolVersions"]
    assert isinstance(supported, list)
    assert len(supported) > 0
    assert all(isinstance(v, str) for v in supported)
    # The list must end with the targeted version.
    assert supported[-1] == PROTOCOL_VERSION
    # The list must equal the canonical constant (order-preserving).
    assert supported == list(PROTOCOL_VERSIONS_SUPPORTED)


def test_mcp_info_is_json_serialisable() -> None:
    """mcp.info() output round-trips through json.dumps / json.loads."""
    record = mcp_mod.info()
    serialised = json.loads(json.dumps(record))
    assert serialised["name"] == record["name"]
    assert serialised["version"] == record["version"]
    assert serialised["protocolVersion"] == record["protocolVersion"]
    assert serialised["supportedProtocolVersions"] == record["supportedProtocolVersions"]


def test_mcp_info_supported_versions_align_with_sdk() -> None:
    """PROTOCOL_VERSIONS_SUPPORTED mirrors mcp.shared.version.SUPPORTED_PROTOCOL_VERSIONS.

    This test fails loudly when the SDK pin is bumped but the constant is not
    updated, or vice versa.  Fix: update PROTOCOL_VERSIONS_SUPPORTED in
    waitbus/_mcp_constants.py to match the new SDK list.
    """
    from mcp.shared.version import SUPPORTED_PROTOCOL_VERSIONS as SDK_SUPPORTED

    from waitbus._mcp_constants import PROTOCOL_VERSIONS_SUPPORTED

    assert list(PROTOCOL_VERSIONS_SUPPORTED) == list(SDK_SUPPORTED), (
        f"waitbus._mcp_constants.PROTOCOL_VERSIONS_SUPPORTED "
        f"{list(PROTOCOL_VERSIONS_SUPPORTED)} does not match the pinned SDK's "
        f"mcp.shared.version.SUPPORTED_PROTOCOL_VERSIONS {list(SDK_SUPPORTED)}; "
        "update PROTOCOL_VERSIONS_SUPPORTED after bumping the mcp pin"
    )
