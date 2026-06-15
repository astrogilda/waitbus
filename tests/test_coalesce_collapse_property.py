"""Property test: coalesce_replay emits the latest-event_id frame per entity.

For a backlog of workflow_run frames, the emitted set must equal the
max-event_id frame per unique run_id, ordered by event_id ascending.
"""

from __future__ import annotations

import json
import socket
import struct
import threading
from typing import Any

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from waitbus._broadcast_sub import SubscriberHandle, WaitOutcome
from waitbus._frame import encode_frame
from waitbus.coalesce import coalesce_replay

# ---------------------------------------------------------------------------
# Wire-frame helpers
# ---------------------------------------------------------------------------

_IDLE_SECONDS = 0.4


def _decode_frame_bytes(raw: bytes) -> dict[str, Any]:
    """Unpack one length-prefixed frame and return the decoded dict."""
    (length,) = struct.unpack(">I", raw[:4])
    result: dict[str, Any] = json.loads(raw[4 : 4 + length])
    return result


# ---------------------------------------------------------------------------
# Hypothesis strategies
# ---------------------------------------------------------------------------


@st.composite
def workflow_run_frame_bytes(draw: st.DrawFn) -> bytes:
    """Build one length-prefixed JSON-encoded workflow_run frame."""
    event_id = draw(
        st.text(
            alphabet="0123456789ABCDEFGHJKMNPQRSTVWXYZ",  # Crockford base32
            min_size=26,
            max_size=26,
        )
    )
    run_id = draw(st.integers(min_value=1, max_value=10**12))
    conclusion = draw(st.sampled_from([None, "success", "failure", "cancelled"]))
    frame: dict[str, Any] = {
        "event_id": event_id,
        "kind": "event",
        "event_type": "workflow_run",
        "owner": "acme",
        "repo": "widgets",
        "received_at": 0,
        "delivery_id": f"gh:{event_id}",
        "summary": "",
        "fields": {
            "source": "github",
            "event_type": "workflow_run",
            "run_id": run_id,
            "conclusion": conclusion,
            "status": "completed" if conclusion else "in_progress",
        },
    }
    return encode_frame(json.dumps(frame, separators=(",", ":")).encode("utf-8"))


# ---------------------------------------------------------------------------
# Drive helper (mirrors tests/test_coalesce.py::_drive)
# ---------------------------------------------------------------------------


def _drive(
    bytes_to_send: list[bytes],
    *,
    idle_seconds: float = _IDLE_SECONDS,
) -> tuple[WaitOutcome, list[dict[str, Any]]]:
    """Run coalesce_replay against a socketpair pre-loaded with frames."""
    server, client = socket.socketpair()
    try:
        for chunk in bytes_to_send:
            server.sendall(chunk)

        emitted: list[dict[str, Any]] = []

        def _emit(frame: dict[str, Any]) -> None:
            emitted.append(frame)

        outcome_holder: list[WaitOutcome] = []

        def _run() -> None:
            outcome_holder.append(
                coalesce_replay(
                    SubscriberHandle(sock=client),
                    emit=_emit,
                    idle_seconds=idle_seconds,
                    cursor=None,
                    live_tail=False,
                )
            )

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        t.join(timeout=idle_seconds + 5.0)
        assert not t.is_alive(), "coalesce_replay did not exit within the idle window"
        return outcome_holder[0], emitted
    finally:
        server.close()
        client.close()


# ---------------------------------------------------------------------------
# Property test
# ---------------------------------------------------------------------------


@given(frames_bytes=st.lists(workflow_run_frame_bytes(), max_size=24))
@settings(
    max_examples=50,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.large_base_example],
)
def test_coalesce_collapse_emits_latest_event_id_per_entity(
    frames_bytes: list[bytes],
) -> None:
    """The emitted frames equal the latest-event_id-per-entity projection.

    For workflow_run frames keyed on (github, run, run_id), each entity
    appears exactly once in the output (the highest-event_id frame for
    that entity), in monotonic event_id order.
    """
    # Decode input frames to reconstruct the model's expected projection.
    decoded_inputs: list[dict[str, Any]] = [_decode_frame_bytes(raw) for raw in frames_bytes]

    # Build expected: for each run_id keep the frame with the max event_id.
    # event_id strings are Crockford base32 ULIDs — lexicographic max == chronological max.
    best_by_run_id: dict[int, dict[str, Any]] = {}
    for frame in decoded_inputs:
        run_id: int = frame["fields"]["run_id"]
        current_best = best_by_run_id.get(run_id)
        if current_best is None or frame["event_id"] > current_best["event_id"]:
            best_by_run_id[run_id] = frame

    # Expected output is sorted by event_id ascending.
    expected_ids = sorted(f["event_id"] for f in best_by_run_id.values())

    # Run coalesce_replay and collect emitted frames.
    outcome, emitted = _drive(frames_bytes)

    assert outcome.timed_out is True
    assert [f["event_id"] for f in emitted] == expected_ids
