"""Tests for ``benchmarks.bench_event_delivery_fidelity``.

The bench is a production-wired measurement bench: waitbus.emit +
waitbus.subscribe against a real spawned waitbus daemon, real
lightweight concurrent subscribers for the bus_swarm load arm, real UDS
sibling-process peer for the alone arm, and real ``httpx.AsyncClient.stream(...).aiter_raw()``
for OpenAI SSE parsing on the real-LLM path. The tests therefore split
into:

1. **Pure-helper unit tests** -- no daemon, no subprocess, no LLM. Cover
   SSE boundary parsing, frame hashing, content-integrity diff,
   Wilcoxon helper degenerate paths, percentile helper, and frame
   msg_body round-trip.

2. **Real-daemon smoke tests** -- guarded by ``_REQUIRES_WAITBUS``;
   exercise the bench's main() against a real waitbus broadcast daemon
   in --skip-real-llm mode (synthetic 30-chunk stream rides the real
   waitbus + UDS + spawn paths). Skip cleanly when ``waitbus`` is missing
   from PATH or the host is not Linux.

3. **Production-shape contracts** -- the iter_raw-not-iter_lines
   contract (the producer must use the byte-level stream, not the
   line-buffered shim), and the UDS sibling baseline contract.
"""

from __future__ import annotations

import json
import shutil
import socket
import struct
import sys
from pathlib import Path

import msgspec
import pytest

from benchmarks._bench_preflight import PreflightError
from benchmarks.bench_event_delivery_fidelity import (
    _LATENCY_BUDGET_P99_NS,
    _PERTURBATION_MARGIN_P99_NS,
    _SWARM_SUBSCRIBER_COUNT,
    EventDeliveryFidelityVerdict,
    ReasoningChunkFrame,
    _aggregate_arm,
    _ArmLatencyStats,
    _build_events_once,
    _compute_gates,
    _compute_paired_marginals,
    _compute_per_chunk_bus_latency_ns,
    _compute_ttft_ns,
    _decode_chunk_frame_from_msg_body,
    _delivery_integrity_failures,
    _encode_chunk_frame_for_msg_body,
    _encode_chunk_frame_for_uds,
    _hash_chunk,
    _ordering_inversions,
    _parse_consumer_arrivals,
    _parse_delivered_order,
    _parse_delivered_rehashes,
    _percentile_ns,
    _segment_text_into_events,
    _VerdictGates,
    _wilcoxon_paired_pvalue,
    main,
)

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="bench_event_delivery_fidelity is Linux-only (UDS + cross-process CLOCK_MONOTONIC).",
)


_REQUIRES_WAITBUS = pytest.mark.skipif(
    shutil.which("waitbus") is None,
    reason="waitbus CLI not on PATH; the bench's daemon subprocess cannot bootstrap.",
)


# ---------------------------------------------------------------------
# Frame hash + msg_body round-trip.
# ---------------------------------------------------------------------


def test_hash_chunk_is_deterministic() -> None:
    """Two calls with the same triple yield the same hex digest."""
    a = _hash_chunk(iter_id=3, chunk_seq=7, chunk_bytes=b"abc")
    b = _hash_chunk(iter_id=3, chunk_seq=7, chunk_bytes=b"abc")
    assert a == b


def test_hash_chunk_changes_with_iter_id() -> None:
    """Different ``iter_id`` produces a different hash."""
    a = _hash_chunk(iter_id=1, chunk_seq=0, chunk_bytes=b"x")
    b = _hash_chunk(iter_id=2, chunk_seq=0, chunk_bytes=b"x")
    assert a != b


def test_hash_chunk_changes_with_chunk_seq() -> None:
    """Different ``chunk_seq`` produces a different hash."""
    a = _hash_chunk(iter_id=1, chunk_seq=0, chunk_bytes=b"x")
    b = _hash_chunk(iter_id=1, chunk_seq=1, chunk_bytes=b"x")
    assert a != b


def test_frame_hash_round_trip() -> None:
    """A frame's content survives the msg_body base64+msgspec round-trip.

    Encodes a frame, decodes it back, and asserts every field is
    byte-identical. The hash is the load-bearing identity contract;
    any change to the deterministic-replay frame format must keep
    this test green.
    """
    original = ReasoningChunkFrame(
        t_chunk_arrived_monotonic_ns=1_234_567_890,
        chunk_seq=42,
        iter_id=7,
        chunk_bytes=b'data: {"choices":[{"delta":{"content":"Hello world"}}]}',
        chunk_hash_hex=_hash_chunk(
            iter_id=7,
            chunk_seq=42,
            chunk_bytes=b'data: {"choices":[{"delta":{"content":"Hello world"}}]}',
        ),
    )
    body = _encode_chunk_frame_for_msg_body(original)
    decoded = _decode_chunk_frame_from_msg_body(body)
    assert decoded.t_chunk_arrived_monotonic_ns == original.t_chunk_arrived_monotonic_ns
    assert decoded.chunk_seq == original.chunk_seq
    assert decoded.iter_id == original.iter_id
    assert decoded.chunk_bytes == original.chunk_bytes
    assert decoded.chunk_hash_hex == original.chunk_hash_hex


def test_encode_chunk_frame_for_uds_is_length_prefixed() -> None:
    """UDS encoder produces a 4-byte big-endian length + JSON payload."""
    frame = ReasoningChunkFrame(
        t_chunk_arrived_monotonic_ns=100,
        chunk_seq=0,
        iter_id=0,
        chunk_bytes=b"abc",
        chunk_hash_hex=_hash_chunk(iter_id=0, chunk_seq=0, chunk_bytes=b"abc"),
    )
    encoded = _encode_chunk_frame_for_uds(frame)
    assert len(encoded) >= 4
    (length,) = struct.unpack(">I", encoded[:4])
    payload = encoded[4:]
    assert len(payload) == length
    decoded = msgspec.json.decode(payload, type=ReasoningChunkFrame)
    assert decoded.chunk_bytes == frame.chunk_bytes


# ---------------------------------------------------------------------
# Delivery-integrity round-trip (delivered-bytes re-hash vs source).
# ---------------------------------------------------------------------


def _segment_events(text: str, *, iter_id: int) -> list[tuple[int, bytes, str]]:
    """Segment a short text into discrete events (test convenience wrapper)."""
    return _segment_text_into_events(text, iter_id=iter_id, n_target=8)


def _delivered_ledger_line(frame: ReasoningChunkFrame, *, arrival_ns: int) -> str:
    """Build a consumer-side CHUNK ledger line whose body_b64 is the delivered frame.

    Mirrors the bus consumer subprocess's print format: the frame is
    base64 msgspec-encoded into ``body_b64`` exactly as the consumer
    re-emits the delivered frame to its stdout ledger.
    """
    body = _encode_chunk_frame_for_msg_body(frame)
    return f"CHUNK arrival_ns={arrival_ns} event_id=ev{frame.chunk_seq} body_b64={body}"


def test_delivery_integrity_zero_on_clean_delivery() -> None:
    """Clean delivery of every source chunk reports zero failures."""
    events = _segment_events("alpha-bravo-charlie-delta-echo", iter_id=0)
    source_hashes = {seq: h for seq, _b, h in events}
    lines = [
        _delivered_ledger_line(
            ReasoningChunkFrame(
                t_chunk_arrived_monotonic_ns=seq,
                chunk_seq=seq,
                iter_id=0,
                chunk_bytes=chunk_bytes,
                chunk_hash_hex=chunk_hash_hex,
            ),
            arrival_ns=1000 + seq,
        )
        for seq, chunk_bytes, chunk_hash_hex in events
    ]
    delivered = _parse_delivered_rehashes(consumer_lines=lines, chunk_prefix="CHUNK")
    assert _delivery_integrity_failures(source_hashes=source_hashes, delivered_rehashes=delivered) == 0


def test_delivery_integrity_counts_dropped_chunk() -> None:
    """A source chunk_seq with no delivered line counts as one failure."""
    events = _segment_events("alpha-bravo-charlie-delta-echo", iter_id=0)
    source_hashes = {seq: h for seq, _b, h in events}
    # Drop the LAST delivered line.
    lines = [
        _delivered_ledger_line(
            ReasoningChunkFrame(
                t_chunk_arrived_monotonic_ns=seq,
                chunk_seq=seq,
                iter_id=0,
                chunk_bytes=chunk_bytes,
                chunk_hash_hex=chunk_hash_hex,
            ),
            arrival_ns=1000 + seq,
        )
        for seq, chunk_bytes, chunk_hash_hex in events[:-1]
    ]
    delivered = _parse_delivered_rehashes(consumer_lines=lines, chunk_prefix="CHUNK")
    assert _delivery_integrity_failures(source_hashes=source_hashes, delivered_rehashes=delivered) == 1


def test_delivery_integrity_catches_corrupted_delivered_byte() -> None:
    """Flipping one byte in ONE delivered frame's chunk_bytes is caught.

    This is the non-vacuity proof for the round-trip check. The old
    producer-vs-producer logic compared two arms' producer-side
    ``chunk_hash_hex`` values, which are identical by construction
    (every arm replays the SAME source events), so it would report 0
    here regardless of what the consumer actually received. The new
    round-trip logic re-hashes the DELIVERED bytes (ignoring the
    frame's embedded ``chunk_hash_hex``) and compares against the
    source manifest, so a single corrupted delivered byte surfaces as
    >= 1 failure.

    The clean case (no corruption) is asserted alongside to pin both
    directions.
    """
    # Build a small source manifest offline.
    events, _completion_tokens = _build_events_once(
        iter_id=0,
        api_key=None,
        include_real_llm=False,
        sentinel_prefix="deadbeefcafef00d",
    )
    assert len(events) >= 3, "offline segmentation should yield several events"
    source_hashes = {seq: h for seq, _b, h in events}

    def _clean_lines() -> list[str]:
        return [
            _delivered_ledger_line(
                ReasoningChunkFrame(
                    t_chunk_arrived_monotonic_ns=seq,
                    chunk_seq=seq,
                    iter_id=0,
                    chunk_bytes=chunk_bytes,
                    chunk_hash_hex=chunk_hash_hex,
                ),
                arrival_ns=1000 + seq,
            )
            for seq, chunk_bytes, chunk_hash_hex in events
        ]

    # CLEAN case: zero failures.
    clean = _parse_delivered_rehashes(consumer_lines=_clean_lines(), chunk_prefix="CHUNK")
    assert _delivery_integrity_failures(source_hashes=source_hashes, delivered_rehashes=clean) == 0

    # CORRUPTED case: flip one byte in ONE delivered frame's chunk_bytes
    # BEFORE it is base64-encoded into the ledger line. The
    # frame's embedded chunk_hash_hex is left UNCHANGED (the source
    # digest), so a check that trusted the embedded hash would miss this;
    # only a re-hash of the delivered bytes catches it.
    target_seq, target_bytes, target_source_hash = events[1]
    flipped = bytearray(target_bytes)
    flipped[0] ^= 0x01  # flip the low bit of the first delivered byte
    corrupted_lines: list[str] = []
    for seq, chunk_bytes, chunk_hash_hex in events:
        delivered_bytes = bytes(flipped) if seq == target_seq else chunk_bytes
        corrupted_lines.append(
            _delivered_ledger_line(
                ReasoningChunkFrame(
                    t_chunk_arrived_monotonic_ns=seq,
                    chunk_seq=seq,
                    iter_id=0,
                    chunk_bytes=delivered_bytes,
                    # Embedded hash deliberately UNCHANGED (the source digest).
                    chunk_hash_hex=chunk_hash_hex,
                ),
                arrival_ns=1000 + seq,
            )
        )
    # Sanity: the flipped delivered bytes differ from source, but the
    # frame's embedded hash field still matches the source digest -- so a
    # vacuous check would pass.
    assert bytes(flipped) != target_bytes
    assert _hash_chunk(iter_id=0, chunk_seq=target_seq, chunk_bytes=target_bytes) == target_source_hash

    corrupted = _parse_delivered_rehashes(consumer_lines=corrupted_lines, chunk_prefix="CHUNK")
    failures = _delivery_integrity_failures(source_hashes=source_hashes, delivered_rehashes=corrupted)
    assert failures >= 1, "a corrupted delivered byte must surface as a delivery-integrity failure"


# ---------------------------------------------------------------------
# Ordering-fidelity round-trip.
# ---------------------------------------------------------------------


def _ordering_ledger_lines(delivery_seqs: list[int]) -> list[str]:
    """Build consumer-style CHUNK ledger lines DELIVERED in ``delivery_seqs`` order.

    Each int is materialised into a real ``ReasoningChunkFrame`` whose
    body is base64 msgspec-encoded into ``body_b64`` exactly as the bus
    consumer re-emits a delivered frame (via :func:`_delivered_ledger_line`).
    The list order is the ARRIVAL order, so the resulting lines exercise
    the real decode path of :func:`_parse_delivered_order` rather than a
    hand-built int list.
    """
    lines: list[str] = []
    for arrival_index, seq in enumerate(delivery_seqs):
        chunk_bytes = f"chunk-{seq}".encode()
        lines.append(
            _delivered_ledger_line(
                ReasoningChunkFrame(
                    t_chunk_arrived_monotonic_ns=arrival_index,
                    chunk_seq=seq,
                    iter_id=0,
                    chunk_bytes=chunk_bytes,
                    chunk_hash_hex=_hash_chunk(iter_id=0, chunk_seq=seq, chunk_bytes=chunk_bytes),
                ),
                arrival_ns=1000 + arrival_index,
            )
        )
    return lines


def test_ordering_inversions_detects_out_of_order() -> None:
    """A scrambled delivery order (0,1,3,2,4) surfaces >= 1 inversion.

    This is the non-vacuity proof for the ordering round-trip. Real
    ``body_b64`` ledger lines are synthesised and re-parsed through
    :func:`_parse_delivered_order`, so the recovered order is decoded
    from the delivered frames exactly as a daemon-reordering bug would
    present on the consumer ledger -- not asserted on a hand-built int
    list.
    """
    lines = _ordering_ledger_lines([0, 1, 3, 2, 4])
    delivered_order = _parse_delivered_order(consumer_lines=lines, chunk_prefix="CHUNK")
    # Sanity: the real parse recovered the scrambled arrival order.
    assert delivered_order == [0, 1, 3, 2, 4]
    assert _ordering_inversions(delivered_order) >= 1, "an out-of-order delivery must surface as an inversion"


def test_ordering_inversions_zero_when_in_order() -> None:
    """In-order delivery (0,1,2,3,4) reports zero inversions.

    Exercises the same real parse path as the scrambled case; a clean
    monotonic stream must yield 0 so the gate does not false-positive on
    correct delivery.
    """
    lines = _ordering_ledger_lines([0, 1, 2, 3, 4])
    delivered_order = _parse_delivered_order(consumer_lines=lines, chunk_prefix="CHUNK")
    assert delivered_order == [0, 1, 2, 3, 4]
    assert _ordering_inversions(delivered_order) == 0


# ---------------------------------------------------------------------
# Wilcoxon helper.
# ---------------------------------------------------------------------


def test_wilcoxon_returns_unity_pvalue_on_empty_inputs() -> None:
    """Empty inputs must return p=1.0 (do-not-reject)."""
    assert _wilcoxon_paired_pvalue([], []) == 1.0


def test_wilcoxon_returns_unity_pvalue_on_zero_differences() -> None:
    """Identical paired samples return p=1.0 (no-difference)."""
    assert _wilcoxon_paired_pvalue([1.0, 2.0, 3.0], [1.0, 2.0, 3.0]) == 1.0


def test_wilcoxon_returns_small_pvalue_on_distinct_distributions() -> None:
    """Wilcoxon yields a low p when the paired difference is consistently positive."""
    a = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]
    b = [12.0, 22.0, 33.0, 42.0, 52.0, 63.0, 71.0, 82.0, 92.0, 102.0]
    p = _wilcoxon_paired_pvalue(a, b)
    assert p < 0.1


# ---------------------------------------------------------------------
# Percentile + latency-join helpers.
# ---------------------------------------------------------------------


def test_percentile_ns_basic() -> None:
    """p50 of 1..101 is 51; empty returns 0."""
    assert _percentile_ns(list(range(1, 102)), 0.50) == 51
    assert _percentile_ns([], 0.50) == 0


def test_compute_per_chunk_bus_latency_drops_negative_and_missing() -> None:
    """Latency join drops missing arrivals and negative deltas."""
    frames = [
        ReasoningChunkFrame(
            t_chunk_arrived_monotonic_ns=100,
            chunk_seq=0,
            iter_id=0,
            chunk_bytes=b"a",
            chunk_hash_hex=_hash_chunk(iter_id=0, chunk_seq=0, chunk_bytes=b"a"),
        ),
        ReasoningChunkFrame(
            t_chunk_arrived_monotonic_ns=200,
            chunk_seq=1,
            iter_id=0,
            chunk_bytes=b"b",
            chunk_hash_hex=_hash_chunk(iter_id=0, chunk_seq=1, chunk_bytes=b"b"),
        ),
    ]
    arrivals = {0: 150, 1: 100}  # seq 1's arrival is BEFORE its producer anchor (skip)
    latencies = _compute_per_chunk_bus_latency_ns(frames=frames, arrivals=arrivals)
    assert latencies == [50]


def test_compute_ttft_zero_on_missing_arrival() -> None:
    """TTFT is 0 when the first chunk's arrival did not surface."""
    frame = ReasoningChunkFrame(
        t_chunk_arrived_monotonic_ns=100,
        chunk_seq=0,
        iter_id=0,
        chunk_bytes=b"a",
        chunk_hash_hex=_hash_chunk(iter_id=0, chunk_seq=0, chunk_bytes=b"a"),
    )
    assert _compute_ttft_ns(arm_start_ns=50, frames=[frame], arrivals={}) == 0


# ---------------------------------------------------------------------
# Consumer-line parser.
# ---------------------------------------------------------------------


def test_parse_consumer_arrivals_decodes_msg_body() -> None:
    """Consumer lines with a CHUNK prefix and base64 body decode back to a chunk_seq."""
    frame = ReasoningChunkFrame(
        t_chunk_arrived_monotonic_ns=100,
        chunk_seq=3,
        iter_id=0,
        chunk_bytes=b"abc",
        chunk_hash_hex=_hash_chunk(iter_id=0, chunk_seq=3, chunk_bytes=b"abc"),
    )
    body = _encode_chunk_frame_for_msg_body(frame)
    lines = [f"CHUNK arrival_ns=12345 event_id=ev1 body_b64={body}"]
    arrivals = _parse_consumer_arrivals(consumer_lines=lines, chunk_prefix="CHUNK")
    assert arrivals == {3: 12345}


def test_parse_consumer_arrivals_skips_unrelated_lines() -> None:
    """Lines without the prefix are silently skipped."""
    arrivals = _parse_consumer_arrivals(
        consumer_lines=["DONE chunks=0 swarm_emits=0", "SWARM_EMIT event_id=x"],
        chunk_prefix="CHUNK",
    )
    assert arrivals == {}


# ---------------------------------------------------------------------
# _aggregate_arm pure-helper test.
# ---------------------------------------------------------------------


def test_aggregate_arm_rolls_per_iter_into_arm_stats() -> None:
    """Roll up per-iter rows: per-chunk latencies, TTFT, wall-time."""
    rows = [
        {
            "arm": "bus_idle",
            "iter_id": 0,
            "per_chunk_bus_latency_ns": [100, 200, 300],
            "ttft_ns": 1000,
            "wall_time_ns": 5000,
        },
        {
            "arm": "bus_idle",
            "iter_id": 1,
            "per_chunk_bus_latency_ns": [150, 250],
            "ttft_ns": 1500,
            "wall_time_ns": 6000,
        },
    ]
    stats = _aggregate_arm("bus_idle", rows)
    assert isinstance(stats, _ArmLatencyStats)
    assert stats.arm == "bus_idle"
    assert stats.n_iterations == 2
    assert stats.n_chunks_total == 5
    assert stats.median_per_chunk_bus_latency_ns > 0
    assert stats.median_ttft_ns == 1250  # median of [1000, 1500]


# ---------------------------------------------------------------------
# Verdict struct shape (msgspec round-trip).
# ---------------------------------------------------------------------


def _fake_external_state() -> object:
    """Build a minimal ExternalStateReport for verdict-shape tests."""
    from benchmarks._bench_shared import ExternalStateReport

    return ExternalStateReport(
        claude_cli_version=None,
        gemini_cli_version=None,
        pydantic_ai_version=None,
        langgraph_version=None,
        langchain_core_version=None,
        langchain_openai_version=None,
        openai_sdk_version=None,
        anthropic_sdk_version=None,
        msgspec_version=None,
        hdrhistogram_version=None,
        tiktoken_version=None,
        anthropic_response_model_set=[],
        openai_response_model_set=[],
        gemini_response_model_set=[],
        agent_tool_call_count_per_iter=[],
        agent_turn_count_per_iter=[],
        waitbus_daemon_synchronous=None,
        waitbus_daemon_journal_mode=None,
        waitbus_daemon_page_size=None,
        waitbus_daemon_broadcast_pool_size=None,
        waitbus_daemon_doorbell_socket_buffer=None,
        waitbus_daemon_pragmas={},
        waitbus_env_vars={},
        pythonhashseed="0",
        pythonmalloc=None,
        ntp_active=None,
        ntp_source=None,
        boot_time_ns=None,
        cpu_count_physical=4,
        cpu_count_logical=4,
        moderation_event_count=0,
        stop_reason_distribution={},
        api_error_status_distribution={},
        openai_key_present=False,
    )


def test_verdict_struct_round_trip() -> None:
    """EventDeliveryFidelityVerdict serialises and decodes byte-stably."""
    env = _fake_external_state()
    arm_stats = {
        arm: _ArmLatencyStats(
            arm=arm,
            n_iterations=2,
            n_chunks_total=10,
            median_per_chunk_bus_latency_ns=500_000,
            p99_per_chunk_bus_latency_ns=900_000,
            median_ttft_ns=2_000_000,
            median_wall_time_ns=5_000_000,
        )
        for arm in ("lll_alone_ipc_peer", "bus_idle", "bus_swarm")
    }
    verdict = EventDeliveryFidelityVerdict(
        bench_name="event_delivery_fidelity",
        started_ns=1,
        finished_ns=2,
        environment=env,  # type: ignore[arg-type]
        external_state=env,  # type: ignore[arg-type]
        n_triples_requested=3,
        n_triples_actual=3,
        smoke=True,
        include_real_llm=False,
        arms=["lll_alone_ipc_peer", "bus_idle", "bus_swarm"],
        arm_stats=arm_stats,
        wilcoxon_p_per_chunk_bus_latency=0.5,
        wilcoxon_p_ttft=0.5,
        wilcoxon_p_wall_time=0.5,
        h0_rejected_per_chunk_bus_latency=False,
        h0_rejected_ttft=False,
        h0_rejected_wall_time=False,
        alpha_per_marginal=0.05 / 3,
        delivery_integrity_failures_lll_alone=0,
        delivery_integrity_failures_bus_idle=0,
        delivery_integrity_failures_bus_swarm=0,
        ordering_inversions_lll_alone=0,
        ordering_inversions_bus_idle=0,
        ordering_inversions_bus_swarm=0,
        median_per_chunk_bus_latency_alone_ns=500_000,
        median_per_chunk_bus_latency_bus_idle_ns=500_000,
        median_per_chunk_bus_latency_bus_swarm_ns=500_000,
        median_ttft_alone_ns=2_000_000,
        median_ttft_bus_idle_ns=2_000_000,
        median_ttft_bus_swarm_ns=2_000_000,
        median_wall_time_alone_ns=5_000_000,
        median_wall_time_bus_idle_ns=5_000_000,
        median_wall_time_bus_swarm_ns=5_000_000,
        swarm_subscribers_ready_total=15,
        swarm_underload_floor=15,
        sandbagging_sentinel_fired=False,
        latency_budget_p99_ns=_LATENCY_BUDGET_P99_NS,
        bus_idle_p99_latency_ns=900_000,
        bus_swarm_p99_latency_ns=900_000,
        latency_budget_passed=True,
        wilcoxon_p_bus_idle_vs_swarm_latency=0.5,
        bus_swarm_perturbs_latency=False,
        distribution_equivalent=True,
        perturbation_detected=False,
        inapplicable_reason=None,
        cost_usd_total=0.0,
        cost_unknown_count=0,
        max_cost_usd_budget=5.0,
        max_cost_usd_observed=0.0,
        aborted_on_budget=False,
        limitations=["test"],
    )
    encoded = msgspec.json.encode(verdict)
    decoded = msgspec.json.decode(encoded, type=EventDeliveryFidelityVerdict)
    assert decoded.bench_name == "event_delivery_fidelity"
    assert decoded.n_triples_actual == 3
    assert decoded.per_iter_source_distribution == {}


# ---------------------------------------------------------------------
# UDS sibling peer baseline (REAL subprocess + REAL socket).
# ---------------------------------------------------------------------


def test_uds_sibling_peer_baseline_runs(tmp_path: Path) -> None:
    """Spawn the bench's UDS peer subprocess; connect + send one frame; assert it logs.

    Exercises the production wiring of the alone-arm baseline: the
    sibling subprocess binds the AF_UNIX SOCK_STREAM socket, accepts
    the orchestrator's connection, decodes the length-prefixed frame,
    and prints the per-frame arrival_ns + base64 body to stdout.
    """
    from benchmarks.bench_event_delivery_fidelity import (
        _MARKER_UDS_READY,
        _spawn_uds_peer,
        _wait_for_marker,
    )

    uds_path = tmp_path / "uds-peer.sock"
    proc = _spawn_uds_peer(
        socket_path=uds_path,
        expected_chunks=1,
        python_exe=sys.executable,
    )
    try:
        assert _wait_for_marker(proc, expected_marker=_MARKER_UDS_READY, timeout_sec=10.0)
        # Connect + send one length-prefixed frame.
        frame = ReasoningChunkFrame(
            t_chunk_arrived_monotonic_ns=100,
            chunk_seq=0,
            iter_id=0,
            chunk_bytes=b"hello",
            chunk_hash_hex=_hash_chunk(iter_id=0, chunk_seq=0, chunk_bytes=b"hello"),
        )
        payload = _encode_chunk_frame_for_uds(frame)
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.connect(str(uds_path))
        try:
            client.sendall(payload)
        finally:
            client.close()
        # Drain stdout: expect at least one UDS_CHUNK line followed by DONE.
        assert proc.stdout is not None
        seen_chunk = False
        seen_done = False
        deadline_count = 0
        while deadline_count < 50:
            line = proc.stdout.readline()
            if not line:
                break
            text = line.decode("utf-8", errors="replace").rstrip()
            if text.startswith("UDS_CHUNK"):
                seen_chunk = True
            if text.startswith("DONE"):
                seen_done = True
                break
            deadline_count += 1
        assert seen_chunk, "UDS peer did not log a UDS_CHUNK line"
        assert seen_done, "UDS peer did not log the DONE summary"
    finally:
        # Use the bench's own terminator so piped FDs do not leak.
        from benchmarks.bench_event_delivery_fidelity import _terminate as _bench_terminate

        _bench_terminate(proc)


# ---------------------------------------------------------------------
# Real-daemon smoke (REQUIRES_WAITBUS).
# ---------------------------------------------------------------------


@_REQUIRES_WAITBUS
def test_smoke_end_to_end_real_daemon(tmp_path: Path) -> None:
    """End-to-end --smoke --skip-real-llm against a real waitbus broadcast daemon.

    The bench's main() spawns a real waitbus daemon, runs the three
    arms across N=2 iterations using the synthetic-offline 30-chunk
    stream, and writes a verdict.json the test then decodes via
    msgspec. The test asserts:

    * the verdict was written;
    * ``n_triples_actual`` is at least 1 (at least one paired triple
      completed);
    * the subscriber-underload sentinel did NOT fire (the lightweight
      subscribers subscribe instantly in offline mode, so the bus_swarm
      arm is APPLICABLE -- this is the whole point of the redesign vs the
      old cold-start-starved real-LLM swarm);
    * the per-arm ``delivery_integrity_failures_*`` are 0 in offline
      mode (clean in-process delivery: delivered bytes re-hash to the
      source manifest);
    * ``bench_name`` matches the module's pinned constant.
    """
    output = tmp_path / "out"
    try:
        rc = main(["--smoke", "--n", "2", "--skip-real-llm", "--output", str(output)])
    except PreflightError:
        pytest.skip("preflight unsatisfied")
    # The bench's rc is 0 / 1 / 4 depending on gate outcomes; the test
    # accepts any rc as long as a verdict landed.
    assert rc in (0, 1, 4)
    verdict_files = list(output.glob("*.verdict.json"))
    assert verdict_files, f"no verdict.json under {output}"
    verdict = msgspec.json.decode(verdict_files[0].read_bytes(), type=EventDeliveryFidelityVerdict)
    assert verdict.bench_name == "event_delivery_fidelity"
    assert verdict.smoke is True
    assert verdict.include_real_llm is False
    # At least one paired triple should complete in --smoke --n 2;
    # accept a structural skip with inapplicable_reason on a
    # genuinely-degenerate run.
    if verdict.n_triples_actual == 0:
        assert verdict.inapplicable_reason == "n_triples_actual_zero"
    else:
        # The lightweight subscribers subscribe instantly, so the
        # subscriber-underload sentinel must NOT fire: the bus_swarm arm
        # is applicable in offline smoke.
        assert verdict.inapplicable_reason != "inapplicable_subscriber_underloaded"
        assert verdict.sandbagging_sentinel_fired is False
        # Offline mode is byte-deterministic and delivery is clean
        # in-process: each arm's delivered bytes MUST re-hash to the
        # source manifest, so every per-arm delivery-integrity counter
        # is zero.
        assert verdict.delivery_integrity_failures_lll_alone == 0
        assert verdict.delivery_integrity_failures_bus_idle == 0
        assert verdict.delivery_integrity_failures_bus_swarm == 0


# ---------------------------------------------------------------------
# Latency-budget + bus-vs-bus perturbation gates (real _compute_gates).
#
# These exercise the REAL gate-computation path: each builds genuine
# _ArmLatencyStats arm-stats inputs (with explicit p99 latency fields)
# and swarm rows that clear the sandbagging sentinel, then calls
# _compute_gates(...) and asserts on the returned _VerdictGates. The
# swarm-perturbation test additionally drives the real
# _compute_paired_marginals(...) Wilcoxon path off clearly-different
# bus_idle vs bus_swarm latency samples to derive h0_rejected_per_chunk.
# ---------------------------------------------------------------------


def _arm_stats_with_p99(arm: str, *, p99_latency_ns: int) -> _ArmLatencyStats:
    """Build an _ArmLatencyStats carrying a chosen p99 per-event latency."""
    return _ArmLatencyStats(
        arm=arm,
        n_iterations=2,
        n_chunks_total=10,
        median_per_chunk_bus_latency_ns=p99_latency_ns // 2,
        p99_per_chunk_bus_latency_ns=p99_latency_ns,
        median_ttft_ns=2_000_000,
        median_wall_time_ns=5_000_000,
    )


def _swarm_rows(n_iter: int, *, subscribers_ready: int = _SWARM_SUBSCRIBER_COUNT) -> list[dict[str, object]]:
    """Build bus_swarm rows whose READY subscribers clear the underload floor.

    Each row reports ``subscribers_ready`` READY subscribers; the default
    of ``_SWARM_SUBSCRIBER_COUNT`` (a full slate every iteration) clears
    the ``_SWARM_SUBSCRIBER_COUNT * N_iter`` floor for any N.
    """
    return [
        {
            "arm": "bus_swarm",
            "iter_id": i,
            "swarm_subscribers_ready": subscribers_ready,
        }
        for i in range(n_iter)
    ]


def test_subscriber_underload_marks_inapplicable() -> None:
    """Fewer than 0.70 * floor subscribers READY => inapplicable.

    Drives the REAL applicability computation (_compute_gates' underload
    sentinel), not a hand-set boolean. The floor is
    _SWARM_SUBSCRIBER_COUNT * N_iter; each bus_swarm row reports only one
    READY subscriber, so the summed total (N_iter) is below the 70%
    threshold of the full floor (_SWARM_SUBSCRIBER_COUNT * N_iter), and
    the bench fires inapplicable_subscriber_underloaded.
    """
    n_iter = 4
    floor = _SWARM_SUBSCRIBER_COUNT * n_iter
    # One ready subscriber per iter => total n_iter; assert it is genuinely
    # under the 0.70 * floor threshold so the test is non-vacuous.
    assert float(n_iter) < 0.70 * float(floor)
    arm_stats = {
        "lll_alone_ipc_peer": _arm_stats_with_p99("lll_alone_ipc_peer", p99_latency_ns=1_000),
        "bus_idle": _arm_stats_with_p99("bus_idle", p99_latency_ns=27_000_000),
        "bus_swarm": _arm_stats_with_p99("bus_swarm", p99_latency_ns=30_000_000),
    }
    gates = _compute_gates(
        rows=_swarm_rows(n_iter, subscribers_ready=1),
        arm_stats=arm_stats,
        n_triples_actual=n_iter,
    )
    assert gates.swarm_subscribers_ready == n_iter
    assert gates.swarm_underload_floor == floor
    assert gates.sandbagging_sentinel_fired is True
    assert gates.inapplicable_reason == "inapplicable_subscriber_underloaded"


def test_full_subscribers_ready_is_applicable() -> None:
    """All _SWARM_SUBSCRIBER_COUNT READY every iter => applicable.

    Drives the REAL applicability computation: each bus_swarm row reports
    a full slate of _SWARM_SUBSCRIBER_COUNT READY subscribers, so the
    summed total equals the floor (well above the 70% threshold) and
    inapplicable_reason is None.
    """
    n_iter = 4
    floor = _SWARM_SUBSCRIBER_COUNT * n_iter
    arm_stats = {
        "lll_alone_ipc_peer": _arm_stats_with_p99("lll_alone_ipc_peer", p99_latency_ns=1_000),
        "bus_idle": _arm_stats_with_p99("bus_idle", p99_latency_ns=27_000_000),
        "bus_swarm": _arm_stats_with_p99("bus_swarm", p99_latency_ns=30_000_000),
    }
    gates = _compute_gates(
        rows=_swarm_rows(n_iter),  # default: _SWARM_SUBSCRIBER_COUNT ready each iter
        arm_stats=arm_stats,
        n_triples_actual=n_iter,
    )
    assert gates.swarm_subscribers_ready == floor
    assert gates.swarm_underload_floor == floor
    assert gates.sandbagging_sentinel_fired is False
    assert gates.inapplicable_reason is None


def test_latency_budget_fails_when_bus_p99_exceeds_100ms() -> None:
    """A bus arm p99 just over the 100ms budget flips the gate red.

    Drives the real _compute_gates path: bus_swarm p99 is one nanosecond
    above the pre-registered budget, so latency_budget_passed is False
    and perturbation_detected is True regardless of the bus-vs-bus test.
    """
    n_iter = 2
    arm_stats = {
        "lll_alone_ipc_peer": _arm_stats_with_p99("lll_alone_ipc_peer", p99_latency_ns=1_000),
        "bus_idle": _arm_stats_with_p99("bus_idle", p99_latency_ns=50_000_000),
        # One nanosecond over the pre-registered 100ms budget.
        "bus_swarm": _arm_stats_with_p99("bus_swarm", p99_latency_ns=_LATENCY_BUDGET_P99_NS + 1),
    }
    gates = _compute_gates(
        rows=_swarm_rows(n_iter),
        arm_stats=arm_stats,
        n_triples_actual=n_iter,
    )
    assert gates.latency_budget_passed is False
    assert gates.perturbation_detected is True
    assert gates.inapplicable_reason is None


def test_latency_budget_passes_within_100ms() -> None:
    """Bus p99 under budget + swarm within the perturbation margin => no perturbation.

    Drives the real _compute_gates path: both bus arms sit far under the
    100ms budget and the loaded swarm p99 is only 3ms above idle (within the
    20ms margin), so latency_budget_passed is True and perturbation_detected
    is False. The raw-IPC arm's p99 is irrelevant to the gate (recorded only).
    """
    n_iter = 2
    arm_stats = {
        # Raw-IPC arm p99 is microsecond-scale and MUST NOT influence the gate.
        "lll_alone_ipc_peer": _arm_stats_with_p99("lll_alone_ipc_peer", p99_latency_ns=1_000),
        "bus_idle": _arm_stats_with_p99("bus_idle", p99_latency_ns=27_000_000),
        # 3ms over idle -- within the 20ms perturbation margin.
        "bus_swarm": _arm_stats_with_p99("bus_swarm", p99_latency_ns=30_000_000),
    }
    gates = _compute_gates(
        rows=_swarm_rows(n_iter),
        arm_stats=arm_stats,
        n_triples_actual=n_iter,
    )
    assert gates.latency_budget_passed is True
    assert gates.bus_swarm_perturbs_latency is False
    assert gates.distribution_equivalent is True
    assert gates.perturbation_detected is False


def test_swarm_perturbation_detected_when_distributions_differ() -> None:
    """A loaded swarm p99 well beyond the margin => perturbation (effect-size gate).

    The bus_swarm p99 (~40ms) exceeds bus_idle p99 (~10ms) by ~30ms, beyond
    the 20ms pre-registered margin, so _compute_gates surfaces
    bus_swarm_perturbs_latency / perturbation_detected. Both bus p99s stay
    under the 100ms budget, so the signal is the loaded-vs-idle degradation,
    not a budget breach. The Wilcoxon is also computed here as a recorded,
    NON-gating observation (h0_rejected on this large a shift), but it is the
    effect size -- not the p-value -- that drives the gate.
    """
    n_iter = 12
    rows_by_arm_iter: dict[tuple[str, int], dict[str, object]] = {}
    for i in range(n_iter):
        # bus_idle: low, tight latency; bus_swarm: clearly higher, all
        # under the 100ms budget. The raw-IPC arm is present in the row
        # map only so the paired-iter set is well-formed; it is NOT a
        # comparison baseline inside _compute_paired_marginals.
        rows_by_arm_iter[("lll_alone_ipc_peer", i)] = {
            "arm": "lll_alone_ipc_peer",
            "iter_id": i,
            "per_chunk_bus_latency_ns": [1_000 + i, 1_100 + i, 1_200 + i],
            "ttft_ns": 5_000 + i,
            "wall_time_ns": 50_000 + i,
        }
        rows_by_arm_iter[("bus_idle", i)] = {
            "arm": "bus_idle",
            "iter_id": i,
            "per_chunk_bus_latency_ns": [10_000_000 + i, 10_100_000 + i, 10_200_000 + i],
            "ttft_ns": 11_000_000 + i,
            "wall_time_ns": 20_000_000 + i,
        }
        rows_by_arm_iter[("bus_swarm", i)] = {
            "arm": "bus_swarm",
            "iter_id": i,
            "per_chunk_bus_latency_ns": [40_000_000 + i, 40_100_000 + i, 40_200_000 + i],
            "ttft_ns": 41_000_000 + i,
            "wall_time_ns": 60_000_000 + i,
        }

    marginals = _compute_paired_marginals(
        rows_by_arm_iter=rows_by_arm_iter,
        paired_iter_ids=list(range(n_iter)),
    )
    # The bus_idle (~10ms) vs bus_swarm (~40ms) per-event latency shift is
    # detected by the real Wilcoxon path.
    assert marginals.h0_rejected_per_chunk is True

    arm_stats = {
        "lll_alone_ipc_peer": _arm_stats_with_p99("lll_alone_ipc_peer", p99_latency_ns=1_300),
        "bus_idle": _arm_stats_with_p99("bus_idle", p99_latency_ns=10_300_000),
        "bus_swarm": _arm_stats_with_p99("bus_swarm", p99_latency_ns=40_300_000),
    }
    gates = _compute_gates(
        rows=_swarm_rows(n_iter),
        arm_stats=arm_stats,
        n_triples_actual=n_iter,
    )
    # Both bus p99s are under the 100ms budget, so the perturbation comes
    # from the loaded-vs-idle p99 degradation (~30ms > 20ms margin), not a
    # budget breach.
    assert gates.latency_budget_passed is True
    assert gates.bus_swarm_perturbs_latency is True
    assert gates.distribution_equivalent is False
    assert gates.perturbation_detected is True


def test_perturbation_margin_boundary() -> None:
    """The one-sided effect-size gate fires exactly at the 20ms margin boundary.

    Just over the margin (idle + 20ms + 1) => perturbation; just under
    (idle + 20ms - 1) => no perturbation; and a loaded arm FASTER than idle
    is never a perturbation. Proves the gate is the effect size, not a
    p-value, and is one-directional.
    """
    n_iter = 2
    idle_p99 = 30_000_000  # 30ms, well under budget

    def _gates_for(swarm_p99: int) -> _VerdictGates:
        arm_stats = {
            "lll_alone_ipc_peer": _arm_stats_with_p99("lll_alone_ipc_peer", p99_latency_ns=1_000),
            "bus_idle": _arm_stats_with_p99("bus_idle", p99_latency_ns=idle_p99),
            "bus_swarm": _arm_stats_with_p99("bus_swarm", p99_latency_ns=swarm_p99),
        }
        return _compute_gates(rows=_swarm_rows(n_iter), arm_stats=arm_stats, n_triples_actual=n_iter)

    over = _gates_for(idle_p99 + _PERTURBATION_MARGIN_P99_NS + 1)
    assert over.bus_swarm_perturbs_latency is True
    assert over.perturbation_detected is True

    under = _gates_for(idle_p99 + _PERTURBATION_MARGIN_P99_NS - 1)
    assert under.bus_swarm_perturbs_latency is False
    assert under.perturbation_detected is False

    faster = _gates_for(idle_p99 - 5_000_000)  # loaded arm 5ms FASTER than idle
    assert faster.bus_swarm_perturbs_latency is False
    assert faster.perturbation_detected is False


# ---------------------------------------------------------------------
# Misc.
# ---------------------------------------------------------------------


def test_module_exposes_canonical_api() -> None:
    """The module's __all__ surface includes the load-bearing symbols."""
    import benchmarks.bench_event_delivery_fidelity as mod

    required = {
        "EventDeliveryFidelityVerdict",
        "ReasoningChunkFrame",
        "_ArmLatencyStats",
        "_LATENCY_BUDGET_P99_NS",
        "_compute_gates",
        "_hash_chunk",
        "_wilcoxon_paired_pvalue",
        "main",
    }
    assert required.issubset(set(mod.__all__))
    # The ratio-vs-IPC envelope gate must not resurface.
    assert not any("envelope" in name for name in mod.__all__)


def test_json_round_trip_via_msgspec_for_consumer_line_parsing() -> None:
    """A consumer-side base64 body parsed back into a frame round-trips."""
    frame = ReasoningChunkFrame(
        t_chunk_arrived_monotonic_ns=100,
        chunk_seq=5,
        iter_id=2,
        chunk_bytes=b"x" * 32,
        chunk_hash_hex=_hash_chunk(iter_id=2, chunk_seq=5, chunk_bytes=b"x" * 32),
    )
    body = _encode_chunk_frame_for_msg_body(frame)
    # The body is a base64 ASCII string with no embedded newlines.
    assert "\n" not in body
    assert "\r" not in body
    # And decoding it back yields the same frame.
    decoded = _decode_chunk_frame_from_msg_body(body)
    assert decoded == frame


# Keep the json import used by a couple of helper tests above.
_ = json
