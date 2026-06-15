"""Gating tests for ``benchmarks._bench_anchor.emit_anchor_event``.

The anchor primitive stamps caller-supplied provenance (``repo`` /
``ingest_method`` / ``delivery_id_prefix``) onto the sentinel row so a bench's
anchor is not mis-attributed to the stress controller. These tests fake the
daemon-side ``emit`` so they assert the constructed ``EventInsert`` without a
live daemon.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from benchmarks._bench_anchor import SEED_EVENT_TYPE, SEED_SOURCE, emit_anchor_event


def _capture_emit(captured: dict[str, Any]) -> Any:
    def _fake_emit(event_insert: Any, *, db_path: Path, doorbell_path: Path) -> Any:
        captured["insert"] = event_insert
        captured["db_path"] = db_path
        captured["doorbell_path"] = doorbell_path
        return SimpleNamespace(event=SimpleNamespace(event_id="EVT-FAKE-1"))

    return _fake_emit


def test_emit_anchor_event_stamps_caller_provenance(monkeypatch: pytest.MonkeyPatch) -> None:
    """The anchor row carries the caller's repo / ingest_method, not a default."""
    captured: dict[str, Any] = {}
    monkeypatch.setattr("waitbus._emit.emit", _capture_emit(captured))
    event_id = emit_anchor_event(
        seed_scope_id="scope-xyz",
        db_path=Path("/tmp/db"),
        doorbell_path=Path("/tmp/doorbell.sock"),
        repo="bench",
        ingest_method="bench_multistream_proof_anchor",
        delivery_id_prefix="bench-multistream-anchor",
    )
    assert event_id == "EVT-FAKE-1"
    insert = captured["insert"]
    assert insert.repo == "bench"
    assert insert.ingest_method == "bench_multistream_proof_anchor"
    assert insert.owner == "anchor:scope-xyz"
    assert insert.delivery_id.startswith("bench-multistream-anchor:")
    # source / event_type default to the registered seed taxonomy.
    assert insert.source == SEED_SOURCE
    assert insert.event_type == SEED_EVENT_TYPE


def test_controller_wrapper_stamps_stress_provenance(monkeypatch: pytest.MonkeyPatch) -> None:
    """The stress-controller adapter stamps stress identity onto its anchor."""
    captured: dict[str, Any] = {}
    monkeypatch.setattr("waitbus._emit.emit", _capture_emit(captured))
    from scripts.stress._controller import _emit_anchor_event

    event_id = _emit_anchor_event(
        seed_scope_id="s",
        db_path=Path("/tmp/db"),
        doorbell_path=Path("/tmp/doorbell.sock"),
    )
    assert event_id == "EVT-FAKE-1"
    insert = captured["insert"]
    assert insert.repo == "stress"
    assert insert.ingest_method == "waitbus_stress_real_controller"
    assert insert.delivery_id.startswith("stress-real-anchor:")
