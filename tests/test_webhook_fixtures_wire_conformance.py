"""Wire conformance tests for the webhook fixture corpus.

Each fixture is loaded, passed through the same field-extraction seam
used by the production listener (``listener._event_from_webhook_payload``),
and asserted to produce no parse exceptions and a valid EventInsert with
the expected invariant fields.

Production seam: ``waitbus.listener._event_from_webhook_payload``

"""

from __future__ import annotations

import json
import pathlib
from typing import Any

import pytest

FIXTURES_DIR = pathlib.Path(__file__).parent / "data" / "webhook_fixtures"

# Map each fixture file to the GitHub event type the listener would receive.
# Fixtures for event types the listener ignores (non-workflow events) are
# expected to produce an EventInsert with empty owner/repo/run_id — still
# exercising the JSON parse + field-extraction path without assertion errors.
_FIXTURE_EVENT_TYPES: dict[str, str] = {
    "workflow_run_action_requested_conclusion_null.json": "workflow_run",
    "workflow_run_missing_head_commit.json": "workflow_run",
    "workflow_job_completed_failure.json": "workflow_job",
    "pull_request_body_unicode_control.json": "pull_request",
    "push_with_large_deletion.json": "push",
    "commit_message_emoji.json": "push",
    "pull_request_review_with_pii_email.json": "pull_request_review",
    "issues_opened_with_secret_pattern.json": "issues",
    "release_published.json": "release",
    "delete_branch.json": "delete",
}

# Event types the production listener actively fields into its DB columns.
_LISTENER_ACTIVE_TYPES = frozenset({"workflow_run", "workflow_job"})


def _all_fixtures() -> list[tuple[str, str]]:
    """Return (fixture_name, event_type) pairs for parametrize."""
    pairs = []
    for name, etype in _FIXTURE_EVENT_TYPES.items():
        pairs.append((name, etype))
    return pairs


try:
    from waitbus import listener as _listener

    _LISTENER_AVAILABLE = True
except ImportError:
    _LISTENER_AVAILABLE = False


def _build_event(event_type: str, payload: dict[str, Any]) -> Any:
    """Call the production field-extraction function with constant context fields."""
    return _listener._event_from_webhook_payload(
        event_type,
        payload,
        delivery_id="fixture-test",
        source="github",
        received_at_ns=0,
        payload_json=json.dumps(payload),
        ingest_method="webhook",
    )


@pytest.mark.skipif(not _LISTENER_AVAILABLE, reason="waitbus.listener not importable in this env")
@pytest.mark.parametrize("fixture_name,event_type", _all_fixtures())
def test_fixture_parses_as_json(fixture_name: str, event_type: str) -> None:
    """Every fixture must be valid JSON (basic sanity gate)."""
    path = FIXTURES_DIR / fixture_name
    data = json.loads(path.read_bytes())
    assert isinstance(data, dict), f"{fixture_name}: top-level must be a JSON object"


@pytest.mark.skipif(not _LISTENER_AVAILABLE, reason="waitbus.listener not importable in this env")
@pytest.mark.parametrize("fixture_name,event_type", _all_fixtures())
def test_fixture_wire_no_exception(fixture_name: str, event_type: str) -> None:
    """Field-extraction must not raise for any fixture payload."""
    path = FIXTURES_DIR / fixture_name
    payload: dict[str, Any] = json.loads(path.read_bytes())
    # Must not raise — defensive coercion inside _event_from_webhook_payload
    # is expected to handle every shape, including non-workflow event types.
    event = _build_event(event_type, payload)
    assert event is not None


@pytest.mark.skipif(not _LISTENER_AVAILABLE, reason="waitbus.listener not importable in this env")
@pytest.mark.parametrize("fixture_name,event_type", [(n, t) for n, t in _all_fixtures() if t in _LISTENER_ACTIVE_TYPES])
def test_active_fixture_event_type_set(fixture_name: str, event_type: str) -> None:
    """Active fixture types (workflow_run, workflow_job) must set event_type."""
    path = FIXTURES_DIR / fixture_name
    payload: dict[str, Any] = json.loads(path.read_bytes())
    event = _build_event(event_type, payload)
    assert event.event_type == event_type


@pytest.mark.skipif(not _LISTENER_AVAILABLE, reason="waitbus.listener not importable in this env")
def test_workflow_run_conclusion_null_fixture() -> None:
    """Fixture: workflow_run with action=requested and conclusion=null.

    The listener must not coerce null conclusion to a non-null sentinel;
    it must survive intact as None through field extraction.
    """
    path = FIXTURES_DIR / "workflow_run_action_requested_conclusion_null.json"
    payload: dict[str, Any] = json.loads(path.read_bytes())
    event = _build_event("workflow_run", payload)
    assert event.conclusion is None
    assert event.status == "queued"
    assert event.run_id == 8000001


@pytest.mark.skipif(not _LISTENER_AVAILABLE, reason="waitbus.listener not importable in this env")
def test_workflow_run_null_head_commit_fixture() -> None:
    """Fixture: workflow_run with head_commit: null must not raise."""
    path = FIXTURES_DIR / "workflow_run_missing_head_commit.json"
    payload: dict[str, Any] = json.loads(path.read_bytes())
    event = _build_event("workflow_run", payload)
    assert event.run_id == 8000002
    assert event.conclusion == "success"


@pytest.mark.skipif(not _LISTENER_AVAILABLE, reason="waitbus.listener not importable in this env")
def test_workflow_job_completed_failure_fixture() -> None:
    """Fixture: workflow_job/completed/failure must extract job_id and parent."""
    path = FIXTURES_DIR / "workflow_job_completed_failure.json"
    payload: dict[str, Any] = json.loads(path.read_bytes())
    event = _build_event("workflow_job", payload)
    assert event.event_type == "workflow_job"
    assert event.conclusion == "failure"
    assert event.job_id == 6000001
    assert event.parent_run_id == 8000003


@pytest.mark.skipif(not _LISTENER_AVAILABLE, reason="waitbus.listener not importable in this env")
def test_pull_request_unicode_control_fixture() -> None:
    """Fixture: pull_request with unicode control chars in body must not raise."""
    path = FIXTURES_DIR / "pull_request_body_unicode_control.json"
    payload: dict[str, Any] = json.loads(path.read_bytes())
    # Verify the body actually contains the expected control chars
    body: str = payload["pull_request"]["body"]
    assert chr(0) in body, "fixture missing NUL"
    assert chr(8) in body, "fixture missing BS"
    assert chr(0x202E) in body, "fixture missing RTL override"
    assert chr(0x2028) in body, "fixture missing line separator"
    assert chr(0x2029) in body, "fixture missing paragraph separator"
    # Field extraction must not raise or corrupt
    event = _build_event("pull_request", payload)
    assert event is not None


@pytest.mark.skipif(not _LISTENER_AVAILABLE, reason="waitbus.listener not importable in this env")
def test_issues_with_secret_pattern_fixture() -> None:
    """Fixture: issue body containing secret-like strings must not raise."""
    path = FIXTURES_DIR / "issues_opened_with_secret_pattern.json"
    payload: dict[str, Any] = json.loads(path.read_bytes())
    body: str = payload["issue"]["body"]
    # Verify the fixture body actually contains the secret-like pattern
    assert "ghsec_" in body, "fixture missing secret pattern"
    event = _build_event("issues", payload)
    assert event is not None


@pytest.mark.skipif(not _LISTENER_AVAILABLE, reason="waitbus.listener not importable in this env")
def test_commit_message_emoji_fixture() -> None:
    """Fixture: commit message with Regional Indicator flag emoji must survive."""
    path = FIXTURES_DIR / "commit_message_emoji.json"
    payload: dict[str, Any] = json.loads(path.read_bytes())
    msg: str = payload["head_commit"]["message"]
    # Regional Indicator Symbol Letter U+1F1FA, U+1F1F8 = US flag
    assert "\U0001f1fa\U0001f1f8" in msg, "fixture missing US flag emoji"
    event = _build_event("push", payload)
    assert event is not None


@pytest.mark.skipif(not _LISTENER_AVAILABLE, reason="waitbus.listener not importable in this env")
def test_push_large_deletion_fixture() -> None:
    """Fixture: push with 22 removed files must not raise."""
    path = FIXTURES_DIR / "push_with_large_deletion.json"
    payload: dict[str, Any] = json.loads(path.read_bytes())
    assert len(payload["commits"][0]["removed"]) == 22
    event = _build_event("push", payload)
    assert event is not None


@pytest.mark.skipif(not _LISTENER_AVAILABLE, reason="waitbus.listener not importable in this env")
def test_release_published_fixture() -> None:
    """Fixture: release/published baseline must not raise."""
    path = FIXTURES_DIR / "release_published.json"
    payload: dict[str, Any] = json.loads(path.read_bytes())
    event = _build_event("release", payload)
    assert event is not None


@pytest.mark.skipif(not _LISTENER_AVAILABLE, reason="waitbus.listener not importable in this env")
def test_delete_branch_fixture() -> None:
    """Fixture: delete event (branch deletion) must not raise."""
    path = FIXTURES_DIR / "delete_branch.json"
    payload: dict[str, Any] = json.loads(path.read_bytes())
    event = _build_event("delete", payload)
    assert event is not None
