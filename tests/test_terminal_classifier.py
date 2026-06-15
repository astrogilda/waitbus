"""Unit tests for the shared per-source terminal-state classifier.

`_terminal` is the single neutral home for the GitHub-conclusion
frozensets (previously in `pr_monitor`) and for the per-source
``entity_key`` / ``is_terminal`` seam consumed by the coalesced replay
mode (and, later, by an MCP Tasks adapter). The tests pin:

* the move kept the frozensets byte-identical (same values, no semantic
  drift), so `pr_monitor.AGG_SQL` is unperturbed;
* `wait._exit_code_if_terminal` agrees with the moved frozensets;
* the per-source `(source, event_type, fields) -> entity_key` table;
* the per-source `is_terminal` rules incl. the explicit
  "watchdog liveness is never terminal" pass-through.
"""

from __future__ import annotations

from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from waitbus import pr_monitor, wait
from waitbus._terminal import (
    FAILURE_CONCLUSIONS,
    NON_TERMINAL_CONCLUSIONS,
    SUCCESS_CONCLUSION,
    entity_key,
    is_terminal,
)


def _frame(source: str, event_type: str, **fields: Any) -> dict[str, Any]:
    """Build a broadcast-wire-shaped frame for the classifier tests."""
    return {
        "id": fields.pop("id", "01HZTERM0000000000000000A1"),
        "event_type": event_type,
        "fields": {"source": source, **fields},
    }


# --- frozenset move-preservation -------------------------------------------


def test_frozensets_byte_identical_to_known_values() -> None:
    """The move from pr_monitor to _terminal must NOT change values."""
    assert SUCCESS_CONCLUSION == "success"
    assert frozenset({"failure", "cancelled", "timed_out"}) == FAILURE_CONCLUSIONS
    assert frozenset({"skipped", "neutral", "action_required", "stale"}) == NON_TERMINAL_CONCLUSIONS


def test_pr_monitor_agg_sql_uses_the_canonical_failure_set() -> None:
    """AGG_SQL is derived (sorted) from the canonical frozensets; assert
    the interpolated body contains every failure value, no others."""
    body = pr_monitor.AGG_SQL
    for value in FAILURE_CONCLUSIONS:
        assert f"'{value}'" in body
    # SUCCESS_CONCLUSION is interpolated too (per the canonical OK path).
    assert f"'{SUCCESS_CONCLUSION}'" in body


def test_wait_exit_code_if_terminal_agrees_with_frozensets() -> None:
    """wait._exit_code_if_terminal must bucket every value the same way
    _terminal does (no parallel hand-maintained map)."""
    assert wait._exit_code_if_terminal(SUCCESS_CONCLUSION) == 0
    for value in FAILURE_CONCLUSIONS:
        assert wait._exit_code_if_terminal(value) == 1
    for value in NON_TERMINAL_CONCLUSIONS:
        assert wait._exit_code_if_terminal(value) is None
    assert wait._exit_code_if_terminal(None) is None
    assert wait._exit_code_if_terminal("") is None


# --- entity_key: the per-source table --------------------------------------


def test_entity_key_github_run() -> None:
    f = _frame("github", "workflow_run", run_id=42, job_id=None)
    assert entity_key(f) == ("github", "run", "42")


def test_entity_key_github_job() -> None:
    f = _frame("github", "workflow_job", run_id=42, job_id=99)
    # workflow_job dispatches on job_id first, regardless of run_id presence.
    assert entity_key(f) == ("github", "job", "99")


def test_entity_key_github_without_identity_is_passthrough() -> None:
    """A malformed GitHub row missing its identity column returns None
    (pass-through) so the coalesced consumer never mis-collapses it."""
    assert entity_key(_frame("github", "workflow_job", job_id=None)) is None
    assert entity_key(_frame("github", "workflow_run", run_id=None)) is None


def test_entity_key_alertmanager_fingerprint() -> None:
    f = _frame("alertmanager", "prometheus_alert", alert_fingerprint="fp-abc")
    assert entity_key(f) == ("alertmanager", "alert", "fp-abc")


def test_entity_key_alertmanager_watchdog_is_passthrough() -> None:
    """Watchdog liveness has no stable per-firing entity; pass-through."""
    f = _frame("alertmanager", "prometheus_watchdog", alert_fingerprint=None)
    assert entity_key(f) is None


@pytest.mark.parametrize("source", ["pytest", "docker", "fs"])
def test_entity_key_local_sources_are_passthrough(source: str) -> None:
    """Local watcher sources have no stable upstream entity id; the
    coalesced consumer must deliver them verbatim."""
    f = _frame(source, "fs_change")
    assert entity_key(f) is None


def test_entity_key_no_fields_dict_is_none() -> None:
    """A defensively malformed frame (no fields dict) does not crash."""
    assert entity_key({"id": "x", "event_type": "workflow_run"}) is None


# --- entity_key: edge-case narrowing --------------------------------------


@pytest.mark.parametrize("run_id", [0, -1, "", None, True, False, 5.0, "5"])
def test_entity_key_github_run_rejects_non_positive_int(run_id: Any) -> None:
    """run_id must be ``type(v) is int and v > 0``; everything else
    pass-throughs.

    Notable cases:
    - bool: ``True`` / ``False`` are int subclasses; ``type(v) is int``
      (NOT isinstance) excludes them so we never emit ``("github",
      "run", "True")``.
    - 0, -1: GitHub's API guarantees positive int64; defence-in-depth.
    - float 5.0, str "5": shape errors at the listener boundary; the
      consumer-side narrowing here pairs with that.
    """
    f = _frame("github", "workflow_run", run_id=run_id)
    assert entity_key(f) is None


@pytest.mark.parametrize("job_id", [0, -1, "", None, True, False, 5.0, "5"])
def test_entity_key_github_job_rejects_non_positive_int(job_id: Any) -> None:
    """Same narrowing as ``run_id``: bool exclusion + positive int only."""
    f = _frame("github", "workflow_job", job_id=job_id)
    assert entity_key(f) is None


@pytest.mark.parametrize("fingerprint", ["", None, 0, 42, True, False, b"fp"])
def test_entity_key_alertmanager_rejects_non_string_fingerprint(
    fingerprint: Any,
) -> None:
    """Alert fingerprint must be a non-empty str; falsy / wrong-type
    values pass-through (deliver verbatim, never collapse)."""
    f = _frame("alertmanager", "prometheus_alert", alert_fingerprint=fingerprint)
    assert entity_key(f) is None


# --- is_terminal: the per-source rules -------------------------------------


def test_is_terminal_github_success() -> None:
    f = _frame("github", "workflow_run", run_id=1, conclusion="success")
    assert is_terminal(f) is True


@pytest.mark.parametrize("conclusion", sorted(FAILURE_CONCLUSIONS))
def test_is_terminal_github_failure_bucket(conclusion: str) -> None:
    f = _frame("github", "workflow_run", run_id=1, conclusion=conclusion)
    assert is_terminal(f) is True


@pytest.mark.parametrize("conclusion", sorted(NON_TERMINAL_CONCLUSIONS))
def test_is_terminal_github_non_terminal(conclusion: str) -> None:
    f = _frame("github", "workflow_run", run_id=1, conclusion=conclusion)
    assert is_terminal(f) is False


def test_is_terminal_github_none_or_empty() -> None:
    assert is_terminal(_frame("github", "workflow_run", run_id=1, conclusion=None)) is False
    assert is_terminal(_frame("github", "workflow_run", run_id=1, conclusion="")) is False


def test_is_terminal_alertmanager_resolved() -> None:
    f = _frame(
        "alertmanager",
        "prometheus_alert",
        alert_fingerprint="fp",
        status="resolved",
    )
    assert is_terminal(f) is True


def test_is_terminal_alertmanager_firing() -> None:
    f = _frame(
        "alertmanager",
        "prometheus_alert",
        alert_fingerprint="fp",
        status="firing",
    )
    assert is_terminal(f) is False


def test_is_terminal_watchdog_never_terminal() -> None:
    """prometheus_watchdog is liveness, never terminal even if status
    happens to be 'resolved' on the wire."""
    f = _frame(
        "alertmanager",
        "prometheus_watchdog",
        alert_fingerprint=None,
        status="resolved",
    )
    assert is_terminal(f) is False


@pytest.mark.parametrize("source", ["pytest", "docker", "fs"])
def test_is_terminal_local_sources_are_never_terminal(source: str) -> None:
    """Local sources have no terminal-state semantics; always False."""
    f = _frame(source, "fs_change")
    assert is_terminal(f) is False


def test_is_terminal_no_fields_dict() -> None:
    assert is_terminal({"id": "x", "event_type": "workflow_run"}) is False


# --- property-based coverage: arbitrary input space --------------------------

_field_values = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-(10**18), max_value=10**18),
    st.text(min_size=0, max_size=128),
    st.lists(st.integers(), max_size=4),
)

_arbitrary_fields = st.dictionaries(
    st.text(min_size=0, max_size=24),
    _field_values,
)


@given(
    source=st.text(min_size=0, max_size=24),
    event_type=st.text(min_size=0, max_size=64),
    fields=_arbitrary_fields,
)
@settings(max_examples=200, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_entity_key_never_raises_on_arbitrary_fields(
    source: str,
    event_type: str,
    fields: dict[str, Any],
) -> None:
    """entity_key must never raise and must return None or a 3-tuple of strings."""
    frame: dict[str, Any] = {
        "id": "01HZTERM0000000000000000A1",
        "event_type": event_type,
        "fields": {"source": source, **fields},
    }
    result = entity_key(frame)
    assert result is None or (
        isinstance(result, tuple) and len(result) == 3 and all(isinstance(e, str) for e in result)
    )


@given(
    source=st.text(min_size=0, max_size=24),
    event_type=st.text(min_size=0, max_size=64),
    fields=_arbitrary_fields,
)
@settings(max_examples=200, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_is_terminal_never_raises_on_arbitrary_fields(
    source: str,
    event_type: str,
    fields: dict[str, Any],
) -> None:
    """is_terminal must never raise and must always return a bool."""
    frame: dict[str, Any] = {
        "id": "01HZTERM0000000000000000A1",
        "event_type": event_type,
        "fields": {"source": source, **fields},
    }
    result = is_terminal(frame)
    assert isinstance(result, bool)
