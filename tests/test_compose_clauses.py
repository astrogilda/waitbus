"""Unit tests for cross-source clause composition.

Covers the OR predicate combinator (``_predicate.compose_any``), the
``source:expr`` clause parser, the per-clause predicate builder, and the
sticky-satisfaction conjunction tracker (``_compose.AllOfTracker``).
Pure-dict frames, no daemon -- mirrors ``test_predicate.py``'s shape.
"""

from __future__ import annotations

from typing import Any

import pytest

from waitbus._compose import AllOfTracker, Clause, clause_predicate, parse_clause
from waitbus._predicate import Predicate, compose_any, parse_match

# --- compose_any (OR combinator) ---------------------------------------------


def test_compose_any_zero_args_is_always_false() -> None:
    falsum = compose_any()
    assert falsum({"fields": {"anything": 1}}) is False
    assert falsum({}) is False
    assert falsum.source == ""


def test_compose_any_single_predicate_is_returned_unchanged() -> None:
    p = parse_match(['fields.source="docker"'])
    assert compose_any(p) is p


def test_compose_any_matches_when_any_predicate_matches() -> None:
    a = parse_match(['fields.source="docker"'])
    b = parse_match(['fields.source="pytest"'])
    either = compose_any(a, b)
    assert either({"fields": {"source": "docker"}}) is True
    assert either({"fields": {"source": "pytest"}}) is True
    assert either({"fields": {"source": "github"}}) is False


def test_compose_any_order_does_not_change_verdict() -> None:
    a = parse_match(['fields.source="docker"'])
    b = parse_match(['fields.source="pytest"'])
    frame = {"fields": {"source": "pytest"}}
    assert compose_any(a, b)(frame) == compose_any(b, a)(frame)


def test_compose_any_source_joins_with_pipe() -> None:
    a = parse_match(['fields.source="docker"'])
    b = parse_match(['fields.source="pytest"'])
    assert compose_any(a, b).source == f"{a.source} | {b.source}"


def test_compose_any_skips_empty_source_text_in_join() -> None:
    a = parse_match(['fields.source="docker"'])
    blank = Predicate(evaluate=lambda _f: False, source="")
    assert compose_any(a, blank).source == a.source


# --- parse_clause -------------------------------------------------------------


def test_parse_clause_happy_path() -> None:
    assert parse_clause('pytest:fields.conclusion="success"') == Clause(
        source="pytest",
        spec='fields.conclusion="success"',
        text='pytest:fields.conclusion="success"',
    )


def test_parse_clause_splits_on_first_colon_only() -> None:
    # A colon inside the JSON value (after '=') belongs to the spec half.
    assert parse_clause('fs:fields.path="a:b"') == Clause(
        source="fs", spec='fields.path="a:b"', text='fs:fields.path="a:b"'
    )


def test_parse_clause_carries_the_verbatim_text() -> None:
    # The text field is the clause exactly as typed, not a reconstruction.
    clause = parse_clause('docker:fields.action="die"')
    assert clause.text == 'docker:fields.action="die"'


def test_parse_clause_no_colon_is_error() -> None:
    with pytest.raises(ValueError, match="must be source:key=json_literal"):
        parse_clause('fields.conclusion="success"')


def test_parse_clause_empty_source_is_error() -> None:
    with pytest.raises(ValueError, match="clause source must match"):
        parse_clause(':fields.x="y"')


def test_parse_clause_bad_source_charset_is_error() -> None:
    # A forgotten source prefix mis-parses into a non-source left half and
    # must fail charset validation naming the clause verbatim.
    with pytest.raises(ValueError, match="clause source must match") as excinfo:
        parse_clause('fields.x="a:b"')
    assert 'fields.x="a' in str(excinfo.value)


def test_parse_clause_uppercase_source_is_error() -> None:
    with pytest.raises(ValueError, match="clause source must match"):
        parse_clause('Docker:fields.action="die"')


def test_parse_clause_overlong_source_is_error() -> None:
    with pytest.raises(ValueError, match="clause source must match"):
        parse_clause("a" * 33 + ':fields.x="y"')


def test_parse_clause_empty_expression_is_error() -> None:
    with pytest.raises(ValueError, match="clause expression must be non-empty"):
        parse_clause("docker:")


# --- clause_predicate ---------------------------------------------------------


def test_clause_predicate_matches_only_its_source() -> None:
    pred = clause_predicate(parse_clause('docker:fields.event_type="docker_container"'))
    assert pred({"fields": {"source": "docker", "event_type": "docker_container"}}) is True
    # Same key, wrong source: must NOT match.
    assert pred({"fields": {"source": "pytest", "event_type": "docker_container"}}) is False
    # Right source, wrong value: must NOT match.
    assert pred({"fields": {"source": "docker", "event_type": "other"}}) is False
    # Missing fields entirely: must NOT match.
    assert pred({}) is False


def test_clause_predicate_source_is_the_verbatim_clause_text() -> None:
    # Timeout output prints Predicate.source: it must be the clause as
    # typed, never the lowered 'fields.source=... key=lit' join.
    pred = clause_predicate(parse_clause('docker:fields.event_type="docker_container"'))
    assert pred.source == 'docker:fields.event_type="docker_container"'
    assert "fields.source=" not in pred.source


def test_clause_predicate_malformed_spec_propagates_valueerror() -> None:
    with pytest.raises(ValueError, match="must be key=json_literal"):
        clause_predicate(parse_clause("docker:bare_word"))


def test_clause_predicate_bad_json_rhs_propagates_valueerror() -> None:
    with pytest.raises(ValueError, match="JSON literal"):
        clause_predicate(parse_clause("docker:fields.x=not_json"))


def test_clause_predicate_overlong_spec_propagates_valueerror() -> None:
    spec = 'fields.x="' + "a" * 300 + '"'
    with pytest.raises(ValueError, match="256-byte cap"):
        clause_predicate(parse_clause(f"docker:{spec}"))


# --- AllOfTracker -------------------------------------------------------------


def _frame(source: str, **fields: Any) -> dict[str, Any]:
    return {"fields": {"source": source, **fields}}


def _two_clause_tracker() -> AllOfTracker:
    return AllOfTracker(
        [
            clause_predicate(parse_clause('pytest:fields.event_type="pytest_session"')),
            clause_predicate(parse_clause('docker:fields.event_type="docker_container"')),
        ]
    )


def test_tracker_sticky_across_frames_in_order() -> None:
    tracker = _two_clause_tracker()
    assert tracker.update(_frame("pytest", event_type="pytest_session")) is False
    assert tracker.update(_frame("docker", event_type="docker_container")) is True


def test_tracker_sticky_across_frames_reversed_order() -> None:
    tracker = _two_clause_tracker()
    assert tracker.update(_frame("docker", event_type="docker_container")) is False
    assert tracker.update(_frame("pytest", event_type="pytest_session")) is True


def test_tracker_satisfied_clause_stays_satisfied() -> None:
    tracker = _two_clause_tracker()
    tracker.update(_frame("pytest", event_type="pytest_session"))
    # Non-matching noise must not unsatisfy the already-satisfied clause.
    assert tracker.update(_frame("fs", event_type="fs_change")) is False
    assert tracker.outstanding == ('docker:fields.event_type="docker_container"',)
    assert tracker.update(_frame("docker", event_type="docker_container")) is True


def test_tracker_single_frame_can_satisfy_multiple_clauses() -> None:
    tracker = AllOfTracker(
        [
            clause_predicate(parse_clause('docker:fields.event_type="docker_container"')),
            clause_predicate(parse_clause('docker:fields.conclusion="success"')),
        ]
    )
    frame = _frame("docker", event_type="docker_container", conclusion="success")
    assert tracker.update(frame) is True
    assert tracker.outstanding == ()


def test_tracker_non_matching_frames_leave_all_outstanding() -> None:
    tracker = _two_clause_tracker()
    assert tracker.update(_frame("github", event_type="workflow_job")) is False
    assert len(tracker.outstanding) == 2


def test_tracker_outstanding_names_unsatisfied_clauses_verbatim() -> None:
    # The timeout message prints these: they must be the operator's typed
    # source:spec clause, never the lowered fields.source=... predicate.
    tracker = _two_clause_tracker()
    tracker.update(_frame("docker", event_type="docker_container"))
    (remaining,) = tracker.outstanding
    assert remaining == 'pytest:fields.event_type="pytest_session"'
    assert "fields.source=" not in remaining


def test_tracker_repeated_same_source_clauses_are_independent() -> None:
    tracker = AllOfTracker(
        [
            clause_predicate(parse_clause('docker:fields.action="start"')),
            clause_predicate(parse_clause('docker:fields.action="die"')),
        ]
    )
    assert tracker.update(_frame("docker", action="start")) is False
    assert tracker.update(_frame("docker", action="die")) is True
