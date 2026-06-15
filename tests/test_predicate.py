"""Unit tests for the source-agnostic event-match predicate engine.

Covers the Layer-1 grammar (``--match k=json_literal``, AND across
distinct keys, OR within a repeated key), the named-condition + Layer-2
evaluator registries, ``compose``, and the dotted-key resolver. Pure
in-process; no daemon, no socket, no SQLite.
"""

from __future__ import annotations

import dataclasses
import json
import re
from collections.abc import Iterator

import pytest

from waitbus import _predicate
from waitbus._predicate import (
    EvaluatorUnavailableError,
    Predicate,
    compose,
    head_sha_prefix,
    parse_match,
    register_condition,
    register_evaluator,
    resolve_condition,
    resolve_evaluator,
)

# --- registry isolation -----------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_registries() -> Iterator[None]:
    """Snapshot and restore the module-level registries around every test.

    The registries are module-globals by design (a registered condition
    must persist across the wait-CLI invocation that registered it), so
    tests have to clean up after themselves; an autouse fixture is the
    cheapest way to guarantee that.
    """
    saved_conditions = dict(_predicate._CONDITIONS)
    saved_evaluators = dict(_predicate._EVALUATORS)
    _predicate._CONDITIONS.clear()
    _predicate._EVALUATORS.clear()
    try:
        yield
    finally:
        _predicate._CONDITIONS.clear()
        _predicate._CONDITIONS.update(saved_conditions)
        _predicate._EVALUATORS.clear()
        _predicate._EVALUATORS.update(saved_evaluators)


# --- _dig (dotted-key resolver) ---------------------------------------------


class TestDig:
    def test_top_level_key(self) -> None:
        assert _predicate._dig({"a": 1}, "a") == 1

    def test_nested_dict(self) -> None:
        assert _predicate._dig({"a": {"b": {"c": 42}}}, "a.b.c") == 42

    def test_list_positive_index(self) -> None:
        assert _predicate._dig({"items": ["x", "y", "z"]}, "items.1") == "y"

    def test_list_negative_index(self) -> None:
        assert _predicate._dig({"items": ["x", "y", "z"]}, "items.-1") == "z"

    def test_list_out_of_range_is_missing(self) -> None:
        assert _predicate._dig({"items": ["x"]}, "items.5") is _predicate._MISSING

    def test_missing_key_is_missing(self) -> None:
        assert _predicate._dig({"a": 1}, "b") is _predicate._MISSING

    def test_missing_through_path_is_missing(self) -> None:
        assert _predicate._dig({"a": {"b": 1}}, "a.c.d") is _predicate._MISSING

    def test_scalar_mid_traversal_is_missing(self) -> None:
        # `a` resolves to 1 (a scalar); `.b` cannot continue.
        assert _predicate._dig({"a": 1}, "a.b") is _predicate._MISSING

    def test_json_null_value_is_preserved(self) -> None:
        # A JSON null is a legitimate match target (distinct from _MISSING).
        assert _predicate._dig({"a": None}, "a") is None
        assert _predicate._dig({"a": None}, "a") is not _predicate._MISSING

    def test_non_int_list_index_is_missing(self) -> None:
        assert _predicate._dig({"items": [1, 2]}, "items.foo") is _predicate._MISSING


# --- parse_match (Layer-1 grammar) ------------------------------------------


class TestParseMatchHappyPath:
    def test_empty_specs_is_tautology(self) -> None:
        pred = parse_match([])
        assert pred({}) is True
        assert pred({"anything": "goes"}) is True
        assert pred.source == ""

    def test_single_string_match(self) -> None:
        pred = parse_match(['fields.head_sha="abc123"'])
        assert pred({"fields": {"head_sha": "abc123"}}) is True
        assert pred({"fields": {"head_sha": "xyz789"}}) is False

    def test_single_int_match_typed(self) -> None:
        # JSON literal: 12345 is int, not string "12345".
        pred = parse_match(["fields.run_id=12345"])
        assert pred({"fields": {"run_id": 12345}}) is True
        assert pred({"fields": {"run_id": "12345"}}) is False

    def test_single_bool_match(self) -> None:
        pred = parse_match(["fields.merged=true"])
        assert pred({"fields": {"merged": True}}) is True
        assert pred({"fields": {"merged": False}}) is False

    def test_single_null_match(self) -> None:
        pred = parse_match(["fields.parent_run_id=null"])
        assert pred({"fields": {"parent_run_id": None}}) is True
        assert pred({"fields": {"parent_run_id": "abc"}}) is False

    def test_null_does_not_match_absent_key(self) -> None:
        # _MISSING sentinel guards against the silent-match-on-absent-key
        # defect; --match key=null must match an explicit JSON null,
        # NOT a missing key.
        pred = parse_match(["fields.parent_run_id=null"])
        assert pred({"fields": {}}) is False

    def test_single_list_literal_match(self) -> None:
        pred = parse_match(['fields.tags=["nightly","staging"]'])
        assert pred({"fields": {"tags": ["nightly", "staging"]}}) is True
        # Order matters; this is JSON equality, not set equality.
        assert pred({"fields": {"tags": ["staging", "nightly"]}}) is False

    def test_and_across_keys(self) -> None:
        pred = parse_match(['fields.source="github"', 'fields.conclusion="success"'])
        assert pred({"fields": {"source": "github", "conclusion": "success"}}) is True
        assert pred({"fields": {"source": "github", "conclusion": "failure"}}) is False
        assert pred({"fields": {"source": "docker", "conclusion": "success"}}) is False

    def test_or_within_repeated_key(self) -> None:
        pred = parse_match(['fields.conclusion="failure"', 'fields.conclusion="cancelled"'])
        assert pred({"fields": {"conclusion": "failure"}}) is True
        assert pred({"fields": {"conclusion": "cancelled"}}) is True
        assert pred({"fields": {"conclusion": "success"}}) is False

    def test_split_on_first_equals_only(self) -> None:
        # JSON RHS containing `=` (e.g. a base64-padded string) must
        # parse correctly; we split on the first `=` only.
        pred = parse_match(['fields.token="abc=="'])
        assert pred({"fields": {"token": "abc=="}}) is True

    def test_source_text_round_trip(self) -> None:
        # Predicate.source is the join of the spec list (forensic log).
        pred = parse_match(['fields.a="x"', "fields.b=42"])
        assert "fields.a=" in pred.source
        assert "fields.b=42" in pred.source


class TestParseMatchErrors:
    def test_missing_equals(self) -> None:
        with pytest.raises(ValueError, match="must be key=json_literal"):
            parse_match(["fields.bare_word"])

    def test_empty_key(self) -> None:
        with pytest.raises(ValueError, match="key must be non-empty"):
            parse_match(["=42"])

    def test_empty_dotted_segment(self) -> None:
        with pytest.raises(ValueError, match="empty dotted segment"):
            parse_match(['fields..head_sha="abc"'])

    def test_bad_json_rhs(self) -> None:
        with pytest.raises(ValueError, match="JSON literal"):
            parse_match(["fields.head_sha=abc"])  # bare word, not "abc"

    def test_over_length_single_spec(self) -> None:
        # The cap is per-spec: one giant spec is rejected.
        big = "x" * 300
        with pytest.raises(ValueError, match="exceeds 256-byte cap"):
            parse_match([f'fields.huge="{big}"'])

    def test_two_small_specs_can_legitimately_join_over_256(self) -> None:
        # AND-combining many in-bound specs that happen to JOIN to >256
        # bytes is legitimate; the cap is per-spec, not on the joined
        # source. Two ~150-byte specs (each well under 256) compose
        # cleanly even though the joined string is ~300 bytes.
        spec_a = f'fields.a="{"a" * 130}"'
        spec_b = f'fields.b="{"b" * 130}"'
        assert len(spec_a) < 256 and len(spec_b) < 256
        assert len(" ".join([spec_a, spec_b])) > 256
        pred = parse_match([spec_a, spec_b])  # must not raise
        assert pred({"fields": {"a": "a" * 130, "b": "b" * 130}}) is True

    def test_offending_spec_is_quoted_in_error(self) -> None:
        # Forensic-traceback requirement: the offending spec must appear
        # verbatim in the error message.
        with pytest.raises(ValueError, match=re.escape("'fields.bad=abc'")):
            parse_match(["fields.bad=abc"])


# --- compose -----------------------------------------------------------------


class TestCompose:
    def test_no_args_is_tautology(self) -> None:
        pred = compose()
        assert pred({}) is True
        assert pred.source == ""

    def test_single_predicate_passthrough(self) -> None:
        inner = parse_match(["fields.x=1"])
        composed = compose(inner)
        assert composed is inner  # short-circuit; no wrapping overhead.

    def test_and_short_circuits_on_first_false(self) -> None:
        calls: list[str] = []

        def make(name: str, returns: bool) -> Predicate:
            def _ev(_frame: dict[str, object]) -> bool:
                calls.append(name)
                return returns

            return Predicate(evaluate=_ev, source=name)

        composed = compose(make("a", True), make("b", False), make("c", True))
        assert composed({}) is False
        assert calls == ["a", "b"]  # c was never evaluated.

    def test_source_text_joins_with_ampersand(self) -> None:
        composed = compose(
            parse_match(["fields.a=1"]),
            parse_match(["fields.b=2"]),
        )
        assert " & " in composed.source

    def test_empty_source_skipped_in_join(self) -> None:
        composed = compose(compose(), parse_match(["fields.x=1"]))
        # The empty tautology's source must not leave a dangling " & ".
        assert not composed.source.startswith(" & ")
        assert not composed.source.endswith(" & ")


# --- register_condition / resolve_condition ---------------------------------


class TestConditionRegistry:
    def test_register_and_resolve(self) -> None:
        def factory() -> Predicate:
            return Predicate(evaluate=lambda f: f.get("ok") is True, source="ok-cond")

        register_condition("ok-cond", factory)
        pred = resolve_condition("ok-cond")
        assert pred({"ok": True}) is True
        assert pred({"ok": False}) is False

    def test_re_register_raises(self) -> None:
        register_condition("dup", lambda: parse_match([]))
        with pytest.raises(ValueError, match="already registered"):
            register_condition("dup", lambda: parse_match([]))

    def test_unknown_name_lists_registered(self) -> None:
        register_condition("known-a", lambda: parse_match([]))
        register_condition("known-b", lambda: parse_match([]))
        with pytest.raises(KeyError) as exc:
            resolve_condition("typo")
        # KeyError repr wraps in quotes; check the message via .args.
        msg = exc.value.args[0]
        assert "typo" in msg
        assert "known-a" in msg
        assert "known-b" in msg

    def test_unknown_name_when_registry_empty(self) -> None:
        with pytest.raises(KeyError, match="none registered"):
            resolve_condition("anything")

    @pytest.mark.parametrize(
        "bad_name",
        ["", "1starts-with-digit", "has space", "has/slash", "x" * 33],
    )
    def test_name_validation(self, bad_name: str) -> None:
        with pytest.raises(ValueError):
            register_condition(bad_name, lambda: parse_match([]))


# --- register_evaluator / resolve_evaluator (Layer-2 plugin slot) -----------


class TestEvaluatorRegistry:
    def test_unregistered_raises_unavailable_with_exact_message(self) -> None:
        with pytest.raises(EvaluatorUnavailableError) as exc:
            resolve_evaluator("cel", "fields.x > 5")
        assert str(exc.value) == "to use --match-cel, install waitbus[cel]"

    def test_unregistered_jmespath_message(self) -> None:
        with pytest.raises(EvaluatorUnavailableError) as exc:
            resolve_evaluator("jmespath", "fields.x")
        assert str(exc.value) == "to use --match-jmespath, install waitbus[jmespath]"

    def test_register_and_resolve(self) -> None:
        def fake_cel_factory(expr: str) -> Predicate:
            # Toy: matches iff the frame has fields.matches_expr == expr.
            return Predicate(
                evaluate=lambda f: f.get("fields", {}).get("matches_expr") == expr,
                source=f"cel:{expr}",
            )

        register_evaluator("cel", fake_cel_factory)
        pred = resolve_evaluator("cel", "x > 5")
        assert pred({"fields": {"matches_expr": "x > 5"}}) is True
        assert pred({"fields": {"matches_expr": "other"}}) is False
        assert pred.source == "cel:x > 5"

    def test_re_register_raises(self) -> None:
        register_evaluator("dup", lambda _expr: parse_match([]))
        with pytest.raises(ValueError, match="already registered"):
            register_evaluator("dup", lambda _expr: parse_match([]))

    def test_over_length_expression_rejected(self) -> None:
        register_evaluator("cel", lambda _expr: parse_match([]))
        with pytest.raises(ValueError, match="exceeds 256-byte cap"):
            resolve_evaluator("cel", "x" * 300)


# --- Predicate callable shape ------------------------------------------------


class TestPredicateValueType:
    def test_callable_protocol(self) -> None:
        pred = parse_match(["fields.x=1"])
        # Both call paths must work identically.
        assert pred({"fields": {"x": 1}}) is True
        assert pred.evaluate({"fields": {"x": 1}}) is True

    def test_frozen(self) -> None:
        pred = parse_match(["fields.x=1"])
        with pytest.raises(dataclasses.FrozenInstanceError):
            pred.source = "tampered"  # type: ignore[misc]


# --- generic exact head_sha match (the --match path, NOT --sha) -------------


class TestExactHeadShaMatch:
    """The generic `--match fields.head_sha=<json>` path is an EXACT match.

    This is distinct from `--sha`, which is a git-style prefix (see
    TestHeadShaPrefix). Verifies the exact path's bool-equivalence on the
    predicate side; the wait CLI's exit-code mapping is in
    tests/test_waitbus_wait.py.
    """

    def test_exact_head_sha_equivalence(self) -> None:
        sha = "abc123def456"
        exact = parse_match([f"fields.head_sha={json.dumps(sha)}"])

        # Equivalent hand-written matcher:
        def hand(frame: dict[str, object]) -> bool:
            fields = frame.get("fields")
            return isinstance(fields, dict) and fields.get("head_sha") == sha

        frames: list[dict[str, object]] = [
            {"fields": {"head_sha": sha}},
            {"fields": {"head_sha": "other"}},
            {"fields": {}},
            {},
            {"fields": {"head_sha": sha, "extra": "ignored"}},
        ]
        for frame in frames:
            assert exact(frame) == hand(frame), frame


# --- --sha git-style prefix predicate ---------------------------------------


class TestHeadShaPrefix:
    """`waitbus wait --sha` lowers to a case-insensitive head_sha PREFIX match."""

    def test_prefix_matches_full_sha(self) -> None:
        full = "abc1234def5678901234567890abcdef12345678"
        pred = head_sha_prefix(full[:7])
        assert pred({"fields": {"head_sha": full}})

    def test_full_length_behaves_like_exact(self) -> None:
        full = "abc1234def5678901234567890abcdef12345678"
        pred = head_sha_prefix(full)
        assert pred({"fields": {"head_sha": full}})
        assert not pred({"fields": {"head_sha": full[:-1] + "0"}})

    def test_case_insensitive(self) -> None:
        pred = head_sha_prefix("ABC1234")
        assert pred({"fields": {"head_sha": "abc1234def"}})

    def test_anchored_not_substring(self) -> None:
        pred = head_sha_prefix("abc1234")
        assert not pred({"fields": {"head_sha": "999abc1234def"}})

    def test_missing_or_non_str_head_sha_does_not_match(self) -> None:
        pred = head_sha_prefix("abc1234")
        assert not pred({"fields": {}})
        assert not pred({})
        assert not pred({"fields": {"head_sha": None}})
        assert not pred({"fields": {"head_sha": 1234567}})

    @pytest.mark.parametrize("bad", ["abc12", "a", ""])
    def test_too_short_raises(self, bad: str) -> None:
        with pytest.raises(ValueError, match="at least 7 hex"):
            head_sha_prefix(bad)

    @pytest.mark.parametrize("bad", ["nothex1", "ghijklm", "abc 123"])
    def test_non_hex_raises(self, bad: str) -> None:
        with pytest.raises(ValueError, match="hexadecimal"):
            head_sha_prefix(bad)

    def test_over_40_chars_raises(self) -> None:
        with pytest.raises(ValueError, match="40-char"):
            head_sha_prefix("a" * 41)
