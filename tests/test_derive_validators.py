"""Unit tests for the five ``_validate_*`` helpers in ``scripts/derive_gh_distributions``.

Each helper is tested with:
- a minimal valid document that produces no errors (happy path), and
- a document missing a required element that produces an expected error (error path).

Documents are constructed inline so the tests document the expected schema
shape for each validator without depending on the committed TOML file.
"""

from __future__ import annotations

from typing import Any

import pytest

from scripts.derive_gh_distributions import (
    _validate_hawkes,
    _validate_meta_fields,
    _validate_probability_tables,
    _validate_top_level_tables,
    _validate_transitions,
)

# ---------------------------------------------------------------------------
# Minimal valid document shapes (used as baselines)
# ---------------------------------------------------------------------------

_VALID_TOP_LEVEL: dict[str, Any] = {
    "meta": {},
    "workflow_names": {},
    "branch_patterns": {},
    "conclusions": {},
    "exit_codes_on_failure": {},
    "hawkes": {},
    "transitions": {},
    "source_mix_default": {},
}

_VALID_META: dict[str, Any] = {
    "meta": {
        "schema_version": 1,
        "generator_version": "0.1.0",
        "derived_at_iso": "2026-01-01T00:00:00Z",
        "derivation_mode": "test",
        "sources": [],
        "licenses": [],
    }
}

_VALID_HAWKES: dict[str, Any] = {
    "hawkes": {
        "small": {"mu_per_sec": 0.1, "alpha": 0.5, "beta": 1.0},
        "medium": {"mu_per_sec": 0.2, "alpha": 0.5, "beta": 1.0},
        "large": {"mu_per_sec": 0.3, "alpha": 0.5, "beta": 1.0},
    }
}

_VALID_TRANSITIONS: dict[str, Any] = {
    "transitions": {
        "success": {"success": 0.8, "failure": 0.2},
    }
}


# ---------------------------------------------------------------------------
# _validate_top_level_tables
# ---------------------------------------------------------------------------


class TestValidateTopLevelTables:
    def test_happy_path_all_tables_present(self) -> None:
        errors = _validate_top_level_tables(_VALID_TOP_LEVEL)
        assert errors == []

    def test_error_path_missing_table(self) -> None:
        doc = {k: v for k, v in _VALID_TOP_LEVEL.items() if k != "hawkes"}
        errors = _validate_top_level_tables(doc)
        assert len(errors) == 1
        assert "[top-tables]" in errors[0]
        assert "hawkes" in errors[0]


# ---------------------------------------------------------------------------
# _validate_meta_fields
# ---------------------------------------------------------------------------


class TestValidateMetaFields:
    def test_happy_path_all_fields_present(self) -> None:
        errors = _validate_meta_fields(_VALID_META)
        assert errors == []

    def test_error_path_missing_field(self) -> None:
        doc: dict[str, Any] = {"meta": {k: v for k, v in _VALID_META["meta"].items() if k != "schema_version"}}
        errors = _validate_meta_fields(doc)
        assert len(errors) == 1
        assert "[meta]" in errors[0]
        assert "schema_version" in errors[0]


# ---------------------------------------------------------------------------
# _validate_probability_tables
# ---------------------------------------------------------------------------


class TestValidateProbabilityTables:
    def test_happy_path_sums_to_one(self) -> None:
        doc: dict[str, Any] = {"workflow_names": {"CI": 0.6, "Build": 0.4}}
        errors = _validate_probability_tables(doc)
        assert errors == []

    def test_error_path_does_not_sum_to_one(self) -> None:
        doc: dict[str, Any] = {"workflow_names": {"CI": 0.6, "Build": 0.2}}
        errors = _validate_probability_tables(doc)
        assert len(errors) == 1
        assert "[probabilities]" in errors[0]
        assert "workflow_names" in errors[0]

    def test_happy_path_empty_table_skipped(self) -> None:
        # Empty tables are not validated — they may represent a skeleton TOML.
        doc: dict[str, Any] = {"workflow_names": {}}
        errors = _validate_probability_tables(doc)
        assert errors == []


# ---------------------------------------------------------------------------
# _validate_hawkes
# ---------------------------------------------------------------------------


class TestValidateHawkes:
    def test_happy_path_all_classes_and_params_present(self) -> None:
        errors = _validate_hawkes(_VALID_HAWKES)
        assert errors == []

    def test_error_path_missing_class(self) -> None:
        doc: dict[str, Any] = {
            "hawkes": {
                "small": {"mu_per_sec": 0.1, "alpha": 0.5, "beta": 1.0},
                "medium": {"mu_per_sec": 0.2, "alpha": 0.5, "beta": 1.0},
                # "large" absent
            }
        }
        errors = _validate_hawkes(doc)
        assert len(errors) == 1
        assert "[hawkes]" in errors[0]
        assert "large" in errors[0]

    def test_error_path_missing_param(self) -> None:
        doc: dict[str, Any] = {
            "hawkes": {
                "small": {"mu_per_sec": 0.1, "alpha": 0.5},  # missing "beta"
                "medium": {"mu_per_sec": 0.2, "alpha": 0.5, "beta": 1.0},
                "large": {"mu_per_sec": 0.3, "alpha": 0.5, "beta": 1.0},
            }
        }
        errors = _validate_hawkes(doc)
        assert len(errors) == 1
        assert "[hawkes]" in errors[0]
        assert "small.beta" in errors[0]

    def test_error_path_non_positive_param(self) -> None:
        doc: dict[str, Any] = {
            "hawkes": {
                "small": {"mu_per_sec": -1.0, "alpha": 0.5, "beta": 1.0},
                "medium": {"mu_per_sec": 0.2, "alpha": 0.5, "beta": 1.0},
                "large": {"mu_per_sec": 0.3, "alpha": 0.5, "beta": 1.0},
            }
        }
        errors = _validate_hawkes(doc)
        assert len(errors) == 1
        assert "[hawkes]" in errors[0]
        assert "positive" in errors[0]


# ---------------------------------------------------------------------------
# _validate_transitions
# ---------------------------------------------------------------------------


class TestValidateTransitions:
    def test_happy_path_rows_sum_to_one(self) -> None:
        errors = _validate_transitions(_VALID_TRANSITIONS)
        assert errors == []

    def test_error_path_row_does_not_sum_to_one(self) -> None:
        doc: dict[str, Any] = {
            "transitions": {
                "success": {"success": 0.9, "failure": 0.5},  # sums to 1.4
            }
        }
        errors = _validate_transitions(doc)
        assert len(errors) == 1
        assert "[transitions]" in errors[0]
        assert "success" in errors[0]

    @pytest.mark.parametrize("bad_value", [None, "not-a-dict", 42])
    def test_happy_path_non_dict_transitions_skipped(self, bad_value: Any) -> None:
        # A non-dict top-level transitions value returns no errors.
        doc: dict[str, Any] = {"transitions": bad_value}
        errors = _validate_transitions(doc)
        assert errors == []
