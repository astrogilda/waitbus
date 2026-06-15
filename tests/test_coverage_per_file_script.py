"""Contract tests for scripts/coverage_per_file.py.

Covers:
- _load_coverage: missing file, malformed JSON, missing 'files' key.
- _is_excluded: exact-suffix match, glob match, no match.
- _run: all-pass, one-fail, excluded file skipped, threshold boundary.
- main: CLI argument parsing via subprocess to verify --help, exit codes.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SCRIPT = Path(__file__).parent.parent / "scripts" / "coverage_per_file.py"


def _make_coverage_json(
    tmp_path: Path,
    files: dict[str, float],
    filename: str = "coverage.json",
) -> Path:
    """Write a minimal coverage.json with the given file->pct mapping.

    Args:
        tmp_path: Temporary directory provided by pytest.
        files: Mapping of filepath string to percent_covered value.
        filename: Output filename inside tmp_path.

    Returns:
        Path to the written JSON file.
    """
    data: dict[str, Any] = {
        "files": {
            fp: {
                "summary": {"percent_covered": pct},
                "missing_lines": [],
            }
            for fp, pct in files.items()
        }
    }
    out = tmp_path / filename
    out.write_text(json.dumps(data))
    return out


def _run_script(*args: str) -> tuple[int, str, str]:
    """Run coverage_per_file.py as a subprocess and return (returncode, stdout, stderr).

    Args:
        *args: CLI arguments passed after the script path.

    Returns:
        Tuple of (returncode, stdout text, stderr text).
    """
    result = subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        capture_output=True,
        text=True,
    )
    return result.returncode, result.stdout, result.stderr


# ---------------------------------------------------------------------------
# Import the module under test (for unit-level tests)
# ---------------------------------------------------------------------------

_spec = importlib.util.spec_from_file_location("coverage_per_file", SCRIPT)
assert _spec is not None
assert _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

_load_coverage = _mod._load_coverage
_is_excluded = _mod._is_excluded
_run = _mod._run


# ---------------------------------------------------------------------------
# _load_coverage
# ---------------------------------------------------------------------------


class TestLoadCoverage:
    def test_missing_file_exits_2(self, tmp_path: Path) -> None:
        with pytest.raises(SystemExit) as exc_info:
            _load_coverage(tmp_path / "nonexistent.json")
        assert exc_info.value.code == 2

    def test_malformed_json_exits_2(self, tmp_path: Path) -> None:
        bad = tmp_path / "bad.json"
        bad.write_text("not json {{{")
        with pytest.raises(SystemExit) as exc_info:
            _load_coverage(bad)
        assert exc_info.value.code == 2

    def test_missing_files_key_exits_2(self, tmp_path: Path) -> None:
        no_files = tmp_path / "no_files.json"
        no_files.write_text(json.dumps({"meta": {}}))
        with pytest.raises(SystemExit) as exc_info:
            _load_coverage(no_files)
        assert exc_info.value.code == 2

    def test_valid_file_returns_dict(self, tmp_path: Path) -> None:
        cov = _make_coverage_json(tmp_path, {"waitbus/foo.py": 90.0})
        data = _load_coverage(cov)
        assert "files" in data
        assert "waitbus/foo.py" in data["files"]


# ---------------------------------------------------------------------------
# _is_excluded
# ---------------------------------------------------------------------------


class TestIsExcluded:
    def test_no_patterns_never_excluded(self) -> None:
        assert _is_excluded("waitbus/foo.py", []) is False

    def test_exact_suffix_match(self) -> None:
        assert _is_excluded("waitbus/replay.py", ["replay.py"]) is True

    def test_glob_wildcard_match(self) -> None:
        assert _is_excluded("waitbus/replay.py", ["**/replay.py"]) is True

    def test_glob_no_match(self) -> None:
        assert _is_excluded("waitbus/broadcast.py", ["**/replay.py"]) is False

    def test_multiple_patterns_first_matches(self) -> None:
        assert _is_excluded("waitbus/foo.py", ["bar.py", "foo.py"]) is True

    def test_partial_path_no_accidental_match(self) -> None:
        # "replay" must not accidentally match "replay_ext.py"
        assert _is_excluded("waitbus/replay_ext.py", ["replay.py"]) is False


# ---------------------------------------------------------------------------
# _run
# ---------------------------------------------------------------------------


class TestRun:
    def test_all_pass_returns_zero(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        data: dict[str, Any] = {
            "files": {
                "waitbus/a.py": {"summary": {"percent_covered": 90.0}, "missing_lines": []},
                "waitbus/b.py": {"summary": {"percent_covered": 80.0}, "missing_lines": []},
            }
        }
        code = _run(data, 80.0, [])
        assert code == 0
        out = capsys.readouterr().out
        assert "PASS" in out
        # Summary reads "2 PASS, 0 FAIL" which contains "FAIL"; verify no
        # per-file row status is FAIL by checking for "0 FAIL" in summary.
        assert "0 FAIL" in out

    def test_one_fail_returns_one(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        data: dict[str, Any] = {
            "files": {
                "waitbus/low.py": {"summary": {"percent_covered": 50.0}, "missing_lines": [1, 2]},
                "waitbus/ok.py": {"summary": {"percent_covered": 95.0}, "missing_lines": []},
            }
        }
        code = _run(data, 80.0, [])
        assert code == 1
        out = capsys.readouterr().out
        assert "FAIL" in out
        assert "waitbus/low.py" in out

    def test_excluded_file_shows_skip(self, capsys: pytest.CaptureFixture[str]) -> None:
        data: dict[str, Any] = {
            "files": {
                "waitbus/skip.py": {"summary": {"percent_covered": 10.0}, "missing_lines": [1]},
            }
        }
        code = _run(data, 80.0, ["skip.py"])
        assert code == 0  # excluded file doesn't cause failure
        out = capsys.readouterr().out
        assert "SKIP" in out

    def test_threshold_boundary_exactly_at_threshold_passes(self, capsys: pytest.CaptureFixture[str]) -> None:
        data: dict[str, Any] = {
            "files": {
                "waitbus/exact.py": {"summary": {"percent_covered": 80.0}, "missing_lines": []},
            }
        }
        code = _run(data, 80.0, [])
        assert code == 0

    def test_threshold_boundary_one_below_fails(self, capsys: pytest.CaptureFixture[str]) -> None:
        data: dict[str, Any] = {
            "files": {
                "waitbus/low.py": {"summary": {"percent_covered": 79.9}, "missing_lines": [5]},
            }
        }
        code = _run(data, 80.0, [])
        assert code == 1

    def test_custom_threshold_lower(self, capsys: pytest.CaptureFixture[str]) -> None:
        data: dict[str, Any] = {
            "files": {
                "waitbus/partial.py": {"summary": {"percent_covered": 70.0}, "missing_lines": [1]},
            }
        }
        code = _run(data, 65.0, [])
        assert code == 0

    def test_missing_lines_count_displayed(self, capsys: pytest.CaptureFixture[str]) -> None:
        data: dict[str, Any] = {
            "files": {
                "waitbus/x.py": {
                    "summary": {"percent_covered": 50.0},
                    "missing_lines": [10, 20, 30],
                },
            }
        }
        _run(data, 80.0, [])
        out = capsys.readouterr().out
        assert "3" in out  # 3 uncovered lines shown


# ---------------------------------------------------------------------------
# CLI (subprocess) tests
# ---------------------------------------------------------------------------


class TestCLI:
    def test_help_exit_zero(self) -> None:
        code, out, _ = _run_script("--help")
        assert code == 0
        assert "threshold" in out.lower()

    def test_missing_coverage_file_exit_two(self) -> None:
        code, _, _ = _run_script("/nonexistent/coverage.json")
        assert code == 2

    def test_all_pass_exit_zero(self, tmp_path: Path) -> None:
        cov = _make_coverage_json(tmp_path, {"waitbus/a.py": 90.0})
        code, _, _ = _run_script(str(cov))
        assert code == 0

    def test_failing_file_exit_one(self, tmp_path: Path) -> None:
        cov = _make_coverage_json(tmp_path, {"waitbus/low.py": 40.0})
        code, out, _ = _run_script(str(cov))
        assert code == 1
        assert "FAIL" in out

    def test_custom_threshold_flag(self, tmp_path: Path) -> None:
        cov = _make_coverage_json(tmp_path, {"waitbus/partial.py": 70.0})
        # 70% passes at --threshold 65
        code_low, _, _ = _run_script(str(cov), "--threshold", "65")
        assert code_low == 0
        # 70% fails at --threshold 80 (default)
        code_high, _, _ = _run_script(str(cov))
        assert code_high == 1

    def test_exclude_flag_skips_file(self, tmp_path: Path) -> None:
        cov = _make_coverage_json(tmp_path, {"waitbus/low.py": 10.0})
        code, out, _ = _run_script(str(cov), "--exclude", "low.py")
        assert code == 0
        assert "SKIP" in out

    def test_exclude_repeatable(self, tmp_path: Path) -> None:
        cov = _make_coverage_json(
            tmp_path,
            {
                "waitbus/a.py": 10.0,
                "waitbus/b.py": 10.0,
                "waitbus/ok.py": 90.0,
            },
        )
        code, out, _ = _run_script(str(cov), "--exclude", "a.py", "--exclude", "b.py")
        assert code == 0
        assert out.count("SKIP") == 2
