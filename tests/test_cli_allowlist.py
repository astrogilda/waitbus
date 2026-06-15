"""CLI-layer tests for ``waitbus allowlist`` (list / add / remove / verify / repair).

Covers every verb's exit-code contract from ``docs/EXIT_CODES.md`` plus
the on-disk allowlist-file shape after each mutation. Pairs the
plugin-policy contract tests in ``tests/test_plugin_config.py`` (which
exercise the model layer) with the typer-layer wiring + exit-code
mapping.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from waitbus import cli

# sysexits-aligned exit codes are imported from the canonical seam so
# any future re-numbering tracks here automatically.
from waitbus.cli._exit_codes import (
    EX_CONFIG,
    EX_DATAERR,
    EX_NOINPUT,
    EX_OK,
    EX_PROTOCOL,
)
from waitbus.sources._attestation import (
    AttestationToolingMissingError,
    AttestationVerificationError,
    VerifiedPublisher,
)
from waitbus.sources._config import append_publisher_pin, load_allowlist


def _ep(name: str, dist: Any | None = None) -> MagicMock:
    """MagicMock shaped like importlib.metadata.EntryPoint."""
    ep = MagicMock()
    ep.name = name
    ep.value = f"fake_pkg:{name}"
    ep.load.return_value = MagicMock()
    ep.dist = dist
    return ep


# ---------------------------------------------------------------------------
# allowlist list
# ---------------------------------------------------------------------------


def test_allowlist_list_empty_text_says_no_pins(isolated_waitbus_config: Path) -> None:
    """`waitbus allowlist list` on an empty allowlist prints '(no publisher pins recorded)'."""
    runner = CliRunner()
    result = runner.invoke(cli.app, ["allowlist", "list"])
    assert result.exit_code == EX_OK, result.output
    assert "(no publisher pins recorded)" in result.output


def test_allowlist_list_empty_json_emits_empty_array(isolated_waitbus_config: Path) -> None:
    """`waitbus allowlist list --json` on an empty allowlist emits an empty JSON array."""
    runner = CliRunner()
    result = runner.invoke(cli.app, ["allowlist", "list", "--json"])
    assert result.exit_code == EX_OK, result.output
    assert json.loads(result.output) == []


def test_allowlist_list_populated_text_renders_aligned_table(isolated_waitbus_config: Path) -> None:
    """Two pre-seeded pins render as a 4-column aligned table."""
    append_publisher_pin(name="alpha", publisher_kind="GitHub", publisher_identity="org/alpha @ wf.yml")
    append_publisher_pin(name="bravo", publisher_kind="GitHub", publisher_identity="org/bravo @ wf.yml")
    runner = CliRunner()
    result = runner.invoke(cli.app, ["allowlist", "list"])
    assert result.exit_code == EX_OK, result.output
    # Header row + at least both names present.
    assert "name" in result.output
    assert "publisher-kind" in result.output
    assert "alpha" in result.output
    assert "bravo" in result.output


def test_allowlist_list_populated_json_sorted_with_documented_keys(isolated_waitbus_config: Path) -> None:
    """--json output is a list of dicts with the four documented keys."""
    append_publisher_pin(name="charlie", publisher_kind="GitHub", publisher_identity="org/charlie @ wf.yml")
    runner = CliRunner()
    result = runner.invoke(cli.app, ["allowlist", "list", "--json"])
    assert result.exit_code == EX_OK, result.output
    payload = json.loads(result.output)
    assert len(payload) == 1
    assert set(payload[0].keys()) == {"name", "publisher-kind", "publisher-identity", "first-pinned-at"}


# ---------------------------------------------------------------------------
# allowlist add
# ---------------------------------------------------------------------------


def test_allowlist_add_records_new_pin(isolated_waitbus_config: Path) -> None:
    """`waitbus allowlist add` records a new pin in the on-disk file."""
    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "allowlist",
            "add",
            "circleci",
            "--publisher-kind",
            "GitHub",
            "--publisher-identity",
            "org/waitbus-circleci @ .github/workflows/release.yml",
        ],
    )
    assert result.exit_code == EX_OK, result.output
    assert "pinned" in result.output
    pin = load_allowlist().for_source("circleci")
    assert pin is not None
    assert pin.publisher_kind == "GitHub"


def test_allowlist_add_rejects_rebinding_existing_pin(isolated_waitbus_config: Path) -> None:
    """Adding a second pin for an already-pinned name exits 2 (typer Exit code)."""
    append_publisher_pin(name="dup", publisher_kind="GitHub", publisher_identity="org/old @ wf.yml")
    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "allowlist",
            "add",
            "dup",
            "--publisher-kind",
            "GitHub",
            "--publisher-identity",
            "org/new @ wf.yml",
        ],
    )
    assert result.exit_code == 2
    assert "already pinned" in result.output
    assert "allowlist remove" in result.output


# ---------------------------------------------------------------------------
# allowlist remove
# ---------------------------------------------------------------------------


def test_allowlist_remove_deletes_existing(isolated_waitbus_config: Path) -> None:
    """`waitbus allowlist remove <name>` deletes an existing pin."""
    append_publisher_pin(name="goner", publisher_kind="GitHub", publisher_identity="org/goner @ wf.yml")
    runner = CliRunner()
    result = runner.invoke(cli.app, ["allowlist", "remove", "goner"])
    assert result.exit_code == EX_OK, result.output
    assert load_allowlist().for_source("goner") is None


def test_allowlist_remove_missing_exits_2(isolated_waitbus_config: Path) -> None:
    """Removing a name with no recorded pin exits 2 (informational error)."""
    runner = CliRunner()
    result = runner.invoke(cli.app, ["allowlist", "remove", "never_pinned"])
    assert result.exit_code == 2
    assert "no recorded pin" in result.output


# ---------------------------------------------------------------------------
# allowlist verify -- sysexits.h exit-code coverage
# ---------------------------------------------------------------------------


def test_allowlist_verify_no_recorded_pin_exits_ex_noinput(isolated_waitbus_config: Path) -> None:
    """No recorded pin -> EX_NOINPUT (66)."""
    runner = CliRunner()
    result = runner.invoke(cli.app, ["allowlist", "verify", "ghost"])
    assert result.exit_code == EX_NOINPUT
    assert "no recorded pin" in result.output


def test_allowlist_verify_no_installed_plugin_exits_ex_noinput(isolated_waitbus_config: Path) -> None:
    """Pin recorded but plugin not installed -> EX_NOINPUT (66) (was exit-0 false-pass)."""
    append_publisher_pin(name="absent_plugin", publisher_kind="GitHub", publisher_identity="org/absent @ wf.yml")

    with patch("waitbus.sources._registry.entry_points_by_name", return_value={}):
        runner = CliRunner()
        result = runner.invoke(cli.app, ["allowlist", "verify", "absent_plugin"])
    assert result.exit_code == EX_NOINPUT
    assert "not currently installed" in result.output


def test_allowlist_verify_tooling_missing_exits_ex_config(isolated_waitbus_config: Path) -> None:
    """waitbus[plugin-verify] missing -> EX_CONFIG (78) (was exit-0 false-pass)."""
    append_publisher_pin(name="tooling_dep", publisher_kind="GitHub", publisher_identity="org/x @ wf.yml")

    with (
        patch(
            "waitbus.sources._registry.entry_points_by_name",
            return_value={"tooling_dep": _ep("tooling_dep", dist=MagicMock())},
        ),
        patch(
            "waitbus.sources._attestation.verify_distribution",
            side_effect=AttestationToolingMissingError("install waitbus[plugin-verify]"),
        ),
    ):
        runner = CliRunner()
        result = runner.invoke(cli.app, ["allowlist", "verify", "tooling_dep"])
    assert result.exit_code == EX_CONFIG
    assert "attestations tooling not available" in result.output


def test_allowlist_verify_no_attestation_exits_ex_dataerr(isolated_waitbus_config: Path) -> None:
    """Wheel installed but no PEP 740 attestation -> EX_DATAERR (65)."""
    append_publisher_pin(name="no_att", publisher_kind="GitHub", publisher_identity="org/x @ wf.yml")

    with (
        patch(
            "waitbus.sources._registry.entry_points_by_name",
            return_value={"no_att": _ep("no_att", dist=MagicMock())},
        ),
        patch("waitbus.sources._attestation.verify_distribution", return_value=None),
    ):
        runner = CliRunner()
        result = runner.invoke(cli.app, ["allowlist", "verify", "no_att"])
    assert result.exit_code == EX_DATAERR
    assert "(none)" in result.output


def test_allowlist_verify_match_exits_ex_ok(isolated_waitbus_config: Path) -> None:
    """Live attestation matches the recorded pin -> EX_OK (0)."""
    append_publisher_pin(name="ok_plugin", publisher_kind="GitHub", publisher_identity="org/ok @ wf.yml")

    matching = VerifiedPublisher(
        publisher_kind="GitHub",
        publisher_identity="org/ok @ wf.yml",
        predicate_type="https://docs.pypi.org/attestations/publish/v1",
    )
    with (
        patch(
            "waitbus.sources._registry.entry_points_by_name",
            return_value={"ok_plugin": _ep("ok_plugin", dist=MagicMock())},
        ),
        patch("waitbus.sources._attestation.verify_distribution", return_value=matching),
    ):
        runner = CliRunner()
        result = runner.invoke(cli.app, ["allowlist", "verify", "ok_plugin"])
    assert result.exit_code == EX_OK, result.output
    assert "result: match" in result.output


def test_allowlist_verify_mismatch_exits_ex_protocol(isolated_waitbus_config: Path) -> None:
    """Live attestation disagrees with the recorded pin -> EX_PROTOCOL (76)."""
    append_publisher_pin(name="moved", publisher_kind="GitHub", publisher_identity="org/old @ wf.yml")

    different = VerifiedPublisher(
        publisher_kind="GitHub",
        publisher_identity="org/new @ wf.yml",
        predicate_type="https://docs.pypi.org/attestations/publish/v1",
    )
    with (
        patch(
            "waitbus.sources._registry.entry_points_by_name",
            return_value={"moved": _ep("moved", dist=MagicMock())},
        ),
        patch("waitbus.sources._attestation.verify_distribution", return_value=different),
    ):
        runner = CliRunner()
        result = runner.invoke(cli.app, ["allowlist", "verify", "moved"])
    assert result.exit_code == EX_PROTOCOL
    assert "MISMATCH" in result.output


def test_allowlist_verify_live_verify_failure_exits_ex_protocol(isolated_waitbus_config: Path) -> None:
    """Sigstore verify failure during live comparison -> EX_PROTOCOL (76)."""
    append_publisher_pin(name="bad_sig", publisher_kind="GitHub", publisher_identity="org/x @ wf.yml")

    with (
        patch(
            "waitbus.sources._registry.entry_points_by_name",
            return_value={"bad_sig": _ep("bad_sig", dist=MagicMock())},
        ),
        patch(
            "waitbus.sources._attestation.verify_distribution",
            side_effect=AttestationVerificationError("signature mismatch"),
        ),
    ):
        runner = CliRunner()
        result = runner.invoke(cli.app, ["allowlist", "verify", "bad_sig"])
    assert result.exit_code == EX_PROTOCOL
    assert "live verification failed" in result.output


# ---------------------------------------------------------------------------
# allowlist repair
# ---------------------------------------------------------------------------


def test_allowlist_repair_canonicalises_valid_file(isolated_waitbus_config: Path) -> None:
    """`waitbus allowlist repair` rewrites a valid file in canonical form."""
    append_publisher_pin(name="zeta", publisher_kind="GitHub", publisher_identity="org/zeta @ wf.yml")
    append_publisher_pin(name="alpha", publisher_kind="GitHub", publisher_identity="org/alpha @ wf.yml")
    runner = CliRunner()
    result = runner.invoke(cli.app, ["allowlist", "repair"])
    assert result.exit_code == EX_OK, result.output
    assert "repaired" in result.output

    # After repair, the file is sorted by name (alpha then zeta).
    content = (isolated_waitbus_config / "waitbus" / "plugins.allowlist.toml").read_text(encoding="utf-8")
    assert content.find('name = "alpha"') < content.find('name = "zeta"')


def test_allowlist_repair_dry_run_does_not_modify_file(isolated_waitbus_config: Path) -> None:
    """--dry-run prints the canonical form to stdout without modifying the file."""
    append_publisher_pin(name="dry", publisher_kind="GitHub", publisher_identity="org/dry @ wf.yml")
    path = isolated_waitbus_config / "waitbus" / "plugins.allowlist.toml"
    before = path.read_text(encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(cli.app, ["allowlist", "repair", "--dry-run"])
    assert result.exit_code == EX_OK, result.output
    assert 'name = "dry"' in result.output
    assert path.read_text(encoding="utf-8") == before


def test_allowlist_repair_unparseable_toml_exits_2(isolated_waitbus_config: Path) -> None:
    """Repair on syntactically-broken TOML exits 2 with a parser error message."""
    waitbus_dir = isolated_waitbus_config / "waitbus"
    waitbus_dir.mkdir(parents=True, exist_ok=True)
    (waitbus_dir / "plugins.allowlist.toml").write_text("[[broken\n", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(cli.app, ["allowlist", "repair"])
    assert result.exit_code == 2
    assert "could not parse" in result.output
