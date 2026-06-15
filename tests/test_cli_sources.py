"""CLI-layer tests for ``waitbus source`` (list / show / verify).

Covers every verb's exit-code contract from ``docs/EXIT_CODES.md`` plus
the JSON / text output shape and the registered-vs-installed view
distinction. Pairs ``tests/test_custom_sources.py`` (which exercises
the registry model) with the typer-layer wiring + sysexits.h exit-code
mapping.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from waitbus import cli
from waitbus.cli._exit_codes import EX_CONFIG, EX_DATAERR, EX_NOINPUT, EX_OK, EX_PROTOCOL
from waitbus.sources._attestation import (
    AttestationToolingMissingError,
    AttestationVerificationError,
    VerifiedPublisher,
)
from waitbus.sources._protocol import SOURCE_PLUGIN_API_VERSION, SourceSpec
from waitbus.sources._registry import register_plugin


def _ep(name: str, dist: Any | None = None) -> MagicMock:
    """MagicMock shaped like importlib.metadata.EntryPoint."""
    ep = MagicMock()
    ep.name = name
    ep.value = f"fake_pkg:{name}"
    ep.load.return_value = MagicMock()
    ep.dist = dist
    return ep


def _stub_plugin(name: str, event_types: tuple[str, ...] = ("e",)) -> Any:
    """Minimal SourcePlugin-shaped stub returning a valid SourceSpec."""

    class _Stub:
        def spec(self) -> SourceSpec:
            return SourceSpec(
                name=name,
                event_types=event_types,
                api_version=SOURCE_PLUGIN_API_VERSION,
            )

    return _Stub()


def _dist(name: str = "fake-plugin", version: str = "1.0") -> MagicMock:
    """MagicMock shaped like importlib.metadata.Distribution."""
    dist = MagicMock()
    dist.name = name
    dist.metadata = {"Version": version}
    return dist


# ---------------------------------------------------------------------------
# source list
# ---------------------------------------------------------------------------


def test_source_list_text_shows_every_builtin(isolated_waitbus_config: Path) -> None:
    """`waitbus source list` (text) prints all six built-in sources."""
    runner = CliRunner()
    result = runner.invoke(cli.app, ["source", "list"])
    assert result.exit_code == EX_OK, result.output
    for builtin in ("github", "alertmanager", "pytest", "docker", "fs", "agent"):
        assert builtin in result.output


def test_source_list_json_emits_ten_documented_keys(isolated_waitbus_config: Path) -> None:
    """`waitbus source list --json` emits the ten-column row shape per built-in."""
    runner = CliRunner()
    result = runner.invoke(cli.app, ["source", "list", "--json"])
    assert result.exit_code == EX_OK, result.output
    rows = json.loads(result.output)
    assert len(rows) == 6  # six built-ins
    expected_keys = {
        "name",
        "kind",
        "api-version",
        "event-types",
        "registered-by",
        "loaded-from",
        "signature-status",
        "publisher-identity",
        "allowlist-status",
        "last-used",
    }
    for row in rows:
        assert expected_keys.issubset(row.keys())


def test_source_list_includes_registered_plugin(isolated_waitbus_config: Path) -> None:
    """A plugin registered in-process shows up in the `source list` table."""
    plugin = _stub_plugin("ext_demo", event_types=("ext_event",))
    register_plugin(_ep("ext_demo"), plugin)

    runner = CliRunner()
    result = runner.invoke(cli.app, ["source", "list", "--json"])
    assert result.exit_code == EX_OK, result.output
    rows = json.loads(result.output)
    by_name = {r["name"]: r for r in rows}
    assert "ext_demo" in by_name
    assert by_name["ext_demo"]["kind"] == "plugin"


# ---------------------------------------------------------------------------
# source show
# ---------------------------------------------------------------------------


def test_source_show_builtin_includes_payload_schema_field(isolated_waitbus_config: Path) -> None:
    """`waitbus source show github` includes the payload-schema field."""
    runner = CliRunner()
    result = runner.invoke(cli.app, ["source", "show", "github", "--json"])
    assert result.exit_code == EX_OK, result.output
    payload = json.loads(result.output)
    assert payload["name"] == "github"
    assert "payload-schema" in payload
    assert "attestation-json" in payload


def test_source_show_unknown_source_exits_2(isolated_waitbus_config: Path) -> None:
    """`waitbus source show <missing>` exits 2 (current `_exit_with_error` default)."""
    runner = CliRunner()
    result = runner.invoke(cli.app, ["source", "show", "nonexistent"])
    assert result.exit_code == 2
    assert "unknown source" in result.output


# ---------------------------------------------------------------------------
# source verify -- sysexits.h exit-code coverage
# ---------------------------------------------------------------------------


def test_source_verify_builtin_exits_ex_ok(isolated_waitbus_config: Path) -> None:
    """Built-in source has no separate attestation -> EX_OK (0)."""
    runner = CliRunner()
    result = runner.invoke(cli.app, ["source", "verify", "github"])
    assert result.exit_code == EX_OK, result.output
    assert "n/a" in result.output


def test_source_verify_unknown_source_exits_ex_noinput(isolated_waitbus_config: Path) -> None:
    """Unknown source -> EX_NOINPUT (66)."""
    runner = CliRunner()
    result = runner.invoke(cli.app, ["source", "verify", "neverheard"])
    assert result.exit_code == EX_NOINPUT
    assert "unknown source" in result.output


def test_source_verify_plugin_no_installed_dist_exits_ex_noinput(isolated_waitbus_config: Path) -> None:
    """Plugin entry-point with no installed distribution -> EX_NOINPUT (66)."""
    plugin = _stub_plugin("orphan_plugin", event_types=("orphan_event",))
    register_plugin(_ep("orphan_plugin"), plugin)

    # entry_points_by_name returns ep with dist=None to simulate the
    # "name registered, plugin uninstalled" race.
    with patch(
        "waitbus.sources._registry.entry_points_by_name",
        return_value={"orphan_plugin": _ep("orphan_plugin", dist=None)},
    ):
        runner = CliRunner()
        result = runner.invoke(cli.app, ["source", "verify", "orphan_plugin"])
    assert result.exit_code == EX_NOINPUT
    assert "no installed distribution" in result.output


def test_source_verify_plugin_no_attestation_exits_ex_dataerr(isolated_waitbus_config: Path) -> None:
    """Plugin installed but no PEP 740 attestation -> EX_DATAERR (65)."""
    plugin = _stub_plugin("clean_plugin", event_types=("clean_event",))
    register_plugin(_ep("clean_plugin"), plugin)

    with (
        patch(
            "waitbus.sources._registry.entry_points_by_name",
            return_value={"clean_plugin": _ep("clean_plugin", dist=_dist())},
        ),
        patch("waitbus.sources._attestation.verify_distribution", return_value=None),
    ):
        runner = CliRunner()
        result = runner.invoke(cli.app, ["source", "verify", "clean_plugin"])
    assert result.exit_code == EX_DATAERR
    assert "no attestation found" in result.output


def test_source_verify_plugin_tooling_missing_exits_ex_config(isolated_waitbus_config: Path) -> None:
    """waitbus[plugin-verify] missing -> EX_CONFIG (78)."""
    plugin = _stub_plugin("no_tooling", event_types=("no_tooling_event",))
    register_plugin(_ep("no_tooling"), plugin)

    with (
        patch(
            "waitbus.sources._registry.entry_points_by_name",
            return_value={"no_tooling": _ep("no_tooling", dist=_dist())},
        ),
        patch(
            "waitbus.sources._attestation.verify_distribution",
            side_effect=AttestationToolingMissingError("install waitbus[plugin-verify]"),
        ),
    ):
        runner = CliRunner()
        result = runner.invoke(cli.app, ["source", "verify", "no_tooling"])
    assert result.exit_code == EX_CONFIG
    assert "plugin-verify" in result.output


def test_source_verify_plugin_signature_failure_exits_ex_protocol(isolated_waitbus_config: Path) -> None:
    """Sigstore signature mismatch -> EX_PROTOCOL (76)."""
    plugin = _stub_plugin("bad_sig", event_types=("bad_sig_event",))
    register_plugin(_ep("bad_sig"), plugin)

    with (
        patch(
            "waitbus.sources._registry.entry_points_by_name",
            return_value={"bad_sig": _ep("bad_sig", dist=_dist())},
        ),
        patch(
            "waitbus.sources._attestation.verify_distribution",
            side_effect=AttestationVerificationError("signature mismatch"),
        ),
    ):
        runner = CliRunner()
        result = runner.invoke(cli.app, ["source", "verify", "bad_sig"])
    assert result.exit_code == EX_PROTOCOL
    assert "verification failed" in result.output


def test_source_verify_plugin_success_exits_ex_ok(isolated_waitbus_config: Path) -> None:
    """Plugin verified successfully -> EX_OK (0) + identity printed."""
    plugin = _stub_plugin("happy_plugin", event_types=("happy_event",))
    register_plugin(_ep("happy_plugin"), plugin)

    verified = VerifiedPublisher(
        publisher_kind="GitHub",
        publisher_identity="org/happy @ wf.yml",
        predicate_type="https://docs.pypi.org/attestations/publish/v1",
    )
    with (
        patch(
            "waitbus.sources._registry.entry_points_by_name",
            return_value={"happy_plugin": _ep("happy_plugin", dist=_dist())},
        ),
        patch("waitbus.sources._attestation.verify_distribution", return_value=verified),
    ):
        runner = CliRunner()
        result = runner.invoke(cli.app, ["source", "verify", "happy_plugin"])
    assert result.exit_code == EX_OK, result.output
    assert "verified publisher" in result.output
    assert "org/happy @ wf.yml" in result.output


# ---------------------------------------------------------------------------
# source show / source list — coverage of _source_row + source_show text branches
# ---------------------------------------------------------------------------


def test_source_show_text_builtin_renders_labelled_pairs(isolated_waitbus_config: Path) -> None:
    """`waitbus source show github` (text mode) renders labelled key:value pairs.

    Exercises the text-output branch of `source_show`: the labelled-pairs
    loop, the missing-attestation fallback ("(none)"), and the dynamic
    field-width compute. Built-in source has no attestation field.
    """
    runner = CliRunner()
    result = runner.invoke(cli.app, ["source", "show", "github"])
    assert result.exit_code == EX_OK, result.output
    for label in ("name", "kind", "api-version", "event-types", "payload-schema"):
        assert label in result.output
    assert "attestation-json: (none)" in result.output


def test_source_show_text_plugin_with_attestation_renders_json_block(
    isolated_waitbus_config: Path,
) -> None:
    """`waitbus source show <plugin>` (text) embeds the attestation JSON block.

    Exercises `source_show`'s plugin-with-dist branch: `read_attestation_json`
    is called via the function-body-top import lifted in the previous
    commit; the text path emits the JSON after the "attestation-json:"
    label.
    """
    plugin = _stub_plugin("att_plugin", event_types=("att_event",))
    register_plugin(_ep("att_plugin"), plugin)

    fake_att_json = '{"version": 1, "attestations": []}'
    with (
        patch(
            "waitbus.sources._registry.entry_points_by_name",
            return_value={"att_plugin": _ep("att_plugin", dist=_dist())},
        ),
        patch(
            "waitbus.sources._attestation.read_attestation_json",
            return_value=fake_att_json,
        ),
    ):
        runner = CliRunner()
        result = runner.invoke(cli.app, ["source", "show", "att_plugin"])
    assert result.exit_code == EX_OK, result.output
    assert "attestation-json:" in result.output
    assert fake_att_json in result.output


def test_source_row_plugin_with_dist_and_verified_publisher(
    isolated_waitbus_config: Path,
) -> None:
    """`source list --json` for a plugin with installed dist + verified publisher.

    Exercises `_source_row` plugin branches: `ep.dist` not-None builds
    `registered-by = "<name> <version>"`; the `plugin_publishers()` hit
    sets `signature-status = "verified"` and `publisher-identity`.
    """
    plugin = _stub_plugin("verif_plugin", event_types=("verif_event",))
    register_plugin(_ep("verif_plugin"), plugin)

    verified = VerifiedPublisher(
        publisher_kind="GitHub",
        publisher_identity="org/verif @ release.yml",
        predicate_type="https://docs.pypi.org/attestations/publish/v1",
    )
    with (
        patch(
            "waitbus.sources._registry.entry_points_by_name",
            return_value={"verif_plugin": _ep("verif_plugin", dist=_dist("fake-plugin", "2.5"))},
        ),
        patch(
            "waitbus.sources._registry.plugin_publishers",
            return_value={"verif_plugin": verified},
        ),
    ):
        runner = CliRunner()
        result = runner.invoke(cli.app, ["source", "list", "--json"])
    assert result.exit_code == EX_OK, result.output
    rows = json.loads(result.output)
    by_name = {r["name"]: r for r in rows}
    assert "verif_plugin" in by_name
    row = by_name["verif_plugin"]
    assert row["registered-by"] == "fake-plugin 2.5"
    assert row["signature-status"] == "verified"
    assert "org/verif @ release.yml" in row["publisher-identity"]


def test_source_show_plugin_with_non_none_payload_schema(isolated_waitbus_config: Path) -> None:
    """Plugin whose SourceSpec carries a payload_schema renders the dotted name.

    Exercises `_source_row` payload-schema branch: when `spec.payload_schema`
    is not None, it renders as `<module>.<qualname>` instead of the
    `"None"` sentinel.
    """

    import msgspec

    class _Payload(msgspec.Struct):
        field: str = ""

    class _PluginWithSchema:
        def spec(self) -> SourceSpec:
            return SourceSpec(
                name="schemaful_plugin",
                event_types=("schema_event",),
                api_version=SOURCE_PLUGIN_API_VERSION,
                payload_schema=_Payload,
            )

    register_plugin(_ep("schemaful_plugin"), _PluginWithSchema())
    runner = CliRunner()
    result = runner.invoke(cli.app, ["source", "show", "schemaful_plugin", "--json"])
    assert result.exit_code == EX_OK, result.output
    payload = json.loads(result.output)
    assert payload["payload-schema"] != "None"
    assert "_Payload" in payload["payload-schema"]
