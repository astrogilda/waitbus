"""Tests for the custom-source plugin registry extension.

Covers the five invariants that the plugin registry must satisfy:
registration via entry-point, round-trip through SQLite, typo detection,
same-publisher upgrade (silent), and different-publisher shadow rejection.
Plus three supporting contract tests: built-in shadowing, API-version
mismatch, and wrong-signature detection.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from waitbus import _db
from waitbus import _emit as emit_mod
from waitbus._types import EventInsert
from waitbus.sources._attestation import (
    AttestationToolingMissingError,
    AttestationVerificationError,
    VerifiedPublisher,
)
from waitbus.sources._config import (
    AllowlistCorruptError,
    PluginPolicy,
    append_publisher_pin,
    load_allowlist,
)
from waitbus.sources._protocol import (
    SOURCE_PLUGIN_API_VERSION,
    PluginContractError,
    PluginDuplicateRegistrationError,
    PluginShadowError,
    PluginVersionMismatchError,
    SourceSpec,
)
from waitbus.sources._registry import (
    _clear_for_test_isolation,
    discover_plugins,
    discover_plugins_once,
    entry_points_by_name,
    event_types_supported,
    is_known_source,
    known_sources,
    plugin_publishers,
    register_plugin,
    register_source,
)

# ---------------------------------------------------------------------------
# Autouse fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clean_source_registry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Isolate every test from the real config directory and plugin state.

    Calls _clear_for_test_isolation() both before and after each test so plugin
    registrations from one test cannot leak into the next. Also redirects
    the XDG config home so the operator's real allowlist is never touched.
    """
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    _clear_for_test_isolation()
    yield
    _clear_for_test_isolation()


@pytest.fixture(autouse=True)
def _silence_doorbell(monkeypatch: pytest.MonkeyPatch) -> None:
    """Suppress broadcast doorbell ring so no daemon socket is required."""
    monkeypatch.setattr(_db._doorbell, "ring", lambda _path=None: None)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_stub_plugin(
    name: str = "test_source",
    event_types: tuple[str, ...] = ("test_event",),
    api_version: int = SOURCE_PLUGIN_API_VERSION,
) -> Any:
    """Return a minimal object satisfying the SourcePlugin protocol."""

    class _StubPlugin:
        def spec(self) -> SourceSpec:
            return SourceSpec(name=name, event_types=event_types, api_version=api_version)

    return _StubPlugin()


def _make_entry_point(name: str, plugin: Any, dist: Any = None) -> MagicMock:
    """Build a MagicMock shaped like an importlib.metadata.EntryPoint."""
    ep = MagicMock()
    ep.name = name
    ep.value = f"fake_pkg:{name}"
    ep.load.return_value = plugin
    ep.dist = dist
    return ep


def _insert_event(source: str, db_path: Path) -> EventInsert:
    ei = EventInsert(
        delivery_id=f"test:{source}:{time.time_ns()}",
        source=source,
        event_type="test_event",
        owner="acme",
        repo="widgets",
        received_at=time.time_ns(),
        payload_json="{}",
        ingest_method="manual",
    )
    emit_mod.emit(ei, db_path=db_path)
    return ei


# ---------------------------------------------------------------------------
# Core invariant tests
# ---------------------------------------------------------------------------


def test_plugin_registers_cleanly_via_entry_point(monkeypatch: pytest.MonkeyPatch) -> None:
    """A well-formed plugin entry-point registers its spec into the known-source set.

    discover_plugins() returns the spec, is_known_source() returns True, and
    the name appears in known_sources().
    """
    plugin = _make_stub_plugin("test_source")
    ep = _make_entry_point("test_source", plugin)

    from waitbus.sources import _registry
    from waitbus.sources._config import PluginPolicy

    monkeypatch.setattr(_registry, "entry_points", lambda group: [ep])

    specs = discover_plugins(policy=PluginPolicy(autoload=True))

    assert len(specs) == 1
    assert specs[0].name == "test_source"
    assert is_known_source("test_source")
    assert "test_source" in known_sources()


def test_round_trip_through_sqlite_preserves_source(tmp_path: Path) -> None:
    """Emitting an event with a plugin-registered source round-trips correctly.

    After register_plugin() adds the fake source, an EventInsert with that
    source can be constructed, persisted via emit(), and read back from the
    SQLite store with source intact.
    """
    db_path = tmp_path / "events.db"
    _db.ensure_schema(db_path)

    plugin = _make_stub_plugin("fake_source", event_types=("test_event",))
    ep = _make_entry_point("fake_source", plugin)
    register_plugin(ep, plugin)

    ei = _insert_event("fake_source", db_path)

    with _db.connect(db_path, readonly=True) as conn:
        row = conn.execute(
            "SELECT source FROM events WHERE delivery_id = ?",
            (ei.delivery_id,),
        ).fetchone()

    assert row is not None
    assert row[0] == "fake_source"


def test_typoed_source_still_raises_with_known_set(tmp_path: Path) -> None:
    """Constructing EventInsert with an unknown source raises ValueError.

    After registering one plugin source the error message enumerates
    both built-in and plugin-registered names so the operator can spot the typo.
    """
    plugin = _make_stub_plugin("real_source", event_types=("test_event",))
    ep = _make_entry_point("real_source", plugin)
    register_plugin(ep, plugin)

    with pytest.raises(ValueError, match="real_source"):
        EventInsert(
            delivery_id="d1",
            source="typo_source",
            event_type="test_event",
            owner="acme",
            repo="widgets",
            received_at=time.time_ns(),
            payload_json="{}",
            ingest_method="manual",
        )


def test_same_publisher_collision_silently_allowed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Re-registering a pinned source from the same publisher is silent (version upgrade).

    Pre-pin source name "upgrade_source" to publisher P. Then call
    register_plugin() with a plugin whose verify_distribution returns the same
    publisher P. No exception is raised and the source is registered.
    """
    append_publisher_pin(
        name="upgrade_source",
        publisher_kind="GitHub",
        publisher_identity="org/repo @ .github/workflows/release.yml",
    )

    same_publisher = VerifiedPublisher(
        publisher_kind="GitHub",
        publisher_identity="org/repo @ .github/workflows/release.yml",
        predicate_type="https://docs.pypi.org/attestations/publish/v1",
    )

    from waitbus.sources import _registry

    monkeypatch.setattr(_registry, "verify_distribution", lambda dist: same_publisher)

    plugin = _make_stub_plugin("upgrade_source", event_types=("upgrade_event",))
    # ep.dist must be non-None so _verify_publisher calls verify_distribution
    # rather than short-circuiting with None (the "no installed distribution" path).
    ep = _make_entry_point("upgrade_source", plugin, dist=MagicMock())
    # Must not raise; same publisher is the version-upgrade path.
    spec = register_plugin(ep, plugin)
    assert spec.name == "upgrade_source"
    assert is_known_source("upgrade_source")


def test_different_publisher_collision_raises_shadow_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A different publisher trying to register a pinned name raises PluginShadowError.

    Pre-pin "pinned_source" to publisher P. monkeypatch verify_distribution
    to return publisher P' (different identity). register_plugin() must raise
    PluginShadowError naming both publishers and pointing at allowlist verify.
    """
    append_publisher_pin(
        name="pinned_source",
        publisher_kind="GitHub",
        publisher_identity="original/repo @ .github/workflows/release.yml",
    )

    different_publisher = VerifiedPublisher(
        publisher_kind="GitHub",
        publisher_identity="attacker/repo @ .github/workflows/release.yml",
        predicate_type="https://docs.pypi.org/attestations/publish/v1",
    )

    from waitbus.sources import _registry

    monkeypatch.setattr(_registry, "verify_distribution", lambda dist: different_publisher)

    plugin = _make_stub_plugin("pinned_source", event_types=("pinned_event",))
    # ep.dist must be non-None so _verify_publisher reaches verify_distribution.
    ep = _make_entry_point("pinned_source", plugin, dist=MagicMock())

    with pytest.raises(PluginShadowError) as exc_info:
        register_plugin(ep, plugin)

    msg = str(exc_info.value)
    assert "original/repo" in msg
    assert "attacker/repo" in msg
    assert "allowlist" in msg.lower()


# ---------------------------------------------------------------------------
# Supporting contract tests
# ---------------------------------------------------------------------------


def test_plugin_shadowing_builtin_name_raises() -> None:
    """A plugin trying to register the name 'github' raises PluginShadowError.

    Built-in source names are reserved; a third-party plugin must not claim
    them, regardless of publisher verification status.
    """
    plugin = _make_stub_plugin("github")
    ep = _make_entry_point("github", plugin)

    with pytest.raises(PluginShadowError, match="github"):
        register_plugin(ep, plugin)


def test_plugin_api_version_mismatch_raises() -> None:
    """A plugin with api_version=99 raises PluginVersionMismatchError."""
    plugin = _make_stub_plugin("newish_source", api_version=99)
    ep = _make_entry_point("newish_source", plugin)

    with pytest.raises(PluginVersionMismatchError, match="99"):
        register_plugin(ep, plugin)


def test_plugin_with_wrong_signature_raises_contract_error() -> None:
    """A plugin whose spec() takes two required operator-provided args raises PluginContractError.

    The validator inspects the bound-method signature. A bound method
    spec(self, arg1, arg2) presents two required positionals to
    inspect.signature (self is stripped in the bound form), which exceeds
    the allowed maximum of zero operator-provided arguments.
    """

    class _BadPlugin:
        def spec(self, _arg1: str, _arg2: str) -> SourceSpec:  # pragma: no cover
            # Underscore-prefixed names: vulture-silent; ``inspect.signature``
            # still sees two required positional parameters (self stripped in
            # the bound form), which is the contract this test exercises.
            return SourceSpec(name="bad", event_types=("e",))

    ep = _make_entry_point("bad_source", _BadPlugin())

    with pytest.raises(PluginContractError):
        register_plugin(ep, _BadPlugin())


# ---------------------------------------------------------------------------
# Contract-validation tests (entry-point name consistency + event_type
# uniqueness + ExceptionGroup aggregation + structural-fallback removal)
# ---------------------------------------------------------------------------


def test_register_plugin_rejects_silent_name_swap() -> None:
    """Entry-point key must equal SourceSpec.name; mismatch raises PluginContractError.

    A plugin whose pyproject.toml declares ``[project.entry-points."waitbus.sources.v1"]
    pretty_circleci = "waitbus_circleci:plugin"`` but whose ``spec().name = "circleci"``
    would confuse operators reading pyproject vs. ``waitbus source list`` output.
    waitbus refuses the rename at registration.
    """
    plugin = _make_stub_plugin("real_name")
    ep = _make_entry_point("ep_key_disagrees", plugin)

    with pytest.raises(PluginContractError, match="entry-point key"):
        register_plugin(ep, plugin)


def test_register_plugin_rejects_event_type_collision_with_builtin() -> None:
    """A plugin claiming a built-in's event_type raises PluginContractError.

    The built-in ``github`` source claims event_types ``("workflow_run",
    "workflow_job")``. A plugin trying to register a source with
    event_type ``"workflow_run"`` would make ``waitbus emit
    --event-type workflow_run`` ambiguous and would silently let the
    broadcaster default-subscriber filter pass events from either
    source under the same label. waitbus refuses the collision.
    """
    plugin = _make_stub_plugin("rogue_source", event_types=("workflow_run",))
    ep = _make_entry_point("rogue_source", plugin)

    with pytest.raises(PluginContractError, match="already claimed by another source"):
        register_plugin(ep, plugin)


def test_register_plugin_rejects_event_type_collision_with_other_plugin() -> None:
    """Two plugins both claiming the same event_type: second one raises.

    First plugin registers cleanly; second plugin tries to claim the
    same event_type value (under a different source name) and is
    rejected with PluginContractError.
    """
    first_plugin = _make_stub_plugin("first_source", event_types=("shared_event",))
    first_ep = _make_entry_point("first_source", first_plugin)
    register_plugin(first_ep, first_plugin)

    second_plugin = _make_stub_plugin("second_source", event_types=("shared_event",))
    second_ep = _make_entry_point("second_source", second_plugin)

    with pytest.raises(PluginContractError, match="already claimed by another source"):
        register_plugin(second_ep, second_plugin)


def test_discover_plugins_aggregates_failures_via_exception_group(monkeypatch: pytest.MonkeyPatch) -> None:
    """Multiple policy failures surface as a single ExceptionGroup.

    Three plugins fail at discovery: one with a bad spec signature
    (PluginContractError), one shadowing a built-in name
    (PluginShadowError), one with mismatched api_version
    (PluginVersionMismatchError). discover_plugins must raise an
    ExceptionGroup naming all three rather than only the first.
    """
    from waitbus.sources import _registry
    from waitbus.sources._config import PluginPolicy

    # Plugin 1: bad signature -> PluginContractError
    class _BadSigPlugin:
        def spec(self, x: str, y: str) -> SourceSpec:  # pragma: no cover
            return SourceSpec(name="ungood", event_types=("e",))

    # Plugin 2: shadows built-in 'github' -> PluginShadowError
    shadow_plugin = _make_stub_plugin("github")

    # Plugin 3: api_version mismatch -> PluginVersionMismatchError
    version_plugin = _make_stub_plugin("future_source", api_version=99)

    eps = [
        _make_entry_point("ungood", _BadSigPlugin()),
        _make_entry_point("github", shadow_plugin),
        _make_entry_point("future_source", version_plugin),
    ]
    monkeypatch.setattr(_registry, "entry_points", lambda group: eps)

    with pytest.raises(ExceptionGroup) as exc_info:
        _registry.discover_plugins(policy=PluginPolicy(autoload=True))

    group = exc_info.value
    assert len(group.exceptions) == 3
    types = {type(e).__name__ for e in group.exceptions}
    assert types == {
        "PluginContractError",
        "PluginShadowError",
        "PluginVersionMismatchError",
    }


def test_verify_failure_logs_warning_does_not_kill_daemon(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A PEP 740 verify failure surfaces as None + WARN, never propagates.

    The previous _verify_publisher caught only AttestationToolingMissingError;
    a real AttestationVerificationError (sigstore signature mismatch, TUF
    transient error, malformed envelope) would propagate up through
    register_plugin and kill daemon startup. That is a denial-of-service
    path: one broken plugin among many would take the whole daemon down.

    With the failure now caught at the registry seam, the daemon logs a
    WARNING and registers the plugin as unverified; the operator's
    allowlist policy then decides whether to load it.
    """
    from waitbus.sources import _registry
    from waitbus.sources._attestation import AttestationVerificationError

    def _explode(_dist: object) -> None:
        raise AttestationVerificationError("signature mismatch (simulated)")

    monkeypatch.setattr(_registry, "verify_distribution", _explode)

    plugin = _make_stub_plugin("doss_resistant", event_types=("doss_event",))
    ep = _make_entry_point("doss_resistant", plugin, dist=MagicMock())

    with caplog.at_level("WARNING", logger="waitbus.sources.registry"):
        spec = register_plugin(ep, plugin)

    assert spec.name == "doss_resistant"
    assert is_known_source("doss_resistant")
    assert any("PEP 740 verification failed" in record.message for record in caplog.records)


def test_register_plugin_rejects_non_sourcespec_object() -> None:
    """A duck-typed object passing the old structural fallback now raises.

    The previous _validate_spec accepted any object with .name and
    .event_types attributes. The fallback was removed in an earlier
    cleanup because it widened the attack surface and the
    cross-waitbus-minor compatibility it nominally provided contradicts
    the actual versioning mechanism (entry-point group name carries
    the major version).
    """
    from types import SimpleNamespace

    duck_typed = SimpleNamespace(name="duck", event_types=("quack",), api_version=1, payload_schema=None)

    class _DuckPlugin:
        def spec(self) -> object:
            return duck_typed

    plugin = _DuckPlugin()
    ep = _make_entry_point("duck", plugin)

    with pytest.raises(PluginContractError, match="not a SourceSpec"):
        register_plugin(ep, plugin)


# ---------------------------------------------------------------------------
# In-process registration seam (register_source)
# ---------------------------------------------------------------------------


def test_register_source_adds_in_process_source() -> None:
    """register_source() adds a first-party spec to the known-source set in-process.

    No entry-point and no attestation: the returned spec is the input, and the
    name is immediately visible via is_known_source() / known_sources().
    """
    spec = SourceSpec(name="linear", event_types=("issue_opened", "issue_closed"))
    returned = register_source(spec)

    assert returned is spec
    assert is_known_source("linear")
    assert "linear" in known_sources()
    assert known_sources()["linear"].event_types == ("issue_opened", "issue_closed")


def test_register_source_event_round_trips_through_sqlite(tmp_path: Path) -> None:
    """An event whose source was registered in-process persists and reads back.

    EventInsert validation accepts the in-process source (it flows through the
    same is_known_source() gate the entry-point path feeds), and the row
    round-trips through SQLite with source intact.
    """
    db_path = tmp_path / "events.db"
    _db.ensure_schema(db_path)
    register_source(SourceSpec(name="linear", event_types=("test_event",)))

    ei = _insert_event("linear", db_path)

    with _db.connect(db_path, readonly=True) as conn:
        row = conn.execute(
            "SELECT source FROM events WHERE delivery_id = ?",
            (ei.delivery_id,),
        ).fetchone()

    assert row is not None
    assert row[0] == "linear"


def test_register_source_records_no_publisher() -> None:
    """In-process registration carries no attested publisher (None), distinct from
    a PEP 740-verified entry-point plugin."""
    register_source(SourceSpec(name="linear", event_types=("issue_opened",)))
    assert plugin_publishers()["linear"] is None


def test_register_source_rejects_non_sourcespec() -> None:
    """A non-SourceSpec argument is rejected with PluginContractError."""
    with pytest.raises(PluginContractError, match="expects a SourceSpec"):
        register_source("not a spec")  # type: ignore[arg-type]


def test_register_source_rejects_builtin_shadow() -> None:
    """An in-process source may not shadow a built-in source name."""
    with pytest.raises(PluginShadowError, match="shadow a built-in"):
        register_source(SourceSpec(name="github", event_types=("issue_opened",)))


def test_register_source_rejects_duplicate() -> None:
    """Registering the same source name twice in one process is rejected."""
    register_source(SourceSpec(name="linear", event_types=("issue_opened",)))
    with pytest.raises(PluginDuplicateRegistrationError, match="already registered"):
        register_source(SourceSpec(name="linear", event_types=("issue_updated",)))


def test_register_source_rejects_event_type_collision() -> None:
    """An in-process source may not claim an event_type owned by another source.

    The shared _event_type_collisions rule rejects reuse of a built-in
    event_type (here github's 'workflow_run').
    """
    with pytest.raises(PluginContractError, match="already claimed"):
        register_source(SourceSpec(name="linear", event_types=("workflow_run",)))


def test_register_source_rejects_api_version_mismatch() -> None:
    """A spec built against a different api_version is rejected."""
    with pytest.raises(PluginVersionMismatchError, match="api_version"):
        register_source(
            SourceSpec(name="linear", event_types=("issue_opened",), api_version=SOURCE_PLUGIN_API_VERSION + 1)
        )


def test_register_source_coexists_with_entry_point_plugin(monkeypatch: pytest.MonkeyPatch) -> None:
    """In-process and entry-point registration coexist (the pluggy pattern).

    A plugin discovered via the entry-point path and a source registered
    in-process are both visible in known_sources(), with no interference.
    """
    from waitbus.sources import _registry
    from waitbus.sources._config import PluginPolicy

    plugin = _make_stub_plugin("circleci", event_types=("circleci_job",))
    ep = _make_entry_point("circleci", plugin)
    monkeypatch.setattr(_registry, "entry_points", lambda group: [ep])
    discover_plugins(policy=PluginPolicy(autoload=True))

    register_source(SourceSpec(name="linear", event_types=("issue_opened",)))

    assert is_known_source("circleci")
    assert is_known_source("linear")
    assert {"circleci", "linear"} <= set(known_sources())


# ---------------------------------------------------------------------------
# Branch coverage: accessors, callable validation, attestation, TOFU,
# policy filtering, and discovery error handling
# ---------------------------------------------------------------------------


def test_event_types_supported_unions_known_sources() -> None:
    """event_types_supported() unions every known source's event_types and widens
    when an in-process source registers."""
    before = event_types_supported()
    assert "workflow_run" in before  # built-in github
    register_source(SourceSpec(name="linear", event_types=("issue_opened",)))
    after = event_types_supported()
    assert "issue_opened" in after
    assert before < after  # proper superset after registration


def test_register_plugin_rejects_object_without_spec_method() -> None:
    """A plugin object missing a spec() method is rejected before registration."""
    ep = _make_entry_point("nospec", object())
    with pytest.raises(PluginContractError, match="missing required"):
        register_plugin(ep, object())


def test_register_plugin_rejects_uninspectable_spec_signature(monkeypatch: pytest.MonkeyPatch) -> None:
    """A spec() whose signature cannot be introspected is rejected cleanly."""

    def _raise(*_a: object, **_k: object) -> object:
        raise ValueError("no signature for builtin")

    # Patch via the string target so the test does not reach through _registry's
    # imported `inspect` attribute (which is not an explicit re-export and trips
    # mypy --strict's no-implicit-reexport check).
    monkeypatch.setattr("waitbus.sources._registry.inspect.signature", _raise)
    plugin = _make_stub_plugin("uninspectable")
    ep = _make_entry_point("uninspectable", plugin)
    with pytest.raises(PluginContractError, match="uninspectable signature"):
        register_plugin(ep, plugin)


def test_register_plugin_rejects_duplicate_name() -> None:
    """Registering the same plugin name twice raises PluginDuplicateRegistrationError."""
    plugin = _make_stub_plugin("dupe", event_types=("dupe_event",))
    ep = _make_entry_point("dupe", plugin)
    register_plugin(ep, plugin)
    with pytest.raises(PluginDuplicateRegistrationError, match="already registered"):
        register_plugin(ep, plugin)


def test_verify_publisher_tolerates_verification_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """A failed PEP 740 verification logs and registers the source unverified."""
    from waitbus.sources import _registry

    def _raise(_dist: object) -> object:
        raise AttestationVerificationError("sigstore mismatch")

    monkeypatch.setattr(_registry, "verify_distribution", _raise)
    plugin = _make_stub_plugin("att_fail", event_types=("att_fail_event",))
    ep = _make_entry_point("att_fail", plugin, dist=MagicMock())  # dist present -> verify attempted
    spec = register_plugin(ep, plugin)
    assert spec.name == "att_fail"
    assert plugin_publishers()["att_fail"] is None  # treated as unverified


def test_verify_publisher_tolerates_tooling_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing attestation tooling logs and registers the source unverified."""
    from waitbus.sources import _registry

    def _raise(_dist: object) -> object:
        raise AttestationToolingMissingError("waitbus[plugin-verify] not installed")

    monkeypatch.setattr(_registry, "verify_distribution", _raise)
    plugin = _make_stub_plugin("no_tooling", event_types=("no_tooling_event",))
    ep = _make_entry_point("no_tooling", plugin, dist=MagicMock())
    register_plugin(ep, plugin)
    assert plugin_publishers()["no_tooling"] is None


def test_enforce_tofu_first_install_pins_verified_publisher(monkeypatch: pytest.MonkeyPatch) -> None:
    """First install of a verified plugin records a TOFU pin in the allowlist."""
    from waitbus.sources import _registry

    pub = VerifiedPublisher(
        publisher_kind="GitHub",
        publisher_identity="acme/plugin@refs/tags/v1",
        predicate_type="https://docs.pypi.org/attestations/publish/v1",
    )
    monkeypatch.setattr(_registry, "verify_distribution", lambda _d: pub)
    plugin = _make_stub_plugin("pinned", event_types=("pinned_event",))
    ep = _make_entry_point("pinned", plugin, dist=MagicMock())
    register_plugin(ep, plugin)
    assert plugin_publishers()["pinned"] == pub
    assert load_allowlist().for_source("pinned") is not None  # pin persisted


def test_enforce_tofu_corrupt_allowlist_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    """A corrupt allowlist degrades to an un-pinned view rather than failing."""
    from waitbus.sources import _registry

    def _raise() -> object:
        raise AllowlistCorruptError("bad toml")

    monkeypatch.setattr(_registry, "load_allowlist", _raise)
    plugin = _make_stub_plugin("corrupt_ok", event_types=("corrupt_ok_event",))
    ep = _make_entry_point("corrupt_ok", plugin)  # dist=None -> verified None
    spec = register_plugin(ep, plugin)  # corrupt allowlist + unverified -> registers, no pin
    assert spec.name == "corrupt_ok"


def test_enforce_tofu_pinned_then_unverified_is_shadow() -> None:
    """A name pinned to a publisher, re-registered with no attestation, is rejected."""
    append_publisher_pin(name="shadowy", publisher_kind="GitHub", publisher_identity="acme/x@v1")
    plugin = _make_stub_plugin("shadowy", event_types=("shadowy_event",))
    ep = _make_entry_point("shadowy", plugin)  # dist=None -> verified None
    with pytest.raises(PluginShadowError, match="no verifiable PEP 740 attestation"):
        register_plugin(ep, plugin)


def test_discover_plugins_tolerates_load_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """A plugin that fails to import is logged and skipped, not fatal."""
    from waitbus.sources import _registry

    ep = MagicMock()
    ep.name = "broken_import"
    ep.value = "broken_pkg:broken_import"
    ep.load.side_effect = ImportError("boom")
    monkeypatch.setattr(_registry, "entry_points", lambda group: [ep])
    assert discover_plugins(policy=PluginPolicy(autoload=True)) == []


def test_discover_plugins_tolerates_spec_raising(monkeypatch: pytest.MonkeyPatch) -> None:
    """A plugin whose spec() raises an untyped error is logged and skipped."""
    from waitbus.sources import _registry

    class _Boom:
        def spec(self) -> SourceSpec:
            raise RuntimeError("spec exploded")

    ep = _make_entry_point("boom", _Boom())
    monkeypatch.setattr(_registry, "entry_points", lambda group: [ep])
    assert discover_plugins(policy=PluginPolicy(autoload=True)) == []


def test_select_entry_points_autoload_false_empty_allow(monkeypatch: pytest.MonkeyPatch) -> None:
    """autoload=False with an empty allow-list loads nothing."""
    from waitbus.sources import _registry

    ep = _make_entry_point("p", _make_stub_plugin("p", event_types=("p_event",)))
    monkeypatch.setattr(_registry, "entry_points", lambda group: [ep])
    assert discover_plugins(policy=PluginPolicy(autoload=False, allow=())) == []


def test_select_entry_points_autoload_false_with_allow(monkeypatch: pytest.MonkeyPatch) -> None:
    """autoload=False loads only names in the allow-list."""
    from waitbus.sources import _registry

    ep = _make_entry_point("allowed", _make_stub_plugin("allowed", event_types=("allowed_event",)))
    monkeypatch.setattr(_registry, "entry_points", lambda group: [ep])
    specs = discover_plugins(policy=PluginPolicy(autoload=False, allow=("allowed",)))
    assert [s.name for s in specs] == ["allowed"]


def test_select_entry_points_deny_blocks(monkeypatch: pytest.MonkeyPatch) -> None:
    """A denied name is never loaded even under autoload=True."""
    from waitbus.sources import _registry

    ep = _make_entry_point("denied", _make_stub_plugin("denied", event_types=("denied_event",)))
    monkeypatch.setattr(_registry, "entry_points", lambda group: [ep])
    assert discover_plugins(policy=PluginPolicy(autoload=True, deny=("denied",))) == []


def test_discover_plugins_once_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    """The first discover_plugins_once registers; subsequent calls no-op."""
    from waitbus.sources import _registry

    ep = _make_entry_point("once", _make_stub_plugin("once", event_types=("once_event",)))
    monkeypatch.setattr(_registry, "entry_points", lambda group: [ep])
    first = discover_plugins_once(policy=PluginPolicy(autoload=True))
    second = discover_plugins_once(policy=PluginPolicy(autoload=True))
    assert [s.name for s in first] == ["once"]
    assert second == []


def test_discover_plugins_once_reraises_and_stays_marked(monkeypatch: pytest.MonkeyPatch) -> None:
    """A policy failure propagates and leaves discovery marked done (no retry)."""
    from waitbus.sources import _registry

    # A plugin shadowing a built-in raises inside discover_plugins -> ExceptionGroup.
    ep = _make_entry_point("github", _make_stub_plugin("github", event_types=("github_clash",)))
    monkeypatch.setattr(_registry, "entry_points", lambda group: [ep])
    with pytest.raises(ExceptionGroup):
        discover_plugins_once(policy=PluginPolicy(autoload=True))
    # The flag stays set: a second call no-ops rather than retrying.
    assert discover_plugins_once(policy=PluginPolicy(autoload=True)) == []


def test_entry_points_by_name_maps_group_by_name(monkeypatch: pytest.MonkeyPatch) -> None:
    """entry_points_by_name() returns the waitbus.sources.v1 group keyed by EP name."""
    from waitbus.sources import _registry

    ep = _make_entry_point("mapped", _make_stub_plugin("mapped"))
    monkeypatch.setattr(_registry, "entry_points", lambda group: [ep])
    mapping = entry_points_by_name()
    assert set(mapping) == {"mapped"}
    assert mapping["mapped"] is ep
