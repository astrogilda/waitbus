"""Built-in source registry: the single source of truth for waitbus's
first-party event-source taxonomy.

This module owns the in-process mapping from canonical source names
(``"github"``, ``"alertmanager"``, ``"pytest"``, ``"docker"``,
``"fs"``, ``"agent"``) to typed :class:`SourceSpec` records. The dict is the
authoritative source-of-truth for:

* The :class:`~waitbus._types.EventInsert` /
  :class:`~waitbus._types.Event` ``__post_init__`` validators
  (called automatically by msgspec on construction AND on decode /
  convert, per ``msgspec/docs/structs.rst:164-171``).
* The CLI's ``--source`` parser
  (:func:`waitbus._emit._parse_source`).
* The CloudEvents URN projection (``urn:waitbus:source:<name>``).
* The set the broadcaster's default subscriber-filter accepts, via
  :func:`event_types_supported`.

There is no parallel constants-module of source names: producers
reference the canonical source name as a bare string literal
(``source="github"``). The dual citizenship of an enum that is also a
constants module was rejected because this registry is the single
canonical surface and producer-site typos are caught loudly at runtime
by
``__post_init__`` (which works for plugin sources too -- a built-in
constants module would cover built-ins only).

Plugin-registered sources discovered via
``importlib.metadata.entry_points(group="waitbus.sources.v1")`` are
added by :func:`discover_plugins`. The function-shaped accessors
(:func:`known_sources`, :func:`event_types_supported`,
:func:`is_known_source`) pick up plugin additions automatically;
consumers that captured a snapshot via ``_BUILTIN_SOURCES``
directly would miss plugin additions — so the public surface
does NOT export the underlying dict.

The discovery flow:

1. :func:`discover_plugins` is called once per daemon process,
   typically at startup. It loads operator policy from
   :mod:`waitbus.sources._config`.
2. If :data:`PluginPolicy.autoload` is True, every entry-point in
   the group is loaded; if False, only names in ``allow`` are
   considered. ``deny`` is always applied.
3. For each candidate, the plugin's :class:`~._protocol.SourcePlugin`
   instance is loaded via :meth:`importlib.metadata.EntryPoint.load`,
   its :meth:`~._protocol.SourcePlugin.spec` method called, and the
   returned :class:`~._protocol.SourceSpec` validated.
4. The plugin's wheel is verified against any PEP 740 attestation
   present (best-effort; missing attestation is allowed when the
   ``waitbus[plugin-verify]`` extra is not installed). The verified
   publisher identity is compared against
   ``$XDG_CONFIG_HOME/waitbus/plugins.allowlist.toml`` under TOFU
   semantics: first-seen pins; mismatches raise
   :class:`~._protocol.PluginShadowError`.
5. On success the spec is added to ``_PLUGIN_SOURCES``; the
   :func:`known_sources` accessor then includes it.
"""

from __future__ import annotations

import inspect
import logging
import threading
from collections import ChainMap
from collections.abc import Mapping
from importlib.metadata import EntryPoint, entry_points
from itertools import chain
from types import MappingProxyType
from typing import Any, Final

from ._attestation import (
    AttestationToolingMissingError,
    AttestationVerificationError,
    VerifiedPublisher,
    verify_distribution,
)
from ._config import (
    Allowlist,
    AllowlistCorruptError,
    PluginPolicy,
    append_publisher_pin,
    load_allowlist,
    load_plugin_policy,
)
from ._protocol import (
    SOURCE_PLUGIN_API_VERSION,
    PluginContractError,
    PluginDuplicateRegistrationError,
    PluginShadowError,
    PluginVersionMismatchError,
    SourceSpec,
)

_log = logging.getLogger("waitbus.sources.registry")

#: The entry-point group waitbus enumerates for plugin discovery.
#: The ``.v1`` suffix encodes the contract version in the group name
#: itself: a future breaking change ships as ``waitbus.sources.v2`` and
#: waitbus enumerates both groups during the transition window.
#:
#: TODO(v2-dual-enum): the dual-walk of ``waitbus.sources.v1`` +
#: ``waitbus.sources.v2`` is a transition-window promise but is NOT YET
#: implemented -- the discovery code below reads only
#: ``ENTRY_POINT_GROUP``. When a v2 contract
#: ships, replace this constant with a tuple of (group_name,
#: api_version) and update ``discover_plugins`` /
#: ``entry_points_by_name`` to walk both groups, merging the v1 and
#: v2 results with v2 taking precedence on name collision.
ENTRY_POINT_GROUP: Final[str] = "waitbus.sources.v1"

# Module-private. Consumers MUST use the function accessors below so
# the union with plugin-discovered sources is transparent at every
# lookup. Held as a tuple of (name, SourceSpec) pairs and materialised
# into a mapping at module load. Keeps the literal declaration
# grep-friendly.
_BUILTIN_SOURCES_RAW: Final[tuple[tuple[str, SourceSpec], ...]] = (
    (
        "github",
        SourceSpec(
            name="github",
            event_types=("workflow_run", "workflow_job"),
            payload_schema=None,
            api_version=SOURCE_PLUGIN_API_VERSION,
        ),
    ),
    (
        "alertmanager",
        SourceSpec(
            name="alertmanager",
            event_types=("prometheus_alert", "prometheus_watchdog"),
            payload_schema=None,
            api_version=SOURCE_PLUGIN_API_VERSION,
        ),
    ),
    (
        "pytest",
        SourceSpec(
            name="pytest",
            event_types=("pytest_session",),
            payload_schema=None,
            api_version=SOURCE_PLUGIN_API_VERSION,
        ),
    ),
    (
        "docker",
        SourceSpec(
            name="docker",
            event_types=("docker_container",),
            payload_schema=None,
            api_version=SOURCE_PLUGIN_API_VERSION,
        ),
    ),
    (
        "fs",
        SourceSpec(
            name="fs",
            event_types=("fs_change",),
            payload_schema=None,
            api_version=SOURCE_PLUGIN_API_VERSION,
        ),
    ),
    (
        # In-process agent emission: the ingress class for events an agent puts
        # on the bus itself (not a watcher of an external system). Owns the
        # canonical agent vocabulary -- addressed messages (`agent_message`,
        # carrying the msg_* addressing facet) and coordination broadcasts
        # (`agent_claim`, `agent_task_failed`). A first-class built-in (not a
        # demo-scoped registration): adding a SourceSpec to this taxonomy is a
        # validation entry only and starts NO daemon-resident watcher, so it
        # leaves the daemon footprint and the soak baseline untouched.
        "agent",
        SourceSpec(
            name="agent",
            event_types=("agent_message", "agent_claim", "agent_task_failed"),
            payload_schema=None,
            api_version=SOURCE_PLUGIN_API_VERSION,
        ),
    ),
)

_BUILTIN_SOURCES: Final[dict[str, SourceSpec]] = dict(_BUILTIN_SOURCES_RAW)

#: Plugin-registered source specs, populated by :func:`discover_plugins`
#: at daemon startup. Module-global because it is process-singleton
#: shared state across the registry's accessors. Module-private to
#: discourage direct mutation -- :func:`register_plugin` is the seam.
_PLUGIN_SOURCES: dict[str, SourceSpec] = {}

#: Plugin-registered publisher identities for the runtime registration
#: log (separate from the persisted allowlist, which lives on disk in
#: ``plugins.allowlist.toml`` and is read by :func:`load_allowlist`).
#: Indexed by source name. Used by :func:`waitbus sources list` to
#: surface "where did this plugin come from in this process?".
_PLUGIN_PUBLISHERS: dict[str, VerifiedPublisher | None] = {}

#: Reentrant lock guarding the check-then-act on ``_PLUGIN_SOURCES``
#: and ``_PLUGIN_PUBLISHERS``. ``RLock`` rather than plain ``Lock``
#: because :func:`discover_plugins_once` holds the lock while calling
#: :func:`register_plugin`, which itself acquires the lock to make its
#: own check-then-act atomic; a non-reentrant lock would self-deadlock.
#: Same lock guards :data:`_DISCOVERED` so the idempotency check is
#: atomic with the registration loop.
_REGISTRY_LOCK: Final[threading.RLock] = threading.RLock()

#: Process-singleton flag: True after :func:`discover_plugins_once` has
#: completed (regardless of how many plugins it found / rejected).
#: A second call returns immediately. :func:`_clear_for_test_isolation`
#: resets it so tests/benchmarks can re-discover with a different policy
#: or a different monkeypatched ``entry_points`` view.
_DISCOVERED: bool = False


def known_sources() -> Mapping[str, SourceSpec]:
    """Return the current known-source set (built-ins plus plugin-registered).

    Returns a live, read-only :class:`collections.ChainMap` view over
    ``(_BUILTIN_SOURCES, _PLUGIN_SOURCES)``. Subsequent plugin
    registrations performed via :func:`register_plugin` are visible
    through any previously-returned view because ``ChainMap`` holds
    its component dicts by reference, not by copy -- so a caller that
    captures the view once at startup and queries it repeatedly will
    see new plugins as they register. Built-ins come first so any
    equal-name lookup resolves against the built-in spec; built-in
    shadowing is already rejected at :func:`register_plugin` time
    via :class:`~._protocol.PluginShadowError`, so equal-name
    conflicts never reach this lookup path.
    """
    return ChainMap(_BUILTIN_SOURCES, _PLUGIN_SOURCES)


def is_known_source(name: str) -> bool:
    """Return True iff ``name`` is a registered source name.

    Called by the :class:`~waitbus._types.EventInsert` and
    :class:`~waitbus._types.Event` ``__post_init__`` validators
    to fail-fast on typo'd / unknown source values without coupling
    ``_types.py`` to the full registry surface. O(1) hash lookup.
    """
    return name in known_sources()


def event_types_supported() -> frozenset[str]:
    """Return the union of ``event_type`` values across all known sources.

    Returns a frozen set of every ``event_type`` value any known source
    is allowed to emit. The function form (not a module-level
    ``Final[frozenset[str]]`` constant) is deliberate: the set widens
    automatically when ``discover_plugins_once`` registers a plugin
    source, without invalidating consumers that captured an older
    snapshot.

    The frozenset is recomputed on each call. Cost is roughly
    O(n_sources + n_event_types) — under a microsecond for the
    current taxonomy. If a future profiler shows this on a hot path,
    add a process-lifetime ``functools.cache`` wrapper that is
    invalidated by the plugin-registration entrypoint.
    """
    return frozenset(chain.from_iterable(spec.event_types for spec in known_sources().values()))


def _validate_spec(spec: object, ep_value: str) -> SourceSpec:
    """Require ``spec`` to be a real :class:`SourceSpec` instance.

    The previous version of this function carried a duck-typed
    structural fallback that accepted any object exposing ``.name`` and
    ``.event_types`` -- justified as cross-waitbus-minor-version
    compatibility for a plugin that imported a different waitbus's
    ``SourceSpec``. That justification contradicts the versioning
    mechanism (the entry-point group name carries the major version as
    ``waitbus.sources.v1`` and a future breaking change ships as ``.v2``;
    cross-minor compatibility is not a design goal). The fallback also
    widened the attack surface -- a malicious plugin could return a
    duck-typed object with a property-shaped ``.name`` and slip past
    the post-init validators. Removed in an earlier cleanup.

    Raises :class:`PluginContractError` if ``spec`` is not a
    :class:`SourceSpec`.
    """
    if not isinstance(spec, SourceSpec):
        raise PluginContractError(
            f"plugin entry-point {ep_value!r} returned object of type "
            f"{type(spec).__name__!r} which is not a SourceSpec instance. "
            f"Plugin authors must construct and return a SourceSpec; "
            f"duck-typed objects are no longer accepted."
        )
    return spec


def _validate_plugin_callable(plugin: object, ep_value: str) -> None:
    """Validate that ``plugin`` looks like a :class:`SourcePlugin`.

    Replaces what ``@runtime_checkable Protocol`` + ``isinstance`` would
    have done -- but uses :func:`inspect.signature` to check that
    ``spec`` is a zero-arg callable. Raises :class:`PluginContractError`
    on mismatch.

    The previous version of this function also required ``fetch`` to be
    callable, but the registry never invoked ``fetch`` -- the method was
    Protocol documentation, not contract. waitbus producers emit via the
    public ``emit()`` API. ``fetch`` was dropped from the SourcePlugin
    Protocol in an earlier cleanup; the corresponding check is
    gone from this function.
    """
    spec_method = getattr(plugin, "spec", None)
    if spec_method is None or not callable(spec_method):
        raise PluginContractError(f"plugin {ep_value!r} is missing required ``spec()`` method")
    try:
        sig = inspect.signature(spec_method)
    except (TypeError, ValueError) as exc:
        raise PluginContractError(f"plugin {ep_value!r} ``spec`` method has uninspectable signature: {exc}") from exc
    # ``spec`` takes no operator-provided arguments. Bound-method form
    # has zero parameters; unbound function form has one (``self``).
    positional = [
        p
        for p in sig.parameters.values()
        if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        and p.default is inspect.Parameter.empty
    ]
    # Allow exactly 0 or 1 required positional (``self`` for unbound).
    if len(positional) > 1:
        raise PluginContractError(
            f"plugin {ep_value!r} ``spec`` method takes more than one required positional "
            f"argument ({len(positional)}); the contract is zero-arg"
        )


def _verify_publisher(ep: EntryPoint, source_name: str) -> VerifiedPublisher | None:
    """Verify the plugin's PEP 740 attestation, return the publisher.

    Returns the :class:`VerifiedPublisher` on success, or ``None`` if
    the plugin wheel carries no attestation OR if verification failed
    transiently. Verification failures (Sigstore signature mismatch,
    TUF transient errors, malformed provenance envelope) are logged
    as WARNING and returned as ``None`` rather than propagating --
    the TOFU policy then treats the plugin as unverified (the
    operator's allowlist still decides whether to load it) instead of
    killing daemon startup. This avoids a denial-of-service path where
    a single broken plugin among many can take the whole daemon down.

    Tolerates :class:`AttestationToolingMissingError` (the
    ``waitbus[plugin-verify]`` extra is not installed): logs a warning
    and returns ``None``. Operators who skip the extra take on the
    responsibility of pre-vetting plugin wheels via OS-level
    provenance.
    """
    dist = ep.dist
    if dist is None:
        _log.warning("plugin entry-point %s has no installed distribution; skipping attestation", ep.value)
        return None
    try:
        return verify_distribution(dist)
    except AttestationVerificationError as exc:
        _log.warning(
            "PEP 740 verification failed for source %r: %s. Plugin will be "
            "treated as unverified; the allowlist policy decides whether to "
            "load it. Investigate the plugin wheel + provenance file before "
            "trusting subsequent installs.",
            source_name,
            exc,
        )
        return None
    except AttestationToolingMissingError:
        _log.warning(
            "waitbus[plugin-verify] not installed; source %r registered without "
            "PEP 740 attestation verification. Install the extra to enable "
            "publisher-bound TOFU enforcement.",
            source_name,
        )
        return None


def _enforce_tofu(source_name: str, verified: VerifiedPublisher | None) -> None:
    """Apply the publisher-bound TOFU policy for ``source_name``.

    * First-install (no pin recorded): if a verified publisher exists,
      pin it. If not, the source is registered without a pin (the
      operator's responsibility to vet).
    * Subsequent install with matching publisher: silent allow.
    * Subsequent install with mismatching publisher: raise
      :class:`PluginShadowError`. This is the typosquat / vendor-
      shadow defence.

    Corrupt allowlist handling: when ``load_allowlist`` raises
    :class:`AllowlistCorruptError`, the function logs a structured
    WARNING with operator-action guidance and falls back to a
    clean-slate TOFU view (treats the source as un-pinned). This is
    the documented HID-B5 trade-off: a hard daemon-fail on corrupt
    allowlist would be a denial-of-service vector (one bad byte
    bricks the whole event bus); a graceful fallback opens a
    bounded TOFU-bypass window between corruption and operator
    repair, mitigated by the WARN-level log being immediately
    visible to the operator. Recover via ``waitbus allowlist repair``.
    """
    try:
        allowlist = load_allowlist()
    except AllowlistCorruptError as exc:
        _log.warning(
            "publisher allowlist file is corrupt (%s); registering source %r "
            "without a TOFU pin. Run `waitbus allowlist repair` to fix the file. "
            "Operator action required: review every plugin source emitted "
            "between this WARN and the repair, as the corruption window "
            "opens a TOFU-bypass surface.",
            exc,
            source_name,
        )
        allowlist = Allowlist(pins={})
    prior = allowlist.for_source(source_name)
    if prior is None:
        if verified is not None:
            append_publisher_pin(
                name=source_name,
                publisher_kind=verified.publisher_kind,
                publisher_identity=verified.publisher_identity,
            )
        return
    if verified is None:
        raise PluginShadowError(
            f"source {source_name!r} is pinned to publisher "
            f"{prior.publisher_kind}:{prior.publisher_identity!r}, but the candidate "
            "plugin carries no verifiable PEP 740 attestation. Either install "
            "`waitbus[plugin-verify]` and re-run, or `waitbus allowlist remove` if you "
            "intend to drop the pin."
        )
    if prior.publisher_kind != verified.publisher_kind or prior.publisher_identity != verified.publisher_identity:
        raise PluginShadowError(
            f"source {source_name!r} is pinned to "
            f"{prior.publisher_kind}:{prior.publisher_identity!r}, but the candidate "
            f"plugin is signed by {verified.publisher_kind}:{verified.publisher_identity!r}. "
            "Run `waitbus allowlist verify <name>` to inspect, or `waitbus allowlist remove "
            f"{source_name}` if the vendor change is intentional."
        )


def _event_type_collisions(spec: SourceSpec) -> set[str]:
    """Return the ``event_type`` values in ``spec`` already claimed by some
    OTHER known source (built-in or plugin-registered).

    Each ``(source, event_type)`` pair must be unique across the registry: two
    sources both claiming ``"workflow_run"`` would make
    ``waitbus emit --source X --event-type workflow_run`` ambiguous and would let
    the broadcaster's default-subscriber filter pass events from either source
    under either label. Shared by :func:`register_plugin` (entry-point path) and
    :func:`register_source` (in-process path) so the uniqueness rule cannot drift
    between the two registration paths. Call under :data:`_REGISTRY_LOCK`: it
    reads the live :func:`known_sources` view, which a concurrent registration
    could mutate.
    """
    existing: frozenset[str] = frozenset(
        chain.from_iterable(
            other_spec.event_types for other_name, other_spec in known_sources().items() if other_name != spec.name
        )
    )
    return set(spec.event_types) & existing


def _validate_common(spec: SourceSpec) -> None:
    """Run the registry-level checks shared by register_plugin and register_source.

    Single-sources the four rules both registration paths enforce identically:
    API-version match, built-in-shadow rejection, duplicate-name rejection, and the
    ``(source, event_type)`` uniqueness rule (:func:`_event_type_collisions`).
    Before this helper existed each path re-typed these guards, so a rule change
    had to be made (and tested) twice. The :class:`SourceSpec` field invariants
    (name / event-type regex, open-core forbidden-field prefixes) already ran in
    ``SourceSpec.__post_init__`` at construction and are not repeated here.

    The duplicate-name and collision checks read :data:`_PLUGIN_SOURCES`, so the
    caller MUST hold :data:`_REGISTRY_LOCK` -- both callers already do, for the
    check-then-record critical section.
    """
    if spec.api_version != SOURCE_PLUGIN_API_VERSION:
        raise PluginVersionMismatchError(
            f"source {spec.name!r} declares api_version={spec.api_version}, "
            f"but waitbus speaks api_version={SOURCE_PLUGIN_API_VERSION}. "
            f"Upgrade `waitbus` or pin the source to a compatible version."
        )
    if spec.name in _BUILTIN_SOURCES:
        raise PluginShadowError(
            f"source name {spec.name!r} would shadow a built-in waitbus source. "
            "Choose a different source name (for an entry-point plugin, rename the "
            "entry-point or fork it under a different source name)."
        )
    if spec.name in _PLUGIN_SOURCES:
        raise PluginDuplicateRegistrationError(
            f"source name {spec.name!r} is already registered in this process. "
            "Two sources claim the same name; resolve via the waitbus allowlist, "
            "uninstall one plugin, or register the in-process source only once."
        )
    collisions = _event_type_collisions(spec)
    if collisions:
        raise PluginContractError(
            f"source {spec.name!r} declares event_types {sorted(collisions)!r} already "
            "claimed by another source. Each (source, event_type) pair must be unique "
            "across the registry. Rename the colliding event_type values."
        )


def register_plugin(ep: EntryPoint, plugin: Any) -> SourceSpec:
    """Validate and register one plugin entry-point.

    Hard-errors on every contract violation. Successful registration
    adds the plugin's :class:`SourceSpec` to :data:`_PLUGIN_SOURCES`
    and (when verified) records the publisher in the TOFU allowlist.

    Raises:
        :class:`PluginContractError`: the plugin object is missing
            required methods or has an unsupported signature.
        :class:`PluginVersionMismatchError`: the plugin declares an
            ``api_version`` other than :data:`SOURCE_PLUGIN_API_VERSION`.
        :class:`PluginShadowError`: the plugin would shadow a built-in
            source, or a TOFU-pinned name's publisher mismatches.
        :class:`ValueError`: ``register_plugin`` was called twice for
            the same source name in the same process (this is a
            duplicate-load bug, not a TOFU policy violation).
    """
    _validate_plugin_callable(plugin, ep.value)
    spec_obj = plugin.spec()
    spec = _validate_spec(spec_obj, ep.value)

    # Refuse a silent name swap between the entry-point key and the
    # SourceSpec.name. The entry-point key is
    # what the operator sees in their pyproject.toml + in ``pip show``;
    # if it disagrees with the runtime-visible source name, operators
    # configuring the allowlist or writing emit() calls would chase a
    # phantom. Hard-error so the discrepancy surfaces at startup. (This guard is
    # entry-point-specific; the cross-path rules live in _validate_common.)
    if ep.name != spec.name:
        raise PluginContractError(
            f"plugin entry-point {ep.value!r} registers source name "
            f"{spec.name!r}, but the entry-point key is {ep.name!r}. "
            f"The entry-point name and SourceSpec.name must match -- a "
            f"silent rename would confuse operators reading "
            f"pyproject.toml against ``waitbus source list`` output. "
            f"Rename either the entry-point key or the SourceSpec.name "
            f"so they agree."
        )

    with _REGISTRY_LOCK:
        _validate_common(spec)
        verified = _verify_publisher(ep, spec.name)
        _enforce_tofu(spec.name, verified)

        _PLUGIN_SOURCES[spec.name] = spec
        _PLUGIN_PUBLISHERS[spec.name] = verified
        _log.info(
            "registered plugin source name=%s ep=%s publisher=%s",
            spec.name,
            ep.value,
            verified.publisher_identity if verified is not None else "unverified",
        )
        return spec


def register_source(spec: SourceSpec) -> SourceSpec:
    """Register a first-party source IN-PROCESS, without an entry-point or attestation.

    This is the imperative counterpart to entry-point discovery
    (:func:`discover_plugins` / :func:`register_plugin`): trusted first-party
    code that already holds a :class:`SourceSpec` -- a built-in synthesizer, the
    ``swarm-demo`` discovery instrument, a test, or a benchmark -- registers it
    directly, the way pytest's ``PluginManager.register()`` coexists with
    ``load_setuptools_entrypoints()``.

    It reuses the SAME validation the entry-point path enforces. The
    :class:`SourceSpec` field invariants (name / event-type regex, and the
    open-core forbidden-field prefixes) already ran in ``SourceSpec.__post_init__``
    at construction; this adds the registry-level checks: API-version match,
    built-in-shadow rejection, duplicate-name rejection, and the shared
    ``(source, event_type)`` uniqueness rule (:func:`_event_type_collisions`).

    It SKIPS the PEP 740 attestation + publisher-TOFU machinery that
    :func:`register_plugin` runs. That machinery defends against a
    distribution-channel threat -- a malicious or typosquatting on-disk *wheel* --
    which does not exist for in-process code the operator is already running. The
    publisher record is therefore ``None`` (no attested publisher), the same value
    an unverified entry-point plugin gets.

    Scope: the registration lives for the lifetime of the process (it is added to
    the same process-singleton :data:`_PLUGIN_SOURCES` the discovery flow uses);
    there is no persistence and no cross-machine propagation, consistent with the
    single-user-workstation trust model. Tests and benchmarks reset it via
    :func:`_clear_for_test_isolation`.

    Returns the registered :class:`SourceSpec`.

    Raises:
        :class:`PluginContractError`: ``spec`` is not a :class:`SourceSpec`, or
            its ``event_types`` collide with an already-registered source.
        :class:`PluginVersionMismatchError`: ``spec.api_version`` does not equal
            :data:`SOURCE_PLUGIN_API_VERSION`.
        :class:`PluginShadowError`: ``spec.name`` shadows a built-in source.
        :class:`PluginDuplicateRegistrationError`: ``spec.name`` is already
            registered in this process.
    """
    if not isinstance(spec, SourceSpec):
        raise PluginContractError(
            f"register_source expects a SourceSpec instance; got {type(spec).__name__!r}. "
            "Construct a SourceSpec (which validates its fields, including the open-core "
            "forbidden-field rule) and pass it."
        )

    with _REGISTRY_LOCK:
        _validate_common(spec)
        _PLUGIN_SOURCES[spec.name] = spec
        _PLUGIN_PUBLISHERS[spec.name] = None
        _log.info("registered in-process source name=%s event_types=%s", spec.name, spec.event_types)
        return spec


def _select_entry_points(policy: PluginPolicy) -> list[EntryPoint]:
    """Filter discovered entry-points against the operator policy.

    Returns the subset of entry-points waitbus should attempt to load.
    """
    discovered = list(entry_points(group=ENTRY_POINT_GROUP))
    if not policy.autoload:
        if not policy.allow:
            return []
        return [ep for ep in discovered if ep.name in policy.allow]
    if policy.deny:
        return [ep for ep in discovered if ep.name not in policy.deny]
    return discovered


def discover_plugins(*, policy: PluginPolicy | None = None) -> list[SourceSpec]:
    """Discover and register plugin sources via the entry-point group.

    Called once per daemon process, typically at startup. Loads the
    operator policy via :func:`load_plugin_policy` unless ``policy``
    is supplied (the override is for tests). Returns the list of
    :class:`SourceSpec` objects successfully registered in this call.

    A failure to load OR register one plugin does NOT short-circuit
    the rest: each plugin's failure is logged and the next plugin is
    attempted. This mirrors the pytest convention (one bad plugin
    must not prevent the rest from loading) while still surfacing
    every failure clearly in the daemon log. Catastrophic policy-
    enforcement failures (:class:`PluginShadowError`,
    :class:`PluginVersionMismatchError`) ARE re-raised as a typed
    exception block at the end so the daemon can fail-fast at
    startup; per-plugin import errors are tolerated.
    """
    resolved_policy = policy if policy is not None else load_plugin_policy()
    selected = _select_entry_points(resolved_policy)
    registered: list[SourceSpec] = []
    policy_failures: list[Exception] = []

    for ep in selected:
        try:
            plugin_obj = ep.load()
        except Exception as exc:
            _log.error("plugin %s failed to import: %s", ep.value, exc, exc_info=True)
            continue
        try:
            spec = register_plugin(ep, plugin_obj)
        except (
            PluginContractError,
            PluginVersionMismatchError,
            PluginShadowError,
            PluginDuplicateRegistrationError,
            AttestationVerificationError,
        ) as exc:
            _log.error("plugin %s rejected: %s", ep.value, exc)
            policy_failures.append(exc)
            continue
        except Exception as exc:
            _log.error("plugin %s spec() raised: %s", ep.value, exc, exc_info=True)
            continue
        registered.append(spec)

    if policy_failures:
        # ExceptionGroup surfaces EVERY rejection at once. Operators
        # debugging a daemon that won't start get the full list in a
        # single stack rather than the previous "first-only" behaviour
        # where fixing one rejection just revealed the next. Python
        # 3.11+ ExceptionGroup is the standard shape for batch-failure
        # reporting; waitbus targets 3.12 so the dependency is solid.
        raise ExceptionGroup(
            f"plugin discovery rejected {len(policy_failures)} plugin(s)",
            policy_failures,
        )
    return registered


def discover_plugins_once(*, policy: PluginPolicy | None = None) -> list[SourceSpec]:
    """Idempotent, thread-safe wrapper around :func:`discover_plugins`.

    Daemons (``waitbus broadcast serve``, ``waitbus listener serve``, etc.)
    call this at startup. The first call walks the entry-point group
    and registers every plugin per :func:`discover_plugins`'s contract;
    every subsequent call within the same process is a no-op that
    returns an empty list. The :data:`_DISCOVERED` flag plus the
    :data:`_REGISTRY_LOCK` ensure two concurrent startup paths (a
    common case in tests that spin up multiple daemons in-process,
    or a future systemd unit that starts broadcast + listener
    simultaneously) execute discovery exactly once.

    Idempotency prevents the case where re-entry would silently lose
    every plugin via the duplicate-name ``ValueError`` that
    :func:`discover_plugins` catches as ``Exception``. Thread-safety
    prevents the check-then-act on ``_PLUGIN_SOURCES`` from racing
    under concurrent registration.

    Pass ``policy`` only when tests / benchmarks need to override
    operator policy (e.g. to allow a specific entry-point name that
    isn't in the operator's TOML). Production callers omit it; the
    operator's policy is loaded from ``$XDG_CONFIG_HOME/waitbus/config.toml``.

    Returns the list of newly-registered specs on the first call; an
    empty list on every subsequent call. Callers needing the cumulative
    registered set should use :func:`known_sources` instead.
    """
    global _DISCOVERED
    with _REGISTRY_LOCK:
        if _DISCOVERED:
            return []
        # Mark BEFORE the call so a recursive re-entry (e.g. a plugin
        # whose import triggers another import of waitbus) short-circuits
        # rather than recursing. The post-call assignment would leave a
        # narrow window in which a recursive caller re-runs discovery.
        _DISCOVERED = True
        try:
            return discover_plugins(policy=policy)
        except BaseException:
            # On policy-failure raise, leave the flag set: the daemon
            # is about to die from the typed exception anyway, and
            # leaving the flag clear would cause a (futile) re-attempt
            # if some caller swallows the exception. Operator semantics
            # are "discovery ran once and failed", not "discovery never
            # ran".
            raise


def entry_points_by_name() -> Mapping[str, EntryPoint]:
    """Return the ``waitbus.sources.v1`` entry-point group keyed by EP name.

    Public helper for CLI verbs (``waitbus source list``, ``source show``,
    ``source verify``, ``allowlist verify``) that need the
    "what is installed on disk" view -- a different concept from
    :func:`known_sources` (which is the "what is registered with the
    daemon's in-process registry" view, populated by
    :func:`discover_plugins_once`). The two views legitimately
    differ: a plugin can be installed-on-disk but unregistered (TOFU
    mismatch, attestation failure, or simply uninstalled-from-the-
    runtime registry); a name can be allowlist-pinned but the
    associated plugin uninstalled. CLI verbs that want to surface
    "what does this name resolve to right now" walk the entry-point
    group via this helper.

    No caching: ``entry_points`` itself walks installed dist-infos
    on every call (a few ms). Callers that need point-in-time
    consistency across multiple lookups should ``dict(entry_points_by_name())``
    once at the top of the verb and reuse the snapshot.
    """
    return {ep.name: ep for ep in entry_points(group=ENTRY_POINT_GROUP)}


def plugin_publishers() -> Mapping[str, VerifiedPublisher | None]:
    """Return the publisher identities recorded for each plugin source.

    Used by ``waitbus sources list`` to surface "where did this plugin
    come from?" without re-running attestation verification.

    Returns a ``MappingProxyType`` view onto the internal registry: the
    Mapping contract is the public surface, and a read-only proxy makes
    the read-only intent enforceable at runtime without allocating a
    full dict copy on every call.
    """
    return MappingProxyType(_PLUGIN_PUBLISHERS)


def _clear_for_test_isolation() -> None:
    """Clear plugin registrations + the one-shot discovery flag.

    Test-isolation seam reachable from ``tests/`` autouse fixtures AND
    from ``benchmarks/`` measurement loops (e.g.
    ``benchmarks/bench_predicate_eval_latency_multi.py`` installs a fake
    plugin for one measurement window and resets between windows). The
    name documents both use cases: it is "clear state to isolate the
    next test or measurement", not "clear state at runtime" -- production
    daemons MUST NOT call this.

    Renamed from ``_reset_for_tests`` (the original name implied tests-only
    but the benchmark use was always legitimate; the rename surfaces the
    real contract). The leading underscore signals "non-public API,
    cross-module access by tests/benchmarks is the documented exception"
    per the project's underscore-with-test-access convention.
    """
    global _DISCOVERED
    with _REGISTRY_LOCK:
        _PLUGIN_SOURCES.clear()
        _PLUGIN_PUBLISHERS.clear()
        _DISCOVERED = False


__all__ = [
    "ENTRY_POINT_GROUP",
    "discover_plugins",
    "discover_plugins_once",
    "entry_points_by_name",
    "event_types_supported",
    "is_known_source",
    "known_sources",
    "plugin_publishers",
    "register_plugin",
    "register_source",
]
