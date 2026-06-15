"""Public extension contract for the ``waitbus.sources.v1`` entry-point group.

This module defines the typed, frozen, exception-stable surface that third-
party Python packages target when they register custom event sources with
waitbus. The runtime registry lives in :mod:`waitbus.sources._registry`;
this module is pure declarations so plugin authors can import it without
pulling in waitbus's daemon dependency closure.

Contract:

* :class:`SourceSpec` is a frozen :class:`msgspec.Struct` plugin authors
  return from their entry-point callable. The four fields capture
  (canonical source name, allowed ``event_type`` values, optional payload
  schema, API version). waitbus validates every field at construction time
  via ``__post_init__`` (msgspec invokes it on direct construction AND on
  ``msgspec.json.decode`` / ``msgspec.convert``, per
  ``msgspec/docs/structs.rst:164-171``) so an ill-formed spec fails fast
  at the boundary instead of poisoning the registry. Subclassing
  ``SourceSpec`` triggers ``__init_subclass__`` which AST-walks the
  subclass's ``__struct_fields__`` against
  :data:`_FORBIDDEN_SOURCESPEC_FIELD_PREFIXES`; this extends the
  local-primitive separability invariant (enforced in-tree by
  ``tests/test_sourcespec_local_boundary.py``) to third-party subclasses.

* :class:`SourcePlugin` is a :class:`typing.Protocol` marker used for
  **static typing only** (mypy --strict reads it; ``isinstance`` does
  not). ``@runtime_checkable`` is deliberately omitted — it has known
  CPython performance cost and verifies only attribute *presence*. The
  registry calls :func:`inspect.signature` at registration time on the
  Protocol's single method (``spec``) and raises
  :class:`PluginContractError` on mismatch. waitbus does NOT prescribe a
  producer interface on the plugin object: producers emit via the
  public :func:`waitbus.emit` API. (A previous version of
  this Protocol also declared a ``fetch`` method, but the registry
  never invoked it and the example plugin's implementation raised
  ``NotImplementedError`` — the method was documentation, not contract.
  Dropped in an earlier cleanup.)

* :class:`PluginContractError`, :class:`PluginShadowError`,
  :class:`PluginVersionMismatchError`,
  :class:`PluginDuplicateRegistrationError` are the four exception
  classes the registry raises. All four subclass :class:`ValueError`
  so callers catching ``ValueError`` (the established waitbus convention
  for typed configuration / registration errors) still work; the typed-
  subclass surface exists so operators can disambiguate failure modes in
  their own scripts and so waitbus's CLI can print mode-specific
  remediation hints.

Local-primitive boundary: :class:`SourceSpec` MUST NEVER carry a field whose
name signals a network-coordination / relay / multi-tenant role. The
forbidden-name list is enforced (a) by
``tests/test_sourcespec_local_boundary.py`` as a binding AST-walk test
of the local-primitive separability rule against ``SourceSpec`` itself, AND
(b) by ``SourceSpec.__init_subclass__`` against any third-party subclass
at class-definition time. The test owns the canonical invariant
definition for the waitbus tree; the dunder extends the same invariant to
plugin-author subclasses.

API version: the entry-point group name carries the API-version suffix
(``waitbus.sources.v1``). When waitbus introduces a breaking ``SourceSpec``
change, the group becomes ``waitbus.sources.v2`` and waitbus enumerates both
groups during the transition window. ``SourceSpec.api_version`` is a
secondary check that catches the case where a plugin's wheel was
installed against a now-incompatible waitbus release but the entry-point
group name happens to still match.
"""

from __future__ import annotations

import re
from typing import Any, Final, Protocol

import msgspec

# Public version of the SourceSpec contract. Bumped when the contract
# changes incompatibly. The entry-point group name also carries this
# (``waitbus.sources.v1``); both are checked at registration time.
SOURCE_PLUGIN_API_VERSION: Final[int] = 1

# Forbidden field-name prefixes for SourceSpec. Enforced by
# ``tests/test_sourcespec_local_boundary.py`` against SourceSpec itself AND
# by ``SourceSpec.__init_subclass__`` against any third-party subclass.
# Any field whose name matches any prefix below is a violation of the
# local-primitive separability rule (the ``SourceSpec`` surface must not assume
# relay / account / tenant / cluster / network-coordination context).
# The test owns the canonical invariant; ``__init_subclass__`` extends
# the same invariant to subclasses defined outside the waitbus tree.
_FORBIDDEN_SOURCESPEC_FIELD_PREFIXES: Final[tuple[str, ...]] = (
    "relay_",
    "auth_",
    "account_",
    "oidc_",
    "tenant_",
    "cluster_",
    "endpoint_",
)

# Canonical source-name shape. Used by ``SourceSpec.__post_init__`` to
# validate both ``name`` itself and each member of ``event_types``.
# Matches lowercase ASCII identifiers starting with a letter, allowing
# digits and underscores after the first character. The built-in
# taxonomy (``github``, ``alertmanager``, ``pytest``, ``docker``, ``fs``)
# all conform; the regex doubles as an invariant lock-in for built-ins.
_SOURCE_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z][a-z0-9_]*$")


class PluginContractError(ValueError):
    """A plugin's ``SourceSpec`` or ``spec()`` method violated the contract.

    Raised at registration time when ``inspect.signature(plugin.spec)``
    rejects the plugin's callable, when the returned object is not a
    :class:`SourceSpec`, when a field shape violates the documented
    constraints (regex-mismatched name / empty event_types / non-
    msgspec.Struct payload_schema / non-positive api_version), or when
    the plugin's entry-point name disagrees with the returned
    ``SourceSpec.name``.
    """


class PluginShadowError(ValueError):
    """A plugin tried to register a source name already bound to a different publisher.

    Raised when the entry-point loader encounters a name registered by a
    publisher OIDC identity that does not match the TOFU-pinned identity
    in ``$XDG_CONFIG_HOME/waitbus/plugins.allowlist.toml``. The operator
    must explicitly resolve via ``waitbus allowlist verify <name>``.
    """


class PluginVersionMismatchError(ValueError):
    """A plugin's declared API version does not match waitbus's expected version.

    Raised when the plugin's ``SourceSpec.api_version`` does not equal
    :data:`SOURCE_PLUGIN_API_VERSION`. The entry-point group name
    (``waitbus.sources.v1``) is the primary version selector; this
    secondary check catches plugins whose group name happens to match
    but whose internal contract version has drifted.
    """


class PluginDuplicateRegistrationError(ValueError):
    """Two simultaneously-loaded plugins claim the same source name.

    Raised by ``register_plugin`` when a second plugin tries to register
    a source name already present in the in-process plugin registry.
    Distinct from :class:`PluginShadowError` (which is the TOFU
    publisher-mismatch path against a *pinned* name); duplicate-
    registration is the same-process, same-startup collision between
    two installed plugins both claiming e.g. ``"circleci"``.

    Subclasses :class:`ValueError` like its three sibling errors so the
    waitbus convention of catching ``ValueError`` for registration-error
    polymorphism continues to work; the typed class exists so
    :func:`discover_plugins`'s except ladder can route it to
    ``policy_failures`` rather than swallowing it as a generic exception.
    """


class SourceSpec(msgspec.Struct, frozen=True, kw_only=True):
    """Typed registration shape returned by a plugin's entry-point callable.

    Frozen :class:`msgspec.Struct`: instances are hashable + immutable, so
    the registry can use them as dict values without defensive copies and
    they survive WeakValueDictionary lookups cleanly. msgspec validates
    field *types* at construction; ``__post_init__`` validates *values*
    (regex shape, non-empty tuples, positive int-not-bool).

    The msgspec.Struct base is shared with :class:`~waitbus._types.EventInsert`
    and :class:`~waitbus._types.Event` — the only other validated
    frozen types in the codebase. Plugin authors who want strict payload
    validation set ``payload_schema`` to their own ``msgspec.Struct``
    subclass and let waitbus decode the payload via the standard msgspec
    decoder; ``payload_schema=None`` keeps the payload opaque JSON
    (matches the current github/alertmanager/pytest/docker/fs flexibility).

    Attributes:
        name: The canonical source name as it appears in
            ``Event.source`` / ``EventInsert.source`` and on the wire
            (e.g. ``"circleci"``, ``"jenkins"``). MUST be a non-empty
            lowercase ASCII string matching ``[a-z][a-z0-9_]*``.
            Enforced by ``__post_init__``.
        event_types: The ``event_type`` values this source is allowed
            to emit. MUST be a non-empty tuple of strings each matching
            the same regex as ``name``. waitbus's broadcaster default-
            subscriber filter merges this into ``event_types_supported``
            so subscribers with default settings receive plugin-emitted
            events. Enforced by ``__post_init__``.
        payload_schema: Optional :class:`msgspec.Struct` subclass the
            plugin's payload conforms to. When set, waitbus validates
            decoded payloads against this type via msgspec's standard
            decoder; when None, the payload is opaque JSON.
            ``__post_init__`` rejects non-None values that are not
            ``msgspec.Struct`` subclasses (including ``int``, ``tuple``,
            arbitrary user classes).
        api_version: Plugin contract version. MUST equal
            :data:`SOURCE_PLUGIN_API_VERSION` (a positive int; ``True``
            and ``False`` are rejected even though Python's ``bool`` is
            ``int``-subclass). Enforced by ``__post_init__`` for the
            "is positive non-bool" check; the equality check happens in
            ``register_plugin``.
    """

    name: str
    event_types: tuple[str, ...]
    payload_schema: type | None = None
    api_version: int = SOURCE_PLUGIN_API_VERSION

    def __post_init__(self) -> None:
        """Validate field values at construction.

        Runs on direct construction, ``msgspec.json.decode``, and
        ``msgspec.convert`` per the msgspec.Struct contract. Hard-errors
        on ill-formed values at the boundary instead of letting them
        poison the registry.
        """
        if not isinstance(self.name, str) or not _SOURCE_NAME_RE.match(self.name):
            raise ValueError(
                f"SourceSpec.name must be a non-empty lowercase ASCII string "
                f"matching {_SOURCE_NAME_RE.pattern!r}; got {self.name!r}"
            )
        if not self.event_types:
            raise ValueError(
                f"SourceSpec.event_types must be a non-empty tuple of event-type strings; got {self.event_types!r}"
            )
        for event_type in self.event_types:
            if not isinstance(event_type, str) or not _SOURCE_NAME_RE.match(event_type):
                raise ValueError(
                    f"SourceSpec.event_types members must each be non-empty "
                    f"lowercase ASCII strings matching {_SOURCE_NAME_RE.pattern!r}; "
                    f"got {event_type!r} in {self.event_types!r}"
                )
        # ``payload_schema`` is documented as ``msgspec.Struct`` subclass
        # or None. ``int``, ``tuple``, arbitrary user classes are all
        # rejected; the registry's decoder relies on the msgspec.Struct
        # invariants.
        if self.payload_schema is not None and not (
            isinstance(self.payload_schema, type) and issubclass(self.payload_schema, msgspec.Struct)
        ):
            raise ValueError(
                f"SourceSpec.payload_schema must be None or a msgspec.Struct subclass; got {self.payload_schema!r}"
            )
        # ``bool`` is a subclass of ``int`` in Python; explicit ``type(x) is int``
        # rejects ``True`` / ``False`` while still admitting plain ``int``.
        if type(self.api_version) is not int or self.api_version <= 0:
            raise ValueError(f"SourceSpec.api_version must be a positive int (not bool); got {self.api_version!r}")

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Extend the local-primitive separability invariant to subclasses.

        ``tests/test_sourcespec_local_boundary.py`` enforces the forbidden-
        prefix list against ``SourceSpec`` itself via an AST walk; that
        test cannot reach a third-party subclass defined in a plugin
        author's package. This dunder closes the gap: at the moment a
        subclass is created, we walk its own ``__annotations__``
        against :data:`_FORBIDDEN_SOURCESPEC_FIELD_PREFIXES` and raise
        ``TypeError`` if any annotation key carries a forbidden prefix.

        ``__annotations__`` is the right inspection seam (rather than
        msgspec.Struct's ``__struct_fields__``) because Python invokes
        ``__init_subclass__`` during the ``type.__init__`` phase BEFORE
        msgspec's metaclass populates ``__struct_fields__``. Annotations
        on the class body are available immediately at the
        ``type.__new__`` phase, so they ARE populated when this hook
        runs. The subclass-only annotation view (we do not walk
        inherited annotations) is exactly the right scope: we are
        checking what the third-party subclass ADDED, not the
        invariants of the parent (which the AST-walk test already
        covers).
        """
        super().__init_subclass__(**kwargs)
        own_annotations: dict[str, object] = vars(cls).get("__annotations__", {})
        for field_name in own_annotations:
            for forbidden_prefix in _FORBIDDEN_SOURCESPEC_FIELD_PREFIXES:
                if field_name.startswith(forbidden_prefix):
                    raise TypeError(
                        f"SourceSpec subclass {cls.__qualname__!r} declares "
                        f"field {field_name!r} whose name starts with the "
                        f"forbidden prefix {forbidden_prefix!r}. The "
                        f"local-primitive separability rule forbids relay / auth / "
                        f"account / oidc / tenant / cluster / endpoint fields "
                        f"on SourceSpec; if you need network-coordination "
                        f"state, carry it on your plugin object, not on "
                        f"SourceSpec."
                    )


class SourcePlugin(Protocol):
    """Static type alias for a third-party source plugin.

    Plugin authors implement one method:

    * ``spec()`` returns the plugin's :class:`SourceSpec`. Called once
      at registration time. The signature is validated by
      :func:`inspect.signature`; deviations raise
      :class:`PluginContractError`.

    Producers emit events via the public :func:`waitbus.emit`
    API. waitbus does NOT prescribe any method on the plugin object for
    that purpose: plugins are typically daemons or schedulers that own
    their own event-producing loop and call ``emit()`` directly. (An
    earlier version of this Protocol declared a ``fetch`` method, but
    it was never invoked by waitbus and the example plugin raised
    ``NotImplementedError``; the method was documentation, not contract,
    and was dropped in an earlier cleanup.)

    NOT decorated ``@runtime_checkable`` — see module docstring for the
    rationale. Use this Protocol for static typing; let the registry
    validate via :func:`inspect.signature` at registration time.
    """

    def spec(self) -> SourceSpec:
        """Return the plugin's typed registration shape."""
        ...


__all__ = [
    "SOURCE_PLUGIN_API_VERSION",
    "PluginContractError",
    "PluginDuplicateRegistrationError",
    "PluginShadowError",
    "PluginVersionMismatchError",
    "SourcePlugin",
    "SourceSpec",
]
