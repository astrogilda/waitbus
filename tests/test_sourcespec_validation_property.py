"""Property-based validation tests for SourceSpec.__post_init__.

Hypothesis-driven tests asserting that the SourceSpec contract validators
(name regex, event_types non-empty + regex, payload_schema isinstance,
api_version positive non-bool) hold across a broad sample of inputs.
Any input failing the regex / non-empty / type checks must raise
ValueError at construction; the property tests are

Anchored to the existing project Hypothesis idiom (cf.
``tests/test_property_validators.py``).
"""

from __future__ import annotations

import re

import msgspec
import pytest
from hypothesis import assume, given
from hypothesis import strategies as st

from waitbus.sources._protocol import (
    SOURCE_PLUGIN_API_VERSION,
    SourceSpec,
)

_VALID_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")


# ---------------------------------------------------------------------------
# Valid-input round-trip (positive cases)
# ---------------------------------------------------------------------------


@given(
    name=st.from_regex(_VALID_NAME_RE, fullmatch=True),
    event_types=st.lists(
        st.from_regex(_VALID_NAME_RE, fullmatch=True),
        min_size=1,
        max_size=8,
        unique=True,
    ),
)
def test_valid_name_and_event_types_construct_successfully(name: str, event_types: list[str]) -> None:
    """Any input passing the documented regex constructs without raising."""
    spec = SourceSpec(name=name, event_types=tuple(event_types))
    assert spec.name == name
    assert spec.event_types == tuple(event_types)
    assert spec.payload_schema is None
    assert spec.api_version == SOURCE_PLUGIN_API_VERSION


# ---------------------------------------------------------------------------
# Invalid name rejections
# ---------------------------------------------------------------------------


@given(name=st.text())
def test_arbitrary_string_either_matches_regex_or_raises(name: str) -> None:
    """Either ``name`` matches the regex (constructs) or it raises ValueError."""
    if _VALID_NAME_RE.match(name):
        SourceSpec(name=name, event_types=("e",))  # must not raise
    else:
        with pytest.raises(ValueError, match=r"SourceSpec\.name"):
            SourceSpec(name=name, event_types=("e",))


@pytest.mark.parametrize(
    "bad_name",
    [
        "",  # empty
        "With-Caps",  # uppercase + hyphen
        "1leading_digit",  # leading digit
        "has space",  # internal space
        "trailing-dash",  # hyphen
        "GitHub",  # uppercase
        " whitespace_leading",
        "whitespace_trailing ",
    ],
)
def test_name_explicit_invalid_strings_rejected(bad_name: str) -> None:
    """Explicit-invalid name strings raise ValueError citing the regex."""
    with pytest.raises(ValueError, match=r"SourceSpec\.name"):
        SourceSpec(name=bad_name, event_types=("e",))


# ---------------------------------------------------------------------------
# Invalid event_types rejections
# ---------------------------------------------------------------------------


def test_empty_event_types_tuple_rejected() -> None:
    """``event_types=()`` raises ValueError."""
    with pytest.raises(ValueError, match=r"SourceSpec\.event_types must be a non-empty tuple"):
        SourceSpec(name="x", event_types=())


@given(event_types=st.lists(st.text(), min_size=1, max_size=4))
def test_event_types_members_must_each_match_regex(event_types: list[str]) -> None:
    """Each member of event_types must match the source-name regex."""
    bad = [et for et in event_types if not _VALID_NAME_RE.match(et)]
    if bad:
        with pytest.raises(ValueError, match=r"SourceSpec\.event_types"):
            SourceSpec(name="x", event_types=tuple(event_types))
    else:
        SourceSpec(name="x", event_types=tuple(event_types))  # must not raise


@pytest.mark.parametrize(
    "bad_event_types",
    [
        ("",),  # empty string member
        ("real_event", ""),  # mixed
        ("With-Caps",),  # uppercase + hyphen
        ("1leading",),  # leading digit
        ("has space",),  # internal space
    ],
)
def test_event_types_explicit_invalid_strings_rejected(bad_event_types: tuple[str, ...]) -> None:
    """Explicit-invalid event_type strings raise ValueError."""
    with pytest.raises(ValueError, match=r"SourceSpec\.event_types"):
        SourceSpec(name="x", event_types=bad_event_types)


# ---------------------------------------------------------------------------
# payload_schema isinstance check
# ---------------------------------------------------------------------------


class _ValidSchema(msgspec.Struct, frozen=True):
    """Worked example of a valid payload_schema (msgspec.Struct subclass)."""

    field: str


class _PlainClass:
    """A plain (non-msgspec) class -- must be rejected as payload_schema."""


@pytest.mark.parametrize(
    "bad_schema",
    [
        int,  # builtin type
        tuple,  # builtin type
        str,
        _PlainClass,  # plain class, not msgspec.Struct subclass
        42,  # instance, not type
        "not a type",
        [],
    ],
)
def test_payload_schema_must_be_msgspec_struct_or_none(bad_schema: object) -> None:
    """``payload_schema`` rejects ``int``, ``tuple``, plain classes, instances."""
    with pytest.raises(ValueError, match=r"SourceSpec\.payload_schema"):
        SourceSpec(name="x", event_types=("e",), payload_schema=bad_schema)  # type: ignore[arg-type]


def test_payload_schema_msgspec_struct_subclass_accepted() -> None:
    """A real msgspec.Struct subclass is accepted as payload_schema."""
    spec = SourceSpec(name="x", event_types=("e",), payload_schema=_ValidSchema)
    assert spec.payload_schema is _ValidSchema


def test_payload_schema_none_accepted() -> None:
    """``payload_schema=None`` is the default and is accepted."""
    spec = SourceSpec(name="x", event_types=("e",))
    assert spec.payload_schema is None


# ---------------------------------------------------------------------------
# api_version positive-non-bool check
# ---------------------------------------------------------------------------


@given(api_version=st.integers())
def test_api_version_must_be_positive_int(api_version: int) -> None:
    """Integer api_version: positive constructs, non-positive raises."""
    assume(type(api_version) is int)  # exclude any synthesized bools
    if api_version > 0:
        SourceSpec(name="x", event_types=("e",), api_version=api_version)
    else:
        with pytest.raises(ValueError, match=r"SourceSpec\.api_version"):
            SourceSpec(name="x", event_types=("e",), api_version=api_version)


@pytest.mark.parametrize("bad_value", [True, False, 0, -1, -100])
def test_api_version_explicit_invalid_values_rejected(bad_value: object) -> None:
    """``True``, ``False``, ``0``, negative ints all raise ValueError."""
    with pytest.raises(ValueError, match=r"SourceSpec\.api_version"):
        SourceSpec(name="x", event_types=("e",), api_version=bad_value)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# __init_subclass__ open-core invariant
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "forbidden_field",
    [
        "relay_endpoint",
        "auth_token",
        "account_id",
        "oidc_audience",
        "tenant_uuid",
        "cluster_name",
        "endpoint_url",
    ],
)
def test_sourcespec_subclass_with_forbidden_prefix_fails_at_class_creation(forbidden_field: str) -> None:
    """A SourceSpec subclass declaring a forbidden-prefix field raises TypeError.

    Uses ``exec()`` with a templated class body because ``msgspec.defstruct``
    bypasses Python-level ``__init_subclass__`` (it creates the class via
    the msgspec C extension's class-builder, not the standard ``type()``
    path). A real plugin author writing a ``class MaliciousSpec(SourceSpec):``
    block in their source goes through the standard machinery, which DOES
    fire our hook -- so testing via ``exec()`` of an equivalent source
    fragment matches the real adversary path.
    """
    src = f"class _Malicious(SourceSpec, frozen=True, kw_only=True):\n    {forbidden_field}: str = ''\n"
    namespace = {"SourceSpec": SourceSpec}
    with pytest.raises(TypeError, match="forbidden prefix"):
        exec(src, namespace)


def test_sourcespec_subclass_with_allowed_fields_succeeds() -> None:
    """A SourceSpec subclass declaring only non-forbidden fields is allowed."""
    src = "class _Benign(SourceSpec, frozen=True, kw_only=True):\n    extra_metadata: str = ''\n"
    namespace: dict[str, object] = {"SourceSpec": SourceSpec}
    exec(src, namespace)  # must not raise; subclassing per se is allowed
    benign_cls = namespace["_Benign"]
    assert isinstance(benign_cls, type)
