"""Tests for the PEP 740 attestation verification module.

Covers:

* ``AttestationToolingMissingError`` raised when ``pypi_attestations`` is
  not installed (the optional ``waitbus[plugin-verify]`` extra).
* ``verify_distribution`` returns ``None`` for installs that lack a
  ``.provenance`` file (the common case for plugins not yet adopting
  Trusted Publishing).
* ``verify_distribution`` returns ``None`` for editable installs (no
  dist-info on disk) and for PyPI-name installs (no
  ``direct_url.json``); both are documented gaps in PEP 610.
* ``read_attestation_json`` returns the raw on-disk provenance JSON
  for display (used by ``waitbus source show``) without re-running
  cryptographic verification.
* End-to-end happy path: a hand-crafted ``Provenance`` envelope plus
  a ``direct_url.json`` cross-check yields a :class:`VerifiedPublisher`,
  with the Sigstore-backed ``Attestation.verify`` monkeypatched (the
  unit test exercises the waitbus plumbing; real cryptographic
  verification belongs in an integration test against a published
  wheel).
* Per-Trusted-Publisher-kind identity formatting (GitHub, GitLab,
  Google, CircleCI). Unknown kinds raise
  :class:`AttestationVerificationError` because an unrecognised
  publisher identity must not slip into the TOFU allowlist as a
  generic string.
"""

from __future__ import annotations

import json
from importlib.metadata import Distribution
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from waitbus.sources._attestation import (
    AttestationToolingMissingError,
    AttestationVerificationError,
    VerifiedPublisher,
    _format_publisher_identity,
    dist_info_dir,
    read_attestation_json,
    verify_distribution,
)


def _make_dist(files: list[Any] | None = None, name: str = "fake-plugin") -> MagicMock:
    """Return a MagicMock shaped like importlib.metadata.Distribution."""
    dist = MagicMock(spec=Distribution)
    dist.name = name
    dist.files = files
    return dist


def _make_file_record(name: str, locate_path: Path | None = None) -> MagicMock:
    """Return a MagicMock shaped like a distribution file record."""
    record = MagicMock()
    record.name = name
    record.locate.return_value = locate_path
    return record


def _build_dist_info(
    tmp_path: Path,
    *,
    project_name: str = "fake_plugin",
    version: str = "1.0",
    wheel_tags: str = "py3-none-any",
    direct_url_payload: dict[str, Any] | None = None,
    provenance_json: str | None = None,
) -> tuple[Path, MagicMock]:
    """Materialise a dist-info directory on disk + return its Distribution mock.

    Mirrors the layout pip 25+ produces post-install:

    * ``<project>-<version>.dist-info/METADATA`` (canonical anchor used by
      ``dist_info_dir`` to locate the directory).
    * ``<project>-<version>.dist-info/direct_url.json`` (PEP 610) when
      ``direct_url_payload`` is supplied.
    * ``<project>-<version>.dist-info/<wheel-stem>.provenance`` (PEP 740)
      when ``provenance_json`` is supplied.
    """
    info_dir = tmp_path / f"{project_name}-{version}.dist-info"
    info_dir.mkdir()
    metadata_path = info_dir / "METADATA"
    metadata_path.write_text(
        f"Metadata-Version: 2.1\nName: {project_name}\nVersion: {version}\n",
        encoding="utf-8",
    )
    if direct_url_payload is not None:
        (info_dir / "direct_url.json").write_text(json.dumps(direct_url_payload), encoding="utf-8")
    if provenance_json is not None:
        wheel_stem = f"{project_name}-{version}-{wheel_tags}.whl"
        (info_dir / f"{wheel_stem}.provenance").write_text(provenance_json, encoding="utf-8")
    metadata_record = _make_file_record("METADATA", locate_path=metadata_path)
    dist = _make_dist(files=[metadata_record], name=project_name)
    return info_dir, dist


def _direct_url_payload(*, project: str, version: str, wheel_tags: str, sha256: str) -> dict[str, Any]:
    """Build a PEP 610 ``direct_url.json`` payload pointing at a PyPI wheel."""
    wheel_filename = f"{project}-{version}-{wheel_tags}.whl"
    return {
        "url": f"https://files.pythonhosted.org/packages/abcdef/{wheel_filename}",
        "archive_info": {"hash": f"sha256={sha256}"},
    }


# ---------------------------------------------------------------------------
# AttestationToolingMissingError path
# ---------------------------------------------------------------------------


def test_tooling_missing_error_raised_when_pypi_attestations_absent() -> None:
    """verify_distribution raises AttestationToolingMissingError when pypi_attestations is not installed."""
    dist = _make_dist(files=[])

    with (
        patch(
            "waitbus.sources._attestation._load_attestations_module",
            side_effect=AttestationToolingMissingError("pypi_attestations not installed"),
        ),
        pytest.raises(AttestationToolingMissingError),
    ):
        verify_distribution(dist)


# ---------------------------------------------------------------------------
# Structural-gap paths -- return None (do not raise)
# ---------------------------------------------------------------------------


def test_verify_distribution_returns_none_when_no_dist_info_on_disk() -> None:
    """Editable installs / broken RECORDs have no locatable dist-info -> None."""
    dist = _make_dist(files=None)
    with patch("waitbus.sources._attestation._load_attestations_module", return_value=MagicMock()):
        assert verify_distribution(dist) is None


def test_verify_distribution_returns_none_when_no_provenance_file(tmp_path: Path) -> None:
    """A dist-info with no <wheel-stem>.provenance returns None (common case)."""
    _, dist = _build_dist_info(tmp_path)
    with patch("waitbus.sources._attestation._load_attestations_module", return_value=MagicMock()):
        assert verify_distribution(dist) is None


def test_verify_distribution_returns_none_when_no_direct_url_json(tmp_path: Path) -> None:
    """A provenance file without direct_url.json returns None + WARN.

    PyPI-name installs (``pip install foo``) do not write direct_url.json
    per PEP 610. Without an independent wheel-digest source, waitbus cannot
    cross-check the wheel against the signed attestation subject, so the
    verifier degrades gracefully to None rather than trivially passing
    against the attestation's self-claimed digest.
    """
    _, dist = _build_dist_info(tmp_path, provenance_json='{"version":1,"attestation_bundles":[]}')
    with patch("waitbus.sources._attestation._load_attestations_module", return_value=MagicMock()):
        assert verify_distribution(dist) is None


def test_verify_distribution_returns_none_when_direct_url_lacks_sha256(tmp_path: Path) -> None:
    """direct_url.json with an md5 (not sha256) hash -> None.

    PEP 740 binds a sha256; waitbus refuses to trust other hash algorithms.
    """
    payload = {"url": "https://example.com/x-1.0-py3-none-any.whl", "archive_info": {"hash": "md5=deadbeef"}}
    _, dist = _build_dist_info(
        tmp_path,
        direct_url_payload=payload,
        provenance_json='{"version":1,"attestation_bundles":[]}',
    )
    with patch("waitbus.sources._attestation._load_attestations_module", return_value=MagicMock()):
        assert verify_distribution(dist) is None


# ---------------------------------------------------------------------------
# read_attestation_json -- public helper for `waitbus source show`
# ---------------------------------------------------------------------------


def test_read_attestation_json_returns_raw_envelope(tmp_path: Path) -> None:
    """read_attestation_json returns the raw .provenance JSON, no parsing."""
    envelope = '{"version":1,"attestation_bundles":[{"publisher":{"kind":"GitHub"},"attestations":[]}]}'
    _, dist = _build_dist_info(tmp_path, provenance_json=envelope)
    assert read_attestation_json(dist) == envelope


def test_read_attestation_json_returns_none_when_no_dist_info() -> None:
    """read_attestation_json returns None when dist-info is missing."""
    dist = _make_dist(files=None)
    assert read_attestation_json(dist) is None


def test_read_attestation_json_returns_none_when_no_provenance(tmp_path: Path) -> None:
    """read_attestation_json returns None when the dist-info has no .provenance file."""
    _, dist = _build_dist_info(tmp_path)
    assert read_attestation_json(dist) is None


# ---------------------------------------------------------------------------
# dist_info_dir -- public helper
# ---------------------------------------------------------------------------


def test_dist_info_dir_locates_directory(tmp_path: Path) -> None:
    info_dir, dist = _build_dist_info(tmp_path)
    assert dist_info_dir(dist) == info_dir


def test_dist_info_dir_returns_none_when_files_missing() -> None:
    assert dist_info_dir(_make_dist(files=None)) is None


# ---------------------------------------------------------------------------
# End-to-end happy path -- real Provenance envelope, monkeypatched verify
# ---------------------------------------------------------------------------


def test_verify_distribution_returns_verified_publisher_on_success(tmp_path: Path) -> None:
    """A well-formed Provenance + direct_url.json yields a VerifiedPublisher.

    The Sigstore-backed ``Attestation.verify`` is monkeypatched; the test
    exercises the waitbus plumbing (Provenance.model_validate_json,
    AttestationBundle.publisher extraction, identity formatting, return
    shape). Real cryptographic verification belongs in an integration
    test against a published wheel.
    """
    sha256 = "0" * 64
    direct_url = _direct_url_payload(project="fake_plugin", version="1.0", wheel_tags="py3-none-any", sha256=sha256)
    _, dist = _build_dist_info(
        tmp_path,
        direct_url_payload=direct_url,
        provenance_json='{"_envelope_content":"opaque_to_waitbus_unit_tests"}',
    )

    # Construct a stub pypi_attestations module that returns a fake
    # Provenance object on model_validate_json. The fake exposes the
    # AttestationBundle / Publisher / Attestation shapes the waitbus
    # verifier walks.
    stub_publisher = MagicMock()
    stub_publisher.kind = "GitHub"
    stub_publisher.repository = "astrogilda/fake-plugin"
    stub_publisher.workflow = ".github/workflows/release.yml"

    stub_attestation = MagicMock()
    stub_attestation.verify.return_value = ("https://docs.pypi.org/attestations/publish/v1", None)

    stub_bundle = MagicMock()
    stub_bundle.publisher = stub_publisher
    stub_bundle.attestations = [stub_attestation]

    stub_provenance = MagicMock()
    stub_provenance.attestation_bundles = [stub_bundle]

    class _StubAttestationError(Exception):
        pass

    class _StubVerificationError(_StubAttestationError):
        pass

    stub_mod = MagicMock()
    stub_mod.AttestationError = _StubAttestationError
    stub_mod.VerificationError = _StubVerificationError
    stub_mod.Provenance.model_validate_json.return_value = stub_provenance
    stub_mod.Distribution.return_value = MagicMock()

    with patch("waitbus.sources._attestation._load_attestations_module", return_value=stub_mod):
        result = verify_distribution(dist)

    assert isinstance(result, VerifiedPublisher)
    assert result.publisher_kind == "GitHub"
    assert result.publisher_identity == "astrogilda/fake-plugin @ .github/workflows/release.yml"
    assert result.predicate_type == "https://docs.pypi.org/attestations/publish/v1"

    # Confirm verify() was called with the bundle's publisher as identity,
    # not None. (The pre-fix bug passed identity=None which sigstore rejects.)
    assert stub_attestation.verify.call_count == 1
    _args, kwargs = stub_attestation.verify.call_args
    assert kwargs["identity"] is stub_publisher
    # And the supplied Distribution was synthesised from direct_url.json:
    stub_mod.Distribution.assert_called_once_with(
        name="fake_plugin-1.0-py3-none-any.whl",
        digest=sha256,
    )


# ---------------------------------------------------------------------------
# Verify failure -> AttestationVerificationError
# ---------------------------------------------------------------------------


def test_verify_distribution_raises_on_signature_verification_failure(tmp_path: Path) -> None:
    """Sigstore VerificationError surfaces as AttestationVerificationError."""
    sha256 = "1" * 64
    direct_url = _direct_url_payload(project="evil_plugin", version="1.0", wheel_tags="py3-none-any", sha256=sha256)
    _, dist = _build_dist_info(
        tmp_path,
        project_name="evil_plugin",
        direct_url_payload=direct_url,
        provenance_json='{"_opaque":"ok"}',
    )

    class _StubAttestationError(Exception):
        pass

    class _StubVerificationError(_StubAttestationError):
        pass

    stub_attestation = MagicMock()
    stub_attestation.verify.side_effect = _StubVerificationError("signature mismatch")

    stub_publisher = MagicMock()
    stub_publisher.kind = "GitHub"
    stub_publisher.repository = "attacker/repo"
    stub_publisher.workflow = ".github/workflows/release.yml"

    stub_bundle = MagicMock()
    stub_bundle.publisher = stub_publisher
    stub_bundle.attestations = [stub_attestation]

    stub_provenance = MagicMock()
    stub_provenance.attestation_bundles = [stub_bundle]

    stub_mod = MagicMock()
    stub_mod.AttestationError = _StubAttestationError
    stub_mod.VerificationError = _StubVerificationError
    stub_mod.Provenance.model_validate_json.return_value = stub_provenance

    with (
        patch("waitbus.sources._attestation._load_attestations_module", return_value=stub_mod),
        pytest.raises(AttestationVerificationError, match="signature mismatch"),
    ):
        verify_distribution(dist)


def test_verify_distribution_raises_on_malformed_provenance_envelope(tmp_path: Path) -> None:
    """A Provenance.model_validate_json failure surfaces as AttestationVerificationError."""
    sha256 = "2" * 64
    direct_url = _direct_url_payload(project="bad_plugin", version="1.0", wheel_tags="py3-none-any", sha256=sha256)
    _, dist = _build_dist_info(
        tmp_path,
        project_name="bad_plugin",
        direct_url_payload=direct_url,
        provenance_json="not valid json {",
    )

    class _StubAttestationError(Exception):
        pass

    class _StubVerificationError(_StubAttestationError):
        pass

    stub_mod = MagicMock()
    stub_mod.AttestationError = _StubAttestationError
    stub_mod.VerificationError = _StubVerificationError
    stub_mod.Provenance.model_validate_json.side_effect = _StubAttestationError("malformed envelope")

    with (
        patch("waitbus.sources._attestation._load_attestations_module", return_value=stub_mod),
        pytest.raises(AttestationVerificationError, match="malformed envelope"),
    ):
        verify_distribution(dist)


# ---------------------------------------------------------------------------
# _format_publisher_identity per-kind tests
# ---------------------------------------------------------------------------


def _publisher(kind: str, **attrs: Any) -> Any:
    """Build a stub publisher object with the given kind and extra attributes."""
    pub = MagicMock()
    pub.kind = kind
    for attr, value in attrs.items():
        setattr(pub, attr, value)
    return pub


def test_format_publisher_identity_github_form() -> None:
    """GitHub Trusted Publisher formats as '<repo> @ <workflow>'."""
    pub = _publisher("GitHub", repository="org/repo", workflow=".github/workflows/x.yml")
    assert _format_publisher_identity(pub) == "org/repo @ .github/workflows/x.yml"


def test_format_publisher_identity_gitlab_form() -> None:
    """GitLab Trusted Publisher formats as 'gitlab:<repo> @ <workflow_filepath>'."""
    pub = _publisher("GitLab", repository="group/project", workflow_filepath=".gitlab-ci.yml")
    assert _format_publisher_identity(pub) == "gitlab:group/project @ .gitlab-ci.yml"


def test_format_publisher_identity_google_form() -> None:
    """Google Trusted Publisher formats as 'google:<email>'."""
    pub = _publisher("Google", email="deployer@project.iam.gserviceaccount.com")
    assert _format_publisher_identity(pub) == "google:deployer@project.iam.gserviceaccount.com"


def test_format_publisher_identity_circleci_form() -> None:
    """CircleCI Trusted Publisher formats as 'circleci:<project_id>/<pipeline_definition_id>'.

    ``vcs_origin`` and ``vcs_ref`` are informational and may be None for
    webhook-triggered pipelines; the canonical identity is the
    (project_id, pipeline_definition_id) pair the operator registers
    with PyPI.
    """
    pub = _publisher(
        "CircleCI",
        project_id="aaaa-bbbb-cccc",
        pipeline_definition_id="dddd-eeee-ffff",
        vcs_origin="github.com/org/repo",
        vcs_ref="refs/heads/main",
    )
    assert _format_publisher_identity(pub) == "circleci:aaaa-bbbb-cccc/dddd-eeee-ffff"


def test_format_publisher_identity_unknown_kind_raises() -> None:
    """An unrecognised publisher kind raises AttestationVerificationError.

    The previous version of this function silently serialised attributes
    into a fallback identity string. That hid TOFU-allowlist drift if a
    future Trusted Publisher kind landed in pypi_attestations and waitbus
    had no formatter for it. The strict-rejection behaviour forces a
    DEC + code update before any new publisher kind can pin into the
    allowlist.
    """
    pub = _publisher("ActiveState", organization="myorg", project="myproject")
    with pytest.raises(AttestationVerificationError, match="unrecognised Trusted Publisher kind"):
        _format_publisher_identity(pub)
