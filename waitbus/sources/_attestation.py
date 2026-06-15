"""In-process verification of PEP 740 attestations for plugin wheels.

waitbus verifies that a third-party plugin's installed wheel carries a
:pep:`740` digital attestation binding the wheel hash to a publisher
identity (e.g., a GitHub repo + workflow file via Trusted Publishing).
The verified identity feeds into the publisher-bound TOFU policy that
prevents typosquat-shadowing of built-in source names.

The verification is done **in-process** using
:func:`pypi_attestations.Attestation.verify`. Shelling out to the
``pypi-attestations`` CLI would add ~80-200ms subprocess spawn, lose
the structured exceptions (``VerificationError``, ``ConversionError``),
and open an argument-injection seam.

The :mod:`pypi_attestations` dependency lives behind the
``waitbus[plugin-verify]`` optional extra. Operators who manage plugin
trust through the OS package manager / pinned hashes can run waitbus
without the extra; the registry then records each plugin as
unverified and the operator must explicitly add it to the allowlist
by name. Importing this module without the extra raises
:class:`AttestationToolingMissingError` lazily so the import path
stays usable for type-only usage.

Verification algorithm (per PEP 740 + PEP 610):

1. Locate the dist-info directory for the installed plugin.
2. Read ``direct_url.json`` (PEP 610) for the wheel filename + SHA-256
   digest. The digest is the cryptographic anchor: without an
   independent source for the wheel's hash, we cannot detect a wheel
   substitution attack (where an attacker swaps the legitimate wheel
   for a malicious one but preserves the legitimate attestation
   file). ``direct_url.json`` is the only post-install source for
   the original wheel digest that pip writes; it is present when
   the user installed via URL / VCS / local-path / ``--find-links``,
   and absent when the user ran ``pip install <name>`` (pip's PEP
   610 covers only the non-name install paths). For PyPI-name
   installs we cannot cross-check the wheel; verification gracefully
   returns ``None`` with a structured log warning, and the operator
   can opt in to allowlist-only trust per plugin.
3. Locate the ``<wheel-stem>.provenance`` file inside the dist-info
   directory (single file per PEP 740, NOT a ``provenance/``
   subdirectory containing loose ``.attestation`` files).
4. Deserialise the file as :class:`pypi_attestations.Provenance`
   (NOT :class:`Attestation` directly -- the on-disk envelope is
   the ``Provenance`` shape that wraps one or more attestation
   bundles).
5. For each :class:`AttestationBundle` in the envelope, the bundle's
   :attr:`publisher` field is the verification identity. Call
   :meth:`Attestation.verify` against each attestation in the
   bundle, using the bundle's publisher as the ``identity`` argument
   (passing ``identity=None`` is rejected by sigstore).
6. On the first successful verification, return a
   :class:`VerifiedPublisher` derived from the bundle's publisher.
"""

from __future__ import annotations

import importlib
import json
import logging
from importlib.metadata import Distribution
from pathlib import Path
from typing import Any, Final

import msgspec

_log = logging.getLogger("waitbus.sources.attestation")


class AttestationToolingMissingError(RuntimeError):
    """The ``waitbus[plugin-verify]`` extra is not installed.

    Raised when :func:`verify_distribution` is called but the
    :mod:`pypi_attestations` / :mod:`sigstore` import fails. The
    message points the operator at the canonical install command so
    the failure is self-healing.
    """


class AttestationVerificationError(RuntimeError):
    """The plugin wheel's attestation failed verification.

    Wraps :class:`pypi_attestations.VerificationError` and
    :class:`pypi_attestations.ConversionError` so the caller does
    not need to import the underlying library to except-clause
    against verification failures. Raised only when the underlying
    crypto check fails -- structural problems (missing wheel on
    disk, no ``direct_url.json`` hash, missing provenance file)
    return ``None`` instead so the daemon can log + continue.
    """


class VerifiedPublisher(msgspec.Struct, frozen=True, kw_only=True):
    """Outcome of a successful PEP 740 attestation verification.

    Attributes:
        publisher_kind: The Trusted-Publisher kind reported by
            ``pypi_attestations`` (``"GitHub"``, ``"GitLab"``,
            ``"Google"``, ``"CircleCI"``). String to avoid importing
            the upstream type into the waitbus public surface.
        publisher_identity: The canonical identity string -- e.g.
            ``"astrogilda/waitbus-circleci @ .github/workflows/release.yml"``
            for GitHub Trusted Publishers. Stable across plugin
            version upgrades; used as the TOFU-pin key.
        predicate_type: The in-toto predicate type the attestation
            carries (``"https://docs.pypi.org/attestations/publish/v1"``
            for the implicit publish attestation; SLSA Provenance v1
            URI for richer attestations).
    """

    publisher_kind: str
    publisher_identity: str
    predicate_type: str


# Filename pip writes for PEP 610 direct-URL metadata. Present when
# the install was from URL / VCS / local file / --find-links; absent
# for plain ``pip install <name>`` resolutions.
_DIRECT_URL_FILENAME: Final[str] = "direct_url.json"


def _load_attestations_module() -> Any:
    """Lazy-import :mod:`pypi_attestations`.

    Raised as :class:`AttestationToolingMissingError` if the import
    fails so callers can match on a waitbus-owned exception type
    without depending on the upstream library. The error message
    names the install command so the operator can self-heal.
    """
    try:
        return importlib.import_module("pypi_attestations")
    except ImportError as exc:
        raise AttestationToolingMissingError(
            "pypi_attestations is not installed; install the optional extra "
            "to enable in-process PEP 740 attestation verification: "
            "`pip install 'waitbus[plugin-verify]'`"
        ) from exc


def dist_info_dir(dist: Distribution) -> Path | None:
    """Return the ``.dist-info`` directory for an installed distribution.

    Public helper used by both the verifier and the ``waitbus source show``
    CLI verb (which displays the on-disk attestation JSON without
    re-running cryptographic verification). The ``.dist-info`` directory
    is the canonical anchor for everything PEP 376 / PEP 610 / PEP 740
    records about an installed wheel.

    Returns ``None`` when the distribution does not expose a file list
    (broken installs, missing RECORD), or when the dist-info parent
    cannot be located on disk (editable installs that point at a
    source tree without dist-info materialisation).
    """
    files = dist.files
    if files is None:
        return None
    # The wheel's ``METADATA`` lives at ``<pkg>-<ver>.dist-info/METADATA``;
    # the parent gives us the dist-info directory whose siblings carry
    # the PEP 610 + PEP 740 sidecars.
    for record in files:
        if record.name == "METADATA":
            located = record.locate()
            if located is not None:
                return Path(located).parent
    return None


def read_attestation_json(dist: Distribution) -> str | None:
    """Return the raw ``<wheel-stem>.provenance`` JSON for ``dist``, if any.

    Public helper for ``waitbus source show`` to render the on-disk
    attestation envelope without re-running cryptographic verification.
    Returns ``None`` when the distribution has no dist-info on disk OR
    no provenance file. Raises no exceptions; broken JSON is returned
    verbatim so the caller can decide whether to display or warn.
    """
    info_dir = dist_info_dir(dist)
    if info_dir is None:
        return None
    provenance_files = sorted(info_dir.glob("*.provenance"))
    if not provenance_files:
        return None
    return provenance_files[0].read_text(encoding="utf-8")


def _read_wheel_descriptor(info_dir: Path) -> tuple[str, str] | None:
    """Return ``(wheel_filename, sha256_hex)`` from ``direct_url.json``, if available.

    PEP 610 ``direct_url.json`` is the only post-install source for
    the original wheel SHA-256 that pip writes. It is present when
    the install was from URL / VCS / local-path / ``--find-links``,
    and absent for ``pip install <name>`` (a documented gap in PEP
    610). Returns ``None`` when the file is absent or does not
    record an ``archive_info.hash`` value of the expected
    ``sha256=<hex>`` form.

    The wheel filename is derived from the URL's path component
    (its basename), which pip preserves verbatim from the source
    PyPI / index URL.
    """
    direct_url = info_dir / _DIRECT_URL_FILENAME
    if not direct_url.exists():
        return None
    try:
        payload = json.loads(direct_url.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    url = payload.get("url")
    archive_info: dict[str, Any] = payload.get("archive_info") or {}
    raw_hash = archive_info.get("hash")
    if not isinstance(url, str) or not isinstance(raw_hash, str):
        return None
    if not raw_hash.startswith("sha256="):
        # PEP 610 uses ``<algorithm>=<hex>``; waitbus only accepts SHA-256
        # because PEP 740 attestations bind a SHA-256 subject.
        return None
    sha256_hex = raw_hash[len("sha256=") :]
    # Wheel filename is the URL's path basename.
    wheel_filename = url.rsplit("/", 1)[-1]
    if not wheel_filename.endswith(".whl") and not wheel_filename.endswith(".tar.gz"):
        return None
    return wheel_filename, sha256_hex


def verify_distribution(dist: Distribution) -> VerifiedPublisher | None:
    """Verify the PEP 740 attestation bound to ``dist``'s wheel, if any.

    Returns:
        :class:`VerifiedPublisher` on successful cryptographic verification.
        ``None`` when the distribution carries no attestation OR when the
        wheel digest cannot be cross-checked (PyPI-name installs without
        ``direct_url.json``, editable installs without dist-info).
        Operators who do not use PEP 740 still get a working registry;
        the allowlist policy decides whether to load such plugins.

    Raises:
        :class:`AttestationToolingMissingError`: when the
            ``waitbus[plugin-verify]`` extra is not installed.
        :class:`AttestationVerificationError`: when an attestation
            is present and the wheel digest is known, but Sigstore-
            backed verification fails.
    """
    attestations_mod = _load_attestations_module()
    info_dir = dist_info_dir(dist)
    if info_dir is None:
        # No locatable dist-info -- can happen for editable installs
        # or distributions with broken RECORD files. Treat as
        # unverifiable rather than an error so the operator can still
        # opt-in via the allowlist.
        _log.info(
            "no dist-info on disk for %s; skipping attestation verification "
            "(operator must add explicit allowlist entry if trust is required)",
            dist.name,
        )
        return None

    provenance_files = sorted(info_dir.glob("*.provenance"))
    if not provenance_files:
        # No PEP 740 attestation in the dist-info -- common for plugins
        # not yet adopting Trusted Publishing. Returning None here means
        # the registry will record the plugin as unverified; TOFU then
        # treats it as "no prior pin = no challenge".
        return None

    descriptor = _read_wheel_descriptor(info_dir)
    if descriptor is None:
        # The wheel was installed via ``pip install <name>`` from PyPI
        # (no direct_url.json), OR direct_url.json is malformed. We
        # cannot cross-check the wheel's digest against the
        # attestation's signed subject without an independent digest
        # source, so verification would either trivially pass (against
        # the attestation's own self-claimed digest) or reject any
        # value we synthesise. Document the gap and return None so
        # the operator can opt-in via the allowlist if they have
        # out-of-band trust.
        _log.warning(
            "%s has a PEP 740 attestation but no direct_url.json wheel "
            "digest; cannot cross-check the wheel against the signed "
            "subject. Install plugins via `pip install --find-links` "
            "(which writes direct_url.json) for in-process verification, "
            "or pre-add the publisher to "
            "$XDG_CONFIG_HOME/waitbus/plugins.allowlist.toml for trust.",
            dist.name,
        )
        return None

    wheel_filename, sha256_hex = descriptor
    pypi_distribution = attestations_mod.Distribution(name=wheel_filename, digest=sha256_hex)

    for provenance_path in provenance_files:
        try:
            provenance_json = provenance_path.read_text(encoding="utf-8")
        except OSError as exc:
            _log.warning("could not read %s: %s", provenance_path, exc)
            continue
        try:
            provenance = attestations_mod.Provenance.model_validate_json(provenance_json)
        except attestations_mod.AttestationError as exc:
            # Malformed envelope -- typed as ConversionError or a
            # pydantic ValidationError under the AttestationError
            # base. Treat as verification failure (the file is
            # supposed to be a well-formed Provenance envelope).
            raise AttestationVerificationError(
                f"failed to deserialise {provenance_path.name} as a Provenance envelope: {exc}"
            ) from exc

        for bundle in provenance.attestation_bundles:
            for attestation in bundle.attestations:
                try:
                    predicate_type, _predicate = attestation.verify(
                        identity=bundle.publisher,
                        dist=pypi_distribution,
                        staging=False,
                        offline=False,
                    )
                except attestations_mod.VerificationError as exc:
                    raise AttestationVerificationError(
                        f"PEP 740 verification failed for {dist.name} ({provenance_path.name}): {exc}"
                    ) from exc
                except attestations_mod.AttestationError as exc:
                    # ConversionError or other non-verify error from
                    # inside the attestation library; surface as a
                    # verify failure so the daemon's TOFU policy
                    # treats it as untrusted.
                    raise AttestationVerificationError(
                        f"PEP 740 attestation processing failed for {dist.name} ({provenance_path.name}): {exc}"
                    ) from exc

                return VerifiedPublisher(
                    publisher_kind=str(bundle.publisher.kind),
                    publisher_identity=_format_publisher_identity(bundle.publisher),
                    predicate_type=str(predicate_type),
                )

    return None


def _format_publisher_identity(publisher: Any) -> str:
    """Render a ``pypi_attestations`` Publisher object as a TOFU-key string.

    The format ``"<repo> @ <workflow>"`` for GitHub Trusted Publishers
    is the canonical operator-visible identity. Each Publisher kind
    gets an explicit branch so the allowlist file stores a stable,
    kind-disambiguated string. Unknown kinds raise
    :class:`AttestationVerificationError` because an identity we
    cannot canonicalise must not silently slip into the TOFU
    allowlist as e.g. ``"unknown:..."`` -- the allowlist is the
    operator's only audit trail for who is allowed to publish each
    source.
    """
    kind = getattr(publisher, "kind", None)
    if kind == "GitHub":
        return f"{publisher.repository} @ {publisher.workflow}"
    if kind == "GitLab":
        return f"gitlab:{publisher.repository} @ {publisher.workflow_filepath}"
    if kind == "Google":
        return f"google:{publisher.email}"
    if kind == "CircleCI":
        # CircleCI's Trusted Publisher identity is the
        # ``(project_id, pipeline_definition_id)`` pair the operator
        # registers with PyPI. ``vcs_origin`` is informational and
        # may be ``None`` for webhook-triggered pipelines, so it is
        # not part of the canonical identity.
        return f"circleci:{publisher.project_id}/{publisher.pipeline_definition_id}"
    raise AttestationVerificationError(
        f"unrecognised Trusted Publisher kind {kind!r}; the waitbus allowlist "
        "needs an explicit branch for each Publisher kind so identity "
        "strings stay stable across plugin upgrades."
    )


__all__ = [
    "AttestationToolingMissingError",
    "AttestationVerificationError",
    "VerifiedPublisher",
    "dist_info_dir",
    "read_attestation_json",
    "verify_distribution",
]
