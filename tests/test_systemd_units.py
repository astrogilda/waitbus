"""Structural lints for the shipped systemd-user units.

These tests fail any unit that drifts from the shape waitbus guarantees
to operators: every Documentation= URI resolves on the open web (no
file:// references to an author's home directory), every ExecStart=
path uses the relocatable `%h/.local/bin/` form, and no directive
embeds a hardcoded `/home/<user>/` substring.

All assertions run as fast filesystem reads with no subprocess calls,
so each parametrised case completes well under 100 ms.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_SYSTEMD_DIR = Path(__file__).parents[1] / "systemd"
_MANIFEST_PATH = _SYSTEMD_DIR / "MANIFEST.txt"
_COMMON_CONF = _SYSTEMD_DIR / "waitbus-common.conf"

_UNIT_FILES = sorted(p for p in _SYSTEMD_DIR.iterdir() if p.suffix in (".service", ".socket", ".timer"))

_DOC_URL_RE = re.compile(r"^(https://github\.com/[^\s]+|man:[^\s]+|info:[^\s]+|https://[^\s]+\.readthedocs\.io[^\s]*)$")
_ABSOLUTE_HOME_RE = re.compile(r"/home/[a-z][^/\s]*/")
_PATH_DIRECTIVES = ("WorkingDirectory=", "ReadWritePaths=", "BindReadOnlyPaths=")
_ALLOWED_PATH_PREFIXES = ("%h/", "%S/", "%t/", "%E/", "%C/")


def _read_manifest() -> set[str]:
    entries: set[str] = set()
    for line in _MANIFEST_PATH.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        entries.add(stripped)
    return entries


@pytest.mark.parametrize("unit_path", _UNIT_FILES, ids=lambda p: p.name)
def test_documentation_url_is_public(unit_path: Path) -> None:
    text = unit_path.read_text()
    matches = re.findall(r"^Documentation=(.+)$", text, re.MULTILINE)
    for raw in matches:
        for url in raw.split():
            assert _DOC_URL_RE.match(url), (
                f"{unit_path.name}: Documentation= URI {url!r} must be a "
                "public https://github.com/, man:, info:, or readthedocs URL"
            )


@pytest.mark.parametrize("unit_path", _UNIT_FILES, ids=lambda p: p.name)
def test_no_absolute_home_paths(unit_path: Path) -> None:
    text = unit_path.read_text()
    hits = _ABSOLUTE_HOME_RE.findall(text)
    assert not hits, (
        f"{unit_path.name} contains hardcoded /home/<user>/ paths: {hits}. "
        "Use %h/ or %S/ specifiers instead so the unit relocates per operator."
    )


@pytest.mark.parametrize("unit_path", _UNIT_FILES, ids=lambda p: p.name)
def test_unit_appears_in_manifest(unit_path: Path) -> None:
    manifest = _read_manifest()
    assert unit_path.name in manifest, (
        f"{unit_path.name} is on disk but missing from systemd/MANIFEST.txt. "
        "Add it so `waitbus install-systemd --sync` can detect orphans."
    )


@pytest.mark.parametrize("unit_path", _UNIT_FILES, ids=lambda p: p.name)
def test_execstart_uses_relocatable_path(unit_path: Path) -> None:
    text = unit_path.read_text()
    for line in text.splitlines():
        if not line.startswith("ExecStart="):
            continue
        value = line.split("=", 1)[1].strip()
        # ExecStart=/bin/bash -c '...' is acceptable for the forward unit
        # because /bin/bash is a system-managed path, not a per-operator
        # one. Anything else under /home or %h/.local/ must use %h/.
        if value.startswith("/bin/") or value.startswith("/usr/bin/"):
            continue
        assert value.startswith("%h/.local/bin/"), (
            f"{unit_path.name}: ExecStart={value!r} must start with "
            "%h/.local/bin/ (or /bin/, /usr/bin/) so the unit resolves "
            "across pip --user, uv tool install, and pipx layouts."
        )


@pytest.mark.parametrize("unit_path", _UNIT_FILES, ids=lambda p: p.name)
def test_path_directives_use_specifiers_only(unit_path: Path) -> None:
    text = unit_path.read_text()
    for line in text.splitlines():
        stripped = line.strip()
        if not any(stripped.startswith(d) for d in _PATH_DIRECTIVES):
            continue
        value = stripped.split("=", 1)[1].strip()
        for token in value.split():
            assert token.startswith(_ALLOWED_PATH_PREFIXES), (
                f"{unit_path.name}: {stripped!r} contains token {token!r} "
                f"that does not start with one of {_ALLOWED_PATH_PREFIXES}. "
                "Absolute paths break relocatable installs."
            )


def test_manifest_lists_only_existing_units() -> None:
    on_disk = {p.name for p in _UNIT_FILES}
    manifest = _read_manifest()
    stale = manifest - on_disk
    assert not stale, (
        f"systemd/MANIFEST.txt references files that no longer exist: "
        f"{sorted(stale)}. Remove them so `--sync` does not flag false orphans."
    )


def test_forward_unit_slug_decoder_preserves_underscores() -> None:
    """The forwarder's instance specifier must round-trip a slug whose
    owner and repo each contain an underscore. The previous decoder used
    bash's ${1/_//} substitution, which replaced only the first
    underscore and therefore mangled slugs like `org_dev/my_repo` into
    `org/dev_my_repo`. The current encoding relies on systemd's `%I`
    specifier (which decodes `\\x2f` back to `/`) and passes the slug
    through `gh webhook forward --repo=` unmodified."""
    forward = (_SYSTEMD_DIR / "waitbus-forward@.service").read_text()
    # Inspect only directive lines (skip comments) so that historical
    # references to the old `${1/_//}` decoder in commentary do not
    # trigger the regression check.
    directive_text = "\n".join(line for line in forward.splitlines() if not line.lstrip().startswith("#"))
    # `%I` (capital I) decodes systemd-escape sequences such as `\x2f`
    # back to `/`; `%i` (lowercase) leaves them escaped. The unit must
    # use %I so a slug like `org_dev\x2fmy_repo` arrives at gh as the
    # literal `org_dev/my_repo`.
    assert "%I" in directive_text, (
        "waitbus-forward@.service must reference %I (capital, decoded) "
        "so escaped slashes in the instance specifier round-trip to '/'."
    )
    assert "${1/_//}" not in directive_text and "${1/_/}" not in directive_text, (
        "waitbus-forward@.service must not use the bash ${1/_//} "
        "single-replacement decoder; that path mangles repo slugs where "
        "the owner or repository name contains an underscore."
    )
    # The systemd specifier expansion `%I` decodes `\x2f` back to `/`,
    # so a slug like `org_dev\x2fmy_repo` arrives at bash as the
    # positional argument `org_dev/my_repo`. The ExecStart must forward
    # that argument to gh as-is via `--repo="$1"` (doubled to `$$1`
    # because systemd treats `$` specially).
    # ExecStart is a multi-line block joined by trailing backslashes.
    # Look at the whole directive_text (comments stripped) for the
    # `--repo="$1"` substring; `$` is doubled to `$$` because systemd
    # treats `$` specially.
    assert "ExecStart=" in directive_text, "waitbus-forward@.service missing ExecStart="
    assert '--repo="$$1"' in directive_text, (
        "ExecStart must pass the (already decoded) %I-derived positional "
        'argument to gh as --repo="$1" so the slug arrives intact.'
    )
    # The ExecStart is multi-line via backslash continuations; the last
    # line of that block carries the `_ %I` terminator that gives bash
    # the decoded slug as $1.
    lines = forward.splitlines()
    last_with_specifier = max(i for i, ln in enumerate(lines) if "%I" in ln)
    assert lines[last_with_specifier].rstrip().endswith("%I"), (
        "The ExecStart block must terminate with `_ %I` so bash receives the decoded slug as $1."
    )


def test_common_conf_directives_inline_in_long_lived_units() -> None:
    """Every directive declared in `waitbus-common.conf` must appear
    verbatim in each long-lived `.service` unit. Modern systemd has no
    runtime include mechanism, so the common file is enforced as a
    code-review reference instead of a loadable drop-in."""
    common_text = _COMMON_CONF.read_text()
    common_directives = {
        line.strip()
        for line in common_text.splitlines()
        if line.strip() and not line.lstrip().startswith("#") and not line.strip().startswith("[")
    }
    long_lived = {
        "waitbus-listener.service",
        "waitbus-broadcast.service",
    }
    for unit_name in long_lived:
        unit_path = _SYSTEMD_DIR / unit_name
        unit_text = unit_path.read_text()
        unit_lines = {line.strip() for line in unit_text.splitlines()}
        missing = common_directives - unit_lines
        assert not missing, (
            f"{unit_name} is missing hardening directives from "
            f"systemd/waitbus-common.conf: {sorted(missing)}. "
            "Update the unit in lockstep with the reference file."
        )
