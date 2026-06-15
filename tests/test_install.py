"""Wheel-install integration test.

Builds the project wheel, installs it into a fresh venv, then asserts:
1. Every console-script in pyproject.toml [project.scripts] resolves to
   <venv>/bin/<name> and is executable.
2. Every systemd unit declared in [tool.hatch.build.targets.wheel.shared-data]
   landed at <venv>/share/systemd/user/<name>.
3. `systemd-analyze --user verify` passes on every shipped unit.

The shared-data placement is load-bearing for `waitbus install-systemd` to
locate canonical units via sysconfig.get_path('data'). If this test breaks,
operators on `uv tool install waitbus` will have a broken systemd install.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tomllib
import venv  # fallback for non-uv environments
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent

pytestmark = [
    pytest.mark.slow,
    pytest.mark.serial,
    pytest.mark.skipif(sys.platform != "linux", reason="systemd-analyze is Linux-only"),
]


def _clean_env() -> dict[str, str]:
    """Subprocess environment with noise-reduction flags."""
    env = os.environ.copy()
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["PIP_DISABLE_PIP_VERSION_CHECK"] = "1"
    return env


@pytest.fixture(scope="module")
def built_wheel(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Build the project wheel once for the entire module."""
    dist_dir = tmp_path_factory.mktemp("dist")
    env = _clean_env()

    if shutil.which("uv"):
        # uv build is faster; --no-sources prevents pulling from index
        result = subprocess.run(
            ["uv", "build", "--wheel", "--out-dir", str(dist_dir)],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            env=env,
        )
    else:
        # Skip if neither uv nor the `build` package is available
        try:
            subprocess.run(
                [sys.executable, "-c", "import build"],
                check=True,
                capture_output=True,
            )
        except subprocess.CalledProcessError:
            pytest.skip("Neither 'uv' nor the 'build' package is available; install build>=1.2")
        result = subprocess.run(
            [sys.executable, "-m", "build", "--wheel", "--outdir", str(dist_dir), str(PROJECT_ROOT)],
            capture_output=True,
            text=True,
            env=env,
        )

    if result.returncode != 0:
        pytest.fail(f"Wheel build failed (exit {result.returncode}):\nstdout: {result.stdout}\nstderr: {result.stderr}")

    wheels = list(dist_dir.glob("waitbus-*.whl"))
    assert len(wheels) == 1, f"Expected exactly one wheel, found: {wheels}"
    return wheels[0]


@pytest.fixture(scope="module")
def installed_venv(tmp_path_factory: pytest.TempPathFactory, built_wheel: Path) -> Path:
    """Create a fresh venv and install the wheel into it.

    Uses `uv venv` + `uv pip install` when uv is available (avoids libpython
    shared-lib issues with uv-managed Python runtimes). Falls back to the
    stdlib `venv` module + subprocess pip otherwise.
    """
    venv_dir = tmp_path_factory.mktemp("venv")
    env = _clean_env()

    if shutil.which("uv"):
        # uv handles its own managed-Python runtimes correctly
        result = subprocess.run(
            ["uv", "venv", str(venv_dir)],
            capture_output=True,
            text=True,
            env=env,
        )
        if result.returncode != 0:
            pytest.fail(f"uv venv failed (exit {result.returncode}):\nstdout: {result.stdout}\nstderr: {result.stderr}")
        result = subprocess.run(
            ["uv", "pip", "install", "--python", str(venv_dir / "bin" / "python"), str(built_wheel)],
            capture_output=True,
            text=True,
            env=env,
        )
    else:
        venv.create(str(venv_dir), with_pip=True)
        pip = venv_dir / "bin" / "pip"
        result = subprocess.run(
            [str(pip), "install", str(built_wheel)],
            capture_output=True,
            text=True,
            env=env,
        )

    if result.returncode != 0:
        pytest.fail(
            f"Wheel install failed (exit {result.returncode}):\nstdout: {result.stdout}\nstderr: {result.stderr}"
        )
    return venv_dir


def _expected_scripts() -> list[str]:
    """Read console-script names from pyproject.toml [project.scripts]."""
    data = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text())
    return list(data["project"]["scripts"].keys())


def _expected_units() -> list[str]:
    """Read systemd unit filenames from pyproject.toml shared-data entries."""
    data = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text())
    shared: dict[str, str] = data["tool"]["hatch"]["build"]["targets"]["wheel"]["shared-data"]
    return [
        Path(dest).name
        for dest in shared.values()
        if dest.startswith("share/systemd/user/") and Path(dest).suffix in {".service", ".socket", ".timer"}
    ]


def test_wheel_creates_console_scripts(installed_venv: Path) -> None:
    """All [project.scripts] entries must appear as executable files in <venv>/bin/."""
    bin_dir = installed_venv / "bin"
    scripts = _expected_scripts()
    assert scripts, "pyproject.toml [project.scripts] is empty — check the fixture"
    for script in scripts:
        path = bin_dir / script
        assert path.exists(), f"Console-script missing: {path}"
        assert os.access(path, os.X_OK), f"Console-script not executable: {path}"


def test_wheel_ships_systemd_units(installed_venv: Path) -> None:
    """All shared-data systemd units must land at <venv>/share/systemd/user/."""
    units_dir = installed_venv / "share" / "systemd" / "user"
    assert units_dir.is_dir(), (
        f"Shared-data systemd directory missing: {units_dir}\n"
        "Check [tool.hatch.build.targets.wheel.shared-data] in pyproject.toml."
    )
    units = _expected_units()
    assert units, "No .service/.socket/.timer entries found in pyproject.toml shared-data"
    for unit in units:
        assert (units_dir / unit).exists(), f"Unit missing: {units_dir / unit}"


def test_shipped_units_pass_systemd_analyze(installed_venv: Path) -> None:
    """systemd-analyze --user verify must exit 0 on every shipped unit file.

    The unit ExecStart directives use ``%h/.local/bin/waitbus-*``, which
    systemd-analyze resolves to ``~/.local/bin/``.  That directory is the
    install target for ``pip install --user`` and ``uv tool install``, but in
    CI the binaries live under the test venv.  We create temporary symlinks at
    ``~/.local/bin/waitbus-*`` pointing into the venv's ``bin/`` and remove
    them unconditionally after the assertion.
    """
    if not shutil.which("systemd-analyze"):
        pytest.skip("systemd-analyze binary not on PATH")

    venv_bin = installed_venv / "bin"
    local_bin = Path.home() / ".local" / "bin"
    local_bin.mkdir(parents=True, exist_ok=True)

    scripts = _expected_scripts()
    created_links: list[Path] = []
    try:
        for script in scripts:
            link = local_bin / script
            target = venv_bin / script
            if not link.exists() and target.exists():
                link.symlink_to(target)
                created_links.append(link)

        units_dir = installed_venv / "share" / "systemd" / "user"
        unit_paths = [str(units_dir / unit) for unit in _expected_units()]

        result = subprocess.run(
            ["systemd-analyze", "--user", "verify", *unit_paths],
            capture_output=True,
            text=True,
        )
        # systemd-analyze exits 0 on clean. Common false-positives like
        # "Failed to resolve user 'something'" are allowed via stderr but
        # exit_code must be 0.
        assert result.returncode == 0, (
            f"systemd-analyze --user verify failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
        )
    finally:
        for link in created_links:
            link.unlink(missing_ok=True)
