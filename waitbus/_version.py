"""Single source of truth for the package version at runtime."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _version

try:
    PACKAGE_VERSION: str = _version("waitbus")
except PackageNotFoundError:  # pragma: no cover — editable install fallback
    PACKAGE_VERSION = "0.0.0+unknown"
