"""``python -m scripts.soak`` entry-point.

Two-line delegation to ``_main.main`` so the package supports both
``python -m scripts.soak [...]`` (CI workflow + Hetzner run script) and
direct programmatic use (``from scripts.soak._main import main``) without
re-export aliases at the package top level.
"""

from __future__ import annotations

import sys

from scripts.soak._main import main

if __name__ == "__main__":
    sys.exit(main())
