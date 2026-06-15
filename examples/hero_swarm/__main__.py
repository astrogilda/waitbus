"""Package entry point: ``python -m examples.hero_swarm [<role> ...]``.

Kept as a distinct module from ``orchestrate`` so that running the demo via
``-m`` does not re-execute an already-imported module (which would emit a
runpy double-import ``RuntimeWarning`` that surfaces in the recorded demo).
``orchestrate.main`` parses ``sys.argv`` and dispatches to the orchestrator
(no args) or to a child role.
"""

from __future__ import annotations

import sys

from examples.hero_swarm.orchestrate import main

if __name__ == "__main__":
    sys.exit(main())
