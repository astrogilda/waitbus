"""Source-tree-only helper scripts and shared lint modules.

This package is intentionally NOT installed (not in ``waitbus/``
and not listed in ``[tool.hatch.build.targets.wheel].packages``). The
``__init__.py`` exists solely so tests can import shared helpers via the ``pythonpath = ["."]`` pytest
config, without per-file ``sys.path`` mutation.

Standalone-script invocation (``python scripts/sync-versions.py``,
``python scripts/derive_poll_costs.py``, etc.) continues to work as before;
package mode and script mode coexist.
"""
