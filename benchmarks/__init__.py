"""waitbus benchmark suite.

Per-source TTFAE benches + per-source polling baselines + cross-source
predicate-wait + tokens-saved + idle RSS + throughput. Methodology is
documented in :doc:`BENCHMARKING.md`; the load-bearing rules
(open-loop scheduling, Wilson-rank-binomial CIs on percentile order
statistics, gc-disabled companion runs) are enforced by the shared
harness in :mod:`benchmarks._harness`.

This package is NOT included in the sdist (the
``[tool.hatch.build.targets.sdist].only-include`` whitelist in
``pyproject.toml`` excludes it). Bench scripts are for maintainers
and CI; pip-install users do not need them.
"""
