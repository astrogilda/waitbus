"""HERO cross-harness swarm demo: two different real agent frameworks, one bus.

Two genuinely different agent frameworks (Pydantic AI + LangGraph) run as
SEPARATE OS processes subscribed to one local broadcast daemon; a peer fails and
a peer on the OTHER framework -- plus a live ``waitbus top`` view -- react to the
failure broadcast, all offline (fake models) with deterministic, supervised
teardown. See :mod:`examples.hero_swarm.orchestrate` and the directory README.
"""

from __future__ import annotations

from examples.hero_swarm.orchestrate import HeroResult, run_hero_demo

__all__ = ["HeroResult", "run_hero_demo"]
