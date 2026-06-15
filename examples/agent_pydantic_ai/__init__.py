"""Pydantic AI + waitbus integration example (real agent, fake model, real bus).

See :mod:`examples.agent_pydantic_ai.agent` and the directory README.
"""

from __future__ import annotations

from examples.agent_pydantic_ai.agent import (
    EventCapture,
    WaitbusDeps,
    build_agent,
    run,
)

__all__ = ["EventCapture", "WaitbusDeps", "build_agent", "run"]
