"""Single-source structured-JSON log helper shared by every daemon module.

`structured(logger, level, event, **fields)` emits one compact JSON line on
the given stdlib logger. The `ts` field is unix-epoch-seconds float
(preserved for grep/jq pipelines that key on epoch numerics rather than
ISO strings); `default=str` lets callers log arbitrary payload values
(bytes, exception objects, frame dicts) without a custom encoder.

This module replaces the four near-identical inline `_log` helpers that
previously lived in `_db.py`, `listener.py`, `broadcast.py`, and
`etag_poll.py`. structlog was evaluated and declined for v0.2.0: measured
+8.16 MiB RSS / +102 ms cold start on Python 3.13.5 against the daemon's
20 MiB budget, with no operator-visible JSON-shape win (structlog 25.5.0).
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any


def structured(logger: logging.Logger, level: int, event: str, **fields: Any) -> None:
    """Emit one structured JSON log line on the given logger.

    Compact-separator JSON keeps each line under typical journald
    line-length thresholds; ``default=str`` lets callers log arbitrary
    payload values (bytes, exception objects, frame dicts) without a
    custom encoder.
    """
    record: dict[str, Any] = {"ts": time.time(), "event": event}
    record.update(fields)
    logger.log(level, json.dumps(record, separators=(",", ":"), default=str))
