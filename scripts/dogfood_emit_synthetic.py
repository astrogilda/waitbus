"""Emit one synthetic github workflow_run event for the dogfood-wait smoke.

Used by ``.github/workflows/dogfood-wait.yml`` to inject a matching
event into the broadcast daemon so the ``waitbus wait`` invocation
running in parallel sees a frame and returns zero. Kept as a separate
file (instead of inline ``python -c`` in YAML) so the multi-line
emit body does not collide with YAML's parsing rules.
"""

from __future__ import annotations

import time

from waitbus import _emit as emit_mod
from waitbus._types import EventInsert


def main() -> int:
    emit_mod.emit(
        EventInsert(
            delivery_id=f"dogfood-{time.time_ns()}",
            source="github",
            event_type="workflow_run",
            owner="astrogilda",
            repo="waitbus",
            received_at=time.time_ns(),
            payload_json="{}",
            ingest_method="dogfood",
            status="completed",
            conclusion="success",
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
