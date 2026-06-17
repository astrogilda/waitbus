"""Enforce the 1:1 contract between wire ``subscribe_rejected`` reasons and
the typed exceptions ``await_predicate`` raises.

The broadcast token was removed, so the two consumer-facing framed reasons
are ``version`` and ``lag_limit_exceeded``. An unknown or absent reason
falls to the base ``BroadcastConnectionError`` rather than being mislabelled
as a specific failure class. This test pins each consumer-facing wire reason
to its dedicated exception type so a future reason addition that forgets the
mapping fails CI.
"""

from __future__ import annotations

import pytest

from waitbus._broadcast_sub import (
    _REJECT_REASON_EXCEPTIONS,
    BroadcastConnectionError,
    ProtocolVersionError,
    SubscriberLaggedError,
)

# The consumer-facing wire reasons (CONSUMER_API.md §3) each get a dedicated
# typed exception. Internal faults (e.g. ``replay_db_error``) close the socket
# silently and never reach the consumer's reject-dispatch map.
_CONSUMER_FACING: dict[str, type[BroadcastConnectionError]] = {
    "version": ProtocolVersionError,
    "lag_limit_exceeded": SubscriberLaggedError,
}


def test_reject_subclasses_share_broadcast_connection_error_base() -> None:
    for cls in (ProtocolVersionError, SubscriberLaggedError):
        assert issubclass(cls, BroadcastConnectionError)


@pytest.mark.parametrize(("reason", "exc_cls"), sorted(_CONSUMER_FACING.items()))
def test_consumer_facing_reason_maps_to_dedicated_exception(
    reason: str, exc_cls: type[BroadcastConnectionError]
) -> None:
    assert _REJECT_REASON_EXCEPTIONS.get(reason) is exc_cls


def test_production_map_is_exactly_the_consumer_facing_reasons() -> None:
    """No drift: the production map is exactly the consumer-facing reasons,
    each mapped to its dedicated (non-base) exception. Internal faults are
    silent closes and must not appear here."""
    assert _REJECT_REASON_EXCEPTIONS == _CONSUMER_FACING


def test_unknown_reason_falls_to_base() -> None:
    assert _REJECT_REASON_EXCEPTIONS.get("some_future_reason", BroadcastConnectionError) is BroadcastConnectionError
