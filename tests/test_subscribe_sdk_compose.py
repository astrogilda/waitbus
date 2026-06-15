"""Tests for cross-source clause composition on the public subscribe SDK.

Mirrors ``test_subscribe_sdk.py``'s shape: the ``running_daemon`` fixture
runs the daemon in the test's event loop, so the synchronous ``wait_for`` /
``subscribe`` calls run on a worker thread. The eager-validation matrix
needs no daemon at all.
"""

from __future__ import annotations

import contextlib
import sys
import threading
from pathlib import Path
from typing import Any

import pytest

from tests._agent_harness import insert_event_row
from tests._daemon_helpers import (
    await_subscribers as _await_subscribers,
)
from tests._daemon_helpers import (
    await_thread as _await_thread,
)
from waitbus import _subscribe as sdk
from waitbus import broadcast
from waitbus._subscribe import EventFrame

pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="broadcast daemon SO_PEERCRED check is Linux-only",
)

_PYTEST_CLAUSE = 'pytest:fields.event_type="pytest_session"'
_DOCKER_CLAUSE = 'docker:fields.event_type="docker_container"'


# --- wait_for(all_of=...) -----------------------------------------------------


@pytest.mark.asyncio
async def test_wait_for_all_of_returns_the_completing_frame(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    """Two clauses completed by two DIFFERENT events; the returned frame is
    the one that satisfied the LAST outstanding clause."""
    daemon, paths = running_daemon
    result: list[EventFrame | None] = []

    def run() -> None:
        result.append(
            sdk.wait_for(
                all_of=[_PYTEST_CLAUSE, _DOCKER_CLAUSE],
                timeout=3.0,
                socket_path=str(paths["broadcast"]),
            )
        )

    t = threading.Thread(target=run, daemon=True)
    t.start()
    await _await_subscribers(daemon)
    insert_event_row(paths["db"], "sc-1", source="pytest", event_type="pytest_session", repo="pytest")
    insert_event_row(paths["db"], "sc-2", source="docker", event_type="docker_container")
    await _await_thread(t)
    assert not t.is_alive(), "wait_for(all_of=...) hung"
    event = result[0]
    assert isinstance(event, EventFrame)
    assert event.delivery_id == "sc-2"
    assert event.fields.get("source") == "docker"


@pytest.mark.asyncio
async def test_wait_for_all_of_timeout_with_outstanding_clause_returns_none(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    daemon, paths = running_daemon
    result: list[EventFrame | None] = []

    def run() -> None:
        result.append(
            sdk.wait_for(
                all_of=[_PYTEST_CLAUSE, _DOCKER_CLAUSE],
                timeout=0.4,
                socket_path=str(paths["broadcast"]),
            )
        )

    t = threading.Thread(target=run, daemon=True)
    t.start()
    await _await_subscribers(daemon)
    # Only one clause is ever satisfied -> the conjunction times out.
    insert_event_row(paths["db"], "sc-3", source="docker", event_type="docker_container")
    await _await_thread(t)
    assert not t.is_alive()
    assert result == [None]


# --- wait_for(first_of=...) ---------------------------------------------------


@pytest.mark.asyncio
async def test_wait_for_first_of_returns_the_earlier_of_two(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    daemon, paths = running_daemon
    result: list[EventFrame | None] = []

    def run() -> None:
        result.append(
            sdk.wait_for(
                first_of=[_PYTEST_CLAUSE, _DOCKER_CLAUSE],
                timeout=3.0,
                socket_path=str(paths["broadcast"]),
            )
        )

    t = threading.Thread(target=run, daemon=True)
    t.start()
    await _await_subscribers(daemon)
    insert_event_row(paths["db"], "sc-4", source="docker", event_type="docker_container")
    insert_event_row(paths["db"], "sc-5", source="pytest", event_type="pytest_session", repo="pytest")
    await _await_thread(t)
    assert not t.is_alive()
    event = result[0]
    assert isinstance(event, EventFrame)
    assert event.delivery_id == "sc-4"


# --- subscribe(first_of=...) --------------------------------------------------


@pytest.mark.asyncio
async def test_subscribe_first_of_streams_events_from_both_sources(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    daemon, paths = running_daemon
    got: list[EventFrame] = []

    def run() -> None:
        stream = sdk.subscribe(
            first_of=[_DOCKER_CLAUSE, _PYTEST_CLAUSE],
            socket_path=str(paths["broadcast"]),
        )
        with contextlib.closing(stream) as events:
            for ev in events:
                got.append(ev)
                if len(got) >= 2:
                    break

    t = threading.Thread(target=run, daemon=True)
    t.start()
    await _await_subscribers(daemon)
    insert_event_row(paths["db"], "sc-6", source="docker", event_type="docker_container")
    insert_event_row(paths["db"], "sc-7", source="pytest", event_type="pytest_session", repo="pytest")
    await _await_thread(t)
    assert not t.is_alive()
    assert {e.fields.get("source") for e in got} == {"docker", "pytest"}


@pytest.mark.asyncio
async def test_asubscribe_first_of_yields_matching_event(
    running_daemon: tuple[broadcast.Broadcast, dict[str, Path]],
) -> None:
    import asyncio

    daemon, paths = running_daemon
    got: list[EventFrame] = []

    async def consume() -> None:
        async for ev in sdk.asubscribe(
            first_of=[_DOCKER_CLAUSE, _PYTEST_CLAUSE],
            socket_path=str(paths["broadcast"]),
        ):
            got.append(ev)
            break

    task = asyncio.create_task(consume())
    await asyncio.sleep(0)
    await _await_subscribers(daemon)
    insert_event_row(paths["db"], "sc-8", source="docker", event_type="docker_container")
    await asyncio.wait_for(task, timeout=4.0)
    assert len(got) == 1
    assert got[0].delivery_id == "sc-8"


# --- eager validation (no daemon needed) --------------------------------------


def test_wait_for_all_of_and_first_of_together_raises() -> None:
    with pytest.raises(ValueError, match="mutually exclusive"):
        sdk.wait_for(all_of=[_PYTEST_CLAUSE], first_of=[_DOCKER_CLAUSE])


@pytest.mark.parametrize(
    "kwargs",
    [
        {"match": 'fields.x="y"'},
        {"source": "docker"},
        {"to": "agent_b"},
    ],
)
def test_wait_for_all_of_rejects_single_source_kwargs(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="cannot be combined with match/source/to"):
        sdk.wait_for(all_of=[_PYTEST_CLAUSE], **kwargs)


def test_wait_for_first_of_rejects_source_kwarg() -> None:
    with pytest.raises(ValueError, match="cannot be combined with match/source/to"):
        sdk.wait_for(first_of=[_DOCKER_CLAUSE], source="docker")


def test_subscribe_first_of_rejects_match_kwarg() -> None:
    with pytest.raises(ValueError, match="cannot be combined with match/source/to"):
        next(iter(sdk.subscribe(match='fields.x="y"', first_of=[_DOCKER_CLAUSE])))


@pytest.mark.asyncio
async def test_asubscribe_first_of_rejects_to_kwarg() -> None:
    with pytest.raises(ValueError, match="cannot be combined with match/source/to"):
        async for _ in sdk.asubscribe(to="agent_b", first_of=[_DOCKER_CLAUSE]):
            pass


def test_wait_for_empty_clause_list_raises() -> None:
    with pytest.raises(ValueError, match="clause list must be non-empty"):
        sdk.wait_for(all_of=[])


def test_wait_for_malformed_clause_raises_valueerror() -> None:
    with pytest.raises(ValueError, match="source:key=json_literal"):
        sdk.wait_for(first_of=["bare_word"])


def test_wait_for_bad_clause_source_raises_valueerror() -> None:
    with pytest.raises(ValueError, match="clause source must match"):
        sdk.wait_for(all_of=['fields.x="a:b"'])
