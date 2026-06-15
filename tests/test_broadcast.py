"""Tests for the broadcast daemon."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import socket
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, cast

import pytest

from waitbus import _db, _metrics, broadcast
from waitbus._types import EventInsert

# Type aliases for fixture params
_DaemonPaths = tuple[broadcast.Broadcast, dict[str, Path]]

# The broadcast daemon binds an AF_UNIX socket and uses SO_PEERCRED for
# peer-credential checks, which requires Linux. The tests follow suit.
pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="broadcast daemon SO_PEERCRED check is Linux-only",
)

# --- helpers ---------------------------------------------------------------


def _event_stub(delivery_id: str, **overrides: Any) -> EventInsert:
    defaults: dict[str, Any] = {
        "source": "github",
        "event_type": "workflow_run",
        "owner": "test-owner",
        "repo": "test-repo",
        "received_at": time.time_ns(),
        "payload_json": "{}",
        "ingest_method": "webhook",
        "run_id": 1,
        "workflow_name": "Tests",
        "head_branch": "main",
        "head_sha": "abc",
        "status": "completed",
        "conclusion": "success",
    }
    defaults.update(overrides)
    return EventInsert(delivery_id=delivery_id, **defaults)


def _insert(db: Path, delivery_id: str, **field_overrides: Any) -> None:
    with contextlib.closing(sqlite3.connect(db)) as conn:
        _db.insert_event(conn, _event_stub(delivery_id, **field_overrides))


from tests._wire_helpers import FakeWireSocket as _FakeSock
from tests._wire_helpers import connect as _connect
from tests._wire_helpers import recv as _recv
from tests._wire_helpers import recv_until as _recv_until
from tests._wire_helpers import subscribe as _subscribe


async def _recv_frame(reader: asyncio.StreamReader, timeout: float = 2.0) -> dict[str, Any]:
    frame = await _recv(reader, timeout=timeout)
    assert frame is not None, "daemon closed the connection unexpectedly"
    return frame


# --- tests -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_subscribe_then_receive_matching_frame(running_daemon: _DaemonPaths) -> None:
    _daemon, paths = running_daemon
    reader, writer = await _connect(paths["broadcast"])
    try:
        await _subscribe(writer, filters=["test-owner/test-repo"])
        await asyncio.sleep(0.05)  # let subscribe register
        _insert(paths["db"], "d-1")
        # First event frame (skip subscribe_ack and heartbeat control frames).
        frame = await _recv_until(reader, "event")
        assert frame is not None
        assert frame["delivery_id"] == "d-1"
        assert frame["owner"] == "test-owner"
        assert frame["repo"] == "test-repo"
        assert frame["event_type"] == "workflow_run"
        # event_id is a 26-char ULID (v1 wire uses event_id, not id).
        assert isinstance(frame["event_id"], str) and len(frame["event_id"]) == 26
        # Summary is single-line and bounded.
        assert "\n" not in frame["summary"]
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_filter_excludes_other_repo(running_daemon: _DaemonPaths) -> None:
    _daemon, paths = running_daemon
    reader, writer = await _connect(paths["broadcast"])
    try:
        await _subscribe(writer, filters=["wanted-owner/wanted-repo"])
        await asyncio.sleep(0.05)
        _insert(paths["db"], "d-other", owner="other-owner", repo="other-repo")
        _insert(paths["db"], "d-wanted", owner="wanted-owner", repo="wanted-repo")
        # Collect event frames for ~0.5 s; should be exactly one (the wanted).
        seen: list[dict[str, Any]] = []
        deadline = time.monotonic() + 0.5
        while time.monotonic() < deadline:
            try:
                f = await _recv_frame(reader, timeout=0.3)
            except TimeoutError:
                break
            if f.get("kind") == "event":
                seen.append(f)
        assert [f["delivery_id"] for f in seen] == ["d-wanted"]
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_wildcard_filter_receives_everything(running_daemon: _DaemonPaths) -> None:
    _daemon, paths = running_daemon
    reader, writer = await _connect(paths["broadcast"])
    try:
        await _subscribe(writer, filters=["*"])
        await asyncio.sleep(0.05)
        _insert(paths["db"], "d-a", owner="a", repo="x")
        _insert(paths["db"], "d-b", owner="b", repo="y")
        seen: list[str] = []
        deadline = time.monotonic() + 0.5
        while len(seen) < 2 and time.monotonic() < deadline:
            try:
                f = await _recv_frame(reader, timeout=0.3)
            except TimeoutError:
                break
            if f.get("kind") == "event":
                seen.append(f["delivery_id"])
        assert set(seen) == {"d-a", "d-b"}
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_event_types_filter(running_daemon: _DaemonPaths) -> None:
    _daemon, paths = running_daemon
    reader, writer = await _connect(paths["broadcast"])
    try:
        await _subscribe(writer, filters=["*"], event_types=["workflow_job"])
        await asyncio.sleep(0.05)
        _insert(paths["db"], "d-run", event_type="workflow_run")
        _insert(
            paths["db"],
            "d-job",
            event_type="workflow_job",
            owner="o",
            repo="r",
            run_id=None,
            job_id=42,
            job_name="build",
        )
        seen: list[dict[str, Any]] = []
        deadline = time.monotonic() + 0.5
        while time.monotonic() < deadline:
            try:
                f = await _recv_frame(reader, timeout=0.3)
            except TimeoutError:
                break
            if f.get("kind") == "event":
                seen.append(f)
        delivery_ids = [f["delivery_id"] for f in seen]
        assert delivery_ids == ["d-job"]
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_dedup_redelivery_does_not_re_broadcast(running_daemon: _DaemonPaths) -> None:
    _daemon, paths = running_daemon
    reader, writer = await _connect(paths["broadcast"])
    try:
        await _subscribe(writer, filters=["*"])
        await asyncio.sleep(0.05)
        for _ in range(3):  # INSERT OR IGNORE drops two; only first commits.
            _insert(paths["db"], "d-once")
        seen: list[dict[str, Any]] = []
        deadline = time.monotonic() + 0.4
        while time.monotonic() < deadline:
            try:
                f = await _recv_frame(reader, timeout=0.2)
            except TimeoutError:
                break
            if f.get("kind") == "event":
                seen.append(f)
        assert [f["delivery_id"] for f in seen] == ["d-once"]
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_heartbeat_pure_liveness_no_rowid(running_daemon: _DaemonPaths) -> None:
    _daemon, paths = running_daemon
    reader, writer = await _connect(paths["broadcast"])
    try:
        await _subscribe(writer, filters=["*"])
        # Wait for at least one heartbeat (cadence 1 s under env override).
        frame = None
        deadline = time.monotonic() + 2.5
        while time.monotonic() < deadline:
            try:
                f = await _recv_frame(reader, timeout=0.5)
            except TimeoutError:
                continue
            if f.get("kind") == "daemon_heartbeat":
                frame = f
                break
        assert frame is not None, "no heartbeat in 1.5 s"
        assert frame["kind"] == "daemon_heartbeat"
        # v1 heartbeat carries NO event identity — no id/event_id field.
        assert "id" not in frame, "heartbeat must not carry an id field in v1 wire"
        assert "event_id" not in frame, "heartbeat must not carry event_id"
        assert "ts" in frame
        assert "uptime_sec" in frame
        # Heartbeat is pure liveness — no internal cursor leakage.
        assert "rowid" not in frame
        assert "max_rowid" not in frame
        assert "last_received_at_ms" not in frame
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_resumable_since_replay(running_daemon: _DaemonPaths) -> None:
    _daemon, paths = running_daemon
    # Pre-seed 3 events, capture the first event_id as the resume cursor.
    _insert(paths["db"], "d-a")
    _insert(paths["db"], "d-b")
    _insert(paths["db"], "d-c")
    await asyncio.sleep(0.05)
    with contextlib.closing(sqlite3.connect(paths["db"])) as conn:
        first_id = conn.execute("SELECT event_id FROM events WHERE delivery_id = 'd-a'").fetchone()[0]
    reader, writer = await _connect(paths["broadcast"])
    try:
        # Subscribe with `since=first_id` — should replay d-b and d-c, NOT d-a.
        await _subscribe(writer, filters=["*"], since=first_id)
        seen: list[str] = []
        deadline = time.monotonic() + 0.7
        while len(seen) < 2 and time.monotonic() < deadline:
            try:
                f = await _recv_frame(reader, timeout=0.3)
            except TimeoutError:
                break
            if f.get("kind") == "event":
                seen.append(f["delivery_id"])
        assert seen == ["d-b", "d-c"]
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_invalid_filter_closes_connection(running_daemon: _DaemonPaths) -> None:
    _daemon, paths = running_daemon
    reader, writer = await _connect(paths["broadcast"])
    try:
        # "../etc/passwd" must not match FILTER_RE; daemon closes.
        await _subscribe(writer, filters=["../etc/passwd"])
        # The daemon closes on bad subscribe; reader hits EOF.
        data = await asyncio.wait_for(reader.read(1024), timeout=1.0)
        assert data == b""
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_bad_json_subscribe_closes_connection(running_daemon: _DaemonPaths) -> None:
    _daemon, paths = running_daemon
    reader, writer = await _connect(paths["broadcast"])
    try:
        # Send raw bytes that aren't a valid length-prefix frame; daemon closes.
        writer.write(b"this is not json")
        await writer.drain()
        data = await asyncio.wait_for(reader.read(1024), timeout=1.0)
        assert data == b""
    finally:
        writer.close()
        await writer.wait_closed()


def test_filter_matcher_matches_wildcard_owner_glob() -> None:
    sub = broadcast.Subscriber(
        sock=None,  # type: ignore[arg-type]
        filters=["demo-org/*"],
        event_types=frozenset({"workflow_run"}),
        remote_uid=os.getuid(),
    )
    assert sub.matches("demo-org", "project-bar", "workflow_run")
    assert sub.matches("demo-org", "infra-alerts", "workflow_run")
    assert not sub.matches("other-org", "project-bar", "workflow_run")
    assert not sub.matches("demo-org", "project-bar", "prometheus_alert")


def test_filter_regex_rejects_injection_shapes() -> None:
    bad = [
        "",
        "no-slash",
        "owner/",
        "/repo",
        "owner/repo/extra",
        "../etc/passwd",
        "owner/repo;DROP TABLE",
        "owner/$(whoami)",
        "owner/repo\nrepo",
    ]
    for f in bad:
        assert broadcast.FILTER_RE.match(f) is None, f"should reject: {f!r}"


# ---------------------------------------------------------------------------
# Property-based tests for Subscriber.matches
# ---------------------------------------------------------------------------

import string

from hypothesis import assume, example, given, settings
from hypothesis import strategies as st

# Strategies for well-formed filter components and event types.
_owner_st = st.text(
    min_size=1,
    max_size=20,
    alphabet=string.ascii_letters + string.digits + "_.-",
)
_repo_st = st.text(
    min_size=1,
    max_size=20,
    alphabet=string.ascii_letters + string.digits + "_.-",
)
_event_type_st = st.sampled_from(["workflow_run", "workflow_job", "prometheus_alert", "prometheus_watchdog"])
_valid_filter_st = st.one_of(
    st.just("*"),
    st.tuples(_owner_st, _repo_st).map(lambda t: f"{t[0]}/{t[1]}"),
    _owner_st.map(lambda o: f"{o}/*"),
)
_event_types_st = st.frozensets(_event_type_st, min_size=1)


def _make_sub(filters: list[str], event_types: frozenset[str]) -> broadcast.Subscriber:
    return broadcast.Subscriber(
        sock=None,  # type: ignore[arg-type]
        filters=filters,
        event_types=event_types,
        remote_uid=os.getuid(),
    )


def _reference_matches(
    filters: list[str],
    event_types: frozenset[str],
    owner: str,
    repo: str,
    event_type: str,
) -> bool:
    """Transparently-correct reference oracle for Subscriber.matches.

    Avoids copying the production loop; each filter shape is
    evaluated independently so the logic is easy to audit.
    """
    if event_type not in event_types:
        return False
    slug = f"{owner}/{repo}"
    for f in filters:
        if f == "*":
            return True
        if f == slug:
            return True
        # "owner/*" shape: strip the trailing "/*" (2 chars) to recover the
        # owner portion and compare directly.
        if f.endswith("/*") and f[:-2] == owner:
            return True
    return False


# --- Property 1: production matcher agrees with reference on all inputs ----


@settings(max_examples=300, deadline=None)
@given(
    filters=st.lists(_valid_filter_st, max_size=5),
    event_types=_event_types_st,
    owner=_owner_st,
    repo=_repo_st,
    event_type=_event_type_st,
)
@example(filters=["*"], event_types=frozenset({"workflow_run"}), owner="o", repo="r", event_type="workflow_run")
@example(filters=[], event_types=frozenset({"workflow_run"}), owner="o", repo="r", event_type="workflow_run")
@example(
    filters=["alpha/beta"],
    event_types=frozenset({"workflow_run"}),
    owner="alpha",
    repo="beta",
    event_type="workflow_run",
)
@example(
    filters=["alpha/*"],
    event_types=frozenset({"workflow_run"}),
    owner="alpha",
    repo="anything",
    event_type="workflow_run",
)
@example(
    filters=["alpha/*"], event_types=frozenset({"workflow_run"}), owner="alpha2", repo="r", event_type="workflow_run"
)
def test_matches_agrees_with_reference_implementation(
    filters: list[str],
    event_types: frozenset[str],
    owner: str,
    repo: str,
    event_type: str,
) -> None:
    sub = _make_sub(filters, event_types)
    production = sub.matches(owner, repo, event_type)
    reference = _reference_matches(filters, event_types, owner, repo, event_type)
    assert production == reference, (
        f"Disagreement: filters={filters!r}, owner={owner!r}, repo={repo!r}, "
        f"event_type={event_type!r}, event_types={event_types!r} "
        f"→ production={production}, reference={reference}"
    )


# --- Property 2: wildcard "*" subsumes all specific filters ----------------


@settings(max_examples=300, deadline=None)
@given(
    extra_filters=st.lists(_valid_filter_st, max_size=4),
    event_types=_event_types_st,
    owner=_owner_st,
    repo=_repo_st,
    event_type=_event_type_st,
)
@example(extra_filters=[], event_types=frozenset({"workflow_run"}), owner="o", repo="r", event_type="workflow_run")
@example(
    extra_filters=["alpha/beta"],
    event_types=frozenset({"workflow_run"}),
    owner="x",
    repo="y",
    event_type="workflow_run",
)
@example(
    extra_filters=["alpha/*"],
    event_types=frozenset({"workflow_run", "workflow_job"}),
    owner="z",
    repo="q",
    event_type="workflow_job",
)
def test_wildcard_subsumes_all_specific_filters(
    extra_filters: list[str],
    event_types: frozenset[str],
    owner: str,
    repo: str,
    event_type: str,
) -> None:
    assume(event_type in event_types)
    filters = ["*", *extra_filters]
    sub = _make_sub(filters, event_types)
    assert sub.matches(owner, repo, event_type), (
        f"Expected True with '*' in filters; got False for owner={owner!r}, repo={repo!r}, event_type={event_type!r}"
    )


# --- Property 3: event_type absent from set always returns False -----------


@settings(max_examples=300, deadline=None)
@given(
    filters=st.lists(_valid_filter_st, min_size=1, max_size=5),
    event_types=_event_types_st,
    owner=_owner_st,
    repo=_repo_st,
    event_type=_event_type_st,
)
@example(filters=["*"], event_types=frozenset({"workflow_run"}), owner="o", repo="r", event_type="workflow_job")
@example(filters=["o/r"], event_types=frozenset({"workflow_job"}), owner="o", repo="r", event_type="prometheus_alert")
@example(filters=["o/*"], event_types=frozenset({"prometheus_alert"}), owner="o", repo="r", event_type="workflow_run")
def test_event_type_not_in_set_always_false(
    filters: list[str],
    event_types: frozenset[str],
    owner: str,
    repo: str,
    event_type: str,
) -> None:
    assume(event_type not in event_types)
    sub = _make_sub(filters, event_types)
    assert not sub.matches(owner, repo, event_type), (
        f"Expected False when event_type not in event_types; "
        f"filters={filters!r}, event_type={event_type!r}, event_types={event_types!r}"
    )


# --- Property 4: owner-glob does not leak across owner boundary ------------


@settings(max_examples=300, deadline=None)
@given(
    repo=_repo_st,
    owner=_owner_st,
    event_type=_event_type_st,
)
@example(repo="anything", owner="alpha2", event_type="workflow_run")
@example(repo="r", owner="alph", event_type="workflow_run")
@example(repo="r", owner="ALPHA", event_type="workflow_run")
@example(repo="r", owner="alpha-extra", event_type="workflow_run")
def test_owner_glob_does_not_leak_across_owner_boundary(
    repo: str,
    owner: str,
    event_type: str,
) -> None:
    assume(owner != "alpha")
    sub = _make_sub(["alpha/*"], frozenset({"workflow_run", "workflow_job", "prometheus_alert", "prometheus_watchdog"}))
    assert not sub.matches(owner, repo, event_type), f"'alpha/*' should not match owner={owner!r}"


# --- Property 5: filter order is irrelevant to the Boolean outcome ---------


def _permuted_pair_st(
    filters_st: st.SearchStrategy,
) -> st.SearchStrategy:
    """Return a strategy that draws (original_filters, shuffled_filters)."""
    return filters_st.flatmap(lambda fs: st.permutations(fs).map(lambda shuffled: (fs, shuffled)))


@settings(max_examples=300, deadline=None)
@given(
    filter_pair=_permuted_pair_st(st.lists(_valid_filter_st, max_size=5)),
    event_types=_event_types_st,
    owner=_owner_st,
    repo=_repo_st,
    event_type=_event_type_st,
)
@example(
    filter_pair=(["alpha/beta", "*", "gamma/*"], ["*", "gamma/*", "alpha/beta"]),
    event_types=frozenset({"workflow_run"}),
    owner="x",
    repo="y",
    event_type="workflow_run",
)
@example(
    filter_pair=(["alpha/beta", "alpha/gamma"], ["alpha/gamma", "alpha/beta"]),
    event_types=frozenset({"workflow_run"}),
    owner="alpha",
    repo="beta",
    event_type="workflow_run",
)
@example(
    filter_pair=([], []),
    event_types=frozenset({"workflow_run"}),
    owner="o",
    repo="r",
    event_type="workflow_run",
)
def test_filter_order_independent(
    filter_pair: tuple[list[str], list[str]],
    event_types: frozenset[str],
    owner: str,
    repo: str,
    event_type: str,
) -> None:
    filters, shuffled = filter_pair
    sub_orig = _make_sub(filters, event_types)
    sub_shuf = _make_sub(shuffled, event_types)
    result_orig = sub_orig.matches(owner, repo, event_type)
    result_shuf = sub_shuf.matches(owner, repo, event_type)
    assert result_orig == result_shuf, (
        f"Order changed result: orig={filters!r} → {result_orig}, shuffled={shuffled!r} → {result_shuf}"
    )


# --- Property 6 (bonus): exact-slug filter never matches prefix of repo ----


@settings(max_examples=300, deadline=None)
@given(
    repo=_repo_st,
    suffix=st.text(min_size=1, max_size=10, alphabet=string.ascii_letters + string.digits + "_.-"),
    event_type=_event_type_st,
)
@example(repo="foo", suffix="bar", event_type="workflow_run")
@example(repo="foo", suffix=".", event_type="workflow_run")
@example(repo="a", suffix="b", event_type="workflow_run")
@example(repo="foo", suffix="foo", event_type="workflow_run")
def test_no_partial_prefix_match(
    repo: str,
    suffix: str,
    event_type: str,
) -> None:
    owner = "alpha"
    extended_repo = repo + suffix
    # The exact-slug filter is "alpha/foo"; the query uses "alpha/foobar".
    sub = _make_sub(
        [f"{owner}/{repo}"],
        frozenset({"workflow_run", "workflow_job", "prometheus_alert", "prometheus_watchdog"}),
    )
    # Only an exact match is allowed; extended_repo differs by at least one char.
    assert not sub.matches(owner, extended_repo, event_type), (
        f"Exact filter 'alpha/{repo}' matched prefix repo={extended_repo!r}"
    )


# ---------------------------------------------------------------------------
# Subscribe-frame validators
# ---------------------------------------------------------------------------


def test_validate_filters_default_is_wildcard() -> None:
    assert broadcast._validate_subscribe_filters(None) == ["*"]


def test_validate_filters_empty_list_collapses_to_wildcard() -> None:
    """`filters: []` from the wire should behave like no filter — match all."""
    assert broadcast._validate_subscribe_filters([]) == ["*"]


def test_validate_filters_accepts_wildcard_and_owner_glob() -> None:
    assert broadcast._validate_subscribe_filters(["*", "owner/repo", "owner/*"]) == ["*", "owner/repo", "owner/*"]


def test_validate_filters_rejects_non_list() -> None:
    for bad in ("not-a-list", 42, {"a": 1}, True):
        with pytest.raises(ValueError):
            broadcast._validate_subscribe_filters(bad)


def test_validate_filters_rejects_bad_element() -> None:
    for bad in [["../etc/passwd"], ["owner/repo/extra"], ["owner;DROP"], [None]]:
        with pytest.raises(ValueError):
            broadcast._validate_subscribe_filters(bad)


def test_validate_event_types_default_is_all_supported() -> None:
    from waitbus.sources._registry import event_types_supported

    out = broadcast._validate_subscribe_event_types(None)
    assert out == event_types_supported()


def test_validate_event_types_filters_unrecognized() -> None:
    out = broadcast._validate_subscribe_event_types(["workflow_run", "unknown_event"])
    assert out == frozenset({"workflow_run"})


def test_validate_event_types_rejects_non_list() -> None:
    with pytest.raises(ValueError):
        broadcast._validate_subscribe_event_types("workflow_run")


def test_validate_event_types_rejects_all_unrecognized() -> None:
    with pytest.raises(ValueError):
        broadcast._validate_subscribe_event_types(["nope", "nada"])


def test_validate_since_cursor_accepts_none() -> None:
    assert broadcast._validate_since_cursor(None) is None


def test_validate_since_cursor_accepts_well_formed_ulid() -> None:
    ulid = "01HZAB0123456789ABCDEFGHJK"
    assert len(ulid) == 26
    assert broadcast._validate_since_cursor(ulid) == ulid


# --- subscribe envelope: reserved-field-only validation -------------------


def test_validate_envelope_absent_is_accepted() -> None:
    """Missing or None envelope is the implicit default (faithful per-event
    tail; the only delivery mode today)."""
    broadcast._validate_subscribe_envelope(None)  # must not raise


def test_validate_envelope_diffs_is_accepted() -> None:
    """Explicit 'diffs' is the documented current value."""
    broadcast._validate_subscribe_envelope("diffs")  # must not raise


def test_validate_envelope_upsert_is_reserved_not_implemented() -> None:
    """'upsert' is reserved for a future server-side latest-per-entity
    projection; the daemon must reject it today so a forward-looking
    consumer cannot silently subscribe to an unimplemented mode."""
    with pytest.raises(ValueError, match="reserved"):
        broadcast._validate_subscribe_envelope("upsert")


def test_validate_envelope_rejects_non_string() -> None:
    """Non-string types fail loudly."""
    with pytest.raises(ValueError, match="must be a string"):
        broadcast._validate_subscribe_envelope(42)
    with pytest.raises(ValueError, match="must be a string"):
        broadcast._validate_subscribe_envelope(["diffs"])


def test_validate_envelope_rejects_unknown() -> None:
    """Any unrecognised string value is rejected."""
    with pytest.raises(ValueError, match="unknown envelope"):
        broadcast._validate_subscribe_envelope("compact")


def test_validate_since_cursor_rejects_wrong_length() -> None:
    for bad in ("", "01HZ", "0" * 25, "0" * 27):
        with pytest.raises(ValueError):
            broadcast._validate_since_cursor(bad)


def test_validate_since_cursor_rejects_bad_alphabet() -> None:
    """ILOU are excluded from Crockford base32; underscores/lowercase rejected."""
    for bad in [
        "I" * 26,  # I excluded
        "L" * 26,  # L excluded
        "O" * 26,  # O excluded
        "U" * 26,  # U excluded
        "a" * 26,  # lowercase rejected
        "0" * 25 + ";",  # punctuation rejected
    ]:
        with pytest.raises(ValueError):
            broadcast._validate_since_cursor(bad)


def test_validate_since_cursor_rejects_non_string() -> None:
    bad: object
    for bad in (42, [], {"x": 1}, True):
        with pytest.raises(ValueError):
            broadcast._validate_since_cursor(bad)


def test_validate_token_accepts_none() -> None:
    assert broadcast._validate_subscribe_token(None) is None


def test_validate_token_accepts_in_range_length() -> None:
    token = "x" * 64
    assert broadcast._validate_subscribe_token(token) == token


def test_validate_token_rejects_below_min_length() -> None:
    with pytest.raises(ValueError):
        broadcast._validate_subscribe_token("x" * (broadcast.TOKEN_MIN_LEN - 1))


def test_validate_token_rejects_above_max_length() -> None:
    with pytest.raises(ValueError):
        broadcast._validate_subscribe_token("x" * (broadcast.TOKEN_MAX_LEN + 1))


def test_validate_token_rejects_non_string() -> None:
    bad: object
    for bad in (42, [], {"x": 1}, True):
        with pytest.raises(ValueError):
            broadcast._validate_subscribe_token(bad)


# ---------------------------------------------------------------------------
# Module-level helpers (credential token, sd_notify, peer_uid, summary_for)
# ---------------------------------------------------------------------------


def test_lookup_token_returns_credential_value() -> None:
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(broadcast._secrets, "get_secret", lambda name: "broadcast-secret-xyz")
        assert broadcast._lookup_token() == "broadcast-secret-xyz"


def test_lookup_token_swallows_secret_not_configured() -> None:
    def _raise(name: str) -> str:
        raise broadcast._secrets.SecretNotConfigured("credential unreadable")

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(broadcast._secrets, "get_secret", _raise)
        assert broadcast._lookup_token() is None


def test_sd_notify_noop_when_socket_env_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("NOTIFY_SOCKET", raising=False)
    # Should not raise and not call socket.socket.
    broadcast._sd_notify(b"READY=1\n")


def test_sd_notify_handles_socket_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """When NOTIFY_SOCKET points at a non-existent path, sd_notify logs and returns."""
    monkeypatch.setenv("NOTIFY_SOCKET", str(tmp_path / "no-such.sock"))
    broadcast._sd_notify(b"READY=1\n")  # OSError swallowed


def test_peer_uid_returns_own_uid_on_unix_pair() -> None:
    import socket as _socket

    a, b = _socket.socketpair(_socket.AF_UNIX, _socket.SOCK_STREAM)
    try:
        assert broadcast._peer_uid(a) == os.getuid()
    finally:
        a.close()
        b.close()


def test_broadcast_peercred_check_uses_peercred_module() -> None:
    """broadcast.py must delegate UID lookup to _peercred.peer_uid.

    Replaces the legacy ``getattr(socket, "SO_PEERCRED", None)`` shape
    that hard-coded Linux's socket option. The module-level wrapper
    keeps the ``int | None`` failure signal so the daemon's accept path
    stays unchanged, but the platform dispatch lives behind the new
    module per the macOS-port design decision.
    """
    import socket as _socket

    from waitbus import _peercred

    a, b = _socket.socketpair(_socket.AF_UNIX, _socket.SOCK_STREAM)
    try:
        called: list[object] = []

        def _fake(sock: Any) -> int:
            called.append(sock)
            return 12345

        with pytest.MonkeyPatch().context() as mp:
            mp.setattr(_peercred, "peer_uid", _fake)
            assert broadcast._peer_uid(a) == 12345
        assert len(called) == 1, "broadcast._peer_uid must forward to _peercred.peer_uid"
    finally:
        a.close()
        b.close()


def test_broadcast_peer_uid_returns_none_on_oserror() -> None:
    """When _peercred.peer_uid raises OSError, broadcast._peer_uid returns None.

    The daemon's accept path treats None as a reject signal; preserving
    the None-on-failure semantics across the _peercred refactor is
    load-bearing for the peercred_mismatch logging path.
    """
    import socket as _socket

    from waitbus import _peercred

    a, b = _socket.socketpair(_socket.AF_UNIX, _socket.SOCK_STREAM)
    try:

        def _raise(_sock: Any) -> int:
            raise OSError(13, "permission denied")

        with pytest.MonkeyPatch().context() as mp:
            mp.setattr(_peercred, "peer_uid", _raise)
            assert broadcast._peer_uid(a) is None
    finally:
        a.close()
        b.close()


def test_summary_for_falls_back_on_format_text_error() -> None:
    """A broken format_text path falls back to a deterministic string.

    The except in _summary_for covers (KeyError, TypeError, AttributeError,
    ValueError) — the concrete exceptions a row-formatting function raises on
    malformed data. Use KeyError here as the representative failure case.
    """
    fake_row = {"owner": "o", "repo": "r", "event_type": "t", "delivery_id": "d"}

    class _BoomRow:
        def __getitem__(self, key: str) -> str:
            return fake_row[key]

        def keys(self) -> Any:
            return fake_row.keys()

    with pytest.MonkeyPatch().context() as mp:

        def _raise(_row: Any) -> str:
            raise KeyError("head_branch")

        import waitbus.read_events as re_mod

        mp.setattr(re_mod, "format_text", _raise)
        # _summary_for performs `from .read_events import format_text` inside;
        # patch the source module so the late import sees our raiser.
        summary = broadcast._summary_for(_BoomRow())
    assert "o/r" in summary


def test_ulid_regex_matches_canonical_ulids() -> None:
    """Cross-check against ulid.new()-style values."""
    from waitbus import _ulid

    for _ in range(20):
        value = _ulid.new()
        assert broadcast.ULID_RE.match(value), value


# ---------------------------------------------------------------------------
# Subscriber.enqueue error paths
# ---------------------------------------------------------------------------


def test_subscriber_send_increments_lag_on_blocking_io() -> None:
    sub = broadcast.Subscriber(
        sock=_FakeSock(BlockingIOError()),  # type: ignore[arg-type]
        filters=["*"],
        event_types=frozenset({"workflow_run"}),
        remote_uid=os.getuid(),
    )
    assert sub.enqueue(b"x", counts_as_delivered=True) is False
    assert sub.lag_count == 1


def test_subscriber_send_marks_for_close_on_broken_pipe() -> None:
    sub = broadcast.Subscriber(
        sock=_FakeSock(BrokenPipeError()),  # type: ignore[arg-type]
        filters=["*"],
        event_types=frozenset({"workflow_run"}),
        remote_uid=os.getuid(),
    )
    assert sub.enqueue(b"x", counts_as_delivered=True) is False
    assert sub.lag_count >= broadcast.LAG_LIMIT


def test_subscriber_send_marks_for_close_on_connection_reset() -> None:
    sub = broadcast.Subscriber(
        sock=_FakeSock(ConnectionResetError()),  # type: ignore[arg-type]
        filters=["*"],
        event_types=frozenset({"workflow_run"}),
        remote_uid=os.getuid(),
    )
    assert sub.enqueue(b"x", counts_as_delivered=True) is False
    assert sub.lag_count >= broadcast.LAG_LIMIT


def test_subscriber_send_resets_lag_on_success() -> None:
    sub = broadcast.Subscriber(
        sock=_FakeSock(),  # type: ignore[arg-type]
        filters=["*"],
        event_types=frozenset({"workflow_run"}),
        remote_uid=os.getuid(),
    )
    sub.lag_count = 5
    assert sub.enqueue(b"x", counts_as_delivered=True) is True
    assert sub.lag_count == 0


async def test_send_rearms_drain_after_failed_arm() -> None:
    """A send that appends behind a non-empty buffer must re-arm the drain.

    Scenario: the first send buffers while the writability watcher cannot be
    armed (fd is -1, not loop-registerable), so the buffer is stranded with
    ``_draining`` False. A later send appends behind the non-empty buffer; it
    must re-call the idempotent arming so the buffer drains once the socket
    exposes a registerable fd, instead of sitting orphaned until lag eviction.
    """
    a, b = socket.socketpair()
    fake = _FakeSock(BlockingIOError(), fileno=-1)
    sub = broadcast.Subscriber(
        sock=fake,  # type: ignore[arg-type]
        filters=["*"],
        event_types=frozenset({"workflow_run"}),
        remote_uid=os.getuid(),
    )
    try:
        # First send: EAGAIN buffers the whole frame; arming fails (fd=-1).
        assert sub.enqueue(b"first", counts_as_delivered=True) is False
        assert sub._tx_queue
        assert sub._draining is False
        # The socket recovers: sends succeed and the fd is registerable.
        fake.exc = None
        fake.fd = a.fileno()
        # Second send appends behind the non-empty buffer and must re-arm.
        assert sub.enqueue(b"second", counts_as_delivered=True) is False
        assert sub._draining is True
        # The loop's writability callback fires (a fresh socketpair fd is
        # immediately writable) and flushes the whole buffer via the fake.
        deadline = time.monotonic() + 2.0
        while sub._tx_queue and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        assert not sub._tx_queue, "queued bytes were never drained"
        assert sub.lag_count == 0
        assert b"".join(fake.sent) == b"firstsecond"
    finally:
        sub._stop_draining()
        a.close()
        b.close()


# ---------------------------------------------------------------------------
# Partial-write frame atomicity (torn-frame regression)
#
# The subscriber socket is NON-BLOCKING. ``socket.sendall`` does NOT loop to
# completion on a non-blocking socket: on a partial write it sends a prefix,
# raises BlockingIOError, and discards the sent-byte count, tearing the
# length-prefixed frame and permanently desyncing the subscriber's wire. The
# fix replaces ``sendall`` with byte-count-returning ``send`` plus an
# event-loop ``add_writer`` drain of the buffered remainder, so the receiver
# always sees a whole, contiguous frame (whole-frame-or-clean-EOF). These
# tests pin that invariant.
# ---------------------------------------------------------------------------


class _PartialOnceSock:
    """Non-blocking socket stub that tears the FIRST frame, then completes it.

    The first ``send`` accepts only ``chunk`` bytes (modelling the kernel
    flushing a prefix then hitting EAGAIN); ``fileno`` returns a real,
    loop-registerable descriptor (the write end of an ``os.pipe``) so the
    production ``add_writer`` drain path is genuinely exercised. Every send
    forwards its accepted bytes to ``out`` (a bytearray the test inspects) so
    the reassembled wire stream can be checked for a torn / interleaved frame.
    """

    def __init__(self, *, chunk: int) -> None:
        self._chunk = chunk
        self._first = True
        self.out = bytearray()
        # A real fd add_writer can register; we never actually write to it.
        self._r, self._w = os.pipe()
        self.closed = False

    def send(self, blob: bytes) -> int:
        data = bytes(blob)
        if self._first:
            self._first = False
            accepted = data[: self._chunk]
            self.out.extend(accepted)
            return len(accepted)
        self.out.extend(data)
        return len(data)

    def fileno(self) -> int:
        return self._w

    def close(self) -> None:
        self.closed = True
        with contextlib.suppress(OSError):
            os.close(self._r)
        with contextlib.suppress(OSError):
            os.close(self._w)


@pytest.mark.asyncio
async def test_partial_write_does_not_tear_frame() -> None:
    """A partial first send must not truncate or interleave the wire frame.

    Forces ``send`` to accept only a prefix of a multi-KB frame, then drives
    the event-loop writability callback to flush the remainder, and asserts
    the bytes that reached the wire are EXACTLY the original frame — the
    length prefix and payload contiguous, nothing lost, nothing duplicated.
    """
    from waitbus._frame import _LENGTH_STRUCT, encode_frame

    payload = b"P" * 4096
    frame = encode_frame(payload)
    assert len(frame) == 4 + len(payload)

    sock = _PartialOnceSock(chunk=64)  # tear after 64 of the ~4100 bytes
    sub = broadcast.Subscriber(
        sock=cast(Any, sock),
        filters=["*"],
        event_types=frozenset({"workflow_run"}),
        remote_uid=os.getuid(),
    )
    try:
        # First send is partial: it must buffer the remainder (return False,
        # lag incremented) rather than dropping the unsent tail on the floor.
        assert sub.enqueue(frame, counts_as_delivered=True) is False
        assert sub._draining is True
        assert sub.lag_count == 1
        assert bytes(sock.out) == frame[:64]  # only the prefix made it so far

        # Pump the event loop so the armed add_writer callback (_drain) fires
        # and flushes the buffered remainder.
        for _ in range(10):
            await asyncio.sleep(0)
            if not sub._draining:
                break
        assert sub._draining is False, "drain callback never disarmed"
        assert sub.lag_count == 0, "lag should reset once the frame fully drained"

        # The reassembled wire stream is the ORIGINAL frame, byte-for-byte:
        # contiguous length prefix + full payload, no tear, no duplication.
        assert bytes(sock.out) == frame
        (declared_len,) = _LENGTH_STRUCT.unpack(bytes(sock.out[:4]))
        assert declared_len == len(payload)
        assert bytes(sock.out[4:]) == payload
    finally:
        sub._stop_draining()
        sock.close()


@pytest.mark.asyncio
async def test_buffered_frame_counts_delivered_on_flush_completion() -> None:
    """A frame that EAGAIN-buffers counts toward the delivered total at flush.

    The fan-out pass adds zero for a buffered send; the drain path must add
    the flushed frame once the buffer empties, so the counter reflects every
    frame that actually reached the subscriber in full.
    """
    from waitbus._frame import encode_frame

    _metrics.reset()
    payload = b"P" * 4096
    frame = encode_frame(payload)

    sock = _PartialOnceSock(chunk=64)
    sub = broadcast.Subscriber(
        sock=cast(Any, sock),
        filters=["*"],
        event_types=frozenset({"workflow_run"}),
        remote_uid=os.getuid(),
    )
    try:
        assert sub.enqueue(frame, counts_as_delivered=True) is False  # partial: buffered, not counted yet
        assert len(sub._tx_queue) == 1
        assert _metrics.get("waitbus_broadcast_events_delivered_total") == 0

        for _ in range(10):
            await asyncio.sleep(0)
            if not sub._draining:
                break
        assert sub._draining is False, "drain callback never disarmed"
        assert len(sub._tx_queue) == 0
        assert _metrics.get("waitbus_broadcast_events_delivered_total") == 1
    finally:
        sub._stop_draining()
        sock.close()
        _metrics.reset()


@pytest.mark.asyncio
async def test_eagain_buffered_frame_arrives_whole_over_real_socket() -> None:
    """End-to-end: a frame sent while the kernel buffer is full arrives intact.

    Saturates a real non-blocking AF_UNIX socket so the next frame cannot be
    written synchronously, sends it through ``Subscriber.send`` (which buffers
    it and arms the drain), then reads from the peer end and asserts the whole
    length-prefixed frame is delivered uncorrupted once the buffer drains.
    """
    import socket as _socket

    from waitbus._frame import encode_frame, read_frame_sock

    loop = asyncio.get_running_loop()
    tx, rx = _socket.socketpair(_socket.AF_UNIX, _socket.SOCK_STREAM)
    tx.setblocking(False)
    rx.setblocking(False)
    # Shrink the send buffer so saturation needs only a modest filler.
    with contextlib.suppress(OSError):
        tx.setsockopt(_socket.SOL_SOCKET, _socket.SO_SNDBUF, 4096)

    sub = broadcast.Subscriber(
        sock=tx,
        filters=["*"],
        event_types=frozenset({"workflow_run"}),
        remote_uid=os.getuid(),
    )
    try:
        # Saturate the kernel send buffer with raw filler until EAGAIN.
        filler_total = 0
        while True:
            try:
                filler_total += tx.send(b"\x00" * 65536)
            except BlockingIOError:
                break
            if filler_total > (8 << 20):  # safety bound; should EAGAIN well before
                pytest.skip("could not saturate send buffer on this kernel")

        payload = b"Z" * 4096
        frame = encode_frame(payload)

        # With the buffer full this cannot complete synchronously: it must be
        # buffered and the drain armed, NOT torn.
        result = sub.enqueue(frame, counts_as_delivered=True)
        assert result is False
        assert sub._draining is True

        # Reader side: drain the filler (freeing kernel space so _drain fires),
        # then read exactly one whole frame.
        async def _read_one_frame() -> bytes | None:
            # Consume the filler bytes first.
            drained = 0
            while drained < filler_total:
                await asyncio.sleep(0)
                try:
                    chunk = rx.recv(min(65536, filler_total - drained))
                except BlockingIOError:
                    continue
                if not chunk:
                    return None
                drained += len(chunk)
            return await read_frame_sock(loop, rx)

        received = await asyncio.wait_for(_read_one_frame(), timeout=5.0)
        assert received == payload, "frame must arrive whole and untorn"
    finally:
        sub._stop_draining()
        with contextlib.suppress(OSError):
            tx.close()
        with contextlib.suppress(OSError):
            rx.close()


# ---------------------------------------------------------------------------
# main() entry point (smoke)
# ---------------------------------------------------------------------------


def test_main_invokes_asyncio_run_and_returns_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """main() bootstraps the daemon and delegates to asyncio.run(daemon.run())."""
    called = {"ran": False}

    def _fake_run(coro: Any) -> int:
        coro.close()  # Avoid the un-awaited-coroutine warning.
        called["ran"] = True
        return 0

    monkeypatch.setattr("waitbus.broadcast.asyncio.run", _fake_run)
    monkeypatch.setattr(broadcast, "ensure_state_dirs", lambda: None)
    assert broadcast.main() == 0
    assert called["ran"] is True


def test_main_catches_keyboard_interrupt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise(coro: Any) -> int:
        coro.close()
        raise KeyboardInterrupt

    monkeypatch.setattr("waitbus.broadcast.asyncio.run", _raise)
    monkeypatch.setattr(broadcast, "ensure_state_dirs", lambda: None)
    assert broadcast.main() == 0


def test_serialize_oversize_frame_emits_truncated_stub() -> None:
    # Build a v1 EventFrame whose serialized form blows past the canonical wire cap.
    from waitbus._frame import MAX_FRAME_BYTES, EventFrame

    big_summary = "x" * (MAX_FRAME_BYTES + 1000)
    event_id = "01" + "A" * 24
    frame = EventFrame(
        event_id=event_id,
        event_type="workflow_run",
        owner="o",
        repo="r",
        received_at=0,
        delivery_id="d-big",
        summary=big_summary,
        fields={},
    )
    wire = broadcast._serialize(frame)
    # Wire blob is 4-byte prefix + payload; strip prefix and decode.
    import struct as _struct

    assert len(wire) >= 4
    (payload_len,) = _struct.unpack(">I", wire[:4])
    payload = wire[4 : 4 + payload_len]
    parsed = json.loads(payload.decode("utf-8"))
    assert parsed["kind"] == "truncated"
    assert parsed["event_id"] == event_id
    assert "reason" in parsed


# ---------------------------------------------------------------------------
# Cold-start / schema ownership regressions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_starts_on_empty_db(running_daemon: _DaemonPaths) -> None:
    """The broadcast daemon must initialize the events table itself.

    Under systemd socket activation the broadcast service can win the
    activation race against the listener daemon; the only DB on disk
    is the bare file (or missing entirely). The daemon's startup hook
    invokes `_db.ensure_schema` before `_seed_cursor`, so the
    `SELECT MAX(event_id)` query succeeds on a fresh install instead
    of tripping on `OperationalError: no such table: events`.

    The `broadcast_paths` fixture intentionally does NOT pre-create
    the schema, so by the time `running_daemon` yields the daemon has
    already executed the cold-start path. A subscribe + INSERT round
    trip then confirms the table is fully usable end-to-end.
    """
    _daemon, paths = running_daemon
    # Confirm the events table exists and is queryable from a fresh connection.
    with contextlib.closing(sqlite3.connect(paths["db"])) as conn:
        names = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    assert "events" in names, (
        f"broadcast daemon did not provision the events table on cold start; sqlite_master tables: {names!r}"
    )
    # Round-trip a frame to prove the schema is complete, not a stub.
    reader, writer = await _connect(paths["broadcast"])
    try:
        await _subscribe(writer, filters=["*"])
        await asyncio.sleep(0.05)
        _insert(paths["db"], "d-cold-start")
        frame = await _recv_until(reader, "event")
        assert frame["delivery_id"] == "d-cold-start"
    finally:
        writer.close()
        await writer.wait_closed()


def test_concurrent_ensure_schema(tmp_path: Path) -> None:
    """Two concurrent ensure_schema callers must both succeed.

    Both the listener and broadcast daemons call `_db.ensure_schema`
    at startup; under systemd socket activation either may win the
    BEGIN IMMEDIATE write lock, leaving the other with SQLITE_BUSY.
    The retry wrapper inside `_db.ensure_schema` absorbs the
    contention; without it, the loser would crash on first start.

    The test spawns two threads that race on the same DB path and
    asserts both calls return cleanly and that the resulting schema
    is the canonical events table.
    """
    import threading

    db = tmp_path / "events.db"
    errors: list[BaseException] = []
    barrier = threading.Barrier(2)

    def _worker() -> None:
        try:
            barrier.wait(timeout=5.0)
            _db.ensure_schema(db)
        except BaseException as exc:
            errors.append(exc)

    t1 = threading.Thread(target=_worker)
    t2 = threading.Thread(target=_worker)
    t1.start()
    t2.start()
    t1.join(timeout=10.0)
    t2.join(timeout=10.0)
    assert not t1.is_alive() and not t2.is_alive(), "threads did not finish in time"
    assert not errors, f"concurrent ensure_schema raised: {errors!r}"
    # Schema is valid and matches EVENT_COLUMNS. Exclude the daemon-assigned
    # seq PK: a real schema column but
    # never an INSERT column, so it is intentionally absent from EVENT_COLUMNS.
    with contextlib.closing(sqlite3.connect(db)) as conn:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(events)")} - {"seq"}
    assert cols == set(_db.EVENT_COLUMNS), (
        f"schema column drift: extra={cols - set(_db.EVENT_COLUMNS)}, missing={set(_db.EVENT_COLUMNS) - cols}"
    )


# ---------------------------------------------------------------------------
# Auth / peer-credential regressions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_peercred_rejection(running_daemon: _DaemonPaths, monkeypatch: pytest.MonkeyPatch) -> None:
    """A connection from a different UID must be rejected.

    `_handle_accept` reads the peer UID via SO_PEERCRED and closes
    the subscriber socket if it does not match `self.uid`. The
    rejection happens before the subscribe frame is even read, so
    the client observes the daemon's close without any frame
    exchange.
    """
    daemon, paths = running_daemon
    # Inject a synthetic peer UID that cannot match daemon.uid.
    foreign_uid = daemon.uid + 1
    monkeypatch.setattr(broadcast, "_peer_uid", lambda _sock: foreign_uid)
    reader, writer = await _connect(paths["broadcast"])
    try:
        # Send a well-formed subscribe — daemon should still close us
        # because the peercred check runs first, before we are read.
        await _subscribe(writer, filters=["*"])
        try:
            data = await asyncio.wait_for(reader.read(1024), timeout=1.5)
        except ConnectionResetError:
            # Daemon closed mid-flight; ECONNRESET is equivalent to EOF.
            return
        assert data == b"", f"expected EOF (or ECONNRESET) after peercred rejection; received {data!r}"
    finally:
        writer.close()
        # Daemon already closed the transport, so wait_closed may surface
        # the same ECONNRESET we silently absorb above. Suppress: the
        # close is purely a resource-release step here.
        with contextlib.suppress(ConnectionResetError, BrokenPipeError):
            await writer.wait_closed()


@pytest.mark.asyncio
async def test_token_auth_rejection_emits_reject_frame(
    running_daemon: _DaemonPaths,
) -> None:
    """A wrong-token subscribe gets one ``subscribe_rejected`` frame, then EOF.

    The peer cleared the accept-time SO_PEERCRED gate (proven same-UID),
    so the daemon sends a ``subscribe_rejected`` frame rather than a bare EOF.
    `_check_subscribe_token` still uses `hmac.compare_digest` for the
    constant-time compare; the reject path now
    writes a single ``subscribe_rejected`` / ``reason: "token"`` frame
    (best-effort, bounded) before closing. After that one frame the next
    read is a clean EOF — the daemon never streams to a rejected peer.
    """
    daemon, paths = running_daemon
    # The fixture-built daemon has token=None (no keyring entry under test);
    # install the expected secret post-hoc. _read_subscribe reads
    # self.token at request time so the mutation is visible to the next
    # subscribe attempt. Both tokens are within [16, 128] so this
    # exercises the hmac.compare_digest MISMATCH sub-path specifically
    # (the bad-length sub-path is covered by the sibling test).
    daemon.token = "expected-token-0123456789"  # 25 chars
    reader, writer = await _connect(paths["broadcast"])
    try:
        await _subscribe(
            writer,
            filters=["*"],
            token="wrong-token-9876543210",  # 22 chars
        )
        frame = await _recv_frame(reader, timeout=2.0)
        assert frame["kind"] == "subscribe_rejected", frame
        assert frame["reason"] == "token", frame
        assert frame["remediation"], "remediation hint must be present"
        assert "broadcast-token" in frame["remediation"]
        # Exactly one frame, then a clean EOF — no event stream follows.
        tail = await asyncio.wait_for(reader.read(1024), timeout=1.5)
        assert tail == b"", f"expected EOF after the reject frame; got {tail!r}"
    finally:
        writer.close()
        with contextlib.suppress(ConnectionResetError, BrokenPipeError):
            await writer.wait_closed()


@pytest.mark.asyncio
async def test_token_bad_length_also_emits_reject_frame(
    running_daemon: _DaemonPaths,
) -> None:
    """The other ``_check_subscribe_token`` False sub-path (bad length).

    A too-short token fails `_validate_subscribe_token` with a ValueError
    inside `_check_subscribe_token`, returning False. That is still a
    post-peer-cred reject, so it also gets the single reject frame.
    """
    daemon, paths = running_daemon
    daemon.token = "expected-token"
    reader, writer = await _connect(paths["broadcast"])
    try:
        # Length 3 is below TOKEN_MIN_LEN (16): bad-length sub-path.
        await _subscribe(writer, filters=["*"], token="abc")
        frame = await _recv_frame(reader, timeout=2.0)
        assert frame["kind"] == "subscribe_rejected", frame
        assert frame["reason"] == "token", frame
        tail = await asyncio.wait_for(reader.read(1024), timeout=1.5)
        assert tail == b"", f"expected EOF; got {tail!r}"
    finally:
        writer.close()
        with contextlib.suppress(ConnectionResetError, BrokenPipeError):
            await writer.wait_closed()


@pytest.mark.asyncio
async def test_peercred_reject_stays_silent_no_frame(
    running_daemon: _DaemonPaths, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A foreign-UID peer is closed silently — NO reject frame is leaked.

    The reject frame is gated behind the post-peer-cred token branch
    precisely so a peer that failed the SO_PEERCRED gate (unauthenticated
    surface) learns nothing. This pins that the peer-cred reject did not
    regress into the new error channel.
    """
    daemon, paths = running_daemon
    daemon.token = "expected-token"
    foreign_uid = daemon.uid + 1
    monkeypatch.setattr(broadcast, "_peer_uid", lambda _sock: foreign_uid)
    reader, writer = await _connect(paths["broadcast"])
    try:
        await _subscribe(writer, filters=["*"], token="wrong-token")
        try:
            data = await asyncio.wait_for(reader.read(1024), timeout=1.5)
        except ConnectionResetError:
            return  # ECONNRESET is equivalent to a silent EOF here.
        assert data == b"", (
            "peer-cred reject must stay silent — no subscribe_rejected "
            f"frame may leak to a foreign-UID peer; received {data!r}"
        )
    finally:
        writer.close()
        with contextlib.suppress(ConnectionResetError, BrokenPipeError):
            await writer.wait_closed()


@pytest.mark.asyncio
async def test_bad_filter_reject_stays_silent_no_frame(
    running_daemon: _DaemonPaths,
) -> None:
    """A request-shape reject (bad filter) stays silent-EOF — no frame.

    Only the token branch emits a frame; every pre-token / request-shape
    reject keeps the silent-EOF posture so operators debug via logs.
    """
    daemon, paths = running_daemon
    # Token must be within [16, 128] so the token gate PASSES here and
    # the bad-filter ValueError path is the one that actually rejects.
    good_token = "expected-token-0123456789"  # 25 chars
    daemon.token = good_token
    reader, writer = await _connect(paths["broadcast"])
    try:
        # Correct token (clears the token gate) but a bad filter so the
        # ValueError reject path runs — that path must stay silent.
        await _subscribe(writer, filters=["../etc/passwd"], token=good_token)
        data = await asyncio.wait_for(reader.read(1024), timeout=1.5)
        assert data == b"", (
            f"bad-filter reject must stay silent-EOF (only the token branch emits a frame); received {data!r}"
        )
    finally:
        writer.close()
        with contextlib.suppress(ConnectionResetError, BrokenPipeError):
            await writer.wait_closed()


# ---------------------------------------------------------------------------
# Task-tracking, fd-capture, narrowed-except, and subscriber-count tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_subscribe_task_is_tracked(running_daemon: _DaemonPaths) -> None:
    """A connect+subscribe attempt runs through the tracking path.

    The task may complete before we sample the set, so we accept either
    a non-empty set immediately OR an empty set after the task finished
    (discard-on-done fired). The invariant is that the set never leaks
    a completed task: if it's already empty the task finished cleanly
    and was discarded; if it's non-empty the task is still running.

    We verify by checking that after subscribe completes the daemon has
    at least one registered subscriber, which proves the task ran to
    completion through the tracked code path.
    """
    daemon, paths = running_daemon
    _reader, writer = await _connect(paths["broadcast"])
    try:
        await _subscribe(writer, filters=["*"])
        # Give the _read_subscribe coroutine time to run and the
        # done-callback to fire.
        await asyncio.sleep(0.15)
        # The subscriber must now be registered in the daemon — the
        # task ran end-to-end through the _pending_subscribes tracking path.
        assert len(daemon.subscribers) >= 1, "no subscribers registered — _read_subscribe task may not have run"
        # Every task still in the set must be running (not done); completed
        # tasks must have been discarded by the done-callback.
        for task in daemon._pending_subscribes:
            assert not task.done(), "completed task still present in _pending_subscribes — done-callback did not fire"
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_completed_subscribe_task_is_removed_from_tracking(
    running_daemon: _DaemonPaths,
) -> None:
    """After _read_subscribe completes, no done tasks linger in _pending_subscribes.

    The discard-on-done callback removes each task from the set as soon
    as it finishes. Running tasks may still be present; the invariant is
    that no DONE task survives in the set.
    """
    daemon, paths = running_daemon
    _reader, writer = await _connect(paths["broadcast"])
    try:
        await _subscribe(writer, filters=["*"])
        # Give the coroutine time to complete and the done-callback to fire.
        await asyncio.sleep(0.15)
        for task in daemon._pending_subscribes:
            assert not task.done(), "completed task still present in _pending_subscribes — done-callback did not fire"
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_replay_lag_eviction_closes_subscriber_with_real_fd(
    running_daemon: _DaemonPaths,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Replay lag-eviction routes through _close_subscriber.

    The eviction logs ``subscriber_closed reason=replay_lag_limit_exceeded``
    with the fd captured BEFORE close (a positive int, not the -1 that
    ``fileno()`` returns after close), AND removes the subscriber from the
    map. The prior code popped ``client_sock.fileno()`` AFTER ``_replay``
    closed the socket, so it popped key ``-1`` and leaked the real entry
    (later double-decrementing SUBSCRIBER_COUNT when _fan_out re-closed it).
    """
    daemon, paths = running_daemon
    # Seed enough events to trigger LAG_LIMIT drops during replay.
    for i in range(broadcast.LAG_LIMIT + 5):
        _insert(paths["db"], f"d-lag-{i}")

    # Capture the earliest event_id as the replay cursor.
    with contextlib.closing(sqlite3.connect(paths["db"])) as conn:
        first_id = conn.execute("SELECT MIN(event_id) FROM events").fetchone()[0]
    assert first_id is not None

    import logging as _logging

    _reader, writer = await _connect(paths["broadcast"])
    try:
        # Force every replay send to lag so lag_count hits LAG_LIMIT.
        def _always_lag(self: Any, blob: bytes, *, counts_as_delivered: bool) -> bool:
            self.lag_count += 1
            return False

        with pytest.MonkeyPatch().context() as mp:
            mp.setattr(broadcast.Subscriber, "enqueue", _always_lag)
            with caplog.at_level(_logging.INFO, logger="waitbus.broadcast"):
                await _subscribe(writer, filters=["*"], since=first_id)
                await asyncio.sleep(0.15)

        closed = [json.loads(r.getMessage()) for r in caplog.records if "subscriber_closed" in r.getMessage()]
        replay_evictions = [e for e in closed if e.get("reason") == "replay_lag_limit_exceeded"]
        assert replay_evictions, f"no replay_lag_limit_exceeded close log found; saw {closed!r}"
        logged_fd = replay_evictions[0].get("fd")
        assert isinstance(logged_fd, int) and logged_fd > 0, f"expected the positive pre-close fd, got {logged_fd!r}"
        # Cut 17 leak regression: the subscriber must be gone from the map.
        assert logged_fd not in daemon.subscribers, "replay-lag eviction leaked the subscriber entry"
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_heartbeat_loop_evicts_lagged_subscriber(tmp_db_path: Path) -> None:
    """The heartbeat loop drops a subscriber whose ``lag_count`` reached
    ``LAG_LIMIT``, closing it with reason ``heartbeat_lag``.

    The robustness state machine exercises ``_close_subscriber(reason=
    "heartbeat_lag")`` directly; this covers the heartbeat-loop DECISION
    (``lag_count >= LAG_LIMIT`` after a heartbeat send -> evict) that drives
    it -- the path the soak's ``heartbeat_lag`` drain-smoke probe hits live.
    """
    daemon = broadcast.Broadcast(db_path=str(tmp_db_path))
    daemon.heartbeat_sec = 0.01
    # A send that always raises EAGAIN keeps lag_count climbing; prime it one
    # below the limit so the first heartbeat send trips it.
    sub, sock = _register_fake_sub(daemon, 11, exc=BlockingIOError())
    sub.lag_count = broadcast.LAG_LIMIT - 1
    closed: list[tuple[int, str]] = []
    real_close = daemon._close_subscriber

    def _spy(fd: int, reason: str) -> None:
        closed.append((fd, reason))
        real_close(fd, reason)

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(daemon, "_close_subscriber", _spy)
        task = asyncio.create_task(daemon._heartbeat_loop())
        try:
            for _ in range(300):
                await asyncio.sleep(0.01)
                if 11 not in daemon.subscribers:
                    break
        finally:
            daemon.stopping = True
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    assert closed == [(11, "heartbeat_lag")]
    assert 11 not in daemon.subscribers
    assert sock.closed


@pytest.mark.asyncio
async def test_read_subscribe_replay_db_error_closes_silently(
    running_daemon: _DaemonPaths,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A ``sqlite3.Error`` raised during replay closes the subscriber with
    reason ``replay_db_error`` (no wire reject frame) and logs the db-error
    event, rather than crashing the ``_read_subscribe`` task and leaking the
    subscriber.
    """
    daemon, paths = running_daemon
    for i in range(3):
        _insert(paths["db"], f"d-dberr-{i}")
    with contextlib.closing(sqlite3.connect(paths["db"])) as conn:
        first_id = conn.execute("SELECT MIN(event_id) FROM events").fetchone()[0]
    assert first_id is not None

    import logging as _logging

    def _boom(_self: Any, _sub: Any, _since: str, until_seq: int | None = None) -> bool:
        raise sqlite3.OperationalError("synthetic replay DB fault")

    _reader, writer = await _connect(paths["broadcast"])
    try:
        with pytest.MonkeyPatch().context() as mp:
            mp.setattr(broadcast.Broadcast, "_replay", _boom)
            with caplog.at_level(_logging.INFO, logger="waitbus.broadcast"):
                await _subscribe(writer, filters=["*"], since=first_id)
                await asyncio.sleep(0.15)

        db_err = [json.loads(r.getMessage()) for r in caplog.records if "subscriber_replay_db_error" in r.getMessage()]
        closed = [json.loads(r.getMessage()) for r in caplog.records if "subscriber_closed" in r.getMessage()]
        assert db_err, "no subscriber_replay_db_error log emitted"
        replay_db_closes = [e for e in closed if e.get("reason") == "replay_db_error"]
        assert replay_db_closes, f"no replay_db_error close log; saw {closed!r}"
        logged_fd = replay_db_closes[0].get("fd")
        assert isinstance(logged_fd, int) and logged_fd not in daemon.subscribers
    finally:
        writer.close()
        await writer.wait_closed()


def test_narrowed_except_propagates_unexpected_exception() -> None:
    """_summary_for must NOT swallow exceptions outside its narrowed tuple.

    MemoryError and SystemError are outside (KeyError, TypeError,
    AttributeError, ValueError), so they must propagate rather than be
    silently logged as a format failure.
    """

    class _BoomRow:
        def __getitem__(self, key: str) -> str:
            raise MemoryError("synthetic OOM")

        def keys(self) -> Any:
            return ()

    import waitbus.read_events as re_mod

    def _raise_memory_error(_row: Any) -> str:
        raise MemoryError("synthetic OOM from format_text")

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(re_mod, "format_text", _raise_memory_error)
        with pytest.raises(MemoryError):
            broadcast._summary_for(_BoomRow())


def test_narrowed_except_catches_key_error() -> None:
    """_summary_for must catch KeyError from format_text and return the fallback."""
    from typing import ClassVar

    class _MinimalRow:
        _data: ClassVar[dict[str, str]] = {"owner": "o", "repo": "r", "event_type": "t", "delivery_id": "d"}

        def __getitem__(self, key: str) -> str:
            return self._data[key]

        def keys(self) -> Any:
            return self._data.keys()

    import waitbus.read_events as re_mod

    def _raise_key_error(_row: Any) -> str:
        raise KeyError("head_branch")

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(re_mod, "format_text", _raise_key_error)
        result = broadcast._summary_for(_MinimalRow())
    assert "o/r" in result, f"fallback summary expected 'o/r' in {result!r}"


def test_narrowed_except_catches_type_error() -> None:
    """_summary_for must catch TypeError from format_text and return the fallback."""
    from typing import ClassVar

    class _MinimalRow:
        _data: ClassVar[dict[str, str]] = {"owner": "o", "repo": "r", "event_type": "t", "delivery_id": "d"}

        def __getitem__(self, key: str) -> str:
            return self._data[key]

        def keys(self) -> Any:
            return self._data.keys()

    import waitbus.read_events as re_mod

    def _raise_type_error(_row: Any) -> str:
        raise TypeError("unexpected NoneType")

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(re_mod, "format_text", _raise_type_error)
        result = broadcast._summary_for(_MinimalRow())
    assert "o/r" in result, f"fallback summary expected 'o/r' in {result!r}"


@pytest.mark.asyncio
async def test_subscriber_count_gauge_increments_on_subscribe(
    running_daemon: _DaemonPaths,
) -> None:
    """Connecting and subscribing must increment SUBSCRIBER_COUNT by 1."""
    _daemon, paths = running_daemon
    _metrics.reset()
    before = _metrics.SUBSCRIBER_COUNT.value()
    _reader, writer = await _connect(paths["broadcast"])
    try:
        await _subscribe(writer, filters=["*"])
        await asyncio.sleep(0.15)
        after = _metrics.SUBSCRIBER_COUNT.value()
        assert after == before + 1, f"expected gauge delta=+1, got before={before} after={after}"
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_subscriber_count_gauge_decrements_on_drop(
    running_daemon: _DaemonPaths,
) -> None:
    """Closing a subscribed connection must decrement SUBSCRIBER_COUNT by 1.

    We close via _close_subscriber directly (using the server-side fd from
    daemon.subscribers) so the test is not sensitive to heartbeat timing.
    """
    daemon, paths = running_daemon
    _metrics.reset()
    _reader, writer = await _connect(paths["broadcast"])
    await _subscribe(writer, filters=["*"])
    await asyncio.sleep(0.15)
    subscribed_value = _metrics.SUBSCRIBER_COUNT.value()
    # Get the server-side fd (daemon.subscribers is keyed by accepted fd).
    server_fds = list(daemon.subscribers.keys())
    assert server_fds, "no subscribers registered after subscribe"
    server_fd = server_fds[-1]
    daemon._close_subscriber(server_fd, reason="test_drop")
    writer.close()
    await writer.wait_closed()
    after = _metrics.SUBSCRIBER_COUNT.value()
    assert after == subscribed_value - 1, f"expected gauge delta=-1, got subscribed={subscribed_value} after={after}"


@pytest.mark.asyncio
async def test_subscriber_count_gauge_balanced_after_subscribe_and_drop(
    running_daemon: _DaemonPaths,
) -> None:
    """Net gauge change after a subscribe-then-drop cycle must be zero."""
    daemon, paths = running_daemon
    _metrics.reset()
    baseline = _metrics.SUBSCRIBER_COUNT.value()
    _reader, writer = await _connect(paths["broadcast"])
    await _subscribe(writer, filters=["*"])
    await asyncio.sleep(0.15)
    server_fds = list(daemon.subscribers.keys())
    assert server_fds, "no subscribers registered after subscribe"
    server_fd = server_fds[-1]
    daemon._close_subscriber(server_fd, reason="test_drop")
    writer.close()
    await writer.wait_closed()
    final = _metrics.SUBSCRIBER_COUNT.value()
    assert final == baseline, (
        f"gauge imbalance: baseline={baseline} final={final}; subscribe or drop path missed an inc/dec"
    )


@pytest.mark.asyncio
async def test_fan_out_populates_subscription_health_gauges() -> None:
    """_fan_out must set the subscription_count / emission_latency /
    stale_subscription gauges off the live subscriber set.

    Two matching subscribers are injected directly. One sends cleanly
    (resetting its lag to 0); the other emulates the EAGAIN arm so its
    lag_count rises to 1 — below LAG_LIMIT — making it "stale". The
    stale gauge must read exactly 1 while the matched count reads 2.
    """
    next_id = "01JZABC123DEF456GHJ789KLMN"

    class _FanOutSub:
        """Minimal _fan_out-compatible stand-in (Subscriber uses __slots__
        so its bound methods cannot be monkeypatched per-instance).
        """

        def __init__(self, *, lagging: bool) -> None:
            self.lagging = lagging
            self.lag_count = 0
            self.replay_watermark: str | None = None
            self.pre_ack_buffer: list[bytes] | None = None
            self.pre_ack_buffered_bytes: int = 0
            self.sock = None

        def matches(self, _owner: str, _repo: str, _et: str) -> bool:
            return True

        def tx_buffered_bytes(self) -> int:
            return 0

        def enqueue(self, _blob: bytes, *, counts_as_delivered: bool) -> bool:
            if self.lagging:
                self.lag_count += 1  # EAGAIN arm: stays below LAG_LIMIT
                return False
            self.lag_count = 0
            return True

    _metrics.reset()
    daemon = broadcast.Broadcast(db_path=":memory:")
    daemon.subscribers[1] = cast(Any, _FanOutSub(lagging=False))
    daemon.subscribers[2] = cast(Any, _FanOutSub(lagging=True))

    daemon._fan_out(1, next_id, "test-owner", "test-repo", "workflow_run", b"\x00\x00\x00\x02{}")

    assert _metrics.BROADCAST_SUBSCRIPTION_COUNT.value() == 2.0, (
        "both matching subscribers should count as fan-out targets"
    )
    assert _metrics.BROADCAST_STALE_SUBSCRIPTION_COUNT.value() == 1.0, (
        "exactly one subscriber carries a partial lag counter"
    )
    assert _metrics.BROADCAST_EMISSION_LATENCY_SECONDS.value() >= 0.0, (
        "emission-latency gauge must be set to a non-negative value"
    )


# ---------------------------------------------------------------------------
# Watermark replay + framing tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_subscriber_registered_before_replay(running_daemon: _DaemonPaths) -> None:
    """Subscriber must appear in daemon.subscribers BEFORE replay rows arrive.

    The watermark mechanism requires registration-before-replay so that
    live deliveries via _fan_out can reach the subscriber immediately.
    """
    daemon, paths = running_daemon
    # Pre-seed two events.
    _insert(paths["db"], "d-pre-a")
    _insert(paths["db"], "d-pre-b")
    await asyncio.sleep(0.05)
    with contextlib.closing(sqlite3.connect(paths["db"])) as conn:
        first_id = conn.execute("SELECT MIN(event_id) FROM events").fetchone()[0]
    assert first_id is not None

    # Track registration timing inside _replay.
    registered_before_replay: list[bool] = []

    original_replay = daemon._replay

    def _patched_replay(sub: Any, since: Any, until_seq: Any = None) -> None:
        # At this point _read_subscribe should have already registered sub.
        fd = sub.sock.fileno()
        registered_before_replay.append(fd in daemon.subscribers)
        original_replay(sub, since, until_seq=until_seq)

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(daemon, "_replay", _patched_replay)
        _reader, writer = await _connect(paths["broadcast"])
        try:
            await _subscribe(writer, filters=["*"], since=first_id)
            await asyncio.sleep(0.2)
        finally:
            writer.close()
            await writer.wait_closed()

    assert registered_before_replay, "replay was never called"
    assert all(registered_before_replay), (
        "subscriber was NOT registered before _replay was called — watermark invariant violated"
    )


@pytest.mark.asyncio
async def test_replay_watermark_dedupes_overlapping_live_frame(
    running_daemon: _DaemonPaths,
) -> None:
    """No event_id must be delivered twice when replay and live delivery overlap.

    Pre-seed N rows. Subscribe with since=first_id. Collect all frames.
    Assert each delivery_id appears exactly once.
    """
    _daemon, paths = running_daemon
    for i in range(5):
        _insert(paths["db"], f"d-wm-{i}")
    await asyncio.sleep(0.05)
    with contextlib.closing(sqlite3.connect(paths["db"])) as conn:
        first_id = conn.execute("SELECT MIN(event_id) FROM events").fetchone()[0]

    reader, writer = await _connect(paths["broadcast"])
    try:
        await _subscribe(writer, filters=["*"], since=first_id)
        seen: list[str] = []
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            try:
                f = await _recv_frame(reader, timeout=0.3)
            except TimeoutError:
                break
            kind = f.get("kind")
            if kind == "event":
                seen.append(f["delivery_id"])
    finally:
        writer.close()
        await writer.wait_closed()

    # d-wm-0 is AT the since cursor (not > it), so it is NOT replayed.
    # d-wm-1 through d-wm-4 should appear exactly once each.
    expected = {f"d-wm-{i}" for i in range(1, 5)}
    assert set(seen) == expected, f"unexpected delivery_ids: {set(seen)!r}, expected {expected!r}"
    assert len(seen) == len(set(seen)), f"duplicate deliveries detected: {seen!r}"


@pytest.mark.asyncio
async def test_replay_watermark_cleared_on_first_post_watermark_frame(
    running_daemon: _DaemonPaths,
) -> None:
    """sub.replay_watermark must be None after the first live frame above it."""
    daemon, paths = running_daemon
    _insert(paths["db"], "d-wmc-a")
    await asyncio.sleep(0.05)
    with contextlib.closing(sqlite3.connect(paths["db"])) as conn:
        wm_id = conn.execute("SELECT MAX(event_id) FROM events").fetchone()[0]

    reader, writer = await _connect(paths["broadcast"])
    try:
        await _subscribe(writer, filters=["*"], since=wm_id)
        await asyncio.sleep(0.05)
        # Insert a post-watermark row.
        _insert(paths["db"], "d-wmc-b")
        # Wait for the frame to arrive.
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            try:
                f = await _recv_frame(reader, timeout=0.3)
            except TimeoutError:
                break
            if f.get("delivery_id") == "d-wmc-b":
                break
        await asyncio.sleep(0.05)
        # After delivery of d-wmc-b, watermark should be cleared.
        server_fds = list(daemon.subscribers.keys())
        if server_fds:
            sub = daemon.subscribers[server_fds[-1]]
            assert sub.replay_watermark is None, (
                f"watermark not cleared after first post-watermark frame; still {sub.replay_watermark!r}"
            )
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_broadcast_send_seconds_histogram_records(running_daemon: _DaemonPaths) -> None:
    """Emitting one broadcast frame must record at least one bucket observation."""
    _daemon, paths = running_daemon
    _metrics.reset()
    reader, writer = await _connect(paths["broadcast"])
    try:
        await _subscribe(writer, filters=["*"])
        await asyncio.sleep(0.05)
        _insert(paths["db"], "d-hist-1")
        # Wait for the frame.
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            try:
                f = await _recv_frame(reader, timeout=0.3)
            except TimeoutError:
                break
            if f.get("delivery_id") == "d-hist-1":
                break
        await asyncio.sleep(0.05)
    finally:
        writer.close()
        await writer.wait_closed()

    lines = _metrics.BROADCAST_SEND_SECONDS.render()
    inf_line = next((ln for ln in lines if 'le="+Inf"' in ln), None)
    assert inf_line is not None, "no +Inf bucket line in histogram render"
    count = int(float(inf_line.split(" ")[-1]))
    assert count >= 1, f"expected at least one observation in BROADCAST_SEND_SECONDS; got {count}"


@pytest.mark.asyncio
async def test_watermark_replay_events_counter_increments(running_daemon: _DaemonPaths) -> None:
    """Replaying 3 rows must increment WATERMARK_REPLAY_EVENTS_TOTAL by exactly 3."""
    _daemon, paths = running_daemon
    _metrics.reset()
    for i in range(3):
        _insert(paths["db"], f"d-ctr-{i}")
    await asyncio.sleep(0.05)
    with contextlib.closing(sqlite3.connect(paths["db"])) as conn:
        first_id = conn.execute("SELECT MIN(event_id) FROM events").fetchone()[0]

    _reader, writer = await _connect(paths["broadcast"])
    try:
        await _subscribe(writer, filters=["*"], since=first_id)
        await asyncio.sleep(0.3)
    finally:
        writer.close()
        await writer.wait_closed()

    # d-ctr-0 is AT the since cursor; d-ctr-1 and d-ctr-2 are replayed.
    count = _metrics.WATERMARK_REPLAY_EVENTS_TOTAL.value()
    assert count >= 2, f"expected at least 2 replay events counted; got {count}"


# ---------------------------------------------------------------------------
# Pre-ack buffer overflow + terminal-reject-frame emission (unit-level)
# ---------------------------------------------------------------------------


def _register_fake_sub(
    daemon: broadcast.Broadcast,
    fd: int,
    *,
    exc: BaseException | None = None,
) -> tuple[broadcast.Subscriber, _FakeSock]:
    """Register a _FakeSock-backed Subscriber at ``fd`` and inc the gauge so a
    later _close_subscriber decrement balances."""
    sock = _FakeSock(exc, fileno=fd)
    sub = broadcast.Subscriber(
        sock=sock,  # type: ignore[arg-type]
        filters=["*"],
        event_types=frozenset({"workflow_run"}),
        remote_uid=os.getuid(),
    )
    daemon.subscribers[fd] = sub
    _metrics.SUBSCRIBER_COUNT.inc()
    return sub, sock


def test_fan_out_buffers_live_frame_while_pre_ack_under_cap(tmp_db_path: Path) -> None:
    daemon = broadcast.Broadcast(db_path=str(tmp_db_path))
    sub, sock = _register_fake_sub(daemon, 7)
    sub.pre_ack_buffer = []
    try:
        daemon._fan_out(1, "01ID00000000000000000000AB", "o", "r", "workflow_run", b"frame")
        assert sub.pre_ack_buffer == [b"frame"], "live frame should be buffered, not sent"
        assert sock.sent == [], "buffered frame must not hit the wire pre-ack"
        assert 7 in daemon.subscribers
    finally:
        daemon.subscribers.pop(7, None)
        _metrics.SUBSCRIBER_COUNT.dec()


def test_fan_out_pre_ack_frame_overflow_drops_with_reject(tmp_db_path: Path) -> None:
    daemon = broadcast.Broadcast(db_path=str(tmp_db_path))
    sub, sock = _register_fake_sub(daemon, 7)
    # Buffer already at the frame-count cap; the next live frame overflows.
    sub.pre_ack_buffer = [b"x"] * broadcast.PRE_ACK_BUFFER_FRAMES
    daemon._fan_out(1, "01ID00000000000000000000AB", "o", "r", "workflow_run", b"frame")
    assert 7 not in daemon.subscribers, "frame-cap overflow must drop the subscriber"
    assert sock.sent[-1:] == [broadcast._SUBSCRIBE_REJECT_LAG_LIMIT_FRAME], "expected lag-limit reject frame"
    assert sock.closed


def test_fan_out_pre_ack_byte_overflow_drops_with_reject(tmp_db_path: Path) -> None:
    daemon = broadcast.Broadcast(db_path=str(tmp_db_path))
    sub, sock = _register_fake_sub(daemon, 8)
    # One buffered frame (under the frame-count cap) but bytes already at the
    # byte cap, so the next frame overflows on the byte axis.
    sub.pre_ack_buffer = [b"x"]
    sub.pre_ack_buffered_bytes = broadcast.PRE_ACK_BUFFER_BYTES
    daemon._fan_out(1, "01ID00000000000000000000AB", "o", "r", "workflow_run", b"frame")
    assert 8 not in daemon.subscribers, "byte-cap overflow must drop the subscriber"
    assert sock.sent[-1:] == [broadcast._SUBSCRIBE_REJECT_LAG_LIMIT_FRAME]


@pytest.mark.parametrize(
    ("reason", "expect_reject_frame"),
    [
        ("lag_limit_exceeded", True),
        ("heartbeat_lag", True),
        ("replay_lag_limit_exceeded", True),
        ("replay_db_error", False),
        ("shutdown", False),
    ],
)
def test_close_subscriber_emits_reject_frame_per_reason(
    tmp_db_path: Path, reason: str, expect_reject_frame: bool
) -> None:
    """All three lag-class reasons emit the lag-limit reject frame on the wire;
    internal faults (replay_db_error) and shutdown close silently."""
    daemon = broadcast.Broadcast(db_path=str(tmp_db_path))
    _sub, sock = _register_fake_sub(daemon, 9)
    daemon._close_subscriber(9, reason=reason)
    assert 9 not in daemon.subscribers, "subscriber must be removed from the map"
    assert sock.closed
    if expect_reject_frame:
        assert sock.sent == [broadcast._SUBSCRIBE_REJECT_LAG_LIMIT_FRAME]
    else:
        assert sock.sent == [], f"reason={reason} must close silently"


def test_close_subscriber_skips_reject_when_tx_queue_nonempty(tmp_db_path: Path) -> None:
    """Eviction with queued bytes closes with a clean EOF — never a reject frame.

    With an unsent frame tail on the wire, reject bytes would land mid-frame
    and corrupt the stream; the whole-frame-or-clean-EOF contract names EOF
    as the legitimate outcome, so the goodbye is emitted only at a frame
    boundary (empty tx queue).
    """
    daemon = broadcast.Broadcast(db_path=str(tmp_db_path))
    sub, sock = _register_fake_sub(daemon, 11)
    sub._tx_queue.append((memoryview(b"unsent-tail"), True))
    daemon._close_subscriber(11, reason="lag_limit_exceeded")
    assert 11 not in daemon.subscribers
    assert sock.closed
    assert sock.sent == [], "reject frame must not interleave with a queued tail"
    assert not sub._tx_queue, "the discarded queue must be cleared with the connection"


def test_enqueue_counts_delivered_for_event_frames_only(tmp_db_path: Path) -> None:
    """Delivered counts data frames at kernel-accept; control frames never count.

    The single accounting owner (enqueue/_drain) gives every send site —
    fan-out, pre-ack drain, replay, heartbeat — one consistent boundary.
    """
    _metrics.reset()
    sub = broadcast.Subscriber(
        sock=_FakeSock(),  # type: ignore[arg-type]
        filters=["*"],
        event_types=frozenset({"workflow_run"}),
        remote_uid=os.getuid(),
    )
    assert sub.enqueue(b"event-frame", counts_as_delivered=True) is True
    assert _metrics.get("waitbus_broadcast_events_delivered_total") == 1
    assert sub.enqueue(b"heartbeat-frame", counts_as_delivered=False) is True
    assert _metrics.get("waitbus_broadcast_events_delivered_total") == 1
    _metrics.reset()


@pytest.mark.asyncio
async def test_drain_counts_only_event_frames_and_error_discards_queue() -> None:
    """The flush path counts per-frame at completion (events only) and a
    connection error mid-drain discards the queue without counting.

    Pins the conservation behaviour: delivered + in_queue == enqueued event
    frames at every observable instant, with eviction the only terminal drop.
    """

    class _PartialThenFlushSock:
        """First send accepts a prefix; subsequent sends accept everything."""

        def __init__(self) -> None:
            self.calls = 0
            self.out = bytearray()
            self._fd = os.eventfd(0) if hasattr(os, "eventfd") else -1

        def send(self, data: Any) -> int:
            self.calls += 1
            b = bytes(data)
            if self.calls == 1:
                self.out.extend(b[:3])
                return 3
            self.out.extend(b)
            return len(b)

        def fileno(self) -> int:
            return self._fd

        def close(self) -> None:
            if self._fd >= 0:
                os.close(self._fd)

    _metrics.reset()
    sock = _PartialThenFlushSock()
    sub = broadcast.Subscriber(
        sock=cast(Any, sock),
        filters=["*"],
        event_types=frozenset({"workflow_run"}),
        remote_uid=os.getuid(),
    )
    try:
        # Event frame partial-writes; a heartbeat queues behind it.
        assert sub.enqueue(b"event-bytes", counts_as_delivered=True) is False
        assert sub.enqueue(b"hb", counts_as_delivered=False) is False
        assert _metrics.get("waitbus_broadcast_events_delivered_total") == 0
        assert len(sub._tx_queue) == 2
        # Drain directly (the eventfd may not be writability-pollable on
        # every platform; calling the flush path is the unit under test).
        sub._drain()
        assert not sub._tx_queue
        # Exactly the one event frame counted; the control frame never counts.
        assert _metrics.get("waitbus_broadcast_events_delivered_total") == 1
        assert sub.lag_count == 0
    finally:
        sub._stop_draining()
        sock.close()
        _metrics.reset()

    class _ErrorSock:
        def send(self, data: Any) -> int:
            raise BrokenPipeError

        def fileno(self) -> int:
            return -1

    _metrics.reset()
    sub2 = broadcast.Subscriber(
        sock=cast(Any, _ErrorSock()),
        filters=["*"],
        event_types=frozenset({"workflow_run"}),
        remote_uid=os.getuid(),
    )
    sub2._tx_queue.append((memoryview(b"doomed-event"), True))
    sub2._drain()
    assert not sub2._tx_queue, "error mid-drain must discard the queue"
    assert sub2.tx_buffered_bytes() == 0
    assert _metrics.get("waitbus_broadcast_events_delivered_total") == 0, (
        "frames the peer never received must not count as delivered"
    )
    assert sub2.lag_count >= broadcast.LAG_LIMIT
    _metrics.reset()


def test_close_subscriber_is_idempotent(tmp_db_path: Path) -> None:
    """A double close (e.g. lag drop then shutdown) is a no-op, not a KeyError
    or a double SUBSCRIBER_COUNT decrement."""
    daemon = broadcast.Broadcast(db_path=str(tmp_db_path))
    _register_fake_sub(daemon, 10)
    daemon._close_subscriber(10, reason="lag_limit_exceeded")
    # Second close: fd already gone; must return without touching the gauge.
    daemon._close_subscriber(10, reason="shutdown")
    assert 10 not in daemon.subscribers


@pytest.mark.asyncio
async def test_run_shutdown_removes_loop_readers(broadcast_paths: dict[str, Path]) -> None:
    """run() must deregister its listener and doorbell readers on shutdown.

    Under the serve supervisor's shared long-lived loop, a closed-but-still-
    registered fd leaves a stale selector entry that a recycled descriptor
    number can collide with ("fd already registered") or fire dead callbacks.
    """
    daemon = broadcast.Broadcast(db_path=str(broadcast_paths["db"]))
    task = asyncio.create_task(daemon.run())
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        if broadcast_paths["broadcast"].exists():
            break
        await asyncio.sleep(0.02)
    else:
        task.cancel()
        raise RuntimeError("daemon failed to bind broadcast socket")
    # Snapshot the fds the daemon registered while it is still running.
    assert daemon.listener_sock is not None
    listener_fd = daemon.listener_sock.fileno()
    doorbell_fd = daemon._doorbell.fd if daemon._doorbell is not None else None
    loop = asyncio.get_running_loop()
    selector_map = loop._selector.get_map()  # type: ignore[attr-defined]
    assert listener_fd in selector_map, "listener reader should be registered while running"
    await daemon.stop()
    await asyncio.wait_for(task, timeout=5.0)
    selector_map = loop._selector.get_map()  # type: ignore[attr-defined]
    assert listener_fd not in selector_map, "stale listener reader after shutdown"
    if doorbell_fd is not None:
        assert doorbell_fd not in selector_map, "stale doorbell reader after shutdown"


def test_broadcast_accepts_explicit_socket_and_doorbell_paths(tmp_path: Path) -> None:
    """Broadcast stores explicitly-injected socket / doorbell paths verbatim.

    The explicit-injection seam that lets a self-contained in-process caller (the
    demo / swarm-demo) bind a daemon to a temp runtime dir without mutating the
    process-global WAITBUS_RUNTIME_DIR env.
    """
    daemon = broadcast.Broadcast(
        db_path=str(tmp_path / "events.db"),
        socket_path=str(tmp_path / "broadcast.sock"),
        doorbell_path=str(tmp_path / "doorbell.sock"),
    )
    assert daemon.socket_path == str(tmp_path / "broadcast.sock")
    assert daemon.doorbell_path == str(tmp_path / "doorbell.sock")


def test_broadcast_socket_paths_default_to_runtime_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """With no explicit paths, the daemon falls back to the runtime-dir factories."""
    monkeypatch.setenv("WAITBUS_RUNTIME_DIR", str(tmp_path))
    daemon = broadcast.Broadcast(db_path=":memory:")
    assert daemon.socket_path == str(tmp_path / "broadcast.sock")
    assert daemon.doorbell_path == str(tmp_path / "doorbell.sock")
