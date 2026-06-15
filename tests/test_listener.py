"""Tests for the waitbus listener.

Covers HMAC verification (positive, missing, mismatched, cross-secret),
path dispatch (/webhook, /alertmanager, /watchdog, /healthz, 404),
workflow_run + workflow_job ingestion with delivery_id dedup,
prometheus_alert ingestion with alert_name/severity/fingerprint, body-
length guards, _event_from_webhook_payload shape and defensive-coercion
guards across all four event types, and
HTTP hardening (Transfer-Encoding rejection, duplicate Content-Length,
non-numeric Content-Length, Expect: 100-continue, method allowlist,
Server-header strip, RecursionError on nested JSON).
"""

from __future__ import annotations

import contextlib
import http.client
import json
import socket
import sqlite3
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import msgspec
import pytest
from conftest import ServerHandle
from hypothesis import given, settings
from hypothesis import strategies as st

from waitbus import _config, _db, _http, listener

# Column keys `_event_from_webhook_payload` must populate on `EventInsert`
# regardless of input, after stripping the five HTTP-handler-supplied
# fields (see `_HANDLER_SUPPLIED_KEYS`). Mirrors the schema's column list;
# extend in lockstep with any new column.
_EXPECTED_FIELD_KEYS = {
    "event_type",
    "owner",
    "repo",
    "run_id",
    "workflow_name",
    "head_branch",
    "head_sha",
    "status",
    "conclusion",
    "job_id",
    "job_name",
    "parent_run_id",
    "alert_name",
    "alert_severity",
    "alert_fingerprint",
}

# Fields the HTTP handlers supply to `_event_from_webhook_payload` from
# request context rather than from the parsed payload body. Stripped from
# `msgspec.structs.asdict(ei)` to recover the payload-derived field set
# that the prior `extract_fields` dict shim returned verbatim.
_HANDLER_SUPPLIED_KEYS = {
    "delivery_id",
    "source",
    "received_at",
    "payload_json",
    "ingest_method",
}

# Agent-message addressing facet fields: never populated by the webhook ingest
# path (always None for GitHub events), so they are not part of the
# payload-derived key set and are stripped from this invariant.
_AGENT_FACET_KEYS = {
    "msg_to",
    "msg_from",
    "msg_correlation_id",
    "msg_reply_to",
    "msg_thread",
    "msg_body",
}


def _build_event(event_type: str, payload: dict[str, Any]) -> Any:
    """Construct an EventInsert for assertion tests with constant context fields."""
    return listener._event_from_webhook_payload(
        event_type,
        payload,
        delivery_id="t",
        source="github",
        received_at_ns=0,
        payload_json="",
        ingest_method="webhook",
    )


# --- helpers ----------------------------------------------------------------


def _post(
    url: str,
    body: bytes,
    headers: dict[str, str],
    timeout: float = 2.0,
) -> tuple[int, dict[str, str | None]]:
    req = urllib.request.Request(url, data=body, method="POST", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, dict(resp.headers)
    except urllib.error.HTTPError as exc:
        # HTTPError extends addinfourl which holds an open temporary file
        # since Python 3.14; closing it eagerly avoids a ResourceWarning at
        # GC time (PytestUnraisableExceptionWarning under strict gates).
        with exc:
            return exc.code, dict(exc.headers or {})


def _row(db_path: Path, delivery_id: str) -> tuple[str | None, ...] | None:
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        cur = conn.execute(
            "SELECT event_type, owner, repo, alert_name, alert_severity, "
            "alert_fingerprint, status FROM events WHERE delivery_id = ?",
            (delivery_id,),
        )
        return cast(tuple[str | None, ...] | None, cur.fetchone())


# --- HMAC + dispatch --------------------------------------------------------


def test_unknown_path_returns_404(server_fixture: ServerHandle) -> None:
    code, _ = _post(server_fixture.url("/unknown"), b"{}", {"Content-Length": "2"})
    assert code == 404


def test_webhook_missing_signature(server_fixture: ServerHandle) -> None:
    body = b'{"x":1}'
    code, _ = _post(
        server_fixture.url("/webhook"),
        body,
        {"Content-Length": str(len(body)), "X-GitHub-Event": "workflow_run"},
    )
    assert code == 401


def test_webhook_bad_signature(server_fixture: ServerHandle) -> None:
    body = b'{"x":1}'
    code, _ = _post(
        server_fixture.url("/webhook"),
        body,
        {
            "Content-Length": str(len(body)),
            "X-Hub-Signature-256": "sha256=" + "0" * 64,
            "X-GitHub-Event": "workflow_run",
            "X-GitHub-Delivery": "d1",
        },
    )
    assert code == 401


def test_webhook_bad_length(server_fixture: ServerHandle) -> None:
    code, _ = _post(
        server_fixture.url("/webhook"),
        b"",
        {
            "Content-Length": "0",
            "X-Hub-Signature-256": "sha256=deadbeef",
            "X-GitHub-Event": "workflow_run",
            "X-GitHub-Delivery": "d1",
        },
    )
    assert code == 400


# --- GitHub workflow ingestion ---------------------------------------------


def test_workflow_run_stored(
    server_fixture: ServerHandle,
    tmp_db_path: Path,
    gh_secret: bytes,
    hmac_sig: Callable[[bytes, bytes], str],
) -> None:
    payload = {
        "repository": {"name": "demo-repo", "owner": {"login": "demo-owner"}},
        "workflow_run": {
            "id": 12345,
            "name": "Tests",
            "head_branch": "main",
            "head_sha": "abc123",
            "status": "completed",
            "conclusion": "success",
        },
    }
    body = json.dumps(payload).encode("utf-8")
    code, _ = _post(
        server_fixture.url("/webhook"),
        body,
        {
            "Content-Length": str(len(body)),
            "X-Hub-Signature-256": hmac_sig(gh_secret, body),
            "X-GitHub-Event": "workflow_run",
            "X-GitHub-Delivery": "delivery-1",
        },
    )
    assert code == 200
    row = _row(tmp_db_path, "delivery-1")
    assert row is not None
    assert row == ("workflow_run", "demo-owner", "demo-repo", None, None, None, "completed")


def test_workflow_run_dedup_on_redelivery(
    server_fixture: ServerHandle,
    tmp_db_path: Path,
    gh_secret: bytes,
    hmac_sig: Callable[[bytes, bytes], str],
) -> None:
    payload = {"repository": {"name": "r", "owner": {"login": "o"}}, "workflow_run": {"id": 1}}
    body = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Length": str(len(body)),
        "X-Hub-Signature-256": hmac_sig(gh_secret, body),
        "X-GitHub-Event": "workflow_run",
        "X-GitHub-Delivery": "delivery-dup",
    }
    for _ in range(3):
        code, _ = _post(server_fixture.url("/webhook"), body, headers)
        assert code == 200
    with contextlib.closing(sqlite3.connect(tmp_db_path)) as conn:
        n = conn.execute("SELECT COUNT(*) FROM events WHERE delivery_id = ?", ("delivery-dup",)).fetchone()[0]
    assert n == 1


def test_unsupported_github_event_ignored(
    server_fixture: ServerHandle,
    tmp_db_path: Path,
    gh_secret: bytes,
    hmac_sig: Callable[[bytes, bytes], str],
) -> None:
    body = b"{}"
    code, _ = _post(
        server_fixture.url("/webhook"),
        body,
        {
            "Content-Length": str(len(body)),
            "X-Hub-Signature-256": hmac_sig(gh_secret, body),
            "X-GitHub-Event": "ping",
            "X-GitHub-Delivery": "ping-1",
        },
    )
    assert code == 200
    assert _row(tmp_db_path, "ping-1") is None


# --- alertmanager ----------------------------------------------------------


def test_alertmanager_stored(
    server_fixture: ServerHandle,
    tmp_db_path: Path,
    am_secret: bytes,
    hmac_sig: Callable[[bytes, bytes], str],
) -> None:
    payload = {
        "version": "4",
        "groupKey": '{}:{alertname="HetznerCapacityResourceUnavailable"}',
        "status": "firing",
        "alerts": [
            {
                "status": "firing",
                "labels": {
                    "alertname": "HetznerCapacityResourceUnavailable",
                    "severity": "warning",
                },
                "fingerprint": "fp-deadbeef",
                "startsAt": "2026-04-29T01:00:00Z",
            }
        ],
    }
    body = json.dumps(payload).encode("utf-8")
    code, _ = _post(
        server_fixture.url("/alertmanager"),
        body,
        {
            "Content-Length": str(len(body)),
            "X-Hub-Signature-256": hmac_sig(am_secret, body),
            "X-Alertmanager-Delivery": "alert-delivery-1",
        },
    )
    assert code == 200
    row = _row(tmp_db_path, "alert-delivery-1")
    cfg = _config.get_config()
    assert row == (
        "prometheus_alert",
        cfg.prom_owner,
        cfg.prom_repo,
        "HetznerCapacityResourceUnavailable",
        "warning",
        "fp-deadbeef",
        "firing",
    )


def test_alertmanager_cross_secret_rejected(
    server_fixture: ServerHandle,
    tmp_db_path: Path,
    gh_secret: bytes,
    hmac_sig: Callable[[bytes, bytes], str],
) -> None:
    """Signing with the GitHub secret must fail on the alertmanager path."""
    body = b'{"alerts":[]}'
    code, _ = _post(
        server_fixture.url("/alertmanager"),
        body,
        {
            "Content-Length": str(len(body)),
            "X-Hub-Signature-256": hmac_sig(gh_secret, body),
            "X-Alertmanager-Delivery": "x-secret-confusion",
        },
    )
    assert code == 401
    assert _row(tmp_db_path, "x-secret-confusion") is None


def test_alertmanager_missing_delivery(
    server_fixture: ServerHandle,
    am_secret: bytes,
    hmac_sig: Callable[[bytes, bytes], str],
) -> None:
    body = b'{"alerts":[]}'
    code, _ = _post(
        server_fixture.url("/alertmanager"),
        body,
        {
            "Content-Length": str(len(body)),
            "X-Hub-Signature-256": hmac_sig(am_secret, body),
        },
    )
    assert code == 400


def test_watchdog_path_stores_distinct_event_type(
    server_fixture: ServerHandle,
    tmp_db_path: Path,
    am_secret: bytes,
    hmac_sig: Callable[[bytes, bytes], str],
) -> None:
    """The /watchdog path is the reciprocal heartbeat receive endpoint.

    Same HMAC contract and payload shape as /alertmanager, but the
    event_type column is `prometheus_watchdog` so the absence detector
    can query for it independently of capacity alerts.
    """
    payload = {
        "version": "4",
        "groupKey": '{}:{alertname="Watchdog"}',
        "status": "firing",
        "alerts": [
            {
                "labels": {
                    "alertname": "Watchdog",
                    "severity": "none",
                    "purpose": "dead_mans_switch",
                },
                "fingerprint": "fp-watchdog",
                "startsAt": "2026-04-29T01:00:00Z",
            }
        ],
    }
    body = json.dumps(payload).encode("utf-8")
    code, _ = _post(
        server_fixture.url("/watchdog"),
        body,
        {
            "Content-Length": str(len(body)),
            "X-Hub-Signature-256": hmac_sig(am_secret, body),
            "X-Alertmanager-Delivery": "watchdog-delivery-1",
        },
    )
    assert code == 200
    row = _row(tmp_db_path, "watchdog-delivery-1")
    assert row is not None
    assert row[0] == "prometheus_watchdog"
    assert (row[1], row[2]) == (_config.get_config().prom_owner, _config.get_config().prom_repo)
    assert row[3] == "Watchdog"
    assert row[4] == "none"


def test_watchdog_cross_secret_rejected(
    server_fixture: ServerHandle,
    tmp_db_path: Path,
    gh_secret: bytes,
    hmac_sig: Callable[[bytes, bytes], str],
) -> None:
    body = b'{"alerts":[]}'
    code, _ = _post(
        server_fixture.url("/watchdog"),
        body,
        {
            "Content-Length": str(len(body)),
            "X-Hub-Signature-256": hmac_sig(gh_secret, body),
            "X-Alertmanager-Delivery": "watchdog-cross-secret",
        },
    )
    assert code == 401
    assert _row(tmp_db_path, "watchdog-cross-secret") is None


# --- /healthz --------------------------------------------------------------


def test_healthz(server_fixture: ServerHandle) -> None:
    with urllib.request.urlopen(server_fixture.url("/healthz"), timeout=2) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))
        assert data["status"] == "ok"


# --- _event_from_webhook_payload --------------------------------------------


def test_event_from_webhook_payload_workflow_run_shape() -> None:
    ei = _build_event(
        "workflow_run",
        {
            "repository": {"name": "r", "owner": {"login": "o"}},
            "workflow_run": {
                "id": 1,
                "name": "Tests",
                "head_branch": "main",
                "head_sha": "abc",
                "status": "completed",
                "conclusion": "success",
            },
        },
    )
    assert ei.event_type == "workflow_run"
    assert ei.owner == "o"
    assert ei.repo == "r"
    assert ei.run_id == 1
    assert ei.alert_name is None


def test_event_from_webhook_payload_prometheus_alert_shape() -> None:
    ei = _build_event(
        "prometheus_alert",
        {
            "status": "firing",
            "alerts": [
                {
                    "labels": {"alertname": "X", "severity": "critical"},
                    "fingerprint": "fp1",
                }
            ],
        },
    )
    assert ei.event_type == "prometheus_alert"
    assert (ei.owner, ei.repo) == (
        _config.get_config().prom_owner,
        _config.get_config().prom_repo,
    )
    assert ei.alert_name == "X"
    assert ei.alert_severity == "critical"
    assert ei.alert_fingerprint == "fp1"
    assert ei.status == "firing"
    assert ei.run_id is None


def test_event_from_webhook_payload_empty_alerts_array() -> None:
    ei = _build_event("prometheus_alert", {"alerts": []})
    assert ei.alert_name is None
    assert ei.alert_fingerprint is None


def test_event_from_webhook_payload_unknown_event_type() -> None:
    ei = _build_event("unknown", {})
    assert ei.event_type == "unknown"
    assert ei.run_id is None
    assert ei.alert_name is None


# --- defensive-coercion regression tests ------------------------------------
#
# Each test pins one of the eight isinstance guards that previously lived in
# `extract_fields` and now live inline in `_event_from_webhook_payload`. The
# guards exist because webhook bodies are user-supplied JSON: a non-dict /
# non-list / non-str value at any nesting level must not crash the handler.


def test_event_from_webhook_payload_handles_non_list_alerts() -> None:
    ei = _build_event("prometheus_alert", {"alerts": "not a list"})
    assert ei.alert_name is None


def test_event_from_webhook_payload_handles_non_dict_first_alert() -> None:
    ei = _build_event("prometheus_alert", {"alerts": ["not a dict"]})
    assert ei.alert_name is None


def test_event_from_webhook_payload_handles_non_dict_alert_labels() -> None:
    ei = _build_event("prometheus_alert", {"alerts": [{"labels": "not a dict"}]})
    assert ei.alert_name is None


def test_event_from_webhook_payload_handles_non_str_payload_status() -> None:
    ei = _build_event("prometheus_alert", {"status": 42})
    assert ei.status is None


# --- domain narrowing on run_id / job_id / parent_run_id ------------------
#
# GitHub's API contract guarantees workflow run and job ids are positive
# int64. The narrowing here is defence-in-depth pair with
# _terminal.entity_key's consumer-side narrowing: anything that fails the
# domain (0, -1, True, False, 5.0, '5', None) becomes a NULL identity
# column at DB insertion time so the daemon never mints a bogus
# collapse key. An attacker with the webhook secret could craft a
# payload to reach these cases; real GitHub never emits them.


@pytest.mark.parametrize("bad_id", [0, -1, True, False, 5.0, "5", None, [], {}])
def test_event_from_webhook_payload_workflow_run_rejects_non_positive_int(
    bad_id: object,
) -> None:
    """Non-positive-int run_id coerces to NULL."""
    ei = _build_event(
        "workflow_run",
        {
            "repository": {"name": "r", "owner": {"login": "o"}},
            "workflow_run": {"id": bad_id},
        },
    )
    assert ei.run_id is None


@pytest.mark.parametrize("bad_id", [0, -1, True, False, 5.0, "5", None, [], {}])
def test_event_from_webhook_payload_workflow_job_rejects_non_positive_int(
    bad_id: object,
) -> None:
    """Non-positive-int job_id and parent_run_id coerce to NULL."""
    ei = _build_event(
        "workflow_job",
        {
            "repository": {"name": "r", "owner": {"login": "o"}},
            "workflow_job": {
                "id": bad_id,
                "run_id": bad_id,
                "name": "test",
            },
        },
    )
    assert ei.job_id is None
    assert ei.parent_run_id is None


def test_event_from_webhook_payload_workflow_run_accepts_positive_int() -> None:
    """Sanity: legitimate positive int IDs flow through unchanged."""
    ei = _build_event(
        "workflow_run",
        {
            "repository": {"name": "r", "owner": {"login": "o"}},
            "workflow_run": {"id": 99},
        },
    )
    assert ei.run_id == 99


def test_event_from_webhook_payload_handles_non_dict_repository() -> None:
    ei = _build_event("workflow_run", {"repository": "not a dict"})
    assert ei.owner == ""
    assert ei.repo == ""


def test_event_from_webhook_payload_handles_non_dict_repo_owner() -> None:
    ei = _build_event("workflow_run", {"repository": {"owner": "not a dict"}})
    assert ei.owner == ""


def test_event_from_webhook_payload_handles_non_dict_workflow_run() -> None:
    ei = _build_event("workflow_run", {"workflow_run": "not a dict"})
    assert ei.run_id is None


def test_event_from_webhook_payload_handles_non_dict_workflow_job() -> None:
    ei = _build_event("workflow_job", {"workflow_job": "not a dict"})
    assert ei.job_id is None


# --- property-based tests (hypothesis) --------------------------------------

# `_event_from_webhook_payload` only dereferences via `.get(...)`, so non-dict
# children at leaf positions are safe — the parent `.get(...)` returns None or
# a non-dict and the function falls through.
_JSON_PAYLOAD = st.recursive(
    st.one_of(
        st.none(),
        st.booleans(),
        st.integers(min_value=-(2**31), max_value=2**31 - 1),
        st.text(max_size=20),
    ),
    lambda children: st.one_of(
        st.lists(children, max_size=4),
        st.dictionaries(st.text(min_size=1, max_size=8), children, max_size=4),
    ),
    max_leaves=12,
)

_EVENT_TYPES = st.sampled_from(
    [
        "workflow_run",
        "workflow_job",
        "prometheus_alert",
        "prometheus_watchdog",
        "ping",
        "issues",
        "",
    ]
)


@given(
    event_type=_EVENT_TYPES,
    payload=st.dictionaries(st.text(min_size=1, max_size=10), _JSON_PAYLOAD, max_size=6),
)
@settings(max_examples=200, deadline=500)
def test_event_from_webhook_payload_canonical_key_set_invariant(event_type: str, payload: dict[str, Any]) -> None:
    """The EventInsert produced for any payload shape carries exactly the
    canonical payload-derived field-key set (after stripping the handler-supplied
    context fields and the agent-message facet fields the webhook path never
    populates) and echoes `event_type` verbatim.

    `msgspec.structs.asdict(EventInsert)` returns every struct field; stripping
    the handler-supplied context keys and the agent-facet keys recovers the
    payload-derived set that the prior `extract_fields` dict shim returned.
    """
    ei = _build_event(event_type, payload)
    out_keys = set(msgspec.structs.asdict(ei).keys()) - _HANDLER_SUPPLIED_KEYS - _AGENT_FACET_KEYS
    assert out_keys == _EXPECTED_FIELD_KEYS
    assert ei.event_type == event_type


# ---------------------------------------------------------------------------
# systemd-creds-backed secret loading
# ---------------------------------------------------------------------------


import pytest


def test_lookup_secret_returns_bytes_on_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(listener._secrets, "get_secret", lambda name: "secret-value")
    assert listener._lookup_secret("foo") == b"secret-value"


def test_lookup_secret_returns_none_on_missing_credential(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(listener._secrets, "get_secret", lambda name: None)
    assert listener._lookup_secret("missing") is None


def test_lookup_secret_returns_none_on_secret_not_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise(name: str) -> str:
        raise listener._secrets.SecretNotConfigured("credential unreadable")

    monkeypatch.setattr(listener._secrets, "get_secret", _raise)
    assert listener._lookup_secret("foo") is None


def test_load_secret_exits_2_when_unavailable(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(listener._secrets, "get_secret", lambda name: None)
    with pytest.raises(SystemExit) as ei:
        listener.load_secret("foo")
    assert ei.value.code == 2
    err = capsys.readouterr().err
    assert "'foo'" in err


def test_load_secret_returns_bytes_on_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(listener._secrets, "get_secret", lambda name: "ok")
    assert listener.load_secret("foo") == b"ok"


def test_load_secret_optional_returns_none_on_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(listener._secrets, "get_secret", lambda name: None)
    assert listener.load_secret_optional("foo") is None


def test_load_secret_optional_returns_bytes_on_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(listener._secrets, "get_secret", lambda name: "present-bytes")
    assert listener.load_secret_optional("foo") == b"present-bytes"


# ---------------------------------------------------------------------------
# ReusableThreadingServer subclass invariants (shared base in waitbus._http)
# ---------------------------------------------------------------------------


def test_reusable_threading_server_class_attrs() -> None:
    """Both knobs are class-level; tests must not need to instantiate."""
    cls = _http.ReusableThreadingServer
    assert cls.allow_reuse_address is True
    assert cls.daemon_threads is True


# ---------------------------------------------------------------------------
# Schema parser edge cases
# ---------------------------------------------------------------------------


def test_expected_event_columns_raises_when_table_missing() -> None:
    bad_sql = "CREATE TABLE other (x INTEGER);"
    with pytest.raises(RuntimeError, match=r"schema\.sql does not declare"):
        _db._expected_event_columns(sql_text=bad_sql)


def test_expected_event_columns_raises_on_empty_body() -> None:
    bad_sql = "CREATE TABLE IF NOT EXISTS events ();"
    with pytest.raises(RuntimeError, match="zero columns"):
        _db._expected_event_columns(sql_text=bad_sql)


def test_expected_event_columns_skips_table_level_constraint() -> None:
    sql = """
    CREATE TABLE IF NOT EXISTS events (
        delivery_id TEXT PRIMARY KEY,
        event_type TEXT,
        PRIMARY KEY (delivery_id, event_type)
    );
    """
    cols = _db._expected_event_columns(sql_text=sql)
    names = [c[0] for c in cols]
    assert "delivery_id" in names
    assert "event_type" in names
    assert "PRIMARY" not in names


def test_expected_event_columns_raises_on_unparseable_line() -> None:
    sql = """
    CREATE TABLE IF NOT EXISTS events (
        123invalid TEXT
    );
    """
    with pytest.raises(RuntimeError, match="unparseable column"):
        _db._expected_event_columns(sql_text=sql)


def test_ensure_schema_is_idempotent(tmp_db_path: Path) -> None:
    """ensure_schema can be re-run on a populated DB without errors."""
    listener.ensure_schema(tmp_db_path)
    listener.ensure_schema(tmp_db_path)


def test_ensure_schema_adds_missing_column(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Pre-existing DB without a column gets it added by ensure_schema."""
    import sqlite3 as _sql

    db = tmp_path / "stale.db"
    # Build a DB with a sparse events table so the migration must add cols.
    # sqlite3.Connection's context manager only commits/rolls back; it does
    # NOT close the connection. Wrap in contextlib.closing so the underlying
    # handle is released (otherwise Python 3.14 raises ResourceWarning at GC).
    with contextlib.closing(_sql.connect(db)) as conn:
        conn.execute("CREATE TABLE events (delivery_id TEXT PRIMARY KEY, source TEXT, event_type TEXT)")
        conn.commit()
    listener.ensure_schema(db)
    with contextlib.closing(_sql.connect(db)) as conn:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(events)")}
    # All canonical columns from schema.sql must now exist.
    assert "event_id" in cols
    assert "alert_name" in cols


def test_ensure_schema_handles_pending_rename(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A pending column rename fires when the old name still exists."""
    import sqlite3 as _sql

    db = tmp_path / "renamed.db"
    with contextlib.closing(_sql.connect(db)) as conn:
        conn.execute(
            "CREATE TABLE events (delivery_id TEXT PRIMARY KEY, source TEXT, event_type TEXT, old_status TEXT)"
        )
        conn.commit()
    monkeypatch.setattr(_db, "_PENDING_RENAMES", (("old_status", "status"),))
    listener.ensure_schema(db)
    with contextlib.closing(_sql.connect(db)) as conn:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(events)")}
    assert "status" in cols
    assert "old_status" not in cols


def test_ensure_schema_skips_rename_when_both_columns_exist(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Rename is a no-op when both old and new names already exist."""
    import sqlite3 as _sql

    db = tmp_path / "both.db"
    with contextlib.closing(_sql.connect(db)) as conn:
        conn.execute(
            "CREATE TABLE events ("
            "delivery_id TEXT PRIMARY KEY, "
            "source TEXT, "
            "event_type TEXT, "
            "status TEXT, "
            "old_status TEXT)"
        )
        conn.commit()
    monkeypatch.setattr(_db, "_PENDING_RENAMES", (("old_status", "status"),))
    listener.ensure_schema(db)
    with contextlib.closing(_sql.connect(db)) as conn:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(events)")}
    assert "status" in cols
    assert "old_status" in cols  # both still present


# ---------------------------------------------------------------------------
# HTTP hardening tests
# ---------------------------------------------------------------------------


def _raw_post(
    port: int,
    path: str,
    headers: list[tuple[str, str]],
    body: bytes = b"",
    timeout: float = 3.0,
) -> int:
    """Send a raw HTTP/1.1 POST and return the response status code.

    Uses http.client directly so we can inject arbitrary headers
    (including duplicate Content-Length) that urllib would normalise away.
    """
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    conn.connect()
    conn.putrequest("POST", path)
    for name, value in headers:
        conn.putheader(name, value)
    conn.endheaders(body)
    resp = conn.getresponse()
    status = resp.status
    resp.read()
    conn.close()
    return status


def test_rejects_transfer_encoding_chunked(server_fixture: ServerHandle) -> None:
    """Transfer-Encoding: chunked must be rejected with 411.

    GitHub webhooks and Alertmanager deliveries are always fixed-length.
    Chunked encoding would bypass the MAX_BODY_BYTES cap.
    """
    status = _raw_post(
        server_fixture.port,
        "/webhook",
        [
            ("Transfer-Encoding", "chunked"),
            ("Content-Type", "application/json"),
        ],
    )
    assert status == 411


def test_rejects_duplicate_content_length(server_fixture: ServerHandle) -> None:
    """Two Content-Length headers must be rejected with 400 (RFC 9112 §6.3.3)."""
    status = _raw_post(
        server_fixture.port,
        "/webhook",
        [
            ("Content-Length", "2"),
            ("Content-Length", "1000"),
        ],
        body=b"{}",
    )
    assert status == 400


def test_rejects_non_numeric_content_length(server_fixture: ServerHandle) -> None:
    """A non-integer Content-Length value must be rejected with 400."""
    status = _raw_post(
        server_fixture.port,
        "/webhook",
        [("Content-Length", "abc")],
        body=b"{}",
    )
    assert status == 400


def test_rejects_expect_100_continue(server_fixture: ServerHandle) -> None:
    """Expect: 100-continue must be rejected with 417.

    The listener does not implement the 100-continue flow; clients that
    send this header may block waiting for a continue that never arrives.
    """
    body = b'{"x":1}'
    status = _raw_post(
        server_fixture.port,
        "/webhook",
        [
            ("Content-Length", str(len(body))),
            ("Expect", "100-continue"),
        ],
        body=body,
    )
    assert status == 417


def _do_method(port: int, method: str, timeout: float = 3.0) -> tuple[int, bytes]:
    """Issue an arbitrary HTTP method to /webhook and return (status, body)."""
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    conn.connect()
    conn.putrequest(method, "/webhook")
    conn.putheader("Content-Length", "0")
    conn.endheaders()
    resp = conn.getresponse()
    status = resp.status
    body = resp.read()
    conn.close()
    return status, body


def test_returns_405_for_head(server_fixture: ServerHandle) -> None:
    status, _ = _do_method(server_fixture.port, "HEAD")
    assert status == 405


def test_returns_405_for_options(server_fixture: ServerHandle) -> None:
    status, _ = _do_method(server_fixture.port, "OPTIONS")
    assert status == 405


def test_returns_405_for_put(server_fixture: ServerHandle) -> None:
    status, _ = _do_method(server_fixture.port, "PUT")
    assert status == 405


def test_returns_405_for_delete(server_fixture: ServerHandle) -> None:
    status, _ = _do_method(server_fixture.port, "DELETE")
    assert status == 405


def test_returns_405_for_patch(server_fixture: ServerHandle) -> None:
    status, _ = _do_method(server_fixture.port, "PATCH")
    assert status == 405


def test_405_response_body_is_json(server_fixture: ServerHandle) -> None:
    """The 405 response must carry a JSON body, not an HTML error page."""
    _, body = _do_method(server_fixture.port, "PUT")
    parsed = json.loads(body.decode("utf-8"))
    assert "error" in parsed


def test_405_response_has_allow_header(server_fixture: ServerHandle) -> None:
    """The 405 response must include an Allow header listing POST and GET."""
    conn = http.client.HTTPConnection("127.0.0.1", server_fixture.port, timeout=3)
    conn.connect()
    conn.putrequest("DELETE", "/webhook")
    conn.putheader("Content-Length", "0")
    conn.endheaders()
    resp = conn.getresponse()
    resp.read()
    allow = resp.getheader("Allow") or ""
    conn.close()
    assert "POST" in allow
    assert "GET" in allow


def test_strips_server_response_header(server_fixture: ServerHandle) -> None:
    """GET /healthz must not include a Server header carrying version info.

    Returning '' from version_string() suppresses the header in CPython
    3.11+.  We accept either a missing header or an empty value — both
    satisfy the info-disclosure prevention goal.
    """
    with urllib.request.urlopen(server_fixture.url("/healthz"), timeout=2) as resp:
        server_header = resp.getheader("Server") or ""
    # Should be absent or empty — must not contain recognisable software names.
    assert "Python" not in server_header
    assert "BaseHTTP" not in server_header


def test_recursion_error_on_json_parse_returns_400(
    monkeypatch: pytest.MonkeyPatch,
    server_fixture: ServerHandle,
    gh_secret: bytes,
    hmac_sig: Callable[[bytes, bytes], str],
) -> None:
    """A RecursionError during JSON parsing must return 400.

    Python's json.loads() can raise RecursionError on deeply nested input
    when using the pure-Python decoder (e.g. after sys.setrecursionlimit)
    or on implementations without a C-accelerated parser. We treat it
    the same as JSONDecodeError — a 400 bad request — and this test
    injects the exception to verify the handler catches it correctly.
    """
    import json as _json

    def _raise_recursion(data: Any, **kwargs: Any) -> Any:
        raise RecursionError("maximum recursion depth exceeded")

    monkeypatch.setattr(_json, "loads", _raise_recursion)

    body = b'{"x":1}'
    code, _ = _post(
        server_fixture.url("/webhook"),
        body,
        {
            "Content-Length": str(len(body)),
            "X-Hub-Signature-256": hmac_sig(gh_secret, body),
            "X-GitHub-Event": "workflow_run",
            "X-GitHub-Delivery": "recursion-test",
        },
        timeout=5.0,
    )
    assert code == 400


def test_slow_loris_on_request_line_times_out(server_fixture: ServerHandle) -> None:
    """A client that connects but sends only 1 byte must be dropped.

    The setup() override applies REQUEST_READ_TIMEOUT_SEC to the socket
    before parse_request() runs, so the timeout covers slow-loris attacks
    on the request line and headers — not only on the body.

    This test uses a very short timeout override to keep wall-clock time
    reasonable in CI.
    """
    original_timeout = listener.REQUEST_READ_TIMEOUT_SEC
    # Patch the module-level constant; setup() reads it at connection time.
    listener.REQUEST_READ_TIMEOUT_SEC = 0.3

    try:
        sock = socket.create_connection(("127.0.0.1", server_fixture.port), timeout=5)
        sock.sendall(b"G")  # partial request line — never completes
        # The server must close the connection within timeout + fudge.
        deadline = time.monotonic() + original_timeout + 2.0
        got_eof = False
        while time.monotonic() < deadline:
            try:
                chunk = sock.recv(1024)
            except (OSError, ConnectionResetError):
                got_eof = True
                break
            if chunk == b"":
                got_eof = True
                break
            time.sleep(0.05)
        sock.close()
        assert got_eof, "Server did not close slow-loris connection within budget"
    finally:
        listener.REQUEST_READ_TIMEOUT_SEC = original_timeout
