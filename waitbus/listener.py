#!/usr/bin/env python3
"""Webhook listener for waitbus; routes documented per-handler.

Routes:
- /webhook              GitHub workflow events
- /alertmanager         Prometheus alerts (optional shared secret)
- /watchdog             Prometheus watchdog ping
- /healthz              liveness probe
"""

from __future__ import annotations

import contextlib
import hashlib
import hmac
import json
import logging
import signal
import sqlite3
import sys
import time

# ThreadingHTTPServer dispatches each request on its own worker thread.
# The webhook handlers do non-trivial SQLite work (INSERT OR IGNORE plus a
# doorbell ring), and a single-threaded HTTPServer would stall every
# follow-up webhook delivery for the duration of one slow insert. Each
# handler opens its own short-lived sqlite3 connection inside do_POST, so
# there is no shared mutable handler state across threads.
from http.server import BaseHTTPRequestHandler
from typing import Any

from . import _config, _db, _metrics
from . import _secrets as _secrets
from ._db import ensure_schema as ensure_schema
from ._db import insert_event
from ._http import ReusableThreadingServer
from ._log import structured
from ._paths import db_path, ensure_state_dirs
from ._sdnotify import sd_notify
from ._types import EventInsert

LISTEN_HOST = "127.0.0.1"
LISTEN_PORT = 9000
GITHUB_WEBHOOK_CRED = "github-webhook-secret"
"""systemd-creds credential name for the GitHub webhook HMAC secret.
Load-bearing: the listener exits 2 at startup if the credential is
missing. Stage via ``waitbus install-credentials github-webhook-secret``."""
ALERTMANAGER_HMAC_CRED = "alertmanager-hmac"
"""systemd-creds credential name for the Alertmanager / watchdog HMAC
secret. Optional: when absent, /alertmanager and /watchdog respond 503
but /webhook stays up. Stage via ``waitbus install-credentials
alertmanager-hmac``."""
MAX_BODY_BYTES = 10 * 1024 * 1024  # 10 MiB
REQUEST_READ_TIMEOUT_SEC: float = 30
"""Per-request socket-level read timeout. Defends against slow-drip body
reads (sender drips bytes at 1/min to hold a thread forever). 30s is
generous for any legitimate webhook delivery; GitHub itself times out
its delivery at 10s, so a real webhook always completes within this
budget. Anything longer is misbehaviour or attack."""
ALERT_EVENT_TYPES = frozenset({"prometheus_alert", "prometheus_watchdog"})

logger = logging.getLogger("waitbus.listener")


def _lookup_secret(name: str) -> bytes | None:
    """Return the credential value as bytes, or None on any failure.

    Wraps `_secrets.get_secret` to absorb credential-infrastructure
    failures (unreadable file) as "secret unavailable" — load_secret
    fails fast on None for load-bearing secrets, load_secret_optional
    keeps the daemon running on None for the optional paths.
    """
    try:
        value = _secrets.get_secret(name)
    except _secrets.SecretNotConfigured:
        return None
    if not value:
        return None
    return value.encode("utf-8")


def load_secret(name: str) -> bytes:
    """Fetch a credential from systemd-creds. Exit 2 if missing or unavailable.

    Args:
        name: credential name exposed by ``LoadCredentialEncrypted=<name>:...``
            in the listener unit. Read from ``$CREDENTIALS_DIRECTORY/<name>``.
    """
    secret = _lookup_secret(name)
    if secret is None:
        sys.stderr.write(
            f"waitbus listener: credential {name!r} not found in "
            "$CREDENTIALS_DIRECTORY. Stage it via "
            f"`waitbus install-credentials {name}` and ensure the unit "
            f"declares `LoadCredentialEncrypted={name}:...`.\n"
        )
        sys.exit(2)
    return secret


def load_secret_optional(name: str) -> bytes | None:
    """Look up a credential, returning None when it is unavailable.

    Used for ingress paths that are not load-bearing for every
    deployment (e.g. the /alertmanager Prometheus bridge is only active
    on hosts that wire an upstream alertmanager to waitbus). When the
    credential is missing the listener still binds the GitHub /webhook
    path; the optional path returns 503 service-unavailable to the
    caller, with a one-line warning log at startup naming the missing
    credential so operators can provision it later if needed.
    """
    return _lookup_secret(name)


def _positive_int_or_none(value: Any) -> int | None:
    """Return ``value`` iff it is a positive int domain value, else None.

    Narrows webhook-derived id columns (``run_id``, ``job_id``,
    ``parent_run_id``) to GitHub's documented API contract: every workflow
    run / job id is a positive int64. ``type(v) is int`` (NOT isinstance)
    excludes bool — ``True``/``False`` are int subclasses and would
    otherwise pass through as ``1``/``0`` polluting the entity-key space.

    Defence-in-depth pair with ``_terminal.entity_key``'s consumer-side
    narrowing: GitHub never emits ``run_id=0`` in real flow; an attacker
    with the webhook secret could craft a malformed payload to reach
    that state. Returning None coerces it to a NULL identity column at
    DB insertion time so the daemon never minted a bogus collapse key.
    """
    if type(value) is int and value > 0:
        return value
    return None


def _event_from_webhook_payload(
    event_type: str,
    payload: dict[str, Any],
    *,
    delivery_id: str,
    source: str,
    received_at_ns: int,
    payload_json: str,
    ingest_method: str,
) -> EventInsert:
    """Build a typed EventInsert from a parsed upstream webhook payload.

    Handles workflow_run (run-level state), workflow_job (per-job state,
    sub-second on failure), and prometheus_alert / prometheus_watchdog
    (Alertmanager v4 webhooks). For workflow_job, workflow_name is left
    NULL and is joinable via parent_run_id. For alert events, the GitHub-
    shaped columns are NULL and the alert_* columns carry the payload.

    Webhook payloads are user-supplied JSON; every container access must
    tolerate a non-dict value at any level (e.g. a list or null in place
    of an object). Coerce non-dict / non-list / non-str values to safe
    empties so the EventInsert construction sees only typed values.
    """
    owner: str = ""
    repo_name: str = ""
    run_id: int | None = None
    workflow_name: str | None = None
    head_branch: str | None = None
    head_sha: str | None = None
    status: str | None = None
    conclusion: str | None = None
    job_id: int | None = None
    job_name: str | None = None
    parent_run_id: int | None = None
    alert_name: str | None = None
    alert_severity: str | None = None
    alert_fingerprint: str | None = None

    if event_type in ALERT_EVENT_TYPES:
        cfg = _config.get_config()
        # Defensive against malformed Alertmanager payloads: every nested
        # accessor coerces non-dict / non-list values to safe empties.
        alerts_raw = payload.get("alerts")
        alerts = alerts_raw if isinstance(alerts_raw, list) else []
        first_raw = alerts[0] if alerts else None
        first = first_raw if isinstance(first_raw, dict) else {}
        labels_raw = first.get("labels")
        labels = labels_raw if isinstance(labels_raw, dict) else {}
        owner = cfg.prom_owner
        repo_name = cfg.prom_repo
        status_raw = payload.get("status")
        status = status_raw if isinstance(status_raw, str) else None
        alert_name = labels.get("alertname")
        alert_severity = labels.get("severity")
        alert_fingerprint = first.get("fingerprint")
    else:
        repo_raw = payload.get("repository")
        repo_dict = repo_raw if isinstance(repo_raw, dict) else {}
        owner_raw = repo_dict.get("owner")
        owner_obj = owner_raw if isinstance(owner_raw, dict) else {}
        owner = owner_obj.get("login") or ""
        repo_name = repo_dict.get("name") or ""
        if event_type == "workflow_run":
            run_raw = payload.get("workflow_run")
            run = run_raw if isinstance(run_raw, dict) else {}
            run_id = _positive_int_or_none(run.get("id"))
            workflow_name = run.get("name")
            head_branch = run.get("head_branch")
            head_sha = run.get("head_sha")
            status = run.get("status")
            conclusion = run.get("conclusion")
        elif event_type == "workflow_job":
            job_raw = payload.get("workflow_job")
            job = job_raw if isinstance(job_raw, dict) else {}
            head_branch = job.get("head_branch")
            head_sha = job.get("head_sha")
            status = job.get("status")
            conclusion = job.get("conclusion")
            job_id = _positive_int_or_none(job.get("id"))
            job_name = job.get("name")
            parent_run_id = _positive_int_or_none(job.get("run_id"))

    return EventInsert(
        delivery_id=delivery_id,
        source=source,
        event_type=event_type,
        owner=owner,
        repo=repo_name,
        received_at=received_at_ns,
        payload_json=payload_json,
        ingest_method=ingest_method,
        run_id=run_id,
        workflow_name=workflow_name,
        head_branch=head_branch,
        head_sha=head_sha,
        status=status,
        conclusion=conclusion,
        job_id=job_id,
        job_name=job_name,
        parent_run_id=parent_run_id,
        alert_name=alert_name,
        alert_severity=alert_severity,
        alert_fingerprint=alert_fingerprint,
    )


class WebhookHandler(BaseHTTPRequestHandler):
    """HTTP route handler for the waitbus listener daemon.

    Class-level attributes ``secret`` (required) and ``am_secret``
    (optional) carry the HMAC keys for each route; ``main`` populates
    them at startup so every request handler instance shares the
    same key material without re-fetching from the keyring per
    request. The lifetime model is "one daemon, one keyring lookup
    at boot": rotation requires a daemon restart.
    """

    # Strip the Server response header so the listener does not advertise
    # its implementation details to any client on the loopback interface.
    server_version = ""
    sys_version = ""
    secret: bytes = b""
    # am_secret is None when the optional waitbus/alertmanager-hmac key
    # is not provisioned. The /alertmanager and /watchdog handlers return
    # 503 in that case; the GitHub /webhook path is unaffected.
    am_secret: bytes | None = None

    def setup(self) -> None:
        """Apply the read timeout to the socket before any header parsing.

        BaseHTTPRequestHandler calls `parse_request()` to read the
        request line and headers before `do_POST` / `do_GET` run.
        Setting the timeout here ensures that a slow-loris attack on
        the request line or headers is bounded by the same wall-clock
        budget as the body read — without this override only the body
        read (inside `_read_body`) was covered.
        """
        super().setup()
        self.connection.settimeout(REQUEST_READ_TIMEOUT_SEC)

    def version_string(self) -> str:
        """Return an empty version string to suppress the Server header.

        BaseHTTPRequestHandler builds 'Server: <version_string()>' in
        send_response(). Returning '' causes the header to be omitted
        entirely in CPython 3.11+, preventing version fingerprinting.
        """
        return ""

    # Silence default stderr access log; we emit structured logs ourselves.
    def log_message(self, format: str, *args: Any) -> None:
        """Suppress the BaseHTTPRequestHandler stderr access log.

        The daemon emits its own structured JSON log via ``_log``;
        the default access-log format duplicates that information
        with a less-grep-friendly layout.
        """
        return

    def log_error(self, format: str, *args: Any) -> None:
        """Route handler-level errors through the structured logger.

        BaseHTTPRequestHandler writes error messages (e.g. broken pipe
        on a prematurely closed connection) to stderr via log_message.
        Routing them through the structured JSON logger keeps the error
        stream grep-friendly and prevents any implicit version info from
        appearing in log output.
        """
        structured(logger, logging.WARNING, "handler_error", msg=format % args if args else format)

    def _send(self, code: int, body: dict[str, Any]) -> None:
        """Write a JSON response with explicit Content-Length and Content-Type."""
        payload = json.dumps(body).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _method_not_allowed(self) -> None:
        """Return a JSON 405 with an Allow header listing the supported methods."""
        body = b'{"error":"method not allowed"}'
        self.send_response(405)
        self.send_header("Allow", "POST, GET")
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_HEAD(self) -> None:
        self._method_not_allowed()

    def do_OPTIONS(self) -> None:
        self._method_not_allowed()

    def do_PUT(self) -> None:
        self._method_not_allowed()

    def do_DELETE(self) -> None:
        self._method_not_allowed()

    def do_PATCH(self) -> None:
        self._method_not_allowed()

    def _check_request_framing(self) -> bool:
        """Validate HTTP framing headers before reading the body.

        Checks Transfer-Encoding, duplicate Content-Length, non-numeric
        Content-Length, out-of-range Content-Length, and Expect headers.
        Sends the appropriate error response and returns False if any
        check fails; returns True when the request is well-formed and
        safe to proceed to body reading.

        Rationale for each check:
        - Transfer-Encoding chunked: bypasses the Content-Length body cap;
          GitHub and Alertmanager never chunk, so reject with 411.
        - Duplicate Content-Length: RFC 9112 §6.3.3 says reject; an
          attacker can smuggle a large payload by providing two values.
        - Non-numeric Content-Length: a 400; the body read would
          silently treat it as 0.
        - Out-of-range Content-Length: 413; mirrors the MAX_BODY_BYTES cap
          and guards against negative values.
        - Expect: 100-continue: we do not implement the 100 flow; callers
          that send it may block waiting for a continue that never arrives,
          so reject eagerly with 417.
        """
        # Transfer-Encoding: chunked bypasses the Content-Length body cap.
        if any(h.lower() == "transfer-encoding" for h in self.headers):
            self.send_error(411, "Transfer-Encoding not supported; send a fixed Content-Length")
            return False

        # Duplicate Content-Length is a smuggling vector (RFC 9112 §6.3.3).
        cl_values = self.headers.get_all("Content-Length") or []
        if len(cl_values) > 1:
            self.send_error(400, "Multiple Content-Length headers")
            return False

        # Missing Content-Length: we require it.
        cl_raw = self.headers.get("Content-Length")
        if cl_raw is None:
            self.send_error(411, "Content-Length required")
            return False

        # Non-numeric Content-Length.
        try:
            content_length = int(cl_raw)
        except ValueError:
            self.send_error(400, "Invalid Content-Length")
            return False

        # Out-of-range (negative or oversized).
        if content_length < 0 or content_length > MAX_BODY_BYTES:
            _metrics.incr("waitbus_webhook_bad_length_total", path=self.path)
            self.send_error(413, "Content-Length out of range")
            return False

        # Expect: 100-continue — we do not implement the continue flow.
        if self.headers.get("Expect", "").lower() == "100-continue":
            self.send_error(417, "Expectation Failed")
            return False

        return True

    def _read_body(self) -> bytes | None:
        """Read the request body, enforcing Content-Length bounds.

        Returns the body bytes on success, or None after sending an error
        response on bad length or read timeout.

        Callers must invoke ``_check_request_framing()`` before calling
        this method; ``_read_body`` assumes Content-Length is a valid
        positive integer that does not exceed MAX_BODY_BYTES.

        The socket-level timeout applied in ``setup()`` defends against
        slow-drip readers that hold a thread by streaming the body at
        < 1 B/s. Stalled connections are dropped with a 408.
        """
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0 or length > MAX_BODY_BYTES:
            _metrics.incr("waitbus_webhook_bad_length_total", path=self.path)
            self._send(400, {"status": "bad_length"})
            return None
        try:
            body = self.rfile.read(length)
        except (TimeoutError, OSError) as exc:
            structured(logger, logging.WARNING, "body_read_timeout", error=str(exc), path=self.path)
            _metrics.incr("waitbus_webhook_read_timeout_total", path=self.path)
            # If the peer is gone, the 408 response itself can't be sent —
            # swallow that secondary OSError so we don't pollute the log.
            with contextlib.suppress(OSError):
                self._send(408, {"status": "read_timeout"})
            return None
        if len(body) != length:
            structured(
                logger, logging.WARNING, "body_length_mismatch", path=self.path, declared=length, actual=len(body)
            )
            _metrics.incr("waitbus_webhook_bad_length_total", path=self.path)
            self._send(400, {"status": "bad_length"})
            return None
        return body

    def _verify_hmac(self, body: bytes, secret: bytes) -> bool:
        """Constant-time check of X-Hub-Signature-256 against the given secret.

        Sends the appropriate 401 response on missing/malformed/invalid
        signature. Returns True iff the signature is valid.
        """
        received_sig = self.headers.get("X-Hub-Signature-256", "")
        if not received_sig.startswith("sha256="):
            structured(logger, logging.WARNING, "sig_missing", path=self.path)
            _metrics.incr("waitbus_webhook_hmac_rejected_total", path=self.path, reason="missing")
            self._send(401, {"status": "missing_signature"})
            return False
        provided_hex = received_sig[len("sha256=") :]
        # A correct sha256 hex digest is exactly 64 lowercase hex chars.
        # Catching length errors here lets the operator distinguish "wrong
        # secret" (mismatch) from "garbage header" (malformed) in logs.
        if len(provided_hex) != 64:
            structured(logger, logging.WARNING, "sig_malformed", path=self.path, provided_len=len(provided_hex))
            _metrics.incr("waitbus_webhook_hmac_rejected_total", path=self.path, reason="malformed")
            self._send(401, {"status": "malformed_signature"})
            return False
        expected = hmac.new(secret, body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, provided_hex):
            structured(logger, logging.WARNING, "sig_mismatch", path=self.path)
            _metrics.incr("waitbus_webhook_hmac_rejected_total", path=self.path, reason="mismatch")
            self._send(401, {"status": "invalid_signature"})
            return False
        return True

    def do_POST(self) -> None:
        """Route incoming POSTs to the GitHub / Alertmanager / watchdog handlers.

        Returns 404 for any other path. /alertmanager and /watchdog
        return 503 when the alertmanager HMAC key is unprovisioned;
        /webhook is unaffected by alertmanager-key state and is the
        load-bearing path for the GitHub ingest flow.

        Request framing is validated before dispatch: Transfer-Encoding,
        duplicate Content-Length, non-numeric Content-Length, out-of-range
        Content-Length, and Expect: 100-continue are all rejected here so
        every downstream handler receives a well-formed fixed-length body.
        """
        _metrics.incr("waitbus_webhook_received_total", path=self.path)
        if not self._check_request_framing():
            return
        if self.path == "/webhook":
            self._handle_github()
            return
        if self.path == "/alertmanager":
            if self.am_secret is None:
                self._send(503, {"status": "alertmanager_secret_unprovisioned"})
                return
            self._handle_alertmanager("prometheus_alert")
            return
        if self.path == "/watchdog":
            # Reciprocal heartbeat path: receives the always-firing
            # vector(1) Watchdog alert from the monitor VM. Same HMAC
            # contract and payload shape as /alertmanager, distinct
            # event_type so the watchdog absence detector can query
            # for it independently.
            if self.am_secret is None:
                self._send(503, {"status": "alertmanager_secret_unprovisioned"})
                return
            self._handle_alertmanager("prometheus_watchdog")
            return
        self._send(404, {"status": "not_found"})

    def _handle_github(self) -> None:
        """Verify HMAC, parse JSON, extract fields, and insert one event row.

        Ignores event types other than workflow_run / workflow_job
        with a 200 ``ignored`` reply (so GitHub stops retrying).
        Missing X-GitHub-Delivery is a 400, malformed JSON is a 400,
        sqlite errors yield a 500 — every error path emits a metric
        before responding so operators can attribute traffic from
        the /metrics endpoint without correlating logs.
        """
        body = self._read_body()
        if body is None:
            return
        if not self._verify_hmac(body, self.secret):
            return

        event_type = self.headers.get("X-GitHub-Event", "")
        delivery_id = self.headers.get("X-GitHub-Delivery", "")
        if not delivery_id:
            self._send(400, {"status": "missing_delivery_id"})
            return

        if event_type not in ("workflow_run", "workflow_job"):
            structured(logger, logging.INFO, "ignored", event_type=event_type, delivery=delivery_id)
            _metrics.incr("waitbus_webhook_ignored_total", path=self.path, event_type=event_type or "unset")
            self._send(200, {"status": "ignored"})
            return

        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
            structured(logger, logging.WARNING, "bad_json", error=str(exc))
            _metrics.incr("waitbus_webhook_bad_json_total", path=self.path)
            self._send(400, {"status": "bad_json"})
            return

        body_text = body.decode("utf-8", errors="replace")
        event = _event_from_webhook_payload(
            event_type,
            payload,
            delivery_id=delivery_id,
            source="github",
            received_at_ns=time.time_ns(),
            payload_json=body_text,
            ingest_method="webhook",
        )
        try:
            with _db.connect(db_path()) as conn:
                insert_event(conn, event)
        except sqlite3.Error as exc:
            structured(logger, logging.ERROR, "db_error", error=str(exc), delivery=delivery_id)
            _metrics.incr("waitbus_db_error_total", path=self.path, source="github")
            self._send(500, {"status": "db_error"})
            return

        structured(
            logger,
            logging.INFO,
            "stored",
            delivery=delivery_id,
            owner=event.owner,
            repo=event.repo,
            run_id=event.run_id,
            status=event.status,
            conclusion=event.conclusion,
        )
        self._send(200, {"status": "ok"})

    def _handle_alertmanager(self, event_type: str) -> None:
        """Verify HMAC, parse JSON, store under the supplied ``event_type``.

        Shared between /alertmanager (event_type=prometheus_alert)
        and /watchdog (event_type=prometheus_watchdog). The
        distinction matters for the watchdog-absence detector:
        operators query for the most recent prometheus_watchdog row
        independently from real alert rows.
        """
        # do_POST guards every Alertmanager path against am_secret=None
        # before dispatching here; the assert narrows the type for the
        # _verify_hmac call without re-checking at runtime.
        assert self.am_secret is not None
        body = self._read_body()
        if body is None:
            return
        if not self._verify_hmac(body, self.am_secret):
            return

        delivery_id = self.headers.get("X-Alertmanager-Delivery", "")
        if not delivery_id:
            self._send(400, {"status": "missing_delivery_id"})
            return

        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
            structured(logger, logging.WARNING, "bad_json", error=str(exc), path=self.path)
            _metrics.incr("waitbus_webhook_bad_json_total", path=self.path)
            self._send(400, {"status": "bad_json"})
            return

        body_text = body.decode("utf-8", errors="replace")
        event = _event_from_webhook_payload(
            event_type,
            payload,
            delivery_id=delivery_id,
            source="alertmanager",
            received_at_ns=time.time_ns(),
            payload_json=body_text,
            ingest_method="webhook",
        )
        try:
            with _db.connect(db_path()) as conn:
                insert_event(conn, event)
        except sqlite3.Error as exc:
            structured(logger, logging.ERROR, "db_error", error=str(exc), delivery=delivery_id)
            _metrics.incr("waitbus_db_error_total", path=self.path, source="alertmanager")
            self._send(500, {"status": "db_error"})
            return

        structured(
            logger,
            logging.INFO,
            "alert_stored",
            event_type=event_type,
            delivery=delivery_id,
            alert_name=event.alert_name,
            alert_severity=event.alert_severity,
            status=event.status,
        )
        self._send(200, {"status": "ok"})

    def do_GET(self) -> None:
        """Serve liveness (``/healthz``) and Prometheus scrape (``/metrics``).

        ``/healthz`` returns a tiny JSON ``{"status":"ok"}`` for use
        in shell-prompt indicators and shallow probes. ``/metrics``
        exposes the in-process counter map in the Prometheus text
        exposition format. Any other path returns 404.
        """
        if self.path == "/healthz":
            self._send(200, {"status": "ok"})
            return
        if self.path == "/metrics":
            payload = _metrics.render()
            self.send_response(200)
            self.send_header(
                "Content-Type",
                "text/plain; version=0.0.4; charset=utf-8",
            )
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        self._send(404, {"status": "not_found"})


def build_server(secret: bytes, am_secret: bytes | None) -> ReusableThreadingServer:
    """Construct the bound webhook HTTP server without signal or exit side effects.

    Installs the HMAC key material on :class:`WebhookHandler` and returns a
    bound :class:`ReusableThreadingServer` on ``LISTEN_HOST:LISTEN_PORT``.
    Signal installation and ``sys.exit`` remain ``main()``-only concerns: an
    embedding supervisor needs the server object without them
    (``signal.signal`` raises ``ValueError`` off the main thread, and
    ``load_secret`` exits the whole process on a missing credential).
    """
    WebhookHandler.secret = secret
    WebhookHandler.am_secret = am_secret
    return ReusableThreadingServer((LISTEN_HOST, LISTEN_PORT), WebhookHandler)


def main() -> int:
    """Entry point for the ``waitbus listener serve`` sub-command.

    Bootstrap order matters: state dirs must exist before schema
    creation (the parent dir of the DB), schema must exist before
    the secret lookup (so a missing credential fails loudly without
    corrupting a half-built DB), and signal handlers must be set
    before ``serve_forever`` so SIGTERM during a slow first request
    can still tear down the server cleanly.

    Configuration is environment-driven via ``CiStatusConfig``; the
    daemon takes no positional or named CLI arguments. The typer wrapper
    rejects extra args via
    its default no-allow-extra config so a misconfigured systemd unit
    fails loudly at exec time rather than silently dropping flags.
    """
    cfg = _config.get_config()
    logging.basicConfig(
        level=cfg.log_level,
        format="%(message)s",
        stream=sys.stderr,
    )
    ensure_state_dirs()
    ensure_schema(db_path())
    # Discover entry-point plugin sources so EventInsert.__post_init__'s
    # source-validation accepts plugin-registered names. Shares the
    # process-singleton registry with the broadcast daemon; idempotent so
    # the (rare) case of a single process running both daemons in-process
    # (tests, embedded mode) doesn't re-walk entry-points or
    # re-attest wheels. Policy failures abort listener startup (no events
    # accepted is preferable to silently mis-routing a plugin's events).
    from .sources._registry import discover_plugins_once

    discover_plugins_once()
    server = build_server(
        load_secret(GITHUB_WEBHOOK_CRED),
        load_secret_optional(ALERTMANAGER_HMAC_CRED),
    )

    def _shutdown(signum: int, _frame: Any) -> None:
        """Signal handler: ask the HTTPServer to stop accepting and join."""
        structured(logger, logging.INFO, "shutdown", signal=signum)
        server.shutdown()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)
    structured(logger, logging.INFO, "listening", host=LISTEN_HOST, port=LISTEN_PORT, db=str(db_path()))
    sd_notify(b"READY=1\nSTATUS=accepting webhooks\n")
    try:
        server.serve_forever()
    finally:
        sd_notify(b"STOPPING=1\n")
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
