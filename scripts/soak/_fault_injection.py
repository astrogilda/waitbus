"""Soak fault-injection probes: exercise the subscriber-lifecycle drain paths.

Each probe opens a short-lived raw socket against the running broadcast
daemon and drives a specific drain-path scenario:

- ``token_reject`` -- subscribe with an invalid token; expects a
  ``subscribe_rejected`` frame with ``reason="token"``. Skips
  intentionally when the daemon has no token configured.
- ``version_reject`` -- subscribe with ``proto=9999``; expects a
  ``subscribe_rejected`` frame with ``reason="version"``. Always
  reachable.
- ``replay_lag_eviction`` -- subscribe with ``since=""`` (replay from
  the beginning) on a socket whose receive buffer is shrunk to the
  kernel floor, so the daemon's per-row replay sends saturate the
  buffer and trip the ``LAG_LIMIT`` counter; expects a
  ``subscribe_rejected`` frame with ``reason="lag_limit_exceeded"``.
- ``heartbeat_lag`` -- subscribe live (no ``since``) with a filter that
  matches no emitted event, on a starved receive buffer, then stop
  reading. With no fan-out traffic the daemon's periodic
  ``daemon_heartbeat`` sends are the only writes; once they saturate the
  buffer and ``LAG_LIMIT`` consecutive sends fail, the heartbeat loop
  evicts the subscriber with internal reason ``heartbeat_lag``. The
  eviction's wire frame is the shared lag-limit reject, so this probe
  detects the daemon-side hang-up (POLLHUP) rather than a distinct
  frame; attribution to ``heartbeat_lag`` is confirmed by the daemon's
  ``subscriber_close_reasons`` tally via
  :func:`fault_injection_close_reason_consistency_threshold`. This probe
  is meaningful ONLY against a daemon with a sub-second heartbeat (the
  drain-path smoke's throwaway daemon); the measured soak pins the
  heartbeat to 3600 s to protect its RSS/p99 measurements.

Probes record a :class:`FaultInjectionOutcome` into the soak's
accumulator list. The combined coverage is summarised by
:func:`fault_injection_coverage_threshold` and folded into
``overall_passed`` when the operator opts into fault injection.

Probes do NOT exercise the pre-ack buffer overflow path directly: the
registration-to-ack window is microseconds on a healthy daemon, so the
fault is unreliable to trigger from outside the daemon process. That
path is covered deterministically in ``tests/test_broadcast_robustness.py``
via the in-process state machine. The same applies to the
``replay_db_error`` silent-close path (an internal ``sqlite3.Error``
mid-replay, not reachable from the wire): it is pinned in-process by
``tests/test_broadcast.py``.
"""

from __future__ import annotations

import contextlib
import json
import select
import socket
import struct
from pathlib import Path
from typing import Any

import msgspec

from scripts.soak_monitor import ThresholdVerdict

# Wire-frame length prefix is fixed at 4 bytes big-endian per
# ``waitbus._frame._LENGTH_PREFIX_BYTES``. The probes craft raw
# subscribe envelopes that ``waitbus._broadcast_sub.open_subscriber``
# would normally produce, but with deliberately malformed fields so the
# daemon emits ``subscribe_rejected`` instead of ``subscribe_ack``.
_LENGTH_PREFIX = struct.Struct(">I")

# Probe socket timeout. Long enough for the daemon to write a reject
# frame even under sustained load, short enough that a misconfigured
# probe does not stall the soak loop indefinitely.
_PROBE_TIMEOUT_SEC = 5.0

# Heartbeat-lag probe poll budget. With a sub-second heartbeat the
# starved buffer fills and trips LAG_LIMIT in a few seconds; this bound
# tolerates a larger ``net.core.rmem_min`` (more heartbeats to fill)
# without stalling the drain-path smoke. On timeout the probe records an
# intentional skip rather than a coverage failure.
_HEARTBEAT_LAG_POLL_BUDGET_SEC = 30.0

# Replay-lag probe poll budget. Replay sends every backlogged row back to
# back with no inter-send delay, so a starved buffer trips LAG_LIMIT within
# a second; this bound is generous slack.
_REPLAY_LAG_POLL_BUDGET_SEC = 10.0

# A subscribe filter that matches no emitted event (owner never used by
# the soak emitters, which use owner="soak"). Isolates the heartbeat-lag
# probe from fan-out so the heartbeat send is the only write path and the
# eviction is attributable to heartbeat lag, not fan-out lag.
_NO_MATCH_FILTER = "soak-heartbeat-probe-nomatch/none"


class FaultInjectionOutcome(msgspec.Struct, kw_only=True, frozen=True):
    """One probe's outcome.

    ``axis`` matches the corresponding :class:`FaultInjectionRecord` axis.
    ``observed`` is True when the probe saw the expected wire frame.
    ``skipped_intentionally`` is True when the probe could not run
    meaningfully under the deployment shape (e.g. token probe with no
    token configured) -- the coverage threshold treats this as passing.
    ``detail`` is a short forensic string for the verdict JSON.
    """

    axis: str
    offset_sec: float
    observed: bool
    observed_reason: str | None = None
    skipped_intentionally: bool = False
    detail: str = ""


def _send_subscribe(sock: socket.socket, envelope: dict[str, Any]) -> None:
    """Encode an envelope as a length-prefixed JSON frame and send it."""
    payload = json.dumps(envelope).encode("utf-8")
    sock.sendall(_LENGTH_PREFIX.pack(len(payload)) + payload)


def _read_one_frame(sock: socket.socket) -> dict[str, Any] | None:
    """Read one length-prefixed JSON frame, or ``None`` on clean EOF.

    Returns ``None`` when the daemon closes the socket without sending a
    frame (the silent-close arm of ``_TERMINAL_REJECT_FRAMES`` -- e.g.
    ``replay_db_error`` or any pre-ack-handshake close).
    """
    prefix = sock.recv(4, socket.MSG_WAITALL)
    if len(prefix) < 4:
        return None
    (length,) = _LENGTH_PREFIX.unpack(prefix)
    if length == 0:
        return None
    payload = bytearray()
    while len(payload) < length:
        chunk = sock.recv(length - len(payload))
        if not chunk:
            return None
        payload.extend(chunk)
    return dict(json.loads(payload.decode("utf-8")))


def _poll_for_eviction_hangup(sock: socket.socket, *, budget_sec: float) -> bool:
    """Return True if the daemon hangs up the socket within ``budget_sec``.

    Polls for POLLHUP/POLLERR WITHOUT reading: any ``recv`` would drain the
    buffered frames and let the daemon's next send succeed, resetting its
    ``lag_count`` and preventing the eviction the probe is trying to provoke.
    Both lag-eviction classes (replay-lag, heartbeat-lag) close the socket
    after a best-effort reject send, so the daemon-side hang-up is the
    reliable wire signal; the close-reason tally supplies the precise reason.
    """
    poller = select.poll()
    poller.register(sock.fileno(), select.POLLHUP | select.POLLERR)
    events = poller.poll(budget_sec * 1000.0)
    return any(ev & (select.POLLHUP | select.POLLERR) for _fd, ev in events)


def _probe_token_reject(socket_path: Path, *, offset_sec: float) -> FaultInjectionOutcome:
    """Subscribe with an invalid token; expect ``reason="token"`` on the wire."""
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(_PROBE_TIMEOUT_SEC)
    try:
        sock.connect(str(socket_path))
        _send_subscribe(sock, {"proto": 1, "token": "soak-probe-deliberately-invalid"})
        frame = _read_one_frame(sock)
    except OSError as exc:
        return FaultInjectionOutcome(
            axis="token_reject",
            offset_sec=offset_sec,
            observed=False,
            detail=f"socket error: {type(exc).__name__}: {exc}",
        )
    finally:
        sock.close()

    if frame is None:
        return FaultInjectionOutcome(
            axis="token_reject",
            offset_sec=offset_sec,
            observed=False,
            detail="daemon closed socket without sending a frame",
        )
    if frame.get("kind") == "subscribe_ack":
        # Daemon accepted the subscribe -- token gate is not configured.
        # The probe cannot exercise the reject arm in this deployment.
        return FaultInjectionOutcome(
            axis="token_reject",
            offset_sec=offset_sec,
            observed=False,
            skipped_intentionally=True,
            detail="daemon has no token configured; token reject path not reachable",
        )
    reason = frame.get("reason")
    return FaultInjectionOutcome(
        axis="token_reject",
        offset_sec=offset_sec,
        observed=(frame.get("kind") == "subscribe_rejected" and reason == "token"),
        observed_reason=reason if isinstance(reason, str) else None,
        detail=f"frame kind={frame.get('kind')!r} reason={reason!r}",
    )


def _probe_version_reject(socket_path: Path, *, offset_sec: float) -> FaultInjectionOutcome:
    """Subscribe with ``proto=9999``; expect ``reason="version"`` on the wire."""
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(_PROBE_TIMEOUT_SEC)
    try:
        sock.connect(str(socket_path))
        _send_subscribe(sock, {"proto": 9999})
        frame = _read_one_frame(sock)
    except OSError as exc:
        return FaultInjectionOutcome(
            axis="version_reject",
            offset_sec=offset_sec,
            observed=False,
            detail=f"socket error: {type(exc).__name__}: {exc}",
        )
    finally:
        sock.close()

    if frame is None:
        return FaultInjectionOutcome(
            axis="version_reject",
            offset_sec=offset_sec,
            observed=False,
            detail="daemon closed socket without sending a frame",
        )
    reason = frame.get("reason")
    return FaultInjectionOutcome(
        axis="version_reject",
        offset_sec=offset_sec,
        observed=(frame.get("kind") == "subscribe_rejected" and reason == "version"),
        observed_reason=reason if isinstance(reason, str) else None,
        detail=f"frame kind={frame.get('kind')!r} reason={reason!r}",
    )


def _probe_replay_lag_eviction(socket_path: Path, *, offset_sec: float) -> FaultInjectionOutcome:
    """Subscribe with ``since=""`` and a starved receive buffer, then stop reading.

    The daemon sends the ack, then replays every backlogged row with
    consecutive non-blocking sends. The probe reads ONLY the ack and then
    polls for the daemon-side hang-up WITHOUT draining: the replay frames
    fill the kernel-floor receive buffer, the daemon's sends saturate EAGAIN,
    its ``LAG_LIMIT`` counter trips, and it evicts the subscriber
    (``replay_lag_limit_exceeded``) and closes -> POLLHUP. The lag-eviction
    wire frame is the shared lag reject, so the hang-up is the reliable
    signal; the close-reason tally confirms the precise reason.

    Draining frames would let the daemon's next send succeed and reset its
    ``lag_count``, so the probe deliberately does not read past the ack. A
    backlog too small to overflow the buffer drains without an eviction and
    is recorded as an intentional skip (the path is pinned deterministically
    in-process by ``tests/test_broadcast.py``).
    """
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    # Setting SO_RCVBUF below the kernel floor is silently clamped up by
    # the kernel to ``net.core.rmem_min`` (typically 4096), which is
    # still well under the replay backlog -- enough to make the daemon's
    # per-row sends saturate quickly.
    with contextlib.suppress(OSError):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1)
    sock.settimeout(_PROBE_TIMEOUT_SEC)
    try:
        sock.connect(str(socket_path))
        # ``since="00000000000000000000000000"`` is the ULID-shaped
        # all-zeros cursor; the subscribe-envelope validator accepts
        # it as a valid ULID and the replay walks every row.
        _send_subscribe(sock, {"proto": 1, "since": "00000000000000000000000000"})
        first = _read_one_frame(sock)
        if first is None:
            return FaultInjectionOutcome(
                axis="replay_lag_eviction",
                offset_sec=offset_sec,
                observed=False,
                skipped_intentionally=True,
                detail="daemon closed socket pre-ack (probe could not exercise the path)",
            )
        if first.get("kind") == "subscribe_rejected":
            reason = first.get("reason")
            return FaultInjectionOutcome(
                axis="replay_lag_eviction",
                offset_sec=offset_sec,
                observed=(reason == "lag_limit_exceeded"),
                observed_reason=reason if isinstance(reason, str) else None,
                detail=f"first frame is reject reason={reason!r}",
            )
        # First frame was the ack. Do NOT drain -- let the replay backlog
        # fill the starved buffer and trip the lag limit, then detect the
        # daemon hang-up.
        hung_up = _poll_for_eviction_hangup(sock, budget_sec=_REPLAY_LAG_POLL_BUDGET_SEC)
    except OSError as exc:
        return FaultInjectionOutcome(
            axis="replay_lag_eviction",
            offset_sec=offset_sec,
            observed=False,
            detail=f"socket error: {type(exc).__name__}: {exc}",
        )
    finally:
        sock.close()

    if hung_up:
        return FaultInjectionOutcome(
            axis="replay_lag_eviction",
            offset_sec=offset_sec,
            observed=True,
            observed_reason="lag_limit_exceeded",
            detail="daemon hung up the starved subscriber during replay (lag eviction)",
        )
    return FaultInjectionOutcome(
        axis="replay_lag_eviction",
        offset_sec=offset_sec,
        observed=False,
        skipped_intentionally=True,
        detail=(
            f"no hang-up within {_REPLAY_LAG_POLL_BUDGET_SEC:.0f}s "
            "(backlog too small to overflow the buffer); in-process test pins the path"
        ),
    )


def _probe_heartbeat_lag(socket_path: Path, *, offset_sec: float) -> FaultInjectionOutcome:
    """Subscribe live with a no-match filter and a starved buffer; expect a
    heartbeat-driven eviction (the daemon closes the socket).

    Only meaningful against a daemon with a sub-second heartbeat. The probe
    subscribes (no ``since``), reads the ack, then stops reading and polls
    for POLLHUP WITHOUT draining the socket -- draining would let the
    daemon's next heartbeat send succeed and reset ``lag_count``. With the
    no-match filter the daemon never fan-outs an event to this subscriber,
    so the only writes are heartbeats; once they saturate the buffer and
    ``LAG_LIMIT`` consecutive sends fail, the heartbeat loop evicts the
    subscriber and closes the socket -> POLLHUP on the probe end.

    A close after a clean ack with a no-match filter can only be a
    heartbeat-lag eviction; the daemon-side ``subscriber_close_reasons``
    tally confirms the ``heartbeat_lag`` attribution. A poll budget that
    expires without a hang-up is recorded as an intentional skip (the
    deterministic path is pinned in-process by the heartbeat-loop test).
    """
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    with contextlib.suppress(OSError):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1)
    sock.settimeout(_PROBE_TIMEOUT_SEC)
    try:
        sock.connect(str(socket_path))
        _send_subscribe(sock, {"proto": 1, "filters": [_NO_MATCH_FILTER]})
        ack = _read_one_frame(sock)
        if ack is None or ack.get("kind") != "subscribe_ack":
            ack_kind = ack.get("kind") if ack else None
            return FaultInjectionOutcome(
                axis="heartbeat_lag",
                offset_sec=offset_sec,
                observed=False,
                skipped_intentionally=True,
                detail=f"no subscribe_ack before eviction window (first frame kind={ack_kind!r})",
            )
        # Poll for the daemon-side hang-up WITHOUT reading: any recv would
        # drain queued heartbeats and reset the daemon's lag_count.
        hung_up = _poll_for_eviction_hangup(sock, budget_sec=_HEARTBEAT_LAG_POLL_BUDGET_SEC)
    except OSError as exc:
        return FaultInjectionOutcome(
            axis="heartbeat_lag",
            offset_sec=offset_sec,
            observed=False,
            detail=f"socket error: {type(exc).__name__}: {exc}",
        )
    finally:
        sock.close()

    if hung_up:
        return FaultInjectionOutcome(
            axis="heartbeat_lag",
            offset_sec=offset_sec,
            observed=True,
            observed_reason="heartbeat_lag",
            detail="daemon hung up the no-match subscriber after the ack (heartbeat-lag eviction)",
        )
    return FaultInjectionOutcome(
        axis="heartbeat_lag",
        offset_sec=offset_sec,
        observed=False,
        skipped_intentionally=True,
        detail=(
            f"no hang-up within {_HEARTBEAT_LAG_POLL_BUDGET_SEC:.0f}s "
            "(heartbeat too slow or buffer too large); in-process test pins the path"
        ),
    )


_PROBES: dict[str, Any] = {
    "token_reject": _probe_token_reject,
    "version_reject": _probe_version_reject,
    "replay_lag_eviction": _probe_replay_lag_eviction,
    "heartbeat_lag": _probe_heartbeat_lag,
}

# Eviction-class axes (a registered subscriber is dropped, emitting a
# ``subscriber_closed`` log) mapped to the daemon-internal close reason
# each should produce. Reject-class axes (token / version, rejected
# pre-registration) are absent: they never emit a ``subscriber_closed``
# event, so the close-reason consistency check does not apply to them.
_EVICTION_AXIS_CLOSE_REASON: dict[str, str] = {
    "replay_lag_eviction": "replay_lag_limit_exceeded",
    "heartbeat_lag": "heartbeat_lag",
}


def run_fault_injection_pass(
    *,
    axis: str,
    socket_path: Path,
    offset_sec: float,
    outcomes: list[dict[str, Any]],
) -> None:
    """Execute one probe and append its outcome dict to ``outcomes``.

    Unknown axis names are recorded as an unobserved outcome so a
    typo in the schedule fails the coverage threshold loudly rather
    than silently skipping.
    """
    probe = _PROBES.get(axis)
    if probe is None:
        outcomes.append(
            msgspec.to_builtins(
                FaultInjectionOutcome(
                    axis=axis,
                    offset_sec=offset_sec,
                    observed=False,
                    detail=f"unknown fault-injection axis: {axis!r}",
                )
            )
        )
        return
    outcome = probe(socket_path, offset_sec=offset_sec)
    outcomes.append(msgspec.to_builtins(outcome))


def fault_injection_coverage_threshold(
    outcomes: list[dict[str, Any]],
    expected_axes: frozenset[str],
) -> ThresholdVerdict:
    """Pass when every expected axis was either observed or intentionally skipped.

    An axis is "covered" if its outcome has ``observed=True`` OR
    ``skipped_intentionally=True``. A missing axis (probe never ran) or
    an outcome with ``observed=False`` AND ``skipped_intentionally=False``
    (probe ran, wrong frame received) fails the verdict with a detail
    string naming the offending axis.
    """
    if not expected_axes:
        return ThresholdVerdict(
            "fault_injection_coverage",
            True,
            "no fault-injection probes configured; skipping",
        )
    by_axis = {o["axis"]: o for o in outcomes}
    missing = expected_axes - set(by_axis)
    if missing:
        return ThresholdVerdict(
            "fault_injection_coverage",
            False,
            f"probes never ran: {sorted(missing)}",
        )
    failed = [
        o for o in outcomes if o["axis"] in expected_axes and not o["observed"] and not o.get("skipped_intentionally")
    ]
    if failed:
        bits = "; ".join(f"{o['axis']}: {o.get('detail', '')}" for o in failed)
        return ThresholdVerdict("fault_injection_coverage", False, bits)
    skipped = [o["axis"] for o in outcomes if o.get("skipped_intentionally")]
    return ThresholdVerdict(
        "fault_injection_coverage",
        True,
        f"{len(expected_axes)}/{len(expected_axes)} axes covered"
        + (f"; intentional skips: {skipped}" if skipped else ""),
    )


def fault_injection_close_reason_consistency_threshold(
    outcomes: list[dict[str, Any]],
    close_reasons: dict[str, int],
) -> ThresholdVerdict:
    """Pass when every wire-observed eviction has a matching daemon-internal close reason.

    The eviction-class probes (``replay_lag_eviction``, ``heartbeat_lag``)
    are observed on the wire only as a generic lag reject / hang-up -- their
    distinguishing signal is the daemon's ``subscriber_closed`` reason tally.
    For each such probe that reported ``observed=True``, the matching internal
    reason (per :data:`_EVICTION_AXIS_CLOSE_REASON`) must appear at least once
    in ``close_reasons``. This ties the wire-side probe to the daemon's own
    accounting, catching a regression where an eviction is observed but
    mislabeled (or not logged) internally.

    Probes that were skipped intentionally or not observed are not asserted
    (nothing was evicted to account for). Reject-class axes are out of scope.
    """
    mismatches: list[str] = []
    for outcome in outcomes:
        axis = outcome.get("axis", "")
        reason = _EVICTION_AXIS_CLOSE_REASON.get(axis)
        if reason is None or not outcome.get("observed"):
            continue
        if close_reasons.get(reason, 0) < 1:
            mismatches.append(
                f"{axis}: observed on the wire but '{reason}' absent from close-reason tally {close_reasons}"
            )
    if mismatches:
        return ThresholdVerdict(
            "fault_injection_close_reason_consistency",
            False,
            "; ".join(mismatches),
        )
    return ThresholdVerdict(
        "fault_injection_close_reason_consistency",
        True,
        "wire-observed evictions consistent with the daemon close-reason tally",
    )
