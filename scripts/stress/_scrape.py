"""Read-side scrape helpers for the stress harness.

Splits cleanly into two families:

1. **Daemon-side metrics** -- the broadcast daemon emits one
   ``metrics_snapshot`` structured-log line per
   ``WAITBUS_METRICS_SNAPSHOT_PERIOD_SEC`` (default 5.0 s). The
   harness tails the daemon's stderr / stdout log, filters by
   ``event=="metrics_snapshot"``, and surfaces the ``families`` dict
   produced by ``waitbus._metrics.snapshot()``. No HTTP scrape,
   no new socket -- the channel is the one the daemon already writes
   to.

2. **OS-side metrics** -- per-tick kernel state pulled from procfs,
   sysfs (cgroups v2), and ``ss -xm``. The set is the minimal
   high-signal observation surface for the CPU-throttle,
   slow-consumer, and zero-polling probes:

   - cgroup cpu.stat (``nr_periods``, ``nr_throttled``, ``throttled_usec``)
     so the harness can verify a ``cpu.max`` throttle actually fired.
   - ``ss -xm`` per AF_UNIX peer queues (Recv-Q / Send-Q / skmem) so
     the silent slow-consumer probe has a kernel-buffer-pressure
     signal independent of the daemon-side latency histogram.
   - ``/proc/<pid>/net/unix`` for peer-credential / state liveness.
   - ``/proc/<pid>/status`` voluntary / nonvoluntary context-switch
     counters so the zero-polling structural assertion has a
     corroborating signal beyond ``perf stat`` syscall counts.

Pure stdlib + subprocess; no third-party tooling.
"""

from __future__ import annotations

import json
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import IO

# --- daemon-side metrics_snapshot tail --------------------------------------


@dataclass(slots=True)
class MetricsSnapshot:
    """One parsed ``metrics_snapshot`` log line.

    ``families`` mirrors ``waitbus._metrics.snapshot()`` -- a
    ``{family_name: [MetricSample, ...]}`` dict. ``ts`` is the
    daemon-side wall-clock seconds captured by ``_log.structured`` at
    emission time; the harness uses it to bind the snapshot to the
    correct sweep N when it lines up samples.
    """

    ts: float
    families: dict[str, list[dict[str, object]]]


def tail_metrics_snapshots(stream: IO[str]) -> Iterator[MetricsSnapshot]:
    """Yield one ``MetricsSnapshot`` per ``event=="metrics_snapshot"`` line.

    The caller passes a long-lived readable stream (typically the
    daemon's redirected stderr or a follow-mode file opened in the
    controller). Lines that fail to parse as JSON or that carry a
    different ``event`` are silently skipped; the harness depends only
    on the snapshot lines and never on the other structured-log
    families. Returns when the stream EOFs.
    """
    for line in stream:
        line = line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if record.get("event") != "metrics_snapshot":
            continue
        families = record.get("families")
        if not isinstance(families, dict):
            continue
        ts = record.get("ts")
        ts_float = float(ts) if isinstance(ts, (int, float)) else 0.0
        yield MetricsSnapshot(ts=ts_float, families=families)


# --- cgroup v2 cpu.stat ------------------------------------------------------


@dataclass(slots=True, frozen=True)
class CgroupCpuStat:
    """Parsed ``cpu.stat`` view.

    Mirrors the canonical kernel-documented fields. Reading two
    snapshots a window apart and computing the delta lets a caller
    confirm whether a ``cpu.max`` throttle actually engaged over the
    window (Dan Luu's "throttling problem" pathology -- the throttle is
    correctly configured but never engages).
    """

    nr_periods: int
    nr_throttled: int
    throttled_usec: int
    usage_usec: int


def read_cgroup_cpu_stat(cgroup_path: Path) -> CgroupCpuStat:
    """Read ``<cgroup_path>/cpu.stat`` and return a parsed view.

    Raises ``FileNotFoundError`` if the cgroup leaf does not exist
    (the harness uses this as the signal that the cgroup it intended
    to throttle was never created -- a setup-side bug, not a runtime
    one). All-zero values are normal for an unloaded cgroup.
    """
    raw = (cgroup_path / "cpu.stat").read_text(encoding="ascii")
    values: dict[str, int] = {}
    for line in raw.splitlines():
        if not line:
            continue
        key, _, value_str = line.partition(" ")
        values[key] = int(value_str)
    return CgroupCpuStat(
        nr_periods=values.get("nr_periods", 0),
        nr_throttled=values.get("nr_throttled", 0),
        throttled_usec=values.get("throttled_usec", 0),
        usage_usec=values.get("usage_usec", 0),
    )


# --- ss -xm parser ----------------------------------------------------------


@dataclass(slots=True, frozen=True)
class UnixSocketSample:
    """One row from ``ss -xm`` parsed into structured fields.

    ``recv_q`` and ``send_q`` are the kernel-reported queue depths in
    bytes for that AF_UNIX peer. ``state`` is the textual state column
    (``LISTEN`` / ``ESTAB`` / ``CONNECTED``). ``local_addr`` and
    ``peer_addr`` are the local + peer endpoint strings -- file-system
    paths for path-bound sockets, abstract-namespace markers for
    autobound peers.

    The ``skmem_*`` fields are populated when ``ss -m`` was passed (the
    default in this module); they let the slow-consumer probe see the
    kernel-buffer-pressure values that the daemon-side latency
    histogram alone cannot surface.
    """

    state: str
    recv_q: int
    send_q: int
    local_addr: str
    peer_addr: str
    skmem_rmem_alloc: int
    skmem_wmem_alloc: int
    skmem_rcvbuf: int
    skmem_sndbuf: int


def _parse_skmem_token(skmem: str) -> dict[str, int]:
    """Parse the ``ss -m`` ``skmem:(r0,rb212992,t0,tb4194304,...)`` token."""
    inner = skmem.removeprefix("skmem:(").removesuffix(")")
    out: dict[str, int] = {}
    for part in inner.split(","):
        if not part:
            continue
        # Each part is ``<key><value>`` -- key is letters, value is digits.
        idx = 0
        while idx < len(part) and part[idx].isalpha():
            idx += 1
        if idx == 0 or idx == len(part):
            continue
        out[part[:idx]] = int(part[idx:])
    return out


def list_unix_socket_samples(*, socket_path: Path | None = None) -> list[UnixSocketSample]:
    """Run ``ss -xm`` and return one ``UnixSocketSample`` per peer.

    When ``socket_path`` is given the result is filtered to peers whose
    local or peer address matches that path; otherwise every AF_UNIX
    peer on the system is returned. The harness uses the filtered form
    to track only the daemon's broadcast peers.

    ``ss`` exit codes other than 0 raise ``CalledProcessError``; a
    missing ``ss`` binary raises ``FileNotFoundError``. Both signal a
    harness-setup bug rather than a runtime one.
    """
    proc = subprocess.run(
        ["ss", "--no-header", "-x", "-m"],
        capture_output=True,
        check=True,
        text=True,
    )
    samples: list[UnixSocketSample] = []
    state = recv_q = send_q = local_addr = peer_addr = None
    target = str(socket_path) if socket_path is not None else None
    for raw_line in proc.stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("skmem:"):
            if state is None:
                # Header continuation without a preceding row -- skip.
                continue
            skmem = _parse_skmem_token(line)
            if target is None or target in (local_addr, peer_addr):
                samples.append(
                    UnixSocketSample(
                        state=state or "",
                        recv_q=int(recv_q or 0),
                        send_q=int(send_q or 0),
                        local_addr=local_addr or "",
                        peer_addr=peer_addr or "",
                        skmem_rmem_alloc=skmem.get("r", 0),
                        skmem_wmem_alloc=skmem.get("t", 0),
                        skmem_rcvbuf=skmem.get("rb", 0),
                        skmem_sndbuf=skmem.get("tb", 0),
                    )
                )
            state = recv_q = send_q = local_addr = peer_addr = None
            continue
        # Row line: "State Recv-Q Send-Q Local-Address Peer-Address ..."
        fields = line.split()
        if len(fields) < 5:
            continue
        state, recv_q, send_q, local_addr, peer_addr = fields[0], fields[1], fields[2], fields[3], fields[4]
    return samples


# --- /proc/<pid>/net/unix ----------------------------------------------------


@dataclass(slots=True, frozen=True)
class ProcNetUnixRow:
    """One ``/proc/<pid>/net/unix`` row parsed into structured fields.

    The kernel-documented column order is ``Num RefCount Protocol Flags
    Type St Inode Path``. ``state == "03"`` indicates a connected peer;
    ``state == "01"`` an unconnected listener. The harness uses this
    for liveness checks (a peer the controller spawned and expected to
    still be connected must appear here) without the cost of an
    ``ss`` exec.
    """

    refcount: int
    state: str
    inode: int
    path: str


def read_proc_net_unix(pid: int) -> list[ProcNetUnixRow]:
    """Parse ``/proc/<pid>/net/unix`` and return one row per peer."""
    raw = Path(f"/proc/{pid}/net/unix").read_text(encoding="ascii")
    rows: list[ProcNetUnixRow] = []
    for index, line in enumerate(raw.splitlines()):
        if index == 0 and line.startswith("Num"):
            continue
        fields = line.split()
        if len(fields) < 7:
            continue
        rows.append(
            ProcNetUnixRow(
                refcount=int(fields[1], 16),
                state=fields[5],
                inode=int(fields[6]),
                path=fields[7] if len(fields) >= 8 else "",
            )
        )
    return rows


# --- /proc/<pid>/status ctxt switches ---------------------------------------


@dataclass(slots=True, frozen=True)
class CtxtSwitchSnapshot:
    """Voluntary + nonvoluntary context-switch counters at one instant.

    The zero-polling structural assertion reads two snapshots a
    window apart. ``voluntary_ctxt_switches`` increments whenever the
    task yields the CPU into a blocking syscall (epoll_wait, recv on
    an empty socket); ``nonvoluntary_ctxt_switches`` increments when
    the scheduler preempts a running task. A correctly-blocked
    subscriber over an idle window shows zero of both -- any
    nonvoluntary increment is suggestive evidence of a busy-loop the
    `perf stat` syscall-count assertion will flag in turn.
    """

    voluntary: int
    nonvoluntary: int


def read_ctxt_switches(pid: int) -> CtxtSwitchSnapshot:
    """Parse ``/proc/<pid>/status`` for the two context-switch counters."""
    raw = Path(f"/proc/{pid}/status").read_text(encoding="ascii")
    voluntary = nonvoluntary = 0
    for line in raw.splitlines():
        if line.startswith("voluntary_ctxt_switches:"):
            voluntary = int(line.split()[-1])
        elif line.startswith("nonvoluntary_ctxt_switches:"):
            nonvoluntary = int(line.split()[-1])
    return CtxtSwitchSnapshot(voluntary=voluntary, nonvoluntary=nonvoluntary)
