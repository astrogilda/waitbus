"""Tests for the soak drain-path smoke pre-phase.

Three layers:
- the ``--skip-drain-smoke`` flag parses;
- ``main()`` aborts (rc=1, verdict written) when the pre-phase fails,
  WITHOUT starting the measured soak (monkeypatched -- no daemon spawn);
- the real ``run_drain_path_smoke`` drives a throwaway low-heartbeat daemon
  through all four drain paths and passes (slow, Linux-only, spawns a
  subprocess daemon).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from scripts.soak._drain_smoke import DrainSmokeResult, run_drain_path_smoke
from scripts.soak._main import _build_parser, main
from scripts.soak_monitor import ThresholdVerdict

_LINUX_ONLY = pytest.mark.skipif(sys.platform != "linux", reason="soak harness is Linux-only")


def test_skip_drain_smoke_flag_defaults_off_and_parses() -> None:
    parser = _build_parser()
    assert parser.parse_args([]).skip_drain_smoke is False
    assert parser.parse_args(["--skip-drain-smoke"]).skip_drain_smoke is True


@_LINUX_ONLY
def test_main_aborts_when_drain_smoke_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A failed pre-phase makes main() return 1 and write a failure verdict
    without ever starting the measured soak."""
    failing = DrainSmokeResult(
        passed=False,
        outcomes=[
            {
                "axis": "heartbeat_lag",
                "offset_sec": 0.0,
                "observed": False,
                "observed_reason": None,
                "skipped_intentionally": False,
                "detail": "synthetic miss",
            }
        ],
        close_reasons={},
        verdicts=[ThresholdVerdict("fault_injection_coverage", False, "synthetic failure")],
    )
    spawn_calls: list[int] = []

    def _explode_if_spawned(*_a: object, **_k: object) -> None:
        spawn_calls.append(1)
        raise AssertionError("measured soak must not start after a failed pre-phase")

    monkeypatch.setattr("scripts.soak._main.run_drain_path_smoke", lambda **_k: failing)
    monkeypatch.setattr("scripts.soak._main.spawn_waitbus_daemon", _explode_if_spawned)

    out = tmp_path / "verdict.json"
    rc = main(["--duration", "1s", "--output", str(out)])

    assert rc == 1
    assert spawn_calls == [], "the measured daemon must never be spawned on the abort path"
    doc = json.loads(out.read_text())
    assert doc["overall_passed"] is False
    assert doc["is_partial"] is True
    assert any(v["signal"] == "fault_injection_coverage" and not v["passed"] for v in doc["verdicts"])


@_LINUX_ONLY
def test_final_verdict_close_reasons_survive_tempdir_teardown(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The end-of-run verdict's subscriber_close_reasons must reflect the
    daemon's evictions even though the daemon stderr file lives under a
    TemporaryDirectory deleted before the verdict is computed.

    Regression: the final _compute_verdict_doc previously read
    ctx.daemon_stderr_path after the temp dir was gone, silently yielding
    {}. main() now captures the tally during teardown and threads it in.
    We sentinel-patch the tally source main() uses and assert it reaches the
    verdict; with the bug it would be {} (deleted-path read).
    """
    sentinel = {"heartbeat_lag": 7}
    monkeypatch.setattr("scripts.soak._main._count_close_reasons", lambda _path: dict(sentinel))

    out = tmp_path / "verdict.json"
    rc = main(["--duration", "2s", "--rate", "20", "--skip-drain-smoke", "--output", str(out)])

    assert rc == 0
    doc = json.loads(out.read_text())
    assert doc["subscriber_close_reasons"] == sentinel


@_LINUX_ONLY
@pytest.mark.slow
def test_run_drain_path_smoke_passes_against_throwaway_daemon() -> None:
    """End-to-end: a real throwaway low-heartbeat daemon is driven through all
    four drain paths and the pre-phase passes.

    Slow (~10-15 s): spawns ``waitbus broadcast serve`` as a subprocess, seeds a
    backlog, and waits for a heartbeat-driven eviction.
    """
    result = run_drain_path_smoke()

    assert result.passed, f"drain smoke failed: {[(v.signal, v.detail) for v in result.verdicts if not v.passed]}"
    axes = {o["axis"] for o in result.outcomes}
    assert axes == {"version_reject", "token_reject", "replay_lag_eviction", "heartbeat_lag"}
    # version_reject is always reachable regardless of deployment shape.
    version = next(o for o in result.outcomes if o["axis"] == "version_reject")
    assert version["observed"], f"version_reject must be observed: {version}"

    # Slow-consumer coverage MUST NOT silently decay. Both lag-eviction axes
    # rely on the kernel clamping a 1-byte SO_RCVBUF up to its small floor so a
    # starved subscriber overflows and the daemon evicts it. On a host where
    # that floor is honoured (this gate's host -- verified capable), the probes
    # MUST report observed=True; tolerating skipped_intentionally here would let
    # a regression that SILENCED the eviction pass green via the skip path.
    #
    # Capability guard: a kernel that ignores the SO_RCVBUF floor (e.g. a huge
    # net.core.rmem_min so the buffer never overflows in the poll budget) is the
    # one legitimate reason a lag probe cannot fire. We detect that shape (an
    # honest intentional skip whose detail names the buffer-too-large case) and
    # xfail rather than weaken the default. The DEFAULT on a capable host is the
    # strict observed=True assertion below -- a silenced eviction turns it RED.
    replay = next(o for o in result.outcomes if o["axis"] == "replay_lag_eviction")
    heartbeat = next(o for o in result.outcomes if o["axis"] == "heartbeat_lag")
    if _lag_probe_incapable(replay) and _lag_probe_incapable(heartbeat):
        pytest.xfail(
            "kernel does not honour the SO_RCVBUF floor; lag-eviction probes "
            f"cannot overflow the starved buffer (replay={replay['detail']!r}, "
            f"heartbeat={heartbeat['detail']!r})"
        )

    # replay_lag_eviction: a non-draining replay subscriber on a starved buffer
    # must be evicted by the real daemon, and the daemon's own close-reason
    # tally must record the matching internal reason.
    assert replay["observed"], f"replay_lag_eviction must be observed on a capable host: {replay}"
    assert result.close_reasons.get("replay_lag_limit_exceeded", 0) >= 1, (
        f"replay_lag_eviction observed but daemon close-reason tally missing "
        f"'replay_lag_limit_exceeded': {result.close_reasons}"
    )

    # heartbeat_lag: a non-draining live subscriber on a starved buffer must be
    # evicted by the daemon's heartbeat loop, with the tally confirming it.
    assert heartbeat["observed"], f"heartbeat_lag must be observed on a capable host: {heartbeat}"
    assert result.close_reasons.get("heartbeat_lag", 0) >= 1, (
        f"heartbeat_lag observed but daemon close-reason tally missing 'heartbeat_lag': {result.close_reasons}"
    )


def _lag_probe_incapable(outcome: dict[str, object]) -> bool:
    """True when a lag probe was skipped because the kernel never
    overflowed the starved receive buffer (the only legitimate incapacity).

    This is the buffer-too-large / floor-not-honoured shape from
    ``_probe_replay_lag_eviction`` / ``_probe_heartbeat_lag``. A skip for ANY
    other reason (e.g. a regression that silenced the eviction so no hang-up
    was seen but the detail does not name the buffer) is NOT treated as a
    capability gap -- it falls through to the strict observed=True assertion
    and turns the gate RED.
    """
    return bool(outcome.get("skipped_intentionally")) and "buffer" in str(outcome.get("detail", ""))
