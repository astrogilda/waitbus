"""Behaviour tests for ``replay._drain_and_print`` outcome-to-exit mapping.

The helper is the single locus for the operator-facing exit-code matrix
shared by ``_stream_until_idle`` (faithful) and ``_stream_coalesced``
(opt-in coalesce). Each :class:`waitbus._broadcast_sub.WaitOutcome`
field maps to a distinct exit code + stderr message:

* ``timed_out``      → ``Exit(0)`` + ``"replay caught up (no frames in
  Ns)"`` (the expected replay-batch terminus)
* ``peer_closed``    → ``Exit(0)`` + ``"broadcast connection closed"``
  (clean daemon FIN; restart / shutdown)
* ``cancelled``      → ``Exit(130)`` + ``"replay interrupted"`` (SIGINT;
  matches ``waitbus wait``'s POSIX 128 + signum convention)
* ``framing_error``  → ``Exit(2)`` + ``"broadcast framing error"`` (daemon
  violated wire framing mid-stream — distinct from a clean FIN)

The ``cancelled`` and ``framing_error`` mappings replace pre-existing
silent ``Exit(0)`` fall-throughs.

Drives the helper directly via a stubbed ``engine`` closure — no real
broadcast daemon — so the mapping is exercised independently of the
engine implementations (``await_predicate`` / ``coalesce_replay``).
"""

from __future__ import annotations

import socket
from typing import Any

import pytest
import typer

from waitbus._broadcast_sub import SubscriberHandle, WaitOutcome
from waitbus.replay import _drain_and_print


def _make_sub() -> SubscriberHandle:
    """Build a SubscriberHandle backed by a real socketpair (closeable)."""
    server, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    # Server side is unused by the helper; close it to keep fd usage low.
    server.close()
    return SubscriberHandle(sock=client)


def _outcome(**overrides: bool) -> WaitOutcome:
    """Build a WaitOutcome with all flags False except those overridden."""
    fields: dict[str, Any] = dict(
        matched=False,
        timed_out=False,
        cancelled=False,
        peer_closed=False,
        framing_error=False,
    )
    fields.update(overrides)
    return WaitOutcome(**fields)


def test_drain_and_print_timed_out_exits_zero_with_caught_up_message(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """timed_out: Exit(0) + 'replay caught up' on stderr."""
    sub = _make_sub()
    with pytest.raises(typer.Exit) as ei:
        _drain_and_print(
            sub,
            timeout=30.0,
            engine=lambda: _outcome(timed_out=True),
        )
    assert ei.value.exit_code == 0
    err = capsys.readouterr().err
    assert "replay caught up" in err
    assert "30s" in err
    # Socket was closed by the helper.
    assert sub.sock.fileno() == -1


def test_drain_and_print_peer_closed_exits_zero_with_connection_closed(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """peer_closed: Exit(0) + 'broadcast connection closed'."""
    sub = _make_sub()
    with pytest.raises(typer.Exit) as ei:
        _drain_and_print(
            sub,
            timeout=30.0,
            engine=lambda: _outcome(peer_closed=True),
        )
    assert ei.value.exit_code == 0
    assert "broadcast connection closed" in capsys.readouterr().err
    assert sub.sock.fileno() == -1


def test_drain_and_print_cancelled_exits_130_with_interrupted_message(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """cancelled (SIGINT): Exit(130) + 'replay interrupted'.

    Regression guard: a previous version silently fell through to
    Exit(0) so operators got no feedback that their Ctrl-C had landed.
    """
    sub = _make_sub()
    with pytest.raises(typer.Exit) as ei:
        _drain_and_print(
            sub,
            timeout=30.0,
            engine=lambda: _outcome(cancelled=True),
        )
    assert ei.value.exit_code == 130
    assert "replay interrupted" in capsys.readouterr().err
    assert sub.sock.fileno() == -1


def test_drain_and_print_framing_error_exits_two_with_framing_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """framing_error: Exit(2) + 'broadcast framing error'.

    Regression guard: a previous version collapsed framing_error into
    peer_closed → Exit(0), hiding a wire-protocol regression behind a
    clean exit. The daemon violated wire framing mid-stream; the
    operator must distinguish this from a clean FIN.
    """
    sub = _make_sub()
    with pytest.raises(typer.Exit) as ei:
        _drain_and_print(
            sub,
            timeout=30.0,
            engine=lambda: _outcome(framing_error=True),
        )
    assert ei.value.exit_code == 2
    assert "broadcast framing error" in capsys.readouterr().err
    assert sub.sock.fileno() == -1


def test_drain_and_print_closes_socket_even_when_engine_raises() -> None:
    """The finally block closes the socket regardless of engine exit."""
    sub = _make_sub()

    def _raising_engine() -> WaitOutcome:
        raise RuntimeError("engine boom")

    with pytest.raises(RuntimeError, match="engine boom"):
        _drain_and_print(sub, timeout=1.0, engine=_raising_engine)
    assert sub.sock.fileno() == -1
