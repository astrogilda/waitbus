"""Assert that every sub-command in the locked CLI tree is registered.

Introspects the Typer app graph rather than invoking subprocesses so
the test runs fast and without requiring a fully installed wheel.
"""

from __future__ import annotations

from waitbus import cli


def _command_names(typer_app: object) -> set[str]:
    """Return the set of command names registered on a Typer app."""
    names: set[str] = set()
    for cmd in getattr(typer_app, "registered_commands", []):
        # Typer CommandInfo objects carry .name (explicit) or .callback.__name__
        name = getattr(cmd, "name", None) or (cmd.callback.__name__.replace("_", "-") if cmd.callback else None)
        if name:
            names.add(name)
    return names


def _group_names(typer_app: object) -> set[str]:
    """Return the set of sub-app group names registered on a Typer app.

    Typer stores sub-apps as TyperInfo objects on registered_groups.
    The name is on ``group.typer_instance.info.name`` (the Typer object's
    own registered callback name), not directly on the TyperInfo.
    """
    names: set[str] = set()
    for group in getattr(typer_app, "registered_groups", []):
        # Check TyperInfo.typer_instance.info.name (standard Typer path)
        ti = getattr(group, "typer_instance", None)
        if ti is not None:
            info = getattr(ti, "info", None)
            name = getattr(info, "name", None) if info is not None else None
            if name:
                names.add(name)
    return names


# ---------------------------------------------------------------------------
# root-level commands
# ---------------------------------------------------------------------------


def test_root_commands_registered() -> None:
    """init, install-*, doctor, status, stats, verify-plugin."""
    names = _command_names(cli.app)
    expected = {
        "init",
        "on",
        "swarm-demo",
        "install-systemd",
        "install-launchd",
        "install-credentials",
        "doctor",
        "status",
        "stats",
        "top",
        "verify-plugin",
    }
    assert expected <= names, f"Missing root commands: {expected - names}"


def test_root_sub_apps_registered() -> None:
    """listener, broadcast, etag-poll, mcp, read-events, events, pr-monitor, watchdog-check."""
    names = _group_names(cli.app)
    expected = {
        "listener",
        "broadcast",
        "etag-poll",
        "mcp",
        "read-events",
        "events",
        "pr-monitor",
        "watchdog-check",
    }
    assert expected <= names, f"Missing sub-app groups: {expected - names}"


def test_events_query_registered() -> None:
    names = _command_names(cli.events_app)
    assert "query" in names, f"events sub-commands: {names}"


# ---------------------------------------------------------------------------
# daemon sub-app commands
# ---------------------------------------------------------------------------


def test_listener_serve_registered() -> None:
    names = _command_names(cli.listener_app)
    assert "serve" in names, f"listener sub-commands: {names}"


def test_broadcast_serve_registered() -> None:
    names = _command_names(cli.broadcast_app)
    assert "serve" in names, f"broadcast sub-commands: {names}"


def test_etag_poll_run_registered() -> None:
    names = _command_names(cli.etag_poll_app)
    assert "run" in names, f"etag-poll sub-commands: {names}"


def test_mcp_serve_registered() -> None:
    names = _command_names(cli.mcp_app)
    assert "serve" in names, f"mcp sub-commands: {names}"


def test_read_events_watch_and_list_registered() -> None:
    names = _command_names(cli.read_events_app)
    assert {"watch", "list"} <= names, f"read-events sub-commands: {names}"


def test_pr_monitor_tick_registered() -> None:
    names = _command_names(cli.pr_monitor_app)
    assert "tick" in names, f"pr-monitor sub-commands: {names}"


def test_watchdog_check_run_registered() -> None:
    names = _command_names(cli.watchdog_check_app)
    assert "run" in names, f"watchdog-check sub-commands: {names}"
