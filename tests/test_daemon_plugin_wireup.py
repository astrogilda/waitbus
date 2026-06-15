"""Daemon-startup plugin discovery: wire-up + idempotency + thread-safety.

Regression harness for the wire-up gap where
:func:`waitbus.sources._registry.discover_plugins` was previously
never invoked from production code (only from tests), leaving the
entire plugin-source path dead at runtime. The
:func:`discover_plugins_once` seam is now called from
:func:`waitbus.broadcast.main` and
:func:`waitbus.listener.main`; this file asserts:

1. Calling :func:`discover_plugins_once` registers a stub plugin in the
   process-singleton registry, and :func:`is_known_source` returns True
   for it.
2. Calling :func:`discover_plugins_once` twice is a no-op on the second
   call -- the second call returns an empty list, and the registered
   source stays registered (re-entry must not double-register and must
   not trigger the duplicate-name ``ValueError`` path that
   ``discover_plugins`` catches as ``Exception`` and silently drops).
3. Concurrent :func:`register_plugin` calls from N threads register
   exactly N unique sources. The reentrant lock around the check-then-act
   on ``_PLUGIN_SOURCES`` prevents a race that would otherwise let two
   threads both pass the ``spec.name in _PLUGIN_SOURCES`` check before
   either of them writes.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from waitbus.sources._config import PluginPolicy
from waitbus.sources._protocol import SOURCE_PLUGIN_API_VERSION, SourceSpec
from waitbus.sources._registry import (
    _clear_for_test_isolation,
    discover_plugins_once,
    is_known_source,
    known_sources,
    register_plugin,
)


@pytest.fixture(autouse=True)
def _isolate_registry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Reset registry + discovery flag + redirect XDG before/after each test."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    _clear_for_test_isolation()
    yield
    _clear_for_test_isolation()


def _make_stub_plugin(name: str, event_types: tuple[str, ...] = ("e",)) -> Any:
    """Return a minimal SourcePlugin-shaped stub."""

    class _Stub:
        def spec(self) -> SourceSpec:
            return SourceSpec(
                name=name,
                event_types=event_types,
                api_version=SOURCE_PLUGIN_API_VERSION,
            )

        def fetch(self, *args: object, **kwargs: object) -> None:  # pragma: no cover
            return None

    return _Stub()


def _make_entry_point(name: str, plugin: Any) -> MagicMock:
    """MagicMock shaped like an importlib.metadata.EntryPoint with no dist."""
    ep = MagicMock()
    ep.name = name
    ep.value = f"fake_pkg:{name}"
    ep.load.return_value = plugin
    ep.dist = None  # skips _verify_publisher and TOFU enforcement
    return ep


# ---------------------------------------------------------------------------
# Wire-up regression
# ---------------------------------------------------------------------------


def test_discover_plugins_once_actually_registers_a_plugin(monkeypatch: pytest.MonkeyPatch) -> None:
    """discover_plugins_once with a monkeypatched entry-point view registers the stub.

    Regression test for the wire-up gap. Before the fix, no production
    code called discover_plugins -- a third-party plugin installed via
    pip never reached the registry, and ``waitbus emit --source <plugin>``
    would always fail with "unknown source". With discover_plugins_once
    now wired into broadcast.main() and listener.main(), the equivalent
    in-process simulation must succeed.
    """
    plugin = _make_stub_plugin("ext1_wireup_demo")
    ep = _make_entry_point("ext1_wireup_demo", plugin)

    from waitbus.sources import _registry

    monkeypatch.setattr(_registry, "entry_points", lambda group: [ep])

    assert not is_known_source("ext1_wireup_demo"), "stub must NOT be known before discovery"

    specs = discover_plugins_once(policy=PluginPolicy(autoload=True))

    assert [s.name for s in specs] == ["ext1_wireup_demo"]
    assert is_known_source("ext1_wireup_demo")
    assert "ext1_wireup_demo" in known_sources()


# ---------------------------------------------------------------------------
# Idempotent re-entry
# ---------------------------------------------------------------------------


def test_discover_plugins_once_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Second call is a no-op; first registration survives.

    The previous discover_plugins did not guard against re-entry: a second
    call would re-attempt to register every plugin, the duplicate-name
    ValueError would be raised inside register_plugin, caught silently by
    the bare ``except Exception`` in discover_plugins, and the plugin would
    be silently missing from the registry. discover_plugins_once flips the
    _DISCOVERED flag BEFORE running discovery so a second call short-circuits.
    """
    plugin = _make_stub_plugin("ext1_idempotent")
    ep = _make_entry_point("ext1_idempotent", plugin)

    from waitbus.sources import _registry

    monkeypatch.setattr(_registry, "entry_points", lambda group: [ep])

    first = discover_plugins_once(policy=PluginPolicy(autoload=True))
    second = discover_plugins_once(policy=PluginPolicy(autoload=True))

    assert [s.name for s in first] == ["ext1_idempotent"]
    assert second == [], "second discover_plugins_once must be a no-op"
    # Source remains registered after second call (the no-op did not clear).
    assert is_known_source("ext1_idempotent")


def test_discover_plugins_once_idempotent_across_threads(monkeypatch: pytest.MonkeyPatch) -> None:
    """Concurrent first-callers see one execution, others short-circuit.

    Spin up 16 threads that all call discover_plugins_once with the same
    monkeypatched entry-point view. Exactly one thread should get the
    non-empty return value; the other 15 see ``[]``. The lock + flag
    prevent any thread from running discover_plugins twice.
    """
    plugin = _make_stub_plugin("ext1_thread_idempotent")
    ep = _make_entry_point("ext1_thread_idempotent", plugin)

    from waitbus.sources import _registry

    monkeypatch.setattr(_registry, "entry_points", lambda group: [ep])

    results: list[list[SourceSpec]] = []
    start = threading.Event()

    def _race() -> list[SourceSpec]:
        start.wait()
        return discover_plugins_once(policy=PluginPolicy(autoload=True))

    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = [pool.submit(_race) for _ in range(16)]
        start.set()
        for fut in as_completed(futures):
            results.append(fut.result())

    non_empty = [r for r in results if r]
    assert len(non_empty) == 1, f"exactly one thread should observe discovery; saw {len(non_empty)}"
    assert non_empty[0][0].name == "ext1_thread_idempotent"
    assert is_known_source("ext1_thread_idempotent")


# ---------------------------------------------------------------------------
# register_plugin thread-safety
# ---------------------------------------------------------------------------


def test_register_plugin_thread_safe_with_n_distinct_sources() -> None:
    """N threads each registering a distinct plugin all succeed.

    Without the lock around check-then-act on ``_PLUGIN_SOURCES``, two
    threads racing to register sources with the same name could both pass
    the membership check before either wrote -- and the second write would
    silently overwrite the first. With distinct names there is no logical
    collision, but the lock also serialises the per-write side-effects
    (publisher-pin append, log line emission) so the test asserts the
    end-state is exactly N entries and no spurious failures.
    """
    n = 32
    names = [f"ext1_concurrent_{i:02d}" for i in range(n)]
    # Each stub gets a unique event_type so the (source, event_type)
    # uniqueness check in register_plugin doesn't reject the 2nd-Nth
    # registrations. This test is exercising thread-safety, not the
    # collision-rejection path (which has its own dedicated test).
    plugins = [_make_stub_plugin(name, event_types=(f"event_{i:02d}",)) for i, name in enumerate(names)]
    eps = [_make_entry_point(name, plugin) for name, plugin in zip(names, plugins, strict=True)]

    start = threading.Event()

    def _register(ep: Any, plugin: Any) -> SourceSpec:
        start.wait()
        return register_plugin(ep, plugin)

    with ThreadPoolExecutor(max_workers=n) as pool:
        futures = [pool.submit(_register, ep, p) for ep, p in zip(eps, plugins, strict=True)]
        start.set()
        registered = {fut.result().name for fut in as_completed(futures)}

    assert registered == set(names)
    for name in names:
        assert is_known_source(name), f"source {name!r} missing from registry after concurrent register"


def test_register_plugin_thread_safe_same_name_one_winner() -> None:
    """N threads racing the same source name yield 1 success + N-1 ValueErrors.

    This is the explicit race-condition guard. Without the lock, two threads
    could both pass the ``spec.name in _PLUGIN_SOURCES`` check before either
    of them assigned, then both write -- the second silently overwrites the
    first with no exception raised. With the lock, exactly one wins; the
    others see the post-write state and raise the duplicate-registration
    ValueError as designed.
    """
    n = 16
    name = "ext1_race_winner"
    plugins = [_make_stub_plugin(name) for _ in range(n)]
    eps = [_make_entry_point(name, plugin) for plugin in plugins]

    start = threading.Event()
    successes = 0
    duplicate_errors = 0

    def _register(ep: Any, plugin: Any) -> str:
        start.wait()
        try:
            spec = register_plugin(ep, plugin)
            return f"ok:{spec.name}"
        except ValueError as exc:
            return f"dup:{exc}"

    with ThreadPoolExecutor(max_workers=n) as pool:
        futures = [pool.submit(_register, ep, p) for ep, p in zip(eps, plugins, strict=True)]
        start.set()
        outcomes = [fut.result() for fut in as_completed(futures)]

    for outcome in outcomes:
        if outcome.startswith("ok:"):
            successes += 1
        elif outcome.startswith("dup:"):
            duplicate_errors += 1

    assert successes == 1, f"exactly one register_plugin should win the race; saw {successes}"
    assert duplicate_errors == n - 1, f"expected {n - 1} duplicate-registration failures; saw {duplicate_errors}"
    assert is_known_source(name)
