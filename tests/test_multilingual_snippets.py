"""End-to-end tests for the multilingual subscriber snippets.

Compiles (or `shellcheck`s) each non-Python snippet, runs it against the
``running_daemon`` fixture, emits one event per source, and asserts the
binary's stdout carries all four delivery_ids in the canonical
``delivery_id\\tsource=...\\ttype=...`` format. The Python snippet has
its own test in ``tests/test_subscriber_snippet.py``; this file extends
that contract to Rust, Go, TypeScript, and the bash wait wrapper
(shellcheck-only -- the wrapper integrates with a separate CLI command
exercised by the ``waitbus wait`` suite).

Each test skips with reason when the relevant toolchain is absent.
``ubuntu-latest`` GitHub-Actions runners ship rustc, go, node, and
shellcheck pre-installed; self-hosted runners without those binaries
hit the skip-with-reason path rather than failing.
"""

from __future__ import annotations

import asyncio
import fcntl
import os
import shutil
import subprocess
import time
from pathlib import Path

import msgspec
import pytest

from tests._wire_helpers import read_nonblocking
from waitbus import _emit as emit_mod
from waitbus._types import EventInsert

_SNIPPETS_DIR = Path(__file__).resolve().parents[1] / "docs" / "snippets"
_RUST_SNIPPET = _SNIPPETS_DIR / "minimal_subscriber.rs"
_GO_SNIPPET = _SNIPPETS_DIR / "minimal_subscriber.go"
_TS_SNIPPET = _SNIPPETS_DIR / "minimal_subscriber.ts"
_BASH_WRAPPER = _SNIPPETS_DIR / "wait_for_any_source.sh"


_SOURCE_EVENT_TYPE: dict[str, str] = {
    "github": "workflow_run",
    "pytest": "pytest_session",
    "docker": "docker_container",
    "fs": "fs_change",
}


def _build_event(source: str, delivery_id: str) -> EventInsert:
    return EventInsert(
        delivery_id=delivery_id,
        source=source,
        event_type=_SOURCE_EVENT_TYPE.get(source, "generic_event"),
        owner="bench",
        repo="snippet-test",
        received_at=time.time_ns(),
        payload_json=msgspec.json.encode({"i": 0}).decode(),
        ingest_method="snippet-test",
        status="completed",
        conclusion="success",
    )


async def _drive_warmup_until_received(
    *,
    proc: subprocess.Popen[str],
    db_path: Path,
    timeout_sec: float = 10.0,
) -> None:
    """Emit warmup events at 20 Hz until the subprocess prints one on stdout.

    Larger ``timeout_sec`` than the Python test because the
    compile-then-run subprocesses have more startup overhead (Rust
    binary load, Go runtime init, Node strip-types pass).
    """
    if proc.stdout is None:
        raise RuntimeError("subprocess has no stdout pipe")
    flags = fcntl.fcntl(proc.stdout.fileno(), fcntl.F_GETFL)
    fcntl.fcntl(proc.stdout.fileno(), fcntl.F_SETFL, flags | os.O_NONBLOCK)

    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        warmup_id = f"snippet-test:warmup:{time.time_ns()}"
        emit_mod.emit_batch([_build_event("pytest", warmup_id)], db_path=db_path)
        await asyncio.sleep(0.05)
        data = read_nonblocking(proc.stdout.fileno())
        if "snippet-test:warmup:" in data:
            fcntl.fcntl(proc.stdout.fileno(), fcntl.F_SETFL, flags)
            return

    if proc.stderr is not None:
        flags_err = fcntl.fcntl(proc.stderr.fileno(), fcntl.F_GETFL)
        fcntl.fcntl(proc.stderr.fileno(), fcntl.F_SETFL, flags_err | os.O_NONBLOCK)
        err_data = read_nonblocking(proc.stderr.fileno())
    else:
        err_data = ""
    proc.poll()
    raise TimeoutError(
        f"subprocess did not receive any warmup event within {timeout_sec}s; "
        f"returncode={proc.returncode}; stderr={err_data!r}"
    )


async def _run_subscriber_subprocess(
    *,
    argv: list[str],
    socket_path: Path,
    db_path: Path,
) -> None:
    """Spawn ``argv`` as a subscriber subprocess; assert it streams 4 events.

    Mirrors the Python test's flow: warmup until registered, emit one
    event per source, then drain stdout asserting all four
    delivery_ids land in the expected
    ``delivery_id\\tsource=...\\ttype=...`` format.
    """
    snippet_env = os.environ.copy()
    snippet_env["WAITBUS_BROADCAST_SOCKET"] = str(socket_path)
    snippet_env["PYTHONUNBUFFERED"] = "1"
    proc = subprocess.Popen(
        argv,
        env=snippet_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
        text=True,
    )
    try:
        await _drive_warmup_until_received(proc=proc, db_path=db_path)

        delivery_ids: dict[str, str] = {}
        for source in ("github", "pytest", "docker", "fs"):
            did = f"snippet-test:{source}:{time.time_ns()}"
            delivery_ids[source] = did
            emit_mod.emit_batch([_build_event(source, did)], db_path=db_path)

        if proc.stdout is None:
            raise RuntimeError("subprocess has no stdout pipe")
        flags = fcntl.fcntl(proc.stdout.fileno(), fcntl.F_GETFL)
        fcntl.fcntl(proc.stdout.fileno(), fcntl.F_SETFL, flags | os.O_NONBLOCK)

        seen: set[str] = set()
        buffer = ""
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and len(seen) < 4:
            chunk = read_nonblocking(proc.stdout.fileno())
            if chunk:
                buffer += chunk
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    for source, did in delivery_ids.items():
                        if did in line:
                            assert f"\tsource={source}\t" in line, line
                            seen.add(did)
            await asyncio.sleep(0.02)

        assert seen == set(delivery_ids.values()), f"missing: {set(delivery_ids.values()) - seen}"
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2.0)
        for stream in (proc.stdout, proc.stderr):
            if stream is not None:
                stream.close()


async def test_rust_subscriber_streams_all_four_sources(
    running_daemon: tuple[object, dict[str, Path]], tmp_path: Path
) -> None:
    """Compile minimal_subscriber.rs with rustc and run it against the daemon."""
    rustc = shutil.which("rustc")
    if rustc is None:
        pytest.skip("rustc not available")
    binary = tmp_path / "minimal_subscriber_rust"
    compile_proc = subprocess.run(
        [rustc, "--edition", "2021", str(_RUST_SNIPPET), "-o", str(binary)],
        capture_output=True,
        text=True,
        timeout=60.0,
        check=False,
    )
    assert compile_proc.returncode == 0, f"rustc failed: {compile_proc.stderr}"

    _, paths = running_daemon
    await _run_subscriber_subprocess(
        argv=[str(binary)],
        socket_path=paths["broadcast"],
        db_path=paths["db"],
    )


async def test_go_subscriber_streams_all_four_sources(
    running_daemon: tuple[object, dict[str, Path]], tmp_path: Path
) -> None:
    """Run minimal_subscriber.go via 'go run' against the daemon.

    ``go run`` JIT-compiles and executes in one step; no Cargo-style
    persistent target dir needed. Uses GOCACHE pointing at a tmp dir
    so a CI runner without ``~/.cache/go-build`` write access still
    works.
    """
    go_bin = shutil.which("go")
    if go_bin is None:
        pytest.skip("go not available")

    _, paths = running_daemon
    # GOCACHE defaults to ~/.cache/go-build which speeds up repeat runs
    # (a cold build of a stdlib-only program takes ~25s; a warm build
    # ~1s). The test runner shares this cache across tests; isolation
    # is provided by staging the source file into tmp_path below.
    snippet_env = os.environ.copy()
    snippet_env["WAITBUS_BROADCAST_SOCKET"] = str(paths["broadcast"])

    # Stage the .go file into a tmp dir so ``go run`` does not require
    # a go.mod in the snippets directory.
    staged = tmp_path / "minimal_subscriber.go"
    staged.write_text(_GO_SNIPPET.read_text(encoding="utf-8"), encoding="utf-8")

    proc = subprocess.Popen(
        [go_bin, "run", str(staged)],
        env=snippet_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
        text=True,
    )
    try:
        # Warmup timeout accommodates the cold ``go run`` compile path
        # (~25 s on a fresh GOCACHE; warm runs land in ~1 s).
        await _drive_warmup_until_received(proc=proc, db_path=paths["db"], timeout_sec=45.0)

        delivery_ids: dict[str, str] = {}
        for source in ("github", "pytest", "docker", "fs"):
            did = f"snippet-test:{source}:{time.time_ns()}"
            delivery_ids[source] = did
            emit_mod.emit_batch([_build_event(source, did)], db_path=paths["db"])

        if proc.stdout is None:
            raise RuntimeError("subprocess has no stdout pipe")
        flags = fcntl.fcntl(proc.stdout.fileno(), fcntl.F_GETFL)
        fcntl.fcntl(proc.stdout.fileno(), fcntl.F_SETFL, flags | os.O_NONBLOCK)
        seen: set[str] = set()
        buffer = ""
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and len(seen) < 4:
            chunk = read_nonblocking(proc.stdout.fileno())
            if chunk:
                buffer += chunk
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    for source, did in delivery_ids.items():
                        if did in line:
                            assert f"\tsource={source}\t" in line, line
                            seen.add(did)
            await asyncio.sleep(0.02)
        assert seen == set(delivery_ids.values()), f"missing: {set(delivery_ids.values()) - seen}"
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2.0)
        for stream in (proc.stdout, proc.stderr):
            if stream is not None:
                stream.close()


async def test_typescript_subscriber_streams_all_four_sources(
    running_daemon: tuple[object, dict[str, Path]],
) -> None:
    """Run minimal_subscriber.ts under node --experimental-strip-types.

    Requires Node 22.6+ for the built-in TS stripping. The skip path
    fires both when node is absent and when node version is < 22.6.
    """
    node_bin = shutil.which("node")
    if node_bin is None:
        pytest.skip("node not available")
    version_proc = subprocess.run(
        [node_bin, "--version"],
        capture_output=True,
        text=True,
        timeout=5.0,
        check=False,
    )
    version_str = version_proc.stdout.strip().lstrip("v")
    try:
        major, minor = (int(p) for p in version_str.split(".")[:2])
    except ValueError:
        pytest.skip(f"could not parse node --version output: {version_proc.stdout!r}")
    if (major, minor) < (22, 6):
        pytest.skip(f"node {version_str} < 22.6 (no --experimental-strip-types)")

    _, paths = running_daemon
    await _run_subscriber_subprocess(
        argv=[node_bin, "--experimental-strip-types", "--no-warnings", str(_TS_SNIPPET)],
        socket_path=paths["broadcast"],
        db_path=paths["db"],
    )


@pytest.mark.parametrize(
    "snippet_path,reject_pattern,exit_pattern",
    [
        (
            _SNIPPETS_DIR / "minimal_subscriber.py",
            "subscribe_rejected",
            "return 2",
        ),
        (
            _SNIPPETS_DIR / "minimal_subscriber.go",
            "subscribe_rejected",
            "return 2",
        ),
        (
            _SNIPPETS_DIR / "minimal_subscriber.rs",
            "subscribe_rejected",
            "std::process::exit(2)",
        ),
        (
            _SNIPPETS_DIR / "minimal_subscriber.ts",
            "subscribe_rejected",
            "process.exit(2)",
        ),
    ],
)
def test_snippet_handles_subscribe_rejected(snippet_path: Path, reject_pattern: str, exit_pattern: str) -> None:
    """Each subscriber snippet must handle subscribe_rejected with a non-zero exit."""
    text = snippet_path.read_text(encoding="utf-8")
    assert reject_pattern in text, f"{snippet_path.name} missing '{reject_pattern}' handling arm"
    assert exit_pattern in text, f"{snippet_path.name} missing exit-2 call ('{exit_pattern}')"


def test_bash_wait_wrapper_passes_shellcheck() -> None:
    """The bash wrapper is shellcheck-clean.

    Full integration of ``wait_for_any_source.sh`` requires the
    ``waitbus wait --match`` CLI and a daemon producing the matching
    frames; that surface is covered by the wait command's own test
    suite. Here we only verify the wrapper itself is syntactically and
    statically clean.
    """
    shellcheck = shutil.which("shellcheck")
    if shellcheck is None:
        pytest.skip("shellcheck not available")
    proc = subprocess.run(
        [shellcheck, str(_BASH_WRAPPER)],
        capture_output=True,
        text=True,
        timeout=15.0,
        check=False,
    )
    assert proc.returncode == 0, f"shellcheck failed: {proc.stdout}\n{proc.stderr}"
