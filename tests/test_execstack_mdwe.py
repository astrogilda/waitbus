"""Executable-stack interpreter detection and the MemoryDenyWriteExecute override.

A Python build whose interpreter marks its stack executable (uv and pyenv
standalone builds ship without a non-executable ``GNU_STACK`` ELF header)
makes glibc allocate writable-and-executable thread stacks. The shipped
systemd units set ``MemoryDenyWriteExecute=`` which blocks that, so the
daemons cannot create threads. ``install-systemd`` detects the condition and
drops in an override; these tests cover both halves with crafted ELF inputs
so they do not depend on which interpreter runs the suite.
"""

from __future__ import annotations

import struct
from pathlib import Path

from waitbus.cli._shared import (
    _EXEC_STACK_DROPIN_NAME,
    _install_executable_stack_overrides,
    _interpreter_has_executable_stack,
)

_PT_LOAD = 0x1
_PT_GNU_STACK = 0x6474E551
_PF_R = 0x4
_PF_W = 0x2
_PF_X = 0x1


def _elf64(program_headers: list[tuple[int, int]]) -> bytes:
    """Build a minimal little-endian 64-bit ELF with the given (p_type, p_flags) headers."""
    phentsize = 56
    phnum = len(program_headers)
    phoff = 64
    header = bytearray(64)
    header[0:4] = b"\x7fELF"
    header[4] = 2  # EI_CLASS = ELFCLASS64
    header[5] = 1  # EI_DATA = little-endian
    header[6] = 1  # EI_VERSION
    struct.pack_into("<H", header, 0x10, 2)  # e_type = ET_EXEC
    struct.pack_into("<H", header, 0x12, 0x3E)  # e_machine = x86-64
    struct.pack_into("<I", header, 0x14, 1)  # e_version
    struct.pack_into("<Q", header, 0x20, phoff)  # e_phoff
    struct.pack_into("<H", header, 0x34, 64)  # e_ehsize
    struct.pack_into("<H", header, 0x36, phentsize)  # e_phentsize
    struct.pack_into("<H", header, 0x38, phnum)  # e_phnum
    table = bytearray()
    for p_type, p_flags in program_headers:
        phdr = bytearray(phentsize)
        struct.pack_into("<I", phdr, 0, p_type)
        struct.pack_into("<I", phdr, 4, p_flags)
        table += phdr
    return bytes(header) + bytes(table)


def test_missing_gnu_stack_reads_as_executable(tmp_path: Path) -> None:
    binary = tmp_path / "no-gnu-stack"
    binary.write_bytes(_elf64([(_PT_LOAD, _PF_R | _PF_X)]))
    assert _interpreter_has_executable_stack(binary) is True


def test_executable_gnu_stack_reads_as_executable(tmp_path: Path) -> None:
    binary = tmp_path / "exec-stack"
    binary.write_bytes(_elf64([(_PT_LOAD, _PF_R | _PF_X), (_PT_GNU_STACK, _PF_R | _PF_W | _PF_X)]))
    assert _interpreter_has_executable_stack(binary) is True


def test_non_executable_gnu_stack_reads_as_safe(tmp_path: Path) -> None:
    binary = tmp_path / "safe-stack"
    binary.write_bytes(_elf64([(_PT_LOAD, _PF_R | _PF_X), (_PT_GNU_STACK, _PF_R | _PF_W)]))
    assert _interpreter_has_executable_stack(binary) is False


def test_non_elf_file_returns_none(tmp_path: Path) -> None:
    not_elf = tmp_path / "script.sh"
    not_elf.write_text("#!/bin/sh\necho hi\n")
    assert _interpreter_has_executable_stack(not_elf) is None


def test_missing_file_returns_none(tmp_path: Path) -> None:
    assert _interpreter_has_executable_stack(tmp_path / "does-not-exist") is None


def test_overrides_only_services_that_declare_the_protection(tmp_path: Path) -> None:
    hardened = "[Service]\nExecStart=/bin/true\nMemoryDenyWriteExecute=true\n"
    (tmp_path / "waitbus-listener.service").write_text(hardened)
    (tmp_path / "waitbus-broadcast.service").write_text(hardened)
    (tmp_path / "waitbus-plain.service").write_text("[Service]\nExecStart=/bin/true\n")
    (tmp_path / "waitbus-broadcast.socket").write_text("[Socket]\nListenStream=/run/x\n")

    units = [
        "waitbus-listener.service",
        "waitbus-broadcast.service",
        "waitbus-plain.service",
        "waitbus-broadcast.socket",
    ]
    overridden = _install_executable_stack_overrides(units, tmp_path, dry_run=False)

    assert overridden == ["waitbus-listener.service", "waitbus-broadcast.service"]
    for unit in overridden:
        dropin = tmp_path / f"{unit}.d" / _EXEC_STACK_DROPIN_NAME
        assert dropin.is_file()
        assert "MemoryDenyWriteExecute=false" in dropin.read_text()
    # The unit without the protection and the non-service unit get no drop-in.
    assert not (tmp_path / "waitbus-plain.service.d").exists()
    assert not (tmp_path / "waitbus-broadcast.socket.d").exists()


def test_overrides_dry_run_writes_nothing(tmp_path: Path) -> None:
    (tmp_path / "waitbus-listener.service").write_text("[Service]\nMemoryDenyWriteExecute=true\n")
    overridden = _install_executable_stack_overrides(["waitbus-listener.service"], tmp_path, dry_run=True)
    assert overridden == ["waitbus-listener.service"]
    assert not (tmp_path / "waitbus-listener.service.d").exists()
