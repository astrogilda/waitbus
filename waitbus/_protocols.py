"""Structural typing Protocols decoupling consumers from concrete leaf types.

Currently only RowLike is defined; consumers (read_events, etc.) accept
either a sqlite3.Row or a plain dict[str, Any] without importing
sqlite3 in module-load scope.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class RowLike(Protocol):
    """A row indexed by column name.

    sqlite3.Row satisfies this when row_factory is set to sqlite3.Row.
    Plain dict[str, Any] also satisfies it. Tests can use the latter
    without constructing a real Connection + Row pair.
    """

    def __getitem__(self, key: str) -> Any: ...

    def keys(self) -> Any: ...  # iterable; sqlite3.Row returns a tuple
