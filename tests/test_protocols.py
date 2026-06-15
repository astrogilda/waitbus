"""Tests for waitbus._protocols: RowLike structural Protocol."""

from __future__ import annotations

from typing import Any, ClassVar

from waitbus._protocols import RowLike


def test_dict_satisfies_row_like() -> None:
    """A plain dict[str, Any] must satisfy RowLike at runtime."""
    row: dict[str, Any] = {"owner": "o", "repo": "r", "event_type": "workflow_run"}
    assert isinstance(row, RowLike)
    assert row["owner"] == "o"
    assert "owner" in row


def test_custom_class_satisfies_row_like() -> None:
    """Any object with __getitem__ and keys() satisfies RowLike."""

    class _FakeRow:
        _data: ClassVar[dict[str, str]] = {"delivery_id": "d1", "source": "github"}

        def __getitem__(self, key: str) -> Any:
            return self._data[key]

        def keys(self) -> list[str]:
            return list(self._data.keys())

    row = _FakeRow()
    assert isinstance(row, RowLike)
    assert row["delivery_id"] == "d1"
