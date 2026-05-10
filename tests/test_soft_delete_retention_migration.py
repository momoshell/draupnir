"""Guard tests for the soft-delete retention migration rollback path."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

import pytest


def _load_soft_delete_migration() -> Any:
    """Load the soft-delete migration module directly from disk."""
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "2026_05_10_0008_add_soft_delete_columns.py"
    )
    spec = importlib.util.spec_from_file_location(
        "migration_2026_05_10_0008_add_soft_delete_columns",
        migration_path,
    )
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _FakeScalarResult:
    """Small scalar result stand-in for migration guard tests."""

    def __init__(self, value: bool) -> None:
        self._value = value

    def scalar(self) -> bool:
        """Return the configured scalar value."""
        return self._value


class _FakeBind:
    """Minimal Alembic bind double for downgrade guard tests."""

    def __init__(self, *, project_markers_exist: bool, file_markers_exist: bool) -> None:
        self.project_markers_exist = project_markers_exist
        self.file_markers_exist = file_markers_exist

    def execute(self, statement: Any, *_: Any, **__: Any) -> _FakeScalarResult:
        """Return the configured marker-existence result for each table query."""
        sql = str(statement)
        if "FROM projects" in sql:
            return _FakeScalarResult(self.project_markers_exist)
        if "FROM files" in sql:
            return _FakeScalarResult(self.file_markers_exist)
        raise AssertionError(f"Unexpected SQL: {sql}")


class _FakeOp:
    """Minimal Alembic op double for downgrade guard tests."""

    def __init__(self, *, project_markers_exist: bool, file_markers_exist: bool) -> None:
        self._bind = _FakeBind(
            project_markers_exist=project_markers_exist,
            file_markers_exist=file_markers_exist,
        )
        self.drop_calls: list[tuple[str, str]] = []

    def get_bind(self) -> _FakeBind:
        """Return the fake bind used by the downgrade guard."""
        return self._bind

    def drop_column(self, table_name: str, column_name: str) -> None:
        """Record drop column calls."""
        self.drop_calls.append((table_name, column_name))


def test_soft_delete_migration_downgrade_raises_when_markers_exist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Downgrade should refuse to drop soft-delete columns with retained markers."""
    migration = _load_soft_delete_migration()
    fake_op = _FakeOp(project_markers_exist=False, file_markers_exist=True)
    monkeypatch.setattr(migration, "op", fake_op)

    with pytest.raises(RuntimeError, match="Manual data-preserving rollback is required"):
        migration.downgrade()

    assert fake_op.drop_calls == []


def test_soft_delete_migration_downgrade_drops_columns_without_markers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Downgrade should remain rollback-safe when no deletion markers are present."""
    migration = _load_soft_delete_migration()
    fake_op = _FakeOp(project_markers_exist=False, file_markers_exist=False)
    monkeypatch.setattr(migration, "op", fake_op)

    migration.downgrade()

    assert fake_op.drop_calls == [("files", "deleted_at"), ("projects", "deleted_at")]
