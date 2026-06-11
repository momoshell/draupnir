"""Unit tests for revision-entity insert ordering (self-FK topological sort)."""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from app.jobs.revision_materialization import _order_revision_entity_insert_rows


def _row(row_id: uuid.UUID, *, sequence_index: int, parent: uuid.UUID | None) -> dict[str, Any]:
    return {"id": row_id, "sequence_index": sequence_index, "parent_entity_row_id": parent}


def test_orders_parent_before_children() -> None:
    parent = uuid.uuid4()
    child_a = uuid.uuid4()
    child_b = uuid.uuid4()
    # Children deliberately precede the parent in input order.
    rows = [
        _row(child_a, sequence_index=1, parent=parent),
        _row(child_b, sequence_index=2, parent=parent),
        _row(parent, sequence_index=0, parent=None),
    ]

    ordered = _order_revision_entity_insert_rows(rows)
    ids = [row["id"] for row in ordered]

    assert ids[0] == parent
    assert set(ids[1:]) == {child_a, child_b}
    assert len(ids) == 3


def test_dangling_parent_ref_is_treated_as_root() -> None:
    # parent_entity_row_id pointing outside the batch must not block insertion.
    orphan = uuid.uuid4()
    rows = [_row(orphan, sequence_index=0, parent=uuid.uuid4())]

    ordered = _order_revision_entity_insert_rows(rows)

    assert [row["id"] for row in ordered] == [orphan]


def test_cyclic_parent_dependency_raises() -> None:
    a = uuid.uuid4()
    b = uuid.uuid4()
    rows = [
        _row(a, sequence_index=0, parent=b),
        _row(b, sequence_index=1, parent=a),
    ]

    with pytest.raises(ValueError, match="cyclic parent_entity_row_id dependency"):
        _order_revision_entity_insert_rows(rows)
