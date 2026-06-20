"""Add printed-sheet membership column to revision_entities (#569).

Adds nullable ``on_sheet`` (True = inside a paperspace viewport window, False = off-sheet
title block / key-plan / off-sheet model content, NULL = undetermined) plus an index for
the printed-sheet filter, and backfills existing rows from each row's
``properties_json.sheet_membership.on_sheet`` (NULL where absent — e.g. ingested before
viewport extraction). Projects the #568 tag into a queryable column.

Revision ID: 2026_06_20_0040
Revises: 2026_06_19_0039
Create Date: 2026-06-20 00:40:00.000000
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_06_20_0040"
down_revision: str | None = "2026_06_19_0039"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None

_INDEX = "ix_revision_entities_revision_on_sheet"


def _on_sheet_from_properties(properties: Any) -> bool | None:
    if isinstance(properties, str):
        try:
            properties = json.loads(properties)
        except (ValueError, TypeError):
            return None
    if not isinstance(properties, dict):
        return None
    membership = properties.get("sheet_membership")
    if not isinstance(membership, dict):
        return None
    value = membership.get("on_sheet")
    return value if isinstance(value, bool) else None


def _backfill() -> None:
    bind = op.get_bind()
    rows = (
        bind.execute(sa.text("SELECT id, properties_json FROM revision_entities")).mappings().all()
    )
    update = sa.text("UPDATE revision_entities SET on_sheet = :on_sheet WHERE id = :id")
    for row in rows:
        on_sheet = _on_sheet_from_properties(row["properties_json"])
        if on_sheet is None:
            continue
        bind.execute(update, {"id": row["id"], "on_sheet": on_sheet})


def upgrade() -> None:
    op.add_column("revision_entities", sa.Column("on_sheet", sa.Boolean(), nullable=True))
    op.create_index(_INDEX, "revision_entities", ["drawing_revision_id", "on_sheet"])
    _backfill()


def downgrade() -> None:
    op.drop_index(_INDEX, table_name="revision_entities")
    op.drop_column("revision_entities", "on_sheet")
