"""Add persisted entity bounding-box columns + spatial index (Pre-MCP A1).

Adds nullable ``bbox_min_x/min_y/max_x/max_y`` to ``revision_entities`` (drawing
coordinate units), a composite index for indexed spatial prefilters, and backfills
existing rows by computing the AABB from each row's geometry_json. NULL bbox means
the geometry has no recoverable 2-D extent.

Revision ID: 2026_06_19_0039
Revises: 2026_06_18_0038
Create Date: 2026-06-19 00:39:00.000000
"""

from __future__ import annotations

import json
from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op
from app.ingestion.entity_geometry import compute_entity_bbox

# revision identifiers, used by Alembic.
revision: str = "2026_06_19_0039"
down_revision: str | None = "2026_06_18_0038"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None

_INDEX = "ix_revision_entities_revision_bbox"
_COLUMNS = ("bbox_min_x", "bbox_min_y", "bbox_max_x", "bbox_max_y")


def _backfill() -> None:
    bind = op.get_bind()
    rows = bind.execute(sa.text("SELECT id, geometry_json FROM revision_entities")).mappings().all()
    update = sa.text(
        "UPDATE revision_entities "
        "SET bbox_min_x = :min_x, bbox_min_y = :min_y, "
        "bbox_max_x = :max_x, bbox_max_y = :max_y "
        "WHERE id = :id"
    )
    for row in rows:
        geometry = row["geometry_json"]
        if isinstance(geometry, str):
            try:
                geometry = json.loads(geometry)
            except (ValueError, TypeError):
                geometry = None
        bbox = compute_entity_bbox(geometry)
        if bbox is None:
            continue
        bind.execute(
            update,
            {
                "id": row["id"],
                "min_x": bbox[0],
                "min_y": bbox[1],
                "max_x": bbox[2],
                "max_y": bbox[3],
            },
        )


def upgrade() -> None:
    for column in _COLUMNS:
        op.add_column("revision_entities", sa.Column(column, sa.Float(), nullable=True))
    op.create_index(
        _INDEX,
        "revision_entities",
        ["drawing_revision_id", *_COLUMNS],
    )
    _backfill()


def downgrade() -> None:
    op.drop_index(_INDEX, table_name="revision_entities")
    for column in _COLUMNS:
        op.drop_column("revision_entities", column)
