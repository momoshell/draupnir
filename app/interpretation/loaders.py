"""Shared DB loaders for the interpretation tier.

The single database seam: async functions that load revision data, returning ORM rows / plain
dicts that the pure interpretation functions consume. Keeping loads here (rather than inline in
routes) lets the pure logic be tested with fakes and avoids duplicating the same queries across
features (devices, legend, rooms).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.adapter_run_output import AdapterRunOutput
from app.models.drawing_revision import DrawingRevision
from app.models.revision_materialization import RevisionEntity


async def load_revision_entities_by_type(
    db: AsyncSession,
    revision_id: UUID,
    entity_types: Sequence[str],
    *,
    layer_refs: Sequence[str] | None = None,
    ordered: bool = True,
) -> list[RevisionEntity]:
    """Load a revision's entities of the given type(s), optionally restricted to layer refs."""

    query = select(RevisionEntity).where(
        RevisionEntity.drawing_revision_id == revision_id,
        RevisionEntity.entity_type.in_(list(entity_types)),
    )
    if layer_refs is not None:
        query = query.where(RevisionEntity.layer_ref.in_(list(layer_refs)))
    if ordered:
        query = query.order_by(RevisionEntity.sequence_index, RevisionEntity.id)
    return list((await db.execute(query)).scalars().all())


async def load_legend_text_candidates(
    db: AsyncSession,
    revision_id: UUID,
    *,
    legend_layers: list[str] | None = None,
) -> list[str]:
    """Load raw text strings from legend/key layers for Source B prose input.

    Matches text entities on layers containing LEGEND or KEY (case-insensitive),
    or restricted to ``legend_layers`` when provided. Returns raw text strings only.
    """
    query = select(RevisionEntity).where(
        RevisionEntity.drawing_revision_id == revision_id,
        RevisionEntity.entity_type == "text",
    )
    if legend_layers:
        query = query.where(RevisionEntity.layer_ref.in_(list(legend_layers)))
    else:
        from sqlalchemy import or_

        query = query.where(
            or_(
                RevisionEntity.layer_ref.ilike("%LEGEND%"),
                RevisionEntity.layer_ref.ilike("%KEY%"),
            )
        )

    rows = list((await db.execute(query)).scalars().all())

    texts: list[str] = []
    for row in rows:
        geometry = row.geometry_json or {}
        text = geometry.get("text")
        if isinstance(text, str) and text.strip():
            texts.append(text)
    return texts


async def load_adapter_text_blocks(
    db: AsyncSession,
    revision: DrawingRevision,
) -> list[dict[str, Any]]:
    """Return the revision's extracted text blocks (from its adapter run output), or []."""

    if revision.adapter_run_output_id is None:
        return []
    output = await db.get(AdapterRunOutput, revision.adapter_run_output_id)
    if output is None or not output.canonical_json:
        return []
    metadata = output.canonical_json.get("metadata")
    if not isinstance(metadata, dict):
        return []
    text_blocks = metadata.get("text_blocks")
    return list(text_blocks) if isinstance(text_blocks, list) else []
