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
from app.models.revision_materialization import MATERIALIZATION_TIER_PRIMARY, RevisionEntity


async def load_revision_entities_by_type(
    db: AsyncSession,
    revision_id: UUID,
    entity_types: Sequence[str],
    *,
    layer_refs: Sequence[str] | None = None,
    exclude_off_sheet: bool = False,
    ordered: bool = True,
    tier: str | None = MATERIALIZATION_TIER_PRIMARY,
) -> list[RevisionEntity]:
    """Load a revision's entities of the given type(s), optionally restricted to layer refs.

    When ``exclude_off_sheet`` is set, entities KNOWN to be off the printed sheet
    (``on_sheet IS FALSE``, #569) are dropped while on-sheet (True) and undetermined
    (NULL — e.g. a drawing with no viewports) entities are kept, so the filter degrades
    gracefully instead of emptying drawings whose sheet membership couldn't be determined.

    ``tier`` filters by ``materialization_tier`` (#831): defaults to ``"primary"`` (today,
    every materialized entity is "primary", so this default is byte-identical to the
    pre-#831 unfiltered behavior). Pass ``tier=None`` to see every tier, including any future
    ``"fill_noise"`` rows (e.g. materialization-inspection/debug routes).
    """

    query = select(RevisionEntity).where(
        RevisionEntity.drawing_revision_id == revision_id,
        RevisionEntity.entity_type.in_(list(entity_types)),
    )
    if layer_refs is not None:
        query = query.where(RevisionEntity.layer_ref.in_(list(layer_refs)))
    if exclude_off_sheet:
        query = query.where(RevisionEntity.on_sheet.isnot(False))
    if tier is not None:
        query = query.where(RevisionEntity.materialization_tier == tier)
    if ordered:
        query = query.order_by(RevisionEntity.sequence_index, RevisionEntity.id)
    return list((await db.execute(query)).scalars().all())


async def load_legend_text_candidates(
    db: AsyncSession,
    revision_id: UUID,
    *,
    legend_layers: list[str] | None = None,
    tier: str | None = MATERIALIZATION_TIER_PRIMARY,
) -> list[str]:
    """Load raw text strings from legend/key layers for Source B prose input.

    Matches text entities on layers containing LEGEND or KEY (case-insensitive),
    or restricted to ``legend_layers`` when provided. Returns raw text strings only.

    ``tier`` filters by ``materialization_tier`` (#831); see
    ``load_revision_entities_by_type`` for semantics and the default's compatibility guarantee.
    """
    query = select(RevisionEntity).where(
        RevisionEntity.drawing_revision_id == revision_id,
        RevisionEntity.entity_type == "text",
    )
    if tier is not None:
        query = query.where(RevisionEntity.materialization_tier == tier)
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
