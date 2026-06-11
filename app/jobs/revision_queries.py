"""Shared read-only drawing-revision queries for worker job processing.

Extracted from ``app.jobs.worker`` (issue #387) so both the worker execute stage
and the per-type finalizers can load revision rows without importing each other.
Every function here is a deterministic, side-effect-free SELECT.
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.drawing_revision import DrawingRevision
from app.models.revision_materialization import (
    RevisionBlock,
    RevisionEntity,
    RevisionEntityManifest,
    RevisionLayer,
    RevisionLayout,
)
from app.models.validation_report import ValidationReport


async def _get_latest_drawing_revision(
    session: AsyncSession,
    *,
    project_id: UUID,
    source_file_id: UUID,
) -> DrawingRevision | None:
    """Load the latest drawing revision for a source file."""
    result = await session.execute(
        select(DrawingRevision)
        .where(
            (DrawingRevision.project_id == project_id)
            & (DrawingRevision.source_file_id == source_file_id)
        )
        .order_by(DrawingRevision.revision_sequence.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def _get_drawing_revision(
    session: AsyncSession,
    *,
    revision_id: UUID,
) -> DrawingRevision | None:
    """Load a drawing revision row by identifier."""

    return await session.get(DrawingRevision, revision_id)


async def _get_validation_report_for_revision(
    session: AsyncSession,
    *,
    project_id: UUID,
    drawing_revision_id: UUID,
) -> ValidationReport | None:
    """Load the canonical validation report for a drawing revision."""
    result = await session.execute(
        select(ValidationReport).where(
            (ValidationReport.project_id == project_id)
            & (ValidationReport.drawing_revision_id == drawing_revision_id)
        )
    )
    return result.scalar_one_or_none()


async def _get_revision_layouts_for_revision(
    session: AsyncSession,
    *,
    project_id: UUID,
    source_file_id: UUID,
    drawing_revision_id: UUID,
) -> list[RevisionLayout]:
    """Load deterministic layout rows for a drawing revision."""
    result = await session.execute(
        select(RevisionLayout)
        .where(
            (RevisionLayout.project_id == project_id)
            & (RevisionLayout.source_file_id == source_file_id)
            & (RevisionLayout.drawing_revision_id == drawing_revision_id)
        )
        .order_by(RevisionLayout.sequence_index.asc(), RevisionLayout.id.asc())
    )
    return list(result.scalars().all())


async def _get_revision_layers_for_revision(
    session: AsyncSession,
    *,
    project_id: UUID,
    source_file_id: UUID,
    drawing_revision_id: UUID,
) -> list[RevisionLayer]:
    """Load deterministic layer rows for a drawing revision."""
    result = await session.execute(
        select(RevisionLayer)
        .where(
            (RevisionLayer.project_id == project_id)
            & (RevisionLayer.source_file_id == source_file_id)
            & (RevisionLayer.drawing_revision_id == drawing_revision_id)
        )
        .order_by(RevisionLayer.sequence_index.asc(), RevisionLayer.id.asc())
    )
    return list(result.scalars().all())


async def _get_revision_blocks_for_revision(
    session: AsyncSession,
    *,
    project_id: UUID,
    source_file_id: UUID,
    drawing_revision_id: UUID,
) -> list[RevisionBlock]:
    """Load deterministic block rows for a drawing revision."""
    result = await session.execute(
        select(RevisionBlock)
        .where(
            (RevisionBlock.project_id == project_id)
            & (RevisionBlock.source_file_id == source_file_id)
            & (RevisionBlock.drawing_revision_id == drawing_revision_id)
        )
        .order_by(RevisionBlock.sequence_index.asc(), RevisionBlock.id.asc())
    )
    return list(result.scalars().all())


async def _get_revision_entity_manifest_for_revision(
    session: AsyncSession,
    *,
    project_id: UUID,
    source_file_id: UUID,
    drawing_revision_id: UUID,
) -> RevisionEntityManifest | None:
    """Load the revision entity manifest for a drawing revision."""
    result = await session.execute(
        select(RevisionEntityManifest).where(
            (RevisionEntityManifest.project_id == project_id)
            & (RevisionEntityManifest.source_file_id == source_file_id)
            & (RevisionEntityManifest.drawing_revision_id == drawing_revision_id)
        )
    )
    return result.scalar_one_or_none()


async def _get_revision_entities_for_revision(
    session: AsyncSession,
    *,
    project_id: UUID,
    source_file_id: UUID,
    drawing_revision_id: UUID,
) -> list[RevisionEntity]:
    """Load deterministic materialized entities for a drawing revision."""
    result = await session.execute(
        select(RevisionEntity)
        .where(
            (RevisionEntity.project_id == project_id)
            & (RevisionEntity.source_file_id == source_file_id)
            & (RevisionEntity.drawing_revision_id == drawing_revision_id)
        )
        .order_by(RevisionEntity.sequence_index.asc(), RevisionEntity.id.asc())
    )
    return list(result.scalars().all())


def _revision_reference(
    revision: DrawingRevision | None,
) -> tuple[str | None, int | None]:
    """Return stable revision identifier/sequence details for conflict payloads."""

    if revision is None:
        return None, None

    return str(revision.id), revision.revision_sequence


def _build_revision_conflict_details(
    *,
    base_revision: DrawingRevision | None,
    current_revision: DrawingRevision | None,
) -> dict[str, str | int | None]:
    """Build structured stale-base details for durable job events."""

    base_revision_id, base_revision_sequence = _revision_reference(base_revision)
    current_revision_id, current_revision_sequence = _revision_reference(current_revision)
    return {
        "base_revision_id": base_revision_id,
        "base_revision_sequence": base_revision_sequence,
        "current_revision_id": current_revision_id,
        "current_revision_sequence": current_revision_sequence,
    }
