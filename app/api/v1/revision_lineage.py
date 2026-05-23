"""Revision lineage helpers."""

from collections.abc import Awaitable, Callable
from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import ErrorCode
from app.core.exceptions import create_error_response, raise_not_found
from app.models.drawing_revision import DrawingRevision
from app.models.estimate_version import EstimateVersion
from app.models.file import File
from app.models.project import Project
from app.models.quantity_takeoff import QuantityTakeoff
from app.models.revision_materialization import RevisionEntityManifest
from app.models.validation_report import ValidationReport
from app.schemas.revision import RevisionMaterializationCounts


def _raise_entities_not_materialized(revision_id: UUID) -> None:
    """Raise the standard conflict when revision entities are not materialized."""

    raise HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail=create_error_response(
            code=ErrorCode.NORMALIZED_ENTITIES_NOT_MATERIALIZED,
            message=(
                f"Normalized entities for drawing revision '{revision_id}' "
                "have not been materialized"
            ),
            details=None,
        ),
    )


async def _get_active_file(file_id: UUID, db: AsyncSession) -> File | None:
    """Return an active file visible through a non-deleted project."""

    result = await db.execute(
        select(File)
        .join(Project, Project.id == File.project_id)
        .where((File.id == file_id) & (File.deleted_at.is_(None)) & (Project.deleted_at.is_(None)))
    )
    return result.scalar_one_or_none()


async def _get_active_revision(
    revision_id: UUID,
    db: AsyncSession,
    *,
    for_update: bool = False,
) -> DrawingRevision | None:
    """Return an active revision visible through a non-deleted file and project."""

    query = (
        select(DrawingRevision)
        .join(
            File,
            (File.id == DrawingRevision.source_file_id)
            & (File.project_id == DrawingRevision.project_id),
        )
        .join(Project, Project.id == DrawingRevision.project_id)
        .where(
            (DrawingRevision.id == revision_id)
            & (File.deleted_at.is_(None))
            & (Project.deleted_at.is_(None))
        )
    )
    if for_update:
        query = query.with_for_update(
            of=(DrawingRevision.__table__, File.__table__, Project.__table__)
        )

    result = await db.execute(query)
    return result.scalar_one_or_none()


async def _get_active_validation_report(
    revision_id: UUID,
    db: AsyncSession,
) -> ValidationReport | None:
    """Return the persisted validation report for an active revision, if present."""

    result = await db.execute(
        select(ValidationReport)
        .join(
            DrawingRevision,
            DrawingRevision.id == ValidationReport.drawing_revision_id,
        )
        .join(
            File,
            (File.id == DrawingRevision.source_file_id)
            & (File.project_id == DrawingRevision.project_id),
        )
        .join(Project, Project.id == DrawingRevision.project_id)
        .where(
            (ValidationReport.drawing_revision_id == revision_id)
            & (File.deleted_at.is_(None))
            & (Project.deleted_at.is_(None))
        )
    )
    return result.scalar_one_or_none()


async def _get_active_validation_report_or_404(
    revision_id: UUID,
    db: AsyncSession,
    *,
    get_active_validation_report: Callable[
        [UUID, AsyncSession], Awaitable[ValidationReport | None]
    ] = _get_active_validation_report,
    get_active_revision: Callable[..., Awaitable[DrawingRevision | None]] = _get_active_revision,
) -> ValidationReport:
    """Return an active revision validation report or raise not found."""

    report = await get_active_validation_report(revision_id, db)
    if report is None:
        revision = await get_active_revision(revision_id, db)
        if revision is None:
            raise_not_found("Drawing revision", str(revision_id))
        raise_not_found("Validation report", str(revision_id))

    assert report is not None
    return report


async def _get_revision_manifest(
    revision_id: UUID,
    db: AsyncSession,
) -> RevisionEntityManifest | None:
    """Return the persisted materialization manifest for a revision, if present."""

    result = await db.execute(
        select(RevisionEntityManifest).where(
            RevisionEntityManifest.drawing_revision_id == revision_id
        )
    )
    return result.scalar_one_or_none()


async def _get_active_revision_manifest_or_409(
    revision_id: UUID,
    db: AsyncSession,
    *,
    get_active_revision: Callable[..., Awaitable[DrawingRevision | None]] = _get_active_revision,
    get_revision_manifest: Callable[
        [UUID, AsyncSession], Awaitable[RevisionEntityManifest | None]
    ] = _get_revision_manifest,
    raise_entities_not_materialized: Callable[[UUID], None] = _raise_entities_not_materialized,
) -> RevisionEntityManifest:
    """Return an active revision manifest or raise the standard missing errors."""

    revision = await get_active_revision(revision_id, db)
    if revision is None:
        raise_not_found("Drawing revision", str(revision_id))
    assert revision is not None

    manifest = await get_revision_manifest(revision_id, db)
    if manifest is None:
        raise_entities_not_materialized(revision_id)

    assert manifest is not None
    return manifest


def _manifest_counts(
    manifest: RevisionEntityManifest,
) -> RevisionMaterializationCounts:
    """Convert persisted manifest counts to the public schema."""

    return RevisionMaterializationCounts.model_validate(manifest.counts_json)


async def _get_revision_quantity_takeoff_or_404(
    revision_id: UUID,
    takeoff_id: UUID,
    db: AsyncSession,
) -> QuantityTakeoff:
    """Return a committed quantity takeoff scoped to an active revision."""

    result = await db.execute(
        select(QuantityTakeoff)
        .join(
            File,
            (File.id == QuantityTakeoff.source_file_id)
            & (File.project_id == QuantityTakeoff.project_id),
        )
        .join(Project, Project.id == QuantityTakeoff.project_id)
        .where(
            (QuantityTakeoff.id == takeoff_id)
            & (QuantityTakeoff.drawing_revision_id == revision_id)
            & (File.deleted_at.is_(None))
            & (Project.deleted_at.is_(None))
        )
    )
    takeoff = result.scalar_one_or_none()
    if takeoff is None:
        raise_not_found("Quantity takeoff", str(takeoff_id))

    assert takeoff is not None
    return takeoff


async def _get_revision_estimate_version_or_404(
    revision_id: UUID,
    estimate_version_id: UUID,
    db: AsyncSession,
) -> EstimateVersion:
    """Return a committed estimate version scoped to an active revision."""

    result = await db.execute(
        select(EstimateVersion)
        .join(
            File,
            (File.id == EstimateVersion.source_file_id)
            & (File.project_id == EstimateVersion.project_id),
        )
        .join(Project, Project.id == EstimateVersion.project_id)
        .where(
            (EstimateVersion.id == estimate_version_id)
            & (EstimateVersion.drawing_revision_id == revision_id)
            & (File.deleted_at.is_(None))
            & (Project.deleted_at.is_(None))
        )
    )
    estimate_version = result.scalar_one_or_none()
    if estimate_version is None:
        raise_not_found("Estimate version", str(estimate_version_id))

    assert estimate_version is not None
    return estimate_version
