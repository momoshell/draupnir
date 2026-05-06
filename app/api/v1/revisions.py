"""Revision API routes."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import raise_not_found
from app.db.session import get_db
from app.models.drawing_revision import DrawingRevision
from app.models.validation_report import ValidationReport
from app.schemas.validation_report import (
    ValidationReportResponse,
    build_validation_report_response,
)

revisions_router = APIRouter()


@revisions_router.get(
    "/revisions/{revision_id}/validation-report",
    response_model=ValidationReportResponse,
)
async def get_validation_report(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> ValidationReportResponse:
    """Return the persisted canonical validation report for a drawing revision."""
    result = await db.execute(
        select(ValidationReport).where(ValidationReport.drawing_revision_id == revision_id)
    )
    report = result.scalar_one_or_none()
    if report is None:
        revision = await db.get(DrawingRevision, revision_id)
        if revision is None:
            raise_not_found("Drawing revision", str(revision_id))
        raise_not_found("Validation report", str(revision_id))

    assert report is not None

    return build_validation_report_response(report)
