"""Revision validation-report read routes."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.revision_lineage import _get_active_validation_report_or_404
from app.db.session import get_db
from app.schemas.validation_report import (
    ValidationReportResponse,
    build_validation_report_response,
)

validation_reports_router = APIRouter()


@validation_reports_router.get(
    "/revisions/{revision_id}/validation-report",
    response_model=ValidationReportResponse,
)
async def get_validation_report(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> ValidationReportResponse:
    """Return the persisted canonical validation report for a drawing revision."""

    report = await _get_active_validation_report_or_404(revision_id, db)
    return build_validation_report_response(report)
