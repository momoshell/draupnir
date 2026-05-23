"""Revision adapter-output read routes."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.revision_compat import _get_active_revision
from app.core.exceptions import raise_not_found
from app.db.session import get_db
from app.models.adapter_run_output import AdapterRunOutput
from app.models.drawing_revision import DrawingRevision
from app.models.file import File
from app.models.project import Project
from app.schemas.revision import AdapterRunOutputRead

adapter_outputs_router = APIRouter()


@adapter_outputs_router.get(
    "/revisions/{revision_id}/adapter-output",
    response_model=AdapterRunOutputRead,
)
async def get_revision_adapter_output(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> AdapterRunOutputRead:
    """Return adapter output metadata for an active drawing revision."""

    result = await db.execute(
        select(AdapterRunOutput)
        .join(DrawingRevision, DrawingRevision.adapter_run_output_id == AdapterRunOutput.id)
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
    adapter_output = result.scalar_one_or_none()
    if adapter_output is None:
        revision = await _get_active_revision(revision_id, db)
        if revision is None:
            raise_not_found("Drawing revision", str(revision_id))
        raise_not_found("Adapter run output", str(revision_id))

    return AdapterRunOutputRead.model_validate(adapter_output)


@adapter_outputs_router.get(
    "/adapter-outputs/{adapter_output_id}",
    response_model=AdapterRunOutputRead,
)
async def get_adapter_output(
    adapter_output_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> AdapterRunOutputRead:
    """Return adapter output metadata by identifier."""

    result = await db.execute(
        select(AdapterRunOutput)
        .join(
            File,
            (File.id == AdapterRunOutput.source_file_id)
            & (File.project_id == AdapterRunOutput.project_id),
        )
        .join(Project, Project.id == AdapterRunOutput.project_id)
        .where(
            (AdapterRunOutput.id == adapter_output_id)
            & (File.deleted_at.is_(None))
            & (Project.deleted_at.is_(None))
        )
    )
    adapter_output = result.scalar_one_or_none()
    if adapter_output is None:
        raise_not_found("Adapter run output", str(adapter_output_id))

    return AdapterRunOutputRead.model_validate(adapter_output)
