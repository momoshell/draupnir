"""Revision drawing-scale / units read route."""

from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.revision_lineage import _get_active_revision
from app.core.exceptions import raise_not_found
from app.db.session import get_db
from app.models.adapter_run_output import AdapterRunOutput
from app.models.drawing_revision import DrawingRevision
from app.models.file import File
from app.models.project import Project
from app.schemas.revision import RevisionScaleRead

scale_router = APIRouter()


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


async def resolve_revision_scale(revision_id: UUID, db: AsyncSession) -> RevisionScaleRead:
    """Resolve the drawing scale + units for an active revision.

    Sourced from the revision's adapter-run canonical payload. A revision with no
    adapter run (e.g. changeset-origin) honestly reports ``units = unknown``.
    Raises a 404 if the revision does not exist / is not visible.
    """

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
        # Active revision with no adapter run (changeset-origin): scale is unavailable.
        return RevisionScaleRead(
            units={"normalized": "unknown"}, pdf_scale=None, source_input_family=None
        )

    canonical = _as_dict(adapter_output.canonical_json)
    pdf_scale = canonical.get("pdf_scale")
    return RevisionScaleRead(
        units=_as_dict(canonical.get("units")) or {"normalized": "unknown"},
        pdf_scale=_as_dict(pdf_scale) if isinstance(pdf_scale, dict) else None,
        source_input_family=adapter_output.input_family,
    )


@scale_router.get(
    "/revisions/{revision_id}/scale",
    response_model=RevisionScaleRead,
)
async def get_revision_scale(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> RevisionScaleRead:
    """Return the drawing scale + units for an active revision.

    Sourced from the revision's adapter-run canonical payload. A revision with no
    adapter run (e.g. changeset-origin) honestly reports ``units = unknown``.
    """

    return await resolve_revision_scale(revision_id, db)
