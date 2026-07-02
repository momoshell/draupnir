"""Revision drawing-scale / units read route.

The pure units/pdf-scale helpers moved to ``app.interpretation.scale_resolution``
(#852). ``resolve_revision_scale`` STAYS here: it depends on ``_get_active_revision``
from ``app.api.v1.revision_lineage``, and the new interpretation module must not import
from ``app.api.*`` (grep-verified) — extracting it would either reintroduce that
dependency into interpretation or require injecting ``get_active_revision`` as a
parameter for no behavior benefit. Per the handover spec's decision rule, keeping it in
the route is preferred since the injection isn't trivial-and-load-bearing enough to
justify a second module boundary here.
"""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.revision_lineage import _get_active_revision
from app.core.exceptions import raise_not_found
from app.db.session import get_db
from app.interpretation.scale_resolution import (
    _as_dict,
    _enrich_units_from_pdf_scale,
    _is_contradicted,
    _real_world_dimensions_available,
    _resolve_pdf_scale,
    _units_confidence,
)
from app.models.adapter_run_output import AdapterRunOutput
from app.models.drawing_revision import DrawingRevision
from app.models.file import File
from app.models.project import Project
from app.schemas.revision import RevisionScaleRead

scale_router = APIRouter()


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
            units={"normalized": "unknown"},
            units_confidence="unknown",
            real_world_dimensions_available=False,
            units_contradicted=False,
            pdf_scale=None,
            source_input_family=None,
        )

    canonical = _as_dict(adapter_output.canonical_json)
    pdf_scale = _resolve_pdf_scale(canonical)
    units_raw = _as_dict(canonical.get("units")) or {"normalized": "unknown"}
    # For PDF-vector revisions with a confident derived scale, enrich the units block so
    # consumers see units.normalized = real_world_unit and confidence = "confirmed" rather
    # than the generic {"normalized": "unknown"} the pymupdf adapter always emits.
    # DWG/DXF units are never touched — the enrichment gate checks input_family first.
    units = _enrich_units_from_pdf_scale(units_raw, pdf_scale, adapter_output.input_family)
    return RevisionScaleRead(
        units=units,
        units_confidence=_units_confidence(units),  # type: ignore[arg-type]
        real_world_dimensions_available=_real_world_dimensions_available(units, pdf_scale),
        units_contradicted=_is_contradicted(units),
        pdf_scale=pdf_scale,
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
