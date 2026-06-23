"""Revision interpretation + source-census read routes (#566).

Surfaces two descriptive canonical blocks that #562/#563 populate but no endpoint
previously exposed: ``interpretation`` (length/angle/orientation readings) and
``census`` (raw-object histogram vs what was materialized, plus reader blind spots).

Read-only passthrough of the revision's adapter-run canonical payload — no schema or
gating change. A revision whose adapter emitted no such block (e.g. a non-DWG adapter,
or a changeset-origin revision with no adapter run) honestly reports ``available=False``
rather than fabricating values. A revision that does not exist returns 404.
"""

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
from app.schemas.revision import RevisionCensusRead, RevisionInterpretationRead

canonical_router = APIRouter()


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _opt_dict(value: Any) -> dict[str, Any] | None:
    return dict(value) if isinstance(value, dict) else None


def _opt_int(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


async def _load_revision_adapter_output(
    revision_id: UUID, db: AsyncSession
) -> AdapterRunOutput | None:
    """Resolve the active revision's adapter-run output, or None when it has no run.

    Mirrors the scale route's resolution: joins through the active (non-deleted) file +
    project. Returns None when the revision exists but originated from a changeset (no
    adapter run). Raises 404 when the revision does not exist / is not visible.
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
        # Active revision with no adapter run (changeset-origin): canonical is unavailable.
        return None
    return adapter_output


def _unavailable_interpretation(*, source_input_family: str | None) -> RevisionInterpretationRead:
    """An honest "no interpretation block" response (all readings null)."""
    return RevisionInterpretationRead(
        available=False,
        schema_version=None,
        length=None,
        angle=None,
        orientation=None,
        source_input_family=source_input_family,
    )


def _unavailable_census(*, source_input_family: str | None) -> RevisionCensusRead:
    """An honest "no census block" response (counts null, collections empty)."""
    return RevisionCensusRead(
        available=False,
        schema_version=None,
        source=None,
        raw_object_total=None,
        raw_objects={},
        drawable_candidates=None,
        materialized=None,
        dropped=None,
        unsupported_classes=[],
        source_input_family=source_input_family,
    )


@canonical_router.get(
    "/revisions/{revision_id}/interpretation",
    response_model=RevisionInterpretationRead,
)
async def get_revision_interpretation(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> RevisionInterpretationRead:
    """Return the interpretation summary (length/angle/orientation) for an active revision.

    Sourced from the revision's adapter-run canonical payload (#562). A revision whose
    adapter emitted no interpretation block honestly reports ``available=False``.
    """

    adapter_output = await _load_revision_adapter_output(revision_id, db)
    if adapter_output is None:
        return _unavailable_interpretation(source_input_family=None)

    interpretation = _opt_dict(_as_dict(adapter_output.canonical_json).get("interpretation"))
    if interpretation is None:
        return _unavailable_interpretation(source_input_family=adapter_output.input_family)

    return RevisionInterpretationRead(
        available=True,
        schema_version=interpretation.get("schema_version"),
        length=_opt_dict(interpretation.get("length")),
        angle=_opt_dict(interpretation.get("angle")),
        orientation=_opt_dict(interpretation.get("orientation")),
        source_input_family=adapter_output.input_family,
    )


@canonical_router.get(
    "/revisions/{revision_id}/census",
    response_model=RevisionCensusRead,
)
async def get_revision_census(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> RevisionCensusRead:
    """Return the source census (raw vs materialized + blind spots) for an active revision.

    Sourced from the revision's adapter-run canonical payload (#563). A revision whose
    adapter emitted no census block honestly reports ``available=False``.
    """

    adapter_output = await _load_revision_adapter_output(revision_id, db)
    if adapter_output is None:
        return _unavailable_census(source_input_family=None)

    census = _opt_dict(_as_dict(adapter_output.canonical_json).get("census"))
    if census is None:
        return _unavailable_census(source_input_family=adapter_output.input_family)

    raw_objects_raw = census.get("raw_objects")
    # Counts are ints; coerce a stray float (possible after a JSON numeric round-trip)
    # rather than silently dropping it, which would make the histogram disagree with
    # raw_object_total.
    raw_objects = (
        {
            str(k): int(v)
            for k, v in raw_objects_raw.items()
            if isinstance(v, int | float) and not isinstance(v, bool)
        }
        if isinstance(raw_objects_raw, dict)
        else {}
    )

    unsupported_raw = census.get("unsupported_classes")
    unsupported_classes = (
        [dict(c) for c in unsupported_raw if isinstance(c, dict)]
        if isinstance(unsupported_raw, list)
        else []
    )

    return RevisionCensusRead(
        available=True,
        schema_version=census.get("schema_version"),
        source=census.get("source"),
        raw_object_total=_opt_int(census.get("raw_object_total")),
        raw_objects=raw_objects,
        drawable_candidates=_opt_int(census.get("drawable_candidates")),
        materialized=_opt_int(census.get("materialized")),
        dropped=_opt_dict(census.get("dropped")),
        unsupported_classes=unsupported_classes,
        source_input_family=adapter_output.input_family,
    )
