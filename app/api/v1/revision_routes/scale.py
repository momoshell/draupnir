"""Revision drawing-scale / units read route."""

from collections.abc import Mapping
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


_UNITS_CONFIDENCE_VALUES = frozenset({"declared", "confirmed", "inferred", "unknown"})


def _units_confidence(units: Mapping[str, Any]) -> str:
    """Derive the unit-certainty label from the persisted units block (#557).

    An adapter that records its own ``confidence`` (e.g. the inferred path, #558) wins. Otherwise
    derive: a normalized unit means the adapter confirmed it (DWG ``$INSUNITS`` only emits a unit
    when confirmed); a missing/``unknown`` normalized unit is honestly ``unknown``.
    """
    explicit = units.get("confidence")
    if isinstance(explicit, str) and explicit in _UNITS_CONFIDENCE_VALUES:
        return explicit
    normalized = units.get("normalized")
    if not isinstance(normalized, str) or normalized == "unknown":
        return "unknown"
    return "confirmed"


def _is_contradicted(units: Mapping[str, Any]) -> bool:
    """Return True when the units block carries a truthy contradiction sub-block (#558).

    Defensive: any truthy value means contradicted; missing/falsy means not. Never raises on a
    malformed block — a confirmed unit that a later engine contradicts must not keep reporting
    real_world_dimensions_available=True.
    """
    return bool(units.get("contradiction"))


def _real_world_dimensions_available(
    units: Mapping[str, Any], pdf_scale: Mapping[str, Any] | None
) -> bool:
    """True when real measurements can be quoted: a usable unit conversion factor (DWG) or a
    detected PDF point->real factor (#557). Honest False when neither is present.

    A contradiction on the units block (#558) voids the units signal only; the PDF signal is
    independent and unchanged.
    """
    units_ok = (
        _units_confidence(units) != "unknown"
        and _positive_number(units.get("conversion_factor"))
        and not _is_contradicted(units)
    )
    # The PDF point->real factor is the unambiguous "can convert" signal (present iff a scale
    # ratio + real unit were detected); key on it rather than the variably-typed flag.
    pdf_ok = isinstance(pdf_scale, Mapping) and _positive_number(pdf_scale.get("points_to_real"))
    return bool(units_ok or pdf_ok)


def _positive_number(value: Any) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool) and value > 0


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
    pdf_scale_raw = canonical.get("pdf_scale")
    pdf_scale = _as_dict(pdf_scale_raw) if isinstance(pdf_scale_raw, dict) else None
    units = _as_dict(canonical.get("units")) or {"normalized": "unknown"}
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
