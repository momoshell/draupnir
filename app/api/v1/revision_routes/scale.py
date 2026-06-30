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

# Real-world units the PDF scale factor can be normalized to metres against; mirrors the
# pymupdf adapter's emitted `real_world_unit` values.
_PDF_REAL_WORLD_UNITS = frozenset({"millimeter", "centimeter", "meter"})

# Input-family value for PDF vector drawings (pymupdf adapter).
_INPUT_FAMILY_PDF_VECTOR = "pdf_vector"

# pdf_scale.status value emitted by the pymupdf adapter when a confident ratio + unit were found.
_PDF_SCALE_STATUS_DERIVED = "derived_from_text"


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
    # ratio + real unit were detected). Require BOTH the factor AND a known real-world unit so
    # the signal stays consistent with what consumers can actually convert: without the unit the
    # factor cannot be normalized to metres, so it is honestly not convertible (ADR-004).
    pdf_ok = (
        isinstance(pdf_scale, Mapping)
        and _positive_number(pdf_scale.get("points_to_real"))
        and pdf_scale.get("real_world_unit") in _PDF_REAL_WORLD_UNITS
    )
    return bool(units_ok or pdf_ok)


def _positive_number(value: Any) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool) and value > 0


def _resolve_pdf_scale(canonical: dict[str, Any]) -> dict[str, Any] | None:
    """Extract the pdf_scale block from the canonical payload.

    The pymupdf adapter stores it under ``canonical["metadata"]["pdf_scale"]``; older
    test fixtures and future adapters may place it at the top-level
    ``canonical["pdf_scale"]``.  Check the top level first (explicit wins) then fall
    back to the metadata-nested location so both layouts are supported without
    duplicating the read.
    """
    top_level = canonical.get("pdf_scale")
    if isinstance(top_level, dict):
        return _as_dict(top_level)
    metadata = _as_dict(canonical.get("metadata"))
    nested = metadata.get("pdf_scale")
    if isinstance(nested, dict):
        return _as_dict(nested)
    return None


def _enrich_units_from_pdf_scale(
    units: dict[str, Any],
    pdf_scale: dict[str, Any] | None,
    input_family: str | None,
) -> dict[str, Any]:
    """For PDF-vector revisions with a confident derived scale, surface the real-world unit
    and confidence onto the units block so that ``_units_confidence`` and API consumers
    see a usable ``normalized`` value rather than ``"unknown"``.

    Gate: only when all of the following hold (ADR-004 honesty):
    - ``input_family`` is ``pdf_vector``
    - ``pdf_scale`` is present
    - ``pdf_scale["status"] == "derived_from_text"`` (the adapter found a ratio + unit)
    - ``pdf_scale["real_world_unit"]`` is a recognised unit

    When the gate fails the original ``units`` dict is returned unchanged, preserving
    the existing behaviour for DWG/DXF, changeset-origin, and unconfirmed PDF revisions.
    The returned dict is always a new shallow copy; the original is never mutated.
    """
    if input_family != _INPUT_FAMILY_PDF_VECTOR:
        return units
    if not isinstance(pdf_scale, Mapping):
        return units
    if pdf_scale.get("status") != _PDF_SCALE_STATUS_DERIVED:
        return units
    real_world_unit = pdf_scale.get("real_world_unit")
    if not isinstance(real_world_unit, str) or real_world_unit not in _PDF_REAL_WORLD_UNITS:
        return units
    enriched = dict(units)
    enriched["normalized"] = real_world_unit
    enriched["confidence"] = "confirmed"
    return enriched


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
