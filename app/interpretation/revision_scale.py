"""DB-backed revision scale resolution service."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal, cast
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.adapter_run_output import AdapterRunOutput
from app.models.drawing_revision import DrawingRevision
from app.models.file import File
from app.models.project import Project
from app.schemas.revision import RevisionScaleRead


class RevisionScaleNotFoundError(LookupError):
    """Raised when the requested revision is not active or visible."""


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


_UNITS_CONFIDENCE_VALUES = frozenset({"declared", "confirmed", "inferred", "unknown"})
type UnitsConfidence = Literal["declared", "confirmed", "inferred", "unknown"]

# Real-world units the PDF scale factor can be normalized to metres against;
# mirrors the pymupdf adapter's emitted `real_world_unit` values.
_PDF_REAL_WORLD_UNITS = frozenset({"millimeter", "centimeter", "meter"})


def _units_confidence(units: Mapping[str, Any]) -> UnitsConfidence:
    """Derive the unit-certainty label from the persisted units block (#557)."""
    explicit = units.get("confidence")
    if isinstance(explicit, str) and explicit in _UNITS_CONFIDENCE_VALUES:
        return cast(UnitsConfidence, explicit)
    normalized = units.get("normalized")
    if not isinstance(normalized, str) or normalized == "unknown":
        return "unknown"
    return "confirmed"


def _is_contradicted(units: Mapping[str, Any]) -> bool:
    """Return True when the units block carries a truthy contradiction sub-block (#558)."""
    return bool(units.get("contradiction"))


def _real_world_dimensions_available(
    units: Mapping[str, Any], pdf_scale: Mapping[str, Any] | None
) -> bool:
    """True when real measurements can be quoted from units or PDF scale."""
    units_ok = (
        _units_confidence(units) != "unknown"
        and _positive_number(units.get("conversion_factor"))
        and not _is_contradicted(units)
    )
    pdf_ok = (
        isinstance(pdf_scale, Mapping)
        and _positive_number(pdf_scale.get("points_to_real"))
        and pdf_scale.get("real_world_unit") in _PDF_REAL_WORLD_UNITS
    )
    return bool(units_ok or pdf_ok)


def _positive_number(value: Any) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool) and value > 0


async def resolve_revision_scale(revision_id: UUID, db: AsyncSession) -> RevisionScaleRead:
    """Resolve the drawing scale + units for an active revision.

    Sourced from the revision's adapter-run canonical payload. A revision with no
    adapter run, such as a changeset-origin revision, honestly reports unknown units.
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
        revision_exists = await _active_revision_exists(revision_id, db)
        if not revision_exists:
            raise RevisionScaleNotFoundError(str(revision_id))
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
        units_confidence=_units_confidence(units),
        real_world_dimensions_available=_real_world_dimensions_available(units, pdf_scale),
        units_contradicted=_is_contradicted(units),
        pdf_scale=pdf_scale,
        source_input_family=adapter_output.input_family,
    )


async def _active_revision_exists(revision_id: UUID, db: AsyncSession) -> bool:
    result = await db.execute(
        select(DrawingRevision.id)
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
    return result.scalar_one_or_none() is not None
