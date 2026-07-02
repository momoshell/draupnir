"""Revision scale/units resolution helpers (extracted from the scale route, #852).

Pure interpretation logic over an adapter run's canonical payload: deriving units
confidence, real-world-dimension availability, and the PDF-vector scale enrichment.

``resolve_revision_scale`` itself STAYS in ``app.api.v1.revision_routes.scale`` — it
depends on ``_get_active_revision`` from ``app.api.v1.revision_lineage``, and this module
must not import from ``app.api.*`` (see module docstring in scale.py for the decision
note). Only the pure helpers below moved.

Honesty gates are bit-identical to the pre-extraction route: "Inferred values are never
confirmed", "Real-world quantities are scale-gated" (ADR-004).
"""

from collections.abc import Mapping
from typing import Any

_UNITS_CONFIDENCE_VALUES = frozenset({"declared", "confirmed", "inferred", "unknown"})

# Real-world units the PDF scale factor can be normalized to metres against; mirrors the
# pymupdf adapter's emitted `real_world_unit` values.
_PDF_REAL_WORLD_UNITS = frozenset({"millimeter", "centimeter", "meter"})

# Input-family value for PDF vector drawings (pymupdf adapter).
_INPUT_FAMILY_PDF_VECTOR = "pdf_vector"

# pdf_scale.status value emitted by the pymupdf adapter when a confident ratio + unit were found.
_PDF_SCALE_STATUS_DERIVED = "derived_from_text"


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


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
