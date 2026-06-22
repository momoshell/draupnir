"""DB seam for the routed-service takeoff coordinator (issue #606, Phase 3 / be-p3-01).

Single database seam: all DB access for the takeoff pipeline lives here. The pure
coordinator (``service_takeoff.py``) consumes only the frozen dataclasses exported by
this module -- no ORM, no SQL.

Loads:
- :class:`~app.interpretation.routed_runs.RoutedEntity` list (line/polyline/arc with
  resolved ``.style`` colour).
- :class:`~app.interpretation.service_legend.ServiceLegend` (geometric swatch+text
  pairing from legend layers + prose abbreviation rows).
- :class:`~app.interpretation.run_service_identity.TagPlacement` list (pipe-tag text
  entities with insertion point).
- ``geometry_by_entity_id`` mapping for all loaded routed entities.
- :class:`~app.interpretation.measurement.ScaleContext` from the canonical units block
  (via the same rules as ``scale.py``; ADR-004 compliant).

All functions are tolerant: empty / degenerate revisions return empty lists / unknown
scale, never raise.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.revision_routes.scale import resolve_revision_scale
from app.interpretation.loaders import (
    load_legend_text_candidates,
    load_revision_entities_by_type,
)
from app.interpretation.measurement import ScaleContext
from app.interpretation.rise_drop import RiseDropEntity
from app.interpretation.routed_runs import ROUTED_ENTITY_TYPES, RoutedEntity
from app.interpretation.run_service_identity import TagPlacement
from app.interpretation.service_legend import (
    ProseInput,
    ServiceLegend,
    SwatchInput,
    from_abbreviation_prose,
    from_colour_swatches,
    fuse,
)

# ---------------------------------------------------------------------------
# Input-family constant (ADR-003: branch on family, never on hardcoded colours/layers)
# ---------------------------------------------------------------------------

INPUT_FAMILY_PDF_VECTOR = "pdf_vector"

# ---------------------------------------------------------------------------
# PDF scale normalisation — metres per detected real-world unit (ADR-004).
# Used only when the units block gives no conversion_factor (PDF-origin revisions).
# ---------------------------------------------------------------------------

_METRES_PER_PDF_UNIT: dict[str, float] = {
    "millimeter": 0.001,
    "centimeter": 0.01,
    "meter": 1.0,
}

# ---------------------------------------------------------------------------
# Layer defaults — ilike patterns, never hardcoded to a single value (ADR-003)
# ---------------------------------------------------------------------------

# Layers bearing routed linework: anything matching these tokens (case-insensitive).
_DEFAULT_PIPE_LAYER_TOKENS: tuple[str, ...] = ("pipe",)

# Centerline layers carry the single-line route and are preferred over double-line pipe
# wall layers (which double-count measured length when both walls are present).
_DEFAULT_CENTERLINE_LAYER_TOKENS: tuple[str, ...] = (
    "center line",
    "centre line",
    "centerline",
    "centreline",
)

# Layers bearing pipe-tag text annotations.
_DEFAULT_TAG_LAYER_TOKENS: tuple[str, ...] = ("pipe tag", "pipetag", "tag")

# Entity types that make up rise/drop symbols (ARC + HATCH).  Do NOT include line/polyline
# here -- those are handled by ROUTED_ENTITY_TYPES for the pipe runs.
RISE_DROP_ENTITY_TYPES: tuple[str, ...] = ("arc", "hatch")

# Layer token defaults for rise/drop symbols (ilike, ADR-003).
_DEFAULT_RISE_LAYER_TOKENS: tuple[str, ...] = ("rise", "riser")
_DEFAULT_DROP_LAYER_TOKENS: tuple[str, ...] = ("drop",)

# Radius (drawing units) within which a legend text entity is matched to a swatch.
# Sized for a typical legend row height (~5 mm at 1:50 → ~250 drawing units / mm-unit).
_SWATCH_MATCH_RADIUS: float = 2000.0

# A swatch entity is a small coloured closed or line entity; we identify swatches
# as entities that are NOT text/insert/dimension/hatch on a legend layer and that
# carry a resolved colour.
_NON_SWATCH_ENTITY_TYPES: frozenset[str] = frozenset(
    {"text", "mtext", "insert", "dimension", "hatch", "spline"}
)


# ---------------------------------------------------------------------------
# Public bundle dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ServiceTakeoffInputs:
    """All inputs the takeoff coordinator needs, loaded as a single atomic bundle."""

    routed_entities: list[RoutedEntity]
    legend: ServiceLegend
    tag_placements: list[TagPlacement]
    geometry_by_entity_id: dict[str, dict[str, Any]]
    scale: ScaleContext
    rise_entities: list[RiseDropEntity]
    drop_entities: list[RiseDropEntity]
    input_family: str | None = None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _number(value: Any) -> float | None:
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def _point_xy(value: Any) -> tuple[float, float] | None:
    """Return (x, y) from a mapping with ``x``/``y`` keys, or None."""
    if isinstance(value, Mapping):
        x, y = _number(value.get("x")), _number(value.get("y"))
        if x is not None and y is not None:
            return (x, y)
    return None


def _entity_insertion(geometry: dict[str, Any]) -> tuple[float, float] | None:
    """Return the insertion point for a text/insert entity (mirrors devices._entity_xy)."""
    transform = geometry.get("transform")
    if isinstance(transform, Mapping):
        pt = _point_xy(transform.get("insertion_point"))
        if pt is not None:
            return pt
    return _point_xy(geometry.get("insertion") or geometry.get("insert"))


def _nearest_swatch(
    text_pt: tuple[float, float],
    swatches: list[tuple[tuple[float, float], Mapping[str, Any]]],
    radius: float,
) -> Mapping[str, Any] | None:
    """Return the colour of the nearest swatch within ``radius``, or None.

    Mirrors ``label_rooms._nearest_name`` — radius-bounded nearest-neighbour.
    """
    tx, ty = text_pt
    best_dist: float | None = None
    best_color: Mapping[str, Any] | None = None
    for (sx, sy), color in swatches:
        d = math.hypot(sx - tx, sy - ty)
        if d <= radius and (best_dist is None or d < best_dist):
            best_dist = d
            best_color = color
    return best_color


def _swatch_centroid(geometry: dict[str, Any]) -> tuple[float, float] | None:
    """Best-effort centroid for a small swatch entity (line, polyline, solid, rectangle).

    - line: midpoint of start/end
    - polyline/solid: centroid of vertices or points
    - arc/circle: centre
    - fallback: insertion point
    """
    if "start" in geometry and "end" in geometry:
        s, e = geometry["start"], geometry["end"]
        if len(s) >= 2 and len(e) >= 2:
            return ((s[0] + e[0]) / 2.0, (s[1] + e[1]) / 2.0)

    pts: Any = geometry.get("vertices") or geometry.get("points")
    if pts and len(pts) >= 1:
        xs = [p[0] for p in pts if len(p) >= 2]
        ys = [p[1] for p in pts if len(p) >= 2]
        if xs:
            return (sum(xs) / len(xs), sum(ys) / len(ys))

    center = geometry.get("center")
    if isinstance(center, Mapping):
        pt = _point_xy(center)
        if pt is not None:
            return pt

    return _entity_insertion(geometry)


# ---------------------------------------------------------------------------
# Input-family helpers
# ---------------------------------------------------------------------------


async def _resolve_input_family(db: AsyncSession, revision_id: UUID) -> str | None:
    """Return the input_family for the revision's adapter run, or None when unavailable.

    Swallows HTTPException (revision invisible / no adapter run) — callers treat None
    as "unknown origin" and apply DWG/DXF default paths unchanged.
    """
    try:
        return (await resolve_revision_scale(revision_id, db)).source_input_family
    except HTTPException:
        return None


async def _load_text_blocks(db: AsyncSession, revision_id: UUID) -> list[dict[str, Any]]:
    """Return text_blocks from the active revision's AdapterRunOutput canonical_json.

    Mirrors the join used by resolve_revision_scale (scale.py ~98-113) including
    File/Project deleted_at filters. Returns [] when no adapter run is found, the
    metadata key is absent, or text_blocks is not a list/tuple. Never raises.
    """
    from sqlalchemy import select

    from app.models.adapter_run_output import AdapterRunOutput
    from app.models.drawing_revision import DrawingRevision
    from app.models.file import File
    from app.models.project import Project

    try:
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
            return []
        canonical = adapter_output.canonical_json
        if not isinstance(canonical, dict):
            return []
        metadata = canonical.get("metadata")
        if not isinstance(metadata, dict):
            return []
        tb = metadata.get("text_blocks")
        if not isinstance(tb, (list, tuple)):
            return []
        return [dict(b) for b in tb if isinstance(b, dict)]
    except Exception:  # tolerant, never raises
        return []


# ---------------------------------------------------------------------------
# PDF tag-candidate filter
# ---------------------------------------------------------------------------


def _looks_like_pdf_tag(text: str) -> bool:
    """A PDF text block is a tag candidate only with real tag structure: a digit plus an
    'mm' unit or a diameter glyph. Rejects title-block/notes prose (e.g. '5291 LONDON',
    'Drawn: HK', '1:50') that the bare digits+word parse_tag fallback would otherwise
    fabricate a service from. DWG tags are layer-filtered upstream and unaffected."""
    if not re.search(r"\d", text):
        return False
    # 'mm' unit (case-insensitive, not part of a longer word like 'mmhg') OR a diameter glyph
    # (clean U+00D8 or the U+FFFD replacement seen in some pipelines).
    # Pattern: mm preceded by non-word char OR digit, followed by non-word char.
    # Plain ASCII source (RUF002) -> use escapes.
    return (
        bool(re.search(r"(?i)(?<!\w)mm(?!\w)|(?<=\d)mm(?!\w)", text)) or "Ø" in text or "�" in text
    )


# ---------------------------------------------------------------------------
# Public loaders
# ---------------------------------------------------------------------------


async def load_routed_entities(
    db: AsyncSession,
    revision_id: UUID,
    *,
    layer_refs: list[str] | None = None,
    exclude_off_sheet: bool = True,
    input_family: str | None = None,
) -> list[RoutedEntity]:
    """Load line/polyline/arc entities from routed-run layers as :class:`RoutedEntity`.

    When ``layer_refs`` is None and the revision is pdf_vector, selects ALL routed entity
    types without any layer-token filter (PDFs have no meaningful layers); every returned
    entity gets ``layer_ref=None`` so downstream grouping collapses to colour-only.

    When ``layer_refs`` is None and the revision is DWG/DXF (or family is unknown), queries
    centerline-token layers first; falls back to pipe-token layers only when no centerline
    entities are found. Returns an empty list for degenerate revisions; never raises.
    """
    if input_family is None:
        input_family = await _resolve_input_family(db, revision_id)

    if layer_refs is not None:
        # Explicit caller override — always takes precedence regardless of input_family.
        rows = await load_revision_entities_by_type(
            db,
            revision_id,
            ROUTED_ENTITY_TYPES,
            layer_refs=layer_refs,
            exclude_off_sheet=exclude_off_sheet,
        )
        result: list[RoutedEntity] = []
        for row in rows:
            style = row.style or {}
            color: Mapping[str, Any] | None = (
                style.get("color") if isinstance(style, Mapping) else None
            )
            raw_geom = row.geometry_json
            geometry: dict[str, Any] | None = raw_geom if isinstance(raw_geom, dict) else None
            result.append(
                RoutedEntity(
                    entity_id=str(row.id),
                    entity_type=row.entity_type,
                    layer_ref=row.layer_ref,
                    color=color,
                    geometry=geometry,
                )
            )
        return result

    if input_family == INPUT_FAMILY_PDF_VECTOR:
        # PDF revisions have no meaningful layers; select all routed entity types and
        # normalise layer_ref to None so identify_routed_runs groups by colour only
        # (ADR-003 / ADR-006: honest UNKNOWN groups, never legend-filter the selection).
        from sqlalchemy import select

        from app.models.revision_materialization import RevisionEntity

        q = select(RevisionEntity).where(
            RevisionEntity.drawing_revision_id == revision_id,
            RevisionEntity.entity_type.in_(list(ROUTED_ENTITY_TYPES)),
        )
        if exclude_off_sheet:
            q = q.where(RevisionEntity.on_sheet.isnot(False))
        q = q.order_by(RevisionEntity.sequence_index, RevisionEntity.id)
        rows = list((await db.execute(q)).scalars().all())

        pdf_result: list[RoutedEntity] = []
        for row in rows:
            style = row.style or {}
            pdf_color: Mapping[str, Any] | None = (
                style.get("color") if isinstance(style, Mapping) else None
            )
            raw_geom = row.geometry_json
            pdf_geometry: dict[str, Any] | None = raw_geom if isinstance(raw_geom, dict) else None
            pdf_result.append(
                RoutedEntity(
                    entity_id=str(row.id),
                    entity_type=row.entity_type,
                    layer_ref=None,  # normalised — PDFs have no meaningful layer grouping
                    color=pdf_color,
                    geometry=pdf_geometry,
                )
            )
        return pdf_result

    # DWG/DXF (or unknown family): ilike-based layer selection, ADR-003 compliant.
    from sqlalchemy import or_, select

    from app.models.revision_materialization import RevisionEntity

    async def _query_by_tokens(tokens: tuple[str, ...]) -> list[Any]:
        q = select(RevisionEntity).where(
            RevisionEntity.drawing_revision_id == revision_id,
            RevisionEntity.entity_type.in_(list(ROUTED_ENTITY_TYPES)),
        )
        if exclude_off_sheet:
            q = q.where(RevisionEntity.on_sheet.isnot(False))
        # Token tuples are non-empty constants, so or_ always receives at least one arg.
        conditions = [RevisionEntity.layer_ref.ilike(f"%{token}%") for token in tokens]
        q = q.where(or_(*conditions))
        q = q.order_by(RevisionEntity.sequence_index, RevisionEntity.id)
        return list((await db.execute(q)).scalars().all())

    # Prefer centerline layers (single-line route) over pipe-wall layers. Pipe-outline
    # layers may double-count measured length when both walls of a double-line pipe are
    # present; true double-line detection is a deferred follow-up.
    dwg_rows = await _query_by_tokens(_DEFAULT_CENTERLINE_LAYER_TOKENS)
    if not dwg_rows:
        dwg_rows = await _query_by_tokens(_DEFAULT_PIPE_LAYER_TOKENS)

    dwg_result: list[RoutedEntity] = []
    for row in dwg_rows:
        style = row.style or {}
        dwg_color: Mapping[str, Any] | None = (
            style.get("color") if isinstance(style, Mapping) else None
        )
        raw_geom = row.geometry_json
        dwg_geometry: dict[str, Any] | None = raw_geom if isinstance(raw_geom, dict) else None
        dwg_result.append(
            RoutedEntity(
                entity_id=str(row.id),
                entity_type=row.entity_type,
                layer_ref=row.layer_ref,
                color=dwg_color,
                geometry=dwg_geometry,
            )
        )
    return dwg_result


async def build_service_legend(
    db: AsyncSession,
    revision_id: UUID,
    *,
    legend_layers: list[str] | None = None,
) -> ServiceLegend:
    """Build a :class:`ServiceLegend` from legend-layer swatches and prose abbreviation rows.

    Source A — colour swatches: small coloured linework entities on legend layer(s),
    each paired with the nearest text entity within ``_SWATCH_MATCH_RADIUS`` drawing
    units (radius-bounded nearest-neighbour, mirrors ``label_rooms._nearest_name``).

    Source B — prose abbreviation rows: ``DENOTES``/``=`` rows from the same legend
    layers (via ``load_legend_text_candidates``).

    No-swatch case: returns a prose-only or empty legend, never raises.
    """
    from sqlalchemy import or_, select

    from app.models.revision_materialization import RevisionEntity

    legend_query = select(RevisionEntity).where(
        RevisionEntity.drawing_revision_id == revision_id,
    )
    if legend_layers:
        legend_query = legend_query.where(RevisionEntity.layer_ref.in_(list(legend_layers)))
    else:
        legend_query = legend_query.where(
            or_(
                RevisionEntity.layer_ref.ilike("%LEGEND%"),
                RevisionEntity.layer_ref.ilike("%KEY%"),
            )
        )

    legend_rows = list((await db.execute(legend_query)).scalars().all())

    # Separate text candidates from potential swatch entities.
    text_entries: list[tuple[str, tuple[float, float]]] = []  # (text, point)
    swatch_candidates: list[tuple[tuple[float, float], Mapping[str, Any]]] = []  # (centroid, color)

    for row in legend_rows:
        geometry = row.geometry_json or {}
        if row.entity_type in ("text", "mtext"):
            text = geometry.get("text")
            if not isinstance(text, str) or not text.strip():
                continue
            pt = _entity_insertion(geometry)
            if pt is not None:
                text_entries.append((text.strip(), pt))
        elif row.entity_type not in _NON_SWATCH_ENTITY_TYPES:
            # Candidate swatch: must carry a resolved colour.
            style = row.style or {}
            color: Any = style.get("color") if isinstance(style, Mapping) else None
            if not isinstance(color, Mapping):
                continue
            # Skip by_layer / by_block unresolved colours.
            if color.get("by_layer") or color.get("by_block"):
                continue
            if color.get("rgb") is None and color.get("index") is None:
                continue
            centroid = _swatch_centroid(geometry)
            if centroid is not None:
                swatch_candidates.append((centroid, color))

    # Pair each text entry with its nearest swatch.
    swatch_inputs: list[SwatchInput] = []
    for text, text_pt in text_entries:
        color = _nearest_swatch(text_pt, swatch_candidates, _SWATCH_MATCH_RADIUS)
        if color is not None:
            swatch_inputs.append(SwatchInput(color=color, text=text))

    # Source B — prose abbreviation rows via shared loader.
    prose_texts = await load_legend_text_candidates(db, revision_id, legend_layers=legend_layers)
    prose_inputs = [ProseInput(text=t) for t in prose_texts]

    swatch_entries = from_colour_swatches(swatch_inputs)
    prose_entries = from_abbreviation_prose(prose_inputs)
    return fuse([*swatch_entries, *prose_entries])


async def load_tag_placements(
    db: AsyncSession,
    revision_id: UUID,
    *,
    tag_layers: list[str] | None = None,
    input_family: str | None = None,
) -> list[TagPlacement]:
    """Load pipe-tag text entities as :class:`TagPlacement` objects.

    For pdf_vector revisions (when ``tag_layers`` is None), tags are sourced from
    ``AdapterRunOutput.canonical_json["metadata"]["text_blocks"]`` (PDFs have no text
    entities; all prose lives in text_blocks). Each block's bbox centroid becomes the
    point; ``layer_ref`` is None. Raw text is passed through without pre-parsing
    (the coordinator calls parse_tag which self-filters non-tag prose).

    For DWG/DXF revisions (when ``tag_layers`` is None), selects text entities on layers
    containing default tag-layer tokens (ilike, ADR-003). ``point`` is the entity's
    insertion point; ``text`` is geometry_json['text'].

    Returns [] for empty/degenerate revisions; never raises.
    """
    if input_family is None:
        input_family = await _resolve_input_family(db, revision_id)

    if tag_layers is not None:
        # Explicit caller override — entity-based path regardless of input_family.
        from sqlalchemy import select

        from app.models.revision_materialization import RevisionEntity

        query = select(RevisionEntity).where(
            RevisionEntity.drawing_revision_id == revision_id,
            RevisionEntity.entity_type == "text",
            RevisionEntity.layer_ref.in_(list(tag_layers)),
        )
        rows = list((await db.execute(query)).scalars().all())
        placements: list[TagPlacement] = []
        for row in rows:
            geometry = row.geometry_json or {}
            text = geometry.get("text")
            if not isinstance(text, str) or not text.strip():
                continue
            pt = _entity_insertion(geometry)
            if pt is None:
                continue
            placements.append(TagPlacement(text=text, point=pt, layer_ref=row.layer_ref))
        return placements

    if input_family == INPUT_FAMILY_PDF_VECTOR:
        # PDF tags live in metadata.text_blocks, not as RevisionEntity text rows.
        # Only emit a TagPlacement when the block has real tag structure (digit + mm/diameter);
        # prose like '5291 LONDON' or 'Drawn: HK' is rejected by _looks_like_pdf_tag to prevent
        # parse_tag's bare digits+word fallback from fabricating services.
        blocks = await _load_text_blocks(db, revision_id)
        pdf_placements: list[TagPlacement] = []
        for block in blocks:
            text = block.get("text")
            if not isinstance(text, str) or not text.strip():
                continue
            if not _looks_like_pdf_tag(text):
                continue
            bbox = block.get("bbox")
            if not isinstance(bbox, dict):
                continue
            x_min = _number(bbox.get("x_min"))
            y_min = _number(bbox.get("y_min"))
            x_max = _number(bbox.get("x_max"))
            y_max = _number(bbox.get("y_max"))
            if x_min is None or y_min is None or x_max is None or y_max is None:
                continue
            point = ((x_min + x_max) / 2.0, (y_min + y_max) / 2.0)
            pdf_placements.append(TagPlacement(text=text, point=point, layer_ref=None))
        return pdf_placements

    # DWG/DXF (or unknown family): ilike-based entity text selection, ADR-003 compliant.
    from sqlalchemy import or_, select

    from app.models.revision_materialization import RevisionEntity

    query = select(RevisionEntity).where(
        RevisionEntity.drawing_revision_id == revision_id,
        RevisionEntity.entity_type == "text",
    )
    # _DEFAULT_TAG_LAYER_TOKENS is a non-empty constant, so or_ always receives >= 1 arg.
    conditions = [
        RevisionEntity.layer_ref.ilike(f"%{token}%") for token in _DEFAULT_TAG_LAYER_TOKENS
    ]
    query = query.where(or_(*conditions))

    rows = list((await db.execute(query)).scalars().all())

    dwg_placements: list[TagPlacement] = []
    for row in rows:
        geometry = row.geometry_json or {}
        text = geometry.get("text")
        if not isinstance(text, str) or not text.strip():
            continue
        pt = _entity_insertion(geometry)
        if pt is None:
            continue
        dwg_placements.append(
            TagPlacement(
                text=text,
                point=pt,
                layer_ref=row.layer_ref,
            )
        )
    return dwg_placements


async def load_rise_drop_entities(
    db: AsyncSession,
    revision_id: UUID,
    *,
    kind: str,
    layer_refs: list[str] | None = None,
    exclude_off_sheet: bool = True,
) -> list[RiseDropEntity]:
    """Load ARC+HATCH entities from rise/drop layers as :class:`RiseDropEntity`.

    When ``layer_refs`` is None, selects entities on layers matching the default
    token set for the given ``kind`` (ilike, ADR-003).  Returns [] for degenerate
    revisions; never raises.

    ``kind`` must be ``"rise"`` or ``"drop"`` -- selects the appropriate token set.
    """
    from app.interpretation.rise_drop import KIND_RISE

    if layer_refs is not None:
        rows = await load_revision_entities_by_type(
            db,
            revision_id,
            RISE_DROP_ENTITY_TYPES,
            layer_refs=layer_refs,
            exclude_off_sheet=exclude_off_sheet,
        )
    else:
        from sqlalchemy import or_, select

        from app.models.revision_materialization import RevisionEntity

        tokens = _DEFAULT_RISE_LAYER_TOKENS if kind == KIND_RISE else _DEFAULT_DROP_LAYER_TOKENS

        q = select(RevisionEntity).where(
            RevisionEntity.drawing_revision_id == revision_id,
            RevisionEntity.entity_type.in_(list(RISE_DROP_ENTITY_TYPES)),
        )
        if exclude_off_sheet:
            q = q.where(RevisionEntity.on_sheet.isnot(False))
        conditions = [RevisionEntity.layer_ref.ilike(f"%{token}%") for token in tokens]
        q = q.where(or_(*conditions))
        q = q.order_by(RevisionEntity.sequence_index, RevisionEntity.id)
        rows = list((await db.execute(q)).scalars().all())

    result: list[RiseDropEntity] = []
    for row in rows:
        style = row.style or {}
        color: Mapping[str, Any] | None = style.get("color") if isinstance(style, Mapping) else None
        raw_geom = row.geometry_json
        geometry: dict[str, Any] | None = raw_geom if isinstance(raw_geom, dict) else None
        result.append(
            RiseDropEntity(
                entity_id=str(row.id),
                entity_type=row.entity_type,
                layer_ref=row.layer_ref,
                color=color,
                geometry=geometry,
            )
        )
    return result


async def build_scale_context(
    db: AsyncSession,
    revision_id: UUID,
) -> ScaleContext:
    """Resolve the :class:`ScaleContext` from the revision's canonical units block.

    Delegates entirely to ``resolve_revision_scale`` so the ADR-004 trusted-wrong-hole
    rules are NEVER re-derived here. Maps ``RevisionScaleRead`` fields onto
    ``ScaleContext``. Returns an unknown/unavailable ``ScaleContext`` when the revision
    has no adapter run or does not exist (honest, never raises).
    """
    try:
        scale_read = await resolve_revision_scale(revision_id, db)
    except HTTPException:
        # Revision does not exist or is not visible; return honest unknown.
        return ScaleContext(
            conversion_factor=None,
            real_world_available=False,
            contradicted=False,
            units_confidence="unknown",
        )

    units = scale_read.units or {}
    conversion_factor: float | None = units.get("conversion_factor")
    if not isinstance(conversion_factor, (int, float)) or isinstance(conversion_factor, bool):
        conversion_factor = None

    units_confidence: str = scale_read.units_confidence

    # PDF fallback: when the units block gives no factor, derive metres-per-point from
    # pdf_scale.  ADR-004: PDF title-block detection is always "inferred", never confirmed.
    if conversion_factor is None:
        pdf_scale = scale_read.pdf_scale or {}
        points_to_real = pdf_scale.get("points_to_real")
        real_world_unit = pdf_scale.get("real_world_unit")
        if (
            isinstance(points_to_real, (int, float))
            and not isinstance(points_to_real, bool)
            and points_to_real > 0
            and isinstance(real_world_unit, str)
            and real_world_unit in _METRES_PER_PDF_UNIT
        ):
            conversion_factor = points_to_real * _METRES_PER_PDF_UNIT[real_world_unit]
            units_confidence = "inferred"

    return ScaleContext(
        conversion_factor=conversion_factor,
        real_world_available=scale_read.real_world_dimensions_available,
        contradicted=scale_read.units_contradicted,
        units_confidence=units_confidence,
    )


async def load_service_takeoff_inputs(
    db: AsyncSession,
    revision_id: UUID,
    *,
    layer_refs: list[str] | None = None,
    tag_layers: list[str] | None = None,
    legend_layers: list[str] | None = None,
    rise_layers: list[str] | None = None,
    drop_layers: list[str] | None = None,
    exclude_off_sheet: bool = True,
) -> ServiceTakeoffInputs:
    """Load all inputs the takeoff coordinator needs as a single atomic bundle.

    Composes :func:`load_routed_entities`, :func:`build_service_legend`,
    :func:`load_tag_placements`, :func:`load_rise_drop_entities`, and
    :func:`build_scale_context`. All sub-loaders are tolerant: an empty / degenerate
    revision yields empty lists and unknown scale without raising.

    ``rise_layers`` / ``drop_layers`` override the default ilike-token layer selection
    for rise/drop entities (mirrors the ``layer_refs`` / ``tag_layers`` pattern).

    Resolves ``input_family`` once and passes it to both ``load_routed_entities`` and
    ``load_tag_placements`` so PDF-vector revisions receive the correct code paths. When the
    family resolves to a concrete value (any revision with an adapter run) the loaders use the
    threaded value directly; only a no-adapter-run revision (which resolves to None and returns
    no geometry anyway) re-resolves to None inside each loader.
    """
    from app.interpretation.rise_drop import KIND_DROP, KIND_RISE

    # Resolve input_family once and thread it to both loaders (see docstring on the None edge).
    input_family = await _resolve_input_family(db, revision_id)

    routed_entities = await load_routed_entities(
        db,
        revision_id,
        layer_refs=layer_refs,
        exclude_off_sheet=exclude_off_sheet,
        input_family=input_family,
    )
    legend = await build_service_legend(db, revision_id, legend_layers=legend_layers)
    tag_placements = await load_tag_placements(
        db, revision_id, tag_layers=tag_layers, input_family=input_family
    )
    scale = await build_scale_context(db, revision_id)
    rise_entities = await load_rise_drop_entities(
        db, revision_id, kind=KIND_RISE, layer_refs=rise_layers, exclude_off_sheet=exclude_off_sheet
    )
    drop_entities = await load_rise_drop_entities(
        db, revision_id, kind=KIND_DROP, layer_refs=drop_layers, exclude_off_sheet=exclude_off_sheet
    )

    geometry_by_entity_id: dict[str, dict[str, Any]] = {}
    for routed_ent in routed_entities:
        if routed_ent.geometry is not None:
            geometry_by_entity_id[routed_ent.entity_id] = dict(routed_ent.geometry)
    for rise_ent in rise_entities:
        if rise_ent.geometry is not None:
            geometry_by_entity_id[rise_ent.entity_id] = dict(rise_ent.geometry)
    for drop_ent in drop_entities:
        if drop_ent.geometry is not None:
            geometry_by_entity_id[drop_ent.entity_id] = dict(drop_ent.geometry)

    return ServiceTakeoffInputs(
        routed_entities=routed_entities,
        legend=legend,
        tag_placements=tag_placements,
        geometry_by_entity_id=geometry_by_entity_id,
        scale=scale,
        rise_entities=rise_entities,
        drop_entities=drop_entities,
        input_family=input_family,
    )
