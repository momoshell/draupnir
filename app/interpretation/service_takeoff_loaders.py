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
# Layer defaults — ilike patterns, never hardcoded to a single value (ADR-003)
# ---------------------------------------------------------------------------

# Layers bearing routed linework: anything matching these tokens (case-insensitive).
_DEFAULT_PIPE_LAYER_TOKENS: tuple[str, ...] = ("pipe",)

# Layers bearing pipe-tag text annotations.
_DEFAULT_TAG_LAYER_TOKENS: tuple[str, ...] = ("pipe tag", "pipetag", "tag")

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
# Public loaders
# ---------------------------------------------------------------------------


async def load_routed_entities(
    db: AsyncSession,
    revision_id: UUID,
    *,
    layer_refs: list[str] | None = None,
    exclude_off_sheet: bool = True,
) -> list[RoutedEntity]:
    """Load line/polyline/arc entities from routed-run layers as :class:`RoutedEntity`.

    When ``layer_refs`` is None, loads from all layers matching the default pipe-layer
    token patterns (ilike). Returns an empty list for degenerate revisions; never raises.
    """
    if layer_refs is not None:
        rows = await load_revision_entities_by_type(
            db,
            revision_id,
            ROUTED_ENTITY_TYPES,
            layer_refs=layer_refs,
            exclude_off_sheet=exclude_off_sheet,
        )
    else:
        # ilike-based layer selection — sheet-agnostic, ADR-003 compliant.
        from sqlalchemy import or_, select

        from app.models.revision_materialization import RevisionEntity

        query = select(RevisionEntity).where(
            RevisionEntity.drawing_revision_id == revision_id,
            RevisionEntity.entity_type.in_(list(ROUTED_ENTITY_TYPES)),
        )
        if exclude_off_sheet:
            query = query.where(RevisionEntity.on_sheet.isnot(False))
        layer_conditions = [
            RevisionEntity.layer_ref.ilike(f"%{token}%") for token in _DEFAULT_PIPE_LAYER_TOKENS
        ]
        if len(layer_conditions) == 1:
            query = query.where(layer_conditions[0])
        else:
            query = query.where(or_(*layer_conditions))
        query = query.order_by(RevisionEntity.sequence_index, RevisionEntity.id)
        rows = list((await db.execute(query)).scalars().all())

    result: list[RoutedEntity] = []
    for row in rows:
        style = row.style or {}
        color: Mapping[str, Any] | None = style.get("color") if isinstance(style, Mapping) else None
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
) -> list[TagPlacement]:
    """Load pipe-tag text entities as :class:`TagPlacement` objects.

    When ``tag_layers`` is None, selects text entities on layers containing default
    tag-layer tokens (ilike, ADR-003). ``point`` is the entity's insertion point;
    ``text`` is geometry_json['text']. Raw text is passed through without pre-parsing
    (the coordinator calls parse_tag). Returns [] for empty/degenerate revisions.
    """
    from sqlalchemy import or_, select

    from app.models.revision_materialization import RevisionEntity

    query = select(RevisionEntity).where(
        RevisionEntity.drawing_revision_id == revision_id,
        RevisionEntity.entity_type == "text",
    )
    if tag_layers:
        query = query.where(RevisionEntity.layer_ref.in_(list(tag_layers)))
    else:
        conditions = [
            RevisionEntity.layer_ref.ilike(f"%{token}%") for token in _DEFAULT_TAG_LAYER_TOKENS
        ]
        if len(conditions) == 1:
            query = query.where(conditions[0])
        else:
            query = query.where(or_(*conditions))

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
        placements.append(
            TagPlacement(
                text=text,
                point=pt,
                layer_ref=row.layer_ref,
            )
        )
    return placements


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

    return ScaleContext(
        conversion_factor=conversion_factor,
        real_world_available=scale_read.real_world_dimensions_available,
        contradicted=scale_read.units_contradicted,
        units_confidence=scale_read.units_confidence,
    )


async def load_service_takeoff_inputs(
    db: AsyncSession,
    revision_id: UUID,
    *,
    layer_refs: list[str] | None = None,
    tag_layers: list[str] | None = None,
    legend_layers: list[str] | None = None,
    exclude_off_sheet: bool = True,
) -> ServiceTakeoffInputs:
    """Load all inputs the takeoff coordinator needs as a single atomic bundle.

    Composes :func:`load_routed_entities`, :func:`build_service_legend`,
    :func:`load_tag_placements`, and :func:`build_scale_context`. All sub-loaders
    are tolerant: an empty / degenerate revision yields empty lists and unknown scale
    without raising.
    """
    routed_entities = await load_routed_entities(
        db, revision_id, layer_refs=layer_refs, exclude_off_sheet=exclude_off_sheet
    )
    legend = await build_service_legend(db, revision_id, legend_layers=legend_layers)
    tag_placements = await load_tag_placements(db, revision_id, tag_layers=tag_layers)
    scale = await build_scale_context(db, revision_id)

    geometry_by_entity_id: dict[str, dict[str, Any]] = {}
    for entity in routed_entities:
        if entity.geometry is not None:
            geometry_by_entity_id[entity.entity_id] = dict(entity.geometry)

    return ServiceTakeoffInputs(
        routed_entities=routed_entities,
        legend=legend,
        tag_placements=tag_placements,
        geometry_by_entity_id=geometry_by_entity_id,
        scale=scale,
    )
