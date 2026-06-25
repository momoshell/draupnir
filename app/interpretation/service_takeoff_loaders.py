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
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.revision_routes.scale import resolve_revision_scale
from app.ingestion.centerline_contract import _xy
from app.interpretation.loaders import (
    load_legend_text_candidates,
    load_revision_entities_by_type,
)
from app.interpretation.measurement import ScaleContext
from app.interpretation.rise_drop import RiseDropEntity
from app.interpretation.routed_runs import ROUTED_ENTITY_TYPES, RoutedEntity
from app.interpretation.run_service_identity import TagPlacement
from app.interpretation.service_fill_takeoff import FillBand
from app.interpretation.service_legend import (
    ProseInput,
    ServiceLegend,
    SwatchInput,
    from_abbreviation_prose,
    from_colour_swatches,
    fuse,
)
from app.interpretation.service_legend import (
    _normalize_text as _normalize_legend_text,
)
from app.interpretation.service_legend import (
    colour_key as _colour_key,
)
from app.interpretation.tag_stack_service import BundleColourBand, StackHeader, TagStackText

# ---------------------------------------------------------------------------
# Input-family constant (ADR-003: branch on family, never on hardcoded colours/layers)
# ---------------------------------------------------------------------------

INPUT_FAMILY_PDF_VECTOR = "pdf_vector"

# ---------------------------------------------------------------------------
# PDF legend-reader constants (ADR-003: only generic anchor text; NO colours/layers)
# ---------------------------------------------------------------------------

_ANCHOR_MAX_CHARS: int = 30
_REGION_PAD_LEFT: float = 15.0
_REGION_PAD_TOP: float = 5.0
_REGION_WIDTH: float = 320.0
_REGION_HEIGHT: float = 420.0
_SWATCH_LINE_LEN_MIN: float = 12.0
_SWATCH_LINE_LEN_MAX: float = 70.0
_SWATCH_RECT_W_MIN: float = 8.0
_SWATCH_RECT_W_MAX: float = 60.0
_SWATCH_RECT_H_MIN: float = 3.0
_SWATCH_RECT_H_MAX: float = 22.0
_ROW_Y_TOL: float = 6.0
_PAIR_X_MAX_GAP: float = 320.0
_LEGEND_ANCHOR_RE: re.Pattern[str] = re.compile(r"(?i)\b(?:legend|key)\b")
_LEGEND_ANCHOR_EXCLUDE_RE: re.Pattern[str] = re.compile(r"(?i)\b(?:key\s*plan|notes?)\b")

# Stack-header pattern: "FROM TOP TO BOTTOM", "TOP TO BOTTOM TA:", etc.
# FROM is optional — drawings sometimes omit it (ADR-003: any layer).
_STACK_HEADER_RE: re.Pattern[str] = re.compile(
    r"(?:FROM\s+)?(TOP\s+TO\s+BOTTOM|BOTTOM\s+TO\s+TOP|LEFT\s+TO\s+RIGHT|RIGHT\s+TO\s+LEFT)",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# PDF colour helpers (mirrors pymupdf._rgb_hex; do NOT import from there)
# ---------------------------------------------------------------------------


def _rgb_tuple_to_hex(rgb: tuple[float, ...]) -> str:
    """Convert a 0..1 float RGB tuple to a 6-char lowercase hex string.

    mirrors pymupdf._rgb_hex
    """
    return "".join(f"{max(0, min(255, round(c * 255))):02x}" for c in rgb[:3])


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

# Layers bearing routed linework for all routed-container disciplines (case-insensitive ilike).
# Each token covers a distinct containment type so that sheets without an authored Center Line
# layer (e.g. cable-tray, conduit, duct sheets) still yield routed entities via the fallback.
# Entities loaded via this fallback route to the DERIVED (rail-pair) centerline producer (#681),
# whereas authored Center Line layers use the preferred direct query above.
# ADR-003: generic discipline vocabulary — never a firm-specific layer name (e.g. E610G_Cable Tray).
#
# Known edge case (follow-on): a sheet with BOTH an authored Center Line layer AND separate
# containment layers (e.g. tray rails) will return only the Center Line entities — the
# containment layers are silently ignored. Resolving this requires multi-family merging.
_DEFAULT_CONTAINER_LAYER_TOKENS: tuple[str, ...] = (
    "pipe",  # piped services (M-540003 etc.) — preserved for regression
    "tray",  # cable tray (e.g. "Cable Tray", "E610G_Cable Tray")
    "ladder",  # cable ladder
    "trunking",  # trunking / wireway
    "basket",  # wire basket / cable basket
    "conduit",  # conduit runs
    "duct",  # ductwork / air duct. NOTE: %duct% is the one token with English-substring
    # collisions ("product", "conduct", "viaduct") — could match a stray annotation/schedule
    # layer. Low risk in practice (service layers dominate), but if a false-positive surfaces,
    # tighten to a word-boundary / token-aware match rather than widening the ilike.
)

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

# Combined token set for fill-band (HATCH) queries: all three service-layer families.
# Referenced as the default in load_service_fill_bands so the set cannot silently drift.
_SERVICE_FILL_LAYER_TOKENS: tuple[str, ...] = ("pipe", "rise", "drop")

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
# DWG legend anchor-region constants (ADR-003: generic anchor text; NO hardcoded layers)
# ---------------------------------------------------------------------------

# Entity types excluded from DWG region-based swatch collection.
# Intentionally does NOT exclude "hatch" — M-540003 swatches are HATCH entities.
# The shared _NON_SWATCH_ENTITY_TYPES constant retains "hatch" for the layer-name path.
_DWG_NON_SWATCH_ENTITY_TYPES: frozenset[str] = frozenset({"text", "mtext", "insert", "dimension"})

# Region window built around the anchor text insertion point (drawing units).
# Calibrated to M-540003: anchor at (-4.56, 3.31); swatch column at x≈-4.3,
# labels at x≈-3.8, rows at y∈[1.7,2.9]. Region must exclude the keynotes
# starting at x≥-0.66 and title-block text at x≥0.38.
# LEFT: anchor is near the leftmost item; 1 unit left covers hatch offset.
# RIGHT: labels end ~0.75 units right of anchor; 3.5 units keeps us well left of x≈-0.66.
# ABOVE: anchor text sits at anchor y; 0.5 units captures any small overshoot.
# BELOW: rows span ~1.6 units below anchor; 2.0 units covers with margin.
_DWG_REGION_PAD_LEFT: float = 1.0  # units left of anchor x
_DWG_REGION_PAD_RIGHT: float = 3.5  # units right of anchor x; keynotes start ~3.9u right
_DWG_REGION_PAD_ABOVE: float = 0.5  # units above anchor y
_DWG_REGION_PAD_BELOW: float = 2.0  # units below anchor y; rows span ≈1.6u below

# Radius (drawing units) for pairing a label with the nearest in-region swatch.
# M-540003 swatch↔label gap is ~0.5 units; rows are ~0.4 units apart.
# 1.5 units keeps each label bound to its own row's swatch.
_DWG_LEGEND_PAIR_RADIUS: float = 1.5

# Minimum RGB channel spread (max-min across R,G,B in 0-255 space) for a DWG
# swatch colour to be accepted as a service colour.  Achromatic colours (black,
# white, grey, default pen) are not service identifiers.
# M-540003 real swatches: 7ebc32→138, 00ff00→255, 9783dc→89 — all pass.
# Black border lines (000000) → spread 0 — rejected.
_DWG_SWATCH_MIN_CHROMA: int = 30


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
        s = _xy(geometry["start"])
        e = _xy(geometry["end"])
        if s is not None and e is not None:
            return ((s[0] + e[0]) / 2.0, (s[1] + e[1]) / 2.0)

    pts: Any = geometry.get("vertices") or geometry.get("points")
    if isinstance(pts, (list, tuple)) and pts:
        coords = [c for p in pts if (c := _xy(p)) is not None]
        if coords:
            xs = [c[0] for c in coords]
            ys = [c[1] for c in coords]
            return (sum(xs) / len(xs), sum(ys) / len(ys))

    center = geometry.get("center")
    if isinstance(center, Mapping):
        pt = _point_xy(center)
        if pt is not None:
            return pt

    return _entity_insertion(geometry)


def _is_chromatic_colour(
    color: Mapping[str, Any], min_spread: int = _DWG_SWATCH_MIN_CHROMA
) -> bool:
    """Return True when *color* is a service-identifiable (chromatic) colour.

    Achromatic colours (black 000000, white ffffff, grey) have RGB spread
    (max_channel - min_channel) below ``min_spread`` and are rejected —
    they indicate table borders, default pen, or background fill rather than
    a service identity.

    The rgb field may carry a leading alpha byte (e.g. "c200ff00" from the DXF
    transparency encoding); strip it to a 6-char hex string before parsing.

    Falls back to ``True`` (accept) when rgb is absent and only an index is
    present, so index-only colours are not silently discarded.
    """
    rgb_raw = color.get("rgb")
    if isinstance(rgb_raw, str) and rgb_raw:
        rgb_str = rgb_raw.lstrip("#")
        # Strip leading alpha byte if 8-char hex (e.g. "c200ff00" → "00ff00").
        if len(rgb_str) == 8:
            rgb_str = rgb_str[2:]
        if len(rgb_str) == 6:
            try:
                r = int(rgb_str[0:2], 16)
                g = int(rgb_str[2:4], 16)
                b = int(rgb_str[4:6], 16)
                return max(r, g, b) - min(r, g, b) >= min_spread
            except ValueError:
                pass
    # No parseable rgb — accept if index present, reject if neither.
    return color.get("index") is not None


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
# PDF tag-candidate extractor
# ---------------------------------------------------------------------------

# Matches any size expression that can START a pipe tag:
#   - optional diameter glyph + digits + mm  (e.g. "Ø54 mm", "54 mm")
#   - WxH notation                           (e.g. "200 x 100", "650x350")
# Plain ASCII source (RUF002): use Ø escape for the Ø glyph.
# Tag-head: an optional diameter glyph -- clean U+00D8 OR the U+FFFD replacement that
# non-UTF-8 sources emit (mirrors run_tags) -- then a round 'NN mm' or a WxH 'NNxNN' size.
# Matching the garbled glyph too is load-bearing: it is the segment boundary for concatenated
# blocks like '<glyph>76 mm VAC<glyph>42 mm MA', so each tag splits out even when Ø is corrupt.
_PDF_TAG_HEAD_RE: re.Pattern[str] = re.compile(
    "(?:[Ø�]\\s*)?\\d{1,4}\\s*mm\\b|\\d{2,4}\\s*[xX]\\s*\\d{2,4}",
    re.IGNORECASE,
)

# Maximum characters a valid tag candidate may occupy.  Short segments from
# densely-packed med-gas blocks (e.g. "Ø76 mm VAC") pass; an isolated size in
# a long note sentence produces one head whose tail is far longer than this cap
# and is therefore rejected without needing case/stopword heuristics.
_MAX_TAG_CANDIDATE_CHARS: int = 30


def _extract_pdf_tag_candidates(text: str) -> list[str]:
    """Segment a PDF text block into individual tag candidates.

    Uses size-expression head positions to split concatenated tags
    (e.g. med-gas stacks like "Ø76 mm VACØ42 mm MAØ42 mm AGSS") into
    one candidate per head, then rejects any candidate longer than
    ``_MAX_TAG_CANDIDATE_CHARS``.  Blocks with no size head (pure prose) return [].

    All candidates from one block share the block's bbox centroid; finer
    per-tag positioning is a future refinement.
    """
    heads = list(_PDF_TAG_HEAD_RE.finditer(text))
    if not heads:
        return []

    candidates: list[str] = []
    for i, head in enumerate(heads):
        start = head.start()
        end = heads[i + 1].start() if i + 1 < len(heads) else len(text)
        segment = text[start:end].strip()
        if len(segment) <= _MAX_TAG_CANDIDATE_CHARS:
            candidates.append(segment)
    return candidates


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

    # Prefer centerline layers (single-line route) over pipe/container-wall layers. Pipe-outline
    # layers may double-count measured length when both walls of a double-line pipe are
    # present; true double-line detection is a deferred follow-up.
    # Container-discipline layers (tray, duct, conduit …) reach here only when no authored
    # Center Line layer exists; their entities are handed to _derive_centerline (#681).
    dwg_rows = await _query_by_tokens(_DEFAULT_CENTERLINE_LAYER_TOKENS)
    if not dwg_rows:
        dwg_rows = await _query_by_tokens(_DEFAULT_CONTAINER_LAYER_TOKENS)

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


async def _build_pdf_service_legend(db: AsyncSession, revision_id: UUID) -> ServiceLegend:
    """Build a ServiceLegend for a pdf_vector revision from legend-region swatches.

    Discovers legend regions by finding text_blocks matching the legend anchor pattern,
    then queries RevisionEntity for line/polyline swatches inside the region bounding box.
    Colour is read from style.color.rgb (stroke) with fallback to properties_json fill_color_rgb.
    Swatch is paired with the nearest right-of-swatch text block on the same row.

    Returns an empty ServiceLegend on any error (tolerant).
    """
    from sqlalchemy import select

    from app.models.revision_materialization import RevisionEntity

    try:
        text_blocks = await _load_text_blocks(db, revision_id)

        # --- Step 1: find anchor blocks ---
        anchors: list[dict[str, Any]] = []
        for block in text_blocks:
            raw_text = block.get("text")
            if not isinstance(raw_text, str):
                continue
            collapsed = " ".join(raw_text.strip().split())
            if len(collapsed) > _ANCHOR_MAX_CHARS:
                continue
            if not _LEGEND_ANCHOR_RE.search(collapsed):
                continue
            if _LEGEND_ANCHOR_EXCLUDE_RE.search(collapsed):
                continue
            bbox = block.get("bbox")
            if not isinstance(bbox, dict):
                continue
            anchors.append(block)

        if not anchors:
            return ServiceLegend(entries=())

        # --- Step 2: build union of region windows, query swatches once ---
        # Collect all region tuples (x_lo, y_lo, x_hi, y_hi).
        regions: list[tuple[float, float, float, float]] = []
        for anchor in anchors:
            bbox = anchor["bbox"]
            ax_min = _number(bbox.get("x_min"))
            ay_min = _number(bbox.get("y_min"))
            if ax_min is None or ay_min is None:
                continue
            x_lo = ax_min - _REGION_PAD_LEFT
            x_hi = ax_min + _REGION_WIDTH
            y_lo = ay_min - _REGION_PAD_TOP
            y_hi = ay_min + _REGION_HEIGHT
            regions.append((x_lo, y_lo, x_hi, y_hi))

        if not regions:
            return ServiceLegend(entries=())

        # Query all line/polyline entities for this revision (no bbox filter in SQL — filter
        # in Python so a single round-trip covers multiple disjoint anchor regions).
        swatch_rows = list(
            (
                await db.execute(
                    select(RevisionEntity).where(
                        RevisionEntity.drawing_revision_id == revision_id,
                        RevisionEntity.entity_type.in_(["line", "polyline"]),
                        RevisionEntity.bbox_min_x.is_not(None),
                    )
                )
            )
            .scalars()
            .all()
        )

        # --- Step 3: filter swatches by region + size gate ---
        # Each candidate is (color_mapping, center_x, center_y).
        candidates: list[tuple[Mapping[str, Any], float, float]] = []

        for row in swatch_rows:
            bx_min = row.bbox_min_x
            by_min = row.bbox_min_y
            bx_max = row.bbox_max_x
            by_max = row.bbox_max_y
            if bx_min is None or by_min is None or bx_max is None or by_max is None:
                continue

            w = abs(bx_max - bx_min)
            h = abs(by_max - by_min)

            # Size gate: line or rect_like polyline.
            props = row.properties_json or {}
            rect_like_flag: bool = (
                bool(props.get("rect_like")) if isinstance(props, Mapping) else False
            )

            if row.entity_type == "line":
                length = math.hypot(w, h)
                if not (_SWATCH_LINE_LEN_MIN <= length <= _SWATCH_LINE_LEN_MAX):
                    continue
            elif row.entity_type == "polyline":
                if not rect_like_flag:
                    continue
                if not (
                    _SWATCH_RECT_W_MIN <= w <= _SWATCH_RECT_W_MAX
                    and _SWATCH_RECT_H_MIN <= h <= _SWATCH_RECT_H_MAX
                ):
                    continue
            else:
                continue

            # Colour precedence: resolved stroke rgb first; else fill tuple; else skip.
            style = row.style or {}
            color_block: Any = style.get("color") if isinstance(style, Mapping) else None
            resolved_color: Mapping[str, Any] | None = None

            if (
                isinstance(color_block, Mapping)
                and not color_block.get("by_layer")
                and not color_block.get("by_block")
            ):
                rgb_val = color_block.get("rgb")
                if isinstance(rgb_val, str) and rgb_val:
                    resolved_color = {
                        "rgb": rgb_val.lower(),
                        "index": None,
                        "by_layer": False,
                        "by_block": False,
                    }

            if resolved_color is None:
                # Fall back to fill_color_rgb from properties_json.
                fill_raw = props.get("fill_color_rgb") if isinstance(props, Mapping) else None
                if isinstance(fill_raw, (list, tuple)) and len(fill_raw) >= 3:
                    try:
                        fill_hex = _rgb_tuple_to_hex(tuple(float(c) for c in fill_raw))
                        resolved_color = {
                            "rgb": fill_hex,
                            "index": None,
                            "by_layer": False,
                            "by_block": False,
                        }
                    except (TypeError, ValueError):
                        pass

            if resolved_color is None:
                continue

            cx = (bx_min + bx_max) / 2.0
            cy = (by_min + by_max) / 2.0

            # Match against regions.
            for x_lo, y_lo, x_hi, y_hi in regions:
                if x_lo <= bx_min and bx_max <= x_hi and y_lo <= by_min and by_max <= y_hi:
                    candidates.append((resolved_color, cx, cy))
                    break  # first matching region wins

        if not candidates:
            return ServiceLegend(entries=())

        # --- Step 4: pair each swatch with nearest right-of-swatch text on same row ---
        swatch_inputs: list[SwatchInput] = []

        for cand_color, cand_cx, cand_cy in candidates:
            best_text: str | None = None
            best_gap: float = _PAIR_X_MAX_GAP + 1.0

            for block in text_blocks:
                t_bbox = block.get("bbox")
                if not isinstance(t_bbox, dict):
                    continue
                t_x_min = _number(t_bbox.get("x_min"))
                t_y_min = _number(t_bbox.get("y_min"))
                t_x_max = _number(t_bbox.get("x_max"))
                t_y_max = _number(t_bbox.get("y_max"))
                if t_x_min is None or t_y_min is None or t_x_max is None or t_y_max is None:
                    continue
                t_center_y = (t_y_min + t_y_max) / 2.0
                if abs(t_center_y - cand_cy) > _ROW_Y_TOL:
                    continue
                if t_x_min < cand_cx:
                    continue
                gap = t_x_min - cand_cx
                if gap <= _PAIR_X_MAX_GAP and gap < best_gap:
                    raw_text = block.get("text")
                    if isinstance(raw_text, str) and raw_text.strip():
                        best_gap = gap
                        best_text = raw_text.strip()

            if best_text is not None:
                swatch_inputs.append(SwatchInput(color=cand_color, text=best_text))

        return fuse(from_colour_swatches(swatch_inputs))

    except Exception:  # tolerant: never raise
        return ServiceLegend(entries=())


async def _build_dwg_legend_from_anchor(
    db: AsyncSession,
    revision_id: UUID,
) -> tuple[list[SwatchInput], bool]:
    """Discover DWG legend swatches via text anchor + spatial region.

    Returns (swatch_inputs, anchor_found).  ``anchor_found=True`` means at least one
    anchor was detected; the caller should skip the layer-name fallback in this case.
    Never raises — returns ([], False) on any error.

    Algorithm:
    1. Query ALL text/mtext entities (any layer) for the revision.
    2. Normalise each text with ``_normalize_legend_text`` and match against
       ``_LEGEND_ANCHOR_RE`` / ``_LEGEND_ANCHOR_EXCLUDE_RE`` + length cap.
    3. Build an axis-aligned bounding box around each anchor's insertion point.
    4. Query ALL entities (any type) for the revision; collect swatch candidates
       (not in ``_DWG_NON_SWATCH_ENTITY_TYPES``, resolved colour, centroid in region)
       and label candidates (text/mtext insertion in region, raw text non-empty).
    5. Pair each label with its nearest swatch via ``_nearest_swatch``.
    """
    try:
        from sqlalchemy import select

        from app.models.revision_materialization import RevisionEntity

        # --- Step 1: load all text entities (any layer) ---
        text_rows = list(
            (
                await db.execute(
                    select(RevisionEntity)
                    .where(
                        RevisionEntity.drawing_revision_id == revision_id,
                        RevisionEntity.entity_type.in_(["text", "mtext"]),
                    )
                    .order_by(RevisionEntity.sequence_index, RevisionEntity.id)
                )
            )
            .scalars()
            .all()
        )

        # --- Step 2: identify anchor entities ---
        regions: list[tuple[float, float, float, float]] = []  # (x_lo, y_lo, x_hi, y_hi)

        for row in text_rows:
            geometry = row.geometry_json or {}
            raw_text = geometry.get("text")
            if not isinstance(raw_text, str):
                continue
            collapsed = _normalize_legend_text(raw_text)
            if len(collapsed) > _ANCHOR_MAX_CHARS:
                continue
            if not _LEGEND_ANCHOR_RE.search(collapsed):
                continue
            if _LEGEND_ANCHOR_EXCLUDE_RE.search(collapsed):
                continue
            pt = _entity_insertion(geometry)
            if pt is None:
                continue
            ax, ay = pt
            regions.append(
                (
                    ax - _DWG_REGION_PAD_LEFT,
                    ay - _DWG_REGION_PAD_BELOW,
                    ax + _DWG_REGION_PAD_RIGHT,
                    ay + _DWG_REGION_PAD_ABOVE,
                )
            )

        if not regions:
            return [], False

        # --- Step 3: load all entities for this revision (one round-trip) ---
        all_rows = list(
            (
                await db.execute(
                    select(RevisionEntity)
                    .where(RevisionEntity.drawing_revision_id == revision_id)
                    .order_by(RevisionEntity.sequence_index, RevisionEntity.id)
                )
            )
            .scalars()
            .all()
        )

        def _in_any_region(x: float, y: float) -> bool:
            for x_lo, y_lo, x_hi, y_hi in regions:
                if x_lo <= x <= x_hi and y_lo <= y <= y_hi:
                    return True
            return False

        # --- Step 4: collect swatch candidates and label text entries ---
        swatch_candidates: list[tuple[tuple[float, float], Mapping[str, Any]]] = []
        text_entries: list[tuple[str, tuple[float, float]]] = []

        for row in all_rows:
            row_geom: dict[str, Any] = row.geometry_json or {}

            if row.entity_type in ("text", "mtext"):
                raw_text = row_geom.get("text")
                if not isinstance(raw_text, str) or not raw_text.strip():
                    continue
                pt = _entity_insertion(row_geom)
                if pt is not None and _in_any_region(pt[0], pt[1]):
                    text_entries.append((raw_text.strip(), pt))

            elif row.entity_type not in _DWG_NON_SWATCH_ENTITY_TYPES:
                # Swatch candidate: must carry a resolved colour (not by_layer/by_block,
                # rgb or index non-None) AND have a usable centroid that falls in region.
                style = row.style or {}
                color_val: Any = style.get("color") if isinstance(style, Mapping) else None
                if not isinstance(color_val, Mapping):
                    continue
                if color_val.get("by_layer") or color_val.get("by_block"):
                    continue
                if color_val.get("rgb") is None and color_val.get("index") is None:
                    continue
                # Reject achromatic colours (black border lines, default pen, grey).
                # Service swatches are chromatic; greyscale is structural linework.
                if not _is_chromatic_colour(color_val):
                    continue
                centroid = _swatch_centroid(row_geom)
                if centroid is None:
                    continue
                if _in_any_region(centroid[0], centroid[1]):
                    swatch_candidates.append((centroid, color_val))

        # --- Step 5: pair each label with nearest in-region swatch ---
        swatch_inputs: list[SwatchInput] = []
        for text, text_pt in text_entries:
            color = _nearest_swatch(text_pt, swatch_candidates, _DWG_LEGEND_PAIR_RADIUS)
            if color is not None:
                swatch_inputs.append(SwatchInput(color=color, text=text))

        return swatch_inputs, True

    except Exception:  # tolerant: never raise
        return [], False


async def build_service_legend(
    db: AsyncSession,
    revision_id: UUID,
    *,
    legend_layers: list[str] | None = None,
    input_family: str | None = None,
) -> ServiceLegend:
    """Build a :class:`ServiceLegend` from legend-layer swatches and prose abbreviation rows.

    For pdf_vector revisions (when ``input_family`` is ``INPUT_FAMILY_PDF_VECTOR``),
    delegates to ``_build_pdf_service_legend`` which reads from text_blocks anchors and
    RevisionEntity bbox-filtered swatches (no legend layers, no hardcoded colours).

    For DWG/DXF revisions the swatch-discovery precedence is:
    1. Explicit ``legend_layers`` override → layer-IN query (existing passing tests preserved).
    2. Anchor region (no explicit layers): find text matching _LEGEND_ANCHOR_RE on any layer;
       build a spatial region around each anchor; collect HATCH+line+polyline swatches and
       label text that fall inside the region; pair label→nearest swatch.
    3. Layer-name fallback (no explicit layers AND no anchor found): ILIKE %LEGEND%/%KEY%.

    Source B — prose abbreviation rows: ``DENOTES``/``=`` rows via ``load_legend_text_candidates``
    (still uses legend-layer filtering; out of scope for anchor-region discovery).

    No-swatch case: returns a prose-only or empty legend, never raises.
    """
    if input_family is None:
        input_family = await _resolve_input_family(db, revision_id)

    if input_family == INPUT_FAMILY_PDF_VECTOR:
        return await _build_pdf_service_legend(db, revision_id)

    from sqlalchemy import or_, select

    from app.models.revision_materialization import RevisionEntity

    swatch_inputs: list[SwatchInput] = []

    if legend_layers:
        # --- Path 1: explicit legend_layers override (highest precedence) ---
        legend_query = (
            select(RevisionEntity)
            .where(
                RevisionEntity.drawing_revision_id == revision_id,
                RevisionEntity.layer_ref.in_(list(legend_layers)),
            )
            .order_by(RevisionEntity.sequence_index, RevisionEntity.id)
        )
        legend_rows = list((await db.execute(legend_query)).scalars().all())

        text_entries_layer: list[tuple[str, tuple[float, float]]] = []
        swatch_candidates_layer: list[tuple[tuple[float, float], Mapping[str, Any]]] = []

        for row in legend_rows:
            geometry = row.geometry_json or {}
            if row.entity_type in ("text", "mtext"):
                text = geometry.get("text")
                if not isinstance(text, str) or not text.strip():
                    continue
                pt = _entity_insertion(geometry)
                if pt is not None:
                    text_entries_layer.append((text.strip(), pt))
            elif row.entity_type not in _NON_SWATCH_ENTITY_TYPES:
                style = row.style or {}
                color: Any = style.get("color") if isinstance(style, Mapping) else None
                if not isinstance(color, Mapping):
                    continue
                if color.get("by_layer") or color.get("by_block"):
                    continue
                if color.get("rgb") is None and color.get("index") is None:
                    continue
                centroid = _swatch_centroid(geometry)
                if centroid is not None:
                    swatch_candidates_layer.append((centroid, color))

        for text, text_pt in text_entries_layer:
            clr = _nearest_swatch(text_pt, swatch_candidates_layer, _SWATCH_MATCH_RADIUS)
            if clr is not None:
                swatch_inputs.append(SwatchInput(color=clr, text=text))

    else:
        # --- Path 2: anchor-region discovery (no explicit layers) ---
        anchor_swatch_inputs, anchor_found = await _build_dwg_legend_from_anchor(db, revision_id)

        if anchor_found:
            swatch_inputs = anchor_swatch_inputs
        else:
            # --- Path 3: layer-name fallback ---
            legend_query_fallback = (
                select(RevisionEntity)
                .where(
                    RevisionEntity.drawing_revision_id == revision_id,
                    or_(
                        RevisionEntity.layer_ref.ilike("%LEGEND%"),
                        RevisionEntity.layer_ref.ilike("%KEY%"),
                    ),
                )
                .order_by(RevisionEntity.sequence_index, RevisionEntity.id)
            )
            fallback_rows = list((await db.execute(legend_query_fallback)).scalars().all())

            text_entries_fallback: list[tuple[str, tuple[float, float]]] = []
            swatch_candidates_fallback: list[tuple[tuple[float, float], Mapping[str, Any]]] = []

            for row in fallback_rows:
                geometry = row.geometry_json or {}
                if row.entity_type in ("text", "mtext"):
                    text = geometry.get("text")
                    if not isinstance(text, str) or not text.strip():
                        continue
                    pt = _entity_insertion(geometry)
                    if pt is not None:
                        text_entries_fallback.append((text.strip(), pt))
                elif row.entity_type not in _NON_SWATCH_ENTITY_TYPES:
                    style = row.style or {}
                    color_fb: Any = style.get("color") if isinstance(style, Mapping) else None
                    if not isinstance(color_fb, Mapping):
                        continue
                    if color_fb.get("by_layer") or color_fb.get("by_block"):
                        continue
                    if color_fb.get("rgb") is None and color_fb.get("index") is None:
                        continue
                    centroid = _swatch_centroid(geometry)
                    if centroid is not None:
                        swatch_candidates_fallback.append((centroid, color_fb))

            for text, text_pt in text_entries_fallback:
                clr = _nearest_swatch(text_pt, swatch_candidates_fallback, _SWATCH_MATCH_RADIUS)
                if clr is not None:
                    swatch_inputs.append(SwatchInput(color=clr, text=text))

    # Source B — prose abbreviation rows via shared loader.
    # Note: load_legend_text_candidates still uses legend-layer filtering (anchor-region
    # discovery for prose is out of scope here).
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
    broaden_tag_layers: bool = True,
) -> list[TagPlacement]:
    """Load pipe-tag text entities as :class:`TagPlacement` objects.

    For pdf_vector revisions (when ``tag_layers`` is None), tags are sourced from
    ``AdapterRunOutput.canonical_json["metadata"]["text_blocks"]`` (PDFs have no text
    entities; all prose lives in text_blocks). Each block's bbox centroid becomes the
    point; ``layer_ref`` is None. Raw text is passed through without pre-parsing
    (the coordinator calls parse_tag which self-filters non-tag prose).

    For DWG/DXF revisions (when ``tag_layers`` is None and ``broaden_tag_layers`` is
    True, the default), selects ALL text entities for the revision regardless of layer
    (ADR-003: content filtering via parse_tag downstream, not layer-scoping).  When
    ``broaden_tag_layers`` is False, falls back to the legacy ilike filter over
    ``_DEFAULT_TAG_LAYER_TOKENS`` (reachable for tests / explicit opt-out).

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
        # Each block is segmented into individual tag candidates by _extract_pdf_tag_candidates;
        # prose without a size head yields no candidates, and isolated sizes in long note
        # sentences are rejected by the per-candidate length cap — no case/stopword hacks needed.
        # All candidates from one block share the block's bbox centroid (finer per-tag
        # positioning is a future refinement).  parse_tag remains the real gate downstream.
        blocks = await _load_text_blocks(db, revision_id)
        pdf_placements: list[TagPlacement] = []
        for block in blocks:
            block_text = block.get("text")
            if not isinstance(block_text, str) or not block_text.strip():
                continue
            candidates = _extract_pdf_tag_candidates(block_text)
            if not candidates:
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
            for cand in candidates:
                pdf_placements.append(TagPlacement(text=cand, point=point, layer_ref=None))
        return pdf_placements

    # DWG/DXF (or unknown family): entity text selection, ADR-003 compliant.
    # When broaden_tag_layers is True (default): select ALL text entities — layer-scoping
    # is deferred to parse_tag (content gate) + spatial radius (proximity gate).
    # When broaden_tag_layers is False: legacy ilike filter over _DEFAULT_TAG_LAYER_TOKENS.
    from sqlalchemy import or_, select

    from app.models.revision_materialization import RevisionEntity

    query = select(RevisionEntity).where(
        RevisionEntity.drawing_revision_id == revision_id,
        RevisionEntity.entity_type == "text",
    )
    if not broaden_tag_layers:
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


async def load_service_fill_bands(
    db: AsyncSession,
    revision_id: UUID,
    *,
    layer_tokens: tuple[str, ...] = _SERVICE_FILL_LAYER_TOKENS,
    exclude_off_sheet: bool = True,
    input_family: str | None = None,
) -> list[FillBand]:
    """Load HATCH entities from pipe/rise/drop token layers as :class:`FillBand` objects.

    Mirrors the token-layer query pattern of :func:`load_rise_drop_entities`.  Only
    ``hatch`` entity types are selected; non-service colours (colour_key is None) are
    silently skipped.

    ``input_family`` is accepted for interface symmetry but not used in Phase 1 (the
    query is DWG-specific; PDF revisions have no HATCH fill bands on pipe/rise/drop layers).

    Returns ``[]`` for degenerate revisions; never raises.
    """
    del input_family  # Phase 2/3 extension point; unused in Phase 1

    from sqlalchemy import or_, select

    from app.ingestion.centerline_contract import _xy_list
    from app.models.revision_materialization import RevisionEntity

    try:
        q = select(RevisionEntity).where(
            RevisionEntity.drawing_revision_id == revision_id,
            RevisionEntity.entity_type == "hatch",
        )
        if exclude_off_sheet:
            q = q.where(RevisionEntity.on_sheet.isnot(False))
        conditions = [RevisionEntity.layer_ref.ilike(f"%{token}%") for token in layer_tokens]
        q = q.where(or_(*conditions))
        q = q.order_by(RevisionEntity.sequence_index, RevisionEntity.id)
        rows = list((await db.execute(q)).scalars().all())
    except Exception:
        return []

    result: list[FillBand] = []
    for row in rows:
        style = row.style or {}
        color: Mapping[str, Any] | None = style.get("color") if isinstance(style, Mapping) else None
        ck = _colour_key(color)
        if ck is None:
            continue

        colour_index: int | None = color.get("index") if color is not None else None
        colour_rgb: str | None = color.get("rgb") if color is not None else None

        raw_geom = row.geometry_json
        geometry: dict[str, Any] | None = raw_geom if isinstance(raw_geom, dict) else None
        if geometry is None:
            continue

        # Prefer first boundary loop; fall back to vertices field.
        boundary_loops = geometry.get("boundary_loops")
        if isinstance(boundary_loops, (list, tuple)) and boundary_loops:
            pts_raw = boundary_loops[0]
        else:
            pts_raw = geometry.get("vertices")

        ring_pts = _xy_list(pts_raw)
        if len(ring_pts) < 3:
            continue

        result.append(
            FillBand(
                colour_key=ck,
                colour_index=colour_index,
                colour_rgb=colour_rgb,
                ring=tuple(ring_pts),
            )
        )
    return result


async def load_tag_stack_texts(
    db: AsyncSession,
    revision_id: UUID,
    *,
    tag_layers: list[str] | None = None,
    input_family: str | None = None,
) -> list[TagStackText]:
    """Load Pipe-Tag text entities as :class:`TagStackText` objects.

    Maps :func:`load_tag_placements` output (TagPlacement) to the simpler
    TagStackText(text, point) shape expected by ``assign_services_by_tag_stack``.
    Raw text is passed through; parse_tag is applied downstream.

    Returns [] for empty/degenerate revisions; never raises.
    """
    try:
        placements = await load_tag_placements(
            db, revision_id, tag_layers=tag_layers, input_family=input_family
        )
        return [TagStackText(text=p.text, point=p.point) for p in placements]
    except Exception:
        return []


async def load_stack_headers(
    db: AsyncSession,
    revision_id: UUID,
    *,
    input_family: str | None = None,
) -> list[StackHeader]:
    """Load stack orientation header text entities as :class:`StackHeader` objects.

    Queries ALL text/mtext entities on ANY layer (ADR-003: Z010T is generic; do not
    layer-filter). Normalises each text via ``_normalize_legend_text`` (strips \\\\L, %%u
    formatting), then matches case-insensitively against the FROM ... TO ... pattern.

    Returns [] when none match; never raises.
    """
    del input_family  # accepted for symmetry; headers are layer-agnostic in all families

    from app.models.revision_materialization import RevisionEntity

    try:
        q = (
            select(RevisionEntity)
            .where(
                RevisionEntity.drawing_revision_id == revision_id,
                RevisionEntity.entity_type.in_(["text", "mtext"]),
            )
            .order_by(RevisionEntity.sequence_index, RevisionEntity.id)
        )
        rows = list((await db.execute(q)).scalars().all())
    except Exception:
        return []

    result: list[StackHeader] = []
    for row in rows:
        geometry = row.geometry_json or {}
        raw_text = geometry.get("text")
        if not isinstance(raw_text, str) or not raw_text.strip():
            continue
        normalized = _normalize_legend_text(raw_text)
        if not _STACK_HEADER_RE.search(normalized):
            continue
        pt = _entity_insertion(geometry)
        if pt is None:
            continue
        result.append(StackHeader(text=normalized, point=pt))
    return result


async def load_bundle_bands_by_colour(
    db: AsyncSession,
    revision_id: UUID,
    *,
    exclude_off_sheet: bool = True,
    input_family: str | None = None,
) -> dict[str, list[BundleColourBand]]:
    """Load HATCH fill bands grouped by colour_key as :class:`BundleColourBand` lists.

    Regroups :func:`load_service_fill_bands` output (list[FillBand]) by colour_key.
    No new hatch query is issued. Bands with None colour_key are already skipped by
    load_service_fill_bands.

    Returns {} for degenerate/empty revisions; never raises.
    """
    try:
        bands = await load_service_fill_bands(
            db,
            revision_id,
            exclude_off_sheet=exclude_off_sheet,
            input_family=input_family,
        )
    except Exception:
        return {}

    result: dict[str, list[BundleColourBand]] = {}
    for band in bands:
        ck = band.colour_key
        bucket = result.setdefault(ck, [])
        bucket.append(BundleColourBand(colour_key=ck, ring=band.ring))
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
    units_factor = units.get("conversion_factor")
    has_units_factor = isinstance(units_factor, (int, float)) and not isinstance(units_factor, bool)

    units_confidence: str = scale_read.units_confidence
    conversion_factor: float | None

    if has_units_factor:
        # Vector (DWG/DXF): the adapter has ALREADY scaled the stored geometry to the
        # conversion_target (metres) by this very factor — start/end/vertices are in metres
        # (libredwg `_scale_point`, ezdxf `_scaled_point_payload`). The units-block
        # ``conversion_factor`` is metadata describing the transform that was applied, NOT a
        # factor to re-apply. So the takeoff measures the already-metre drawing_length with a
        # factor of 1.0; re-applying ``conversion_factor`` double-converted (1000x under on a
        # mm drawing) — #661 Bug B.
        conversion_factor = 1.0
    else:
        # PDF: geometry is in page points (NOT pre-scaled); derive metres-per-point from
        # pdf_scale. ADR-004: PDF title-block detection is always "inferred", never confirmed.
        conversion_factor = None
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
    legend = await build_service_legend(
        db, revision_id, legend_layers=legend_layers, input_family=input_family
    )
    tag_placements = await load_tag_placements(
        db,
        revision_id,
        tag_layers=tag_layers,
        input_family=input_family,
        broaden_tag_layers=True,
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


async def load_measured_lengths(
    db: AsyncSession,
    revision_id: UUID,
    *,
    algo_version: str | None = None,
) -> tuple[dict[tuple[str | None, str | None], float], set[tuple[str | None, str | None]]]:
    """Load materialized per-group skeleton lengths for a revision at a given algo version.

    Only rows whose ``algo_version`` exactly matches the requested version are included
    (version gate).  Returns a mapping of ``(layer_ref, colour_key) -> skeleton_length_du``
    and the set of present group keys.  Returns ``({}, set())`` for degenerate / no-data
    revisions; never raises.

    Parameters
    ----------
    db:
        Active async session.
    revision_id:
        Drawing revision UUID to query.
    algo_version:
        Algorithm version string to filter on.  When ``None``, defaults to
        :data:`~app.ingestion.centerline_contract.CURRENT_ALGO_VERSION` at call time
        (imported lazily to keep this module DB-seam-only with no circular deps).
    """
    from app.ingestion.centerline_contract import CURRENT_ALGO_VERSION
    from app.models.revision_routed_length import RevisionRoutedLength

    resolved_version = algo_version if algo_version is not None else CURRENT_ALGO_VERSION

    try:
        stmt = select(RevisionRoutedLength).where(
            RevisionRoutedLength.drawing_revision_id == revision_id,
            RevisionRoutedLength.algo_version == resolved_version,
        )
        rows = list((await db.execute(stmt)).scalars().all())
    except Exception:
        return {}, set()

    mapping: dict[tuple[str | None, str | None], float] = {}
    present: set[tuple[str | None, str | None]] = set()
    for row in rows:
        key: tuple[str | None, str | None] = (row.layer_ref, row.colour_key)
        mapping[key] = row.skeleton_length_du
        present.add(key)

    return mapping, present


async def load_measured_geometry(
    db: AsyncSession,
    revision_id: UUID,
    *,
    algo_version: str | None = None,
) -> dict[tuple[str | None, str | None], tuple[tuple[tuple[float, float], ...], ...]]:
    """Load persisted per-group centerline polylines for a revision at a given algo version (#653).

    The read-path counterpart to the centerline producers' geometry: returns a mapping of
    ``(layer_ref, colour_key) -> polylines`` for groups whose ``geometry_json`` was populated
    (DWG/PDF producers; passthrough groups have none and are omitted). Version-gated like
    :func:`load_measured_lengths`. Pure JSON decode — no cv2/skimage, safe on the read path.
    Returns ``{}`` for degenerate / no-data / pre-geometry revisions; never raises.

    Consumed by LP2 per-room clipping (#654).
    """
    from app.ingestion.centerline_contract import (
        CURRENT_ALGO_VERSION,
        polylines_from_geometry_json,
    )
    from app.models.revision_routed_length import RevisionRoutedLength

    resolved_version = algo_version if algo_version is not None else CURRENT_ALGO_VERSION

    try:
        stmt = select(RevisionRoutedLength).where(
            RevisionRoutedLength.drawing_revision_id == revision_id,
            RevisionRoutedLength.algo_version == resolved_version,
        )
        rows = list((await db.execute(stmt)).scalars().all())
    except Exception:
        return {}

    geometry: dict[tuple[str | None, str | None], tuple[tuple[tuple[float, float], ...], ...]] = {}
    for row in rows:
        polylines = polylines_from_geometry_json(row.geometry_json)
        if polylines:
            geometry[(row.layer_ref, row.colour_key)] = polylines

    return geometry
