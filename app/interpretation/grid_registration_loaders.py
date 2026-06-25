"""DB seam for grid-bubble fiducial loading (issue #703).

Loads INSERT entities from the structural grid layer and resolves each bubble's label from
the nearest text entity within a configurable radius.  Pure computation lives in
grid_registration.py (``estimate_grid_transform``).

Nearest-text strategy mirrors ``_nearest_tag`` / ``_rows_to_candidates`` from
``app.interpretation.devices``, but scoped here to avoid coupling that module's DeviceTag
dataclass into this loader.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.ingestion.centerline_contract import _xy
from app.interpretation.grid_registration import GridFiducial, _normalize_label
from app.models.revision_materialization import RevisionEntity

# Default configurable-default layer tokens (ADR-003).
_DEFAULT_GRID_LAYER_TOKENS: tuple[str, ...] = ("z030g", "grid")


def _insertion_point(geometry_json: Any) -> tuple[float, float] | None:
    """Extract the (x, y) insertion point from an INSERT entity's geometry_json.

    Supports the materialized canonical shape: geometry_json["transform"]["insertion_point"]
    as a dict with x/y keys.  Falls back to a top-level "insertion" key for legacy payloads.
    """
    if not isinstance(geometry_json, Mapping):
        return None
    transform = geometry_json.get("transform")
    if isinstance(transform, Mapping):
        pt = _xy(transform.get("insertion_point"))
        if pt is not None:
            return pt
    return _xy(geometry_json.get("insertion") or geometry_json.get("insert"))


def _text_content(geometry_json: Any) -> str | None:
    """Extract text string from a text/mtext entity's geometry_json."""
    if not isinstance(geometry_json, Mapping):
        return None
    text = geometry_json.get("text")
    return text if isinstance(text, str) and text.strip() else None


def _text_position(geometry_json: Any) -> tuple[float, float] | None:
    """Extract the position of a text/mtext entity (insertion key)."""
    if not isinstance(geometry_json, Mapping):
        return None
    return _xy(geometry_json.get("insertion") or geometry_json.get("insert"))


def _nearest_text_label(
    bubble_xy: tuple[float, float],
    text_rows: list[tuple[tuple[float, float], str]],
    radius_m: float,
) -> str | None:
    """Return the nearest text label within *radius_m* of *bubble_xy*, or None.

    *text_rows* is a list of ((x, y), normalized_label) for all candidate text entities.
    Same approach as ``_nearest_tag`` in app.interpretation.devices.
    """
    best_label: str | None = None
    best_dist = math.inf
    bx, by = bubble_xy
    for (tx, ty), label in text_rows:
        d = math.hypot(tx - bx, ty - by)
        if d < best_dist:
            best_dist = d
            best_label = label
    if best_label is None or best_dist > radius_m:
        return None
    return best_label


async def load_grid_fiducials(
    db: AsyncSession,
    revision_id: UUID,
    *,
    grid_layer_tokens: tuple[str, ...] = _DEFAULT_GRID_LAYER_TOKENS,
    label_radius_m: float = 0.5,
) -> list[GridFiducial]:
    """Load grid-bubble INSERT entities and resolve each bubble's label from the nearest text.

    Query strategy
    --------------
    - SELECT INSERT entities whose ``layer_ref`` ILIKE any token in *grid_layer_tokens*, OR
      whose ``block_ref`` ILIKE "%grid%".  The OR gives robustness when the layer name differs
      slightly from the configurable default token.
    - Query all text/mtext entities for the revision once; build a cheap nearest-text lookup.
    - For each bubble, find the nearest text within *label_radius_m*.  A label further away is
      not associated (R-1: the nearest unrelated text is ~2.6 m away; 0.5 m is a clean cut).
    - Bubbles with no resolvable label are skipped (can't be used as fiducials).
    - Missing or malformed geometry is skipped silently (tolerant by design).

    Returns
    -------
    List of GridFiducial(label, point) with labels normalized (stripped + upper-cased).
    """
    # --- Load grid INSERT entities ---
    ilike_conditions = [RevisionEntity.layer_ref.ilike(f"%{token}%") for token in grid_layer_tokens]
    insert_query = select(RevisionEntity).where(
        RevisionEntity.drawing_revision_id == revision_id,
        RevisionEntity.entity_type == "insert",
        or_(
            *ilike_conditions,
            RevisionEntity.block_ref.ilike("%grid%"),
        ),
    )
    insert_rows = list((await db.execute(insert_query)).scalars().all())

    # --- Load all text/mtext entities for this revision (one query) ---
    text_query = select(RevisionEntity).where(
        RevisionEntity.drawing_revision_id == revision_id,
        RevisionEntity.entity_type.in_(["text", "mtext"]),
    )
    text_rows_db = list((await db.execute(text_query)).scalars().all())

    # Build (position, normalized_label) pairs for the nearest-text scan.
    text_candidates: list[tuple[tuple[float, float], str]] = []
    for row in text_rows_db:
        pos = _text_position(row.geometry_json)
        content = _text_content(row.geometry_json)
        if pos is None or content is None:
            continue
        text_candidates.append((pos, _normalize_label(content)))

    # --- Resolve each bubble to a labelled fiducial ---
    fiducials: list[GridFiducial] = []
    for row in insert_rows:
        bubble_xy = _insertion_point(row.geometry_json)
        if bubble_xy is None:
            continue
        label = _nearest_text_label(bubble_xy, text_candidates, label_radius_m)
        if label is None:
            # No text within radius — bubble cannot be used as a fiducial.
            continue
        fiducials.append(GridFiducial(label=label, point=bubble_xy))

    return fiducials
