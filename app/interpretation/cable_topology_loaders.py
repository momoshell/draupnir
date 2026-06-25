"""DB seam for the cable topology builder (issue #693).

Single database seam: all DB / ORM access for the cable-topology pipeline lives
here.  The pure builder (:mod:`app.interpretation.cable_topology`) consumes only
the frozen dataclasses exported by this module — no ORM, no SQL.

Loads:
- :class:`~app.interpretation.cable_topology.SplineInput` list from
  ``revision_entities`` where ``entity_type == 'spline'``.
- :class:`~app.interpretation.cable_topology.DeviceFootprint` list via
  :func:`~app.interpretation.devices.enumerate_devices`, filtered to non-architecture
  / non-annotation blocks (v1 substring filter).

**Footprint source (verified on E-630003):**
INSERT rows have NULL bbox columns — the footprint must be derived by transforming
child-entity local bboxes (from the ``RevisionBlock.payload_json["entities"]`` list)
by the insert's affine transform (insertion_point, scale, rotation_radians, base_point).
The position fallback (degenerate zero-area bbox at the insertion point) is used only
when block children yield no bbox.

All functions are tolerant: missing / degenerate rows are skipped silently.
"""

from __future__ import annotations

import math
import uuid as _uuid_module
from collections.abc import Mapping, Sequence
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.ingestion.centerline_contract import _xy
from app.ingestion.entity_geometry import compute_entity_bbox
from app.interpretation.cable_topology import DeviceFootprint, SplineInput
from app.interpretation.device_identity import (
    _ANNOTATION_FAMILY_PATTERNS,
    ARCHITECTURE_FAMILY_PATTERNS,
)
from app.interpretation.devices import enumerate_devices
from app.models.revision_materialization import RevisionBlock, RevisionEntity

# ---------------------------------------------------------------------------
# v1 block-ref filter (architecture + annotation families)
# ---------------------------------------------------------------------------


def _is_architecture_block(block_ref: str | None) -> bool:
    """Return True if *block_ref* matches any ARCHITECTURE_FAMILY_PATTERNS (v1 substring filter).

    v1 path: case-insensitive substring match against the imported
    ``ARCHITECTURE_FAMILY_PATTERNS`` tuple from device_identity.  A follow-up
    can replace this with full ``classify_instance_kind``/``KIND_DEVICE`` once a
    legend is wired.
    """
    if block_ref is None:
        return False
    lower = block_ref.lower()
    return any(pat.lower() in lower for pat in ARCHITECTURE_FAMILY_PATTERNS)


def _is_annotation_block(block_ref: str | None) -> bool:
    """Return True if *block_ref* matches any _ANNOTATION_FAMILY_PATTERNS (v1 substring filter)."""
    if block_ref is None:
        return False
    lower = block_ref.lower()
    return any(pat.lower() in lower for pat in _ANNOTATION_FAMILY_PATTERNS)


def _is_non_device_block(block_ref: str | None) -> bool:
    """Return True if the block should be excluded from the device footprint list."""
    return _is_architecture_block(block_ref) or _is_annotation_block(block_ref)


# ---------------------------------------------------------------------------
# Affine transform helper (pure, unit-testable)
# ---------------------------------------------------------------------------


def _apply_affine(
    point: tuple[float, float],
    *,
    insertion: tuple[float, float],
    scale: tuple[float, float],
    rotation_rad: float,
    base: tuple[float, float],
) -> tuple[float, float]:
    """Transform a LOCAL block point to world coordinates.

    Formula: world = insertion + R(rotation_rad) · (scale x (local - base))

    - ``point``:        local (x, y) in block-definition space.
    - ``insertion``:    world insertion point of the INSERT entity.
    - ``scale``:        per-axis scale factors (x, y).
    - ``rotation_rad``: rotation in radians (counter-clockwise).
    - ``base``:         block base_point in local space (subtracted before transform).
    """
    lx = (point[0] - base[0]) * scale[0]
    ly = (point[1] - base[1]) * scale[1]
    cos_r = math.cos(rotation_rad)
    sin_r = math.sin(rotation_rad)
    wx = insertion[0] + lx * cos_r - ly * sin_r
    wy = insertion[1] + lx * sin_r + ly * cos_r
    return (wx, wy)


# ---------------------------------------------------------------------------
# Internal: extract insert transform from geometry_json
# ---------------------------------------------------------------------------


def _parse_insert_transform(
    geometry: Any,
) -> tuple[tuple[float, float], tuple[float, float], float] | None:
    """Extract (insertion_pt, scale_xy, rotation_rad) from an INSERT entity's geometry_json.

    Returns None if the geometry is missing or malformed.
    """
    if not isinstance(geometry, Mapping):
        return None
    transform = geometry.get("transform")
    if not isinstance(transform, Mapping):
        return None

    insertion_raw = transform.get("insertion_point")
    insertion_xy = _xy(insertion_raw)
    if insertion_xy is None:
        return None

    scale_raw = transform.get("scale")
    sx, sy = 1.0, 1.0
    if isinstance(scale_raw, Mapping):
        x_val = scale_raw.get("x")
        y_val = scale_raw.get("y")
        if isinstance(x_val, (int, float)) and math.isfinite(float(x_val)):
            sx = float(x_val)
        if isinstance(y_val, (int, float)) and math.isfinite(float(y_val)):
            sy = float(y_val)
    elif isinstance(scale_raw, (int, float)) and math.isfinite(float(scale_raw)):
        sx = sy = float(scale_raw)

    rot_raw = transform.get("rotation_radians")
    rotation_rad = 0.0
    if isinstance(rot_raw, (int, float)) and math.isfinite(float(rot_raw)):
        rotation_rad = float(rot_raw)

    return (insertion_xy, (sx, sy), rotation_rad)


def _parse_base_point(payload: Any) -> tuple[float, float]:
    """Extract the block base_point from a RevisionBlock payload_json; defaults to (0, 0)."""
    if not isinstance(payload, Mapping):
        return (0.0, 0.0)
    bp = _xy(payload.get("base_point"))
    return bp if bp is not None else (0.0, 0.0)


# ---------------------------------------------------------------------------
# Internal: compute world bbox from block children + insert transform
# ---------------------------------------------------------------------------


def _compute_block_world_bbox(
    payload: Any,
    *,
    insertion: tuple[float, float],
    scale: tuple[float, float],
    rotation_rad: float,
) -> tuple[float, float, float, float] | None:
    """Compute the axis-aligned world bbox of a block by transforming all child bboxes.

    For each child entity in ``payload["entities"]``:
    1. Compute local bbox via ``compute_entity_bbox(child["geometry"])``.
    2. Transform the 4 corners of the local bbox by the insert affine.
    3. Union all transformed corners into the final world bbox.

    Returns None if no child yields a usable local bbox.
    """
    if not isinstance(payload, Mapping):
        return None

    base = _parse_base_point(payload)
    raw_children = payload.get("entities")
    if not isinstance(raw_children, (list, tuple)):
        return None

    world_xs: list[float] = []
    world_ys: list[float] = []

    for child in raw_children:
        if not isinstance(child, Mapping):
            continue
        child_geom = child.get("geometry")
        local_bbox = compute_entity_bbox(child_geom)
        if local_bbox is None:
            continue
        lx0, ly0, lx1, ly1 = local_bbox
        # Transform the 4 corners of the local AABB.
        for lx, ly in ((lx0, ly0), (lx0, ly1), (lx1, ly0), (lx1, ly1)):
            wx, wy = _apply_affine(
                (lx, ly),
                insertion=insertion,
                scale=scale,
                rotation_rad=rotation_rad,
                base=base,
            )
            world_xs.append(wx)
            world_ys.append(wy)

    if not world_xs:
        return None
    return (min(world_xs), min(world_ys), max(world_xs), max(world_ys))


# ---------------------------------------------------------------------------
# Spline loader
# ---------------------------------------------------------------------------


async def load_spline_inputs(
    db: AsyncSession,
    revision_id: UUID,
    *,
    exclude_off_sheet: bool = True,
) -> list[SplineInput]:
    """Load all spline entities for *revision_id* as :class:`SplineInput` objects.

    Vertices are extracted from ``geometry_json`` using :func:`_xy` (dict coords).
    ``closed`` is read from ``geometry_json.get('closed', False)``.

    Degenerate rows (missing / unreadable vertices, fewer than 1 vertex) are
    skipped silently — the builder handles <2-vertex splines as ``dropped``.

    ``exclude_off_sheet``: when True, restricts to entities where ``on_sheet`` is
    not False (mirrors the pattern in :mod:`app.interpretation.devices`).
    # TODO: wire the on_sheet filter for splines once #588 confirms the column
    # is populated for spline entities (currently accepted as a no-op kwarg).
    """
    query = select(RevisionEntity).where(
        RevisionEntity.drawing_revision_id == revision_id,
        RevisionEntity.entity_type == "spline",
    )
    if exclude_off_sheet:
        query = query.where(RevisionEntity.on_sheet.isnot(False))

    rows: Sequence[RevisionEntity] = (await db.execute(query)).scalars().all()

    results: list[SplineInput] = []
    for row in rows:
        geometry: Any = row.geometry_json or {}
        raw_vertices = geometry.get("vertices") or geometry.get("points") or []
        if not isinstance(raw_vertices, (list, tuple)):
            continue

        parsed: list[tuple[float, float]] = []
        for v in raw_vertices:
            xy = _xy(v)
            if xy is not None:
                parsed.append(xy)

        # Builder will mark <2 as dropped; we pass through 1-vertex splines so
        # the conservation invariant is maintained by the builder counter.
        if not parsed:
            continue

        closed: bool = bool(geometry.get("closed", False))
        results.append(
            SplineInput(
                entity_id=row.entity_id,
                vertices=tuple(parsed),
                closed=closed,
            )
        )

    return results


# ---------------------------------------------------------------------------
# Device footprint loader
# ---------------------------------------------------------------------------


async def load_device_footprints(
    db: AsyncSession,
    revision_id: UUID,
    *,
    exclude_off_sheet: bool = False,
) -> list[DeviceFootprint]:
    """Load device footprints for *revision_id* filtered to non-architecture/annotation blocks.

    NB: ``exclude_off_sheet`` defaults to **False** here (unlike the device-schedule
    pipeline). On REVIT-exported sheets a device INSERT's ``insertion_point`` is the
    block ORIGIN (e.g. ``(19.65, 0.0)``), metres from the fixture's real plan location,
    so on-sheet membership — computed from the insertion point — wrongly flags real
    fixtures (E-630003: all 20 luminaires) as off-sheet. Cable-terminal association is
    proximity-bounded (``terminal_assoc_m``), so genuinely off-sheet devices that don't
    sit near the spline network simply never associate — making off-sheet exclusion both
    harmful (drops real luminaires) and unnecessary here. Verified: with this default,
    20/20 E-630003 luminaires associate; with ``True``, 0/20.

    Uses :func:`~app.interpretation.devices.enumerate_devices` to walk the full
    block-instance tree (same as the device-schedule pipeline).  Architecture-family
    and annotation-family blocks are removed via the v1 substring filter
    (``ARCHITECTURE_FAMILY_PATTERNS`` + ``_ANNOTATION_FAMILY_PATTERNS``).

    **Footprint derivation (E-630003 verified):**
    INSERT entity bbox columns are NULL for all inserts.  The correct footprint is
    the world bbox of the block's child entities, transformed by the insert's affine
    (insertion_point, scale, rotation_radians, base_point).  When no children yield
    a usable bbox, the insertion point is used as a degenerate position fallback.

    ``kind`` is set to ``"device"`` for all kept (non-architecture/annotation) instances.

    v1 filter note: a follow-up can swap ``_is_non_device_block`` for full
    ``classify_instance_kind``/``KIND_DEVICE`` once a legend is wired.
    """
    devices = await enumerate_devices(db, revision_id, exclude_off_sheet=exclude_off_sheet)

    # Filter out architecture + annotation blocks (v1 substring).
    kept = [d for d in devices if not _is_non_device_block(d.block_ref)]

    if not kept:
        return []

    # Fetch INSERT RevisionEntity rows for root-level devices (no "/" in entity_id).
    # We need block_id + geometry_json["transform"] from the INSERT row.
    root_entity_ids = [d.entity_id for d in kept if "/" not in d.entity_id]

    entity_row_by_id: dict[str, RevisionEntity] = {}
    if root_entity_ids:
        rows: Sequence[RevisionEntity] = (
            (
                await db.execute(
                    select(RevisionEntity).where(
                        RevisionEntity.drawing_revision_id == revision_id,
                        RevisionEntity.entity_id.in_(root_entity_ids),
                    )
                )
            )
            .scalars()
            .all()
        )
        for row in rows:
            entity_row_by_id[row.entity_id] = row

    # Collect all block_ids we need (non-null).
    block_ids: set[_uuid_module.UUID] = set()
    for _eid, row in entity_row_by_id.items():
        if row.block_id is not None:
            block_ids.add(row.block_id)

    # Bulk-fetch RevisionBlock rows by primary-key id.
    block_by_id: dict[_uuid_module.UUID, RevisionBlock] = {}
    if block_ids:
        block_rows: Sequence[RevisionBlock] = (
            (
                await db.execute(
                    select(RevisionBlock).where(
                        RevisionBlock.id.in_(list(block_ids)),
                    )
                )
            )
            .scalars()
            .all()
        )
        for br in block_rows:
            block_by_id[br.id] = br

    footprints: list[DeviceFootprint] = []
    for device in kept:
        # Extract position from Device.position dict {"x": ..., "y": ...}.
        position: tuple[float, float] | None = None
        if device.position is not None:
            px = device.position.get("x")
            py = device.position.get("y")
            if isinstance(px, (int, float)) and isinstance(py, (int, float)):
                position = (float(px), float(py))

        bbox: tuple[float, float, float, float] | None = None

        is_root = "/" not in device.entity_id
        if is_root:
            entity_row = entity_row_by_id.get(device.entity_id)
            if entity_row is not None:
                transform_parsed = _parse_insert_transform(entity_row.geometry_json)
                if transform_parsed is not None:
                    insertion_pt, scale_xy, rotation_rad = transform_parsed
                    block_id = entity_row.block_id
                    if block_id is not None:
                        block_row = block_by_id.get(block_id)
                        if block_row is not None:
                            bbox = _compute_block_world_bbox(
                                block_row.payload_json,
                                insertion=insertion_pt,
                                scale=scale_xy,
                                rotation_rad=rotation_rad,
                            )

        # Fallback: use the insertion_point as a degenerate zero-area bbox.
        # The insertion_point is the block ORIGIN in world space and may be offset
        # from the fixture's visual plan position, but is better than nothing when
        # no children yield a usable bbox.

        footprints.append(
            DeviceFootprint(
                entity_id=device.entity_id,
                block_ref=device.block_ref,
                kind="device",
                bbox=bbox,
                position=position,
            )
        )

    return footprints
