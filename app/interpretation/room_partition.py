"""Shared per-room polyline clip primitive (LP2) — issue #716, Phase R.

This module is PURE: shapely is allowed; NO DB, ORM, FastAPI, cv2, or skimage.
It is the authoritative home of the LP2 clip algorithm so that multiple
coordinators (service_takeoff, future cable_takeoff, …) can consume it without
duplication.

Intentional duplication note
-----------------------------
:func:`_containing_room` here sorts by ``(area, room.id)`` (permutation-invariant
tie-break).  ``rooms._smallest_containing_room`` sorts by area only.  These are
intentionally kept separate; collapsing them would be a behaviour change that is
out of scope for this refactor (#716).

Coordinate-frame contract
--------------------------
:func:`partition_polylines_by_room` does NOT transform its inputs.  Callers are
responsible for supplying ``polylines`` and ``rooms`` already in the same drawing
coordinate frame.
"""

from __future__ import annotations

import math
from collections.abc import Iterator, Sequence
from typing import Any

from shapely.geometry import LineString, MultiLineString

from app.interpretation.geometry import point_in_polygon
from app.interpretation.rooms import Room, _nearest_anchor_distance

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Lengths are drawing units (mm or PDF points); 1e-9 safely drops only zero-length
# point/degenerate clip results, never a legitimate room crossing at architectural scales.
_CLIP_EPS = 1e-9

# Real-world cap (metres) for attributing otherwise-unassigned run length to the nearest
# label-only room (#662). Mirrors the device->label-room precedent
# (room_pipeline.DEFAULT_DEVICE_LABEL_ROOM_RADIUS=8.0, #555): a plant space spans several
# metres; beyond this a run is left unassigned rather than guessed (ADR-004). Converted to
# drawing units per call via scale.conversion_factor — the coordinator works in drawing space.
_LABEL_ROOM_RADIUS_M: float = 8.0


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _containing_room(point: tuple[float, float], rooms: Sequence[Room]) -> Room | None:
    """Smallest polygon room whose polygon contains ``point``.

    Label-only rooms (no polygon) are excluded — they have no boundary to test.
    Secondary key on ``r.id`` ensures the result is permutation-invariant when two
    rooms share exactly the same area.

    Note: tie-break is ``(area, room.id)``, intentionally different from
    ``rooms._smallest_containing_room`` which uses area only — do NOT collapse them.
    """
    containing = [
        room
        for room in rooms
        if room.polygon is not None
        and room.area is not None
        and point_in_polygon(point, room.polygon)
    ]
    if not containing:
        return None
    return min(containing, key=lambda r: (r.area if r.area is not None else 0.0, r.id))


def _nearest_label_room(
    point: tuple[float, float],
    rooms: Sequence[Room],
    *,
    radius: float,
) -> Room | None:
    """Nearest label-only room (no polygon) whose anchor is within ``radius`` of ``point`` (#662).

    Fallback for run length that no polygon room contains. ``radius`` is in the coordinator's
    drawing-coordinate space (caller converts from metres via the scale factor). Distance is the
    distance to the room's NEAREST anchor (merged U-shaped rooms cover several placements, #581).
    Deterministic: ties broken by ``(distance, room.id)`` so the result is permutation-invariant.
    Returns None when there are no label rooms or none within ``radius`` (honest unassigned).
    """
    label_rooms = [r for r in rooms if r.polygon is None and r.location is not None]
    if not label_rooms:
        return None
    nearest = min(label_rooms, key=lambda r: (_nearest_anchor_distance(point, r), r.id))
    if _nearest_anchor_distance(point, nearest) <= radius:
        return nearest
    return None


def _iter_line_segments(
    geom: Any,
) -> Iterator[tuple[tuple[float, float], tuple[float, float]]]:
    """Yield consecutive 2-point segments from a shapely LineString or MultiLineString.

    Handles both geometry types uniformly so callers don't need to branch on
    ``geom_type``. Each yielded pair is ((x0, y0), (x1, y1)) — the two endpoints
    of one straight segment of the original polyline.
    """
    lines = geom.geoms if geom.geom_type == "MultiLineString" else [geom]
    for line in lines:
        coords = list(line.coords)
        for i in range(len(coords) - 1):
            yield (coords[i][0], coords[i][1]), (coords[i + 1][0], coords[i + 1][1])


# ---------------------------------------------------------------------------
# Public LP2 primitive
# ---------------------------------------------------------------------------


def partition_polylines_by_room(
    polylines: tuple[tuple[tuple[float, float], ...], ...],
    rooms: Sequence[Room],
    label_radius: float | None = None,
) -> list[tuple[Room | None, float]]:
    """Distribute centerline polyline length across rooms by clipping (#654, LP2).

    ``polylines`` are already in the room coordinate frame — the caller pre-transforms;
    this function does NOT apply any coordinate transform.  ``rooms`` is an external set
    supplied by the caller.  Returns a list of ``(Room | None, length)`` pairs where
    ``None`` means unassigned (caller owns the sentinel key for that bucket).

    Each polyline segment is assigned to the SMALLEST room whose polygon contains it:
    rooms are processed area-ascending and claimed geometry is subtracted as we go, so
    nested/overlapping rooms never double-count. Length not inside any room falls to
    the unassigned bucket (``room=None``). Lengths are drawing units — polylines and room
    polygons share the drawing coordinate space; scale is applied per bucket downstream.
    Returns ``[]`` when there is no usable line geometry (caller falls back to anchor
    attribution).

    Parameters
    ----------
    polylines:
        Tuple of vertex sequences, each a closed or open polyline in drawing coordinates.
        Only polylines with at least 2 vertices contribute geometry.
    rooms:
        Rooms to clip against.  Polygon rooms are used for containment; label-only rooms
        (no polygon) are used only when ``label_radius`` is provided.
    label_radius:
        When not None, the maximum drawing-unit distance from a segment midpoint to a
        label-only room anchor for the segment to be attributed to that room (#662).
        When None, unclipped remainder is left entirely unassigned (ADR-004).
    """
    lines = [LineString(pl) for pl in polylines if len(pl) >= 2]
    if not lines:
        return []
    remaining: Any = MultiLineString(lines) if len(lines) > 1 else lines[0]
    partitions: list[tuple[Room | None, float]] = []
    rooms_with_polygon = sorted(
        (r for r in rooms if r.polygon is not None and r.area is not None),
        key=lambda r: (r.area if r.area is not None else 0.0, r.id),
    )
    for room in rooms_with_polygon:
        if remaining.is_empty:
            break
        inside = remaining.intersection(room.polygon)
        length = float(inside.length)
        if length > _CLIP_EPS:
            partitions.append((room, length))
            remaining = remaining.difference(room.polygon)
    remainder = float(remaining.length) if not remaining.is_empty else 0.0
    if remainder > _CLIP_EPS:
        if label_radius is None:
            # No scale factor available — cannot compute a meaningful radius; leave whole
            # remainder unassigned (ADR-004: honest degradation, no guessing).
            partitions.append((None, remainder))
        else:
            # Per-segment distribution: walk each straight segment of the remainder,
            # find its nearest label room by midpoint, and accumulate length per room.
            # Segments with no label room in range contribute to an unassigned bucket.
            label_buckets: dict[str, float] = {}  # room.id -> length
            label_room_by_id: dict[str, Room] = {}  # room.id -> Room object
            unassigned_length = 0.0
            for (x0, y0), (x1, y1) in _iter_line_segments(remaining):
                seg_len = math.hypot(x1 - x0, y1 - y0)
                if seg_len <= _CLIP_EPS:
                    continue
                midpoint = ((x0 + x1) / 2.0, (y0 + y1) / 2.0)
                seg_room = _nearest_label_room(midpoint, rooms, radius=label_radius)
                if seg_room is None:
                    unassigned_length += seg_len
                else:
                    label_buckets[seg_room.id] = label_buckets.get(seg_room.id, 0.0) + seg_len
                    label_room_by_id[seg_room.id] = seg_room
            # Emit label-room partitions sorted by room.id (determinism; coordinator is
            # permutation-invariant so ordering here is only for stable output).
            for rid in sorted(label_buckets):
                length = label_buckets[rid]
                if length > _CLIP_EPS:
                    partitions.append((label_room_by_id[rid], length))
            if unassigned_length > _CLIP_EPS:
                partitions.append((None, unassigned_length))
    return partitions
