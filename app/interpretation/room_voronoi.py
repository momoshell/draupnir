"""Bounded Voronoi room-partition primitive (issue #715, Phase R-B).

Pure module: stdlib + ``app.interpretation.geometry`` type aliases ONLY.  No
shapely, scipy, cv2, skimage, DB, or FastAPI.  This is the Voronoi-assignment
fallback consumed by Phase R-C.

The nearest-tag-within-bound classifier IS the Voronoi assignment.  Explicit
Voronoi cell polygons would require shapely/scipy (not core deps) and violate
the pure-stdlib bar, so we use the classifier directly — the nearest anchor
within the bounding envelope is mathematically equivalent for assignment
purposes.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass

# Type aliases mirroring app.interpretation.geometry.  Defined locally so this
# module stays pure-stdlib and importable without shapely (which geometry.py
# loads at module level).  Values are identical; callers that import both
# modules see compatible structural types.
Point = tuple[float, float]
"""A 2D coordinate as an ``(x, y)`` float tuple."""

BoundingBox = tuple[float, float, float, float]
"""An axis-aligned bounding box as ``(min_x, min_y, max_x, max_y)``."""

DEFAULT_BOUND_MARGIN_M: float = 5.0
"""Expand the tag-set bounding envelope by this many metres on every side.

Calibrated against Welbeck drawings.  Points beyond this envelope are treated
as outside the floor plan and assigned ``None``; pass a custom ``margin``
keyword argument to override.
"""


@dataclass(frozen=True, slots=True)
class RoomTag:
    """A room number and every world-point anchor where that number appears.

    ``number`` is the cross-sheet join key (e.g. ``"0.9.01"``).  ``anchors``
    must contain at least one point — a multi-anchor tag (#581) covers U-shaped
    spaces where the same number is placed in each arm.
    """

    number: str
    anchors: tuple[Point, ...]

    def __post_init__(self) -> None:
        if not self.anchors:
            raise ValueError(f"RoomTag {self.number!r}: anchors must be non-empty")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def floor_bound(
    tags: Sequence[RoomTag],
    *,
    margin: float = DEFAULT_BOUND_MARGIN_M,
) -> BoundingBox | None:
    """Return the axis-aligned bounding box of all tag anchors, expanded by ``margin``.

    Returns ``None`` when ``tags`` is empty.  The expansion makes the useful
    floor area slightly larger than the tag extent so that points near-but-not-
    between tags still fall within the partition.
    """
    if not tags:
        return None

    xs: list[float] = []
    ys: list[float] = []
    for tag in tags:
        for x, y in tag.anchors:
            xs.append(x)
            ys.append(y)

    return (
        min(xs) - margin,
        min(ys) - margin,
        max(xs) + margin,
        max(ys) + margin,
    )


def assign_point_to_room(
    point: Point,
    tags: Sequence[RoomTag],
    *,
    margin: float = DEFAULT_BOUND_MARGIN_M,
) -> str | None:
    """Return the nearest in-bound tag's number, or ``None``.

    Returns ``None`` when:

    * ``tags`` is empty, OR
    * ``point`` lies outside ``floor_bound(tags, margin=margin)``.

    The envelope check is **inclusive**: a point exactly on the boundary edge
    is accepted (``>=`` / ``<=``).  The bound is the SOLE rejection criterion —
    a point inside the envelope is always assigned, even if it is far from every
    anchor.

    Distance to a room is the distance to its **nearest** anchor
    (``math.hypot``), not its centroid.  Ties are broken by room number
    lexicographic ascending so the result is deterministic regardless of input
    order.

    **Caller contract — unique numbers:** ``tags`` should carry unique
    ``number`` values.  The cross-sheet join key is ``number``; if two tags
    share a number the tie-break on distance is still deterministic (ascending
    number, then stable ``min``), but only one of the duplicate objects is
    returned.  R-C must dedupe/merge anchors by number before calling so that
    all placements of the same room number are represented in a single
    ``RoomTag``.
    """
    bound = floor_bound(tags, margin=margin)
    if bound is None:
        return None

    min_x, min_y, max_x, max_y = bound
    px, py = point
    if px < min_x or px > max_x or py < min_y or py > max_y:
        return None

    best = min(
        tags,
        key=lambda tag: (_nearest_anchor_distance(point, tag), tag.number),
    )
    return best.number


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _nearest_anchor_distance(point: Point, tag: RoomTag) -> float:
    """Distance from ``point`` to the closest of ``tag``'s anchor points."""
    return min(_distance(point, anchor) for anchor in tag.anchors)


def _distance(a: Point, b: Point) -> float:
    return math.hypot(a[0] - b[0], a[1] - b[1])
