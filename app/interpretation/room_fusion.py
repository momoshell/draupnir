"""Shared room registry — D1 hybrid polygon-first + Voronoi fallback (issue #717, Phase R-C).

Pure module: stdlib + shapely + app.interpretation.{rooms,room_voronoi} ONLY.
No cv2, skimage, DB, or FastAPI.

The ``RoomRegistry`` returned by ``build_room_registry`` is frozen and safe for concurrent
read access. Its ``classify`` method implements the D1 hybrid:

1. Polygon-first: point-in-smallest-polygon wins unconditionally (no Voronoi state is read).
2. Voronoi fallback: ``assign_point_to_room`` over label-only-room anchors, bounded.
3. Unassigned: outside every polygon and outside the Voronoi envelope.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

# Re-export so callers can import from one place.
from app.interpretation.geometry import Point  # noqa: F401 (re-export)
from app.interpretation.room_voronoi import (
    DEFAULT_BOUND_MARGIN_M,
    RoomTag,
    assign_point_to_room,
)
from app.interpretation.rooms import (
    Room,
    _smallest_containing_room,
    room_from_label,
)

DEFAULT_VORONOI_CONFIDENCE: float = 0.4
"""Confidence score assigned to all Voronoi-fallback assignments."""


@dataclass(frozen=True, slots=True)
class RoomAssignment:
    """The result of classifying a world point against the room registry.

    ``room_number`` / ``room_id`` / ``room_name`` are ``None`` when the point
    falls outside every known room (unassigned).  ``boundary_basis`` records
    which evidence path produced the result.
    """

    room_number: str | None
    room_id: str | None
    room_name: str | None
    boundary_basis: str | None  # "polygon" | "voronoi" | None
    confidence: float | None
    needs_review: bool


@dataclass(frozen=True, slots=True)
class RoomRegistry:
    """Frozen registry that classifies world points to rooms (D1 hybrid).

    Build with ``build_room_registry``; call ``classify`` for each point.
    Polygon-first: a point inside any polygon room is resolved before Voronoi
    state is ever consulted — byte-identical results regardless of
    ``voronoi_fallback``.
    """

    # Polygon rooms (Room.polygon is not None).
    _polygon_rooms: tuple[Room, ...]
    # Voronoi tags built from deduplicated label-only rooms.
    _voronoi_tags: tuple[RoomTag, ...]
    # Room numbers whose label location falls inside some polygon room.
    _nested_numbers: frozenset[str]
    # Map number → label-only Room (for metadata lookups on Voronoi hits).
    _label_room_by_number: dict[str, Room]
    # Configuration baked in at build time.
    _voronoi_fallback: bool
    _margin: float
    _voronoi_confidence: float

    @property
    def polygon_rooms(self) -> tuple[Room, ...]:
        """Return the polygon-room tuple (read-only view for callers that need the set)."""
        return self._polygon_rooms

    def classify(self, point: tuple[float, float]) -> RoomAssignment:
        """Classify ``point`` via the D1 hybrid and return a ``RoomAssignment``.

        Step 1 — polygon-first: returns immediately if any polygon room contains
        the point.  Voronoi state is never read in this branch (byte-identity).

        Step 2 — Voronoi fallback (only if enabled): nearest label-only anchor
        within the bounded envelope.

        Step 3 — unassigned.
        """
        # -- Step 1: polygon-first (must return before touching Voronoi) ------
        polygon_room = _smallest_containing_room(point, self._polygon_rooms)
        if polygon_room is not None:
            return RoomAssignment(
                room_number=polygon_room.number,
                room_id=polygon_room.id,
                room_name=polygon_room.name,
                boundary_basis="polygon",
                confidence=polygon_room.confidence,
                needs_review=polygon_room.needs_review,
            )

        # -- Step 2: Voronoi fallback -----------------------------------------
        if self._voronoi_fallback and self._voronoi_tags:
            matched_number = assign_point_to_room(point, self._voronoi_tags, margin=self._margin)
            if matched_number is not None:
                label_room = self._label_room_by_number.get(matched_number)
                base_needs_review = label_room.needs_review if label_room is not None else False
                nested = matched_number in self._nested_numbers
                return RoomAssignment(
                    room_number=matched_number,
                    room_id=label_room.id if label_room is not None else None,
                    room_name=label_room.name if label_room is not None else None,
                    boundary_basis="voronoi",
                    confidence=self._voronoi_confidence,
                    needs_review=base_needs_review or nested,
                )

        # -- Step 3: unassigned -----------------------------------------------
        return RoomAssignment(
            room_number=None,
            room_id=None,
            room_name=None,
            boundary_basis=None,
            confidence=None,
            needs_review=False,
        )


def build_room_registry(
    rooms: Sequence[Room],
    *,
    voronoi_fallback: bool = True,
    margin: float = DEFAULT_BOUND_MARGIN_M,
    voronoi_confidence: float = DEFAULT_VORONOI_CONFIDENCE,
) -> RoomRegistry:
    """Build a frozen :class:`RoomRegistry` from a list of interpreted rooms.

    Rooms are split into two groups:

    * **Polygon rooms** (``room.polygon is not None``): used for point-in-polygon
      containment.
    * **Label-only rooms** (``room.polygon is None and room.location is not None``):
      deduped by number then used to build Voronoi tags.

    Deduplication by number: if two label-only rooms share a ``number``, their
    anchors are merged (order-preserving union, dropping identical points) so that
    ``assign_point_to_room`` never receives two ``RoomTag``\\s with the same number.

    Nested detection: a label-only room whose ``location`` falls inside some polygon
    room is flagged; Voronoi assignments to such rooms set ``needs_review=True``.
    """
    polygon_rooms: list[Room] = []
    label_only_rooms: list[Room] = []

    for room in rooms:
        if room.polygon is not None:
            polygon_rooms.append(room)
        elif room.location is not None:
            label_only_rooms.append(room)

    # Dedupe label-only rooms by number (merge anchors, order-preserving).
    deduped_label_rooms = _dedupe_label_rooms_by_number(label_only_rooms)

    # Build Voronoi tags from deduped label-only rooms.
    voronoi_tags: list[RoomTag] = []
    label_room_by_number: dict[str, Room] = {}
    for room in deduped_label_rooms:
        if room.number is None:
            continue
        anchors = room.anchors if room.anchors else ((room.location,) if room.location else ())
        if not anchors:
            continue
        voronoi_tags.append(RoomTag(number=room.number, anchors=tuple(anchors)))
        label_room_by_number[room.number] = room

    # Nested detection: label-only rooms whose location is inside some polygon room.
    nested_numbers: set[str] = set()
    for room in deduped_label_rooms:
        if room.number is None or room.location is None:
            continue
        containing = _smallest_containing_room(room.location, polygon_rooms)
        if containing is not None:
            nested_numbers.add(room.number)

    return RoomRegistry(
        _polygon_rooms=tuple(polygon_rooms),
        _voronoi_tags=tuple(voronoi_tags),
        _nested_numbers=frozenset(nested_numbers),
        _label_room_by_number=label_room_by_number,
        _voronoi_fallback=voronoi_fallback,
        _margin=margin,
        _voronoi_confidence=voronoi_confidence,
    )


def _dedupe_label_rooms_by_number(rooms: Sequence[Room]) -> list[Room]:
    """Merge label-only rooms with the same number, producing a deterministic result.

    Rooms without a number pass through unchanged. For rooms that share a number:
    - The representative room is chosen by lexicographically smallest ``id``
      (stable regardless of input order).
    - Anchors are the union of every member's anchor points, deduplicated and sorted
      by ``(x, y)`` so the merged set is order-invariant.
    - ``needs_review`` is True if any member has it.

    The position slot in the output list is taken by the first-seen room (preserving
    first-appearance output order), but the representative metadata and sorted anchors
    are always deterministic.
    """
    # Group all rooms by number, preserving first-appearance slot order.
    groups: dict[str, list[Room]] = {}
    slot_of_number: dict[str, int] = {}
    result: list[Room] = []

    for room in rooms:
        if room.number is None:
            result.append(room)
            continue
        if room.number not in groups:
            groups[room.number] = [room]
            slot_of_number[room.number] = len(result)
            result.append(room)  # placeholder; replaced below if group grows
        else:
            groups[room.number].append(room)

    # Second pass: for every numbered group, emit a deterministic merged room.
    for number, group in groups.items():
        if len(group) == 1 and not group[0].needs_review:
            # Common case: no duplicate, no change needed.
            continue

        # Representative: lexicographically smallest id for stable identity.
        rep = min(group, key=lambda r: r.id)

        # Collect all anchor points across the group.
        all_anchors: list[tuple[float, float]] = []
        seen_pts: set[tuple[float, float]] = set()
        for room in group:
            pts: tuple[tuple[float, float], ...] = (
                room.anchors if room.anchors else ((room.location,) if room.location else ())
            )
            for pt in pts:
                if pt not in seen_pts:
                    seen_pts.add(pt)
                    all_anchors.append(pt)

        # Sort anchors by (x, y) so the set is input-order-invariant.
        all_anchors.sort(key=lambda p: (p[0], p[1]))

        merged_needs_review = any(r.needs_review for r in group)

        result[slot_of_number[number]] = room_from_label(
            rep.id,
            source=rep.source,
            location=all_anchors[0] if all_anchors else (rep.location or (0.0, 0.0)),
            name=rep.name,
            number=rep.number,
            confidence=rep.confidence,
            anchors=tuple(all_anchors),
            needs_review=merged_needs_review,
        )

    return result
