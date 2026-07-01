"""Room interpretation strategy selection (issue #479).

A thin pure coordinator over the three room sources, in descending authority:
- IFC spaces (#429), the definitive native source when present,
- explicit room/space-boundary layer (#427), and
- wall-linework polygonization (#428) as the fallback.

``interpret_rooms`` picks the strategy (``auto`` by default) and returns a unified
result. Pure over the :class:`EntityRow` protocol and the R0 primitives, so it is
unit-testable with fixtures; the route does the DB loading and adaptation.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace

from app.interpretation.annotation_zones import discover_zones, filter_labels_by_zones
from app.interpretation.explicit_rooms import interpret_explicit_rooms
from app.interpretation.geometry import point_in_polygon
from app.interpretation.ifc_rooms import interpret_ifc_rooms
from app.interpretation.label_rooms import identify_rooms_from_labels, room_label_layers
from app.interpretation.models import EntityRow
from app.interpretation.rooms import (
    DevicePlacement,
    DeviceRoomAssignment,
    Room,
    RoomLabel,
    assign_devices_to_label_rooms,
    dedupe_label_rooms_by_number,
)
from app.interpretation.wall_rooms import (
    DEFAULT_WALL_MIN_AREA,
    DEFAULT_WALL_SNAP_TOLERANCE,
    interpret_wall_rooms,
)

# PDF has no sheet-scoping mechanism (tag_sheet_membership is a DWG-only no-op on PDF), so
# every room-name-ish text token becomes a room candidate unless annotation zones (legend,
# key, notes, title-block, grid margin) are masked out first (#828 PR-2). DWG (and any other
# family) skips this pass entirely — byte-identical output.
INPUT_FAMILY_PDF_VECTOR = "pdf_vector"

ROOM_STRATEGY_AUTO = "auto"
ROOM_STRATEGY_IFC = "ifc_space"
ROOM_STRATEGY_EXPLICIT = "explicit_layer"
ROOM_STRATEGY_WALLS = "wall_polygonize"

ROOM_STRATEGIES = (
    ROOM_STRATEGY_AUTO,
    ROOM_STRATEGY_IFC,
    ROOM_STRATEGY_EXPLICIT,
    ROOM_STRATEGY_WALLS,
)

# Heuristic max distance from a device to a label-only room's point for nearest-room
# assignment (#555). Generous, since a polygon-less room has no boundary to contain points.
DEFAULT_DEVICE_LABEL_ROOM_RADIUS = 8.0


@dataclass(frozen=True, slots=True)
class RoomInterpretation:
    """Unified room interpretation result with the strategy that produced it."""

    rooms: list[Room]
    device_assignments: list[DeviceRoomAssignment]
    strategy: str
    source_layers: tuple[str, ...]


def interpret_rooms(
    entities: Sequence[EntityRow],
    *,
    devices: Sequence[DevicePlacement],
    labels: Sequence[RoomLabel],
    strategy: str = ROOM_STRATEGY_AUTO,
    snap_tolerance: float = 0.0,
    min_area: float = 0.0,
    input_family: str | None = None,
) -> RoomInterpretation:
    """Interpret rooms by geometry, then enrich + supplement with label identities (#549).

    The geometric strategy (``auto``/``ifc_space``/``explicit_layer``/``wall_polygonize``)
    produces polygon rooms; label clusters (name + room-number, e.g. ``PH Plantroom`` +
    ``0.9.01``) then (a) stamp name/number onto the polygon room that contains them and
    (b) surface as label-only rooms when no polygon contains them — so a fully-labeled but
    un-polygonizable drawing still yields named, numbered rooms.

    ``input_family`` gates the PDF-only annotation-zone exclusion pass (#828 PR-2): on
    ``pdf_vector`` revisions, labels landing in a discovered legend/key/notes/title-block/
    grid-margin zone are excluded from room-label candidacy before scoping and label
    identification. Any other family (including ``None``, unknown origin) skips this pass —
    DWG stays byte-identical.
    """
    if input_family == INPUT_FAMILY_PDF_VECTOR:
        sheet_extent = _label_bounds(labels)
        zones = discover_zones(labels, sheet_extent)
        labels, _excluded = filter_labels_by_zones(labels, zones)

    # Scope to the room-label layer(s) (those bearing a room number) when present, so polygon
    # naming and label-room identification both ignore device-tag text on other layers (#549).
    allowed = room_label_layers(labels)
    room_labels = (
        labels if allowed is None else [label for label in labels if label.layer in allowed]
    )
    base = _interpret_geometric(
        entities,
        devices=devices,
        labels=room_labels,
        strategy=strategy,
        snap_tolerance=snap_tolerance,
        min_area=min_area,
    )
    enriched = _enrich_with_labels(base, room_labels)
    # Devices land in their containing polygon room (done by the strategy); any left over are
    # attached to the nearest label-only room — the no-polygon case (#555).
    extra = assign_devices_to_label_rooms(
        devices,
        enriched.rooms,
        already_assigned={a.device_id for a in enriched.device_assignments},
        radius=DEFAULT_DEVICE_LABEL_ROOM_RADIUS,
    )
    if not extra:
        return enriched
    return replace(enriched, device_assignments=[*enriched.device_assignments, *extra])


def _enrich_with_labels(
    base: RoomInterpretation, labels: Sequence[RoomLabel]
) -> RoomInterpretation:
    """Stamp label name/number onto containing polygon rooms, flag cross-wall duplicate numbers,
    and append unmatched label identities — deduped by number into one room each (#549, #581)."""
    identities = identify_rooms_from_labels(labels)
    if not identities:
        return base

    rooms = list(base.rooms)
    # Resolve containment ONCE against the original geometry (stamping below changes only
    # name/number, never the polygon, so containment is unaffected).
    pairs = [
        (
            identity,
            _containing_polygon_room(identity.location, rooms)
            if identity.location is not None
            else None,
        )
        for identity in identities
    ]

    # (a) Stamp name/number onto containing polygon rooms (first identity per room wins). Keyed
    # by room id so multiple identities landing in one room can't trip object-identity lookups.
    stamp: dict[str, tuple[str | None, str | None]] = {}
    for identity, target in pairs:
        if target is not None and target.id not in stamp:
            stamp[target.id] = (identity.name, identity.number)
    rooms = [
        replace(
            room,
            name=room.name if room.name is not None else stamp[room.id][0],
            number=room.number if room.number is not None else stamp[room.id][1],
        )
        if room.id in stamp
        else room
        for room in rooms
    ]

    # (b) Wall-aware: a number whose labels land in >=2 distinct polygon rooms is a cross-wall
    # duplicate (a likely numbering error) — flag those rooms for review, never silently merge.
    polygons_by_number: dict[str, set[str]] = {}
    for identity, target in pairs:
        if identity.number is not None and target is not None:
            polygons_by_number.setdefault(identity.number, set()).add(target.id)
    flagged = {number for number, ids in polygons_by_number.items() if len(ids) > 1}
    if flagged:
        rooms = [replace(r, needs_review=True) if r.number in flagged else r for r in rooms]

    # (c) Label-only identities: drop those whose number a polygon room already carries, then
    # merge same-number placements into one room with all anchors (#581 — U-shape).
    numbered_polygons = {r.number for r in rooms if r.polygon is not None and r.number is not None}
    label_only = [
        identity
        for identity, target in pairs
        if target is None and (identity.number is None or identity.number not in numbered_polygons)
    ]
    merged = dedupe_label_rooms_by_number(label_only)
    if not merged:
        return replace(base, rooms=rooms)
    # Re-id the appended label rooms so ids stay unique + stable within the merged list.
    appended = [replace(room, id=f"label-room-{index}") for index, room in enumerate(merged)]
    return replace(base, rooms=[*rooms, *appended])


def _label_bounds(labels: Sequence[RoomLabel]) -> tuple[float, float, float, float] | None:
    """Derive a sheet-extent proxy from the label point cloud, or ``None`` when too sparse.

    No canonical page-size metadata is available to this pure pipeline, so the label bounds
    stand in for the sheet extent (grid bubbles + genuine rooms are both text, so the outer
    ring of the label cloud approximates the printed sheet border). Requires at least 2 labels
    so a degenerate single-point bound (zero width/height) can't produce a spurious margin.
    """
    if len(labels) < 2:
        return None
    xs = [label.point[0] for label in labels]
    ys = [label.point[1] for label in labels]
    return (min(xs), min(ys), max(xs), max(ys))


def _containing_polygon_room(point: tuple[float, float], rooms: Sequence[Room]) -> Room | None:
    """Smallest polygon room containing ``point`` (label-only rooms have no polygon)."""
    containing = [
        room
        for room in rooms
        if room.polygon is not None
        and room.area is not None
        and point_in_polygon(point, room.polygon)
    ]
    if not containing:
        return None
    return min(containing, key=lambda room: room.area if room.area is not None else 0.0)


def _interpret_geometric(
    entities: Sequence[EntityRow],
    *,
    devices: Sequence[DevicePlacement],
    labels: Sequence[RoomLabel],
    strategy: str = ROOM_STRATEGY_AUTO,
    snap_tolerance: float = 0.0,
    min_area: float = 0.0,
) -> RoomInterpretation:
    """Geometric room sources only (IFC / explicit / wall), in descending authority."""
    if strategy in (ROOM_STRATEGY_AUTO, ROOM_STRATEGY_IFC):
        ifc = interpret_ifc_rooms(entities, devices=devices, labels=labels)
        if ifc.rooms or strategy == ROOM_STRATEGY_IFC:
            return RoomInterpretation(
                rooms=ifc.rooms,
                device_assignments=ifc.device_assignments,
                strategy=ROOM_STRATEGY_IFC,
                source_layers=(),
            )

    if strategy in (ROOM_STRATEGY_AUTO, ROOM_STRATEGY_EXPLICIT):
        explicit = interpret_explicit_rooms(entities, devices=devices, labels=labels)
        if explicit.rooms or strategy == ROOM_STRATEGY_EXPLICIT:
            return RoomInterpretation(
                rooms=explicit.rooms,
                device_assignments=explicit.device_assignments,
                strategy=ROOM_STRATEGY_EXPLICIT,
                source_layers=explicit.boundary_layers,
            )

    # Real drawings need gap-closing + sliver-dropping to polygonize cleanly; when the caller
    # didn't specify, fall back to the validated robust defaults (#554) rather than 0 (which
    # yields no rooms on Revit-style exports). Explicit non-zero caller values still win.
    walls = interpret_wall_rooms(
        entities,
        devices=devices,
        labels=labels,
        snap_tolerance=snap_tolerance or DEFAULT_WALL_SNAP_TOLERANCE,
        min_area=min_area or DEFAULT_WALL_MIN_AREA,
    )
    return RoomInterpretation(
        rooms=walls.rooms,
        device_assignments=walls.device_assignments,
        strategy=ROOM_STRATEGY_WALLS,
        source_layers=walls.wall_layers,
    )
