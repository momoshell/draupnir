"""Rooms identified from label clusters — name + number, independent of polygons (#549).

Real drawings are fully labeled even when their wall linework won't polygonize: a room
name (``PH Plantroom``) sits beside a room-number token (``0.9.01``) as two separate text
entities ~one text-height apart. We already extract all this text; here we pair each number
with its nearest name to recover a room identity (name + number + location) with no boundary.

When polygons exist, these identities enrich them by containment (handled in the pipeline);
when they don't, the labeled rooms are surfaced directly. Pure over :class:`RoomLabel`.
"""

from __future__ import annotations

import math
from collections.abc import Sequence

from app.interpretation.rooms import Room, RoomLabel, parse_room_number, room_from_label

ROOM_SOURCE_LABEL = "label_cluster"

# A number label is matched to the nearest name label within this distance (drawing units).
# Sized for the typical name-above-number tag stack (~one text height, ~0.5 m on 1:50 sheets).
DEFAULT_LABEL_CLUSTER_RADIUS = 1.5

# Heuristic confidence for a label-only room (no geometry to corroborate the identity).
LABEL_ROOM_CONFIDENCE = 0.7


def room_label_layers(labels: Sequence[RoomLabel]) -> set[str | None] | None:
    """Layers that carry a room-number token — the room-label layers — or None to not restrict.

    Returns ``None`` (no restriction) when no label carries a number, so drawings/fixtures
    with name-only labels are unaffected. When numbers exist, restricting to their layers
    keeps room names + numbers (on the room-label layer) and drops device-tag text elsewhere.
    """
    number_layers = {label.layer for label in labels if parse_room_number(label.text)}
    return number_layers or None


def identify_rooms_from_labels(
    labels: Sequence[RoomLabel],
    *,
    cluster_radius: float = DEFAULT_LABEL_CLUSTER_RADIUS,
) -> list[Room]:
    """Pair room-number labels with their nearest name label into label-derived rooms.

    Restricts to the room-label layers (those bearing a number) so device-tag text on other
    layers can't masquerade as room names. Each number token (``parse_room_number``) is then
    matched to the closest non-number label within ``cluster_radius`` for its name. A label
    that itself carries both a name and a number (a multi-line tag) becomes a room on its own.
    Name labels with no number nearby are still surfaced (name-only). Deterministic: ties break
    by input order, and output is ordered by (number, name) so synthesized ids are stable.
    """
    allowed = room_label_layers(labels)
    if allowed is not None:
        labels = [label for label in labels if label.layer in allowed]

    numbers: list[tuple[str, RoomLabel]] = []
    names: list[RoomLabel] = []
    inline: list[Room] = []
    for label in labels:
        number = parse_room_number(label.text)
        residual = _strip_number(label.text, number) if number else label.text
        if number is not None and residual:
            # A single tag carrying both name and number.
            inline.append(
                room_from_label(
                    "",
                    source=ROOM_SOURCE_LABEL,
                    location=label.point,
                    name=residual,
                    number=number,
                    confidence=LABEL_ROOM_CONFIDENCE,
                )
            )
        elif number is not None:
            numbers.append((number, label))
        elif label.text.strip():
            names.append(label)

    used_names: set[int] = set()
    paired: list[Room] = []
    for number, number_label in numbers:
        name_label, name_index = _nearest_name(number_label, names, used_names, cluster_radius)
        if name_index is not None:
            used_names.add(name_index)
        paired.append(
            room_from_label(
                "",
                source=ROOM_SOURCE_LABEL,
                location=number_label.point,
                name=name_label.text.strip() if name_label is not None else None,
                number=number,
                confidence=LABEL_ROOM_CONFIDENCE,
            )
        )

    name_only = [
        room_from_label(
            "",
            source=ROOM_SOURCE_LABEL,
            location=label.point,
            name=label.text.strip(),
            confidence=LABEL_ROOM_CONFIDENCE,
        )
        for index, label in enumerate(names)
        if index not in used_names
    ]

    rooms = [*inline, *paired, *name_only]
    rooms.sort(key=lambda room: (room.number or "~", room.name or "~", room.location or (0.0, 0.0)))
    return [
        room_from_label(
            f"label-room-{index}",
            source=room.source,
            location=room.location if room.location is not None else (0.0, 0.0),
            name=room.name,
            number=room.number,
            confidence=room.confidence,
        )
        for index, room in enumerate(rooms)
    ]


def _nearest_name(
    number_label: RoomLabel,
    names: Sequence[RoomLabel],
    used: set[int],
    radius: float,
) -> tuple[RoomLabel | None, int | None]:
    best: tuple[float, int] | None = None
    nx, ny = number_label.point
    for index, name_label in enumerate(names):
        if index in used:
            continue
        distance = math.hypot(name_label.point[0] - nx, name_label.point[1] - ny)
        if distance <= radius and (best is None or distance < best[0]):
            best = (distance, index)
    if best is None:
        return None, None
    return names[best[1]], best[1]


def _strip_number(text: str, number: str) -> str:
    return text.replace(number, "").strip()
