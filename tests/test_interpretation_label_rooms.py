"""Unit tests for label-derived room identification (#549).

Fixture-driven over RoomLabel; no DB. Covers room-number parsing, name+number
clustering by proximity, and the label-only / name-only / no-label cases.
"""

from __future__ import annotations

import pytest

from app.interpretation.label_rooms import (
    DEFAULT_LABEL_CLUSTER_RADIUS,
    ROOM_SOURCE_LABEL,
    identify_rooms_from_labels,
)
from app.interpretation.rooms import RoomLabel, parse_room_number


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("0.9.01", "0.9.01"),
        ("PH Plantroom\n0.9.01", "0.9.01"),
        ("Room G.04", "G.04"),
        ("A1.02", "A1.02"),
        ("Plantroom", None),
        ("Level 9", None),
        ("", None),
    ],
)
def test_parse_room_number(text: str, expected: str | None) -> None:
    assert parse_room_number(text) == expected


def test_pairs_name_with_nearest_number() -> None:
    # name above its number (~0.4 m apart), as on real tag stacks
    labels = [
        RoomLabel("PH Plantroom", (30.7, 25.3)),
        RoomLabel("0.9.01", (31.1, 24.9)),
        RoomLabel("Bottle Store", (28.6, 28.8)),
        RoomLabel("0.9.06", (28.8, 28.5)),
    ]
    rooms = identify_rooms_from_labels(labels)
    by_number = {room.number: room.name for room in rooms}
    assert by_number == {"0.9.01": "PH Plantroom", "0.9.06": "Bottle Store"}
    assert all(room.source == ROOM_SOURCE_LABEL for room in rooms)
    assert all(room.polygon is None and room.location is not None for room in rooms)


def test_duplicate_numbers_become_distinct_rooms() -> None:
    # three "AHU Plant / 0.9.04" stacks at different locations → three rooms
    labels = [
        RoomLabel("AHU Plant", (19.0, 23.7)),
        RoomLabel("0.9.04", (19.2, 23.3)),
        RoomLabel("AHU Plant", (46.1, 21.2)),
        RoomLabel("0.9.04", (46.3, 20.9)),
        RoomLabel("AHU Plant", (33.2, 17.5)),
        RoomLabel("0.9.04", (33.4, 17.1)),
    ]
    rooms = identify_rooms_from_labels(labels)
    assert len(rooms) == 3
    assert all(room.name == "AHU Plant" and room.number == "0.9.04" for room in rooms)
    # distinct locations, distinct ids
    assert len({room.location for room in rooms}) == 3
    assert len({room.id for room in rooms}) == 3


def test_inline_name_and_number_in_one_label() -> None:
    rooms = identify_rooms_from_labels([RoomLabel("PH Plantroom 0.9.01", (5.0, 5.0))])
    assert len(rooms) == 1
    assert rooms[0].name == "PH Plantroom"
    assert rooms[0].number == "0.9.01"


def test_name_without_nearby_number_is_name_only() -> None:
    labels = [
        RoomLabel("Corridor", (0.0, 0.0)),
        RoomLabel("0.9.99", (100.0, 100.0)),  # far away — not this name's number
    ]
    rooms = identify_rooms_from_labels(labels)
    corridor = next(room for room in rooms if room.name == "Corridor")
    assert corridor.number is None
    # the distant number is its own (name-less) room
    numbered = next(room for room in rooms if room.number == "0.9.99")
    assert numbered.name is None


def test_number_beyond_radius_not_paired() -> None:
    radius = DEFAULT_LABEL_CLUSTER_RADIUS
    labels = [
        RoomLabel("Office", (0.0, 0.0)),
        RoomLabel("1.2.03", (0.0, radius + 1.0)),
    ]
    rooms = identify_rooms_from_labels(labels)
    numbered = next(room for room in rooms if room.number == "1.2.03")
    assert numbered.name is None


def test_no_labels_yields_no_rooms() -> None:
    assert identify_rooms_from_labels([]) == []
