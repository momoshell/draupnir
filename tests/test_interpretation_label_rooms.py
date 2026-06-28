"""Unit tests for label-derived room identification (#549).

Fixture-driven over RoomLabel; no DB. Covers room-number parsing, name+number
clustering by proximity, and the label-only / name-only / no-label cases.
"""

from __future__ import annotations

import pytest

from app.interpretation.label_rooms import (
    DEFAULT_LABEL_CLUSTER_RADIUS,
    ROOM_SOURCE_LABEL,
    _looks_like_room_name,
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


def test_multi_line_name_joined_top_to_bottom() -> None:
    # "Combined Air" / "Plantroom" / "0.9.05" stacked above the number (the 670003 case):
    # both name lines join into the full name, top line first.
    labels = [
        RoomLabel("Combined Air", (35.88, 20.99), layer="F810A"),
        RoomLabel("Plantroom", (36.04, 20.76), layer="F810A"),
        RoomLabel("0.9.05", (36.23, 20.37), layer="F810A"),
    ]
    rooms = identify_rooms_from_labels(labels)
    room = next(room for room in rooms if room.number == "0.9.05")
    assert room.name == "Combined Air Plantroom"


def test_single_line_name_unaffected_by_multiline_gather() -> None:
    labels = [
        RoomLabel("PH Plantroom", (30.7, 25.3), layer="F810A"),
        RoomLabel("0.9.01", (31.1, 24.9), layer="F810A"),
    ]
    rooms = identify_rooms_from_labels(labels)
    assert next(room for room in rooms if room.number == "0.9.01").name == "PH Plantroom"


def test_side_placed_name_does_not_join_unrelated_lines() -> None:
    # A name far to the side (large |dx|) is NOT part of the stack; the directly-above line wins,
    # and the side name is its own name-only room.
    labels = [
        RoomLabel("Office", (40.0, 20.4), layer="F810A"),  # ~4 units to the side of the number
        RoomLabel("Store", (36.1, 20.7), layer="F810A"),  # directly above the number
        RoomLabel("0.9.07", (36.2, 20.4), layer="F810A"),
    ]
    rooms = identify_rooms_from_labels(labels)
    by_number = {room.number: room.name for room in rooms}
    assert by_number["0.9.07"] == "Store"  # the aligned line, not "Office"
    assert any(room.name == "Office" and room.number is None for room in rooms)


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


# ---------------------------------------------------------------------------
# Content filter: _looks_like_room_name (#771)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        "Cooling Plantroom",
        "Heating & Hot Water",
        "Life Safety & Essential",
        "UPS LV plantroom",
        "Combined Air Plantroom",
        "PH Plantroom",
        "Bottle Store",
        "Corridor",
        "AHU Plant",
        "Office",
    ],
)
def test_looks_like_room_name_accepts_real_names(text: str) -> None:
    assert _looks_like_room_name(text) is True


@pytest.mark.parametrize(
    "text",
    [
        "Ø250 mm LOW LESS HEADER",
        "Ø250 LOW LESS HEADER",
        "6No. OUTDOOR DX UNITS",
        "FROM TOP TO BOTTOM TB :",
        "TOP TO BOTTOM :",
        "100 mmx50 mm REFRIGERANT TRAY",
        "FROM TOP TO BOTTOM",
        "TO BOTTOM",
        "100mm",
        "250 MM",
        "100 x 50",
        "100X50",
        "6No. SOMETHING",
        "LOW LOSS HEADER",
        "LOW LESS HEADER",
        "HEADER",
        "FROM TOP",
        "TB",
        ":",
        "12345",
        "",
        "   ",
    ],
)
def test_looks_like_room_name_rejects_annotations(text: str) -> None:
    assert _looks_like_room_name(text) is False


def test_annotation_note_not_emitted_as_name_only_room() -> None:
    """Note-only labels (no number nearby) must not become name-only rooms (#771)."""
    labels = [
        RoomLabel("Cooling Plantroom", (10.0, 10.0)),
        RoomLabel("Ø250 mm LOW LESS HEADER", (10.0, 9.5)),
        RoomLabel("6No. OUTDOOR DX UNITS", (10.0, 9.0)),
        RoomLabel("FROM TOP TO BOTTOM TB :", (10.0, 8.5)),
        RoomLabel("100 mmx50 mm REFRIGERANT TRAY", (10.0, 8.0)),
    ]
    rooms = identify_rooms_from_labels(labels)
    names = [room.name for room in rooms]
    assert "Cooling Plantroom" in names
    assert not any(
        n for n in names if n and ("Ø" in n or "No." in n or "FROM TOP" in n or "mm" in n.lower())
    )


def test_annotation_note_not_paired_as_numbered_room_name() -> None:
    """An annotation note near a room number must not become that room's name (#771).

    The room (number) should still be created but with name=None.
    """
    labels = [
        RoomLabel("1.9.02", (10.0, 10.0)),
        RoomLabel("Ø250 mm LOW LESS HEADER", (10.0, 10.4)),  # within cluster radius, above
    ]
    rooms = identify_rooms_from_labels(labels)
    assert len(rooms) == 1
    room = rooms[0]
    assert room.number == "1.9.02"
    assert room.name is None  # note text dropped, not attached


def test_real_room_name_still_paired_with_number_when_note_nearby() -> None:
    """Real name wins over a nearby note; note is discarded, not surfaced (#771)."""
    labels = [
        RoomLabel("Cooling Plantroom", (10.0, 10.4)),  # real name above number
        RoomLabel("1.9.02", (10.0, 10.0)),
        RoomLabel("Ø250 mm LOW LESS HEADER", (10.0, 9.6)),  # note below/side — annotation
    ]
    rooms = identify_rooms_from_labels(labels)
    numbered = next(room for room in rooms if room.number == "1.9.02")
    assert numbered.name == "Cooling Plantroom"
    # The note must not surface as a name-only room
    assert not any(room.name and "Ø" in room.name for room in rooms)


def test_name_only_drawing_real_names_still_yield_rooms() -> None:
    """When all labels are names (no numbers), name-only rooms still work (#549/#662)."""
    labels = [
        RoomLabel("Cooling Plantroom", (1.0, 1.0)),
        RoomLabel("Heating & Hot Water", (5.0, 5.0)),
        RoomLabel("UPS LV plantroom", (9.0, 9.0)),
    ]
    rooms = identify_rooms_from_labels(labels)
    assert len(rooms) == 3
    names = {room.name for room in rooms}
    assert names == {"Cooling Plantroom", "Heating & Hot Water", "UPS LV plantroom"}
    assert all(room.number is None for room in rooms)


def test_output_is_deterministic() -> None:
    """Same input always yields same output with the content filter active (#771)."""
    labels = [
        RoomLabel("Cooling Plantroom", (10.0, 10.4)),
        RoomLabel("1.9.02", (10.0, 10.0)),
        RoomLabel("Ø250 mm LOW LESS HEADER", (10.0, 9.6)),
        RoomLabel("Life Safety & Essential", (20.0, 20.0)),
        RoomLabel("1.9.03", (20.0, 19.6)),
    ]
    first = identify_rooms_from_labels(labels)
    second = identify_rooms_from_labels(labels)
    assert first == second
