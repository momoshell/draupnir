"""Unit tests for PDF annotation-zone exclusion (#828 PR-2).

Pure / fixture-driven over RoomLabel; no DB. Covers zone discovery (legend/key/notes/
title-block anchors + grid-margin band) and label filtering (kept vs excluded-with-reason).
Conservative by design: a genuine interior room label must never fall in a discovered zone.
"""

from __future__ import annotations

from app.interpretation.annotation_zones import (
    ZONE_REASON_GRID_MARGIN,
    ZONE_REASON_KEY,
    ZONE_REASON_LEGEND,
    ZONE_REASON_NOTES,
    discover_zones,
    filter_labels_by_zones,
)
from app.interpretation.rooms import RoomLabel


def test_legend_anchor_creates_legend_zone() -> None:
    labels = [RoomLabel("LEGEND", (10.0, 500.0))]
    zones = discover_zones(labels, sheet_extent=None)
    assert len(zones) == 1
    assert zones[0].reason == ZONE_REASON_LEGEND


def test_drainage_key_anchor_creates_key_zone() -> None:
    labels = [RoomLabel("DRAINAGE KEY", (10.0, 500.0))]
    zones = discover_zones(labels, sheet_extent=None)
    assert len(zones) == 1
    assert zones[0].reason == ZONE_REASON_KEY


def test_key_plan_is_not_a_key_anchor() -> None:
    # "KEY PLAN" is a callout, not the drainage/service key block — must not zone.
    labels = [RoomLabel("KEY PLAN", (10.0, 500.0))]
    zones = discover_zones(labels, sheet_extent=None)
    assert zones == []


def test_notes_anchor_creates_notes_zone() -> None:
    labels = [RoomLabel("NOTES", (10.0, 500.0))]
    zones = discover_zones(labels, sheet_extent=None)
    assert len(zones) == 1
    assert zones[0].reason == ZONE_REASON_NOTES


def test_grid_margin_band_from_sheet_extent() -> None:
    zones = discover_zones([], sheet_extent=(0.0, 0.0, 1000.0, 800.0))
    assert len(zones) == 4
    assert all(zone.reason == ZONE_REASON_GRID_MARGIN for zone in zones)


def test_no_sheet_extent_skips_grid_margin() -> None:
    zones = discover_zones([], sheet_extent=None)
    assert zones == []


def test_filter_keeps_interior_labels_and_excludes_zoned_ones() -> None:
    legend_label = RoomLabel("SVP SOIL VENT PIPE", (12.0, 502.0))
    room_label = RoomLabel("0.2.07", (300.0, 300.0))
    labels = [
        RoomLabel("LEGEND", (10.0, 500.0)),
        legend_label,
        room_label,
    ]
    zones = discover_zones(labels, sheet_extent=None)
    kept, excluded = filter_labels_by_zones(labels, zones)
    assert room_label in kept
    excluded_texts = {item.label.text for item in excluded}
    assert "LEGEND" in excluded_texts
    assert "SVP SOIL VENT PIPE" in excluded_texts
    assert all(item.reason == ZONE_REASON_LEGEND for item in excluded)


def test_grid_bubble_near_border_excluded_but_interior_room_kept() -> None:
    sheet_extent = (0.0, 0.0, 1000.0, 800.0)
    grid_bubble = RoomLabel("1", (5.0, 400.0))  # inside the 5% left margin band
    interior_room = RoomLabel("MRI Scanner Rm 1", (500.0, 400.0))  # deep interior
    labels = [grid_bubble, interior_room]
    zones = discover_zones(labels, sheet_extent)
    kept, excluded = filter_labels_by_zones(labels, zones)
    assert interior_room in kept
    assert any(item.label is grid_bubble for item in excluded)


def test_room_label_just_outside_margin_band_is_kept_conservatism() -> None:
    # A genuine room label near, but not inside, the border band must never be excluded.
    sheet_extent = (0.0, 0.0, 1000.0, 800.0)
    # 5% of width = 50; place just past the margin boundary.
    near_border_room = RoomLabel("0.1.02", (55.0, 400.0))
    zones = discover_zones([near_border_room], sheet_extent)
    kept, excluded = filter_labels_by_zones([near_border_room], zones)
    assert near_border_room in kept
    assert excluded == []
