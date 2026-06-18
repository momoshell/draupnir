"""Unit tests for canonical entity bbox extraction (no DB)."""

from __future__ import annotations

from app.ingestion.entity_geometry import compute_entity_bbox


def test_line_start_end() -> None:
    assert compute_entity_bbox({"start": {"x": 0, "y": 1}, "end": {"x": 4, "y": -2}}) == (
        0.0,
        -2.0,
        4.0,
        1.0,
    )


def test_polyline_points_and_vertices() -> None:
    geo = {"points": [[0, 0], [3, 5], [1, 2]]}
    assert compute_entity_bbox(geo) == (0.0, 0.0, 3.0, 5.0)
    assert compute_entity_bbox({"vertices": [{"x": -1, "y": 9}, {"x": 2, "y": 4}]}) == (
        -1.0,
        4.0,
        2.0,
        9.0,
    )


def test_single_point_keys() -> None:
    assert compute_entity_bbox({"position": {"x": 7, "y": 8}}) == (7.0, 8.0, 7.0, 8.0)


def test_circle_uses_radius_extent() -> None:
    assert compute_entity_bbox({"center": {"x": 10, "y": 10}, "radius": 4}) == (
        6.0,
        6.0,
        14.0,
        14.0,
    )


def test_summary_bbox_fallback_dict_and_list() -> None:
    assert compute_entity_bbox(
        {"geometry_summary": {"bbox": {"min_x": 1, "min_y": 2, "max_x": 3, "max_y": 4}}}
    ) == (1.0, 2.0, 3.0, 4.0)
    assert compute_entity_bbox({"geometry_summary": {"bbox": [1, 2, 3, 4]}}) == (1.0, 2.0, 3.0, 4.0)


def test_none_for_unparseable_or_extentless() -> None:
    assert compute_entity_bbox(None) is None
    assert compute_entity_bbox({}) is None
    assert compute_entity_bbox({"start": {"x": "nan"}}) is None
    assert compute_entity_bbox({"points": [[float("inf"), 0]]}) is None
    assert compute_entity_bbox({"label": "TAG-1"}) is None
