"""Unit tests for pure room-geometry primitives (issue #460).

Coordinate-fixture driven; no DB, no mocks.
"""

from __future__ import annotations

from app.interpretation.geometry import (
    bounding_box,
    build_polygon,
    point_in_polygon,
    polygon_area,
    polygonize,
    smallest_containing,
)

# A 10x10 square at the origin.
SQUARE = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]
# A 2x2 square nested fully inside SQUARE.
INNER = [(4.0, 4.0), (6.0, 4.0), (6.0, 6.0), (4.0, 6.0)]


# --- build_polygon ---


def test_build_polygon_from_open_ring() -> None:
    polygon = build_polygon(SQUARE)
    assert polygon is not None
    assert polygon_area(polygon) == 100.0


def test_build_polygon_accepts_explicitly_closed_ring() -> None:
    closed = [*SQUARE, (0.0, 0.0)]
    polygon = build_polygon(closed)
    assert polygon is not None
    assert polygon_area(polygon) == 100.0


def test_build_polygon_too_few_vertices_returns_none() -> None:
    assert build_polygon([(0.0, 0.0), (1.0, 1.0)]) is None


def test_build_polygon_collinear_zero_area_returns_none() -> None:
    assert build_polygon([(0.0, 0.0), (1.0, 0.0), (2.0, 0.0)]) is None


def test_build_polygon_duplicate_vertices_collapse_to_degenerate() -> None:
    # Three points but two coincide -> effectively two distinct -> None.
    assert build_polygon([(0.0, 0.0), (0.0, 0.0), (1.0, 1.0)]) is None


def test_build_polygon_repairs_invalid_ring_with_spike() -> None:
    # A square with a zero-width spike appendage is invalid; buffer(0) repairs it
    # into a single valid polygon (the spike is dropped).
    spike = [
        (0.0, 0.0),
        (10.0, 0.0),
        (10.0, 10.0),
        (0.0, 10.0),
        (0.0, 5.0),
        (-3.0, 5.0),
        (0.0, 5.0),
    ]
    polygon = build_polygon(spike)
    assert polygon is not None
    assert polygon.is_valid
    assert polygon_area(polygon) == 100.0


def test_build_polygon_self_canceling_bowtie_returns_none() -> None:
    # A bow-tie's signed area cancels to zero, so it is not a single room.
    assert build_polygon([(0.0, 0.0), (2.0, 2.0), (2.0, 0.0), (0.0, 2.0)]) is None


# --- point_in_polygon ---


def test_point_inside_square() -> None:
    polygon = build_polygon(SQUARE)
    assert polygon is not None
    assert point_in_polygon((5.0, 5.0), polygon) is True


def test_point_outside_square() -> None:
    polygon = build_polygon(SQUARE)
    assert polygon is not None
    assert point_in_polygon((50.0, 50.0), polygon) is False


def test_point_on_boundary_is_contained() -> None:
    polygon = build_polygon(SQUARE)
    assert polygon is not None
    assert point_in_polygon((0.0, 5.0), polygon) is True


# --- smallest_containing (nesting) ---


def test_smallest_containing_picks_innermost() -> None:
    outer = build_polygon(SQUARE)
    inner = build_polygon(INNER)
    assert outer is not None and inner is not None
    chosen = smallest_containing((5.0, 5.0), [outer, inner])
    assert chosen is inner


def test_smallest_containing_order_independent() -> None:
    outer = build_polygon(SQUARE)
    inner = build_polygon(INNER)
    assert outer is not None and inner is not None
    # Innermost wins regardless of input order.
    assert smallest_containing((5.0, 5.0), [inner, outer]) is inner


def test_smallest_containing_falls_back_to_outer_when_only_outer_contains() -> None:
    outer = build_polygon(SQUARE)
    inner = build_polygon(INNER)
    assert outer is not None and inner is not None
    # A point inside the outer square but outside the inner square.
    assert smallest_containing((1.0, 1.0), [outer, inner]) is outer


def test_smallest_containing_returns_none_when_uncontained() -> None:
    outer = build_polygon(SQUARE)
    assert outer is not None
    assert smallest_containing((99.0, 99.0), [outer]) is None


def test_smallest_containing_empty_polygons_returns_none() -> None:
    assert smallest_containing((0.0, 0.0), []) is None


# --- polygonize ---


def _square_edges(
    square: list[tuple[float, float]],
) -> list[tuple[tuple[float, float], tuple[float, float]]]:
    closed = [*square, square[0]]
    return [(closed[i], closed[i + 1]) for i in range(len(square))]


def test_polygonize_recovers_square_face_from_edges() -> None:
    faces = polygonize(_square_edges(SQUARE))
    assert len(faces) == 1
    assert polygon_area(faces[0]) == 100.0


def test_polygonize_open_linework_yields_no_face() -> None:
    # Three sides of a square -> not closed -> no face.
    edges = _square_edges(SQUARE)[:-1]
    assert polygonize(edges) == []


def test_polygonize_ignores_zero_length_segments() -> None:
    edges = [*_square_edges(SQUARE), ((1.0, 1.0), (1.0, 1.0))]
    faces = polygonize(edges)
    assert len(faces) == 1


def test_polygonize_empty_input() -> None:
    assert polygonize([]) == []


# --- bounding_box ---


def test_bounding_box_of_square() -> None:
    polygon = build_polygon(SQUARE)
    assert polygon is not None
    assert bounding_box(polygon) == (0.0, 0.0, 10.0, 10.0)
