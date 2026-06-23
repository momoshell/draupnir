"""Pure unit tests for the centerline contract geometry (de)serialization (#653).

No DB, no cv2/skimage — the contract module must stay importable on the read path.
"""

from __future__ import annotations

from app.ingestion.centerline_contract import (
    CenterlineGeometry,
    geometry_to_json,
    polylines_from_geometry_json,
)


def test_geometry_to_json_none_when_empty() -> None:
    """A geometry with no polylines (e.g. passthrough producer) serializes to None."""
    geom = CenterlineGeometry(polylines=(), length_du=12.5)
    assert geometry_to_json(geom) is None


def test_geometry_to_json_rounds_and_shapes_payload() -> None:
    geom = CenterlineGeometry(
        polylines=(((0.0, 0.0), (10.123456, 0.0)), ((5.0, 5.0), (5.0, 9.0))),
        length_du=14.123456,
    )
    payload = geometry_to_json(geom)
    assert payload == {
        "schema_version": "0.1",
        "polylines": [[[0.0, 0.0], [10.123, 0.0]], [[5.0, 5.0], [5.0, 9.0]]],
    }


def test_round_trip_preserves_polylines() -> None:
    geom = CenterlineGeometry(
        polylines=(((0.0, 0.0), (10.0, 0.0), (10.0, 5.0)),),
        length_du=15.0,
    )
    restored = polylines_from_geometry_json(geometry_to_json(geom))
    assert restored == (((0.0, 0.0), (10.0, 0.0), (10.0, 5.0)),)


def test_polylines_from_geometry_json_defensive() -> None:
    # Absent / wrong-typed payloads -> empty.
    assert polylines_from_geometry_json(None) == ()
    assert polylines_from_geometry_json("nope") == ()
    assert polylines_from_geometry_json({}) == ()
    assert polylines_from_geometry_json({"polylines": "bad"}) == ()
    # A polyline with < 2 points is dropped (a clip needs segments); short coords skipped.
    assert polylines_from_geometry_json({"polylines": [[[1.0, 2.0]]]}) == ()
    assert polylines_from_geometry_json(
        {"polylines": [[[1.0, 2.0], [3.0]], [[0.0, 0.0], [1.0, 1.0]]]}
    ) == (((0.0, 0.0), (1.0, 1.0)),)
