"""Pure unit tests for the centerline contract geometry (de)serialization (#653).

No DB, no cv2/skimage — the contract module must stay importable on the read path.
"""

from __future__ import annotations

from typing import Any

from app.ingestion.centerline_contract import (
    CenterlineGeometry,
    decompose_geometry,
    entity_group_drawn_length,
    geometry_to_json,
    polylines_from_geometry_json,
)

# --- dict-coord regression (#real-data: materialized geometry uses {"x","y","z"}) ---------


def test_length_and_decompose_handle_dict_coords() -> None:
    """Real materialized geometry uses dict coords {"x","y","z"}, not lists [x,y].

    Regression for the KeyError that broke the centerline producer on the real M-540003
    DWG: the length/decompose helpers must accept BOTH dict and list coordinate shapes.
    """
    geom: dict[str, dict[str, Any]] = {
        "line-dict": {
            "start": {"x": 0.0, "y": 0.0, "z": 0.0},
            "end": {"x": 3.0, "y": 4.0, "z": 0.0},
        },  # length 5.0
        "poly-dict": {
            "vertices": [{"x": 0.0, "y": 0.0}, {"x": 0.0, "y": 10.0}],
        },  # length 10.0
        "line-list": {"start": [0.0, 0.0, 0.0], "end": [6.0, 0.0, 0.0]},  # length 6.0 (list shape)
    }
    ids = ("line-dict", "poly-dict", "line-list")
    assert entity_group_drawn_length(ids, geom) == 21.0
    segs = decompose_geometry(ids, geom)
    assert ((0.0, 0.0), (3.0, 4.0)) in segs
    assert ((0.0, 0.0), (0.0, 10.0)) in segs
    assert ((0.0, 0.0), (6.0, 0.0)) in segs


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
