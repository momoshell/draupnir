"""Unit tests for block-instance expansion (#541)."""

import math
from typing import Any

from app.ingestion.block_expansion import Affine2D, expand_block_instances


def _door_block(ref: str = "DOOR") -> dict[str, Any]:
    return {
        "block_ref": ref,
        "name": ref,
        "base_point": {"x": 0.0, "y": 0.0, "z": 0.0},
        "entities": [
            {
                "entity_id": "d-line",
                "entity_type": "line",
                "geometry": {
                    "start": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "end": {"x": 1.0, "y": 0.0, "z": 0.0},
                    "bbox": {"min": {"x": 0.0, "y": 0.0}, "max": {"x": 1.0, "y": 0.0}},
                },
            },
            {
                "entity_id": "d-circ",
                "entity_type": "circle",
                "geometry": {"center": {"x": 0.0, "y": 0.0, "z": 0.0}, "radius": 0.5},
            },
        ],
    }


def _insert(
    block_ref: str,
    x: float,
    y: float,
    *,
    sx: float = 1.0,
    sy: float = 1.0,
    rot: float = 0.0,
    eid: str = "ins",
    array: dict[str, Any] | None = None,
) -> dict[str, Any]:
    transform: dict[str, Any] = {
        "insertion_point": {"x": x, "y": y, "z": 0.0},
        "scale": {"x": sx, "y": sy, "z": 1.0},
        "rotation_radians": rot,
    }
    if array is not None:
        transform["array"] = array
    return {
        "entity_id": eid,
        "entity_type": "insert",
        "block_ref": block_ref,
        "geometry": {"transform": transform},
    }


def _expanded(canonical: dict[str, Any]) -> list[dict[str, Any]]:
    out = expand_block_instances(canonical)
    return [
        e
        for e in out["entities"]
        if (e.get("provenance_json") or {}).get("origin") == "block_expansion"
    ]


def _by_src(placed: list[dict[str, Any]], src_eid: str) -> dict[str, Any]:
    return next(
        e
        for e in placed
        if (e["provenance_json"]["block_expansion"]["source_entity_id"]) == src_eid
    )


# ---- Affine2D ----


def test_affine_translate_rotate_scale_and_compose() -> None:
    assert Affine2D.translate(3, 4).apply(1, 1) == (4, 5)
    rx, ry = Affine2D.rotate(math.pi / 2).apply(1, 0)
    assert round(rx, 9) == 0 and round(ry, 9) == 1
    assert Affine2D.scale(2, 3).apply(2, 2) == (4, 6)
    # compose: translate ∘ scale → scale first, then translate
    m = Affine2D.translate(10, 0).matmul(Affine2D.scale(2, 2))
    assert m.apply(1, 1) == (12, 2)


def test_affine_conformal_detection() -> None:
    assert Affine2D.rotate(0.7).matmul(Affine2D.scale(2, 2)).is_conformal() is True
    assert Affine2D.scale(2, 1).is_conformal() is False


# ---- expansion ----


def test_translate_only_places_children_at_world_coords() -> None:
    canonical = {"blocks": [_door_block()], "entities": [_insert("DOOR", 10, 20, eid="A")]}
    placed = _expanded(canonical)
    assert len(placed) == 2
    line = _by_src(placed, "d-line")
    assert (line["geometry"]["start"]["x"], line["geometry"]["start"]["y"]) == (10, 20)
    assert (line["geometry"]["end"]["x"], line["geometry"]["end"]["y"]) == (11, 20)
    assert "bbox" not in line["geometry"]  # stale local bbox dropped
    circ = _by_src(placed, "d-circ")
    assert (circ["geometry"]["center"]["x"], circ["geometry"]["center"]["y"]) == (10, 20)
    assert circ["geometry"]["radius"] == 0.5


def test_rotation_and_uniform_scale() -> None:
    canonical = {
        "blocks": [_door_block()],
        "entities": [_insert("DOOR", 0, 0, sx=2, sy=2, rot=math.pi / 2, eid="A")],
    }
    line = _by_src(_expanded(canonical), "d-line")
    # endpoint (1,0) → scale 2 → (2,0) → rotate 90° → (0,2)
    assert round(line["geometry"]["end"]["x"], 9) == 0
    assert round(line["geometry"]["end"]["y"], 9) == 2
    circ = _by_src(_expanded(canonical), "d-circ")
    assert circ["geometry"]["radius"] == 1.0  # 0.5 * uniform scale 2
    assert circ["provenance_json"]["block_expansion"]["approximate_transform"] is False


def test_non_uniform_scale_flags_approximate() -> None:
    canonical = {
        "blocks": [_door_block()],
        "entities": [_insert("DOOR", 0, 0, sx=2, sy=1, eid="A")],
    }
    line = _by_src(_expanded(canonical), "d-line")
    assert line["geometry"]["end"]["x"] == 2 and line["geometry"]["end"]["y"] == 0
    assert line["provenance_json"]["block_expansion"]["approximate_transform"] is True


def test_nested_blocks_compose_transforms() -> None:
    group = {
        "block_ref": "GROUP",
        "name": "GROUP",
        "base_point": {"x": 0.0, "y": 0.0, "z": 0.0},
        "entities": [_insert("DOOR", 5, 0, eid="g-ins")],
    }
    canonical = {"blocks": [_door_block(), group], "entities": [_insert("GROUP", 100, 0, eid="A")]}
    placed = _expanded(canonical)
    line = _by_src(placed, "d-line")
    # DOOR placed at GROUP(100,0) ∘ inner(5,0) = (105,0)
    assert (line["geometry"]["start"]["x"], line["geometry"]["start"]["y"]) == (105, 0)


def test_array_places_each_cell() -> None:
    canonical = {
        "blocks": [_door_block()],
        "entities": [
            _insert(
                "DOOR",
                0,
                0,
                eid="A",
                array={"columns": 2, "rows": 1, "column_spacing": 3, "row_spacing": 0},
            )
        ],
    }
    placed = _expanded(canonical)
    circ_centers = {
        (e["geometry"]["center"]["x"], e["geometry"]["center"]["y"])
        for e in placed
        if e["provenance_json"]["block_expansion"]["source_entity_id"] == "d-circ"
    }
    assert circ_centers == {(0, 0), (3, 0)}


def test_noop_when_blocks_have_no_child_geometry() -> None:
    # ezdxf-style blocks (name + base_point only) → nothing to expand.
    canonical = {
        "blocks": [{"name": "X", "base_point": {"x": 0, "y": 0}}],
        "entities": [_insert("X", 1, 1)],
    }
    out = expand_block_instances(canonical)
    assert len(out["entities"]) == 1


def test_cycle_guard_does_not_recurse_forever() -> None:
    selfref = {
        "block_ref": "A",
        "name": "A",
        "base_point": {"x": 0, "y": 0},
        "entities": [_insert("A", 1, 0, eid="self")],
    }
    canonical = {"blocks": [selfref], "entities": [_insert("A", 0, 0, eid="top")]}
    out = expand_block_instances(canonical)  # must terminate
    assert isinstance(out["entities"], list)
