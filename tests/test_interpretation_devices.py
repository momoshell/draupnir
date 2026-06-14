"""Unit tests for the device / fixture-schedule interpretation engine (pure parts, no DB)."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from app.interpretation.devices import (
    Device,
    _Affine,
    _compose,
    _entity_text,
    _entity_xy,
    _nearest_tag,
    _placement,
    _TagCandidate,
    attach_tags,
    schedule_from_devices,
)


@dataclass
class _FakeEntity:
    entity_id: str
    geometry_json: dict[str, Any] | None = None
    sequence_index: int = 0
    block_ref: str | None = None
    layer_ref: str | None = None


def _tag(entity_id: str, text: str, x: float, y: float) -> _TagCandidate:
    return _TagCandidate(entity_id=entity_id, text=text, layer_ref="Tags", x=x, y=y)


def _device(entity_id: str, x: float | None, y: float | None, block_ref: str | None) -> Device:
    position = {"x": x, "y": y} if x is not None and y is not None else None
    return Device(
        entity_id=entity_id,
        sequence_index=0,
        depth=0,
        block_ref=block_ref,
        layer_ref="F810A",
        position=position,
        tag=None,
    )


def test_entity_xy_reads_insert_transform_and_text_insertion() -> None:
    insert = _FakeEntity(
        "i", geometry_json={"transform": {"insertion_point": {"x": 5.0, "y": 6.0}}}
    )
    text = _FakeEntity("t", geometry_json={"insertion": {"x": 1.0, "y": 2.0}})
    assert _entity_xy(insert) == (5.0, 6.0)
    assert _entity_xy(text) == (1.0, 2.0)
    assert _entity_xy(_FakeEntity("m", geometry_json={"bbox": None})) is None


def test_entity_text_requires_non_empty_string() -> None:
    assert _entity_text(_FakeEntity("a", geometry_json={"text": "SD-01"})) == "SD-01"
    assert _entity_text(_FakeEntity("b", geometry_json={"text": "  "})) is None
    assert _entity_text(_FakeEntity("c", geometry_json={})) is None


def test_placement_extracts_insertion_scale_rotation() -> None:
    geometry = {
        "transform": {
            "insertion_point": {"x": 3.0, "y": 4.0},
            "scale": {"x": 2.0, "y": 2.0},
            "rotation_radians": math.pi,
        }
    }
    assert _placement(geometry) == ((3.0, 4.0), 2.0, math.pi)
    assert _placement({"bbox": None}) is None


def test_affine_apply_translates_scales_rotates() -> None:
    # 90-degree rotation, scale 2, translate (1, 1): local (1, 0) -> (1, 0)*2 rotated 90 = (0, 2)
    affine = _Affine(tx=1.0, ty=1.0, scale=2.0, rot=math.pi / 2)
    x, y = affine.apply(1.0, 0.0)
    assert round(x, 6) == 1.0
    assert round(y, 6) == 3.0


def test_compose_places_nested_block_into_world() -> None:
    parent = _Affine(tx=100.0, ty=0.0, scale=1.0, rot=0.0)
    # A nested insert at local (10, 0), no scale/rotation, block base at origin.
    child = _compose(parent, insertion=(10.0, 0.0), scale=1.0, rotation=0.0, base=(0.0, 0.0))
    assert (round(child.tx, 6), round(child.ty, 6)) == (110.0, 0.0)
    # A point at the child block's origin lands at the child's world translation.
    assert tuple(round(v, 6) for v in child.apply(0.0, 0.0)) == (110.0, 0.0)


def test_nearest_tag_picks_closest_within_max_distance() -> None:
    candidates = [_tag("t1", "FAR", 100.0, 0.0), _tag("t2", "NEAR", 1.0, 0.0)]
    nearest = _nearest_tag((0.0, 0.0), candidates, max_distance=None)
    assert nearest is not None and nearest.text == "NEAR"
    assert _nearest_tag((0.0, 0.0), [candidates[0]], max_distance=10.0) is None
    assert _nearest_tag((0.0, 0.0), [], max_distance=None) is None


def test_attach_tags_associates_by_world_position() -> None:
    devices = [_device("dev-1", 0.0, 0.0, "Smoke"), _device("dev-2", None, None, "Panel")]
    tagged = attach_tags(devices, [_tag("tag-1", "SD-01", 0.5, 0.0)], max_distance=2.0)
    assert tagged[0].tag is not None and tagged[0].tag.text == "SD-01"
    assert tagged[1].tag is None  # no position -> no tag


def test_schedule_from_devices_counts_by_block_ref() -> None:
    devices = [
        _device("a", 0.0, 0.0, "Smoke"),
        _device("b", 1.0, 0.0, "Smoke"),
        _device("c", 2.0, 0.0, "Panel"),
    ]
    assert schedule_from_devices(devices) == [
        {"block_ref": "Smoke", "count": 2},
        {"block_ref": "Panel", "count": 1},
    ]
