"""Unit tests for the device / fixture-schedule interpretation engine (pure, no DB)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.interpretation.devices import (
    _entity_text,
    _entity_xy,
    _nearest_tag,
    _TagCandidate,
    build_devices,
)


@dataclass
class _FakeEntity:
    entity_id: str
    sequence_index: int = 0
    block_ref: str | None = None
    layer_ref: str | None = None
    geometry_json: dict[str, Any] | None = None


def _tag(
    entity_id: str, text: str, x: float, y: float, layer: str | None = "Tags"
) -> _TagCandidate:
    return _TagCandidate(entity_id=entity_id, text=text, layer_ref=layer, x=x, y=y)


def test_entity_xy_reads_insert_transform_and_text_insertion() -> None:
    insert = _FakeEntity(
        "i", geometry_json={"transform": {"insertion_point": {"x": 5.0, "y": 6.0}}}
    )
    text = _FakeEntity("t", geometry_json={"insertion": {"x": 1.0, "y": 2.0}})
    missing = _FakeEntity("m", geometry_json={"bbox": None})

    assert _entity_xy(insert) == (5.0, 6.0)
    assert _entity_xy(text) == (1.0, 2.0)
    assert _entity_xy(missing) is None


def test_entity_text_requires_non_empty_string() -> None:
    assert _entity_text(_FakeEntity("a", geometry_json={"text": "SD-01"})) == "SD-01"
    assert _entity_text(_FakeEntity("b", geometry_json={"text": "  "})) is None
    assert _entity_text(_FakeEntity("c", geometry_json={})) is None


def test_nearest_tag_picks_closest() -> None:
    candidates = [_tag("t1", "FAR", 100.0, 0.0), _tag("t2", "NEAR", 1.0, 1.0)]
    tag = _nearest_tag((0.0, 0.0), candidates, max_distance=None)
    assert tag is not None
    assert tag.text == "NEAR"
    assert round(tag.distance, 4) == round((2.0) ** 0.5, 4)


def test_nearest_tag_respects_max_distance() -> None:
    candidates = [_tag("t1", "FAR", 100.0, 0.0)]
    assert _nearest_tag((0.0, 0.0), candidates, max_distance=10.0) is None
    assert _nearest_tag((0.0, 0.0), candidates, max_distance=200.0) is not None


def test_nearest_tag_handles_no_candidates() -> None:
    assert _nearest_tag((0.0, 0.0), [], max_distance=None) is None


def test_build_devices_associates_and_preserves_metadata() -> None:
    devices = _FakeEntity(
        "dev-1",
        sequence_index=3,
        block_ref="Smoke-Detector",
        layer_ref="F810A",
        geometry_json={"transform": {"insertion_point": {"x": 0.0, "y": 0.0}}},
    )
    candidates = [_tag("tag-1", "SD-01", 0.5, 0.0)]

    [device] = build_devices([devices], candidates, max_distance=5.0)
    assert device.entity_id == "dev-1"
    assert device.sequence_index == 3
    assert device.block_ref == "Smoke-Detector"
    assert device.layer_ref == "F810A"
    assert device.position == {"x": 0.0, "y": 0.0}
    assert device.tag is not None
    assert device.tag.text == "SD-01"


def test_build_devices_without_position_has_no_tag() -> None:
    device_row = _FakeEntity("dev-2", block_ref="Blk", geometry_json={"bbox": None})
    [device] = build_devices([device_row], [_tag("t", "X", 0.0, 0.0)], max_distance=None)
    assert device.position is None
    assert device.tag is None
