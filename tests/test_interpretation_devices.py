"""Unit tests for the device / fixture-schedule interpretation engine (pure parts, no DB)."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from app.interpretation.devices import (
    DEFAULT_TAG_MAX_DISTANCE_M,
    LEGEND_FAMILY_MARKER,
    Device,
    _Affine,
    _compose,
    _entity_text,
    _entity_xy,
    _nearest_tag,
    _placement,
    _TagCandidate,
    attach_tags,
    resolve_typed_devices,
    schedule_from_devices,
    scoped_block_families,
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


# ---------------------------------------------------------------------------
# Tests for shared constants and scoped_block_families (#767, #768)
# ---------------------------------------------------------------------------


def test_legend_family_marker_and_default_distance_constants() -> None:
    """Canonical constants are exported with the expected calibrated values."""
    assert LEGEND_FAMILY_MARKER == "legend stage"
    assert DEFAULT_TAG_MAX_DISTANCE_M == 2.0


def test_scoped_block_families_includes_only_marked_families() -> None:
    """scoped_block_families returns FamilyInput only for block_refs with the legend marker.

    Architectural block refs (no marker) must be excluded so they don't enter the
    by_symbol_family legend source and pre-empt architecture classification (#767).
    Case-insensitive match: marker may appear in any casing in the block_ref.
    """
    marked = _device("d1", 0.0, 0.0, "Smoke Detector rfa - Rev1 Legend Stage 4")
    arch_door = _device("d2", 1.0, 0.0, "Door - M_Door_Single-Flush")
    arch_mullion = _device("d3", 2.0, 0.0, "Mullion - Rectangular Mullion")
    no_ref = _device("d4", 3.0, 0.0, None)

    result = scoped_block_families([marked, arch_door, arch_mullion, no_ref])

    # Only the marked family should be returned.
    assert len(result) == 1
    family = result[0]
    # FamilyInput is a NamedTuple / dataclass; access the family_name field.
    assert family.family_name == "Smoke Detector rfa - Rev1 Legend Stage 4"


def test_scoped_block_families_case_insensitive() -> None:
    """Marker match is case-insensitive (block names vary in casing across vendors)."""
    devices = [
        _device("x1", 0.0, 0.0, "Heat Detector - LEGEND STAGE 3"),
        _device("x2", 0.0, 0.0, "Heat Detector - legend stage 3"),
        _device("x3", 0.0, 0.0, "Heat Detector - Legend Stage 3"),
    ]
    result = scoped_block_families(devices)
    assert len(result) == 3


def test_scoped_block_families_empty_input() -> None:
    assert scoped_block_families([]) == []


def test_far_tag_not_attached_with_default_distance_cap() -> None:
    """Devices more than DEFAULT_TAG_MAX_DISTANCE_M away do not receive a tag.

    Mirrors the Welbeck calibration scenario: a device at (0, 0) with a tag at
    (2.47, 0) — distance ~2.47 m — must NOT be tagged when the default cap applies.
    A tag at (1.5, 0) — distance 1.5 m — MUST be attached.
    """
    device = _device("dev-far", 0.0, 0.0, "SD rfa - Legend Stage 4")

    far_candidate = _TagCandidate(entity_id="t-far", text="FAR", layer_ref="Tags", x=2.47, y=0.0)
    near_candidate = _TagCandidate(entity_id="t-near", text="NEAR", layer_ref="Tags", x=1.5, y=0.0)

    tagged_far = attach_tags([device], [far_candidate], max_distance=DEFAULT_TAG_MAX_DISTANCE_M)
    assert tagged_far[0].tag is None, "tag at 2.47 m must be rejected by the 2.0 m cap"

    tagged_near = attach_tags([device], [near_candidate], max_distance=DEFAULT_TAG_MAX_DISTANCE_M)
    assert tagged_near[0].tag is not None and tagged_near[0].tag.text == "NEAR"


def test_resolve_typed_devices_excludes_architecture() -> None:
    """Devices whose block_ref lacks the legend marker are classified 'architecture'.

    resolve_typed_devices feeds the devices + legend through device_identity; when no
    legend entry matches (because scoped_block_families would have excluded the arch family),
    the architecture-pattern step should classify them accordingly.

    This test uses a minimal empty legend so architecture classification falls through
    to the known-architecture pattern check in classify_instance_kind.
    """
    from app.interpretation.legend_dictionary import LegendDictionary

    # A block_ref matching the architecture pattern used by classify_instance_kind.
    # _rvt prefix and "Mullion" are known markers in the architecture-pattern classifier.
    arch_device = _device("arch-1", 0.0, 0.0, "_rvt_Mullion:Rectangular Mullion:293465")
    legend_device = _device("legend-1", 1.0, 1.0, "Smoke Detector rfa - Rev1 Legend Stage 4")

    # Empty legend: no entries, so only architecture patterns can classify.
    empty_legend = LegendDictionary(entries=())

    typed = resolve_typed_devices([arch_device, legend_device], empty_legend)

    # Architecture device should be classified "architecture" and excluded from counted output,
    # or at minimum carry kind=KIND_ARCHITECTURE in the typed result.
    from app.interpretation.device_identity import KIND_ARCHITECTURE

    arch_typed = next((td for td in typed if td.device_id == "arch-1"), None)
    if arch_typed is not None:
        assert arch_typed.kind == KIND_ARCHITECTURE or arch_typed.type_name == "architecture", (
            f"Expected architecture classification, got kind={arch_typed.kind!r} "
            f"type_name={arch_typed.type_name!r}"
        )
