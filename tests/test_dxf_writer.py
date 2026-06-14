"""Tests for canonical revision DXF writer."""

from __future__ import annotations

import hashlib
import io
import math
from collections.abc import Callable
from typing import Any, cast

import ezdxf
import pytest

from app.cad.dxf import DxfWriteError, DxfWriteOptions, write_canonical_dxf

_EXPECTED_DXF_SHA256 = "a7a15882d01840d616ff6b7dd46166020e6fc6fb46f00ad8317c1deda0d96bf0"
_FIXED_GUID = "{00000000-0000-0000-0000-000000000000}"
_FIXED_JULIAN_DATE = "2451545.0"
_FIXED_EZDXF_MARKER = "0.0 @ 2000-01-01T00:00:00.000000+00:00"
_READ_DXF = cast(Callable[[Any], ezdxf.document.Drawing], cast(Any, ezdxf).read)


def _base_payload() -> dict[str, Any]:
    return {
        "unit": "m",
        "layouts": [
            {
                "layout_ref": "layout-model",
                "payload": {"name": "Model Space"},
            }
        ],
        "layers": [
            {
                "layer_ref": "layer-0",
                "payload": {"name": "0"},
            },
            {
                "layer_ref": "layer-a",
                "payload": {"name": "A-WALL"},
            },
        ],
        "entities": [
            {
                "layout_ref": "layout-model",
                "layer_ref": "layer-0",
                "payload": {
                    "entity_type": "line",
                    "geometry": {"start": [0, 0], "end": [2, 0]},
                },
            },
            {
                "layout_ref": "layout-model",
                "layer_ref": "layer-a",
                "payload_json": {
                    "canonical_entity_json": {
                        "type": "line",
                        "geometry": {
                            "start": {"x": 1, "y": 1},
                            "end": {"x": 2, "y": 2},
                        },
                    }
                },
            },
            {
                "layout_ref": "layout-model",
                "layer_ref": "layer-a",
                "payload": {
                    "type": "polyline",
                    "geometry": {"points": [[0, 0], [1, 0], [1, 1], [0, 1]]},
                    "properties": {"closed": True},
                },
            },
        ],
    }


def _parse_dxf(
    payload: dict[str, Any],
    *,
    options: DxfWriteOptions | None = None,
) -> ezdxf.document.Drawing:
    result = write_canonical_dxf(payload, options=options)
    assert result.warnings == ()
    assert result.diagnostics == ()
    return _READ_DXF(io.StringIO(result.content.decode("utf-8")))


def _read_raw_header_var(content: bytes, *, name: str) -> str:
    lines = content.decode("utf-8").splitlines()
    for index in range(len(lines) - 3):
        if lines[index].strip() != "9":
            continue
        if lines[index + 1].strip() != name:
            continue
        return lines[index + 3].strip()

    raise AssertionError(f"missing DXF header variable {name}")


def test_write_canonical_dxf_renders_revision_json_and_materialized_rows() -> None:
    doc = _parse_dxf(_base_payload())

    assert doc.header["$INSUNITS"] == 6
    assert "0" in doc.layers
    assert "A-WALL" in doc.layers

    entities = list(doc.modelspace())
    assert [entity.dxftype() for entity in entities] == ["LINE", "LINE", "LWPOLYLINE"]
    assert [entity.dxf.layer for entity in entities] == ["0", "A-WALL", "A-WALL"]

    first_line = entities[0]
    second_line = entities[1]
    polyline = entities[2]

    assert tuple(first_line.dxf.start) == (0.0, 0.0, 0.0)
    assert tuple(first_line.dxf.end) == (2.0, 0.0, 0.0)
    assert tuple(second_line.dxf.start) == (1.0, 1.0, 0.0)
    assert tuple(second_line.dxf.end) == (2.0, 2.0, 0.0)
    assert cast(Any, polyline).closed is True


def test_write_canonical_dxf_allows_explicit_layer_zero_without_layer_rows() -> None:
    payload = {
        "unit": "meters",
        "layouts": [{"layout_ref": "layout-model", "payload": {"name": "Model"}}],
        "entities": [
            {
                "layout_ref": "layout-model",
                "payload": {
                    "entity_type": "line",
                    "properties": {"layer": "0"},
                    "geometry": {"points": [[0, 0], [3, 0]]},
                },
            }
        ],
    }

    doc = _parse_dxf(payload)
    entity = next(iter(doc.modelspace()))
    assert entity.dxf.layer == "0"


def test_write_canonical_dxf_unsupported_unit_raises_stable_code() -> None:
    payload = _base_payload()
    payload["unit"] = "feet"

    with pytest.raises(DxfWriteError) as exc_info:
        write_canonical_dxf(payload)

    assert exc_info.value.code == "UNSUPPORTED_UNITS"


def test_write_canonical_dxf_options_override_payload_unit() -> None:
    payload = _base_payload()
    payload["unit"] = "feet"

    doc = _parse_dxf(payload, options=DxfWriteOptions(unit="m"))
    assert doc.header["$INSUNITS"] == 6


def test_write_canonical_dxf_repeat_write_is_byte_stable() -> None:
    payload = _base_payload()
    first = write_canonical_dxf(payload)
    second = write_canonical_dxf(payload)

    assert first.content == second.content
    assert hashlib.sha256(first.content).hexdigest() == _EXPECTED_DXF_SHA256
    assert _read_raw_header_var(first.content, name="$INSUNITS") == "6"
    assert _read_raw_header_var(first.content, name="$TDCREATE") == _FIXED_JULIAN_DATE
    assert _read_raw_header_var(first.content, name="$TDUCREATE") == _FIXED_JULIAN_DATE
    assert _read_raw_header_var(first.content, name="$TDUPDATE") == _FIXED_JULIAN_DATE
    assert _read_raw_header_var(first.content, name="$TDUUPDATE") == _FIXED_JULIAN_DATE
    assert _read_raw_header_var(first.content, name="$FINGERPRINTGUID") == _FIXED_GUID
    assert _read_raw_header_var(first.content, name="$VERSIONGUID") == _FIXED_GUID
    assert first.content.decode("utf-8").count(_FIXED_EZDXF_MARKER) == 2

    first_doc = _READ_DXF(io.StringIO(first.content.decode("utf-8")))
    assert first_doc.header["$INSUNITS"] == 6


def test_write_canonical_dxf_rejects_multiple_non_model_layouts() -> None:
    # With more than one layout we must be able to pick the model layout; if none qualifies
    # the write is ambiguous and still rejected.
    payload = _base_payload()
    payload["layouts"] = [
        {"layout_ref": "layout-paper-1", "payload": {"name": "Paper Space"}},
        {"layout_ref": "layout-paper-2", "payload": {"name": "Sheet 2"}},
    ]

    with pytest.raises(DxfWriteError) as exc_info:
        write_canonical_dxf(payload)

    assert exc_info.value.code == "UNSUPPORTED_LAYOUT"


def test_write_canonical_dxf_maps_single_non_model_layout_to_modelspace() -> None:
    # A lone non-model layout (e.g. a PDF page "page-1") maps to modelspace instead of being
    # rejected; entities referencing it render in modelspace. (Issue #409.)
    payload = _base_payload()
    payload["layouts"] = [
        {"layout_ref": "layout-page", "payload": {"name": "page-1"}},
    ]
    for entity in payload["entities"]:
        entity["layout_ref"] = "layout-page"

    doc = _parse_dxf(payload)

    entities = list(doc.modelspace())
    assert [entity.dxftype() for entity in entities] == ["LINE", "LINE", "LWPOLYLINE"]


def test_write_canonical_dxf_single_layout_accepts_arbitrary_name() -> None:
    payload = _base_payload()
    payload["layouts"] = [
        {"layout_ref": "layout-sheet", "payload": {"name": "Sheet1"}},
    ]
    for entity in payload["entities"]:
        entity["layout_ref"] = "layout-sheet"

    doc = _parse_dxf(payload)

    assert len(list(doc.modelspace())) == 3


def test_write_canonical_dxf_rejects_unsupported_entity_types() -> None:
    payload = _base_payload()
    payload["entities"][0]["payload"] = {
        "entity_type": "spline",
        "geometry": {"start": [0, 0], "end": [1, 1]},
    }

    with pytest.raises(DxfWriteError) as exc_info:
        write_canonical_dxf(payload)

    assert exc_info.value.code == "UNSUPPORTED_ENTITY_TYPE"


def test_write_canonical_dxf_rejects_unknown_layer_references() -> None:
    payload = _base_payload()
    payload["entities"][0]["layer_ref"] = "layer-missing"

    with pytest.raises(DxfWriteError) as exc_info:
        write_canonical_dxf(payload)

    assert exc_info.value.code == "UNKNOWN_LAYER"


def test_write_canonical_dxf_rejects_missing_layer_references_except_zero() -> None:
    payload = _base_payload()
    payload["entities"][0] = {
        "layout_ref": "layout-model",
        "payload": {
            "entity_type": "line",
            "geometry": {"start": [0, 0], "end": [1, 1]},
        },
    }

    with pytest.raises(DxfWriteError) as exc_info:
        write_canonical_dxf(payload)

    assert exc_info.value.code == "UNKNOWN_LAYER"


def test_write_canonical_dxf_allows_block_ref_metadata_on_non_insert_entity() -> None:
    # A block_ref on a non-INSERT entity is owner metadata; the entity still emits normally
    # (block references are now supported via INSERT, not rejected outright).
    payload = _base_payload()
    payload["entities"][0]["payload"]["block_ref"] = "block-1"

    doc = _parse_dxf(payload)
    assert [entity.dxftype() for entity in doc.modelspace()] == ["LINE", "LINE", "LWPOLYLINE"]


def test_write_canonical_dxf_emits_arc_circle_text() -> None:
    payload = _base_payload()
    payload["entities"] = [
        {
            "layout_ref": "layout-model",
            "layer_ref": "layer-0",
            "payload": {
                "entity_type": "circle",
                "geometry": {"center": {"x": 1, "y": 2}, "radius": 0.5},
            },
        },
        {
            "layout_ref": "layout-model",
            "layer_ref": "layer-0",
            "payload": {
                "entity_type": "arc",
                "geometry": {
                    "center": {"x": 0, "y": 0},
                    "radius": 1.0,
                    "start_angle_degrees": 0.0,
                    "end_angle_degrees": 90.0,
                },
            },
        },
        {
            "layout_ref": "layout-model",
            "layer_ref": "layer-a",
            "payload": {
                "entity_type": "text",
                "geometry": {"insertion": {"x": 3, "y": 4}, "text": "SD-01"},
            },
        },
    ]

    doc = _parse_dxf(payload)
    msp = doc.modelspace()
    assert [entity.dxftype() for entity in msp] == ["CIRCLE", "ARC", "TEXT"]
    circle, arc, text = list(msp)
    assert circle.dxf.radius == pytest.approx(0.5)
    assert (circle.dxf.center.x, circle.dxf.center.y) == pytest.approx((1.0, 2.0))
    assert arc.dxf.start_angle == pytest.approx(0.0)
    assert arc.dxf.end_angle == pytest.approx(90.0)
    assert text.dxf.text == "SD-01"


def test_write_canonical_dxf_emits_hatch_from_boundary_loops() -> None:
    payload = _base_payload()
    payload["entities"] = [
        {
            "layout_ref": "layout-model",
            "layer_ref": "layer-0",
            "payload": {
                "entity_type": "hatch",
                "geometry": {
                    "boundary_loops": [
                        [{"x": 0, "y": 0}, {"x": 2, "y": 0}, {"x": 2, "y": 2}, {"x": 0, "y": 2}]
                    ],
                },
            },
        }
    ]

    doc = _parse_dxf(payload)
    hatches = [entity for entity in doc.modelspace() if entity.dxftype() == "HATCH"]
    assert len(hatches) == 1
    assert len(cast(Any, hatches[0]).paths) == 1


def test_write_canonical_dxf_emits_block_definitions_and_insert_references() -> None:
    payload = _base_payload()
    payload["blocks"] = [
        {
            "name": "SMOKE-DETECTOR",
            "block_ref": "SMOKE-DETECTOR",
            "base_point": {"x": 0, "y": 0, "z": 0},
            "entities": [
                {
                    "layer_ref": "layer-0",
                    "payload": {
                        "entity_type": "circle",
                        "geometry": {"center": {"x": 0, "y": 0}, "radius": 0.1},
                    },
                }
            ],
        }
    ]
    # Two placements of the detector block at different positions.
    payload["entities"] = [
        {
            "layout_ref": "layout-model",
            "layer_ref": "layer-0",
            "payload": {
                "entity_type": "insert",
                "block_ref": "SMOKE-DETECTOR",
                "transform": {
                    "insertion_point": {"x": 5, "y": 6},
                    "scale": {"x": 1, "y": 1, "z": 1},
                    "rotation_degrees": 0.0,
                },
            },
        },
        {
            "layout_ref": "layout-model",
            "layer_ref": "layer-0",
            "payload": {
                "entity_type": "insert",
                "block_ref": "SMOKE-DETECTOR",
                "transform": {
                    "insertion_point": {"x": 10, "y": 12},
                    "scale": {"x": 2, "y": 2, "z": 2},
                    "rotation_degrees": 90.0,
                },
            },
        },
    ]

    doc = _parse_dxf(payload)
    # The block definition exists and carries its child geometry.
    assert "SMOKE-DETECTOR" in doc.blocks
    block_types = [entity.dxftype() for entity in doc.blocks.get("SMOKE-DETECTOR")]
    assert block_types == ["CIRCLE"]
    # Both placements emit as INSERT references to the block, not flattened geometry.
    inserts = [entity for entity in doc.modelspace() if entity.dxftype() == "INSERT"]
    assert [insert.dxf.name for insert in inserts] == ["SMOKE-DETECTOR", "SMOKE-DETECTOR"]
    assert (inserts[0].dxf.insert.x, inserts[0].dxf.insert.y) == pytest.approx((5.0, 6.0))
    assert inserts[1].dxf.xscale == pytest.approx(2.0)
    assert inserts[1].dxf.rotation == pytest.approx(90.0)


def test_write_canonical_dxf_allows_null_block_ref_in_revision_json_entity() -> None:
    payload = _base_payload()
    payload["entities"][1]["payload_json"]["canonical_entity_json"]["block_ref"] = None

    doc = _parse_dxf(payload)
    assert [entity.dxftype() for entity in doc.modelspace()] == ["LINE", "LINE", "LWPOLYLINE"]


def test_write_canonical_dxf_rejects_nonzero_z_coordinates() -> None:
    payload = _base_payload()
    payload["entities"][0]["payload"]["geometry"] = {
        "start": [0, 0, 1],
        "end": [2, 0, 0],
    }

    with pytest.raises(DxfWriteError) as exc_info:
        write_canonical_dxf(payload)

    assert exc_info.value.code == "UNSUPPORTED_GEOMETRY"


def test_write_canonical_dxf_requires_entity_meter_units_when_top_level_unit_missing() -> None:
    payload = _base_payload()
    payload.pop("unit")
    payload["entities"][0]["payload"]["geometry"]["units"] = {"normalized": "meters"}
    payload["entities"][1]["payload"] = {
        "entity_type": "line",
        "geometry": {
            "start": {"x": 1, "y": 1},
            "end": {"x": 2, "y": 2},
            "units": {"normalized": "meters"},
        },
    }
    payload["entities"][1].pop("payload_json", None)
    payload["entities"][2]["payload"]["geometry"]["units"] = {"normalized": "meters"}

    doc = _parse_dxf(payload)
    assert doc.header["$INSUNITS"] == 6


def test_write_canonical_dxf_rejects_unitless_entity_units_without_top_level_unit() -> None:
    payload = _base_payload()
    payload.pop("unit")
    payload["entities"][0]["payload"]["geometry"]["units"] = {"normalized": "unitless"}
    payload["entities"][1]["payload"] = {
        "entity_type": "line",
        "geometry": {
            "start": {"x": 1, "y": 1},
            "end": {"x": 2, "y": 2},
            "units": {"normalized": "meters"},
        },
    }
    payload["entities"][1].pop("payload_json", None)
    payload["entities"][2]["payload"]["geometry"]["units"] = {"normalized": "meters"}

    with pytest.raises(DxfWriteError) as exc_info:
        write_canonical_dxf(payload)

    assert exc_info.value.code == "UNSUPPORTED_UNITS"


def test_write_canonical_dxf_unknown_top_level_units_exports_unitless() -> None:
    # Vector PDFs declare units as {"normalized": "unknown"} — export unitless (INSUNITS=0)
    # with a caveat instead of failing the render. (Issue #431.)
    payload = _base_payload()
    payload.pop("unit")
    payload["units"] = {"normalized": "unknown"}

    result = write_canonical_dxf(payload)

    assert any("unitless" in warning.lower() for warning in result.warnings)
    doc = _READ_DXF(io.StringIO(result.content.decode("utf-8")))
    assert doc.header["$INSUNITS"] == 0
    assert len(list(doc.modelspace())) == 3


def test_write_canonical_dxf_null_top_level_units_exports_unitless() -> None:
    payload = _base_payload()
    payload.pop("unit")
    payload["units"] = None

    result = write_canonical_dxf(payload)

    assert result.warnings == (
        "Drawing units are unknown; exported as unitless (INSUNITS=0). "
        "Real-world scale was not applied.",
    )
    doc = _READ_DXF(io.StringIO(result.content.decode("utf-8")))
    assert doc.header["$INSUNITS"] == 0


def test_write_canonical_dxf_units_mapping_meters_resolves_without_warning() -> None:
    payload = _base_payload()
    payload.pop("unit")
    payload["units"] = {"normalized": "meters"}

    result = write_canonical_dxf(payload)

    assert result.warnings == ()
    doc = _READ_DXF(io.StringIO(result.content.decode("utf-8")))
    assert doc.header["$INSUNITS"] == 6


@pytest.mark.parametrize("field_name", ["blocks", "xrefs"])
def test_write_canonical_dxf_rejects_nonempty_top_level_blocks_and_xrefs(
    field_name: str,
) -> None:
    payload = _base_payload()
    payload[field_name] = [{"id": f"{field_name}-1"}]

    with pytest.raises(DxfWriteError) as exc_info:
        write_canonical_dxf(payload)

    assert exc_info.value.code == "UNSUPPORTED_ENTITY_TYPE"


def test_write_canonical_dxf_rejects_nonfinite_coordinates() -> None:
    payload = _base_payload()
    payload["entities"][0]["payload"]["geometry"] = {
        "start": [math.inf, 0],
        "end": [1, 1],
    }

    with pytest.raises(DxfWriteError) as exc_info:
        write_canonical_dxf(payload)

    assert exc_info.value.code == "INVALID_COORDINATE"


def test_write_canonical_dxf_applies_coordinate_scale_with_real_unit() -> None:
    # The PDF real-world export (#436) sets units + a per-drawing coordinate_scale; the writer
    # converts source (point) coordinates into the declared unit and labels INSUNITS.
    payload = _base_payload()
    payload.pop("unit")
    payload["units"] = "mm"
    payload["coordinate_scale"] = 10.0

    result = write_canonical_dxf(payload)

    assert result.warnings == ()
    doc = _READ_DXF(io.StringIO(result.content.decode("utf-8")))
    assert doc.header["$INSUNITS"] == 4  # millimetres
    first_line = next(iter(doc.modelspace()))
    # base payload first line is (0,0)->(2,0); scaled x10 => (0,0)->(20,0)
    assert tuple(first_line.dxf.end) == (20.0, 0.0, 0.0)


def test_write_canonical_dxf_coordinate_scale_defaults_to_identity() -> None:
    payload = _base_payload()
    payload["coordinate_scale"] = 1.0
    doc = _parse_dxf(payload)
    first_line = next(iter(doc.modelspace()))
    assert tuple(first_line.dxf.end) == (2.0, 0.0, 0.0)  # unchanged


def test_write_canonical_dxf_rejects_non_positive_coordinate_scale() -> None:
    payload = _base_payload()
    payload["coordinate_scale"] = 0

    with pytest.raises(DxfWriteError) as exc_info:
        write_canonical_dxf(payload)

    assert exc_info.value.code == "INVALID_COORDINATE"
