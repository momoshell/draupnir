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


def test_write_canonical_dxf_rejects_non_model_layouts() -> None:
    payload = _base_payload()
    payload["layouts"] = [
        {
            "layout_ref": "layout-paper",
            "payload": {"name": "Paper Space"},
        }
    ]

    with pytest.raises(DxfWriteError) as exc_info:
        write_canonical_dxf(payload)

    assert exc_info.value.code == "UNSUPPORTED_LAYOUT"


def test_write_canonical_dxf_rejects_unsupported_entity_types() -> None:
    payload = _base_payload()
    payload["entities"][0]["payload"] = {
        "entity_type": "arc",
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


def test_write_canonical_dxf_rejects_block_refs() -> None:
    payload = _base_payload()
    payload["entities"][0]["payload"]["block_ref"] = "block-1"

    with pytest.raises(DxfWriteError) as exc_info:
        write_canonical_dxf(payload)

    assert exc_info.value.code == "UNSUPPORTED_ENTITY_TYPE"


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
