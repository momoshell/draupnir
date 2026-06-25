"""Tests for the concrete ezdxf-backed DXF ingestion adapter."""

from __future__ import annotations

import asyncio
import hashlib
import json
import types
from collections.abc import Iterator
from math import isfinite
from pathlib import Path
from typing import Any, NoReturn, cast

import ezdxf
import pytest

from app.ingestion.adapters import ezdxf as ezdxf_adapter_module
from app.ingestion.adapters.ezdxf import EzdxfAdapter
from app.ingestion.canonical.geometry import canonical_bbox_from_points
from app.ingestion.contracts import (
    AdapterExecutionOptions,
    AdapterTimeout,
    IngestionAdapter,
    InputFamily,
    JSONValue,
    ProgressUpdate,
)
from app.ingestion.loader import load_adapter
from app.ingestion.registry import get_descriptor
from tests.ingestion_contract_harness import (
    ContractFinalizationExpectation,
    build_contract_source,
    exercise_adapter_contract,
)

_FIXTURE_PATH = Path(__file__).parent / "fixtures" / "dxf" / "simple-line.dxf"


class _CancellationAfterChecks:
    def __init__(self, *, cancel_after: int) -> None:
        self._cancel_after = cancel_after
        self.calls = 0

    def is_cancelled(self) -> bool:
        self.calls += 1
        return self.calls >= self._cancel_after


def _fake_point(x: object, y: object, z: object) -> Any:
    return types.SimpleNamespace(x=x, y=y, z=z)


class _FakeEntity:
    def __init__(
        self,
        *,
        entity_type: str,
        handle: str,
        layer: str,
        layout_name: str,
        start: Any | None = None,
        end: Any | None = None,
    ) -> None:
        self._entity_type = entity_type
        self._layout = types.SimpleNamespace(name=layout_name)
        self.dxf = types.SimpleNamespace(
            handle=handle,
            layer=layer,
            linetype="Continuous",
            start=start,
            end=end,
        )

    def dxftype(self) -> str:
        return self._entity_type

    def get_layout(self) -> Any:
        return self._layout


class _FakePolylineVertex:
    def __init__(
        self,
        location: Any,
        *,
        flags: int = 0,
        start_width: float = 0.0,
        end_width: float = 0.0,
    ) -> None:
        self.dxf = types.SimpleNamespace(
            location=location,
            flags=flags,
            start_width=start_width,
            end_width=end_width,
        )


class _FakePolylineEntity:
    def __init__(
        self,
        *,
        handle: str,
        layer: str,
        layout_name: str,
        points: tuple[Any, ...],
        vertex_flags: tuple[int, ...] | None = None,
        closed: bool = False,
        flags: int = 0,
        vertex_widths: tuple[tuple[float, float], ...] | None = None,
        is_2d_polyline: bool = True,
        is_3d_polyline: bool = False,
        is_polygon_mesh: bool = False,
        is_poly_face_mesh: bool = False,
        has_arc: bool = False,
    ) -> None:
        self._layout = types.SimpleNamespace(name=layout_name)
        self._points = points
        self.is_closed = closed
        self.is_2d_polyline = is_2d_polyline
        self.is_3d_polyline = is_3d_polyline
        self.is_polygon_mesh = is_polygon_mesh
        self.is_poly_face_mesh = is_poly_face_mesh
        self.has_arc = has_arc
        flags_by_vertex = vertex_flags or tuple(0 for _ in points)
        widths_by_vertex = vertex_widths or tuple((0.0, 0.0) for _ in points)
        self.vertices = tuple(
            _FakePolylineVertex(
                point,
                flags=vertex_flag,
                start_width=vertex_width[0],
                end_width=vertex_width[1],
            )
            for point, vertex_flag, vertex_width in zip(
                points,
                flags_by_vertex,
                widths_by_vertex,
                strict=True,
            )
        )
        self.dxf = types.SimpleNamespace(
            handle=handle,
            layer=layer,
            linetype="Continuous",
            flags=flags,
        )

    def dxftype(self) -> str:
        return "POLYLINE"

    def get_layout(self) -> Any:
        return self._layout

    def points_in_wcs(self) -> tuple[Any, ...]:
        return self._points


class _FakeLWPolylineEntity:
    def __init__(
        self,
        *,
        handle: str,
        layer: str,
        layout_name: str,
        points: tuple[Any, ...],
        segment_widths: tuple[tuple[float, float], ...] | None = None,
        const_width: float = 0.0,
        closed: bool = False,
        has_arc: bool = False,
    ) -> None:
        self._layout = types.SimpleNamespace(name=layout_name)
        self._points = points
        self.is_closed = closed
        self.has_arc = has_arc
        widths_by_vertex = segment_widths or tuple((0.0, 0.0) for _ in points)
        self._vertices = tuple(
            (point.x, point.y, start_width, end_width, 0.0)
            for point, (start_width, end_width) in zip(points, widths_by_vertex, strict=True)
        )
        self.has_width = const_width != 0.0 or any(
            start_width != 0.0 or end_width != 0.0 for start_width, end_width in widths_by_vertex
        )
        self.dxf = types.SimpleNamespace(
            handle=handle,
            layer=layer,
            linetype="Continuous",
            flags=1 if closed else 0,
            const_width=const_width,
        )

    def dxftype(self) -> str:
        return "LWPOLYLINE"

    def get_layout(self) -> Any:
        return self._layout

    def __iter__(self) -> Iterator[Any]:
        return iter(self._vertices)

    def vertices_in_wcs(self) -> tuple[Any, ...]:
        return self._points


class _FakeBlock:
    def __init__(
        self,
        *,
        name: str,
        is_xref: bool,
        xref_path: str = "",
        base_point: Any | None = None,
        entities: tuple[Any, ...] = (),
    ) -> None:
        self.name = name
        self.block = types.SimpleNamespace(
            is_xref=is_xref,
            dxf=types.SimpleNamespace(
                base_point=base_point or _fake_point(0.0, 0.0, 0.0),
                xref_path=xref_path,
            ),
        )
        self._entities = entities

    def __iter__(self) -> Iterator[Any]:
        return iter(self._entities)


class _FakeDocument:
    def __init__(
        self,
        *,
        entities: tuple[Any, ...],
        units: int = 6,
        blocks: tuple[Any, ...] = (),
    ) -> None:
        self._entities = entities
        self.header = {"$INSUNITS": units}
        self.units = units
        self.layouts = [
            types.SimpleNamespace(name="Model", dxf=types.SimpleNamespace(taborder=0)),
            types.SimpleNamespace(name="Layout1", dxf=types.SimpleNamespace(taborder=1)),
        ]
        self.layers = [
            types.SimpleNamespace(
                color=7,
                dxf=types.SimpleNamespace(name="0", linetype="Continuous"),
            )
        ]
        self.blocks = list(blocks)
        self.dxfversion = "AC1027"

    def modelspace(self) -> tuple[Any, ...]:
        return self._entities


def _mapping_tuple(value: object) -> tuple[dict[str, object], ...]:
    assert isinstance(value, tuple)
    for item in value:
        assert isinstance(item, dict)
    return cast(tuple[dict[str, object], ...], value)


def _mapping(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return cast(dict[str, object], value)


def _assert_no_nonfinite_numbers(value: object) -> None:
    if isinstance(value, dict):
        for nested in value.values():
            _assert_no_nonfinite_numbers(nested)
        return
    if isinstance(value, (tuple, list)):
        for nested in value:
            _assert_no_nonfinite_numbers(nested)
        return
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        assert isfinite(float(value))


def _assert_common_entity_contract(
    entity: dict[str, object],
    *,
    entity_type: str,
    layout_ref: str,
    layer_ref: str,
) -> None:
    assert entity["entity_id"] == f"dxf:{str(entity['handle']).lower()}"
    assert entity["entity_type"] == entity_type
    assert entity["entity_schema_version"] == "0.1"
    assert entity["confidence"] in {0.0, 0.99}
    assert entity["layout_ref"] == layout_ref
    assert entity["layer_ref"] == layer_ref
    assert entity["block_ref"] is None
    assert entity["parent_entity_ref"] is None

    geometry = _mapping(entity["geometry"])
    assert "units" in geometry
    assert "geometry_summary" in geometry

    properties = _mapping(entity["properties"])
    assert "source_type" in properties
    assert "source_handle" in properties
    assert "adapter_native" in properties

    provenance = _mapping(entity["provenance"])
    assert provenance["origin"] == "adapter_normalized"
    assert provenance["adapter"] == {"key": "ezdxf"}
    assert provenance["adapter_key"] == "ezdxf"
    assert provenance["source_ref"] == provenance["source_entity_ref"]
    assert provenance["source"] == provenance["source_ref"]
    assert provenance["source_identity"] == entity["handle"]
    assert provenance["source_hash"] == provenance["normalized_source_hash"]
    assert provenance["extraction_path"] == ("modelspace", str(properties["source_type"]))
    assert isinstance(provenance["notes"], tuple)
    assert provenance["dxf_handle"] == entity["handle"]
    extra = _mapping(provenance["extra"])
    native = _mapping(extra["native"])
    ezdxf_native = _mapping(native["ezdxf"])
    legacy_aliases = _mapping(extra["legacy_aliases"])
    assert ezdxf_native == {
        "dxf_handle": entity["handle"],
        "native_entity_type": properties["source_type"],
        "layout_name": layout_ref,
        "layer_name": layer_ref,
    }
    assert legacy_aliases == {
        "adapter_key": "ezdxf",
        "source": provenance["source_ref"],
        "source_entity_ref": provenance["source_ref"],
        "normalized_source_hash": provenance["source_hash"],
    }
    assert isinstance(provenance["normalized_source_hash"], str)
    assert len(provenance["normalized_source_hash"]) == 64


def _load_ezdxf_adapter() -> IngestionAdapter:
    return load_adapter(get_descriptor(input_family()))


def input_family() -> InputFamily:
    return InputFamily.DXF


def test_ezdxf_entity_fingerprint_hashes_canonical_json_payload() -> None:
    geometry: dict[str, JSONValue] = {
        "end": {"z": 4.0, "y": -3.0, "x": 7.5},
        "start": {"z": 1.0, "y": 2.5, "x": -1.25},
    }
    expected_geometry: dict[str, JSONValue] = {
        "start": {"x": -1.25, "y": 2.5, "z": 1.0},
        "end": {"x": 7.5, "y": -3.0, "z": 4.0},
    }
    expected_payload: dict[str, JSONValue] = {
        "native_type": "LINE",
        "handle": "",
        "layout_name": "Model",
        "layer_name": "Walls",
        "geometry": expected_geometry,
    }
    expected_hash = hashlib.sha256(
        json.dumps(expected_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()

    assert (
        ezdxf_adapter_module._entity_fingerprint(
            native_type="LINE",
            handle="",
            layout_name="Model",
            layer_name="Walls",
            geometry=geometry,
        )
        == expected_hash
    )
    assert (
        ezdxf_adapter_module._entity_fingerprint(
            native_type="LINE",
            handle="",
            layout_name="Model",
            layer_name="Walls",
            geometry=expected_geometry,
        )
        == expected_hash
    )


def test_ezdxf_line_bbox_payload_uses_coordinate_min_max() -> None:
    start: dict[str, JSONValue] = {"x": 7.5, "y": 2.5, "z": 4.0}
    end: dict[str, JSONValue] = {"x": -1.25, "y": -3.0, "z": 1.0}

    assert ezdxf_adapter_module._bbox_payload(start, end) == {
        "min": {"x": -1.25, "y": -3.0, "z": 1.0},
        "max": {"x": 7.5, "y": 2.5, "z": 4.0},
    }


def test_canonical_bbox_from_points_uses_coordinate_min_max() -> None:
    points = (
        {"x": 7.5, "y": 2.5, "z": 4.0},
        {"x": -1.25, "y": -3.0, "z": 1.0},
        {"x": 0.0, "y": 9.0, "z": -2.0},
    )

    assert canonical_bbox_from_points(points) == {
        "min": {"x": -1.25, "y": -3.0, "z": -2.0},
        "max": {"x": 7.5, "y": 9.0, "z": 4.0},
    }


def test_ezdxf_polyline_bbox_payload_uses_pointwise_coordinate_min_max() -> None:
    points: tuple[dict[str, JSONValue], ...] = (
        {"x": 7.5, "y": 2.5, "z": 4.0},
        {"x": -1.25, "y": -3.0, "z": 1.0},
        {"x": 0.0, "y": 9.0, "z": -2.0},
    )

    assert ezdxf_adapter_module._points_bbox_payload(points) == {
        "min": {"x": -1.25, "y": -3.0, "z": -2.0},
        "max": {"x": 7.5, "y": 9.0, "z": 4.0},
    }


@pytest.mark.asyncio
async def test_create_adapter_loads_through_loader() -> None:
    adapter = _load_ezdxf_adapter()

    availability = adapter.probe()

    assert adapter.descriptor.key == "ezdxf"
    assert availability.status.value == "available"
    assert availability.details is not None
    assert availability.details["package"] == "ezdxf"
    assert availability.details["package_version"]
    assert availability.observed[0].name == "ezdxf"


def test_create_adapter_runtime_load_failure_is_sanitized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_path = "/Users/x/private/.venv/lib/python3.12/site-packages/ezdxf"

    def _raise_runtime_load_error() -> NoReturn:
        raise ModuleNotFoundError(f"No module named 'ezdxf' from {runtime_path}")

    monkeypatch.setattr(
        "app.ingestion.adapters.ezdxf._load_runtime",
        _raise_runtime_load_error,
    )

    with pytest.raises(RuntimeError, match="DXF adapter runtime could not be loaded\\.") as exc:
        _load_ezdxf_adapter()

    assert runtime_path not in str(exc.value)


@pytest.mark.asyncio
async def test_ezdxf_adapter_emits_canonical_line_geometry_for_smoke_fixture() -> None:
    adapter = _load_ezdxf_adapter()
    progress_updates: list[ProgressUpdate] = []
    source = build_contract_source(
        file_path=_FIXTURE_PATH,
        original_name="simple-line.dxf",
    )

    result = await adapter.ingest(
        source,
        AdapterExecutionOptions(
            timeout=AdapterTimeout(seconds=1),
            on_progress=progress_updates.append,
        ),
    )

    assert result.confidence is not None
    assert result.confidence.score is not None and result.confidence.score >= 0.95
    assert result.confidence.review_required is False
    assert result.warnings == ()
    assert [diagnostic.code for diagnostic in result.diagnostics] == [
        "dxf_document_loaded",
        "dxf_entities_extracted",
    ]
    assert [update.stage for update in progress_updates] == [
        "load",
        "extract",
        "extract",
        "finalize",
    ]
    assert progress_updates[-1].percent == 1.0

    units = _mapping(result.canonical["units"])
    assert units["normalized"] == "meter"
    assert units["source"] == "$INSUNITS"
    assert units["source_value"] == 6
    assert units["conversion_factor"] == 1.0

    layouts = _mapping_tuple(result.canonical["layouts"])
    assert [layout["name"] for layout in layouts] == ["Model", "Layout1"]

    layers = _mapping_tuple(result.canonical["layers"])
    assert [layer["name"] for layer in layers] == ["0"]

    blocks = result.canonical["blocks"]
    assert blocks == ()
    assert result.canonical["xrefs"] == ()

    entities = _mapping_tuple(result.canonical["entities"])
    assert len(entities) == 1
    entity = _mapping(entities[0])
    _assert_common_entity_contract(
        entity,
        entity_type="line",
        layout_ref="Model",
        layer_ref="0",
    )
    assert entity["kind"] == "line"
    assert entity["entity_type"] == "line"
    assert entity["handle"] == "1"
    assert entity["layer"] == "0"
    assert entity["layout"] == "Model"
    assert entity["start"] == {"x": 0.0, "y": 0.0, "z": 0.0}
    assert entity["end"] == {"x": 10.0, "y": 0.0, "z": 0.0}
    assert entity["length"] == pytest.approx(10.0)

    geometry = _mapping(entity["geometry"])
    assert geometry["start"] == entity["start"]
    assert geometry["end"] == entity["end"]
    assert geometry["bbox"] == {
        "min": {"x": 0.0, "y": 0.0, "z": 0.0},
        "max": {"x": 10.0, "y": 0.0, "z": 0.0},
    }
    assert geometry["units"] == units
    assert geometry["geometry_summary"] == {
        "kind": "line_segment",
        "length": 10.0,
        "vertex_count": 2,
    }

    properties = _mapping(entity["properties"])
    assert properties["source_type"] == "LINE"
    assert properties["source_handle"] == "1"
    assert properties["quantity_hints"] == {"length": 10.0, "count": 1.0}

    provenance = result.provenance[0]
    assert provenance.adapter_key == "ezdxf"
    assert provenance.source_ref == "originals/simple-line.dxf"


@pytest.mark.asyncio
async def test_ezdxf_adapter_normalizes_non_meter_line_geometry_to_meters(
    tmp_path: Path,
) -> None:
    adapter = _load_ezdxf_adapter()
    source_path = tmp_path / "inch-line.dxf"
    document = cast(Any, ezdxf).new(units=1)
    document.modelspace().add_line((0.0, 0.0, 0.0), (12.0, 0.0, 0.0))
    document.saveas(source_path)

    result = await adapter.ingest(
        build_contract_source(file_path=source_path, original_name=source_path.name),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )

    units = _mapping(result.canonical["units"])
    assert units == {
        "normalized": "meter",
        "source": "$INSUNITS",
        "source_value": 1,
        "conversion_target": "meter",
        "conversion_factor": pytest.approx(0.0254),
    }

    entity = _mapping(_mapping_tuple(result.canonical["entities"])[0])
    assert entity["start"] == {"x": 0.0, "y": 0.0, "z": 0.0}
    assert entity["end"] == {
        "x": pytest.approx(0.3048),
        "y": 0.0,
        "z": 0.0,
    }
    assert entity["length"] == pytest.approx(0.3048)
    assert _mapping(entity["properties"])["quantity_hints"] == {
        "length": pytest.approx(0.3048),
        "count": 1.0,
    }

    native = _mapping(_mapping(_mapping(entity["properties"])["adapter_native"])["ezdxf"])
    assert native["geometry"] == {
        "start": {"x": 0.0, "y": 0.0, "z": 0.0},
        "end": {"x": 12.0, "y": 0.0, "z": 0.0},
        "length": 12.0,
        "units": {"normalized": "inch", "source_value": 1},
    }


@pytest.mark.asyncio
async def test_ezdxf_adapter_attaches_entity_style_and_linetype_table(
    tmp_path: Path,
) -> None:
    """DXF entities carry the same style block as DWG; ByLayer inheritance + dashed (#574)."""
    adapter = _load_ezdxf_adapter()
    source_path = tmp_path / "styled.dxf"
    document = cast(Any, ezdxf).new(setup=True)  # standard linetypes incl. DASHED
    document.layers.add("PHANTOM", color=5, linetype="DASHED", lineweight=13)
    msp = document.modelspace()
    # ByLayer line on a dashed layer → inherits DASHED + color 5 + lw 0.13
    msp.add_line((0.0, 0.0), (1.0, 0.0), dxfattribs={"layer": "PHANTOM"})
    # explicit color + continuous linetype + explicit lineweight
    msp.add_line(
        (0.0, 0.0),
        (2.0, 0.0),
        dxfattribs={"layer": "0", "color": 1, "linetype": "Continuous", "lineweight": 25},
    )
    document.saveas(source_path)

    result = await adapter.ingest(
        build_contract_source(file_path=source_path, original_name=source_path.name),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )

    linetypes = {
        _mapping(lt)["name"]: _mapping(lt)["dashed"]
        for lt in _mapping_tuple(result.canonical["linetypes"])
    }
    assert linetypes.get("DASHED") is True
    assert linetypes.get("Continuous") is False

    entities = _mapping_tuple(result.canonical["entities"])
    by_layer_style = _mapping(_mapping(entities[0])["style"])
    assert _mapping(by_layer_style["linetype"]) == {
        "name": "DASHED",
        "by_layer": True,
        "dashed": True,
        "scale": 1.0,
    }
    by_layer_color = _mapping(by_layer_style["color"])
    assert by_layer_color["index"] == 5
    assert by_layer_color["by_layer"] is True
    assert _mapping(by_layer_style["lineweight"]) == {"raw": 13, "mm": 0.13}

    explicit_style = _mapping(_mapping(entities[1])["style"])
    explicit_color = _mapping(explicit_style["color"])
    assert explicit_color["index"] == 1
    assert explicit_color["by_layer"] is False
    assert _mapping(explicit_style["linetype"])["dashed"] is False
    assert _mapping(explicit_style["lineweight"]) == {"raw": 25, "mm": 0.25}


@pytest.mark.asyncio
async def test_ezdxf_adapter_emits_closed_lwpolyline_perimeter_geometry(
    tmp_path: Path,
) -> None:
    adapter = _load_ezdxf_adapter()
    source_path = tmp_path / "closed-lwpolyline.dxf"
    document = cast(Any, ezdxf).new(units=6)
    document.modelspace().add_lwpolyline(
        [(0.0, 0.0), (2.0, 0.0), (2.0, 1.0), (0.0, 1.0)],
        close=True,
    )
    document.saveas(source_path)

    result = await adapter.ingest(
        build_contract_source(file_path=source_path, original_name=source_path.name),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )

    assert result.confidence is not None
    assert result.confidence.review_required is False
    assert result.warnings == ()

    entity = _mapping(_mapping_tuple(result.canonical["entities"])[0])
    _assert_common_entity_contract(
        entity,
        entity_type="polyline",
        layout_ref="Model",
        layer_ref="0",
    )
    assert entity["kind"] == "polyline"
    assert entity["closed"] is True
    assert "length" not in entity
    assert entity["perimeter"] == pytest.approx(6.0)

    points = _mapping_tuple(entity["points"])
    assert points == (
        {"x": 0.0, "y": 0.0, "z": 0.0},
        {"x": 2.0, "y": 0.0, "z": 0.0},
        {"x": 2.0, "y": 1.0, "z": 0.0},
        {"x": 0.0, "y": 1.0, "z": 0.0},
    )
    assert entity["vertices"] == entity["points"]

    geometry = _mapping(entity["geometry"])
    assert geometry["points"] == entity["points"]
    assert geometry["vertices"] == entity["vertices"]
    assert geometry["closed"] is True
    assert geometry["bbox"] == {
        "min": {"x": 0.0, "y": 0.0, "z": 0.0},
        "max": {"x": 2.0, "y": 1.0, "z": 0.0},
    }
    assert geometry["geometry_summary"] == {
        "kind": "polyline",
        "vertex_count": 4,
        "closed": True,
        "dimensionality": 2,
        "perimeter": 6.0,
    }

    properties = _mapping(entity["properties"])
    assert properties["source_type"] == "LWPOLYLINE"
    assert properties["source_handle"] == entity["handle"]
    assert properties["quantity_hints"] == {"perimeter": 6.0, "count": 1.0}

    native = _mapping(_mapping(_mapping(entity["properties"])["adapter_native"])["ezdxf"])
    assert native == {
        "layer": "0",
        "linetype": "BYLAYER",
        "flags": 1,
        "closed": True,
    }


@pytest.mark.asyncio
async def test_ezdxf_adapter_emits_open_legacy_polyline_length_geometry(
    tmp_path: Path,
) -> None:
    adapter = _load_ezdxf_adapter()
    source_path = tmp_path / "open-legacy-polyline.dxf"
    document = cast(Any, ezdxf).new(units=6)
    document.modelspace().add_polyline3d([(0.0, 0.0, 0.0), (3.0, 0.0, 4.0), (3.0, 4.0, 4.0)])
    document.saveas(source_path)

    result = await adapter.ingest(
        build_contract_source(file_path=source_path, original_name=source_path.name),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )

    assert result.confidence is not None
    assert result.confidence.review_required is False
    assert result.warnings == ()

    entity = _mapping(_mapping_tuple(result.canonical["entities"])[0])
    _assert_common_entity_contract(
        entity,
        entity_type="polyline",
        layout_ref="Model",
        layer_ref="0",
    )
    assert entity["kind"] == "polyline"
    assert entity["closed"] is False
    assert entity["length"] == pytest.approx(9.0)
    assert "perimeter" not in entity

    points = _mapping_tuple(entity["points"])
    assert points == (
        {"x": 0.0, "y": 0.0, "z": 0.0},
        {"x": 3.0, "y": 0.0, "z": 4.0},
        {"x": 3.0, "y": 4.0, "z": 4.0},
    )

    geometry = _mapping(entity["geometry"])
    assert geometry["bbox"] == {
        "min": {"x": 0.0, "y": 0.0, "z": 0.0},
        "max": {"x": 3.0, "y": 4.0, "z": 4.0},
    }
    assert geometry["geometry_summary"] == {
        "kind": "polyline",
        "vertex_count": 3,
        "closed": False,
        "dimensionality": 3,
        "length": 9.0,
    }

    properties = _mapping(entity["properties"])
    assert properties["source_type"] == "POLYLINE"
    assert properties["source_handle"] == entity["handle"]
    assert properties["quantity_hints"] == {"length": 9.0, "count": 1.0}

    native = _mapping(_mapping(_mapping(entity["properties"])["adapter_native"])["ezdxf"])
    assert native == {
        "layer": "0",
        "linetype": "BYLAYER",
        "flags": 8,
        "closed": False,
        "mode": "3d",
    }


def test_ezdxf_adapter_sanitizes_structure_errors() -> None:
    class _FakeDXFStructureError(Exception):
        pass

    runtime = types.SimpleNamespace(
        ezdxf=types.SimpleNamespace(
            __version__="test",
            readfile=lambda _path: (_ for _ in ()).throw(_FakeDXFStructureError("boom")),
        ),
        units=types.SimpleNamespace(),
        dxf_structure_error=_FakeDXFStructureError,
    )
    adapter = EzdxfAdapter(cast(Any, runtime))

    with pytest.raises(RuntimeError, match="DXF source structure is invalid\\."):
        adapter._read_document(Path("broken.dxf"))


@pytest.mark.asyncio
async def test_ezdxf_adapter_review_gates_malformed_numeric_coordinates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _load_ezdxf_adapter()
    malformed_document = _FakeDocument(
        entities=(
            _FakeEntity(
                entity_type="LINE",
                handle="10",
                layer="0",
                layout_name="Model",
                start=_fake_point(float("nan"), 0.0, 0.0),
                end=_fake_point(2.0, 0.0, 0.0),
            ),
        ),
    )
    monkeypatch.setattr(adapter, "_read_document", lambda _path: malformed_document)

    result = await adapter.ingest(
        build_contract_source(file_path=_FIXTURE_PATH, original_name="malformed.dxf"),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )

    assert result.confidence is not None
    assert result.confidence.review_required is True
    assert [warning.code for warning in result.warnings] == ["malformed_coordinates"]

    entity = _mapping(_mapping_tuple(result.canonical["entities"])[0])
    assert entity["entity_type"] == "unknown"
    assert entity["kind"] == "unknown"
    assert entity["handle"] == "10"
    assert _mapping(entity["properties"])["source_type"] == "LINE"
    assert _mapping(entity["geometry"])["bbox"] is None
    assert "start" not in entity
    assert _mapping(result.warnings[0].details)["reason"] == "Point component 'x' must be finite."
    _assert_no_nonfinite_numbers(result.canonical)


@pytest.mark.asyncio
async def test_ezdxf_adapter_review_gates_malformed_polyline_coordinates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _load_ezdxf_adapter()
    malformed_document = _FakeDocument(
        entities=(
            _FakePolylineEntity(
                handle="13",
                layer="0",
                layout_name="Model",
                points=(
                    _fake_point(float("nan"), 0.0, 0.0),
                    _fake_point(2.0, 0.0, 0.0),
                ),
                is_2d_polyline=False,
                is_3d_polyline=True,
            ),
        ),
    )
    monkeypatch.setattr(adapter, "_read_document", lambda _path: malformed_document)

    result = await adapter.ingest(
        build_contract_source(file_path=_FIXTURE_PATH, original_name="malformed-polyline.dxf"),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )

    assert result.confidence is not None
    assert result.confidence.review_required is True
    assert [warning.code for warning in result.warnings] == ["malformed_coordinates"]

    entity = _mapping(_mapping_tuple(result.canonical["entities"])[0])
    assert entity["entity_type"] == "unknown"
    assert entity["kind"] == "unknown"
    assert entity["handle"] == "13"
    assert _mapping(entity["properties"])["source_type"] == "POLYLINE"
    assert _mapping(entity["geometry"])["bbox"] is None
    assert "points" not in entity
    assert _mapping(result.warnings[0].details)["reason"] == "Point component 'x' must be finite."
    _assert_no_nonfinite_numbers(result.canonical)


@pytest.mark.asyncio
async def test_ezdxf_adapter_retains_unsupported_polyline_as_unknown_with_warning(
    tmp_path: Path,
) -> None:
    adapter = _load_ezdxf_adapter()
    source_path = tmp_path / "unsupported-bulged-polyline.dxf"
    document = cast(Any, ezdxf).new(units=6)
    document.modelspace().add_lwpolyline(
        [(0.0, 0.0, 1.0), (1.0, 0.0, 0.0), (1.0, 1.0, 0.0)],
        format="xyb",
    )
    document.saveas(source_path)

    result = await adapter.ingest(
        build_contract_source(file_path=source_path, original_name=source_path.name),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )

    assert result.confidence is not None
    assert result.confidence.review_required is True
    assert result.confidence.score == 0.95
    assert [warning.code for warning in result.warnings] == ["unsupported_entity"]

    entity = _mapping(_mapping_tuple(result.canonical["entities"])[0])
    _assert_common_entity_contract(
        entity,
        entity_type="unknown",
        layout_ref="Model",
        layer_ref="0",
    )
    assert entity["kind"] == "unknown"
    assert _mapping(entity["properties"])["source_type"] == "LWPOLYLINE"
    assert (
        _mapping(result.warnings[0].details)["reason"] == "Polyline bulge arcs are not supported."
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("const_width", "segment_widths"),
    [
        (0.5, None),
        (0.0, ((0.5, 0.0), (0.0, 0.0))),
    ],
)
async def test_ezdxf_adapter_retains_width_bearing_lwpolyline_as_unknown_with_warning(
    monkeypatch: pytest.MonkeyPatch,
    const_width: float,
    segment_widths: tuple[tuple[float, float], ...] | None,
) -> None:
    adapter = _load_ezdxf_adapter()
    unsupported_document = _FakeDocument(
        entities=(
            _FakeLWPolylineEntity(
                handle="14",
                layer="0",
                layout_name="Model",
                points=(
                    _fake_point(0.0, 0.0, 0.0),
                    _fake_point(2.0, 0.0, 0.0),
                ),
                const_width=const_width,
                segment_widths=segment_widths,
            ),
        ),
    )
    monkeypatch.setattr(adapter, "_read_document", lambda _path: unsupported_document)

    result = await adapter.ingest(
        build_contract_source(
            file_path=_FIXTURE_PATH,
            original_name="width-bearing-lwpolyline.dxf",
        ),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )

    assert result.confidence is not None
    assert result.confidence.review_required is True
    assert [warning.code for warning in result.warnings] == ["unsupported_entity"]

    entity = _mapping(_mapping_tuple(result.canonical["entities"])[0])
    assert entity["entity_type"] == "unknown"
    assert entity["kind"] == "unknown"
    assert _mapping(entity["properties"])["source_type"] == "LWPOLYLINE"
    assert _mapping(result.warnings[0].details)["reason"] == "Polyline widths are not supported."
    _assert_no_nonfinite_numbers(result.canonical)


@pytest.mark.asyncio
async def test_ezdxf_adapter_retains_width_bearing_legacy_polyline_as_unknown_with_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _load_ezdxf_adapter()
    unsupported_document = _FakeDocument(
        entities=(
            _FakePolylineEntity(
                handle="15",
                layer="0",
                layout_name="Model",
                points=(
                    _fake_point(0.0, 0.0, 0.0),
                    _fake_point(3.0, 0.0, 0.0),
                ),
                vertex_widths=((0.0, 0.0), (0.5, 0.25)),
            ),
        ),
    )
    monkeypatch.setattr(adapter, "_read_document", lambda _path: unsupported_document)

    result = await adapter.ingest(
        build_contract_source(file_path=_FIXTURE_PATH, original_name="width-bearing-polyline.dxf"),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )

    assert result.confidence is not None
    assert result.confidence.review_required is True
    assert [warning.code for warning in result.warnings] == ["unsupported_entity"]

    entity = _mapping(_mapping_tuple(result.canonical["entities"])[0])
    assert entity["entity_type"] == "unknown"
    assert entity["kind"] == "unknown"
    assert _mapping(entity["properties"])["source_type"] == "POLYLINE"
    assert _mapping(result.warnings[0].details)["reason"] == "Polyline widths are not supported."
    _assert_no_nonfinite_numbers(result.canonical)


@pytest.mark.asyncio
async def test_ezdxf_adapter_retains_vertex_capped_polyline_as_unknown_with_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _load_ezdxf_adapter()
    oversized_document = _FakeDocument(
        entities=(
            _FakeLWPolylineEntity(
                handle="16",
                layer="0",
                layout_name="Model",
                points=(
                    _fake_point(0.0, 0.0, 0.0),
                    _fake_point(1.0, 0.0, 0.0),
                    _fake_point(2.0, 0.0, 0.0),
                ),
            ),
        ),
    )
    monkeypatch.setattr(adapter, "_read_document", lambda _path: oversized_document)
    monkeypatch.setattr("app.ingestion.adapters.ezdxf._MAX_POLYLINE_VERTICES", 2)

    result = await adapter.ingest(
        build_contract_source(file_path=_FIXTURE_PATH, original_name="oversized-polyline.dxf"),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )

    assert result.confidence is not None
    assert result.confidence.review_required is True
    assert [warning.code for warning in result.warnings] == ["polyline_vertex_limit_exceeded"]

    entity = _mapping(_mapping_tuple(result.canonical["entities"])[0])
    assert entity["entity_type"] == "unknown"
    assert entity["kind"] == "unknown"
    assert _mapping(entity["properties"])["source_type"] == "LWPOLYLINE"
    assert _mapping(result.warnings[0].details)["reason"] == (
        "Polyline vertex count exceeds supported limit of 2."
    )
    _assert_no_nonfinite_numbers(result.canonical)


@pytest.mark.asyncio
async def test_ezdxf_adapter_honors_cancellation_during_polyline_vertex_iteration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _load_ezdxf_adapter()
    large_document = _FakeDocument(
        entities=(
            _FakePolylineEntity(
                handle="17",
                layer="0",
                layout_name="Model",
                points=(
                    _fake_point(0.0, 0.0, 0.0),
                    _fake_point(1.0, 0.0, 0.0),
                    _fake_point(2.0, 0.0, 0.0),
                ),
            ),
        ),
    )
    monkeypatch.setattr(adapter, "_read_document", lambda _path: large_document)
    cancellation = _CancellationAfterChecks(cancel_after=4)

    with pytest.raises(asyncio.CancelledError):
        await adapter.ingest(
            build_contract_source(file_path=_FIXTURE_PATH, original_name="cancel-polyline.dxf"),
            AdapterExecutionOptions(
                timeout=AdapterTimeout(seconds=1),
                cancellation=cancellation,
            ),
        )

    assert cancellation.calls >= 4


@pytest.mark.asyncio
async def test_ezdxf_adapter_review_gates_scaled_coordinate_overflow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _load_ezdxf_adapter()
    overflow_document = _FakeDocument(
        units=7,
        entities=(
            _FakeEntity(
                entity_type="LINE",
                handle="11",
                layer="0",
                layout_name="Model",
                start=_fake_point(1e306, 0.0, 0.0),
                end=_fake_point(0.0, 0.0, 0.0),
            ),
        ),
    )
    monkeypatch.setattr(adapter, "_read_document", lambda _path: overflow_document)

    result = await adapter.ingest(
        build_contract_source(file_path=_FIXTURE_PATH, original_name="scaled-overflow.dxf"),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )

    assert result.confidence is not None
    assert result.confidence.review_required is True
    assert [warning.code for warning in result.warnings] == ["malformed_coordinates"]

    entity = _mapping(_mapping_tuple(result.canonical["entities"])[0])
    assert entity["entity_type"] == "unknown"
    assert entity["kind"] == "unknown"
    assert entity["handle"] == "11"
    assert _mapping(entity["properties"])["source_type"] == "LINE"
    assert _mapping(entity["geometry"])["bbox"] is None
    assert "start" not in entity
    assert "end" not in entity
    assert "length" not in entity
    assert _mapping(result.warnings[0].details)["reason"] == "Point component 'x' must be finite."
    _assert_no_nonfinite_numbers(result.canonical)


@pytest.mark.asyncio
async def test_ezdxf_adapter_review_gates_scaled_length_overflow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _load_ezdxf_adapter()
    overflow_document = _FakeDocument(
        units=17,
        entities=(
            _FakeEntity(
                entity_type="LINE",
                handle="12",
                layer="0",
                layout_name="Model",
                start=_fake_point(0.0, 0.0, 0.0),
                end=_fake_point(2e145, 0.0, 0.0),
            ),
        ),
    )
    monkeypatch.setattr(adapter, "_read_document", lambda _path: overflow_document)

    result = await adapter.ingest(
        build_contract_source(file_path=_FIXTURE_PATH, original_name="scaled-length-overflow.dxf"),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )

    assert result.confidence is not None
    assert result.confidence.review_required is True
    assert [warning.code for warning in result.warnings] == ["malformed_coordinates"]

    entity = _mapping(_mapping_tuple(result.canonical["entities"])[0])
    assert entity["entity_type"] == "unknown"
    assert entity["kind"] == "unknown"
    assert entity["handle"] == "12"
    assert _mapping(entity["properties"])["source_type"] == "LINE"
    assert _mapping(entity["geometry"])["bbox"] is None
    assert "start" not in entity
    assert "end" not in entity
    assert "length" not in entity
    assert _mapping(result.warnings[0].details)["reason"] == "Line length must be finite."
    _assert_no_nonfinite_numbers(result.canonical)


@pytest.mark.asyncio
async def test_ezdxf_adapter_omits_scaled_block_base_point_overflow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _load_ezdxf_adapter()
    overflow_document = _FakeDocument(
        entities=(),
        units=17,
        blocks=(
            _FakeBlock(
                name="overflow-block",
                is_xref=False,
                base_point=_fake_point(1e300, 2.0, 3.0),
            ),
        ),
    )
    monkeypatch.setattr(adapter, "_read_document", lambda _path: overflow_document)

    result = await adapter.ingest(
        build_contract_source(file_path=_FIXTURE_PATH, original_name="block-overflow.dxf"),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )

    assert _mapping_tuple(result.canonical["blocks"]) == (
        {
            "name": "overflow-block",
            "base_point": None,
            "entity_count": 0,
        },
    )
    _assert_no_nonfinite_numbers(result.canonical)


@pytest.mark.asyncio
async def test_ezdxf_adapter_sanitizes_xref_paths_and_review_gates_unresolved_refs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _load_ezdxf_adapter()
    xref_document = _FakeDocument(
        entities=(),
        blocks=(
            _FakeBlock(
                name="site-plan",
                is_xref=True,
                xref_path="/Users/x/private/client/site/base-plan.dxf",
            ),
            _FakeBlock(name="missing-ref", is_xref=True, xref_path=""),
        ),
    )
    monkeypatch.setattr(adapter, "_read_document", lambda _path: xref_document)

    result = await adapter.ingest(
        build_contract_source(file_path=_FIXTURE_PATH, original_name="xrefed.dxf"),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )

    assert result.confidence is not None
    assert result.confidence.review_required is True
    assert [warning.code for warning in result.warnings] == ["xref_unresolved"]
    assert _mapping(result.warnings[0].details)["xref_count"] == 2

    xrefs = _mapping_tuple(result.canonical["xrefs"])
    assert xrefs == (
        {
            "name": "site-plan",
            "reference": "base-plan.dxf",
            "path_sha256": cast(str, xrefs[0]["path_sha256"]),
            "status": "review_required",
        },
        {
            "name": "missing-ref",
            "reference": None,
            "path_sha256": None,
            "status": "review_required",
        },
    )
    assert isinstance(xrefs[0]["path_sha256"], str)
    assert len(cast(str, xrefs[0]["path_sha256"])) == 64
    assert "/Users/x/private/client/site/base-plan.dxf" not in str(result.canonical["xrefs"])


@pytest.mark.asyncio
async def test_ezdxf_adapter_passes_shared_contract_harness_for_smoke_fixture() -> None:
    adapter = _load_ezdxf_adapter()
    source = build_contract_source(
        file_path=_FIXTURE_PATH,
        original_name="simple-line.dxf",
    )

    payload = await exercise_adapter_contract(
        adapter,
        source=source,
        input_family=input_family(),
        expectation=ContractFinalizationExpectation(
            validation_status="valid",
            diagnostic_codes=("dxf_document_loaded", "dxf_entities_extracted"),
        ),
        adapter_key="ezdxf",
    )

    assert payload.canonical_json["canonical_entity_schema_version"] == "0.1"
    assert payload.report_json["summary"]["entity_counts"] == {
        "layouts": 2,
        "layers": 1,
        "blocks": 0,
        "entities": 1,
    }
    assert payload.validation_status == "valid"


@pytest.mark.asyncio
async def test_ezdxf_adapter_empty_modelspace_passes_shared_contract_harness(
    tmp_path: Path,
) -> None:
    adapter = _load_ezdxf_adapter()
    source_path = tmp_path / "empty-modelspace.dxf"
    cast(Any, ezdxf).new(units=6).saveas(source_path)

    payload = await exercise_adapter_contract(
        adapter,
        source=build_contract_source(file_path=source_path, original_name=source_path.name),
        input_family=input_family(),
        expectation=ContractFinalizationExpectation(
            validation_status="needs_review",
            diagnostic_codes=("dxf_document_loaded", "dxf_entities_extracted"),
        ),
        adapter_key="ezdxf",
    )

    assert payload.canonical_json["entities"] == []
    assert payload.canonical_json["metadata"]["empty_entities_reason"] == "dxf_modelspace_empty"


@pytest.mark.asyncio
async def test_ezdxf_adapter_retains_unsupported_entities_as_unknown_with_warning(
    tmp_path: Path,
) -> None:
    adapter = _load_ezdxf_adapter()
    source_path = tmp_path / "unsupported-circle.dxf"
    ezdxf_module = cast(Any, ezdxf)
    document = ezdxf_module.new(units=6)
    document.modelspace().add_circle((2.0, 3.0), radius=4.0)
    document.saveas(source_path)
    source = build_contract_source(
        file_path=source_path,
        original_name=source_path.name,
    )

    result = await adapter.ingest(
        source,
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )

    assert result.confidence is not None
    assert result.confidence.review_required is True
    assert result.confidence.score == 0.95
    assert [warning.code for warning in result.warnings] == ["unsupported_entity"]
    assert result.canonical["layers"] == ({"name": "0", "color": 7, "linetype": "Continuous"},)

    entities = _mapping_tuple(result.canonical["entities"])
    assert len(entities) == 1
    entity = _mapping(entities[0])
    _assert_common_entity_contract(
        entity,
        entity_type="unknown",
        layout_ref="Model",
        layer_ref="0",
    )
    assert entity["kind"] == "unknown"
    assert entity["entity_type"] == "unknown"
    assert entity["layer"] == "0"
    assert _mapping(entity["properties"])["source_type"] == "CIRCLE"
    assert _mapping(entity["geometry"])["reason"] == "unsupported_or_invalid_geometry"
    assert _mapping(entity["geometry"])["geometry_summary"] == {
        "kind": "unknown",
        "source_type": "CIRCLE",
        "reason": "unsupported_or_invalid_geometry",
    }
    assert result.warnings[0].details == {
        "entity_type": "CIRCLE",
        "handle": cast(str, entity["handle"]),
        "layer": "0",
        "layout": "Model",
    }


@pytest.mark.asyncio
async def test_ezdxf_adapter_honors_cancellation_checkpoints() -> None:
    adapter = _load_ezdxf_adapter()
    source = build_contract_source(
        file_path=_FIXTURE_PATH,
        original_name="simple-line.dxf",
    )
    cancellation = _CancellationAfterChecks(cancel_after=3)

    with pytest.raises(asyncio.CancelledError):
        await adapter.ingest(
            source,
            AdapterExecutionOptions(
                timeout=AdapterTimeout(seconds=1),
                cancellation=cancellation,
            ),
        )

    assert cancellation.calls >= 3


# ---------------------------------------------------------------------------
# Unit inference — unconfirmed INSUNITS path (#558, P1)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ezdxf_adapter_infers_millimeter_from_building_scale_geometry(
    tmp_path: Path,
) -> None:
    """INSUNITS=0 (unitless) + building-scale entities → inferred millimeter."""
    adapter = _load_ezdxf_adapter()
    source_path = tmp_path / "unconfirmed-mm.dxf"
    # units=0 → unitless / unconfirmed
    document = cast(Any, ezdxf).new(units=0)
    msp = document.modelspace()
    # ~40 m x 20 m floor plan drawn in mm: 40 000 x 20 000 raw units
    msp.add_line((0.0, 0.0, 0.0), (40_000.0, 0.0, 0.0))
    msp.add_line((0.0, 0.0, 0.0), (0.0, 20_000.0, 0.0))
    document.saveas(source_path)

    result = await adapter.ingest(
        build_contract_source(file_path=source_path, original_name=source_path.name),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )

    units = _mapping(result.canonical["units"])
    assert units["confidence"] == "inferred"
    assert units["normalized"] == "millimeter"
    assert units["conversion_factor"] == pytest.approx(0.001)
    assert "basis" in units
    basis = cast(str, units["basis"])
    assert "millimeter" in basis
    inference = _mapping(units["inference"])
    extent_block = _mapping(inference["extent"])
    assert extent_block["width"] == pytest.approx(40_000.0)
    assert extent_block["height"] == pytest.approx(20_000.0)
    candidates = cast("tuple[object, ...]", inference["candidates"])
    assert len(candidates) >= 1
    first_candidate = _mapping(candidates[0])
    assert first_candidate["normalized"] == "millimeter"
    # contradiction not present — no declared code to contradict
    assert "contradiction" not in units


@pytest.mark.asyncio
async def test_ezdxf_adapter_confirmed_units_unchanged_by_inference(
    tmp_path: Path,
) -> None:
    """Confirmed INSUNITS (millimeter, code=4) must not acquire inference keys."""
    adapter = _load_ezdxf_adapter()
    source_path = tmp_path / "confirmed-mm.dxf"
    document = cast(Any, ezdxf).new(units=4)
    document.modelspace().add_line((0.0, 0.0, 0.0), (40_000.0, 0.0, 0.0))
    document.saveas(source_path)

    result = await adapter.ingest(
        build_contract_source(file_path=source_path, original_name=source_path.name),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )

    units = _mapping(result.canonical["units"])
    assert units["normalized"] == "meter"
    assert units["source_value"] == 4
    assert units["conversion_factor"] == pytest.approx(0.001)
    # No inference keys on the confirmed path.
    assert "confidence" not in units
    assert "inference" not in units
    assert "contradiction" not in units
    assert "basis" not in units


@pytest.mark.asyncio
async def test_ezdxf_adapter_unknown_confidence_when_no_usable_bboxes(
    tmp_path: Path,
) -> None:
    """INSUNITS=0 with an empty modelspace → no inference, units stays normalized=unitless."""
    adapter = _load_ezdxf_adapter()
    source_path = tmp_path / "empty-unconfirmed.dxf"
    document = cast(Any, ezdxf).new(units=0)
    # No entities → no bboxes → extent=None → inference result confidence="unknown"
    document.saveas(source_path)

    result = await adapter.ingest(
        build_contract_source(file_path=source_path, original_name=source_path.name),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )

    units = _mapping(result.canonical["units"])
    # No inferred keys when extent is unavailable.
    assert units["normalized"] == "unitless"
    assert "confidence" not in units
    assert "inference" not in units


# ---------------------------------------------------------------------------
# P1 additions — contradiction path + confirmed-path full-dict equality (#600)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ezdxf_adapter_emits_contradiction_when_inferred_contradicts_declared(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unconfirmed units with a METER_SCALE declared code + mm-scale geometry → contradiction.

    ezdxf confirms every METER_SCALE code, so the unconfirmed path is only reachable
    when conversion_factor is None (e.g. codes 0, 8, 9 not in METER_SCALE).  The
    contradiction gate in infer_units also requires the declared_code to be in
    METER_SCALE (so _resolve_declared returns a name).  To exercise the contradiction
    wiring we monkeypatch _units_payload to return a payload that is simultaneously
    unconfirmed (conversion_factor=None → units_review_required=True) and carries a
    declared source_value that IS in METER_SCALE (17 = gigameter, 1 GHz of meters),
    matching the libredwg pattern where code 17 is in METER_SCALE but outside the
    confirmed subset.
    """
    adapter = _load_ezdxf_adapter()

    # Synthesize: unconfirmed payload (conversion_factor=None) but source_value=17
    # (gigameter) which IS in METER_SCALE so infer_units can compare factors.
    # mm-scale geometry (40 000 x 20 000) → infer_units infers millimeter (0.001).
    # ratio = 0.001 / 1e9 << 0.1 → contradicts.
    fake_units: dict[str, Any] = {
        "normalized": "gigameter",
        "source": "$INSUNITS",
        "source_value": 17,
        "conversion_target": "meter",
        "conversion_factor": None,
    }
    fake_document = _FakeDocument(
        entities=(
            _FakeEntity(
                entity_type="LINE",
                handle="A1",
                layer="0",
                layout_name="Model",
                start=_fake_point(0.0, 0.0, 0.0),
                end=_fake_point(40_000.0, 0.0, 0.0),
            ),
            _FakeEntity(
                entity_type="LINE",
                handle="A2",
                layer="0",
                layout_name="Model",
                start=_fake_point(0.0, 0.0, 0.0),
                end=_fake_point(0.0, 20_000.0, 0.0),
            ),
        ),
        units=17,
    )
    monkeypatch.setattr(adapter, "_read_document", lambda _path: fake_document)
    monkeypatch.setattr(
        "app.ingestion.adapters.ezdxf._units_payload",
        lambda _doc: dict(fake_units),
    )

    result = await adapter.ingest(
        build_contract_source(file_path=_FIXTURE_PATH, original_name="gigameter-contradiction.dxf"),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )

    units = _mapping(result.canonical["units"])
    assert units["confidence"] == "inferred"
    assert units["normalized"] == "millimeter"
    contradiction = _mapping(units["contradiction"])
    assert contradiction["declared_normalized"] == "gigameter"
    assert contradiction["inferred_normalized"] == "millimeter"
    assert "basis" in contradiction

    warning_codes = [w.code for w in result.warnings]
    assert "units_unconfirmed" in warning_codes
    assert "ezdxf.units_contradiction" in warning_codes

    contradiction_warning = next(
        w for w in result.warnings if w.code == "ezdxf.units_contradiction"
    )
    cw_details = _mapping(contradiction_warning.details)
    assert cw_details["declared_normalized"] == "gigameter"
    assert cw_details["inferred_normalized"] == "millimeter"


@pytest.mark.asyncio
async def test_ezdxf_adapter_confirmed_units_exact_dict_equality(
    tmp_path: Path,
) -> None:
    """Confirmed INSUNITS must produce an exact units dict — no extra keys on the confirmed path.

    This is a stricter sibling of test_ezdxf_adapter_confirmed_units_unchanged_by_inference:
    instead of checking key absence individually, it asserts full dict equality so that
    accidentally adding an extra key to the confirmed path fails immediately.
    """
    adapter = _load_ezdxf_adapter()
    source_path = tmp_path / "confirmed-mm-exact.dxf"
    document = cast(Any, ezdxf).new(units=4)  # millimeter, code 4
    document.modelspace().add_line((0.0, 0.0, 0.0), (40_000.0, 0.0, 0.0))
    document.saveas(source_path)

    result = await adapter.ingest(
        build_contract_source(file_path=source_path, original_name=source_path.name),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )

    units = _mapping(result.canonical["units"])
    assert units == {
        "normalized": "meter",
        "source": "$INSUNITS",
        "source_value": 4,
        "conversion_target": "meter",
        "conversion_factor": pytest.approx(0.001),
    }


# ---------------------------------------------------------------------------
# P2 -- DIMENSION extraction + Tier-1 unit inference (#558)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ezdxf_dimension_extracted_as_first_class_entity(
    tmp_path: Path,
) -> None:
    """DIMENSION entity extracted as entity_type='dimension' with correct dimension sub-block."""
    adapter = _load_ezdxf_adapter()
    source_path = tmp_path / "dim-extraction.dxf"
    # units=0 (unconfirmed); no setup=True so Standard dimstyle has dimlfac=None -> 1.0.
    document = cast(Any, ezdxf).new(units=0)
    msp = document.modelspace()
    # 18000-mm span linear dim with explicit text override (clearly millimeter-band).
    dim = msp.add_linear_dim(base=(0.0, -1000.0), p1=(0.0, 0.0), p2=(18000.0, 0.0), text="18000")
    dim.render()
    # Building-scale lines to fill out the drawing.
    msp.add_line((0.0, 0.0, 0.0), (18000.0, 0.0, 0.0))
    msp.add_line((0.0, 0.0, 0.0), (0.0, 12000.0, 0.0))
    document.saveas(source_path)

    result = await adapter.ingest(
        build_contract_source(file_path=source_path, original_name=source_path.name),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=2)),
    )

    entities = cast("tuple[object, ...]", result.canonical["entities"])
    dim_entities = [
        _mapping(e) for e in entities if isinstance(e, dict) and e.get("entity_type") == "dimension"
    ]
    assert len(dim_entities) >= 1, "Expected at least one 'dimension' entity"

    dim_entity = dim_entities[0]
    assert dim_entity["entity_type"] == "dimension"
    assert dim_entity["entity_schema_version"] == "0.1"
    assert dim_entity["confidence"] == 0.99

    props = _mapping(dim_entity["properties"])
    adapter_native = _mapping(props["adapter_native"])
    ez = _mapping(adapter_native["ezdxf"])
    dim_block = _mapping(ez["dimension"])

    assert dim_block["raw_span"] == pytest.approx(18000.0)
    assert dim_block["text_override"] == "18000"
    assert dim_block["is_linear"] is True
    assert dim_block["dimtype_base"] == 0

    # Verify the entity is counted as supported (no unsupported_entity warnings for it).
    warning_codes = [w.code for w in result.warnings]
    assert "malformed_dimension" not in warning_codes

    # Verify diagnostics reflect it as supported.
    diagnostics_details = {d.code: d.details for d in result.diagnostics}
    entity_diag = _mapping(diagnostics_details["dxf_entities_extracted"])
    assert cast(int, entity_diag["supported_entity_count"]) >= 1


@pytest.mark.asyncio
async def test_ezdxf_tier1_fires_dimension_text_override(
    tmp_path: Path,
) -> None:
    """Tier-1 inference fires from '18000' text override and yields millimeter.

    18000 (log10=4.255) scores highest in the millimeter band
    (center=4.239) vs centimeter (center=3.239).
    """
    adapter = _load_ezdxf_adapter()
    source_path = tmp_path / "dim-tier1.dxf"
    # units=0 (unconfirmed); no setup=True so Standard dimstyle has dimlfac=None -> 1.0.
    document = cast(Any, ezdxf).new(units=0)
    msp = document.modelspace()
    dim = msp.add_linear_dim(base=(0.0, -1000.0), p1=(0.0, 0.0), p2=(18000.0, 0.0), text="18000")
    dim.render()
    msp.add_line((0.0, 0.0, 0.0), (18000.0, 0.0, 0.0))
    msp.add_line((0.0, 0.0, 0.0), (0.0, 12000.0, 0.0))
    document.saveas(source_path)

    result = await adapter.ingest(
        build_contract_source(file_path=source_path, original_name=source_path.name),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=2)),
    )

    units = _mapping(result.canonical["units"])
    assert units["confidence"] == "inferred"
    assert units["normalized"] == "millimeter"
    assert units["conversion_factor"] == pytest.approx(0.001)
    basis = cast(str, units["basis"])
    assert basis.startswith("dimension_text:")

    inference = _mapping(units["inference"])
    assert cast(int, inference["dimension_observation_count"]) >= 1


@pytest.mark.asyncio
async def test_ezdxf_tier1_outranks_tier2_extent(
    tmp_path: Path,
) -> None:
    """Tier-1 wins even when Tier-2 alone would yield a different band.

    Geometry spans ~3 raw units (meter-band by Tier-2), but the '18000' text
    override (dimlfac=1, log10=4.255) places the inference in millimeter-band
    by Tier-1.  Tier-1 must win over Tier-2.
    """
    adapter = _load_ezdxf_adapter()
    source_path = tmp_path / "dim-tier1-outranks.dxf"
    # units=0 (unconfirmed); no setup=True so Standard dimstyle has dimlfac=None -> 1.0.
    document = cast(Any, ezdxf).new(units=0)
    msp = document.modelspace()
    # 3 raw units span -- Tier-2 alone would call this meter-scale.
    # text="18000" forces Tier-1 into millimeter-band.
    dim = msp.add_linear_dim(base=(0.0, -2.0), p1=(0.0, 0.0), p2=(3.0, 0.0), text="18000")
    dim.render()
    msp.add_line((0.0, 0.0, 0.0), (3.0, 0.0, 0.0))
    msp.add_line((0.0, 0.0, 0.0), (0.0, 2.0, 0.0))
    document.saveas(source_path)

    result = await adapter.ingest(
        build_contract_source(file_path=source_path, original_name=source_path.name),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=2)),
    )

    units = _mapping(result.canonical["units"])
    assert units["confidence"] == "inferred"
    # Tier-1 wins: millimeter (from text override), not meter (from extent geometry).
    assert units["normalized"] == "millimeter"
    basis = cast(str, units["basis"])
    assert basis.startswith("dimension_text:")


@pytest.mark.asyncio
async def test_ezdxf_measured_only_dim_does_not_fire_tier1(
    tmp_path: Path,
) -> None:
    """A measured-only dim (text='<>') provides no Tier-1 vote; basis is extent or unknown."""
    adapter = _load_ezdxf_adapter()
    source_path = tmp_path / "dim-measured-only.dxf"
    document = cast(Any, ezdxf).new(units=0)
    msp = document.modelspace()
    # text="<>" means measured-only; no drafter override.
    dim = msp.add_linear_dim(base=(0.0, -1000.0), p1=(0.0, 0.0), p2=(3600.0, 0.0), text="<>")
    dim.render()
    document.saveas(source_path)

    result = await adapter.ingest(
        build_contract_source(file_path=source_path, original_name=source_path.name),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=2)),
    )

    units = _mapping(result.canonical["units"])
    # May or may not be inferred (Tier-2 might still fire from geometry),
    # but if inferred, must NOT use dimension_text basis.
    if "basis" in units:
        basis = cast(str, units["basis"])
        assert not basis.startswith("dimension_text:")


@pytest.mark.asyncio
async def test_ezdxf_non_linear_dim_extracted_but_no_tier1_vote(
    tmp_path: Path,
) -> None:
    """Radial dims produce entity_type='dimension' with is_linear=False and cast no Tier-1 vote."""
    adapter = _load_ezdxf_adapter()
    source_path = tmp_path / "dim-radial.dxf"
    document = cast(Any, ezdxf).new(units=0)
    msp = document.modelspace()
    # add_radius_dim creates a radius dimension (dimtype_base=4, is_linear=False).
    msp.add_circle((0.0, 0.0), radius=1800.0)
    dim = msp.add_radius_dim(center=(0.0, 0.0), radius=1800.0, angle=45.0, text="R1800")
    dim.render()
    document.saveas(source_path)

    result = await adapter.ingest(
        build_contract_source(file_path=source_path, original_name=source_path.name),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=2)),
    )

    entities = cast("tuple[object, ...]", result.canonical["entities"])
    dim_entities = [
        _mapping(e) for e in entities if isinstance(e, dict) and e.get("entity_type") == "dimension"
    ]
    assert len(dim_entities) >= 1

    dim_entity = dim_entities[0]
    props = _mapping(dim_entity["properties"])
    adapter_native = _mapping(props["adapter_native"])
    ez = _mapping(adapter_native["ezdxf"])
    dim_block = _mapping(ez["dimension"])
    assert dim_block["is_linear"] is False

    # Tier-1 must not have fired because the only dim is non-linear.
    units = _mapping(result.canonical["units"])
    if "basis" in units:
        basis = cast(str, units["basis"])
        assert not basis.startswith("dimension_text:")


@pytest.mark.asyncio
async def test_ezdxf_confirmed_units_unchanged_with_dim_present(
    tmp_path: Path,
) -> None:
    """Confirmed INSUNITS (mm, code=4) + a DIMENSION entity -> exact confirmed dict unchanged."""
    adapter = _load_ezdxf_adapter()
    source_path = tmp_path / "confirmed-with-dim.dxf"
    document = cast(Any, ezdxf).new(units=4)
    msp = document.modelspace()
    dim = msp.add_linear_dim(base=(0.0, -1000.0), p1=(0.0, 0.0), p2=(3600.0, 0.0), text="3600")
    dim.render()
    msp.add_line((0.0, 0.0, 0.0), (3600.0, 0.0, 0.0))
    document.saveas(source_path)

    result = await adapter.ingest(
        build_contract_source(file_path=source_path, original_name=source_path.name),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=2)),
    )

    units = _mapping(result.canonical["units"])
    assert units == {
        "normalized": "meter",
        "source": "$INSUNITS",
        "source_value": 4,
        "conversion_target": "meter",
        "conversion_factor": pytest.approx(0.001),
    }


@pytest.mark.asyncio
async def test_ezdxf_malformed_dimension_degrades_to_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A DIMENSION entity that raises during extraction degrades to unknown + warning."""
    adapter = _load_ezdxf_adapter()

    # Monkeypatch _dimension_entity_payload to always raise so we can verify the
    # try/except fallback path without synthesizing a genuinely malformed DXF.
    import app.ingestion.adapters.ezdxf as _mod

    original = _mod._dimension_entity_payload

    def _always_raise(entity: Any, **kwargs: Any) -> Any:
        raise ValueError("synthetic malformed dimension")

    monkeypatch.setattr(_mod, "_dimension_entity_payload", _always_raise)

    try:
        fake_document = _FakeDocument(
            entities=(
                _FakeEntity(
                    entity_type="DIMENSION",
                    handle="D1",
                    layer="0",
                    layout_name="Model",
                ),
            ),
            units=0,
        )
        monkeypatch.setattr(adapter, "_read_document", lambda _path: fake_document)

        result = await adapter.ingest(
            build_contract_source(file_path=_FIXTURE_PATH, original_name="malformed-dim.dxf"),
            AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
        )

        entities = cast("tuple[object, ...]", result.canonical["entities"])
        assert len(entities) == 1
        entity = _mapping(entities[0])
        assert entity["entity_type"] == "unknown"

        warning_codes = [w.code for w in result.warnings]
        assert "malformed_dimension" in warning_codes
    finally:
        monkeypatch.setattr(_mod, "_dimension_entity_payload", original)


@pytest.mark.asyncio
async def test_ezdxf_tier1_3600_infers_millimeter(
    tmp_path: Path,
) -> None:
    """Tier-1 inference with text='3600' (a typical wall span) yields millimeter.

    3600 (log10 ~ 3.556) falls in both the mm band [3.0, 5.477] and cm band
    [2.0, 4.477].  The construction-default bias picks millimeter (higher
    priority) over centimeter.
    """
    adapter = _load_ezdxf_adapter()
    source_path = tmp_path / "dim-3600-mm.dxf"
    document = cast(Any, ezdxf).new(units=0)
    msp = document.modelspace()
    dim = msp.add_linear_dim(base=(0.0, -1000.0), p1=(0.0, 0.0), p2=(3600.0, 0.0), text="3600")
    dim.render()
    msp.add_line((0.0, 0.0, 0.0), (3600.0, 0.0, 0.0))
    msp.add_line((0.0, 0.0, 0.0), (0.0, 2400.0, 0.0))
    document.saveas(source_path)

    result = await adapter.ingest(
        build_contract_source(file_path=source_path, original_name=source_path.name),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=2)),
    )

    units = _mapping(result.canonical["units"])
    assert units["confidence"] == "inferred"
    assert units["normalized"] == "millimeter"
    assert units["conversion_factor"] == pytest.approx(0.001)
    basis = cast(str, units["basis"])
    assert basis.startswith("dimension_text:")


@pytest.mark.asyncio
async def test_ezdxf_dimlfac_override_resolver(
    tmp_path: Path,
) -> None:
    """Per-dimension DIMLFAC override is resolved via DimStyleOverride.get().

    A dim with override={"dimlfac": 100.0} and text="3600" has
    display_value = 3600 / 100 = 36.0 (log10 ~ 1.556 -> meter band only).
    """
    adapter = _load_ezdxf_adapter()
    source_path = tmp_path / "dim-dimlfac-override.dxf"
    document = cast(Any, ezdxf).new(units=0)
    msp = document.modelspace()
    dim = msp.add_linear_dim(
        base=(0.0, -1000.0),
        p1=(0.0, 0.0),
        p2=(3600.0, 0.0),
        text="3600",
        override={"dimlfac": 100.0},
    )
    dim.render()
    msp.add_line((0.0, 0.0, 0.0), (3600.0, 0.0, 0.0))
    msp.add_line((0.0, 0.0, 0.0), (0.0, 2.0, 0.0))
    document.saveas(source_path)

    result = await adapter.ingest(
        build_contract_source(file_path=source_path, original_name=source_path.name),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=2)),
    )

    # Confirm the dimlfac=100 was read: display_value = 3600 / 100 = 36 -> meter band.
    units = _mapping(result.canonical["units"])
    assert units["confidence"] == "inferred"
    assert units["normalized"] == "meter"
    assert units["conversion_factor"] == pytest.approx(1.0)

    entities = cast("tuple[object, ...]", result.canonical["entities"])
    dim_entities = [
        _mapping(e) for e in entities if isinstance(e, dict) and e.get("entity_type") == "dimension"
    ]
    assert len(dim_entities) >= 1
    props = _mapping(dim_entities[0]["properties"])
    dim_block = _mapping(_mapping(_mapping(props["adapter_native"])["ezdxf"])["dimension"])
    assert dim_block["dimlfac"] == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# SPLINE capture tests (#677, be-677b)
# ---------------------------------------------------------------------------


class _FakeSplineEntity:
    """Minimal fake ezdxf Spline entity for unit-level SPLINE tests."""

    # Class-level flag constants matching ezdxf.entities.Spline
    CLOSED: int = 1
    PERIODIC: int = 2

    def __init__(
        self,
        *,
        handle: str,
        layer: str,
        layout_name: str,
        control_points: list[tuple[float, float, float]],
        fit_points: list[tuple[float, float, float]] | None = None,
        degree: int = 3,
        closed: bool = False,
        periodic: bool = False,
    ) -> None:
        self._layout = types.SimpleNamespace(name=layout_name)
        self._control_points = control_points
        self._fit_points = fit_points or []
        flags = 0
        if closed:
            flags |= self.CLOSED
        if periodic:
            flags |= self.PERIODIC
        self.closed = closed or periodic
        self.dxf = types.SimpleNamespace(
            handle=handle,
            layer=layer,
            linetype="Continuous",
            degree=degree,
            flags=flags,
        )

    def dxftype(self) -> str:
        return "SPLINE"

    def get_layout(self) -> Any:
        return self._layout

    @property
    def control_points(self) -> list[tuple[float, float, float]]:
        return self._control_points

    @property
    def fit_points(self) -> list[tuple[float, float, float]]:
        return self._fit_points


@pytest.mark.asyncio
async def test_ezdxf_adapter_captures_clamped_degree3_spline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Clamped degree-3 SPLINE → entity_type='spline', correct scaled vertices, no length hint."""
    adapter = _load_ezdxf_adapter()
    source_path = tmp_path / "spline-clamped.dxf"
    document = cast(Any, ezdxf).new(units=6)  # 6=metres
    msp = document.modelspace()
    ctrl_pts = [(0.0, 0.0, 0.0), (0.5, 0.0, 0.0), (0.5, 0.5, 0.0), (1.0, 0.5, 0.0)]
    spline = msp.add_spline(dxfattribs={"degree": 3})
    spline.control_points = ctrl_pts
    document.saveas(source_path)

    result = await adapter.ingest(
        build_contract_source(file_path=source_path, original_name=source_path.name),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=2)),
    )

    assert result.confidence is not None
    assert result.warnings == ()

    entities = _mapping_tuple(result.canonical["entities"])
    spline_entities = [e for e in entities if e.get("entity_type") == "spline"]
    assert len(spline_entities) == 1
    sp = _mapping(spline_entities[0])

    _assert_common_entity_contract(sp, entity_type="spline", layout_ref="Model", layer_ref="0")

    # vertices and points must be identical
    assert sp["vertices"] == sp["points"]
    geometry = _mapping(sp["geometry"])
    assert geometry["vertices"] == geometry["points"]
    assert geometry["closed"] is False

    verts = _mapping_tuple(geometry["vertices"])
    assert len(verts) >= 2
    # Endpoints == scaled first/last control point (metres, units=6 so no scaling needed)
    assert verts[0] == {"x": 0.0, "y": 0.0, "z": 0.0}
    assert verts[-1] == {"x": 1.0, "y": 0.5, "z": 0.0}

    # geometry_summary
    gs = _mapping(geometry["geometry_summary"])
    assert gs["kind"] == "spline"
    assert gs["approximation"] == "control_polygon"
    assert gs["source_degree"] == 3
    assert gs["closed"] is False
    assert gs["endpoints_on_curve"] is True
    assert gs["vertex_count"] == len(verts)

    # properties
    props = _mapping(sp["properties"])
    assert props["source_type"] == "SPLINE"
    qty = _mapping(props["quantity_hints"])
    assert "length" not in qty
    assert "perimeter" not in qty
    assert qty["count"] == 1.0

    _assert_no_nonfinite_numbers(sp)


@pytest.mark.asyncio
async def test_ezdxf_adapter_captures_periodic_closed_spline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Periodic/closed SPLINE → closed=True, endpoints_on_curve=False, no length hint."""
    adapter = _load_ezdxf_adapter()
    source_path = tmp_path / "spline-periodic.dxf"
    document = cast(Any, ezdxf).new(units=6)
    msp = document.modelspace()
    # PERIODIC flag = 2, CLOSED flag = 1 → flags=3
    spline = msp.add_spline(dxfattribs={"degree": 3, "flags": 3})
    spline.control_points = [
        (0.0, 0.0, 0.0),
        (1.0, 0.0, 0.0),
        (1.0, 1.0, 0.0),
        (0.0, 1.0, 0.0),
    ]
    document.saveas(source_path)

    result = await adapter.ingest(
        build_contract_source(file_path=source_path, original_name=source_path.name),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=2)),
    )

    assert result.warnings == ()
    entities = _mapping_tuple(result.canonical["entities"])
    spline_entities = [e for e in entities if e.get("entity_type") == "spline"]
    assert len(spline_entities) == 1
    sp = _mapping(spline_entities[0])

    geometry = _mapping(sp["geometry"])
    gs = _mapping(geometry["geometry_summary"])
    assert gs["closed"] is True
    assert gs["endpoints_on_curve"] is False

    qty = _mapping(_mapping(sp["properties"])["quantity_hints"])
    assert "length" not in qty
    assert "perimeter" not in qty
    assert qty["count"] == 1.0

    assert len(_mapping_tuple(geometry["vertices"])) >= 2


@pytest.mark.asyncio
async def test_ezdxf_adapter_spline_uses_fit_points_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When control_points is empty but fit_points present, fall back to fit_points."""
    adapter = _load_ezdxf_adapter()
    fake_spline = _FakeSplineEntity(
        handle="SF1",
        layer="0",
        layout_name="Model",
        control_points=[],
        fit_points=[(0.0, 0.0, 0.0), (2.0, 3.0, 0.0), (4.0, 0.0, 0.0)],
        degree=3,
    )
    document = _FakeDocument(entities=(fake_spline,))
    monkeypatch.setattr(adapter, "_read_document", lambda _path: document)

    result = await adapter.ingest(
        build_contract_source(file_path=_FIXTURE_PATH, original_name="spline-fit.dxf"),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )

    assert result.warnings == ()
    entities = _mapping_tuple(result.canonical["entities"])
    spline_entities = [e for e in entities if e.get("entity_type") == "spline"]
    assert len(spline_entities) == 1
    sp = _mapping(spline_entities[0])
    verts = _mapping_tuple(_mapping(sp["geometry"])["vertices"])
    assert len(verts) == 3
    assert verts[0] == {"x": 0.0, "y": 0.0, "z": 0.0}
    assert verts[-1] == {"x": 4.0, "y": 0.0, "z": 0.0}


@pytest.mark.asyncio
async def test_ezdxf_adapter_spline_malformed_no_points_retained_as_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SPLINE with no control_points and no fit_points → unknown entity + malformed_coordinates."""
    adapter = _load_ezdxf_adapter()
    fake_spline = _FakeSplineEntity(
        handle="SBad",
        layer="0",
        layout_name="Model",
        control_points=[],
        fit_points=[],
        degree=3,
    )
    document = _FakeDocument(entities=(fake_spline,))
    monkeypatch.setattr(adapter, "_read_document", lambda _path: document)

    result = await adapter.ingest(
        build_contract_source(file_path=_FIXTURE_PATH, original_name="spline-empty.dxf"),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )

    assert [w.code for w in result.warnings] == ["malformed_coordinates"]
    entities = _mapping_tuple(result.canonical["entities"])
    assert entities[0]["entity_type"] == "unknown"

    diagnostics_details = {d.code: d.details for d in result.diagnostics}
    entity_diag = _mapping(diagnostics_details["dxf_entities_extracted"])
    assert cast(int, entity_diag["unsupported_entity_count"]) >= 1
    assert cast(int, entity_diag["supported_entity_count"]) == 0


@pytest.mark.asyncio
async def test_ezdxf_adapter_spline_supported_count_increments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Captured SPLINE increments supported_entity_count (not unsupported)."""
    adapter = _load_ezdxf_adapter()
    fake_spline = _FakeSplineEntity(
        handle="SOK",
        layer="Wires",
        layout_name="Model",
        control_points=[(0.0, 0.0, 0.0), (1.0, 0.0, 0.0), (2.0, 1.0, 0.0)],
        degree=3,
    )
    document = _FakeDocument(entities=(fake_spline,))
    monkeypatch.setattr(adapter, "_read_document", lambda _path: document)

    result = await adapter.ingest(
        build_contract_source(file_path=_FIXTURE_PATH, original_name="spline-ok.dxf"),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )

    assert result.warnings == ()
    diagnostics_details = {d.code: d.details for d in result.diagnostics}
    entity_diag = _mapping(diagnostics_details["dxf_entities_extracted"])
    assert cast(int, entity_diag["supported_entity_count"]) == 1
    assert cast(int, entity_diag["unsupported_entity_count"]) == 0


@pytest.mark.asyncio
async def test_ezdxf_spline_symmetry_with_libredwg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cross-adapter symmetry: same logical clamped degree-3 spline → same canonical shape (#369).

    The ezdxf adapter reads the SPLINE from DXF; the libredwg builder is called directly with
    a synthetic dwgread-style record at metre-unit scale. Both must agree on: entity_type,
    source_type, geometry_summary.kind, approximation, endpoints_on_curve, vertex_count, and
    scaled endpoints.
    """
    from app.ingestion.adapters import libredwg as libredwg_module

    # Shared control polygon in metres (unit=6 DXF → no scaling; libredwg scale=1.0)
    ctrl_m = [(0.0, 0.0, 0.0), (0.5, 0.5, 0.0), (1.0, 0.5, 0.0), (1.5, 0.0, 0.0)]

    # --- ezdxf side ---
    adapter = _load_ezdxf_adapter()
    fake_spline = _FakeSplineEntity(
        handle="SYM",
        layer="Wires",
        layout_name="Model",
        control_points=list(ctrl_m),
        degree=3,
    )
    document = _FakeDocument(entities=(fake_spline,), units=6)
    monkeypatch.setattr(adapter, "_read_document", lambda _path: document)

    ezdxf_result = await adapter.ingest(
        build_contract_source(file_path=_FIXTURE_PATH, original_name="symmetry-spline.dxf"),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )
    ezdxf_entities = _mapping_tuple(ezdxf_result.canonical["entities"])
    ezdxf_splines = [e for e in ezdxf_entities if e.get("entity_type") == "spline"]
    assert len(ezdxf_splines) == 1
    ez_sp = _mapping(ezdxf_splines[0])

    # --- libredwg side: call _build_spline_entity directly with metre-unit resolution ---
    # dwgread JSON record uses mm-stored coords for metric, but we pass scale=1.0 (metres)
    # to match the ezdxf side which is already in metres.
    libredwg_record = {
        "type": "SPLINE",
        "handle": "SYM",
        "layer": "Wires",
        "ctrl_pts": [{"x": pt[0], "y": pt[1], "z": pt[2], "w": 1.0} for pt in ctrl_m],
        "degree": 3,
        "periodic": False,
        "closed_b": False,
    }
    units_resolution = libredwg_module._UnitsResolution(
        payload={
            "normalized": "meter",
            "source": "INSUNITS",
            "source_value": 6,
            "conversion_target": "meter",
            "conversion_factor": 1.0,
        },
        scale=1.0,
        confirmed=True,
    )
    lb_entity = libredwg_module._build_spline_entity(libredwg_record, units=units_resolution)
    assert lb_entity is not None

    # --- Compare canonical fields ---
    assert ez_sp["entity_type"] == lb_entity["entity_type"] == "spline"
    assert (
        _mapping(ez_sp["properties"])["source_type"]
        == _mapping(lb_entity["properties"])["source_type"]
        == "SPLINE"
    )

    ez_gs = _mapping(_mapping(ez_sp["geometry"])["geometry_summary"])
    lb_gs = _mapping(_mapping(lb_entity["geometry"])["geometry_summary"])
    assert ez_gs["kind"] == lb_gs["kind"] == "spline"
    assert ez_gs["approximation"] == lb_gs["approximation"] == "control_polygon"
    assert ez_gs["endpoints_on_curve"] == lb_gs["endpoints_on_curve"] is True
    assert ez_gs["vertex_count"] == lb_gs["vertex_count"]
    assert int(cast(int, ez_gs["source_degree"])) == int(cast(int, lb_gs["source_degree"])) == 3

    ez_verts = _mapping_tuple(_mapping(ez_sp["geometry"])["vertices"])
    lb_verts = _mapping_tuple(_mapping(lb_entity["geometry"])["vertices"])
    assert len(ez_verts) == len(lb_verts)
    # Endpoints must match
    assert ez_verts[0] == lb_verts[0]
    assert ez_verts[-1] == lb_verts[-1]
