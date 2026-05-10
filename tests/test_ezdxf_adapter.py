"""Tests for the concrete ezdxf-backed DXF ingestion adapter."""

from __future__ import annotations

import asyncio
import types
from collections.abc import Iterator
from math import isfinite
from pathlib import Path
from typing import Any, NoReturn, cast

import ezdxf
import pytest

from app.ingestion.adapters.ezdxf import EzdxfAdapter
from app.ingestion.contracts import (
    AdapterExecutionOptions,
    AdapterTimeout,
    IngestionAdapter,
    InputFamily,
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
    assert provenance["dxf_handle"] == entity["handle"]
    assert isinstance(provenance["normalized_source_hash"], str)
    assert len(provenance["normalized_source_hash"]) == 64


def _load_ezdxf_adapter() -> IngestionAdapter:
    return load_adapter(get_descriptor(input_family()))


def input_family() -> InputFamily:
    return InputFamily.DXF


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
            review_state="approved",
            quantity_gate="allowed",
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
    assert payload.review_state == "approved"
    assert payload.validation_status == "valid"
    assert payload.quantity_gate == "allowed"


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
            review_state="review_required",
            quantity_gate="review_gated",
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
