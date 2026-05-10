"""Contract tests for the semantic-first IfcOpenShell ingestion adapter."""

from __future__ import annotations

import asyncio
import importlib.machinery
from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from app.ingestion.adapters import ifcopenshell as adapter_module
from app.ingestion.contracts import (
    AdapterExecutionOptions,
    AdapterStatus,
    AdapterTimeout,
    InputFamily,
    ProgressUpdate,
    UploadFormat,
)
from tests.ingestion_contract_harness import (
    ContractFinalizationExpectation,
    build_contract_source,
    exercise_adapter_contract,
)


class _FakeIfcEntity:
    def __init__(
        self,
        ifc_type: str,
        step_id: int,
        *,
        global_id: str | None = None,
        name: str | None = None,
        description: str | None = None,
        long_name: str | None = None,
        phase: str | None = None,
        length_unit: str | None = None,
        representation: object | None = None,
        psets: dict[str, Any] | None = None,
        qtos: dict[str, Any] | None = None,
        material_refs: list[object] | None = None,
        object_type: str | None = None,
        predefined_type: str | None = None,
    ) -> None:
        self._ifc_type = ifc_type
        self._step_id = step_id
        self.GlobalId = global_id
        self.Name = name
        self.Description = description
        self.LongName = long_name
        self.Phase = phase
        self.length_unit = length_unit
        self.Representation = representation
        self.psets: dict[str, Any] | None = psets or {}
        self.qtos: dict[str, Any] | None = qtos or {}
        self.material_refs: list[object] | None = material_refs or []
        self.ObjectType = object_type
        self.PredefinedType = predefined_type

    def is_a(self, query: str | None = None) -> str | bool:
        if query is None:
            return self._ifc_type
        if query == self._ifc_type:
            return True
        return query == "IfcProduct" and self._ifc_type != "IfcProject"

    def id(self) -> int:
        return self._step_id


class _FakeModel:
    def __init__(
        self,
        *,
        schema: str,
        project: _FakeIfcEntity | None,
        products: list[_FakeIfcEntity],
    ) -> None:
        self.schema = schema
        self._project = project
        self._products = products

    def by_type(self, ifc_type: str) -> list[_FakeIfcEntity]:
        if ifc_type == "IfcProject":
            return [] if self._project is None else [self._project]
        if ifc_type == "IfcProduct":
            return list(self._products)
        return []


def _module_spec() -> importlib.machinery.ModuleSpec:
    return importlib.machinery.ModuleSpec(name="ifcopenshell", loader=None)


def _write_ifc(path: Path, *, schema_line: str) -> None:
    path.write_text(
        "ISO-10303-21;\n"
        "HEADER;\n"
        "FILE_DESCRIPTION(('ViewDefinition'),'2;1');\n"
        f"{schema_line}\n"
        "ENDSEC;\n"
        "DATA;\n"
        "ENDSEC;\n"
        "END-ISO-10303-21;\n",
        encoding="utf-8",
    )


def _write_ifc_with_capped_header(path: Path) -> None:
    oversized_description = "X" * adapter_module._HEADER_READ_LIMIT_BYTES
    path.write_text(
        "ISO-10303-21;\n"
        "HEADER;\n"
        "FILE_DESCRIPTION(('" + oversized_description + "'),'2;1');\n"
        "FILE_SCHEMA(('IFC4'));\n"
        "ENDSEC;\n"
        "DATA;\n"
        "ENDSEC;\n"
        "END-ISO-10303-21;\n",
        encoding="utf-8",
    )


def _write_ifc_with_capped_fake_header_terminator(
    path: Path, *, fake_terminator_line: str
) -> None:
    oversized_description = "X" * adapter_module._HEADER_READ_LIMIT_BYTES
    path.write_text(
        "ISO-10303-21;\n"
        "HEADER;\n"
        f"{fake_terminator_line}\n"
        "FILE_DESCRIPTION(('" + oversized_description + "'),'2;1');\n"
        "FILE_SCHEMA(('IFC4'));\n"
        "ENDSEC;\n"
        "DATA;\n"
        "ENDSEC;\n"
        "END-ISO-10303-21;\n",
        encoding="utf-8",
    )


def _build_runtime(model: _FakeModel, *, util_element: object | None = None) -> object:
    helper = util_element or SimpleNamespace(
        get_psets=lambda product, **_: product.qtos if _.get("qtos_only") else product.psets,
        get_materials=lambda product: product.material_refs,
    )
    return SimpleNamespace(open=lambda _: model, util=SimpleNamespace(element=helper))


def _canonical_entities(result: object) -> tuple[Mapping[str, object], ...]:
    return cast(tuple[Mapping[str, object], ...], cast(Any, result).canonical["entities"])


def test_probe_reports_unavailable_without_ifcopenshell_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(adapter_module, "_find_runtime_spec", lambda: None)
    monkeypatch.setattr(adapter_module, "_package_version", lambda: None)

    adapter = adapter_module.create_adapter()
    availability = adapter.probe()

    assert availability.status is AdapterStatus.UNAVAILABLE
    assert availability.issues[0].name == "ifcopenshell"
    assert availability.observed[0].detail == "Optional runtime package is not installed."


@pytest.mark.asyncio
async def test_ifcopenshell_adapter_emits_semantic_canonical_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture_path = tmp_path / "semantic.ifc"
    _write_ifc(fixture_path, schema_line="FILE_SCHEMA(('IFC4'));")

    project = _FakeIfcEntity(
        "IfcProject",
        1,
        global_id="PROJECT-1",
        name="Demo Project",
        description="Semantic adapter smoke model",
        long_name="Demo Project Long Name",
        phase="Concept",
        length_unit="meter",
    )
    representation = SimpleNamespace(
        Representations=[
            SimpleNamespace(
                RepresentationIdentifier="Body",
                RepresentationType="SweptSolid",
                Items=[object()],
            )
        ]
    )
    products = [
        _FakeIfcEntity(
            "IfcWall",
            10,
            global_id="DUPLICATE",
            name="Wall A",
            representation=representation,
            psets={"Pset_WallCommon": {"FireRating": "2HR"}},
            qtos={"Qto_WallBaseQuantities": {"Length": 5.2}},
            material_refs=[SimpleNamespace(Name="Concrete", Category="structural")],
            object_type="Wall",
            predefined_type="STANDARD",
        ),
        _FakeIfcEntity(
            "IfcWall",
            11,
            global_id="DUPLICATE",
            name="Wall B",
            psets={"Pset_WallCommon": {"LoadBearing": True}},
            qtos={"Qto_WallBaseQuantities": {"Length": 7.8}},
            material_refs=[SimpleNamespace(Name="Masonry", Category="finish")],
            object_type="Wall",
            predefined_type="STANDARD",
        ),
        _FakeIfcEntity(
            "IfcDoor",
            42,
            name="Door A",
            psets={"Pset_DoorCommon": {"IsExternal": False}},
            qtos={"Qto_DoorBaseQuantities": {"Area": 2.1}},
            material_refs=[SimpleNamespace(Name="Timber", Category="finish")],
            object_type="Door",
            predefined_type="DOOR",
        ),
    ]
    runtime = _build_runtime(_FakeModel(schema="IFC4", project=project, products=products))

    monkeypatch.setattr(adapter_module, "_find_runtime_spec", _module_spec)
    monkeypatch.setattr(adapter_module, "_package_version", lambda: "0.8.5")
    monkeypatch.setattr(adapter_module, "_load_runtime_module", lambda: runtime)

    adapter = adapter_module.create_adapter()
    source = build_contract_source(
        file_path=fixture_path,
        upload_format=UploadFormat.IFC,
        input_family=InputFamily.IFC,
        media_type="application/step",
        original_name="semantic.ifc",
    )

    payload = await exercise_adapter_contract(
        adapter,
        source=source,
        input_family=InputFamily.IFC,
        adapter_key="ifcopenshell",
        expectation=ContractFinalizationExpectation(
            validation_status="needs_review",
            review_state="review_required",
            quantity_gate="review_gated",
            diagnostic_codes=("ifc.schema_sniff", "ifc.semantic_extract"),
        ),
    )

    canonical = payload.canonical_json
    assert canonical["ifc_schema"] == "IFC4"
    assert canonical["metadata"]["ifc_schema"] == "IFC4"
    assert canonical["canonical_entity_schema_version"] == "0.1"
    assert canonical["project"]["name"] == "Demo Project"
    assert canonical["metadata"]["project"]["global_id"] == "PROJECT-1"

    entities = canonical["entities"]
    assert [entity["id"] for entity in entities] == ["DUPLICATE", "DUPLICATE-2", "#42"]
    assert [entity["entity_id"] for entity in entities] == ["DUPLICATE", "DUPLICATE-2", "#42"]
    assert [entity["ifc_type"] for entity in entities] == ["IfcWall", "IfcWall", "IfcDoor"]
    assert entities[0]["entity_type"] == "ifc_product"
    assert entities[0]["entity_schema_version"] == "0.1"
    assert entities[0]["geometry"]["reason"] == "semantic_metadata_only"
    assert entities[0]["provenance"]["source_entity_ref"] == "entities.DUPLICATE"
    assert entities[0]["psets"][0]["name"] == "Pset_WallCommon"
    assert entities[0]["qtos"][0]["name"] == "Qto_WallBaseQuantities"
    assert entities[0]["material_refs"][0]["name"] == "Concrete"
    assert (
        entities[0]["representation"]["representations"][0]["representation_identifier"]
        == "Body"
    )
    assert canonical["layers"] == [{"name": "IfcWall"}, {"name": "IfcDoor"}]
    assert payload.provenance_json["records"][1]["details"]["entity_count"] == 3
    assert {record["source_ref"] for record in payload.provenance_json["records"]} == {
        "originals/semantic.ifc"
    }


@pytest.mark.asyncio
async def test_ifcopenshell_adapter_orders_duplicate_and_missing_ids_deterministically(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture_path = tmp_path / "ordered-ids.ifc"
    _write_ifc(fixture_path, schema_line="FILE_SCHEMA(('IFC4'));")

    products = [
        _FakeIfcEntity("IfcDoor", 7, name="Door B"),
        _FakeIfcEntity("IfcWall", 11, global_id="DUPLICATE", name="Wall B"),
        _FakeIfcEntity("IfcWindow", 3, name="Window A"),
        _FakeIfcEntity("IfcWall", 10, global_id="DUPLICATE", name="Wall A"),
    ]
    runtime = _build_runtime(_FakeModel(schema="IFC4", project=None, products=products))

    monkeypatch.setattr(adapter_module, "_load_runtime_module", lambda: runtime)

    result = await adapter_module.create_adapter().ingest(
        build_contract_source(
            file_path=fixture_path,
            upload_format=UploadFormat.IFC,
            input_family=InputFamily.IFC,
            media_type="application/step",
            original_name="ordered-ids.ifc",
        ),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )

    assert [entity["id"] for entity in _canonical_entities(result)] == [
        "DUPLICATE",
        "DUPLICATE-2",
        "#3",
        "#7",
    ]


@pytest.mark.asyncio
async def test_ifcopenshell_adapter_gracefully_skips_missing_helpers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture_path = tmp_path / "missing-helpers.ifc"
    _write_ifc(fixture_path, schema_line="FILE_SCHEMA(('IFC4'));")

    project = _FakeIfcEntity("IfcProject", 1, global_id="PROJECT-1", length_unit="meter")
    product = _FakeIfcEntity("IfcWall", 10, global_id="WALL-1", name="Wall A")
    product.psets = None
    product.qtos = None
    product.material_refs = None
    runtime = SimpleNamespace(
        open=lambda _: _FakeModel(schema="IFC4", project=project, products=[product])
    )

    monkeypatch.setattr(adapter_module, "_load_runtime_module", lambda: runtime)
    monkeypatch.setattr(adapter_module, "_resolve_element_module", lambda _runtime: None)

    result = await adapter_module.create_adapter().ingest(
        build_contract_source(
            file_path=fixture_path,
            upload_format=UploadFormat.IFC,
            input_family=InputFamily.IFC,
            media_type="application/step",
            original_name="missing-helpers.ifc",
        ),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )

    entity = _canonical_entities(result)[0]
    assert entity["psets"] == ()
    assert entity["qtos"] == ()
    assert entity["material_refs"] == ()
    assert result.warnings == ()


@pytest.mark.asyncio
async def test_ifcopenshell_adapter_warns_when_helper_lookups_fail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture_path = tmp_path / "helper-failures.ifc"
    _write_ifc(fixture_path, schema_line="FILE_SCHEMA(('IFC4'));")

    project = _FakeIfcEntity("IfcProject", 1, global_id="PROJECT-1", length_unit="meter")
    product = _FakeIfcEntity("IfcWall", 10, global_id="WALL-1", name="Wall A")
    product.psets = None
    product.qtos = None
    product.material_refs = None

    def _raise_helper_failure(*args: object, **kwargs: object) -> object:
        _ = (args, kwargs)
        raise RuntimeError("native helper exploded")

    runtime = _build_runtime(
        _FakeModel(schema="IFC4", project=project, products=[product]),
        util_element=SimpleNamespace(
            get_psets=_raise_helper_failure,
            get_materials=_raise_helper_failure,
        ),
    )

    monkeypatch.setattr(adapter_module, "_load_runtime_module", lambda: runtime)

    result = await adapter_module.create_adapter().ingest(
        build_contract_source(
            file_path=fixture_path,
            upload_format=UploadFormat.IFC,
            input_family=InputFamily.IFC,
            media_type="application/step",
            original_name="helper-failures.ifc",
        ),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )

    entity = _canonical_entities(result)[0]
    assert entity["psets"] == ()
    assert entity["qtos"] == ()
    assert entity["material_refs"] == ()
    assert {warning.code for warning in result.warnings} == {
        "ifc.psets_unavailable",
        "ifc.qtos_unavailable",
        "ifc.material_refs_unavailable",
    }


@pytest.mark.asyncio
async def test_ifcopenshell_adapter_emits_progress_callbacks_in_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture_path = tmp_path / "progress.ifc"
    _write_ifc(fixture_path, schema_line="FILE_SCHEMA(('IFC4'));")

    runtime = _build_runtime(
        _FakeModel(
            schema="IFC4",
            project=None,
            products=[_FakeIfcEntity("IfcWall", 10, global_id="WALL-1", name="Wall A")],
        )
    )
    progress_updates: list[ProgressUpdate] = []

    monkeypatch.setattr(adapter_module, "_load_runtime_module", lambda: runtime)

    await adapter_module.create_adapter().ingest(
        build_contract_source(
            file_path=fixture_path,
            upload_format=UploadFormat.IFC,
            input_family=InputFamily.IFC,
            media_type="application/step",
            original_name="progress.ifc",
        ),
        AdapterExecutionOptions(
            timeout=AdapterTimeout(seconds=1),
            on_progress=progress_updates.append,
        ),
    )

    assert progress_updates == [
        ProgressUpdate(stage="sniff_header", message="Inspecting IFC STEP header", percent=0.10),
        ProgressUpdate(stage="open_model", message="Opening IFC model", percent=0.35),
        ProgressUpdate(
            stage="extract_semantics",
            message="Extracting semantic IFC entities",
            percent=0.65,
        ),
        ProgressUpdate(
            stage="finalize",
            message="Finalizing canonical IFC payload",
            percent=0.90,
        ),
    ]


@pytest.mark.asyncio
async def test_ifcopenshell_adapter_enforces_timeout_checkpoint_before_runtime_load(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture_path = tmp_path / "timeout.ifc"
    _write_ifc(fixture_path, schema_line="FILE_SCHEMA(('IFC4'));")

    runtime_loaded = False

    async def _slow_emit_progress(
        options: AdapterExecutionOptions,
        update: ProgressUpdate,
    ) -> None:
        _ = (options, update)
        await asyncio.sleep(0.02)

    def _unexpected_runtime_load() -> object:
        nonlocal runtime_loaded
        runtime_loaded = True
        raise AssertionError("runtime should not load after timeout checkpoint")

    monkeypatch.setattr(adapter_module, "_emit_progress", _slow_emit_progress)
    monkeypatch.setattr(adapter_module, "_load_runtime_module", _unexpected_runtime_load)

    with pytest.raises(TimeoutError):
        await adapter_module.create_adapter().ingest(
            build_contract_source(
                file_path=fixture_path,
                upload_format=UploadFormat.IFC,
                input_family=InputFamily.IFC,
                media_type="application/step",
                original_name="timeout.ifc",
            ),
            AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.01)),
        )

    assert runtime_loaded is False


@pytest.mark.asyncio
async def test_ifcopenshell_adapter_enforces_cancellation_checkpoint_before_runtime_load(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture_path = tmp_path / "cancel.ifc"
    _write_ifc(fixture_path, schema_line="FILE_SCHEMA(('IFC4'));")

    class _CheckpointCancellation:
        def __init__(self) -> None:
            self.calls = 0

        def is_cancelled(self) -> bool:
            self.calls += 1
            return self.calls >= 2

    cancellation = _CheckpointCancellation()
    runtime_loaded = False

    def _unexpected_runtime_load() -> object:
        nonlocal runtime_loaded
        runtime_loaded = True
        raise AssertionError("runtime should not load after cancellation checkpoint")

    monkeypatch.setattr(adapter_module, "_load_runtime_module", _unexpected_runtime_load)

    with pytest.raises(asyncio.CancelledError):
        await adapter_module.create_adapter().ingest(
            build_contract_source(
                file_path=fixture_path,
                upload_format=UploadFormat.IFC,
                input_family=InputFamily.IFC,
                media_type="application/step",
                original_name="cancel.ifc",
            ),
            AdapterExecutionOptions(
                timeout=AdapterTimeout(seconds=1),
                cancellation=cancellation,
            ),
        )

    assert cancellation.calls == 2
    assert runtime_loaded is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("schema_line", "expected_warning", "expected_schema"),
    [
        ("FILE_NAME('missing-schema.ifc','2026-01-01T00:00:00');", "ifc.schema_missing", None),
        ("FILE_SCHEMA((IFC4));", "ifc.schema_missing", None),
        ("FILE_SCHEMA(('IFC5'));", "ifc.schema_unsupported", "IFC5"),
        ("FILE_SCHEMA(('IFC4X2'));", "ifc.schema_unsupported", "IFC4X2"),
    ],
)
async def test_ifcopenshell_adapter_short_circuits_invalid_schema_through_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    schema_line: str,
    expected_warning: str,
    expected_schema: str | None,
) -> None:
    fixture_path = tmp_path / "invalid-schema.ifc"
    _write_ifc(fixture_path, schema_line=schema_line)

    runtime_called = False

    def _unexpected_runtime_load() -> object:
        nonlocal runtime_called
        runtime_called = True
        raise AssertionError("native runtime should not load for invalid schema sniff path")

    monkeypatch.setattr(adapter_module, "_find_runtime_spec", _module_spec)
    monkeypatch.setattr(adapter_module, "_package_version", lambda: "0.8.5")
    monkeypatch.setattr(adapter_module, "_load_runtime_module", _unexpected_runtime_load)

    adapter = adapter_module.create_adapter()
    source = build_contract_source(
        file_path=fixture_path,
        upload_format=UploadFormat.IFC,
        input_family=InputFamily.IFC,
        media_type="application/step",
        original_name="invalid-schema.ifc",
    )

    payload = await exercise_adapter_contract(
        adapter,
        source=source,
        input_family=InputFamily.IFC,
        adapter_key="ifcopenshell",
        expectation=ContractFinalizationExpectation(
            validation_status="invalid",
            review_state="rejected",
            quantity_gate="blocked",
            warning_codes=(expected_warning,),
            diagnostic_codes=("ifc.schema_sniff", "ifc.semantic_extract"),
        ),
    )

    assert runtime_called is False
    canonical = payload.canonical_json
    if expected_schema is None:
        assert "ifc_schema" not in canonical
        assert "ifc_schema" not in canonical["metadata"]
    else:
        assert canonical["ifc_schema"] == expected_schema
        assert canonical["metadata"]["ifc_schema"] == expected_schema
    assert canonical["entities"] == []
    assert canonical["metadata"]["empty_entities_reason"] == "semantic_extract_skipped"


@pytest.mark.asyncio
async def test_ifcopenshell_adapter_capped_incomplete_header_continues_to_native_parse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture_path = tmp_path / "capped-header.ifc"
    _write_ifc_with_capped_header(fixture_path)

    sniff = adapter_module._sniff_step_header(fixture_path)
    assert sniff.schema is None
    assert sniff.readable is True
    assert sniff.supported is False
    assert sniff.header_complete is False

    runtime_called = False

    def _open_runtime(path: str) -> _FakeModel:
        nonlocal runtime_called
        runtime_called = True
        _ = path
        return _FakeModel(schema="IFC4", project=None, products=[])

    runtime = SimpleNamespace(open=_open_runtime)
    monkeypatch.setattr(adapter_module, "_load_runtime_module", lambda: runtime)
    monkeypatch.setattr(adapter_module, "_resolve_element_module", lambda _runtime: None)

    result = await adapter_module.create_adapter().ingest(
        build_contract_source(
            file_path=fixture_path,
            upload_format=UploadFormat.IFC,
            input_family=InputFamily.IFC,
            media_type="application/step",
            original_name="capped-header.ifc",
        ),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )

    assert runtime_called is True
    assert result.diagnostics[0].details == {
        "ifc_schema": None,
        "readable": True,
        "supported": False,
    }
    assert "ifc.schema_missing" not in {warning.code for warning in result.warnings}
    assert result.canonical["ifc_schema"] == "IFC4"


@pytest.mark.parametrize(
    "fake_terminator_line",
    [
        "FILE_NAME('quoted ENDSEC; marker','2026-01-01T00:00:00');",
        "/* comment ENDSEC; marker */",
    ],
)
def test_sniff_step_header_ignores_embedded_endsec_before_real_header_end(
    tmp_path: Path,
    fake_terminator_line: str,
) -> None:
    fixture_path = tmp_path / "embedded-endsec.ifc"
    _write_ifc_with_capped_fake_header_terminator(
        fixture_path, fake_terminator_line=fake_terminator_line
    )

    sniff = adapter_module._sniff_step_header(fixture_path)

    assert sniff.schema is None
    assert sniff.readable is True
    assert sniff.supported is False
    assert sniff.header_complete is False


@pytest.mark.asyncio
async def test_ifcopenshell_adapter_quoted_endsec_in_capped_header_continues_native_parse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture_path = tmp_path / "quoted-endsec-capped-header.ifc"
    _write_ifc_with_capped_fake_header_terminator(
        fixture_path,
        fake_terminator_line="FILE_NAME('quoted ENDSEC; marker','2026-01-01T00:00:00');",
    )

    runtime_called = False

    def _open_runtime(path: str) -> _FakeModel:
        nonlocal runtime_called
        runtime_called = True
        _ = path
        return _FakeModel(schema="IFC4", project=None, products=[])

    runtime = SimpleNamespace(open=_open_runtime)
    monkeypatch.setattr(adapter_module, "_load_runtime_module", lambda: runtime)

    result = await adapter_module.create_adapter().ingest(
        build_contract_source(
            file_path=fixture_path,
            upload_format=UploadFormat.IFC,
            input_family=InputFamily.IFC,
            media_type="application/step",
            original_name="quoted-endsec-capped-header.ifc",
        ),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )

    assert runtime_called is True
    assert result.diagnostics[0].details == {
        "ifc_schema": None,
        "readable": True,
        "supported": False,
    }
    assert "ifc.schema_missing" not in {warning.code for warning in result.warnings}
    assert result.canonical["ifc_schema"] == "IFC4"


@pytest.mark.asyncio
async def test_ifcopenshell_adapter_non_step_payload_does_not_short_circuit_schema_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture_path = tmp_path / "binary.ifc"
    fixture_path.write_bytes(b"\x00\xff\x00\x01garbage payload")

    runtime_called = False

    def _open_runtime(path: str) -> _FakeModel:
        nonlocal runtime_called
        runtime_called = True
        _ = path
        return _FakeModel(schema="IFC4", project=None, products=[])

    runtime = SimpleNamespace(open=_open_runtime)
    monkeypatch.setattr(adapter_module, "_load_runtime_module", lambda: runtime)

    result = await adapter_module.create_adapter().ingest(
        build_contract_source(
            file_path=fixture_path,
            upload_format=UploadFormat.IFC,
            input_family=InputFamily.IFC,
            media_type="application/octet-stream",
            original_name="nested/input.ifc",
        ),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=1)),
    )

    assert runtime_called is True
    sniff_diagnostic = result.diagnostics[0]
    assert sniff_diagnostic.code == "ifc.schema_sniff"
    assert sniff_diagnostic.details == {"ifc_schema": None, "readable": False, "supported": False}
    assert "ifc.schema_missing" not in {warning.code for warning in result.warnings}
    assert {record.source_ref for record in result.provenance} == {"originals/input.ifc"}
