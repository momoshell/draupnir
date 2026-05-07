"""Tests for ingestion adapter contracts and registry metadata."""

from __future__ import annotations

import asyncio
import math
from collections.abc import Callable
from dataclasses import fields
from pathlib import Path
from typing import Any, cast

import pytest

import app.ingestion.adapters.pymupdf as pymupdf_adapter
from app.core.errors import ErrorCode
from app.ingestion.contracts import (
    AdapterAvailability,
    AdapterCapabilities,
    AdapterDescriptor,
    AdapterDiagnostic,
    AdapterExecutionOptions,
    AdapterFailureKind,
    AdapterResult,
    AdapterSource,
    AdapterStatus,
    AdapterTimeout,
    AvailabilityReason,
    ConfidenceSummary,
    InputFamily,
    LicenseState,
    ProbeKind,
    ProbeObservation,
    ProbeRequirement,
    ProbeStatus,
    ProgressUpdate,
    ProvenanceRecord,
    UploadFormat,
    error_code_for_failure,
    input_families_for_upload_format,
)
from app.ingestion.registry import (
    descriptors_for_upload_format,
    evaluate_availability,
    get_registry,
    list_descriptors,
)


class _FakePoint:
    def __init__(
        self,
        x: float,
        y: float,
        *,
        on_read: Callable[[], None] | None = None,
    ) -> None:
        self._x = x
        self._y = y
        self._on_read = on_read

    @property
    def x(self) -> float:
        if self._on_read is not None:
            callback = self._on_read
            self._on_read = None
            callback()
        return self._x

    @property
    def y(self) -> float:
        return self._y


class _FakeRect:
    def __init__(self, x0: float, y0: float, x1: float, y1: float) -> None:
        self.x0 = x0
        self.y0 = y0
        self.x1 = x1
        self.y1 = y1
        self.width = x1 - x0
        self.height = y1 - y0


class _FakePage:
    def __init__(
        self,
        *,
        drawings: list[dict[str, Any]] | None = None,
        text_payload: dict[str, Any] | None = None,
        rect: _FakeRect | None = None,
    ) -> None:
        self._drawings = drawings or []
        self._text_payload = text_payload or {"blocks": []}
        self.rect = rect or _FakeRect(0.0, 0.0, 100.0, 100.0)
        self.mediabox = self.rect
        self.rotation = 0

    def get_drawings(self) -> list[dict[str, Any]]:
        return self._drawings

    def get_text(self, kind: str) -> dict[str, Any]:
        assert kind == "dict"
        return self._text_payload


class _FakeDocument:
    def __init__(self, pages: list[_FakePage]) -> None:
        self._pages = pages
        self.page_count = len(pages)
        self.closed = False

    def load_page(self, page_index: int) -> _FakePage:
        return self._pages[page_index]

    def close(self) -> None:
        self.closed = True


async def _ingest_fake_document(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    document: _FakeDocument,
    *,
    original_name: str = "vector.pdf",
    timeout: AdapterTimeout | None = None,
    cancellation: Any = None,
    perf_values: list[float] | None = None,
) -> AdapterResult:
    source_path = tmp_path / "vector.pdf"
    source_path.write_bytes(b"%PDF-1.4\n%%EOF\n")
    adapter = cast(pymupdf_adapter.PyMuPDFAdapter, pymupdf_adapter.create_adapter())

    monkeypatch.setattr(adapter, "_runtime_for_ingest", lambda: None)

    async def _fake_extract_with_process(
        source: AdapterSource,
        options: AdapterExecutionOptions,
    ) -> tuple[dict[str, Any], list[Any]]:
        budget = pymupdf_adapter._ExtractionBudget(
            started_at=0.0,
            timeout_seconds=options.timeout.seconds if options.timeout is not None else None,
        )
        try:
            return pymupdf_adapter._extract_document_canonical(
                document,
                source=source,
                options=options,
                budget=budget,
            )
        finally:
            pymupdf_adapter._close_document(document)

    monkeypatch.setattr(pymupdf_adapter, "_extract_with_process", _fake_extract_with_process)

    if perf_values is not None:
        values = iter(perf_values)
        last_value = perf_values[-1]
        monkeypatch.setattr(pymupdf_adapter, "perf_counter", lambda: next(values, last_value))

    return await adapter.ingest(
        AdapterSource(
            file_path=source_path,
            upload_format=UploadFormat.PDF,
            input_family=InputFamily.PDF_VECTOR,
            original_name=original_name,
        ),
        AdapterExecutionOptions(timeout=timeout, cancellation=cancellation),
    )


def test_upload_formats_cover_all_input_families() -> None:
    assert input_families_for_upload_format(UploadFormat.DWG) == (InputFamily.DWG,)
    assert input_families_for_upload_format(UploadFormat.DXF) == (InputFamily.DXF,)
    assert input_families_for_upload_format(UploadFormat.IFC) == (InputFamily.IFC,)
    assert input_families_for_upload_format(UploadFormat.PDF) == (
        InputFamily.PDF_VECTOR,
        InputFamily.PDF_RASTER,
    )


def test_adapter_result_exposes_required_trd_fields() -> None:
    result = AdapterResult(
        canonical={"entities": ({"kind": "line"},)},
        provenance=(
            ProvenanceRecord(
                stage="extract",
                adapter_key="ezdxf",
                source_ref="originals/file.dxf",
            ),
        ),
        confidence=ConfidenceSummary(score=0.97, review_required=False, basis="vector"),
    )

    assert "entities" in result.canonical
    assert result.provenance[0].adapter_key == "ezdxf"
    assert result.confidence is not None
    assert result.warnings == ()
    assert result.diagnostics == ()


def test_registry_is_static_and_covers_every_family() -> None:
    descriptors = list_descriptors()
    registry = get_registry()

    assert descriptors is list_descriptors()
    assert set(registry) == {
        InputFamily.DWG,
        InputFamily.DXF,
        InputFamily.IFC,
        InputFamily.PDF_VECTOR,
        InputFamily.PDF_RASTER,
    }
    assert registry[InputFamily.PDF_VECTOR].module == "app.ingestion.adapters.pymupdf"
    assert all(descriptor.adapter_key == descriptor.key for descriptor in descriptors)
    assert all(descriptor.input_formats == descriptor.upload_formats for descriptor in descriptors)
    assert all(descriptor.output_formats == ("canonical_json",) for descriptor in descriptors)
    assert all(descriptor.bounded_probe_ms > 0 for descriptor in descriptors)
    assert all(descriptor.confidence_range is not None for descriptor in descriptors)
    assert registry[InputFamily.PDF_RASTER].experimental is True
    assert registry[InputFamily.DWG].capabilities.can_read is True
    assert registry[InputFamily.DWG].capabilities.can_write is False
    assert registry[InputFamily.DWG].capabilities.extracts_geometry is True
    assert registry[InputFamily.DWG].capabilities.supports_quantity_hints is True
    assert registry[InputFamily.DWG].capabilities.supports_layout_selection is True
    assert registry[InputFamily.DWG].capabilities.supports_xref_resolution is True
    assert registry[InputFamily.IFC].capabilities.extracts_materials is True

    dwg_license_probe = next(
        probe for probe in registry[InputFamily.DWG].probes if probe.kind is ProbeKind.LICENSE
    )
    pdf_vector_license_probe = next(
        probe
        for probe in registry[InputFamily.PDF_VECTOR].probes
        if probe.kind is ProbeKind.LICENSE
    )
    pdf_raster_binary_probe = next(
        probe for probe in registry[InputFamily.PDF_RASTER].probes if probe.name == "tesseract"
    )

    assert dwg_license_probe.failure_status is AdapterStatus.UNAVAILABLE
    assert pdf_vector_license_probe.failure_status is AdapterStatus.UNAVAILABLE
    assert pdf_raster_binary_probe.failure_status is AdapterStatus.DEGRADED


def test_ifc_registry_metadata_stays_semantic_only() -> None:
    descriptor = get_registry()[InputFamily.IFC]

    assert descriptor.display_name == "IfcOpenShell semantic IFC adapter"
    assert descriptor.capabilities.extracts_geometry is False
    assert descriptor.capabilities.extracts_materials is True
    assert descriptor.capabilities.extracts_layers is True
    assert descriptor.capabilities.supports_quantity_hints is True
    assert descriptor.capabilities.extracts_text is False
    assert descriptor.confidence_range == (0.2, 0.55)
    assert descriptor.notes == (
        "Semantic-only IFC extraction; tessellation and shape creation are disabled.",
    )


def test_registry_rejects_mutation_and_preserves_cached_metadata() -> None:
    registry = get_registry()
    mutable_registry = cast(Any, registry)

    try:
        mutable_registry[InputFamily.DWG] = registry[InputFamily.DXF]
    except TypeError:
        pass
    else:
        raise AssertionError("Expected registry mutation to fail.")

    assert get_registry()[InputFamily.DWG].module == "app.ingestion.adapters.libredwg"


def test_adapter_capabilities_use_trd_aligned_field_names() -> None:
    capability_fields = {field.name for field in fields(AdapterCapabilities)}

    assert capability_fields == {
        "can_read",
        "can_write",
        "extracts_canonical",
        "extracts_provenance",
        "extracts_confidence",
        "extracts_warnings",
        "extracts_diagnostics",
        "extracts_geometry",
        "extracts_materials",
        "extracts_layers",
        "extracts_blocks",
        "extracts_text",
        "supports_exports",
        "supports_quantity_hints",
        "supports_layout_selection",
        "supports_xref_resolution",
    }


def test_availability_contract_uses_trd_status_and_reason_fields() -> None:
    availability_fields = {field.name for field in fields(AdapterAvailability)}

    assert {status.value for status in AdapterStatus} == {
        "available",
        "degraded",
        "unavailable",
    }
    assert {reason.value for reason in AvailabilityReason} == {
        "missing_binary",
        "missing_license",
        "probe_failed",
        "disabled_by_config",
        "unsupported_platform",
    }
    assert "availability_reason" in availability_fields
    assert "reason" not in availability_fields


def test_pdf_upload_format_returns_vector_and_raster_candidates() -> None:
    families = tuple(
        descriptor.family for descriptor in descriptors_for_upload_format(UploadFormat.PDF)
    )

    assert families == (InputFamily.PDF_VECTOR, InputFamily.PDF_RASTER)


def test_pymupdf_create_adapter_returns_vector_pdf_adapter() -> None:
    adapter = pymupdf_adapter.create_adapter()

    assert adapter.descriptor.key == "pymupdf"
    assert adapter.descriptor.family is InputFamily.PDF_VECTOR


def test_pymupdf_probe_is_unavailable_when_package_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise_missing() -> object:
        raise ModuleNotFoundError("No module named 'fitz'")

    monkeypatch.setattr(pymupdf_adapter, "_load_runtime_module", _raise_missing)
    monkeypatch.setattr(pymupdf_adapter, "_package_version", lambda: None)

    availability = pymupdf_adapter.create_adapter(
        license_acknowledged=lambda: True,
    ).probe()

    assert availability.status is AdapterStatus.UNAVAILABLE
    assert availability.availability_reason is AvailabilityReason.PROBE_FAILED
    assert availability.license_state is LicenseState.PRESENT
    assert availability.details == {"package": "fitz", "license_acknowledged": True}
    assert {(item.kind, item.name, item.status) for item in availability.observed} == {
        (ProbeKind.PYTHON_PACKAGE, "fitz", ProbeStatus.MISSING),
        (ProbeKind.LICENSE, "pymupdf-deployment-review", ProbeStatus.AVAILABLE),
    }


def test_pymupdf_probe_handles_generic_runtime_probe_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise_generic_runtime_error() -> object:
        raise RuntimeError("runtime probe exploded")

    monkeypatch.setattr(pymupdf_adapter, "_load_runtime_module", _raise_generic_runtime_error)
    monkeypatch.setattr(pymupdf_adapter, "_package_version", lambda: "1.26.0")

    availability = pymupdf_adapter.create_adapter(
        license_acknowledged=lambda: True,
    ).probe()

    assert availability.status is AdapterStatus.UNAVAILABLE
    assert availability.availability_reason is AvailabilityReason.PROBE_FAILED
    details = availability.details
    assert details is not None
    assert details["package"] == "fitz"
    assert details["license_acknowledged"] is True
    assert "runtime probe exploded" not in str(details)
    assert "runtime probe exploded" not in str(availability.observed)
    assert "runtime probe exploded" not in str(availability)


def test_pymupdf_probe_handles_license_provider_exception_with_sanitized_details(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise_license_provider_error() -> bool:
        raise RuntimeError("sensitive license provider failure")

    monkeypatch.setattr(pymupdf_adapter, "_load_runtime_module", lambda: object())
    monkeypatch.setattr(pymupdf_adapter, "_runtime_version", lambda _runtime: "1.26.0")
    monkeypatch.setattr(pymupdf_adapter, "_package_version", lambda: "1.26.0")

    availability = pymupdf_adapter.create_adapter(
        license_acknowledged=_raise_license_provider_error,
    ).probe()

    assert availability.status is AdapterStatus.UNAVAILABLE
    assert availability.availability_reason is AvailabilityReason.PROBE_FAILED
    details = availability.details
    assert details is not None
    assert details["package"] == "fitz"
    assert "sensitive license provider failure" not in str(details)
    assert "sensitive license provider failure" not in str(availability)


def test_pymupdf_probe_is_unavailable_without_license_acknowledgement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _runtime_version(_runtime: object) -> str:
        return "1.26.0"

    monkeypatch.setattr(pymupdf_adapter, "_load_runtime_module", lambda: object())
    monkeypatch.setattr(pymupdf_adapter, "_runtime_version", _runtime_version)
    monkeypatch.setattr(pymupdf_adapter, "_package_version", lambda: "1.26.0")

    availability = pymupdf_adapter.create_adapter().probe()

    assert availability.status is AdapterStatus.UNAVAILABLE
    assert availability.availability_reason is AvailabilityReason.MISSING_LICENSE
    assert availability.license_state is LicenseState.MISSING
    assert availability.details == {
        "package": "fitz",
        "package_version": "1.26.0",
        "license_acknowledged": False,
    }
    assert {(item.kind, item.name, item.status) for item in availability.observed} == {
        (ProbeKind.PYTHON_PACKAGE, "fitz", ProbeStatus.AVAILABLE),
        (ProbeKind.LICENSE, "pymupdf-deployment-review", ProbeStatus.MISSING),
    }


def test_pymupdf_probe_is_available_with_license_acknowledgement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _runtime_version(_runtime: object) -> str:
        return "1.26.0"

    monkeypatch.setattr(pymupdf_adapter, "_load_runtime_module", lambda: object())
    monkeypatch.setattr(pymupdf_adapter, "_runtime_version", _runtime_version)
    monkeypatch.setattr(pymupdf_adapter, "_package_version", lambda: "1.26.0")

    availability = pymupdf_adapter.create_adapter(
        license_acknowledged=lambda: True,
    ).probe()

    assert availability.status is AdapterStatus.AVAILABLE
    assert availability.availability_reason is None
    assert availability.license_state is LicenseState.PRESENT
    assert availability.details == {
        "package": "fitz",
        "package_version": "1.26.0",
        "license_acknowledged": True,
    }
    assert {(item.kind, item.name, item.status) for item in availability.observed} == {
        (ProbeKind.PYTHON_PACKAGE, "fitz", ProbeStatus.AVAILABLE),
        (ProbeKind.LICENSE, "pymupdf-deployment-review", ProbeStatus.AVAILABLE),
    }


def test_pymupdf_probe_uses_package_version_when_runtime_attrs_are_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pymupdf_adapter, "_load_runtime_module", lambda: object())
    monkeypatch.setattr(pymupdf_adapter, "_package_version", lambda: "9.9.9")

    availability = pymupdf_adapter.create_adapter(
        license_acknowledged=lambda: True,
    ).probe()

    assert availability.status is AdapterStatus.AVAILABLE
    details = availability.details
    assert details is not None
    assert details["package_version"] == "9.9.9"


@pytest.mark.asyncio
async def test_pymupdf_ingest_refuses_missing_license_before_open(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "vector.pdf"
    source_path.write_bytes(b"%PDF-1.4\n%%EOF\n")
    process_attempted = False

    def _runtime_version(_runtime: object) -> str:
        return "1.26.0"

    async def _extract_with_process(
        source: AdapterSource,
        options: AdapterExecutionOptions,
    ) -> tuple[dict[str, Any], list[Any]]:
        nonlocal process_attempted
        process_attempted = True
        _ = (source, options)
        raise AssertionError("process extraction should not be attempted")

    monkeypatch.setattr(pymupdf_adapter, "_load_runtime_module", lambda: object())
    monkeypatch.setattr(pymupdf_adapter, "_runtime_version", _runtime_version)
    monkeypatch.setattr(pymupdf_adapter, "_package_version", lambda: "1.26.0")
    monkeypatch.setattr(pymupdf_adapter, "_extract_with_process", _extract_with_process)

    adapter = pymupdf_adapter.create_adapter()

    with pytest.raises(PermissionError):
        await adapter.ingest(
            AdapterSource(
                file_path=source_path,
                upload_format=UploadFormat.PDF,
                input_family=InputFamily.PDF_VECTOR,
            ),
            AdapterExecutionOptions(),
        )

    assert process_attempted is False


@pytest.mark.asyncio
async def test_pymupdf_vector_fixture_extracts_metadata_only_text() -> None:
    adapter = pymupdf_adapter.create_adapter(license_acknowledged=lambda: True)
    availability = adapter.probe()
    if availability.status is not AdapterStatus.AVAILABLE:
        pytest.skip("PyMuPDF runtime not installed for vector PDF smoke test.")

    fixture_path = Path(__file__).parent / "fixtures" / "pdf" / "vector-smoke.pdf"
    result = await adapter.ingest(
        AdapterSource(
            file_path=fixture_path,
            upload_format=UploadFormat.PDF,
            input_family=InputFamily.PDF_VECTOR,
        ),
        AdapterExecutionOptions(),
    )

    assert result.confidence is not None
    assert result.confidence.review_required is True
    assert result.canonical["schema_version"] == "0.1"
    assert result.canonical["canonical_entity_schema_version"] == "0.1"
    assert result.canonical["blocks"] == ()
    assert result.canonical["xrefs"] == ()
    assert result.canonical["layouts"] == (
        {
            "name": "page-1",
            "page_number": 1,
            "width": 100.0,
            "height": 100.0,
            "rotation": 0,
            "bbox": {"x_min": 0.0, "y_min": 0.0, "x_max": 100.0, "y_max": 100.0},
        },
    )
    layers = cast(tuple[dict[str, str], ...], result.canonical["layers"])
    assert {layer["name"] for layer in layers} == {"default"}

    entities = cast(tuple[dict[str, Any], ...], result.canonical["entities"])
    assert len(entities) == 1
    entity = entities[0]
    assert entity["entity_id"] == "page-1:drawing-0:entity-0"
    assert entity["entity_type"] == "polyline"
    assert entity["entity_schema_version"] == "0.1"
    assert entity["id"] == entity["entity_id"]
    assert entity["kind"] == "polyline"
    assert entity["layout"] == "page-1"
    assert entity["layer"] == "default"
    assert entity["points"] == (
        {"x": 10.0, "y": 90.0},
        {"x": 90.0, "y": 90.0},
        {"x": 90.0, "y": 10.0},
    )
    assert entity["bbox"] == {
        "x_min": 10.0,
        "y_min": 10.0,
        "x_max": 90.0,
        "y_max": 90.0,
    }
    assert entity["properties"]["rect_like"] is False
    assert entity["provenance"] == {
        "page_number": 1,
        "drawing_index": 0,
        "item_indices": (0, 1),
        "source": "pymupdf.get_drawings",
    }

    geometry = cast(dict[str, Any], entity["geometry"])
    assert geometry["kind"] == entity["entity_type"]
    assert geometry["coordinate_space"] == "pdf_page_space_unrotated"
    assert geometry["unit"] == "point"
    assert geometry["bbox"] == entity["bbox"]
    assert geometry["points"] == entity["points"]
    assert geometry["summary"] == {
        "point_count": 3,
        "segment_count": 2,
        "closed": False,
    }

    metadata = cast(dict[str, Any], result.canonical["metadata"])
    assert metadata["geometry_mode"] == "vector"
    assert metadata["pdf_scale"]["status"] == "unconfirmed"
    assert metadata["pdf_scale"]["coordinate_space"] == "pdf_page_space_unrotated"
    assert metadata["text_blocks"] == (
        {
            "page_number": 1,
            "layout": "page-1",
            "block_number": 0,
            "bbox": {
                "x_min": 12.0,
                "y_min": 75.363998,
                "x_max": 20.664001,
                "y_max": 91.372002,
            },
            "text": "V",
        },
    )
    assert all("text" not in entity for entity in entities)
    assert result.warnings == ()


@pytest.mark.asyncio
async def test_pymupdf_ingest_uses_process_hook_instead_of_parent_parser_calls(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "vector.pdf"
    source_path.write_bytes(b"%PDF-1.4\n%%EOF\n")
    adapter = cast(pymupdf_adapter.PyMuPDFAdapter, pymupdf_adapter.create_adapter())

    monkeypatch.setattr(adapter, "_runtime_for_ingest", lambda: None)
    monkeypatch.setattr(
        pymupdf_adapter,
        "_open_document",
        lambda _runtime, _path: (_ for _ in ()).throw(
            AssertionError("parent process should not open the document")
        ),
    )

    async def _extract_with_process(
        source: AdapterSource,
        options: AdapterExecutionOptions,
    ) -> tuple[dict[str, Any], list[Any]]:
        _ = (source, options)
        return (
            {
                "schema_version": "0.1",
                "canonical_entity_schema_version": "0.1",
                "units": {"normalized": "unknown"},
                "coordinate_system": {
                    "name": "pdf_page_space_unrotated",
                    "origin": "top_left",
                    "x_axis": "right",
                    "y_axis": "down",
                },
                "layouts": (),
                "layers": ({"name": "default"},),
                "blocks": (),
                "entities": (),
                "xrefs": (),
                "metadata": {
                    "source_format": UploadFormat.PDF.value,
                    "geometry_mode": "vector",
                    "page_count": 0,
                    "default_layer": "default",
                    "pdf_scale": {
                        "status": "unconfirmed",
                        "coordinate_space": "pdf_page_space_unrotated",
                        "unit": "point",
                        "real_world_units": False,
                    },
                    "text_blocks": (),
                },
            },
            [],
        )

    monkeypatch.setattr(pymupdf_adapter, "_extract_with_process", _extract_with_process)

    result = await adapter.ingest(
        AdapterSource(
            file_path=source_path,
            upload_format=UploadFormat.PDF,
            input_family=InputFamily.PDF_VECTOR,
        ),
        AdapterExecutionOptions(),
    )

    assert result.canonical["entities"] == ()


@pytest.mark.asyncio
async def test_pymupdf_ingest_timeout_stops_child_process(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "vector.pdf"
    source_path.write_bytes(b"%PDF-1.4\n%%EOF\n")
    adapter = cast(pymupdf_adapter.PyMuPDFAdapter, pymupdf_adapter.create_adapter())

    class _FakeHandle:
        def __init__(self) -> None:
            self.closed = False
            self.terminated = False
            self.killed = False

        def poll(self) -> bool:
            return False

        def recv(self) -> dict[str, Any]:
            raise AssertionError("child envelope should not be received")

        def is_alive(self) -> bool:
            return not self.killed

        def join(self, timeout: float | None = None) -> None:
            _ = timeout

        def terminate(self) -> None:
            self.terminated = True

        def kill(self) -> None:
            self.killed = True

        def close(self) -> None:
            self.closed = True

    handle = _FakeHandle()
    perf_values = iter([0.0, 0.0, 0.02])

    monkeypatch.setattr(adapter, "_runtime_for_ingest", lambda: None)
    monkeypatch.setattr(pymupdf_adapter, "_start_extraction_process", lambda _request: handle)
    monkeypatch.setattr(pymupdf_adapter, "_PROCESS_POLL_INTERVAL_SECONDS", 0.0)
    monkeypatch.setattr(
        pymupdf_adapter,
        "perf_counter",
        lambda: next(perf_values, 0.02),
    )

    with pytest.raises(TimeoutError) as exc_info:
        await adapter.ingest(
            AdapterSource(
                file_path=source_path,
                upload_format=UploadFormat.PDF,
                input_family=InputFamily.PDF_VECTOR,
            ),
            AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.01)),
        )

    assert str(exc_info.value) == "PyMuPDF extraction timed out."
    assert handle.terminated is True
    assert handle.killed is True
    assert handle.closed is True


@pytest.mark.asyncio
async def test_pymupdf_ingest_cancellation_stops_child_process(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "vector.pdf"
    source_path.write_bytes(b"%PDF-1.4\n%%EOF\n")
    adapter = cast(pymupdf_adapter.PyMuPDFAdapter, pymupdf_adapter.create_adapter())

    class _FakeHandle:
        def __init__(self) -> None:
            self.closed = False
            self.terminated = False
            self.killed = False

        def poll(self) -> bool:
            return False

        def recv(self) -> dict[str, Any]:
            raise AssertionError("child envelope should not be received")

        def is_alive(self) -> bool:
            return not self.killed

        def join(self, timeout: float | None = None) -> None:
            _ = timeout

        def terminate(self) -> None:
            self.terminated = True

        def kill(self) -> None:
            self.killed = True

        def close(self) -> None:
            self.closed = True

    class _CancellationHandle:
        def __init__(self) -> None:
            self.calls = 0

        def is_cancelled(self) -> bool:
            self.calls += 1
            return self.calls >= 3

    cancellation = _CancellationHandle()
    handle = _FakeHandle()

    monkeypatch.setattr(adapter, "_runtime_for_ingest", lambda: None)
    monkeypatch.setattr(pymupdf_adapter, "_start_extraction_process", lambda _request: handle)
    monkeypatch.setattr(pymupdf_adapter, "_PROCESS_POLL_INTERVAL_SECONDS", 0.0)

    with pytest.raises(asyncio.CancelledError):
        await adapter.ingest(
            AdapterSource(
                file_path=source_path,
                upload_format=UploadFormat.PDF,
                input_family=InputFamily.PDF_VECTOR,
            ),
            AdapterExecutionOptions(cancellation=cancellation),
        )

    assert handle.terminated is True
    assert handle.killed is True
    assert handle.closed is True


@pytest.mark.asyncio
async def test_pymupdf_ingest_enforces_timeout_during_page_boundaries(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    document = _FakeDocument([_FakePage()])

    with pytest.raises(TimeoutError) as exc_info:
        await _ingest_fake_document(
            monkeypatch,
            tmp_path,
            document,
            timeout=AdapterTimeout(seconds=0.005),
            perf_values=[0.0, 0.0, 0.0, 0.0, 0.006],
        )

    assert str(exc_info.value) == "PyMuPDF extraction timed out."
    assert document.closed is True


@pytest.mark.asyncio
async def test_pymupdf_ingest_enforces_cancellation_inside_drawing_loop(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class _CancellationHandle:
        def __init__(self) -> None:
            self.cancelled = False

        def is_cancelled(self) -> bool:
            return self.cancelled

    cancellation = _CancellationHandle()

    drawing = {
        "items": (
            (
                "l",
                _FakePoint(0.0, 0.0, on_read=lambda: setattr(cancellation, "cancelled", True)),
                _FakePoint(10.0, 10.0),
            ),
            ("l", _FakePoint(10.0, 10.0), _FakePoint(20.0, 20.0)),
        ),
        "width": 1.0,
        "color": (0.1, 0.2, 0.3),
    }
    document = _FakeDocument([_FakePage(drawings=[drawing])])

    with pytest.raises(asyncio.CancelledError):
        await _ingest_fake_document(
            monkeypatch,
            tmp_path,
            document,
            cancellation=cancellation,
        )

    assert document.closed is True


@pytest.mark.asyncio
async def test_pymupdf_ingest_enforces_page_cap(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    document = _FakeDocument([_FakePage(), _FakePage()])
    monkeypatch.setattr(pymupdf_adapter, "_MAX_PAGES", 1)

    with pytest.raises(pymupdf_adapter.PyMuPDFExtractionLimitError) as exc_info:
        await _ingest_fake_document(monkeypatch, tmp_path, document)

    assert str(exc_info.value) == "PyMuPDF extraction exceeded page limit."


@pytest.mark.asyncio
async def test_pymupdf_ingest_enforces_entity_cap(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    drawing_one = {
        "items": (("l", _FakePoint(0.0, 0.0), _FakePoint(10.0, 0.0)),),
        "width": 1.0,
        "color": (0.1, 0.2, 0.3),
    }
    drawing_two = {
        "items": (("l", _FakePoint(20.0, 20.0), _FakePoint(30.0, 20.0)),),
        "width": 1.0,
        "color": (0.1, 0.2, 0.3),
    }
    document = _FakeDocument([_FakePage(drawings=[drawing_one, drawing_two])])
    monkeypatch.setattr(pymupdf_adapter, "_MAX_ENTITIES", 1)

    with pytest.raises(pymupdf_adapter.PyMuPDFExtractionLimitError) as exc_info:
        await _ingest_fake_document(monkeypatch, tmp_path, document)

    assert str(exc_info.value) == "PyMuPDF extraction exceeded entity limit."


@pytest.mark.asyncio
async def test_pymupdf_ingest_enforces_page_and_total_drawings_caps(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    drawing = {
        "items": (("l", _FakePoint(0.0, 0.0), _FakePoint(10.0, 0.0)),),
        "width": 1.0,
        "color": (0.1, 0.2, 0.3),
    }

    monkeypatch.setattr(pymupdf_adapter, "_MAX_DRAWINGS_PER_PAGE", 1)
    with pytest.raises(pymupdf_adapter.PyMuPDFExtractionLimitError) as page_exc_info:
        await _ingest_fake_document(
            monkeypatch,
            tmp_path,
            _FakeDocument([_FakePage(drawings=[drawing, drawing])]),
        )
    assert str(page_exc_info.value) == "PyMuPDF extraction exceeded page drawing limit."

    monkeypatch.setattr(pymupdf_adapter, "_MAX_DRAWINGS_PER_PAGE", 10)
    monkeypatch.setattr(pymupdf_adapter, "_MAX_TOTAL_DRAWINGS", 1)
    with pytest.raises(pymupdf_adapter.PyMuPDFExtractionLimitError) as total_exc_info:
        await _ingest_fake_document(
            monkeypatch,
            tmp_path,
            _FakeDocument([_FakePage(drawings=[drawing]), _FakePage(drawings=[drawing])]),
        )
    assert str(total_exc_info.value) == "PyMuPDF extraction exceeded total drawing limit."


@pytest.mark.asyncio
async def test_pymupdf_ingest_enforces_path_item_cap(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    document = _FakeDocument(
        [
            _FakePage(
                drawings=[
                    {
                        "items": (
                            ("l", _FakePoint(0.0, 0.0), _FakePoint(10.0, 0.0)),
                            ("l", _FakePoint(10.0, 0.0), _FakePoint(20.0, 0.0)),
                            ("l", _FakePoint(20.0, 0.0), _FakePoint(30.0, 0.0)),
                        ),
                        "width": 1.0,
                        "color": (0.1, 0.2, 0.3),
                    }
                ]
            )
        ]
    )
    monkeypatch.setattr(pymupdf_adapter, "_MAX_PATH_ITEMS_PER_DRAWING", 2)

    with pytest.raises(pymupdf_adapter.PyMuPDFExtractionLimitError) as exc_info:
        await _ingest_fake_document(monkeypatch, tmp_path, document)

    assert str(exc_info.value) == "PyMuPDF extraction exceeded drawing path item limit."


@pytest.mark.asyncio
async def test_pymupdf_ingest_enforces_connected_polyline_point_cap(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    document = _FakeDocument(
        [
            _FakePage(
                drawings=[
                    {
                        "items": (
                            ("l", _FakePoint(0.0, 0.0), _FakePoint(10.0, 0.0)),
                            ("l", _FakePoint(10.0, 0.0), _FakePoint(20.0, 0.0)),
                            ("l", _FakePoint(20.0, 0.0), _FakePoint(30.0, 0.0)),
                        ),
                        "width": 1.0,
                        "color": (0.1, 0.2, 0.3),
                    }
                ]
            )
        ]
    )
    monkeypatch.setattr(pymupdf_adapter, "_MAX_POINTS_PER_ENTITY", 3)

    with pytest.raises(pymupdf_adapter.PyMuPDFExtractionLimitError) as exc_info:
        await _ingest_fake_document(monkeypatch, tmp_path, document)

    assert str(exc_info.value) == "PyMuPDF extraction exceeded entity point limit."


@pytest.mark.asyncio
async def test_pymupdf_ingest_enforces_text_block_and_text_byte_caps(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    text_payload = {
        "blocks": [
            {
                "type": 0,
                "number": 0,
                "bbox": (0.0, 0.0, 10.0, 10.0),
                "lines": [{"spans": [{"text": "alpha"}]}],
            },
            {
                "type": 0,
                "number": 1,
                "bbox": (20.0, 20.0, 30.0, 30.0),
                "lines": [{"spans": [{"text": "beta"}]}],
            },
        ]
    }
    document = _FakeDocument([_FakePage(text_payload=text_payload)])

    monkeypatch.setattr(pymupdf_adapter, "_MAX_TEXT_BLOCKS", 1)
    with pytest.raises(pymupdf_adapter.PyMuPDFExtractionLimitError) as block_exc_info:
        await _ingest_fake_document(monkeypatch, tmp_path, document)
    assert str(block_exc_info.value) == "PyMuPDF extraction exceeded text block limit."

    monkeypatch.setattr(pymupdf_adapter, "_MAX_TEXT_BLOCKS", 10_000)
    monkeypatch.setattr(pymupdf_adapter, "_MAX_TEXT_BYTES", 4)
    with pytest.raises(pymupdf_adapter.PyMuPDFExtractionLimitError) as bytes_exc_info:
        await _ingest_fake_document(monkeypatch, tmp_path, document)
    assert str(bytes_exc_info.value) == "PyMuPDF extraction exceeded text content limit."


@pytest.mark.asyncio
async def test_pymupdf_ingest_skips_non_finite_entities_and_text_blocks_with_sanitized_warnings(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    document = _FakeDocument(
        [
            _FakePage(
                drawings=[
                    {
                        "items": (("l", _FakePoint(math.inf, 0.0), _FakePoint(10.0, 10.0)),),
                        "width": 1.0,
                        "color": (0.1, 0.2, 0.3),
                    },
                    {
                        "items": (("l", _FakePoint(10.0, 10.0), _FakePoint(20.0, 20.0)),),
                        "width": math.nan,
                        "color": (0.1, 0.2, 0.3),
                    },
                    {
                        "items": (("l", _FakePoint(30.0, 30.0), _FakePoint(40.0, 40.0)),),
                        "width": 1.0,
                        "color": (0.1, 0.2, 0.3),
                    },
                ],
                text_payload={
                    "blocks": [
                        {
                            "type": 0,
                            "number": 0,
                            "bbox": (0.0, 0.0, math.inf, 10.0),
                            "lines": [{"spans": [{"text": "bad"}]}],
                        },
                        {
                            "type": 0,
                            "number": 1,
                            "bbox": (10.0, 10.0, 20.0, 20.0),
                            "lines": [{"spans": [{"text": "good"}]}],
                        },
                    ]
                },
            )
        ]
    )

    result = await _ingest_fake_document(
        monkeypatch,
        tmp_path,
        document,
        original_name="../nested/plan.pdf",
    )

    assert result.provenance[0].source_ref == "originals/plan.pdf"

    entities = cast(tuple[dict[str, Any], ...], result.canonical["entities"])
    assert len(entities) == 1
    assert entities[0]["start"] == {"x": 30.0, "y": 30.0}
    assert entities[0]["end"] == {"x": 40.0, "y": 40.0}

    metadata = cast(dict[str, Any], result.canonical["metadata"])
    assert metadata["text_blocks"] == (
        {
            "page_number": 1,
            "layout": "page-1",
            "block_number": 1,
            "bbox": {
                "x_min": 10.0,
                "y_min": 10.0,
                "x_max": 20.0,
                "y_max": 20.0,
            },
            "text": "good",
        },
    )

    warning_codes = {warning.code for warning in result.warnings}
    assert warning_codes == {
        "pymupdf_path_item_non_finite",
        "pymupdf_entity_non_finite",
        "pymupdf_text_block_non_finite",
    }


@pytest.mark.parametrize(
    ("original_name", "expected"),
    [
        ("C:\\Users\\alice\\plan.pdf", "originals/plan.pdf"),
        ("..\\nested\\plan.pdf", "originals/plan.pdf"),
        ("\\\\server\\share\\plan.pdf", "originals/plan.pdf"),
        ("../nested/plan.pdf", "originals/plan.pdf"),
        ("", "originals/source.pdf"),
    ],
)
def test_pymupdf_durable_source_ref_sanitizes_windows_and_empty_names(
    tmp_path: Path,
    original_name: str,
    expected: str,
) -> None:
    source = AdapterSource(
        file_path=tmp_path / "source.pdf",
        upload_format=UploadFormat.PDF,
        input_family=InputFamily.PDF_VECTOR,
        original_name=original_name,
    )

    assert pymupdf_adapter._durable_source_ref(source) == expected


def test_degraded_status_keeps_optional_binary_issue_visible() -> None:
    descriptor = AdapterDescriptor(
        key="test-adapter",
        family=InputFamily.PDF_RASTER,
        upload_formats=(UploadFormat.PDF,),
        display_name="Test Adapter",
        module="tests.fake",
        license_name="Proprietary",
        capabilities=AdapterCapabilities(),
        confidence_range=(0.3, 0.9),
        probes=(
            ProbeRequirement(
                kind=ProbeKind.BINARY,
                name="vectorizer",
                failure_status=AdapterStatus.DEGRADED,
                detail="Vectorizer binary is optional but recommended.",
            ),
        ),
    )

    availability = evaluate_availability(
        descriptor,
        observations=(
            ProbeObservation(
                kind=ProbeKind.BINARY,
                name="vectorizer",
                status=ProbeStatus.MISSING,
            ),
        ),
    )

    assert availability.status is AdapterStatus.DEGRADED
    assert availability.availability_reason is AvailabilityReason.MISSING_BINARY
    assert availability.license_state is LicenseState.NOT_REQUIRED
    assert availability.last_checked_at is not None
    assert availability.details == {
        "required_probe_count": 1,
        "observed_probe_count": 1,
        "missing_probe_count": 0,
        "issue_count": 1,
    }
    assert {(issue.kind, issue.name) for issue in availability.issues} == {
        (ProbeKind.BINARY, "vectorizer"),
    }


def test_missing_required_probe_observations_block_availability() -> None:
    descriptor = AdapterDescriptor(
        key="missing-probes",
        family=InputFamily.DWG,
        upload_formats=(UploadFormat.DWG,),
        display_name="Missing Probes",
        module="tests.fake",
        license_name="Restricted",
        capabilities=AdapterCapabilities(),
        confidence_range=(0.95, 1.0),
        probes=(
            ProbeRequirement(
                kind=ProbeKind.BINARY,
                name="dwgread",
                failure_status=AdapterStatus.UNAVAILABLE,
                detail="Binary is required.",
            ),
            ProbeRequirement(
                kind=ProbeKind.LICENSE,
                name="deployment-review",
                failure_status=AdapterStatus.UNAVAILABLE,
                detail="License review is required.",
            ),
        ),
    )

    availability = evaluate_availability(descriptor, observations=())

    assert availability.status is AdapterStatus.UNAVAILABLE
    assert availability.availability_reason is AvailabilityReason.MISSING_BINARY
    assert availability.license_state is LicenseState.UNKNOWN
    assert availability.details == {
        "required_probe_count": 2,
        "observed_probe_count": 0,
        "missing_probe_count": 2,
        "issue_count": 2,
    }
    assert [(issue.kind, issue.observed_status) for issue in availability.issues] == [
        (ProbeKind.BINARY, ProbeStatus.UNKNOWN),
        (ProbeKind.LICENSE, ProbeStatus.UNKNOWN),
    ]


def test_missing_required_license_probe_is_unavailable() -> None:
    descriptor = AdapterDescriptor(
        key="required-license",
        family=InputFamily.PDF_VECTOR,
        upload_formats=(UploadFormat.PDF,),
        display_name="Required License",
        module="tests.fake",
        license_name="Restricted",
        capabilities=AdapterCapabilities(),
        confidence_range=(0.6, 0.95),
        probes=(
            ProbeRequirement(
                kind=ProbeKind.LICENSE,
                name="deployment-review",
                failure_status=AdapterStatus.UNAVAILABLE,
                detail="License review is required before use.",
            ),
        ),
    )

    availability = evaluate_availability(
        descriptor,
        observations=(
            ProbeObservation(
                kind=ProbeKind.LICENSE,
                name="deployment-review",
                status=ProbeStatus.MISSING,
            ),
        ),
    )

    assert availability.status is AdapterStatus.UNAVAILABLE
    assert availability.availability_reason is AvailabilityReason.MISSING_LICENSE
    assert availability.license_state is LicenseState.MISSING
    assert availability.issues[0].detail == "License review is required before use."


def test_availability_defaults_license_state_when_no_license_probe_exists() -> None:
    descriptor = AdapterDescriptor(
        key="no-license-probe",
        family=InputFamily.DXF,
        upload_formats=(UploadFormat.DXF,),
        display_name="No License Probe",
        module="tests.fake",
        license_name="MIT",
        capabilities=AdapterCapabilities(extracts_geometry=True),
        confidence_range=(0.95, 1.0),
        probes=(
            ProbeRequirement(
                kind=ProbeKind.PYTHON_PACKAGE,
                name="ezdxf",
                failure_status=AdapterStatus.UNAVAILABLE,
                detail="Package is required.",
            ),
        ),
    )

    availability = evaluate_availability(
        descriptor,
        observations=(
            ProbeObservation(
                kind=ProbeKind.PYTHON_PACKAGE,
                name="ezdxf",
                status=ProbeStatus.AVAILABLE,
            ),
        ),
    )

    assert availability.status is AdapterStatus.AVAILABLE
    assert availability.availability_reason is None
    assert availability.license_state is LicenseState.NOT_REQUIRED


def test_contract_validation_types_exist() -> None:
    timeout = AdapterTimeout(seconds=5)
    progress = ProgressUpdate(stage="extract", completed=1, total=4, percent=0.25)
    diagnostic = AdapterDiagnostic(code="probe", message="timed", elapsed_ms=4.2)

    assert timeout.seconds == 5
    assert progress.stage == "extract"
    assert diagnostic.elapsed_ms == 4.2


def test_error_mapping_uses_shared_error_code_enum() -> None:
    expected = {
        AdapterFailureKind.UNSUPPORTED_FORMAT: ErrorCode.INPUT_UNSUPPORTED_FORMAT,
        AdapterFailureKind.UNAVAILABLE: ErrorCode.ADAPTER_UNAVAILABLE,
        AdapterFailureKind.TIMEOUT: ErrorCode.ADAPTER_TIMEOUT,
        AdapterFailureKind.FAILED: ErrorCode.ADAPTER_FAILED,
        AdapterFailureKind.CANCELLED: ErrorCode.JOB_CANCELLED,
        AdapterFailureKind.INTERNAL: ErrorCode.INTERNAL_ERROR,
    }

    assert {kind: error_code_for_failure(kind) for kind in AdapterFailureKind} == expected


def test_adapter_source_rejects_invalid_upload_family_pairing() -> None:
    from app.ingestion.contracts import AdapterSource

    try:
        AdapterSource(
            file_path=Path("drawing.dwg"),
            upload_format=UploadFormat.DWG,
            input_family=InputFamily.PDF_VECTOR,
        )
    except ValueError as exc:
        assert "is not valid" in str(exc)
    else:
        raise AssertionError("Expected invalid upload/family pairing to fail.")
