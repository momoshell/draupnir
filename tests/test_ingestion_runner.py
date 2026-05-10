"""Tests for ingestion runner scaffolding and source staging."""

from __future__ import annotations

import asyncio
import hashlib
import importlib.machinery
import types
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, BinaryIO, Literal, cast
from uuid import uuid4

import ezdxf
import pytest

from app.core.errors import ErrorCode
from app.ingestion.adapters import ifcopenshell as ifcopenshell_adapter_module
from app.ingestion.adapters import pymupdf as pymupdf_adapter_module
from app.ingestion.contracts import (
    AdapterAvailability,
    AdapterResult,
    AdapterStatus,
    AdapterTimeout,
    AdapterUnavailableError,
    AvailabilityReason,
    ConfidenceSummary,
    InputFamily,
    ProgressUpdate,
    ProvenanceRecord,
    UploadFormat,
)
from app.ingestion.runner import IngestionRunnerError, IngestionRunRequest, run_ingestion
from app.ingestion.selection import select_adapter_candidates
from app.ingestion.source import (
    OriginalSourceMaterialization,
    OriginalSourceReadError,
    OriginalSourceStageError,
    materialize_original_source,
)
from app.storage.keys import build_original_storage_key
from app.storage.local import LocalFilesystemStorage
from app.storage.memory import MemoryStorage

_DXF_SMOKE_FIXTURE = Path(__file__).parent / "fixtures" / "dxf" / "simple-line.dxf"
_IFC_SMOKE_BODY = (
    b"ISO-10303-21;\n"
    b"HEADER;\n"
    b"FILE_SCHEMA(('IFC4'));\n"
    b"ENDSEC;\n"
    b"DATA;\n"
    b"ENDSEC;\n"
    b"END-ISO-10303-21;\n"
)


class _FakeAdapter:
    version = "test-1.0"

    def __init__(self, *, seen_paths: list[Path]) -> None:
        self._seen_paths = seen_paths

    def probe(self) -> AdapterAvailability:
        return AdapterAvailability(status=AdapterStatus.AVAILABLE)

    async def ingest(self, source, options) -> AdapterResult:  # type: ignore[no-untyped-def]
        self._seen_paths.append(source.file_path)
        assert source.file_path.exists()
        assert options.timeout is not None
        assert 0 < options.timeout.seconds <= 300
        return AdapterResult(
            canonical={"entities": ({"kind": "line"},)},
            provenance=(
                ProvenanceRecord(
                    stage="extract",
                    adapter_key="fake-raster",
                    source_ref="originals/source.pdf",
                ),
            ),
            confidence=ConfidenceSummary(score=0.61, review_required=False, basis="raster"),
        )


class _AdapterModule(types.ModuleType):
    create_adapter: Callable[[], object]

    def __init__(self, name: str, create_adapter: Callable[[], object]) -> None:
        super().__init__(name)
        self.create_adapter = create_adapter


class _FakePyMuPDFPoint:
    def __init__(self, x: float, y: float) -> None:
        self.x = x
        self.y = y


class _FakePyMuPDFRect:
    def __init__(self, x0: float, y0: float, x1: float, y1: float) -> None:
        self.x0 = x0
        self.y0 = y0
        self.x1 = x1
        self.y1 = y1
        self.width = x1 - x0
        self.height = y1 - y0


class _FakePyMuPDFPage:
    def __init__(
        self,
        *,
        drawings: list[dict[str, Any]] | None = None,
        text_payload: dict[str, Any] | None = None,
    ) -> None:
        self._drawings = drawings or []
        self._text_payload = text_payload or {"blocks": []}
        self.rect = _FakePyMuPDFRect(0.0, 0.0, 100.0, 100.0)
        self.mediabox = self.rect
        self.rotation = 0

    def get_drawings(self) -> list[dict[str, Any]]:
        return self._drawings

    def get_text(self, kind: str) -> dict[str, Any]:
        assert kind == "dict"
        return self._text_payload


class _FakePyMuPDFDocument:
    def __init__(self, pages: list[_FakePyMuPDFPage]) -> None:
        self._pages = pages
        self.page_count = len(pages)

    def load_page(self, page_index: int) -> _FakePyMuPDFPage:
        return self._pages[page_index]

    def close(self) -> None:
        return None


def _ifcopenshell_module_spec() -> importlib.machinery.ModuleSpec:
    return importlib.machinery.ModuleSpec(name="ifcopenshell", loader=None)


@pytest.mark.asyncio
async def test_materialize_original_source_stages_and_cleans_up(tmp_path: Path) -> None:
    storage = MemoryStorage()
    file_id = uuid4()
    body = b"%PDF-1.7\n"
    checksum = hashlib.sha256(body).hexdigest()
    key = build_original_storage_key(file_id, checksum)
    await storage.put(key, body, immutable=True)

    materialization = OriginalSourceMaterialization(
        file_id=file_id,
        checksum_sha256=checksum,
        upload_format=UploadFormat.PDF,
        input_family=InputFamily.PDF_VECTOR,
        media_type="application/pdf",
        original_name="../drawing.pdf",
    )

    async with materialize_original_source(
        materialization,
        storage=storage,
        temp_root=tmp_path,
    ) as source:
        staged_path = source.file_path
        assert staged_path.exists()
        assert staged_path.name == "source.pdf"
        assert staged_path.read_bytes() == body
        assert source.original_name == "../drawing.pdf"

    assert not staged_path.exists()


@pytest.mark.asyncio
async def test_materialize_original_source_uses_copy_to_path_when_get_is_broken(
    tmp_path: Path,
) -> None:
    class _CopyOnlyMemoryStorage(MemoryStorage):
        async def get(self, *_args: object, **_kwargs: object) -> Any:
            raise AssertionError("get should not be used for staging")

    storage = _CopyOnlyMemoryStorage()
    file_id = uuid4()
    body = b"%PDF-1.7\n"
    checksum = hashlib.sha256(body).hexdigest()
    key = build_original_storage_key(file_id, checksum)
    await storage.put(key, body, immutable=True)

    materialization = OriginalSourceMaterialization(
        file_id=file_id,
        checksum_sha256=checksum,
        upload_format=UploadFormat.PDF,
        input_family=InputFamily.PDF_VECTOR,
        media_type="application/pdf",
    )

    async with materialize_original_source(
        materialization,
        storage=storage,
        temp_root=tmp_path,
    ) as source:
        assert source.file_path.read_bytes() == body


@pytest.mark.asyncio
async def test_materialize_original_source_maps_local_source_read_oserror_to_read_failed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = LocalFilesystemStorage(tmp_path / "storage")
    file_id = uuid4()
    body = b"%PDF-1.7\n"
    checksum = hashlib.sha256(body).hexdigest()
    key = build_original_storage_key(file_id, checksum)
    await storage.put(key, body, immutable=True)
    temp_root = tmp_path / "temp"
    temp_root.mkdir()

    class _BrokenSourceStream:
        def __enter__(self) -> _BrokenSourceStream:
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> Literal[False]:
            return False

        def read(self, _size: int) -> bytes:
            raise OSError("source read failed")

        def close(self) -> None:
            return None

    def _open_copy_source(_source_path: Path, _key: str) -> BinaryIO:
        return cast(BinaryIO, _BrokenSourceStream())

    monkeypatch.setattr(storage, "_open_copy_source", _open_copy_source)

    materialization = OriginalSourceMaterialization(
        file_id=file_id,
        checksum_sha256=checksum,
        upload_format=UploadFormat.PDF,
        input_family=InputFamily.PDF_VECTOR,
        media_type="application/pdf",
    )

    with pytest.raises(OriginalSourceReadError) as exc_info:
        async with materialize_original_source(
            materialization,
            storage=storage,
            temp_root=temp_root,
        ):
            pytest.fail("materialization should fail before yielding")

    assert exc_info.value.reason == "read_failed"
    assert exc_info.value.storage_key == key
    assert list(temp_root.iterdir()) == []


@pytest.mark.asyncio
async def test_materialize_original_source_maps_destination_write_oserror_to_stage_failed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = LocalFilesystemStorage(tmp_path / "storage")
    file_id = uuid4()
    body = b"%PDF-1.7\n"
    checksum = hashlib.sha256(body).hexdigest()
    key = build_original_storage_key(file_id, checksum)
    await storage.put(key, body, immutable=True)
    temp_root = tmp_path / "temp"
    temp_root.mkdir()

    class _BrokenDestinationStream:
        def __init__(self, stream: BinaryIO) -> None:
            self._stream = stream

        def __enter__(self) -> _BrokenDestinationStream:
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> Literal[False]:
            self._stream.close()
            return False

        def write(self, _chunk: bytes) -> int:
            raise OSError("destination write failed")

        def flush(self) -> None:
            self._stream.flush()

        def fileno(self) -> int:
            return self._stream.fileno()

        def close(self) -> None:
            self._stream.close()

    def _open_copy_destination(destination: Path, _key: str) -> BinaryIO:
        return cast(BinaryIO, _BrokenDestinationStream(destination.open("xb")))

    monkeypatch.setattr(storage, "_open_copy_destination", _open_copy_destination)

    materialization = OriginalSourceMaterialization(
        file_id=file_id,
        checksum_sha256=checksum,
        upload_format=UploadFormat.PDF,
        input_family=InputFamily.PDF_VECTOR,
        media_type="application/pdf",
    )

    with pytest.raises(OriginalSourceStageError) as exc_info:
        async with materialize_original_source(
            materialization,
            storage=storage,
            temp_root=temp_root,
        ):
            pytest.fail("materialization should fail before yielding")

    assert exc_info.value.reason == "stage_failed"
    assert list(temp_root.iterdir()) == []


def test_select_adapter_candidates_keeps_pdf_vector_then_raster_order() -> None:
    candidates = select_adapter_candidates("pdf", media_type="application/pdf")

    assert [(candidate.upload_format, candidate.input_family) for candidate in candidates] == [
        (UploadFormat.PDF, InputFamily.PDF_VECTOR),
        (UploadFormat.PDF, InputFamily.PDF_RASTER),
    ]


@pytest.mark.asyncio
async def test_run_ingestion_falls_back_to_next_candidate_and_is_deterministic(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    storage = MemoryStorage()
    file_id = uuid4()
    job_id = uuid4()
    extraction_profile_id = uuid4()
    body = b"%PDF-1.7\n"
    checksum = hashlib.sha256(body).hexdigest()
    key = build_original_storage_key(file_id, checksum)
    await storage.put(key, body, immutable=True)
    seen_paths: list[Path] = []

    raster_module = _AdapterModule(
        "fake_raster_module",
        lambda: _FakeAdapter(seen_paths=seen_paths),
    )

    def fake_import_module(module_name: str) -> types.ModuleType:
        if module_name == "app.ingestion.adapters.pymupdf":
            raise ModuleNotFoundError(name=module_name)
        if module_name == "app.ingestion.adapters.vtracer_tesseract":
            return raster_module
        raise AssertionError(f"Unexpected module import: {module_name}")

    monkeypatch.setattr("app.ingestion.loader.importlib.import_module", fake_import_module)

    request = IngestionRunRequest(
        job_id=job_id,
        file_id=file_id,
        checksum_sha256=checksum,
        detected_format="pdf",
        media_type="application/pdf",
        original_name="sheet.pdf",
        extraction_profile_id=extraction_profile_id,
        initial_job_id=job_id,
    )
    generated_at = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)

    payload_one = await run_ingestion(
        request,
        storage=storage,
        temp_root=tmp_path,
        generated_at=generated_at,
    )
    payload_two = await run_ingestion(
        request,
        storage=storage,
        temp_root=tmp_path,
        generated_at=generated_at,
    )

    assert payload_one.adapter_key == "vtracer_tesseract"
    assert payload_one.adapter_version == "test-1.0"
    assert payload_one.input_family == InputFamily.PDF_RASTER.value
    assert payload_one.revision_kind == "ingest"
    assert payload_one.review_state == "review_required"
    assert payload_one.validation_status == "needs_review"
    assert payload_one.quantity_gate == "review_gated"
    assert payload_one.confidence_score == 0.61
    assert payload_one.report_json["checks"]
    assert payload_one.result_checksum_sha256 == payload_two.result_checksum_sha256
    assert payload_one.canonical_json["canonical_entity_schema_version"] == "0.1"
    assert payload_one.report_json["summary"]["entity_counts"] == {
        "layouts": 0,
        "layers": 0,
        "blocks": 0,
        "entities": 1,
    }
    assert seen_paths
    assert all(not path.exists() for path in seen_paths)


@pytest.mark.asyncio
async def test_run_ingestion_maps_missing_factory_to_sanitized_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broken_module = types.ModuleType("broken_adapter_module")

    def fake_import_module(module_name: str) -> types.ModuleType:
        if module_name == "app.ingestion.adapters.ezdxf":
            return broken_module
        raise AssertionError(f"Unexpected module import: {module_name}")

    monkeypatch.setattr("app.ingestion.loader.importlib.import_module", fake_import_module)

    request = IngestionRunRequest(
        job_id=uuid4(),
        file_id=uuid4(),
        checksum_sha256="deadbeef",
        detected_format="dxf",
        media_type="application/dxf",
    )

    with pytest.raises(IngestionRunnerError) as exc_info:
        await run_ingestion(request, storage=MemoryStorage())

    error = exc_info.value
    assert error.error_code is ErrorCode.ADAPTER_UNAVAILABLE
    assert error.failure_kind.value == "unavailable"
    assert error.message == "Adapter could not be loaded."
    assert error.details["reason"] == "factory_missing"


@pytest.mark.asyncio
async def test_run_ingestion_maps_missing_dependency_to_sanitized_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_import_module(module_name: str) -> types.ModuleType:
        if module_name == "app.ingestion.adapters.ezdxf":
            raise ModuleNotFoundError(name="ezdxf")
        raise AssertionError(f"Unexpected module import: {module_name}")

    monkeypatch.setattr("app.ingestion.loader.importlib.import_module", fake_import_module)

    request = IngestionRunRequest(
        job_id=uuid4(),
        file_id=uuid4(),
        checksum_sha256="deadbeef",
        detected_format="dxf",
        media_type="application/dxf",
    )

    with pytest.raises(IngestionRunnerError) as exc_info:
        await run_ingestion(request, storage=MemoryStorage())

    error = exc_info.value
    assert error.error_code is ErrorCode.ADAPTER_UNAVAILABLE
    assert error.failure_kind.value == "unavailable"
    assert error.message == "Adapter could not be loaded."
    assert error.details["reason"] == "dependency_missing"


@pytest.mark.asyncio
async def test_run_ingestion_maps_ifcopenshell_runtime_missing_during_execute(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    storage = MemoryStorage()
    file_id = uuid4()
    checksum = hashlib.sha256(_IFC_SMOKE_BODY).hexdigest()
    key = build_original_storage_key(file_id, checksum)
    await storage.put(key, _IFC_SMOKE_BODY, immutable=True)

    def fake_import_module(module_name: str) -> types.ModuleType:
        if module_name == "app.ingestion.adapters.ifcopenshell":
            return ifcopenshell_adapter_module
        raise AssertionError(f"Unexpected module import: {module_name}")

    monkeypatch.setattr("app.ingestion.loader.importlib.import_module", fake_import_module)
    monkeypatch.setattr(
        ifcopenshell_adapter_module,
        "_find_runtime_spec",
        _ifcopenshell_module_spec,
    )
    monkeypatch.setattr(ifcopenshell_adapter_module, "_package_version", lambda: "0.8.5")

    def _raise_missing_runtime() -> object:
        raise ModuleNotFoundError(name="ifcopenshell")

    monkeypatch.setattr(
        ifcopenshell_adapter_module,
        "_load_runtime_module",
        _raise_missing_runtime,
    )

    request = IngestionRunRequest(
        job_id=uuid4(),
        file_id=file_id,
        checksum_sha256=checksum,
        detected_format="ifc",
        media_type="application/step",
        original_name="nested/broken.ifc",
    )

    with pytest.raises(IngestionRunnerError) as exc_info:
        await run_ingestion(request, storage=storage, temp_root=tmp_path)

    error = exc_info.value
    assert error.error_code is ErrorCode.ADAPTER_UNAVAILABLE
    assert error.failure_kind.value == "unavailable"
    assert error.message == "Adapter execution dependency was unavailable."
    assert error.details == {
        "adapter_key": "ifcopenshell",
        "stage": "execute",
        "reason": "dependency_missing",
        "dependency": "ifcopenshell",
    }


@pytest.mark.asyncio
async def test_run_ingestion_maps_shared_adapter_availability_error_during_execute(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    storage = MemoryStorage()
    file_id = uuid4()
    checksum = hashlib.sha256(_IFC_SMOKE_BODY).hexdigest()
    key = build_original_storage_key(file_id, checksum)
    await storage.put(key, _IFC_SMOKE_BODY, immutable=True)

    class _UnavailableIfcAdapter:
        def probe(self) -> AdapterAvailability:
            return AdapterAvailability(status=AdapterStatus.AVAILABLE)

        async def ingest(self, source, options) -> AdapterResult:  # type: ignore[no-untyped-def]
            assert source.file_path.exists()
            _ = options
            raise AdapterUnavailableError(
                AvailabilityReason.PROBE_FAILED,
                detail="ifc runtime preflight failed",
            )

    unavailable_module = _AdapterModule(
        "unavailable_ifc_adapter_module",
        lambda: _UnavailableIfcAdapter(),
    )

    def fake_import_module(module_name: str) -> types.ModuleType:
        if module_name == "app.ingestion.adapters.ifcopenshell":
            return unavailable_module
        raise AssertionError(f"Unexpected module import: {module_name}")

    monkeypatch.setattr("app.ingestion.loader.importlib.import_module", fake_import_module)

    request = IngestionRunRequest(
        job_id=uuid4(),
        file_id=file_id,
        checksum_sha256=checksum,
        detected_format="ifc",
        media_type="application/step",
        original_name="nested/unavailable.ifc",
    )

    with pytest.raises(IngestionRunnerError) as exc_info:
        await run_ingestion(request, storage=storage, temp_root=tmp_path)

    error = exc_info.value
    assert error.error_code is ErrorCode.ADAPTER_UNAVAILABLE
    assert error.failure_kind.value == "unavailable"
    assert error.message == "Adapter preflight reported unavailable."
    assert error.details == {
        "adapter_key": "ifcopenshell",
        "stage": "execute",
        "reason": "probe_failed",
        "detail": "ifc runtime preflight failed",
    }


@pytest.mark.asyncio
async def test_run_ingestion_maps_wrapped_runtime_dependency_failure_to_sanitized_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def create_adapter() -> object:
        raise RuntimeError("DXF adapter runtime could not be loaded.") from ModuleNotFoundError(
            name="ezdxf"
        )

    wrapped_module = _AdapterModule("wrapped_runtime_failure_module", create_adapter)

    def fake_import_module(module_name: str) -> types.ModuleType:
        if module_name == "app.ingestion.adapters.ezdxf":
            return wrapped_module
        raise AssertionError(f"Unexpected module import: {module_name}")

    monkeypatch.setattr("app.ingestion.loader.importlib.import_module", fake_import_module)

    request = IngestionRunRequest(
        job_id=uuid4(),
        file_id=uuid4(),
        checksum_sha256="deadbeef",
        detected_format="dxf",
        media_type="application/dxf",
    )

    with pytest.raises(IngestionRunnerError) as exc_info:
        await run_ingestion(request, storage=MemoryStorage())

    error = exc_info.value
    assert error.error_code is ErrorCode.ADAPTER_UNAVAILABLE
    assert error.failure_kind.value == "unavailable"
    assert error.message == "Adapter could not be loaded."
    assert error.details["reason"] == "dependency_missing"


@pytest.mark.asyncio
async def test_run_ingestion_maps_unsupported_format_to_typed_error() -> None:
    request = IngestionRunRequest(
        job_id=uuid4(),
        file_id=uuid4(),
        checksum_sha256="deadbeef",
        detected_format="txt",
        media_type="text/plain",
    )

    with pytest.raises(IngestionRunnerError) as exc_info:
        await run_ingestion(request, storage=MemoryStorage())

    error = exc_info.value
    assert error.error_code is ErrorCode.INPUT_UNSUPPORTED_FORMAT
    assert error.failure_kind.value == "unsupported_format"
    assert error.message == "Input format is not supported for ingestion."


@pytest.mark.asyncio
async def test_run_ingestion_passes_progress_and_cancellation_to_adapter(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    storage = MemoryStorage()
    file_id = uuid4()
    job_id = uuid4()
    body = b"0\nSECTION\n2\nENTITIES\n0\nENDSEC\n0\nEOF\n"
    checksum = hashlib.sha256(body).hexdigest()
    key = build_original_storage_key(file_id, checksum)
    await storage.put(key, body, immutable=True)

    progress_updates: list[ProgressUpdate] = []

    class _CancellationHandle:
        def is_cancelled(self) -> bool:
            return False

    cancellation = _CancellationHandle()

    class _RecordingAdapter:
        def probe(self) -> AdapterAvailability:
            return AdapterAvailability(status=AdapterStatus.AVAILABLE)

        async def ingest(self, source, options) -> AdapterResult:  # type: ignore[no-untyped-def]
            assert source.file_path.exists()
            assert options.cancellation is cancellation
            assert options.on_progress is not None
            update = ProgressUpdate(
                stage="extract",
                message="Reading entities",
                completed=1,
                total=2,
                percent=0.5,
            )
            options.on_progress(update)
            return AdapterResult(
                canonical={"entities": ({"kind": "line"},)},
                provenance=(
                    ProvenanceRecord(
                        stage="extract",
                        adapter_key="fake-dxf",
                        source_ref="originals/source.dxf",
                    ),
                ),
                confidence=ConfidenceSummary(score=0.98, review_required=False, basis="vector"),
            )

    module = _AdapterModule("recording_adapter_module", lambda: _RecordingAdapter())

    def fake_import_module(module_name: str) -> types.ModuleType:
        if module_name == "app.ingestion.adapters.ezdxf":
            return module
        raise AssertionError(f"Unexpected module import: {module_name}")

    monkeypatch.setattr("app.ingestion.loader.importlib.import_module", fake_import_module)

    request = IngestionRunRequest(
        job_id=job_id,
        file_id=file_id,
        checksum_sha256=checksum,
        detected_format="dxf",
        media_type="application/dxf",
        original_name="drawing.dxf",
        initial_job_id=job_id,
    )

    payload = await run_ingestion(
        request,
        storage=storage,
        temp_root=tmp_path,
        cancellation=cancellation,
        on_progress=progress_updates.append,
    )

    assert payload.adapter_key == "ezdxf"
    assert progress_updates == [
        ProgressUpdate(
            stage="extract",
            message="Reading entities",
            completed=1,
            total=2,
            percent=0.5,
        )
    ]


@pytest.mark.asyncio
async def test_run_ingestion_maps_storage_read_errors_to_storage_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _AdapterModule(
        "storage_failure_adapter_module",
        lambda: _FakeAdapter(seen_paths=[]),
    )

    def fake_import_module(module_name: str) -> types.ModuleType:
        if module_name == "app.ingestion.adapters.ezdxf":
            return module
        raise AssertionError(f"Unexpected module import: {module_name}")

    monkeypatch.setattr("app.ingestion.loader.importlib.import_module", fake_import_module)

    request = IngestionRunRequest(
        job_id=uuid4(),
        file_id=uuid4(),
        checksum_sha256="deadbeef",
        detected_format="dxf",
        media_type="application/dxf",
    )

    with pytest.raises(IngestionRunnerError) as exc_info:
        await run_ingestion(request, storage=MemoryStorage())

    error = exc_info.value
    assert error.error_code is ErrorCode.STORAGE_FAILED
    assert error.failure_kind.value == "failed"
    assert error.message == "Failed to read original source from storage."
    assert error.details["reason"] == "not_found"


@pytest.mark.asyncio
async def test_run_ingestion_maps_storage_checksum_mismatch_to_storage_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = MemoryStorage()
    file_id = uuid4()
    expected_body = b"0\nSECTION\n2\nHEADER\n0\nENDSEC\n0\nEOF\n"
    actual_body = b"0\nSECTION\n2\nENTITIES\n0\nENDSEC\n0\nEOF\n"
    expected_checksum = hashlib.sha256(expected_body).hexdigest()
    actual_checksum = hashlib.sha256(actual_body).hexdigest()
    key = build_original_storage_key(file_id, expected_checksum)
    await storage.put(key, actual_body, immutable=True)

    module = _AdapterModule(
        "storage_checksum_mismatch_adapter_module",
        lambda: _FakeAdapter(seen_paths=[]),
    )

    def fake_import_module(module_name: str) -> types.ModuleType:
        if module_name == "app.ingestion.adapters.ezdxf":
            return module
        raise AssertionError(f"Unexpected module import: {module_name}")

    monkeypatch.setattr("app.ingestion.loader.importlib.import_module", fake_import_module)

    request = IngestionRunRequest(
        job_id=uuid4(),
        file_id=file_id,
        checksum_sha256=expected_checksum,
        detected_format="dxf",
        media_type="application/dxf",
    )

    with pytest.raises(IngestionRunnerError) as exc_info:
        await run_ingestion(request, storage=storage)

    error = exc_info.value
    assert error.error_code is ErrorCode.STORAGE_FAILED
    assert error.failure_kind.value == "failed"
    assert error.message == "Failed to read original source from storage."
    assert error.details["reason"] == "checksum_mismatch"
    assert expected_checksum not in str(error.details)
    assert actual_checksum not in str(error.details)


@pytest.mark.asyncio
async def test_run_ingestion_smoke_uses_real_ezdxf_adapter(tmp_path: Path) -> None:
    storage = MemoryStorage()
    file_id = uuid4()
    job_id = uuid4()
    body = _DXF_SMOKE_FIXTURE.read_bytes()
    checksum = hashlib.sha256(body).hexdigest()
    key = build_original_storage_key(file_id, checksum)
    await storage.put(key, body, immutable=True)
    progress_updates: list[ProgressUpdate] = []

    request = IngestionRunRequest(
        job_id=job_id,
        file_id=file_id,
        checksum_sha256=checksum,
        detected_format="dxf",
        media_type="application/dxf",
        original_name="simple-line.dxf",
        initial_job_id=job_id,
    )

    payload = await run_ingestion(
        request,
        storage=storage,
        temp_root=tmp_path,
        on_progress=progress_updates.append,
    )

    assert payload.adapter_key == "ezdxf"
    assert payload.adapter_version != "unknown"
    assert payload.input_family == InputFamily.DXF.value
    assert payload.revision_kind == "ingest"
    assert payload.review_state == "approved"
    assert payload.validation_status == "valid"
    assert payload.quantity_gate == "allowed"
    assert payload.confidence_score >= 0.95
    assert payload.canonical_json["canonical_entity_schema_version"] == "0.1"
    assert payload.canonical_json["units"] == {
        "normalized": "meter",
        "source": "$INSUNITS",
        "source_value": 6,
        "conversion_target": "meter",
        "conversion_factor": 1.0,
    }
    assert payload.canonical_json["layers"] == [
        {"name": "0", "color": 7, "linetype": "Continuous"}
    ]
    assert payload.canonical_json["blocks"] == []
    assert payload.canonical_json["xrefs"] == []
    assert payload.report_json["summary"]["entity_counts"] == {
        "layouts": 2,
        "layers": 1,
        "blocks": 0,
        "entities": 1,
    }
    assert progress_updates[-1] == ProgressUpdate(
        stage="finalize",
        message="Preparing canonical DXF payload.",
        completed=1,
        total=1,
        percent=1.0,
    )

    entity = payload.canonical_json["entities"][0]
    assert entity["entity_id"] == "dxf:1"
    assert entity["kind"] == "line"
    assert entity["entity_type"] == "line"
    assert entity["entity_schema_version"] == "0.1"
    assert entity["handle"] == "1"
    assert entity["layer"] == "0"
    assert entity["layout"] == "Model"
    assert entity["layout_ref"] == "Model"
    assert entity["layer_ref"] == "0"
    assert entity["block_ref"] is None
    assert entity["parent_entity_ref"] is None
    assert entity["start"] == {"x": 0.0, "y": 0.0, "z": 0.0}
    assert entity["end"] == {"x": 10.0, "y": 0.0, "z": 0.0}
    assert entity["length"] == 10.0
    assert entity["geometry"] == {
        "start": {"x": 0.0, "y": 0.0, "z": 0.0},
        "end": {"x": 10.0, "y": 0.0, "z": 0.0},
        "bbox": {
            "min": {"x": 0.0, "y": 0.0, "z": 0.0},
            "max": {"x": 10.0, "y": 0.0, "z": 0.0},
        },
        "units": payload.canonical_json["units"],
        "geometry_summary": {
            "kind": "line_segment",
            "length": 10.0,
            "vertex_count": 2,
        },
    }
    assert entity["properties"]["source_type"] == "LINE"
    assert entity["properties"]["source_handle"] == "1"
    assert entity["properties"]["quantity_hints"] == {"length": 10.0, "count": 1.0}
    assert entity["provenance"]["dxf_handle"] == "1"


@pytest.mark.asyncio
async def test_run_ingestion_review_gates_unitless_dxf_quantities(tmp_path: Path) -> None:
    storage = MemoryStorage()
    file_id = uuid4()
    job_id = uuid4()
    source_path = tmp_path / "unitless-line.dxf"
    dxf_document = cast(Any, ezdxf).new(units=0)
    dxf_document.modelspace().add_line((0.0, 0.0, 0.0), (10.0, 0.0, 0.0))
    dxf_document.saveas(source_path)

    body = source_path.read_bytes()
    checksum = hashlib.sha256(body).hexdigest()
    key = build_original_storage_key(file_id, checksum)
    await storage.put(key, body, immutable=True)

    request = IngestionRunRequest(
        job_id=job_id,
        file_id=file_id,
        checksum_sha256=checksum,
        detected_format="dxf",
        media_type="application/dxf",
        original_name=source_path.name,
        initial_job_id=job_id,
    )

    payload = await run_ingestion(
        request,
        storage=storage,
        temp_root=tmp_path,
    )

    assert payload.review_state == "review_required"
    assert payload.validation_status == "needs_review"
    assert payload.quantity_gate == "review_gated"
    assert payload.canonical_json["units"] == {
        "normalized": "unitless",
        "source": "$INSUNITS",
        "source_value": 0,
        "conversion_target": "meter",
        "conversion_factor": None,
    }


@pytest.mark.asyncio
async def test_run_ingestion_maps_malformed_dxf_to_sanitized_adapter_failure(
    tmp_path: Path,
) -> None:
    storage = MemoryStorage()
    file_id = uuid4()
    body = b"not a dxf at all\n"
    checksum = hashlib.sha256(body).hexdigest()
    key = build_original_storage_key(file_id, checksum)
    await storage.put(key, body, immutable=True)

    request = IngestionRunRequest(
        job_id=uuid4(),
        file_id=file_id,
        checksum_sha256=checksum,
        detected_format="dxf",
        media_type="application/dxf",
        original_name="broken.dxf",
    )

    with pytest.raises(IngestionRunnerError) as exc_info:
        await run_ingestion(request, storage=storage, temp_root=tmp_path)

    error = exc_info.value
    assert error.error_code is ErrorCode.ADAPTER_FAILED
    assert error.failure_kind.value == "failed"
    assert error.message == "Adapter execution failed."
    assert error.details == {"adapter_key": "ezdxf"}
    assert "not a dxf" not in str(error)


@pytest.mark.asyncio
async def test_run_ingestion_sanitizes_ifcopenshell_native_parse_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    storage = MemoryStorage()
    file_id = uuid4()
    binary_body = b"\x00\xff\x00\x01 not a step payload"
    checksum = hashlib.sha256(binary_body).hexdigest()
    key = build_original_storage_key(file_id, checksum)
    await storage.put(key, binary_body, immutable=True)

    def fake_import_module(module_name: str) -> types.ModuleType:
        if module_name == "app.ingestion.adapters.ifcopenshell":
            return ifcopenshell_adapter_module
        raise AssertionError(f"Unexpected module import: {module_name}")

    def _raise_native_parse_failure(path: str) -> object:
        raise RuntimeError(f"native parse failed for {path}: broken STEP payload")

    runtime = types.SimpleNamespace(open=_raise_native_parse_failure)

    monkeypatch.setattr("app.ingestion.loader.importlib.import_module", fake_import_module)
    monkeypatch.setattr(
        ifcopenshell_adapter_module,
        "_find_runtime_spec",
        _ifcopenshell_module_spec,
    )
    monkeypatch.setattr(ifcopenshell_adapter_module, "_package_version", lambda: "0.8.5")
    monkeypatch.setattr(ifcopenshell_adapter_module, "_load_runtime_module", lambda: runtime)

    request = IngestionRunRequest(
        job_id=uuid4(),
        file_id=file_id,
        checksum_sha256=checksum,
        detected_format="ifc",
        media_type="application/step",
        original_name="broken.ifc",
    )

    with pytest.raises(IngestionRunnerError) as exc_info:
        await run_ingestion(request, storage=storage, temp_root=tmp_path)

    error = exc_info.value
    assert error.error_code is ErrorCode.ADAPTER_FAILED
    assert error.failure_kind.value == "failed"
    assert error.message == "Adapter execution failed."
    assert error.details == {"adapter_key": "ifcopenshell"}
    assert "broken STEP payload" not in str(error)
    assert "source.ifc" not in str(error)


@pytest.mark.asyncio
async def test_run_ingestion_does_not_fall_through_after_runtime_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    storage = MemoryStorage()
    file_id = uuid4()
    body = b"%PDF-1.7\n"
    checksum = hashlib.sha256(body).hexdigest()
    key = build_original_storage_key(file_id, checksum)
    await storage.put(key, body, immutable=True)
    attempted_adapters: list[str] = []

    class _FailingVectorAdapter:
        def probe(self) -> AdapterAvailability:
            return AdapterAvailability(status=AdapterStatus.AVAILABLE)

        async def ingest(self, source, options) -> AdapterResult:  # type: ignore[no-untyped-def]
            attempted_adapters.append("pymupdf")
            assert source.file_path.exists()
            _ = options
            raise RuntimeError("vector parse failed")

    class _UnexpectedRasterAdapter:
        def probe(self) -> AdapterAvailability:
            return AdapterAvailability(status=AdapterStatus.AVAILABLE)

        async def ingest(self, source, options) -> AdapterResult:  # type: ignore[no-untyped-def]
            attempted_adapters.append("vtracer_tesseract")
            _ = (source, options)
            raise AssertionError("Runner should not fall through after runtime failure")

    vector_module = _AdapterModule(
        "failing_vector_adapter_module",
        lambda: _FailingVectorAdapter(),
    )
    raster_module = _AdapterModule(
        "unexpected_raster_adapter_module",
        lambda: _UnexpectedRasterAdapter(),
    )

    def fake_import_module(module_name: str) -> types.ModuleType:
        if module_name == "app.ingestion.adapters.pymupdf":
            return vector_module
        if module_name == "app.ingestion.adapters.vtracer_tesseract":
            return raster_module
        raise AssertionError(f"Unexpected module import: {module_name}")

    monkeypatch.setattr("app.ingestion.loader.importlib.import_module", fake_import_module)

    request = IngestionRunRequest(
        job_id=uuid4(),
        file_id=file_id,
        checksum_sha256=checksum,
        detected_format="pdf",
        media_type="application/pdf",
        original_name="sheet.pdf",
    )

    with pytest.raises(IngestionRunnerError) as exc_info:
        await run_ingestion(request, storage=storage, temp_root=tmp_path)

    error = exc_info.value
    assert error.error_code is ErrorCode.ADAPTER_FAILED
    assert error.failure_kind.value == "failed"
    assert attempted_adapters == ["pymupdf"]


@pytest.mark.asyncio
async def test_run_ingestion_maps_pymupdf_execute_dependency_missing_without_raster_fallback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    storage = MemoryStorage()
    file_id = uuid4()
    body = b"%PDF-1.7\n"
    checksum = hashlib.sha256(body).hexdigest()
    key = build_original_storage_key(file_id, checksum)
    await storage.put(key, body, immutable=True)
    attempted_adapters: list[str] = []
    imported_modules: list[str] = []

    class _MissingRuntimeVectorAdapter:
        def probe(self) -> AdapterAvailability:
            return AdapterAvailability(status=AdapterStatus.AVAILABLE)

        async def ingest(self, source, options) -> AdapterResult:  # type: ignore[no-untyped-def]
            attempted_adapters.append("pymupdf")
            assert source.file_path.exists()
            _ = options
            raise ModuleNotFoundError(name="fitz")

    class _UnexpectedRasterAdapter:
        def probe(self) -> AdapterAvailability:
            return AdapterAvailability(status=AdapterStatus.AVAILABLE)

        async def ingest(self, source, options) -> AdapterResult:  # type: ignore[no-untyped-def]
            attempted_adapters.append("vtracer_tesseract")
            _ = (source, options)
            raise AssertionError("Runner should not import or run raster fallback")

    vector_module = _AdapterModule(
        "missing_runtime_vector_adapter_module",
        lambda: _MissingRuntimeVectorAdapter(),
    )
    raster_module = _AdapterModule(
        "unexpected_raster_dependency_adapter_module",
        lambda: _UnexpectedRasterAdapter(),
    )

    def fake_import_module(module_name: str) -> types.ModuleType:
        imported_modules.append(module_name)
        if module_name == "app.ingestion.adapters.pymupdf":
            return vector_module
        if module_name == "app.ingestion.adapters.vtracer_tesseract":
            return raster_module
        raise AssertionError(f"Unexpected module import: {module_name}")

    monkeypatch.setattr("app.ingestion.loader.importlib.import_module", fake_import_module)

    request = IngestionRunRequest(
        job_id=uuid4(),
        file_id=file_id,
        checksum_sha256=checksum,
        detected_format="pdf",
        media_type="application/pdf",
        original_name="sheet.pdf",
    )

    with pytest.raises(IngestionRunnerError) as exc_info:
        await run_ingestion(request, storage=storage, temp_root=tmp_path)

    error = exc_info.value
    assert error.error_code is ErrorCode.ADAPTER_UNAVAILABLE
    assert error.failure_kind.value == "unavailable"
    assert error.message == "Adapter execution dependency was unavailable."
    assert error.details == {
        "adapter_key": "pymupdf",
        "stage": "execute",
        "reason": "dependency_missing",
        "dependency": "fitz",
    }
    assert imported_modules == ["app.ingestion.adapters.pymupdf"]
    assert attempted_adapters == ["pymupdf"]


@pytest.mark.asyncio
async def test_run_ingestion_maps_pymupdf_missing_license_without_raster_fallback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    storage = MemoryStorage()
    file_id = uuid4()
    body = b"%PDF-1.7\n"
    checksum = hashlib.sha256(body).hexdigest()
    key = build_original_storage_key(file_id, checksum)
    await storage.put(key, body, immutable=True)
    imported_modules: list[str] = []
    process_attempted = False

    class _UnexpectedRasterAdapter:
        def probe(self) -> AdapterAvailability:
            return AdapterAvailability(status=AdapterStatus.AVAILABLE)

        async def ingest(self, source, options) -> AdapterResult:  # type: ignore[no-untyped-def]
            _ = (source, options)
            raise AssertionError("Runner should not import or run raster fallback")

    raster_module = _AdapterModule(
        "unexpected_raster_missing_license_module",
        lambda: _UnexpectedRasterAdapter(),
    )

    def fake_import_module(module_name: str) -> types.ModuleType:
        imported_modules.append(module_name)
        if module_name == "app.ingestion.adapters.pymupdf":
            return pymupdf_adapter_module
        if module_name == "app.ingestion.adapters.vtracer_tesseract":
            return raster_module
        raise AssertionError(f"Unexpected module import: {module_name}")

    async def _extract_with_process(source, options) -> tuple[dict[str, Any], list[Any]]:  # type: ignore[no-untyped-def]
        nonlocal process_attempted
        process_attempted = True
        _ = (source, options)
        raise AssertionError("process extraction should not be attempted")

    monkeypatch.setattr("app.ingestion.loader.importlib.import_module", fake_import_module)
    monkeypatch.setattr(pymupdf_adapter_module, "_load_runtime_module", lambda: object())
    monkeypatch.setattr(pymupdf_adapter_module, "_runtime_version", lambda _runtime: "1.26.0")
    monkeypatch.setattr(pymupdf_adapter_module, "_package_version", lambda: "1.26.0")
    monkeypatch.setattr(pymupdf_adapter_module, "_extract_with_process", _extract_with_process)

    request = IngestionRunRequest(
        job_id=uuid4(),
        file_id=file_id,
        checksum_sha256=checksum,
        detected_format="pdf",
        media_type="application/pdf",
        original_name="sheet.pdf",
    )

    with pytest.raises(IngestionRunnerError) as exc_info:
        await run_ingestion(request, storage=storage, temp_root=tmp_path)

    error = exc_info.value
    assert error.error_code is ErrorCode.ADAPTER_UNAVAILABLE
    assert error.failure_kind.value == "unavailable"
    assert error.message == "Adapter preflight reported unavailable."
    assert error.details == {
        "adapter_key": "pymupdf",
        "stage": "execute",
        "reason": "missing_license",
    }
    assert imported_modules == ["app.ingestion.adapters.pymupdf"]
    assert process_attempted is False


@pytest.mark.asyncio
async def test_run_ingestion_maps_vector_execute_timeout_without_importing_raster_fallback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    storage = MemoryStorage()
    file_id = uuid4()
    body = b"%PDF-1.7\n"
    checksum = hashlib.sha256(body).hexdigest()
    key = build_original_storage_key(file_id, checksum)
    await storage.put(key, body, immutable=True)
    attempted_adapters: list[str] = []
    imported_modules: list[str] = []

    class _SlowVectorAdapter:
        def probe(self) -> AdapterAvailability:
            return AdapterAvailability(status=AdapterStatus.AVAILABLE)

        async def ingest(self, source, options) -> AdapterResult:  # type: ignore[no-untyped-def]
            attempted_adapters.append("pymupdf")
            assert source.file_path.exists()
            _ = options
            await asyncio.sleep(0.02)
            raise AssertionError("Runner should time out before returning from vector adapter")

    class _UnexpectedRasterAdapter:
        def probe(self) -> AdapterAvailability:
            return AdapterAvailability(status=AdapterStatus.AVAILABLE)

        async def ingest(self, source, options) -> AdapterResult:  # type: ignore[no-untyped-def]
            attempted_adapters.append("vtracer_tesseract")
            _ = (source, options)
            raise AssertionError("Runner should not import or run raster fallback after timeout")

    vector_module = _AdapterModule(
        "slow_vector_adapter_module",
        lambda: _SlowVectorAdapter(),
    )
    raster_module = _AdapterModule(
        "unexpected_raster_timeout_adapter_module",
        lambda: _UnexpectedRasterAdapter(),
    )

    def fake_import_module(module_name: str) -> types.ModuleType:
        imported_modules.append(module_name)
        if module_name == "app.ingestion.adapters.pymupdf":
            return vector_module
        if module_name == "app.ingestion.adapters.vtracer_tesseract":
            return raster_module
        raise AssertionError(f"Unexpected module import: {module_name}")

    monkeypatch.setattr("app.ingestion.loader.importlib.import_module", fake_import_module)

    request = IngestionRunRequest(
        job_id=uuid4(),
        file_id=file_id,
        checksum_sha256=checksum,
        detected_format="pdf",
        media_type="application/pdf",
        original_name="sheet.pdf",
    )

    with pytest.raises(IngestionRunnerError) as exc_info:
        await run_ingestion(
            request,
            storage=storage,
            temp_root=tmp_path,
            timeout=AdapterTimeout(seconds=0.01),
        )

    error = exc_info.value
    assert error.error_code is ErrorCode.ADAPTER_TIMEOUT
    assert error.failure_kind.value == "timeout"
    assert error.details["stage"] == "execute"
    assert imported_modules == ["app.ingestion.adapters.pymupdf"]
    assert attempted_adapters == ["pymupdf"]


@pytest.mark.asyncio
async def test_run_ingestion_maps_pymupdf_internal_timeout_without_raster_fallback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    storage = MemoryStorage()
    file_id = uuid4()
    body = b"%PDF-1.7\n"
    checksum = hashlib.sha256(body).hexdigest()
    key = build_original_storage_key(file_id, checksum)
    await storage.put(key, body, immutable=True)
    imported_modules: list[str] = []

    def fake_import_module(module_name: str) -> types.ModuleType:
        imported_modules.append(module_name)
        if module_name == "app.ingestion.adapters.pymupdf":
            return pymupdf_adapter_module
        if module_name == "app.ingestion.adapters.vtracer_tesseract":
            raise AssertionError("Raster fallback should not be imported after vector timeout")
        raise AssertionError(f"Unexpected module import: {module_name}")

    async def _extract_with_process(source, options) -> tuple[dict[str, Any], list[Any]]:  # type: ignore[no-untyped-def]
        _ = (source, options)
        raise TimeoutError("PyMuPDF extraction timed out.")

    monkeypatch.setattr("app.ingestion.loader.importlib.import_module", fake_import_module)
    monkeypatch.setattr(pymupdf_adapter_module, "_license_unacknowledged", lambda: True)
    monkeypatch.setattr(pymupdf_adapter_module, "_load_runtime_module", lambda: object())
    monkeypatch.setattr(pymupdf_adapter_module, "_runtime_version", lambda _runtime: "1.26.0")
    monkeypatch.setattr(pymupdf_adapter_module, "_package_version", lambda: "1.26.0")
    monkeypatch.setattr(pymupdf_adapter_module, "_extract_with_process", _extract_with_process)

    request = IngestionRunRequest(
        job_id=uuid4(),
        file_id=file_id,
        checksum_sha256=checksum,
        detected_format="pdf",
        media_type="application/pdf",
        original_name="sheet.pdf",
    )

    with pytest.raises(IngestionRunnerError) as exc_info:
        await run_ingestion(
            request,
            storage=storage,
            temp_root=tmp_path,
            timeout=AdapterTimeout(seconds=0.005),
        )

    error = exc_info.value
    assert error.error_code is ErrorCode.ADAPTER_TIMEOUT
    assert error.failure_kind.value == "timeout"
    assert error.details == {"adapter_key": "pymupdf", "stage": "execute"}
    assert imported_modules == ["app.ingestion.adapters.pymupdf"]


@pytest.mark.asyncio
async def test_run_ingestion_keeps_pymupdf_cap_failure_without_raster_fallback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    storage = MemoryStorage()
    file_id = uuid4()
    body = b"%PDF-1.7\n"
    checksum = hashlib.sha256(body).hexdigest()
    key = build_original_storage_key(file_id, checksum)
    await storage.put(key, body, immutable=True)
    imported_modules: list[str] = []

    def fake_import_module(module_name: str) -> types.ModuleType:
        imported_modules.append(module_name)
        if module_name == "app.ingestion.adapters.pymupdf":
            return pymupdf_adapter_module
        if module_name == "app.ingestion.adapters.vtracer_tesseract":
            raise AssertionError("Raster fallback should not be imported after vector cap failure")
        raise AssertionError(f"Unexpected module import: {module_name}")

    async def _extract_with_process(source, options) -> tuple[dict[str, Any], list[Any]]:  # type: ignore[no-untyped-def]
        _ = (source, options)
        raise pymupdf_adapter_module.PyMuPDFExtractionLimitError(
            "PyMuPDF extraction exceeded entity limit."
        )

    monkeypatch.setattr("app.ingestion.loader.importlib.import_module", fake_import_module)
    monkeypatch.setattr(pymupdf_adapter_module, "_license_unacknowledged", lambda: True)
    monkeypatch.setattr(pymupdf_adapter_module, "_load_runtime_module", lambda: object())
    monkeypatch.setattr(pymupdf_adapter_module, "_runtime_version", lambda _runtime: "1.26.0")
    monkeypatch.setattr(pymupdf_adapter_module, "_package_version", lambda: "1.26.0")
    monkeypatch.setattr(pymupdf_adapter_module, "_extract_with_process", _extract_with_process)

    request = IngestionRunRequest(
        job_id=uuid4(),
        file_id=file_id,
        checksum_sha256=checksum,
        detected_format="pdf",
        media_type="application/pdf",
        original_name="sheet.pdf",
    )

    with pytest.raises(IngestionRunnerError) as exc_info:
        await run_ingestion(request, storage=storage, temp_root=tmp_path)

    error = exc_info.value
    assert error.error_code is ErrorCode.ADAPTER_FAILED
    assert error.failure_kind.value == "failed"
    assert error.details == {"adapter_key": "pymupdf"}
    assert imported_modules == ["app.ingestion.adapters.pymupdf"]


@pytest.mark.asyncio
async def test_run_ingestion_keeps_pymupdf_no_vector_result_without_raster_fallback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    storage = MemoryStorage()
    file_id = uuid4()
    body = b"%PDF-1.7\n"
    checksum = hashlib.sha256(body).hexdigest()
    key = build_original_storage_key(file_id, checksum)
    await storage.put(key, body, immutable=True)
    attempted_adapters: list[str] = []
    imported_modules: list[str] = []

    class _NoVectorAdapter:
        version = "vector-no-entities-1.0"

        def probe(self) -> AdapterAvailability:
            return AdapterAvailability(status=AdapterStatus.AVAILABLE)

        async def ingest(self, source, options) -> AdapterResult:  # type: ignore[no-untyped-def]
            attempted_adapters.append("pymupdf")
            assert source.file_path.exists()
            _ = options
            return AdapterResult(
                canonical={"entities": ()},
                provenance=(
                    ProvenanceRecord(
                        stage="extract",
                        adapter_key="pymupdf",
                        source_ref="originals/source.pdf",
                    ),
                ),
                confidence=ConfidenceSummary(score=0.59, review_required=True, basis="vector"),
                warnings=cast(Any, ("No vector drawing entities were extracted.",)),
            )

    vector_module = _AdapterModule("no_vector_adapter_module", lambda: _NoVectorAdapter())

    def fake_import_module(module_name: str) -> types.ModuleType:
        imported_modules.append(module_name)
        if module_name == "app.ingestion.adapters.pymupdf":
            return vector_module
        if module_name == "app.ingestion.adapters.vtracer_tesseract":
            raise AssertionError("Raster fallback should not be imported for no-vector result")
        raise AssertionError(f"Unexpected module import: {module_name}")

    monkeypatch.setattr("app.ingestion.loader.importlib.import_module", fake_import_module)

    request = IngestionRunRequest(
        job_id=uuid4(),
        file_id=file_id,
        checksum_sha256=checksum,
        detected_format="pdf",
        media_type="application/pdf",
        original_name="sheet.pdf",
    )

    payload = await run_ingestion(request, storage=storage, temp_root=tmp_path)

    assert payload.adapter_key == "pymupdf"
    assert payload.input_family == InputFamily.PDF_VECTOR.value
    assert payload.review_state == "review_required"
    assert payload.validation_status == "needs_review"
    assert payload.quantity_gate == "review_gated"
    assert payload.canonical_json["entities"] == []
    assert payload.report_json["summary"]["entity_counts"]["entities"] == 0
    assert payload.report_json["checks"]
    assert imported_modules == ["app.ingestion.adapters.pymupdf"]
    assert attempted_adapters == ["pymupdf"]


@pytest.mark.asyncio
async def test_run_ingestion_enforces_source_timeout_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _AdapterModule("timeout_adapter_module", lambda: _FakeAdapter(seen_paths=[]))

    class _SlowMemoryStorage(MemoryStorage):
        async def copy_to_path(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            await asyncio.sleep(0.02)
            return await super().copy_to_path(*args, **kwargs)

    def fake_import_module(module_name: str) -> types.ModuleType:
        if module_name == "app.ingestion.adapters.ezdxf":
            return module
        raise AssertionError(f"Unexpected module import: {module_name}")

    monkeypatch.setattr("app.ingestion.loader.importlib.import_module", fake_import_module)

    storage = _SlowMemoryStorage()
    file_id = uuid4()
    body = b"0\nSECTION\n2\nENTITIES\n0\nENDSEC\n0\nEOF\n"
    checksum = hashlib.sha256(body).hexdigest()
    key = build_original_storage_key(file_id, checksum)
    await storage.put(key, body, immutable=True)

    request = IngestionRunRequest(
        job_id=uuid4(),
        file_id=file_id,
        checksum_sha256=checksum,
        detected_format="dxf",
        media_type="application/dxf",
    )

    with pytest.raises(IngestionRunnerError) as exc_info:
        await run_ingestion(request, storage=storage, timeout=AdapterTimeout(seconds=0.01))

    error = exc_info.value
    assert error.error_code is ErrorCode.ADAPTER_TIMEOUT
    assert error.details["stage"] == "source"


@pytest.mark.asyncio
async def test_run_ingestion_cleans_up_staged_source_after_adapter_execution_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    storage = MemoryStorage()
    file_id = uuid4()
    body = b"0\nSECTION\n2\nENTITIES\n0\nENDSEC\n0\nEOF\n"
    checksum = hashlib.sha256(body).hexdigest()
    key = build_original_storage_key(file_id, checksum)
    await storage.put(key, body, immutable=True)
    seen_paths: list[Path] = []

    class _FailingAdapter:
        def probe(self) -> AdapterAvailability:
            return AdapterAvailability(status=AdapterStatus.AVAILABLE)

        async def ingest(self, source, options) -> AdapterResult:  # type: ignore[no-untyped-def]
            seen_paths.append(source.file_path)
            assert source.file_path.exists()
            _ = options
            raise RuntimeError("adapter exploded")

    module = _AdapterModule("failing_cleanup_adapter_module", lambda: _FailingAdapter())

    def fake_import_module(module_name: str) -> types.ModuleType:
        if module_name == "app.ingestion.adapters.ezdxf":
            return module
        raise AssertionError(f"Unexpected module import: {module_name}")

    monkeypatch.setattr("app.ingestion.loader.importlib.import_module", fake_import_module)

    request = IngestionRunRequest(
        job_id=uuid4(),
        file_id=file_id,
        checksum_sha256=checksum,
        detected_format="dxf",
        media_type="application/dxf",
        original_name="drawing.dxf",
    )

    with pytest.raises(IngestionRunnerError) as exc_info:
        await run_ingestion(request, storage=storage, temp_root=tmp_path)

    error = exc_info.value
    assert error.error_code is ErrorCode.ADAPTER_FAILED
    assert error.failure_kind.value == "failed"
    assert seen_paths
    assert all(not path.exists() for path in seen_paths)


@pytest.mark.asyncio
async def test_run_ingestion_enforces_cancellation_checkpoint_before_execute(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    storage = MemoryStorage()
    file_id = uuid4()
    body = b"0\nSECTION\n2\nENTITIES\n0\nENDSEC\n0\nEOF\n"
    checksum = hashlib.sha256(body).hexdigest()
    key = build_original_storage_key(file_id, checksum)
    await storage.put(key, body, immutable=True)

    class _CheckpointCancellationHandle:
        def __init__(self) -> None:
            self._calls = 0

        def is_cancelled(self) -> bool:
            self._calls += 1
            return self._calls >= 4

    cancellation = _CheckpointCancellationHandle()

    class _UnexpectedAdapter:
        def probe(self) -> AdapterAvailability:
            return AdapterAvailability(status=AdapterStatus.AVAILABLE)

        async def ingest(self, source, options) -> AdapterResult:  # type: ignore[no-untyped-def]
            _ = (source, options)
            raise AssertionError("Adapter ingest should not start after cancellation checkpoint")

    module = _AdapterModule("cancelled_adapter_module", lambda: _UnexpectedAdapter())

    def fake_import_module(module_name: str) -> types.ModuleType:
        if module_name == "app.ingestion.adapters.ezdxf":
            return module
        raise AssertionError(f"Unexpected module import: {module_name}")

    monkeypatch.setattr("app.ingestion.loader.importlib.import_module", fake_import_module)

    request = IngestionRunRequest(
        job_id=uuid4(),
        file_id=file_id,
        checksum_sha256=checksum,
        detected_format="dxf",
        media_type="application/dxf",
    )

    with pytest.raises(IngestionRunnerError) as exc_info:
        await run_ingestion(
            request,
            storage=storage,
            temp_root=tmp_path,
            cancellation=cancellation,
        )

    error = exc_info.value
    assert error.error_code is ErrorCode.JOB_CANCELLED
    assert error.details["stage"] == "execute"
