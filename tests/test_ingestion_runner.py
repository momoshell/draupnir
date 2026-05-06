"""Tests for ingestion runner scaffolding and source staging."""

from __future__ import annotations

import asyncio
import hashlib
import types
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

from app.core.errors import ErrorCode
from app.ingestion.contracts import (
    AdapterAvailability,
    AdapterResult,
    AdapterStatus,
    AdapterTimeout,
    ConfidenceSummary,
    InputFamily,
    ProgressUpdate,
    ProvenanceRecord,
    UploadFormat,
)
from app.ingestion.runner import IngestionRunnerError, IngestionRunRequest, run_ingestion
from app.ingestion.selection import select_adapter_candidates
from app.ingestion.source import OriginalSourceMaterialization, materialize_original_source
from app.storage.keys import build_original_storage_key
from app.storage.memory import MemoryStorage


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
    assert payload_one.review_state == "provisional"
    assert payload_one.validation_status == "valid"
    assert payload_one.quantity_gate == "allowed_provisional"
    assert payload_one.confidence_score == 0.61
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
async def test_run_ingestion_enforces_source_timeout_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _AdapterModule("timeout_adapter_module", lambda: _FakeAdapter(seen_paths=[]))

    class _SlowMemoryStorage(MemoryStorage):
        async def get(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            await asyncio.sleep(0.02)
            return await super().get(*args, **kwargs)

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
