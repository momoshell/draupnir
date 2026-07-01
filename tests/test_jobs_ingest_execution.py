"""Unit tests for ingest runner execution helpers."""

from __future__ import annotations

import uuid
from typing import Any

from app.core.errors import ErrorCode
from app.ingestion.contracts import AdapterFailureKind, AdapterTimeout, ProgressUpdate
from app.ingestion.runner import IngestionRunnerError, IngestionRunRequest
from app.jobs import ingest_execution
from app.jobs.execution_monitoring import PersistedJobCancellationHandle


class _FakeLogger:
    def __init__(self) -> None:
        self.errors: list[tuple[str, dict[str, Any]]] = []

    def error(self, event: str, **fields: Any) -> None:
        self.errors.append((event, fields))


def _request() -> IngestionRunRequest:
    return IngestionRunRequest(
        job_id=uuid.uuid4(),
        file_id=uuid.uuid4(),
        checksum_sha256="a" * 64,
        detected_format="pdf",
        media_type="application/pdf",
    )


def test_runner_error_log_fields_only_keeps_safe_details() -> None:
    error = IngestionRunnerError(
        error_code=ErrorCode.ADAPTER_FAILED,
        failure_kind=AdapterFailureKind.FAILED,
        message="Adapter execution failed.",
        details={
            "adapter_key": "pymupdf",
            "stage": "extract",
            "output_size_bytes": 123,
            "stderr": "sensitive output",
            "source_path": "/tmp/private/source.pdf",
        },
    )

    assert ingest_execution.runner_error_log_fields(error) == {
        "error_code": ErrorCode.ADAPTER_FAILED.value,
        "failure_kind": AdapterFailureKind.FAILED.value,
        "error_message": "Adapter execution failed.",
        "adapter_key": "pymupdf",
        "stage": "extract",
        "output_size_bytes": 123,
    }


def test_runner_supports_keyword_detects_explicit_and_var_keyword() -> None:
    async def explicit_runner(
        request: IngestionRunRequest,
        *,
        timeout: AdapterTimeout,
    ) -> object:
        _ = (request, timeout)
        return object()

    async def var_keyword_runner(request: IngestionRunRequest, **kwargs: Any) -> object:
        _ = (request, kwargs)
        return object()

    async def request_only_runner(request: IngestionRunRequest) -> object:
        _ = request
        return object()

    assert ingest_execution.runner_supports_keyword(explicit_runner, "timeout") is True
    assert ingest_execution.runner_supports_keyword(explicit_runner, "cancellation") is False
    assert ingest_execution.runner_supports_keyword(var_keyword_runner, "cancellation") is True
    assert ingest_execution.runner_supports_keyword(request_only_runner, "timeout") is False


async def test_invoke_ingestion_runner_passes_only_supported_keywords() -> None:
    request = _request()
    timeout = AdapterTimeout(seconds=5)
    cancellation = PersistedJobCancellationHandle()
    payload = object()
    calls: list[dict[str, Any]] = []

    def on_progress(update: ProgressUpdate) -> None:
        _ = update

    async def runner(
        request_arg: IngestionRunRequest,
        *,
        timeout: AdapterTimeout,
        on_progress: Any,
    ) -> object:
        calls.append(
            {
                "request": request_arg,
                "timeout": timeout,
                "on_progress": on_progress,
            }
        )
        return payload

    result = await ingest_execution.invoke_ingestion_runner(
        request,
        runner=runner,
        timeout=timeout,
        cancellation=cancellation,
        on_progress=on_progress,
    )

    assert result is payload
    assert calls == [
        {
            "request": request,
            "timeout": timeout,
            "on_progress": on_progress,
        }
    ]


async def test_invoke_ingestion_runner_passes_all_keywords_to_var_keyword_runner() -> None:
    request = _request()
    timeout = AdapterTimeout(seconds=5)
    cancellation = PersistedJobCancellationHandle()
    payload = object()
    calls: list[dict[str, Any]] = []

    def on_progress(update: ProgressUpdate) -> None:
        _ = update

    async def runner(request_arg: IngestionRunRequest, **kwargs: Any) -> object:
        calls.append({"request": request_arg, "kwargs": kwargs})
        return payload

    result = await ingest_execution.invoke_ingestion_runner(
        request,
        runner=runner,
        timeout=timeout,
        cancellation=cancellation,
        on_progress=on_progress,
    )

    assert result is payload
    assert calls == [
        {
            "request": request,
            "kwargs": {
                "timeout": timeout,
                "cancellation": cancellation,
                "on_progress": on_progress,
            },
        }
    ]


async def test_handle_ingest_runner_error_marks_cancellation_without_failure_log() -> None:
    job_id = uuid.uuid4()
    attempt_token = uuid.uuid4()
    logger = _FakeLogger()
    cancelled_calls: list[dict[str, Any]] = []
    failed_calls: list[dict[str, Any]] = []
    error = IngestionRunnerError(
        error_code=ErrorCode.JOB_CANCELLED,
        failure_kind=AdapterFailureKind.CANCELLED,
        message="Job was cancelled.",
    )

    async def mark_job_cancelled_with_log(job_id: uuid.UUID, **kwargs: Any) -> bool:
        cancelled_calls.append({"job_id": job_id, **kwargs})
        return True

    async def mark_job_failed(job_id: uuid.UUID, **kwargs: Any) -> bool:
        failed_calls.append({"job_id": job_id, **kwargs})
        return True

    await ingest_execution.handle_ingest_runner_error(
        job_id,
        attempt_token=attempt_token,
        exc=error,
        mark_job_cancelled_with_log=mark_job_cancelled_with_log,
        mark_job_failed=mark_job_failed,
        logger_instance=logger,
    )

    assert cancelled_calls == [
        {
            "job_id": job_id,
            "attempt_token": attempt_token,
            "log_event": "ingest_job_cancelled_during_execution",
            "log_fields": {"error_code": ErrorCode.JOB_CANCELLED.value},
        }
    ]
    assert failed_calls == []
    assert logger.errors == []


async def test_handle_ingest_runner_error_marks_failure_with_safe_details() -> None:
    job_id = uuid.uuid4()
    attempt_token = uuid.uuid4()
    logger = _FakeLogger()
    failed_calls: list[dict[str, Any]] = []
    error = IngestionRunnerError(
        error_code=ErrorCode.ADAPTER_FAILED,
        failure_kind=AdapterFailureKind.FAILED,
        message="Adapter execution failed.",
        details={"adapter_key": "pymupdf", "stderr": "do not persist"},
    )

    async def mark_job_cancelled_with_log(job_id: uuid.UUID, **kwargs: Any) -> bool:
        raise AssertionError("non-cancelled failures should not mark cancelled")

    async def mark_job_failed(job_id: uuid.UUID, **kwargs: Any) -> bool:
        failed_calls.append({"job_id": job_id, **kwargs})
        return True

    await ingest_execution.handle_ingest_runner_error(
        job_id,
        attempt_token=attempt_token,
        exc=error,
        mark_job_cancelled_with_log=mark_job_cancelled_with_log,
        mark_job_failed=mark_job_failed,
        logger_instance=logger,
    )

    expected_details = {
        "error_code": ErrorCode.ADAPTER_FAILED.value,
        "failure_kind": AdapterFailureKind.FAILED.value,
        "error_message": "Adapter execution failed.",
        "adapter_key": "pymupdf",
    }
    assert failed_calls == [
        {
            "job_id": job_id,
            "error_message": "Adapter execution failed.",
            "error_code": ErrorCode.ADAPTER_FAILED,
            "attempt_token": attempt_token,
            "error_details": expected_details,
        }
    ]
    assert logger.errors == [("ingest_job_failed", {"job_id": str(job_id), **expected_details})]


def test_build_ingest_finalization_error_log_fields_preserves_legacy_payload() -> None:
    assert ingest_execution.build_ingest_finalization_error_log_fields(
        RuntimeError("finalize exploded")
    ) == {"error": "finalize exploded"}
