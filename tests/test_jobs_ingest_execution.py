"""Unit tests for ingest runner execution helpers."""

from __future__ import annotations

import uuid
from typing import Any

from app.core.errors import ErrorCode
from app.ingestion.contracts import AdapterFailureKind, AdapterTimeout, ProgressUpdate
from app.ingestion.runner import IngestionRunnerError, IngestionRunRequest
from app.jobs import ingest_execution
from app.jobs.execution_monitoring import PersistedJobCancellationHandle


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
