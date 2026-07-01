"""Ingest runner execution helpers used by worker job attempts."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import Any, cast
from uuid import UUID

from app.core.errors import ErrorCode
from app.ingestion.contracts import AdapterTimeout
from app.ingestion.finalization import IngestFinalizationPayload
from app.ingestion.runner import IngestionRunnerError, IngestionRunRequest
from app.jobs.execution_monitoring import PersistedJobCancellationHandle

SAFE_RUNNER_ERROR_DETAIL_KEYS = (
    "adapter_key",
    "input_family",
    "reason",
    "detail",
    "stage",
    "detected_format",
    "media_type",
    "output_kind",
    "max_output_bytes",
    "output_size_bytes",
)


def runner_error_log_fields(
    exc: IngestionRunnerError,
    *,
    safe_detail_keys: tuple[str, ...] = SAFE_RUNNER_ERROR_DETAIL_KEYS,
) -> dict[str, Any]:
    """Return whitelisted structured fields for expected runner failures."""
    data_json: dict[str, Any] = {
        "error_code": exc.error_code.value,
        "failure_kind": exc.failure_kind.value,
        "error_message": exc.message,
    }
    for key in safe_detail_keys:
        value = exc.details.get(key)
        if value is not None:
            data_json[key] = value
    return data_json


def runner_supports_keyword(runner: Callable[..., Any], keyword: str) -> bool:
    """Return whether a runner callable accepts a given keyword."""
    signature = inspect.signature(runner)
    parameters = signature.parameters.values()
    if any(parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters):
        return True
    return keyword in signature.parameters


async def invoke_ingestion_runner(
    request: IngestionRunRequest,
    *,
    runner: Callable[..., Any],
    timeout: AdapterTimeout,
    cancellation: PersistedJobCancellationHandle,
    on_progress: Any,
) -> IngestFinalizationPayload:
    """Call the runner while remaining compatible with patched test doubles."""
    kwargs: dict[str, Any] = {}
    if runner_supports_keyword(runner, "timeout"):
        kwargs["timeout"] = timeout
    if runner_supports_keyword(runner, "cancellation"):
        kwargs["cancellation"] = cancellation
    if runner_supports_keyword(runner, "on_progress"):
        kwargs["on_progress"] = on_progress
    return cast(IngestFinalizationPayload, await runner(request, **kwargs))


async def handle_ingest_runner_error(
    job_id: UUID,
    *,
    attempt_token: UUID,
    exc: IngestionRunnerError,
    mark_job_cancelled_with_log: Callable[..., Awaitable[Any]],
    mark_job_failed: Callable[..., Awaitable[Any]],
    logger_instance: Any,
    runner_error_log_fields_func: Callable[[IngestionRunnerError], dict[str, Any]] = (
        runner_error_log_fields
    ),
) -> None:
    """Persist and log the expected ingest runner failure contract."""
    if exc.error_code is ErrorCode.JOB_CANCELLED:
        await mark_job_cancelled_with_log(
            job_id,
            attempt_token=attempt_token,
            log_event="ingest_job_cancelled_during_execution",
            log_fields={"error_code": exc.error_code.value},
        )
        return

    error_details = runner_error_log_fields_func(exc)
    await mark_job_failed(
        job_id,
        error_message=exc.message,
        error_code=exc.error_code,
        attempt_token=attempt_token,
        error_details=error_details,
    )
    logger_instance.error("ingest_job_failed", job_id=str(job_id), **error_details)


def build_ingest_finalization_error_log_fields(exc: Exception) -> dict[str, str]:
    """Preserve the legacy ingest finalization error log payload."""
    return {"error": str(exc)}
