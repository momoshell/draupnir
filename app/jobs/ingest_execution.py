"""Ingest runner execution helpers used by worker job attempts."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, cast

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
