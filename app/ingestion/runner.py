"""Scaffolding for adapter-backed ingestion runner execution."""

from __future__ import annotations

import asyncio
import time
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from app.core.errors import ErrorCode
from app.ingestion.contracts import (
    AdapterExecutionOptions,
    AdapterFailureKind,
    AdapterResult,
    AdapterSource,
    AdapterTimeout,
    AvailabilityReason,
    CancellationHandle,
    IngestionAdapter,
    ProgressCallback,
)
from app.ingestion.finalization import (
    IngestFinalizationContext,
    IngestFinalizationPayload,
    build_ingest_finalization_payload,
)
from app.ingestion.loader import load_adapter
from app.ingestion.selection import AdapterCandidate, select_adapter_candidates
from app.ingestion.source import (
    OriginalSourceMaterialization,
    OriginalSourceReadError,
    OriginalSourceStageError,
    materialize_original_source,
)
from app.storage import Storage

_DEFAULT_ADAPTER_TIMEOUT = AdapterTimeout(seconds=300)
_EXECUTE_TIME_DEPENDENCIES: dict[str, str] = {
    "ifcopenshell": "ifcopenshell",
    "pymupdf": "fitz",
}


@dataclass(frozen=True, slots=True)
class IngestionRunRequest:
    """Immutable inputs required to run an adapter-backed ingest attempt."""

    job_id: UUID
    file_id: UUID
    checksum_sha256: str
    detected_format: str | None
    media_type: str | None
    original_name: str | None = None
    extraction_profile_id: UUID | None = None
    initial_job_id: UUID | None = None


class IngestionRunnerError(Exception):
    """Sanitized typed failure surfaced by ingestion runner scaffolding."""

    def __init__(
        self,
        *,
        error_code: ErrorCode,
        failure_kind: AdapterFailureKind,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.failure_kind = failure_kind
        self.message = message
        self.details = details or {}


@dataclass(frozen=True, slots=True)
class _ExecutionPolicy:
    """Shared timeout/cancellation budget for a single ingestion attempt."""

    timeout: AdapterTimeout | None
    cancellation: CancellationHandle | None
    started_at: float

    @classmethod
    def start(
        cls,
        *,
        timeout: AdapterTimeout | None,
        cancellation: CancellationHandle | None,
    ) -> _ExecutionPolicy:
        return cls(timeout=timeout, cancellation=cancellation, started_at=time.monotonic())

    def checkpoint(self, *, stage: str, adapter_key: str | None = None) -> AdapterTimeout | None:
        if self.cancellation is not None and self.cancellation.is_cancelled():
            raise _cancelled_error(stage=stage, adapter_key=adapter_key)

        if self.timeout is None:
            return None

        remaining_seconds = self.timeout.seconds - (time.monotonic() - self.started_at)
        if remaining_seconds <= 0:
            raise _timeout_error(stage=stage, adapter_key=adapter_key)

        return AdapterTimeout(seconds=remaining_seconds)


async def run_ingestion(
    request: IngestionRunRequest,
    *,
    storage: Storage | None = None,
    temp_root: Path | None = None,
    timeout: AdapterTimeout | None = None,
    cancellation: CancellationHandle | None = None,
    on_progress: ProgressCallback | None = None,
    generated_at: datetime | None = None,
) -> IngestFinalizationPayload:
    """Run the first loadable adapter candidate and build a payload."""
    policy = _ExecutionPolicy.start(
        timeout=timeout or _DEFAULT_ADAPTER_TIMEOUT,
        cancellation=cancellation,
    )

    try:
        candidates = select_adapter_candidates(
            request.detected_format,
            media_type=request.media_type,
        )
    except ValueError as exc:
        raise IngestionRunnerError(
            error_code=ErrorCode.INPUT_UNSUPPORTED_FORMAT,
            failure_kind=AdapterFailureKind.UNSUPPORTED_FORMAT,
            message="Input format is not supported for ingestion.",
            details={
                "detected_format": request.detected_format,
                "media_type": request.media_type,
            },
        ) from exc

    last_unavailable_error: IngestionRunnerError | None = None
    for candidate in candidates:
        policy.checkpoint(stage="load", adapter_key=candidate.descriptor.key)
        try:
            adapter = load_adapter(candidate.descriptor)
        except ModuleNotFoundError as exc:
            last_unavailable_error = _adapter_load_error(
                candidate,
                reason=(
                    "module_missing"
                    if exc.name in {None, candidate.descriptor.module}
                    else "dependency_missing"
                ),
            )
            continue
        except AttributeError:
            reason = "factory_missing"
            last_unavailable_error = _adapter_load_error(candidate, reason=reason)
            continue
        except TypeError:
            reason = "factory_invalid"
            last_unavailable_error = _adapter_load_error(candidate, reason=reason)
            continue
        except RuntimeError as exc:
            wrapped_load_error = _wrapped_adapter_load_error(candidate, exc)
            if wrapped_load_error is None:
                raise
            last_unavailable_error = wrapped_load_error
            continue

        materialization = OriginalSourceMaterialization(
            file_id=request.file_id,
            checksum_sha256=request.checksum_sha256,
            upload_format=candidate.upload_format,
            input_family=candidate.input_family,
            media_type=request.media_type,
            original_name=request.original_name,
        )
        source_context = materialize_original_source(
            materialization,
            storage=storage,
            temp_root=temp_root,
        )
        source: AdapterSource | None = None
        try:
            source = await _enter_materialized_source(
                source_context,
                timeout=policy.checkpoint(stage="source", adapter_key=candidate.descriptor.key),
                adapter_key=candidate.descriptor.key,
            )
            policy.checkpoint(stage="source", adapter_key=candidate.descriptor.key)
            remaining_timeout = policy.checkpoint(
                stage="execute",
                adapter_key=candidate.descriptor.key,
            )
            result = await _execute_adapter(
                adapter,
                source,
                timeout=remaining_timeout,
                cancellation=cancellation,
                on_progress=on_progress,
                adapter_key=candidate.descriptor.key,
            )
            policy.checkpoint(stage="execute", adapter_key=candidate.descriptor.key)
        except OriginalSourceReadError as exc:
            raise _storage_error(candidate, exc) from exc
        except OriginalSourceStageError as exc:
            raise _staging_error(candidate, exc) from exc
        except asyncio.CancelledError as exc:
            raise _cancelled_error(stage="source", adapter_key=candidate.descriptor.key) from exc
        finally:
            if source is not None:
                await source_context.__aexit__(None, None, None)

        return build_ingest_finalization_payload(
            IngestFinalizationContext(
                job_id=request.job_id,
                file_id=request.file_id,
                extraction_profile_id=request.extraction_profile_id,
                initial_job_id=request.initial_job_id,
                input_family=candidate.input_family,
                adapter_key=candidate.descriptor.key,
                adapter_version=_adapter_version(candidate, adapter),
            ),
            result=result,
            generated_at=generated_at,
        )

    if last_unavailable_error is not None:
        raise last_unavailable_error

    raise IngestionRunnerError(
        error_code=ErrorCode.ADAPTER_UNAVAILABLE,
        failure_kind=AdapterFailureKind.UNAVAILABLE,
        message="No ingestion adapter candidates were available.",
    )


async def _execute_adapter(
    adapter: IngestionAdapter,
    source: AdapterSource,
    *,
    timeout: AdapterTimeout | None,
    cancellation: CancellationHandle | None,
    on_progress: ProgressCallback | None,
    adapter_key: str,
) -> AdapterResult:
    options = AdapterExecutionOptions(
        timeout=timeout,
        cancellation=cancellation,
        on_progress=on_progress,
    )

    try:
        ingestion = adapter.ingest(source, options)
        if timeout is None:
            return await ingestion
        return await asyncio.wait_for(ingestion, timeout=timeout.seconds)
    except TimeoutError as exc:
        raise IngestionRunnerError(
            error_code=ErrorCode.ADAPTER_TIMEOUT,
            failure_kind=AdapterFailureKind.TIMEOUT,
            message="Adapter execution timed out.",
            details={"adapter_key": adapter_key, "stage": "execute"},
        ) from exc
    except asyncio.CancelledError as exc:
        raise IngestionRunnerError(
            error_code=ErrorCode.JOB_CANCELLED,
            failure_kind=AdapterFailureKind.CANCELLED,
            message="Adapter execution was cancelled.",
            details={"adapter_key": adapter_key, "stage": "execute"},
        ) from exc
    except ModuleNotFoundError as exc:
        dependency_error = _execute_dependency_missing_error(adapter_key, exc)
        if dependency_error is not None:
            raise dependency_error from exc
        raise IngestionRunnerError(
            error_code=ErrorCode.ADAPTER_FAILED,
            failure_kind=AdapterFailureKind.FAILED,
            message="Adapter execution failed.",
            details={"adapter_key": adapter_key},
        ) from exc
    except Exception as exc:
        unavailable_error = _execute_preflight_unavailable_error(adapter_key, exc)
        if unavailable_error is not None:
            raise unavailable_error from exc
        raise IngestionRunnerError(
            error_code=ErrorCode.ADAPTER_FAILED,
            failure_kind=AdapterFailureKind.FAILED,
            message="Adapter execution failed.",
            details={"adapter_key": adapter_key},
        ) from exc


def _adapter_load_error(candidate: AdapterCandidate, *, reason: str) -> IngestionRunnerError:
    return IngestionRunnerError(
        error_code=ErrorCode.ADAPTER_UNAVAILABLE,
        failure_kind=AdapterFailureKind.UNAVAILABLE,
        message="Adapter could not be loaded.",
        details={
            "adapter_key": candidate.descriptor.key,
            "input_family": candidate.input_family.value,
            "reason": reason,
        },
    )


def _execute_dependency_missing_error(
    adapter_key: str,
    exc: ModuleNotFoundError,
) -> IngestionRunnerError | None:
    dependency = _EXECUTE_TIME_DEPENDENCIES.get(adapter_key)
    if dependency is None or exc.name != dependency:
        return None

    return IngestionRunnerError(
        error_code=ErrorCode.ADAPTER_UNAVAILABLE,
        failure_kind=AdapterFailureKind.UNAVAILABLE,
        message="Adapter execution dependency was unavailable.",
        details={
            "adapter_key": adapter_key,
            "stage": "execute",
            "reason": "dependency_missing",
            "dependency": dependency,
        },
    )


def _execute_preflight_unavailable_error(
    adapter_key: str,
    exc: Exception,
) -> IngestionRunnerError | None:
    if adapter_key != "pymupdf":
        return None

    availability_reason = getattr(exc, "availability_reason", None)
    if availability_reason not in {
        AvailabilityReason.MISSING_LICENSE,
        AvailabilityReason.PROBE_FAILED,
    }:
        return None

    return IngestionRunnerError(
        error_code=ErrorCode.ADAPTER_UNAVAILABLE,
        failure_kind=AdapterFailureKind.UNAVAILABLE,
        message="Adapter preflight reported unavailable.",
        details={
            "adapter_key": adapter_key,
            "stage": "execute",
            "reason": availability_reason.value,
        },
    )


def _wrapped_adapter_load_error(
    candidate: AdapterCandidate,
    exc: RuntimeError,
) -> IngestionRunnerError | None:
    cause = exc.__cause__
    if isinstance(cause, ModuleNotFoundError):
        return _adapter_load_error(
            candidate,
            reason=(
                "module_missing"
                if cause.name in {None, candidate.descriptor.module}
                else "dependency_missing"
            ),
        )
    if isinstance(cause, AttributeError):
        return _adapter_load_error(candidate, reason="factory_missing")
    if isinstance(cause, TypeError):
        return _adapter_load_error(candidate, reason="factory_invalid")
    return None


def _storage_error(
    candidate: AdapterCandidate,
    exc: OriginalSourceReadError,
) -> IngestionRunnerError:
    return IngestionRunnerError(
        error_code=ErrorCode.STORAGE_FAILED,
        failure_kind=AdapterFailureKind.FAILED,
        message="Failed to read original source from storage.",
        details={
            "adapter_key": candidate.descriptor.key,
            "input_family": candidate.input_family.value,
            "reason": exc.reason,
        },
    )


def _staging_error(
    candidate: AdapterCandidate,
    exc: OriginalSourceStageError,
) -> IngestionRunnerError:
    return IngestionRunnerError(
        error_code=ErrorCode.STORAGE_FAILED,
        failure_kind=AdapterFailureKind.FAILED,
        message="Failed to stage original source.",
        details={
            "adapter_key": candidate.descriptor.key,
            "input_family": candidate.input_family.value,
            "reason": exc.reason,
        },
    )


async def _enter_materialized_source(
    source_context: AbstractAsyncContextManager[AdapterSource],
    *,
    timeout: AdapterTimeout | None,
    adapter_key: str,
) -> AdapterSource:
    try:
        if timeout is None:
            return await source_context.__aenter__()
        return await asyncio.wait_for(source_context.__aenter__(), timeout=timeout.seconds)
    except TimeoutError as exc:
        raise _timeout_error(stage="source", adapter_key=adapter_key) from exc
    except asyncio.CancelledError as exc:
        raise _cancelled_error(stage="source", adapter_key=adapter_key) from exc


def _timeout_error(*, stage: str, adapter_key: str | None) -> IngestionRunnerError:
    details: dict[str, Any] = {"stage": stage}
    if adapter_key is not None:
        details["adapter_key"] = adapter_key

    return IngestionRunnerError(
        error_code=ErrorCode.ADAPTER_TIMEOUT,
        failure_kind=AdapterFailureKind.TIMEOUT,
        message="Adapter execution timed out.",
        details=details,
    )


def _cancelled_error(*, stage: str, adapter_key: str | None) -> IngestionRunnerError:
    details: dict[str, Any] = {"stage": stage}
    if adapter_key is not None:
        details["adapter_key"] = adapter_key

    return IngestionRunnerError(
        error_code=ErrorCode.JOB_CANCELLED,
        failure_kind=AdapterFailureKind.CANCELLED,
        message="Adapter execution was cancelled.",
        details=details,
    )


def _adapter_version(candidate: AdapterCandidate, adapter: IngestionAdapter) -> str:
    declared_version = candidate.descriptor.adapter_version
    if declared_version is not None:
        return declared_version

    runtime_version = getattr(adapter, "version", None)
    if isinstance(runtime_version, str) and runtime_version:
        return runtime_version

    return "unknown"
