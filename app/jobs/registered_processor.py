"""Shared registered worker job processing shell."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, cast
from uuid import UUID

from app.jobs import runner as job_runner


@dataclass(frozen=True, slots=True)
class RegisteredJobAttemptResult:
    """Deferred finalization kwargs returned by a registered job execution step."""

    finalize_kwargs: dict[str, Any]


@dataclass(frozen=True, slots=True)
class RegisteredJobProcessSpec:
    """Shared shell configuration for registered persisted worker jobs."""

    job_type_name: job_runner.JobTypeName
    input_error_type: type[Exception] | None
    input_failure_log_event: str | None
    stale_attempt_log_event: str
    revision_conflict_log_event: str
    cancelled_during_execution_log_event: str
    cancelled_during_finalization_log_event: str
    process_failed_log_event: str
    finalization_failed_log_event: str
    succeeded_log_event: str
    process_error_message: str
    finalize_error_message: str
    execution_result_arg_name: str | None = None
    execution_error_type: type[Exception] | None = None
    execution_error_handler_name: str | None = None
    inactive_source_log_event: str | None = None
    finalization_exception_log_fields_name: str | None = None
    reraise_revision_conflict: bool = False


def get_registered_job_handler(job_type_name: job_runner.JobTypeName) -> job_runner.JobHandler:
    """Return registered worker metadata for one persisted job type."""
    handler = job_runner.get_job_handler(job_type_name)
    if handler is None:
        raise LookupError(f"No worker handler registered for job type '{job_type_name}'")
    return handler


def resolve_registered_job_callable(name: str, *, namespace: dict[str, Any]) -> Any:
    """Late-bind a worker callable by name for monkeypatch-friendly wrappers."""
    resolved = namespace.get(name)
    if resolved is None:
        raise LookupError(f"Worker callable '{name}' is not defined")
    return resolved


async def process_registered_job(
    job_id: UUID,
    *,
    spec: RegisteredJobProcessSpec,
    deps: Any,
    ensure_worker_database_configured: Callable[[], None],
    get_registered_job_handler_func: Callable[
        [job_runner.JobTypeName], job_runner.JobHandler
    ] = get_registered_job_handler,
    begin_or_resume_registered_job: Callable[..., Awaitable[Any]],
    cancel_registered_job_if_requested: Callable[..., Awaitable[bool]],
    resolve_registered_job_callable_func: Callable[[str], Any],
    with_job_attempt_lease_renewal: Callable[..., Awaitable[Any]],
    mark_job_failed_for_revision_conflict: Callable[..., Awaitable[None]],
    mark_job_cancelled_with_log: Callable[..., Awaitable[None]],
    mark_changeset_apply_job_failed_for_revision_conflict: Callable[..., Awaitable[None]],
    mark_changeset_apply_job_failed_for_input_error: Callable[..., Awaitable[None]],
    mark_job_failed: Callable[..., Awaitable[bool]],
    mark_job_failed_with_internal_error_log: Callable[..., Awaitable[None]],
    inactive_source_error_type: type[Exception],
    stale_job_attempt_error_type: type[Exception],
    revision_conflict_error_type: type[Exception],
    changeset_apply_job_error_type: type[Exception],
    logger_instance: Any,
) -> None:
    """Run one registered persisted worker job through the shared execution shell."""
    ensure_worker_database_configured()

    handler = get_registered_job_handler_func(spec.job_type_name)
    execute_name = handler.execute_name
    finalize_name = handler.finalize_name
    if execute_name is None:
        raise LookupError(f"Worker handler '{spec.job_type_name}' is missing execute_name")
    if finalize_name is None:
        raise LookupError(f"Worker handler '{spec.job_type_name}' is missing finalize_name")

    lease = await begin_or_resume_registered_job(job_id, process_name=handler.process_name)
    if lease is None:
        return

    if await cancel_registered_job_if_requested(
        job_id,
        attempt_token=lease.token,
        log_event=spec.cancelled_during_execution_log_event,
    ):
        return

    execute_job = cast(Any, resolve_registered_job_callable_func(execute_name))
    finalize_job = cast(Any, resolve_registered_job_callable_func(finalize_name))

    try:
        execution_result = await with_job_attempt_lease_renewal(
            execute_job(job_id, attempt_token=lease.token, deps=deps),
            job_id=job_id,
            attempt_token=lease.token,
        )
    except asyncio.CancelledError:
        await mark_job_cancelled_with_log(
            job_id,
            attempt_token=lease.token,
            log_event=spec.cancelled_during_execution_log_event,
        )
        raise
    except Exception as exc:
        if isinstance(exc, inactive_source_error_type):
            if spec.inactive_source_log_event is None:
                raise
            logger_instance.info(spec.inactive_source_log_event, job_id=str(job_id))
            return
        if isinstance(exc, stale_job_attempt_error_type):
            logger_instance.info(spec.stale_attempt_log_event, job_id=str(job_id))
            return
        if isinstance(exc, revision_conflict_error_type):
            if spec.job_type_name == "changeset_apply":
                await mark_changeset_apply_job_failed_for_revision_conflict(
                    job_id,
                    attempt_token=lease.token,
                    log_event=spec.revision_conflict_log_event,
                    exc=exc,
                )
            else:
                await mark_job_failed_for_revision_conflict(
                    job_id,
                    attempt_token=lease.token,
                    log_event=spec.revision_conflict_log_event,
                    exc=exc,
                )
            if spec.reraise_revision_conflict:
                raise
            return
        if spec.input_error_type is not None and isinstance(exc, spec.input_error_type):
            input_error = cast(Any, exc)
            failure_details = getattr(input_error, "details", None)
            if spec.input_failure_log_event is None:
                raise AssertionError(
                    f"Registered job '{spec.job_type_name}' is missing input_failure_log_event"
                ) from exc
            if spec.job_type_name == "changeset_apply":
                assert isinstance(input_error, changeset_apply_job_error_type)
                await mark_changeset_apply_job_failed_for_input_error(
                    job_id,
                    attempt_token=lease.token,
                    log_event=spec.input_failure_log_event,
                    exc=input_error,
                )
            else:
                await mark_job_failed(
                    job_id,
                    error_message=input_error.message,
                    error_code=input_error.error_code,
                    attempt_token=lease.token,
                    error_details=failure_details,
                )
                logger_instance.warning(
                    spec.input_failure_log_event,
                    job_id=str(job_id),
                    error_code=input_error.error_code.value,
                    **(failure_details or {}),
                )
            return

        if spec.execution_error_type is not None and isinstance(exc, spec.execution_error_type):
            if spec.execution_error_handler_name is None:
                raise AssertionError(
                    f"Registered job '{spec.job_type_name}' is missing execution_error_handler_name"
                ) from exc
            handle_execution_error = cast(
                Any,
                resolve_registered_job_callable_func(spec.execution_error_handler_name),
            )
            await handle_execution_error(job_id, attempt_token=lease.token, exc=exc)
            raise

        await mark_job_failed_with_internal_error_log(
            job_id,
            attempt_token=lease.token,
            error_message=spec.process_error_message,
            log_event=spec.process_failed_log_event,
        )
        raise

    if execution_result is None:
        return

    if isinstance(execution_result, RegisteredJobAttemptResult):
        finalize_kwargs = execution_result.finalize_kwargs
    else:
        if spec.execution_result_arg_name is None:
            raise TypeError(
                "Registered job "
                f"'{spec.job_type_name}' returned a raw execution result without "
                "execution_result_arg_name"
            )
        finalize_kwargs = {spec.execution_result_arg_name: execution_result}

    try:
        finalized = await with_job_attempt_lease_renewal(
            finalize_job(
                job_id,
                attempt_token=lease.token,
                deps=deps,
                **finalize_kwargs,
            ),
            job_id=job_id,
            attempt_token=lease.token,
        )
    except asyncio.CancelledError:
        await mark_job_cancelled_with_log(
            job_id,
            attempt_token=lease.token,
            log_event=spec.cancelled_during_finalization_log_event,
        )
        raise
    except Exception as exc:
        if isinstance(exc, revision_conflict_error_type):
            if spec.job_type_name == "changeset_apply":
                await mark_changeset_apply_job_failed_for_revision_conflict(
                    job_id,
                    attempt_token=lease.token,
                    log_event=spec.revision_conflict_log_event,
                    exc=exc,
                )
            else:
                await mark_job_failed_for_revision_conflict(
                    job_id,
                    attempt_token=lease.token,
                    log_event=spec.revision_conflict_log_event,
                    exc=exc,
                )
            if spec.reraise_revision_conflict:
                raise
            return

        log_fields: dict[str, Any] | None = None
        if spec.finalization_exception_log_fields_name is not None:
            build_log_fields = cast(
                Callable[[Exception], dict[str, Any]],
                resolve_registered_job_callable_func(spec.finalization_exception_log_fields_name),
            )
            log_fields = build_log_fields(exc)
        await mark_job_failed_with_internal_error_log(
            job_id,
            attempt_token=lease.token,
            error_message=spec.finalize_error_message,
            log_event=spec.finalization_failed_log_event,
            log_fields=log_fields,
        )
        raise

    if finalized:
        logger_instance.info(spec.succeeded_log_event, job_id=str(job_id))
