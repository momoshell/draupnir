"""Terminal job state helpers that combine persistence callbacks with worker logs."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID

from app.core.errors import ErrorCode


async def mark_job_cancelled_with_log(
    job_id: UUID,
    *,
    attempt_token: UUID,
    log_event: str,
    mark_job_cancelled: Callable[..., Awaitable[bool]],
    logger_instance: Any,
    log_fields: dict[str, Any] | None = None,
) -> None:
    """Persist a cancelled terminal state and emit the matching worker log."""
    await mark_job_cancelled(job_id, attempt_token=attempt_token)
    logger_instance.info(log_event, job_id=str(job_id), **(log_fields or {}))


async def mark_job_failed_for_revision_conflict(
    job_id: UUID,
    *,
    attempt_token: UUID,
    log_event: str,
    exc: Any,
    mark_job_failed: Callable[..., Awaitable[bool]],
    logger_instance: Any,
) -> None:
    """Persist and log the shared revision-conflict failure contract."""
    await mark_job_failed(
        job_id,
        error_message=exc.message,
        error_code=ErrorCode.REVISION_CONFLICT,
        attempt_token=attempt_token,
        error_details=exc.details,
    )
    logger_instance.warning(
        log_event,
        job_id=str(job_id),
        error_code=ErrorCode.REVISION_CONFLICT.value,
        **exc.details,
    )


async def mark_job_failed_with_internal_error_log(
    job_id: UUID,
    *,
    attempt_token: UUID,
    error_message: str,
    log_event: str,
    mark_job_failed: Callable[..., Awaitable[bool]],
    logger_instance: Any,
    log_fields: dict[str, Any] | None = None,
) -> None:
    """Persist and log unexpected internal worker failures."""
    await mark_job_failed(
        job_id,
        error_message=error_message,
        attempt_token=attempt_token,
    )
    logger_instance.error(
        log_event,
        job_id=str(job_id),
        **(
            log_fields
            if log_fields is not None
            else {
                "error_code": ErrorCode.INTERNAL_ERROR.value,
                "error_message": error_message,
            }
        ),
        exc_info=True,
    )


async def mark_job_failed_if_recovery_safe(
    job_id: UUID,
    *,
    error_message: str,
    session_maker_factory: Callable[[], Any],
    lock_job_source_for_terminal_mutation: Callable[..., Awaitable[Any]],
    cancel_job_for_inactive_source: Callable[..., Awaitable[bool]],
    job_is_safe_recovery_failure_target: Callable[[Any], bool],
    persist_job_failed: Callable[..., Awaitable[None]],
    logger_instance: Any,
    error_code: ErrorCode = ErrorCode.INTERNAL_ERROR,
    error_details: dict[str, Any] | None = None,
) -> bool:
    """Fail a recovered job only if it is still pending and unowned."""
    session_maker = session_maker_factory()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        locked_source = await lock_job_source_for_terminal_mutation(session, job_id)
        job = locked_source.job

        if (
            locked_source.project.deleted_at is not None
            or locked_source.source_file is None
            or locked_source.source_file.deleted_at is not None
        ):
            await cancel_job_for_inactive_source(
                session,
                job,
                reason="source_deleted",
            )
            logger_instance.info(
                "job_recovery_enqueue_failure_mark_skipped_inactive_source",
                job_id=str(job_id),
                job_type=job.job_type,
                status=job.status,
            )
            return False

        if not job_is_safe_recovery_failure_target(job):
            logger_instance.info(
                "job_recovery_enqueue_failure_mark_skipped_changed_state",
                job_id=str(job_id),
                job_type=job.job_type,
                status=job.status,
            )
            return False

        await persist_job_failed(
            session,
            job,
            error_message=error_message,
            error_code=error_code,
            error_details=error_details,
        )
        await session.commit()

    return True


async def mark_recovery_enqueue_failed(
    job_id: UUID,
    *,
    job_type: str,
    get_enqueue_job_error_message: Callable[[str], str],
    mark_job_failed_if_recovery_safe_func: Callable[..., Awaitable[bool]],
    logger_instance: Any,
) -> bool:
    """Persist and log a sanitized worker-recovery enqueue failure."""
    marked_failed = await mark_job_failed_if_recovery_safe_func(
        job_id,
        error_message=get_enqueue_job_error_message(job_type),
    )
    if not marked_failed:
        return False

    logger_instance.error(
        "job_recovery_enqueue_failed",
        job_id=str(job_id),
        job_type=job_type,
        error_code=ErrorCode.INTERNAL_ERROR.value,
        recovery_action="mark_failed",
    )
    return True
