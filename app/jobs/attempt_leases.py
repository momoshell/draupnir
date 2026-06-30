"""Attempt-lease helpers for worker job execution."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from contextlib import suppress
from datetime import timedelta
from typing import Any
from uuid import UUID

from app.models.job import Job


def job_is_safe_recovery_failure_target(
    job: Job,
    *,
    enqueue_pending_status: str,
) -> bool:
    """Return whether recovery can still safely mark the job failed."""
    return (
        job.status == "pending"
        and job.attempt_token is None
        and job.attempt_lease_expires_at is None
        and job.enqueue_status == enqueue_pending_status
        and job.enqueue_owner_token is None
        and job.enqueue_lease_expires_at is None
    )


async def renew_job_attempt_lease_until_cancelled(
    job_id: UUID,
    *,
    attempt_token: UUID,
    renew_job_attempt_lease: Callable[..., Coroutine[Any, Any, Any]],
    stale_job_attempt_error_type: type[Exception],
    stale_after: timedelta,
    interval: timedelta,
) -> None:
    """Keep an active job attempt from being reclaimed while execution is alive."""
    while True:
        await asyncio.sleep(interval.total_seconds())
        lease = await renew_job_attempt_lease(
            job_id,
            attempt_token=attempt_token,
            stale_after=stale_after,
        )
        if lease is None:
            raise stale_job_attempt_error_type(
                f"Job attempt for '{job_id}' no longer owns the lease"
            )


async def with_job_attempt_lease_renewal[ResultT](
    work: Coroutine[Any, Any, ResultT],
    *,
    job_id: UUID,
    attempt_token: UUID,
    renew_job_attempt_lease_until_cancelled: Callable[..., Coroutine[Any, Any, None]],
) -> ResultT:
    """Run job work while a companion task renews the persisted attempt lease."""
    work_task = asyncio.create_task(work)
    renewal_task = asyncio.create_task(
        renew_job_attempt_lease_until_cancelled(job_id, attempt_token=attempt_token)
    )
    try:
        done, _pending = await asyncio.wait(
            {work_task, renewal_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if renewal_task in done:
            work_task.cancel()
            with suppress(asyncio.CancelledError):
                await work_task
            await renewal_task

        renewal_task.cancel()
        with suppress(asyncio.CancelledError):
            await renewal_task
        return await work_task
    finally:
        for task in (work_task, renewal_task):
            if not task.done():
                task.cancel()
        with suppress(asyncio.CancelledError):
            await renewal_task
