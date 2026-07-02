"""Persisted job-attempt lease adapters and the #850 heartbeat renewal loop.

Thin worker-facing wrappers over ``app.jobs.lifecycle`` attempt-lease primitives,
plus the background renewal loop that keeps a long-running job attempt from
being reclaimed as stale (the #850 heartbeat). Calls the lifecycle primitives
through the ``job_lifecycle`` module attribute (not a captured default) so
``app.jobs.worker.job_lifecycle.*`` monkeypatches still take effect after this
extraction.
"""

# ruff: noqa: SLF001

import asyncio
from collections.abc import Callable, Coroutine
from contextlib import suppress
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from app.db.session import get_session_maker
from app.jobs import lifecycle as job_lifecycle
from app.jobs.recovery import _RUNNING_JOB_STALE_AFTER
from app.models.job import Job

_StaleJobAttemptError = job_lifecycle._StaleJobAttemptError
_JobAttemptLease = job_lifecycle._JobAttemptLease

# #850 heartbeat cadence: renew a claimed attempt lease at 1/3 of its staleness
# window so at least two renewals land before the lease could be reclaimed.
_JOB_ATTEMPT_LEASE_RENEW_INTERVAL = _RUNNING_JOB_STALE_AFTER / 3


def _claim_job_attempt_lease(
    job: Job,
    *,
    now: datetime,
    increment_attempt: bool,
    stale_after: timedelta = _RUNNING_JOB_STALE_AFTER,
) -> _JobAttemptLease:
    """Mint and persist a fresh job-attempt ownership lease."""
    return job_lifecycle._claim_job_attempt_lease(
        job,
        now=now,
        increment_attempt=increment_attempt,
        stale_after=stale_after,
    )


def _is_stale_running_job(
    job: Job,
    *,
    now: datetime,
    stale_after: timedelta = _RUNNING_JOB_STALE_AFTER,
) -> bool:
    """Return whether a running job is old enough to treat as orphaned."""
    return job_lifecycle._is_stale_running_job(
        job,
        now=now,
        stale_after=stale_after,
    )


async def _renew_job_attempt_lease(
    job_id: UUID,
    *,
    attempt_token: UUID,
    stale_after: timedelta = _RUNNING_JOB_STALE_AFTER,
) -> _JobAttemptLease | None:
    """Extend a persisted job-attempt lease if this worker still owns it."""
    return await job_lifecycle._renew_job_attempt_lease_for_update(
        job_id,
        attempt_token=attempt_token,
        stale_after=stale_after,
        session_maker_factory=get_session_maker,
    )


async def _renew_job_attempt_lease_until_cancelled(
    job_id: UUID,
    *,
    attempt_token: UUID,
    stale_after: timedelta = _RUNNING_JOB_STALE_AFTER,
    interval: timedelta = _JOB_ATTEMPT_LEASE_RENEW_INTERVAL,
) -> None:
    """Keep an active job attempt from being reclaimed while execution is still alive."""
    while True:
        await asyncio.sleep(interval.total_seconds())
        lease = await _renew_job_attempt_lease(
            job_id,
            attempt_token=attempt_token,
            stale_after=stale_after,
        )
        if lease is None:
            raise _StaleJobAttemptError(f"Job attempt for '{job_id}' no longer owns the lease")


async def _with_job_attempt_lease_renewal[ResultT](
    work: Coroutine[Any, Any, ResultT],
    *,
    job_id: UUID,
    attempt_token: UUID,
    renew_until_cancelled_func: Callable[..., Coroutine[Any, Any, None]] = (
        _renew_job_attempt_lease_until_cancelled
    ),
) -> ResultT:
    """Run job work while a companion task renews the persisted attempt lease."""
    work_task = asyncio.create_task(work)
    renewal_task = asyncio.create_task(
        renew_until_cancelled_func(job_id, attempt_token=attempt_token)
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
