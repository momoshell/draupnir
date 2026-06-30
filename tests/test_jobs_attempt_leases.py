"""Unit tests for worker attempt-lease helpers."""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest

from app.jobs import attempt_leases
from app.models.job import Job


class _StaleAttemptError(Exception):
    pass


def _job(**overrides: Any) -> Job:
    row = {
        "status": "pending",
        "attempt_token": None,
        "attempt_lease_expires_at": None,
        "enqueue_status": "pending",
        "enqueue_owner_token": None,
        "enqueue_lease_expires_at": None,
    }
    row.update(overrides)
    return cast(Job, SimpleNamespace(**row))


def test_job_is_safe_recovery_failure_target_requires_unclaimed_pending_job() -> None:
    assert (
        attempt_leases.job_is_safe_recovery_failure_target(
            _job(),
            enqueue_pending_status="pending",
        )
        is True
    )
    assert (
        attempt_leases.job_is_safe_recovery_failure_target(
            _job(status="running"),
            enqueue_pending_status="pending",
        )
        is False
    )
    assert (
        attempt_leases.job_is_safe_recovery_failure_target(
            _job(enqueue_owner_token=uuid.uuid4()),
            enqueue_pending_status="pending",
        )
        is False
    )


async def test_renew_job_attempt_lease_until_cancelled_raises_when_lease_is_lost() -> None:
    job_id = uuid.uuid4()
    attempt_token = uuid.uuid4()
    renewals: list[tuple[uuid.UUID, uuid.UUID, timedelta]] = []

    async def renew_job_attempt_lease(
        job_id: uuid.UUID,
        *,
        attempt_token: uuid.UUID,
        stale_after: timedelta,
    ) -> None:
        renewals.append((job_id, attempt_token, stale_after))

    stale_after = timedelta(seconds=10)
    with pytest.raises(_StaleAttemptError, match=str(job_id)):
        await attempt_leases.renew_job_attempt_lease_until_cancelled(
            job_id,
            attempt_token=attempt_token,
            renew_job_attempt_lease=renew_job_attempt_lease,
            stale_job_attempt_error_type=_StaleAttemptError,
            stale_after=stale_after,
            interval=timedelta(seconds=0),
        )

    assert renewals == [(job_id, attempt_token, stale_after)]


async def test_with_job_attempt_lease_renewal_returns_work_result_and_cancels_renewal() -> None:
    job_id = uuid.uuid4()
    token = uuid.uuid4()
    renewal_started = asyncio.Event()
    renewal_cancelled = asyncio.Event()

    async def renew_job_attempt_lease_until_cancelled(
        job_id_arg: uuid.UUID,
        *,
        attempt_token: uuid.UUID,
    ) -> None:
        assert job_id_arg == job_id
        assert attempt_token == token
        renewal_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            renewal_cancelled.set()

    async def work() -> str:
        await renewal_started.wait()
        return "done"

    result = await attempt_leases.with_job_attempt_lease_renewal(
        work(),
        job_id=job_id,
        attempt_token=token,
        renew_job_attempt_lease_until_cancelled=renew_job_attempt_lease_until_cancelled,
    )

    assert result == "done"
    assert renewal_cancelled.is_set()


async def test_with_job_attempt_lease_renewal_cancels_work_when_renewal_fails() -> None:
    job_id = uuid.uuid4()
    attempt_token = uuid.uuid4()
    work_cancelled = asyncio.Event()

    async def renew_job_attempt_lease_until_cancelled(
        _job_id: uuid.UUID,
        *,
        attempt_token: uuid.UUID,
    ) -> None:
        _ = attempt_token
        raise _StaleAttemptError("lease lost")

    async def work() -> None:
        try:
            await asyncio.Event().wait()
        finally:
            work_cancelled.set()

    with pytest.raises(_StaleAttemptError, match="lease lost"):
        await attempt_leases.with_job_attempt_lease_renewal(
            work(),
            job_id=job_id,
            attempt_token=attempt_token,
            renew_job_attempt_lease_until_cancelled=renew_job_attempt_lease_until_cancelled,
        )

    assert work_cancelled.is_set()
