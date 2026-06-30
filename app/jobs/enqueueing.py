"""Durable job enqueue intent publishing helpers."""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from uuid import UUID

from app.models.job import Job

ENQUEUE_STATUS_PENDING = "pending"
ENQUEUE_STATUS_PUBLISHING = "publishing"
ENQUEUE_STATUS_PUBLISHED = "published"
ENQUEUE_LEASE_DURATION = timedelta(minutes=1)
# Capped exponential backoff between job attempts. The first attempt runs with no
# delay; each subsequent re-enqueue waits base * 2**(attempt-1), capped, so a job
# that keeps failing is spaced out instead of being retried as fast as the broker
# can redeliver it.
ENQUEUE_BACKOFF_BASE_SECONDS = 5.0
ENQUEUE_BACKOFF_MAX_SECONDS = 300.0


@dataclass(frozen=True, slots=True)
class EnqueueIntentLease:
    """Persisted ownership token for a claimed durable enqueue intent."""

    token: UUID
    lease_expires_at: datetime


@dataclass(frozen=True, slots=True)
class ClaimedJobEnqueueIntent:
    """Persisted enqueue claim bundled with the routed worker job type."""

    lease: EnqueueIntentLease
    job_type: str
    attempts: int


class _LeaseTokenCallback(Protocol):
    async def __call__(self, job_id: UUID, *, lease_token: UUID) -> bool: ...


class _RecoveryFailureCallback(Protocol):
    async def __call__(self, job_id: UUID, *, job_type: str) -> bool: ...


_ENQUEUE_COUNTDOWN_SECONDS: ContextVar[float] = ContextVar("enqueue_countdown_seconds", default=0.0)


def enqueue_backoff_seconds(attempts: int) -> float:
    """Return the retry delay for a job that has already made ``attempts`` tries."""
    if attempts <= 1:
        return 0.0
    delay = ENQUEUE_BACKOFF_BASE_SECONDS * (2.0 ** (attempts - 2))
    return min(delay, ENQUEUE_BACKOFF_MAX_SECONDS)


def current_enqueue_countdown() -> float | None:
    """Return the active enqueue backoff in seconds, or None when there is none."""
    countdown = _ENQUEUE_COUNTDOWN_SECONDS.get()
    return countdown if countdown > 0.0 else None


def clear_enqueue_intent_lease(job: Job) -> None:
    """Clear persisted ownership fencing for a durable enqueue intent."""
    job.enqueue_owner_token = None
    job.enqueue_lease_expires_at = None


def prepare_job_enqueue_intent(job: Job) -> None:
    """Reset a job's durable enqueue intent to the pending outbox state."""
    job.enqueue_status = ENQUEUE_STATUS_PENDING
    job.enqueue_attempts = 0
    job.enqueue_last_attempted_at = None
    job.enqueue_published_at = None
    clear_enqueue_intent_lease(job)


def is_stale_enqueue_intent(job: Job, *, now: datetime) -> bool:
    """Return whether an in-flight enqueue publish claim can be reclaimed."""
    lease_expires_at = job.enqueue_lease_expires_at
    if lease_expires_at is None:
        return True
    if lease_expires_at.tzinfo is None:
        lease_expires_at = lease_expires_at.replace(tzinfo=UTC)
    return lease_expires_at <= now


def claim_enqueue_intent_lease(
    job: Job,
    *,
    now: datetime,
    lease_duration: timedelta = ENQUEUE_LEASE_DURATION,
) -> EnqueueIntentLease:
    """Mint and persist a fresh ownership lease for broker publication."""
    token = uuid.uuid4()
    lease_expires_at = now + lease_duration
    job.enqueue_status = ENQUEUE_STATUS_PUBLISHING
    job.enqueue_attempts += 1
    job.enqueue_owner_token = token
    job.enqueue_lease_expires_at = lease_expires_at
    job.enqueue_last_attempted_at = now
    return EnqueueIntentLease(token=token, lease_expires_at=lease_expires_at)


async def publish_job_enqueue_intent(
    job_id: UUID,
    *,
    claim_job_enqueue_intent: Callable[[UUID], Awaitable[ClaimedJobEnqueueIntent | None]],
    release_job_enqueue_intent: _LeaseTokenCallback,
    mark_job_enqueue_published: _LeaseTokenCallback,
    mark_recovery_enqueue_failed: _RecoveryFailureCallback,
    get_job_enqueue_publisher: Callable[[str], Callable[[UUID], None] | None],
    logger_instance: Any,
    recovery: bool = False,
    publisher: Callable[[UUID], None] | None = None,
    suppress_exceptions: bool = False,
) -> bool:
    """Best-effort publish for a durable enqueue intent recorded in Postgres."""
    claimed_intent: ClaimedJobEnqueueIntent | None = None
    try:
        claimed_intent = await claim_job_enqueue_intent(job_id)
        if claimed_intent is None:
            return False

        publish = publisher or get_job_enqueue_publisher(claimed_intent.job_type)
        if publish is None:
            await release_job_enqueue_intent(job_id, lease_token=claimed_intent.lease.token)
            return False
        countdown_token = _ENQUEUE_COUNTDOWN_SECONDS.set(
            enqueue_backoff_seconds(claimed_intent.attempts)
        )
        try:
            publish(job_id)
        except Exception:
            _ENQUEUE_COUNTDOWN_SECONDS.reset(countdown_token)
            await release_job_enqueue_intent(job_id, lease_token=claimed_intent.lease.token)
            if recovery:
                await mark_recovery_enqueue_failed(job_id, job_type=claimed_intent.job_type)
            else:
                logger_instance.warning(
                    "job_enqueue_deferred",
                    job_id=str(job_id),
                    job_type=claimed_intent.job_type,
                    recovery_action="worker_start_recovery",
                )
            return False
        else:
            _ENQUEUE_COUNTDOWN_SECONDS.reset(countdown_token)

        published = await mark_job_enqueue_published(job_id, lease_token=claimed_intent.lease.token)
        if not published:
            # The broker publish already succeeded; the enqueue intent is at-least-once and
            # job execution is idempotent, so we still report success. But the PUBLISHED
            # transition was skipped because another owner reclaimed the lease (or the status
            # moved off PUBLISHING) between claim and mark, so surface that race.
            logger_instance.warning(
                "job_enqueue_publish_mark_skipped",
                job_id=str(job_id),
                job_type=claimed_intent.job_type,
                reason="stale_enqueue_lease",
            )
        return True
    except Exception:
        if not suppress_exceptions:
            raise

        logger_instance.warning(
            "job_enqueue_deferred",
            job_id=str(job_id),
            job_type=claimed_intent.job_type if claimed_intent is not None else None,
            recovery_action="worker_start_recovery",
        )
        return False
