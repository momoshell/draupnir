"""Recovery helpers for incomplete persisted worker jobs."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from app.models.job import Job


@dataclass(frozen=True, slots=True)
class RecoveryPolicy:
    """Collaborators and status names used to recover incomplete jobs."""

    enqueue_status_pending: str
    enqueue_status_publishing: str
    is_stale_running_job: Callable[..., bool]
    is_stale_enqueue_intent: Callable[..., bool]
    clear_job_attempt_lease: Callable[[Job], None]
    clear_enqueue_intent_lease: Callable[[Job], None]
    prepare_job_enqueue_intent: Callable[[Job], None]


def recover_incomplete_job_ids(
    jobs: Iterable[Job],
    *,
    now: datetime,
    policy: RecoveryPolicy,
) -> list[UUID]:
    """Mutate recoverable incomplete jobs and return ids to publish."""
    recovered_job_ids: list[UUID] = []
    for job in jobs:
        if job.status == "running":
            if not policy.is_stale_running_job(job, now=now):
                continue
            job.status = "pending"
            job.started_at = None
            job.finished_at = None
            job.error_code = None
            job.error_message = None
            policy.clear_job_attempt_lease(job)
            policy.prepare_job_enqueue_intent(job)
            recovered_job_ids.append(job.id)
            continue

        if (
            job.enqueue_status == policy.enqueue_status_publishing
            and not policy.is_stale_enqueue_intent(job, now=now)
        ):
            continue

        if job.enqueue_status == policy.enqueue_status_publishing:
            job.enqueue_status = policy.enqueue_status_pending
            policy.clear_enqueue_intent_lease(job)

        if job.enqueue_status != policy.enqueue_status_pending:
            continue

        recovered_job_ids.append(job.id)

    return recovered_job_ids
