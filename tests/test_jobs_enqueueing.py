"""Unit tests for durable job enqueue helpers."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest

from app.jobs import enqueueing
from app.models.job import Job, JobType


class _FakeLogger:
    def __init__(self) -> None:
        self.warnings: list[tuple[str, dict[str, Any]]] = []

    def warning(self, event: str, **fields: Any) -> None:
        self.warnings.append((event, fields))


@pytest.mark.asyncio
async def test_publish_job_enqueue_intent_sets_backoff_and_logs_skipped_mark() -> None:
    test_job_id = uuid.uuid4()
    lease_token = uuid.uuid4()
    claimed = enqueueing.ClaimedJobEnqueueIntent(
        lease=enqueueing.EnqueueIntentLease(
            token=lease_token,
            lease_expires_at=datetime.now(UTC) + timedelta(minutes=1),
        ),
        job_type=JobType.INGEST.value,
        attempts=3,
    )
    logger = _FakeLogger()
    published_job_ids: list[uuid.UUID] = []
    observed_countdowns: list[float | None] = []

    async def claim(job_id: uuid.UUID) -> enqueueing.ClaimedJobEnqueueIntent | None:
        assert job_id == test_job_id
        return claimed

    async def release(job_id: uuid.UUID, *, lease_token: uuid.UUID) -> bool:
        raise AssertionError("release should not run on successful publish")

    async def mark_published(job_id: uuid.UUID, *, lease_token: uuid.UUID) -> bool:
        assert job_id == test_job_id
        assert lease_token == claimed.lease.token
        return False

    async def mark_recovery_failed(job_id: uuid.UUID, *, job_type: str) -> bool:
        raise AssertionError("recovery failure should not run on successful publish")

    def publisher(job_id: uuid.UUID) -> None:
        published_job_ids.append(job_id)
        observed_countdowns.append(enqueueing.current_enqueue_countdown())

    result = await enqueueing.publish_job_enqueue_intent(
        test_job_id,
        claim_job_enqueue_intent=claim,
        release_job_enqueue_intent=release,
        mark_job_enqueue_published=mark_published,
        mark_recovery_enqueue_failed=mark_recovery_failed,
        get_job_enqueue_publisher=lambda _job_type: None,
        logger_instance=logger,
        publisher=publisher,
    )

    assert result is True
    assert published_job_ids == [test_job_id]
    assert observed_countdowns == [enqueueing.ENQUEUE_BACKOFF_BASE_SECONDS * 2]
    assert enqueueing.current_enqueue_countdown() is None
    assert logger.warnings == [
        (
            "job_enqueue_publish_mark_skipped",
            {
                "job_id": str(test_job_id),
                "job_type": JobType.INGEST.value,
                "reason": "stale_enqueue_lease",
            },
        )
    ]


def test_enqueue_backoff_seconds_is_capped() -> None:
    assert enqueueing.enqueue_backoff_seconds(0) == 0.0
    assert enqueueing.enqueue_backoff_seconds(1) == 0.0
    assert enqueueing.enqueue_backoff_seconds(2) == enqueueing.ENQUEUE_BACKOFF_BASE_SECONDS
    assert enqueueing.enqueue_backoff_seconds(50) == enqueueing.ENQUEUE_BACKOFF_MAX_SECONDS


def _job(**overrides: Any) -> Job:
    row = {
        "enqueue_status": enqueueing.ENQUEUE_STATUS_PENDING,
        "enqueue_attempts": 3,
        "enqueue_owner_token": uuid.uuid4(),
        "enqueue_lease_expires_at": datetime.now(UTC) + timedelta(minutes=5),
        "enqueue_last_attempted_at": datetime.now(UTC),
        "enqueue_published_at": datetime.now(UTC),
    }
    row.update(overrides)
    return cast(Job, SimpleNamespace(**row))


def test_prepare_job_enqueue_intent_resets_outbox_state() -> None:
    job = _job()

    enqueueing.prepare_job_enqueue_intent(job)

    assert job.enqueue_status == enqueueing.ENQUEUE_STATUS_PENDING
    assert job.enqueue_attempts == 0
    assert job.enqueue_owner_token is None
    assert job.enqueue_lease_expires_at is None
    assert job.enqueue_last_attempted_at is None
    assert job.enqueue_published_at is None


def test_is_stale_enqueue_intent_handles_missing_naive_and_aware_leases() -> None:
    now = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)

    assert enqueueing.is_stale_enqueue_intent(_job(enqueue_lease_expires_at=None), now=now)
    assert enqueueing.is_stale_enqueue_intent(
        _job(enqueue_lease_expires_at=now.replace(tzinfo=None)),
        now=now,
    )
    assert not enqueueing.is_stale_enqueue_intent(
        _job(enqueue_lease_expires_at=now + timedelta(seconds=1)),
        now=now,
    )


def test_claim_enqueue_intent_lease_marks_publishing_and_increments_attempts() -> None:
    now = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    job = _job(enqueue_attempts=2)

    lease = enqueueing.claim_enqueue_intent_lease(
        job,
        now=now,
        lease_duration=timedelta(seconds=30),
    )

    assert job.enqueue_status == enqueueing.ENQUEUE_STATUS_PUBLISHING
    assert job.enqueue_attempts == 3
    assert job.enqueue_owner_token == lease.token
    assert job.enqueue_lease_expires_at == now + timedelta(seconds=30)
    assert job.enqueue_last_attempted_at == now
    assert lease.lease_expires_at == now + timedelta(seconds=30)
