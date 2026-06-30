"""Unit tests for durable job enqueue helpers."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from app.jobs import enqueueing
from app.models.job import JobType


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
