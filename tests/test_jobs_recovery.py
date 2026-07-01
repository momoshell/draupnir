"""Unit tests for incomplete job recovery helpers."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

from app.jobs import recovery
from app.models.job import Job

NOW = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)


def _job(**overrides: Any) -> Job:
    row = {
        "id": uuid.uuid4(),
        "status": "pending",
        "started_at": NOW,
        "finished_at": NOW,
        "error_code": "internal_error",
        "error_message": "failed",
        "attempt_token": uuid.uuid4(),
        "attempt_lease_expires_at": NOW,
        "enqueue_status": "pending",
        "enqueue_owner_token": uuid.uuid4(),
        "enqueue_lease_expires_at": NOW,
        "enqueue_attempts": 1,
        "enqueue_last_attempted_at": NOW,
        "enqueue_published_at": NOW,
    }
    row.update(overrides)
    return cast(Job, SimpleNamespace(**row))


class _PolicySpy:
    def __init__(self, *, stale_running: bool = True, stale_enqueue: bool = True) -> None:
        self.stale_running = stale_running
        self.stale_enqueue = stale_enqueue
        self.cleared_attempt_leases: list[uuid.UUID] = []
        self.cleared_enqueue_leases: list[uuid.UUID] = []
        self.prepared_enqueue_intents: list[uuid.UUID] = []

    def policy(self) -> recovery.RecoveryPolicy:
        return recovery.RecoveryPolicy(
            enqueue_status_pending="pending",
            enqueue_status_publishing="publishing",
            is_stale_running_job=self.is_stale_running_job,
            is_stale_enqueue_intent=self.is_stale_enqueue_intent,
            clear_job_attempt_lease=self.clear_job_attempt_lease,
            clear_enqueue_intent_lease=self.clear_enqueue_intent_lease,
            prepare_job_enqueue_intent=self.prepare_job_enqueue_intent,
        )

    def is_stale_running_job(self, job: Job, *, now: datetime) -> bool:
        assert now == NOW
        _ = job
        return self.stale_running

    def is_stale_enqueue_intent(self, job: Job, *, now: datetime) -> bool:
        assert now == NOW
        _ = job
        return self.stale_enqueue

    def clear_job_attempt_lease(self, job: Job) -> None:
        self.cleared_attempt_leases.append(job.id)
        job.attempt_token = None
        job.attempt_lease_expires_at = None

    def clear_enqueue_intent_lease(self, job: Job) -> None:
        self.cleared_enqueue_leases.append(job.id)
        job.enqueue_owner_token = None
        job.enqueue_lease_expires_at = None

    def prepare_job_enqueue_intent(self, job: Job) -> None:
        self.prepared_enqueue_intents.append(job.id)
        job.enqueue_status = "pending"
        job.enqueue_attempts = 0
        job.enqueue_last_attempted_at = None
        job.enqueue_published_at = None
        self.clear_enqueue_intent_lease(job)


def test_recover_incomplete_job_ids_resets_stale_running_job() -> None:
    job = _job(status="running")
    spy = _PolicySpy(stale_running=True)

    recovered = recovery.recover_incomplete_job_ids([job], now=NOW, policy=spy.policy())

    assert recovered == [job.id]
    assert job.status == "pending"
    assert job.started_at is None
    assert job.finished_at is None
    assert job.error_code is None
    assert job.error_message is None
    assert spy.cleared_attempt_leases == [job.id]
    assert spy.prepared_enqueue_intents == [job.id]


def test_recover_incomplete_job_ids_skips_fresh_running_job() -> None:
    job = _job(status="running")
    spy = _PolicySpy(stale_running=False)

    recovered = recovery.recover_incomplete_job_ids([job], now=NOW, policy=spy.policy())

    assert recovered == []
    assert job.status == "running"
    assert spy.cleared_attempt_leases == []
    assert spy.prepared_enqueue_intents == []


def test_recover_incomplete_job_ids_reclaims_stale_enqueue_publish_claim() -> None:
    job = _job(status="pending", enqueue_status="publishing")
    spy = _PolicySpy(stale_enqueue=True)

    recovered = recovery.recover_incomplete_job_ids([job], now=NOW, policy=spy.policy())

    assert recovered == [job.id]
    assert job.enqueue_status == "pending"
    assert spy.cleared_enqueue_leases == [job.id]


def test_recover_incomplete_job_ids_skips_fresh_enqueue_publish_claim() -> None:
    job = _job(status="pending", enqueue_status="publishing")
    spy = _PolicySpy(stale_enqueue=False)

    recovered = recovery.recover_incomplete_job_ids([job], now=NOW, policy=spy.policy())

    assert recovered == []
    assert job.enqueue_status == "publishing"
    assert spy.cleared_enqueue_leases == []


def test_recover_incomplete_job_ids_skips_non_pending_enqueue_status() -> None:
    job = _job(status="pending", enqueue_status="published")
    spy = _PolicySpy()

    recovered = recovery.recover_incomplete_job_ids([job], now=NOW, policy=spy.policy())

    assert recovered == []
