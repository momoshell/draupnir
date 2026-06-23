"""DB-backed regression tests for _enqueue_centerline_materialization dedup (be-639b).

Verifies that calling _enqueue_centerline_materialization twice for the same
revision produces exactly one CENTERLINE job row (the second call is a no-op),
and that the function never raises.
"""

from __future__ import annotations

import uuid
from typing import Any
from uuid import UUID

import pytest

import app.api.v1.revision_routes.service_takeoff as service_takeoff_route
import app.db.session as session_module
import app.jobs.worker as worker_module
from app.api.v1.revision_routes.service_takeoff import _enqueue_centerline_materialization
from app.ingestion.finalization import IngestFinalizationPayload
from app.ingestion.runner import IngestionRunRequest
from app.models.job import Job, JobType
from tests.conftest import requires_database, truncate_projects_cascade_for_cleanup
from tests.test_ingest_output_persistence import (
    _build_contract_entity,
    _replace_fake_canonical_payload,
)
from tests.test_jobs import _build_fake_ingest_payload
from tests.test_quantity_takeoff_persistence import _seed_quantity_lineage

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def fake_ingestion_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> list[IngestionRunRequest]:
    """Patch worker ingestion with deterministic persisted outputs.

    Required for _seed_quantity_lineage to succeed without a real adapter.
    Mirrors the autouse fixture in test_quantity_takeoff_persistence.py.
    """
    recorded_requests: list[IngestionRunRequest] = []

    async def _fake_run_ingestion(request: IngestionRunRequest) -> IngestFinalizationPayload:
        recorded_requests.append(request)
        entity_suffix = f"{len(recorded_requests):03d}"
        payload = _build_fake_ingest_payload(request)
        return _replace_fake_canonical_payload(
            payload,
            entities=[
                _build_contract_entity(
                    entity_id=f"entity-centerline-enqueue-{entity_suffix}",
                    entity_type="line",
                    layer_ref="A-WALL",
                    source_id=f"entity-source-centerline-enqueue-{entity_suffix}",
                )
            ],
        )

    monkeypatch.setattr(worker_module, "run_ingestion", _fake_run_ingestion)
    return recorded_requests


@pytest.fixture
async def cleanup_db() -> None:  # type: ignore[misc]
    """Truncate project data before and after each test (non-autouse; DB tests opt in)."""
    await truncate_projects_cascade_for_cleanup()
    yield
    await truncate_projects_cascade_for_cleanup()


@pytest.fixture(autouse=True)
def stub_centerline_publish(monkeypatch: pytest.MonkeyPatch) -> list[UUID]:
    """Stub the route-local publish seam so no real Celery broker is hit.

    Returns a list that accumulates every job_id passed to the publisher so
    tests can inspect what was (not) published.
    """
    published: list[UUID] = []

    async def _fake_publish(
        job_id: Any,
        *,
        publisher: Any = None,
        suppress_exceptions: bool = False,
        **_kwargs: Any,
    ) -> None:
        published.append(job_id)

    def _fake_enqueue_centerline(job_id: Any) -> None:
        pass

    monkeypatch.setattr(service_takeoff_route, "_publish_job_enqueue_intent", _fake_publish)
    monkeypatch.setattr(service_takeoff_route, "_enqueue_centerline_job", _fake_enqueue_centerline)
    return published


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _count_centerline_jobs(revision_id: UUID) -> int:
    """Return the count of CENTERLINE jobs for a revision on a fresh session."""
    from sqlalchemy import func, select

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        count = await session.scalar(
            select(func.count()).where(
                Job.base_revision_id == revision_id,
                Job.job_type == JobType.CENTERLINE.value,
            )
        )
    return count or 0


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@requires_database
async def test_enqueue_centerline_dedup_second_call_is_noop(
    async_client: Any,
    cleanup_db: Any,
) -> None:
    """Calling _enqueue_centerline_materialization twice creates exactly one job row.

    Arrange: seed a real drawing_revision with the full FK lineage.
    Act:     call _enqueue_centerline_materialization twice for the same revision.
    Assert:  exactly one CENTERLINE job row exists in the jobs table.
    """
    # Arrange
    seed = await _seed_quantity_lineage(async_client)
    revision_id = seed.drawing_revision_id
    project_id = seed.project_id
    file_id = seed.file_id

    # Act - first call should create a job
    await _enqueue_centerline_materialization(
        revision_id=revision_id,
        project_id=project_id,
        source_file_id=file_id,
    )

    # Act - second call should be a no-op (dedup on pending status)
    await _enqueue_centerline_materialization(
        revision_id=revision_id,
        project_id=project_id,
        source_file_id=file_id,
    )

    # Assert - exactly one job created, the second was deduplicated
    count = await _count_centerline_jobs(revision_id)
    assert count == 1, f"Expected exactly 1 CENTERLINE job for revision {revision_id}, got {count}"


@requires_database
async def test_enqueue_centerline_dedup_only_skips_nonterminal_statuses(
    async_client: Any,
    cleanup_db: Any,
) -> None:
    """Dedup checks only pending/running statuses; a completed job allows a new one.

    Arrange: seed lineage, create a succeeded CENTERLINE job directly.
    Act:     call _enqueue_centerline_materialization.
    Assert:  a second CENTERLINE job row is inserted (total = 2).
    """
    # Arrange
    seed = await _seed_quantity_lineage(async_client)
    revision_id = seed.drawing_revision_id
    project_id = seed.project_id
    file_id = seed.file_id

    # Insert a terminal (succeeded) CENTERLINE job directly
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        terminal_job = Job(
            id=uuid.uuid4(),
            project_id=project_id,
            file_id=file_id,
            extraction_profile_id=None,
            base_revision_id=revision_id,
            parent_job_id=None,
            job_type=JobType.CENTERLINE.value,
            status="succeeded",
            attempts=1,
            max_attempts=3,
            enqueue_status="published",
            enqueue_attempts=1,
            cancel_requested=False,
            error_code=None,
            error_message=None,
            started_at=None,
            finished_at=None,
        )
        session.add(terminal_job)
        await session.commit()

    # Act
    await _enqueue_centerline_materialization(
        revision_id=revision_id,
        project_id=project_id,
        source_file_id=file_id,
    )

    # Assert - new job was enqueued because the existing one is terminal (succeeded)
    count = await _count_centerline_jobs(revision_id)
    assert count == 2, (
        f"Expected 2 CENTERLINE jobs (1 terminal + 1 new) for revision {revision_id}, got {count}"
    )


@requires_database
async def test_enqueue_centerline_does_not_raise(
    async_client: Any,
    cleanup_db: Any,
) -> None:
    """_enqueue_centerline_materialization never propagates exceptions into the read path.

    Arrange: seed lineage so there is a real revision.
    Act:     call the function (publish seam is already stubbed).
    Assert:  no exception is raised.
    """
    seed = await _seed_quantity_lineage(async_client)

    # Should not raise regardless of what happens inside
    await _enqueue_centerline_materialization(
        revision_id=seed.drawing_revision_id,
        project_id=seed.project_id,
        source_file_id=seed.file_id,
    )


async def test_enqueue_centerline_no_session_maker_is_safe_noop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When no session_maker is available, _enqueue_centerline_materialization is a safe no-op.

    Arrange: monkeypatch AsyncSessionLocal to None so get_session_maker() returns None.
    Act:     call _enqueue_centerline_materialization with arbitrary IDs.
    Assert:  no exception is raised and the function returns normally.

    No DB required -- this is a pure unit test.
    """
    from app.db import session as session_mod

    monkeypatch.setattr(session_mod, "AsyncSessionLocal", None)

    # Should return without any error
    await _enqueue_centerline_materialization(
        revision_id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        source_file_id=uuid.uuid4(),
    )
    # Implicit assert: no exception raised
