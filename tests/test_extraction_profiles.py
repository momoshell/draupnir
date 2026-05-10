"""Integration tests for immutable extraction profiles and reprocessing."""

import importlib.util
import math
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from pydantic import ValidationError
from sqlalchemy import select, text

import app.api.v1.files as files_api
import app.db.session as session_module
from app.models.extraction_profile import ExtractionProfile
from app.models.job import Job
from app.schemas.extraction_profile import ExtractionProfileCreate
from app.schemas.job import JobRead
from tests.conftest import requires_database


async def _create_project(async_client: httpx.AsyncClient) -> dict[str, Any]:
    """Create a project and return its payload."""
    response = await async_client.post(
        "/v1/projects",
        json={
            "name": "Extraction Profile Test Project",
            "description": "A project for extraction profile tests",
        },
    )
    assert response.status_code == 201
    return cast(dict[str, Any], response.json())


async def _upload_file(
    async_client: httpx.AsyncClient,
    project_id: str,
) -> dict[str, Any]:
    """Upload a supported file and return its payload."""
    response = await async_client.post(
        f"/v1/projects/{project_id}/files",
        files={"file": ("plan.pdf", b"%PDF-1.7\nprofile-test\n", "application/pdf")},
    )
    assert response.status_code == 201
    return cast(dict[str, Any], response.json())


async def _get_jobs_for_file(file_id: str) -> list[Job]:
    """Load all jobs for a file ordered by creation time."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        result = await session.execute(
            select(Job)
            .where(Job.file_id == uuid.UUID(file_id))
            .order_by(Job.created_at.asc(), Job.id.asc())
        )
        return list(result.scalars())


async def _get_extraction_profile(profile_id: uuid.UUID) -> ExtractionProfile:
    """Load a profile by id."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        profile = await session.get(ExtractionProfile, profile_id)

    assert profile is not None
    return profile


@pytest.fixture
def stub_enqueue_ingest_job(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub enqueue publish so tests do not require RabbitMQ."""

    def _fake_enqueue(job_id: uuid.UUID) -> None:
        _ = job_id

    monkeypatch.setattr(files_api, "enqueue_ingest_job", _fake_enqueue)


@requires_database
class TestExtractionProfiles:
    """Tests for immutable extraction profile persistence and reprocessing."""

    async def test_upload_creates_default_v0_1_profile(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        stub_enqueue_ingest_job: None,
    ) -> None:
        """Uploading should create a default immutable extraction profile."""
        _ = self
        _ = cleanup_projects
        _ = stub_enqueue_ingest_job

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        jobs = await _get_jobs_for_file(str(uploaded["id"]))

        assert len(jobs) == 1
        assert jobs[0].extraction_profile_id is not None
        assert uploaded["initial_job_id"] == str(jobs[0].id)
        assert uploaded["initial_extraction_profile_id"] == str(jobs[0].extraction_profile_id)

        profile = await _get_extraction_profile(jobs[0].extraction_profile_id)
        assert profile.project_id == uuid.UUID(project["id"])
        assert profile.profile_version == "v0.1"
        assert profile.units_override is None
        assert profile.layout_mode == "auto"
        assert profile.xref_handling == "preserve"
        assert profile.block_handling == "expand"
        assert profile.text_extraction is True
        assert profile.dimension_extraction is True
        assert profile.pdf_page_range is None
        assert profile.raster_calibration is None
        assert profile.confidence_threshold == pytest.approx(0.6)

    async def test_reprocess_reuses_existing_profile_by_id(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        stub_enqueue_ingest_job: None,
    ) -> None:
        """Reprocessing should allow creating a new job from an existing profile id."""
        _ = self
        _ = cleanup_projects
        _ = stub_enqueue_ingest_job

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        original_jobs = await _get_jobs_for_file(str(uploaded["id"]))
        original_job = original_jobs[0]

        response = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={"extraction_profile_id": str(original_job.extraction_profile_id)},
        )

        assert response.status_code == 202
        payload = response.json()
        assert payload["file_id"] == uploaded["id"]
        assert payload["project_id"] == project["id"]
        assert payload["job_type"] == "ingest"
        assert payload["status"] == "pending"
        assert payload["extraction_profile_id"] == str(original_job.extraction_profile_id)

        jobs = await _get_jobs_for_file(str(uploaded["id"]))
        assert len(jobs) == 2
        assert jobs[0].id == original_job.id
        assert jobs[0].extraction_profile_id == original_job.extraction_profile_id
        assert jobs[1].id != original_job.id
        assert jobs[1].extraction_profile_id == original_job.extraction_profile_id

    async def test_reprocess_creates_new_profile_from_payload(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        stub_enqueue_ingest_job: None,
    ) -> None:
        """Reprocessing should allow creating a new immutable profile from request payload."""
        _ = self
        _ = cleanup_projects
        _ = stub_enqueue_ingest_job

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        original_jobs = await _get_jobs_for_file(str(uploaded["id"]))
        original_job = original_jobs[0]

        response = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={
                "extraction_profile": {
                    "profile_version": "v0.1",
                    "units_override": "imperial",
                    "layout_mode": "paper_space",
                    "xref_handling": "detach",
                    "block_handling": "preserve",
                    "text_extraction": False,
                    "dimension_extraction": True,
                    "pdf_page_range": "2-4",
                    "raster_calibration": {"scale": 48, "unit": "inch"},
                    "confidence_threshold": 0.95,
                }
            },
        )

        assert response.status_code == 202
        payload = response.json()
        assert payload["extraction_profile_id"] != str(original_job.extraction_profile_id)

        jobs = await _get_jobs_for_file(str(uploaded["id"]))
        assert len(jobs) == 2
        assert jobs[0].extraction_profile_id == original_job.extraction_profile_id
        assert jobs[1].extraction_profile_id != original_job.extraction_profile_id

        assert jobs[1].extraction_profile_id is not None
        profile = await _get_extraction_profile(jobs[1].extraction_profile_id)
        assert profile.project_id == uuid.UUID(project["id"])
        assert profile.profile_version == "v0.1"
        assert profile.units_override == "imperial"
        assert profile.layout_mode == "paper_space"
        assert profile.xref_handling == "detach"
        assert profile.block_handling == "preserve"
        assert profile.text_extraction is False
        assert profile.dimension_extraction is True
        assert profile.pdf_page_range == "2-4"
        assert profile.raster_calibration == {"scale": 48, "unit": "inch"}
        assert profile.confidence_threshold == pytest.approx(0.95)

    async def test_reprocess_rejects_profile_from_another_project(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        stub_enqueue_ingest_job: None,
    ) -> None:
        """Reprocessing should enforce project ownership for existing profiles."""
        _ = self
        _ = cleanup_projects
        _ = stub_enqueue_ingest_job

        first_project = await _create_project(async_client)
        second_project = await _create_project(async_client)

        first_file = await _upload_file(async_client, first_project["id"])
        second_file = await _upload_file(async_client, second_project["id"])

        foreign_jobs = await _get_jobs_for_file(str(second_file["id"]))
        foreign_job = foreign_jobs[0]
        response = await async_client.post(
            f"/v1/projects/{first_project['id']}/files/{first_file['id']}/reprocess",
            json={"extraction_profile_id": str(foreign_job.extraction_profile_id)},
        )

        assert response.status_code == 404
        assert response.json() == {
            "error": {
                "code": "NOT_FOUND",
                "message": (
                    "ExtractionProfile with identifier "
                    f"'{foreign_job.extraction_profile_id}' not found"
                ),
                "details": None,
            }
        }

    async def test_reprocess_rejects_unknown_extraction_profile_fields(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        stub_enqueue_ingest_job: None,
    ) -> None:
        """Reprocessing should reject unexpected extraction profile keys."""
        _ = self
        _ = cleanup_projects
        _ = stub_enqueue_ingest_job

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])

        response = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={
                "extraction_profile": {
                    "unexpected": True,
                }
            },
        )

        assert response.status_code == 422
        assert "unexpected" in response.text
        assert "extra_forbidden" in response.text

    async def test_reprocess_rejects_unknown_top_level_fields(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        stub_enqueue_ingest_job: None,
    ) -> None:
        """Reprocessing should reject unexpected top-level request keys."""
        _ = self
        _ = cleanup_projects
        _ = stub_enqueue_ingest_job

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])

        response = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={
                "extraction_profile_id": uploaded["initial_extraction_profile_id"],
                "unexpected": True,
            },
        )

        assert response.status_code == 422
        assert "unexpected" in response.text
        assert "extra_forbidden" in response.text

    async def test_reprocess_rejects_invalid_extraction_profile_enum_values(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        stub_enqueue_ingest_job: None,
    ) -> None:
        """Reprocessing should reject unsupported profile enum values."""
        _ = self
        _ = cleanup_projects
        _ = stub_enqueue_ingest_job

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])

        response = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={
                "extraction_profile": {
                    "units_override": "yards",
                }
            },
        )

        assert response.status_code == 422
        assert "units_override" in response.text
        assert "yards" in response.text
        assert "literal_error" in response.text

    async def test_reprocess_enqueue_failure_returns_durable_failed_job_details(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        stub_enqueue_ingest_job: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Reprocess enqueue failures should expose safe durable identifiers."""
        _ = self
        _ = cleanup_projects
        _ = stub_enqueue_ingest_job

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])

        def _failing_enqueue(_: uuid.UUID) -> None:
            raise RuntimeError("broker exploded: amqp://user:secret@mq.internal/vhost")

        monkeypatch.setattr(files_api, "enqueue_ingest_job", _failing_enqueue)

        response = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={
                "extraction_profile": {
                    "profile_version": "v0.1",
                    "units_override": "metric",
                    "layout_mode": "paper_space",
                    "xref_handling": "detach",
                    "block_handling": "preserve",
                    "text_extraction": True,
                    "dimension_extraction": False,
                    "pdf_page_range": "1",
                    "raster_calibration": None,
                    "confidence_threshold": 0.75,
                }
            },
        )

        assert response.status_code == 500
        payload = response.json()
        assert payload["error"]["code"] == "INTERNAL_ERROR"
        assert payload["error"]["message"] == "Failed to enqueue ingest job"
        assert payload["error"]["details"] is not None
        assert "broker exploded" not in response.text
        assert "amqp://user:secret@mq.internal/vhost" not in response.text

        details = cast(dict[str, str], payload["error"]["details"])
        jobs = await _get_jobs_for_file(str(uploaded["id"]))
        assert len(jobs) == 2

        failed_job = jobs[1]
        assert details == {
            "file_id": str(failed_job.file_id),
            "job_id": str(failed_job.id),
            "extraction_profile_id": str(failed_job.extraction_profile_id),
            "status": "failed",
        }
        assert failed_job.status == "failed"
        assert failed_job.error_code == "INTERNAL_ERROR"
        assert failed_job.error_message == "Failed to enqueue ingest job"
        assert "broker exploded" not in failed_job.error_message
        assert failed_job.extraction_profile_id is not None

        failed_profile = await _get_extraction_profile(failed_job.extraction_profile_id)
        assert str(failed_profile.id) == details["extraction_profile_id"]
        assert failed_profile.project_id == uuid.UUID(project["id"])
        assert failed_profile.units_override == "metric"
        assert failed_profile.layout_mode == "paper_space"

    async def test_job_constraints_migration_upgrade_backfills_and_validates_profiles(
        self,
    ) -> None:
        """Upgrade should backfill required profiles and validate all new job constraints."""
        _ = self

        engine = session_module.get_engine()
        assert engine is not None

        schema_name = f"test_job_constraints_{uuid.uuid4().hex}"
        migration = _load_job_constraints_migration()
        project_id = uuid.uuid4()
        file_id = uuid.uuid4()
        profile_id = uuid.uuid4()
        ingest_job_id = uuid.uuid4()
        reprocess_job_id = uuid.uuid4()

        try:
            async with engine.begin() as conn:
                await conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))

            async with engine.begin() as conn:
                await conn.execute(text(f'SET search_path TO "{schema_name}"'))
                await conn.run_sync(_install_pre_0007_jobs_schema)

                await conn.execute(
                    text("INSERT INTO projects (id) VALUES (:id)"),
                    {"id": project_id},
                )
                await conn.execute(
                    text(
                        """
                        INSERT INTO files (id, project_id, initial_extraction_profile_id)
                        VALUES (:id, :project_id, :initial_extraction_profile_id)
                        """
                    ),
                    {
                        "id": file_id,
                        "project_id": project_id,
                        "initial_extraction_profile_id": profile_id,
                    },
                )
                await conn.execute(
                    text(
                        """
                        INSERT INTO jobs (
                            id,
                            project_id,
                            file_id,
                            extraction_profile_id,
                            job_type,
                            status,
                            attempts,
                            max_attempts,
                            cancel_requested,
                            created_at
                        ) VALUES (
                            :id,
                            :project_id,
                            :file_id,
                            :extraction_profile_id,
                            :job_type,
                            :status,
                            0,
                            3,
                            false,
                            :created_at
                        )
                        """
                    ),
                    [
                        {
                            "id": ingest_job_id,
                            "project_id": project_id,
                            "file_id": file_id,
                            "extraction_profile_id": None,
                            "job_type": "ingest",
                            "status": "pending",
                            "created_at": datetime(2026, 5, 10, tzinfo=UTC),
                        },
                        {
                            "id": reprocess_job_id,
                            "project_id": project_id,
                            "file_id": file_id,
                            "extraction_profile_id": profile_id,
                            "job_type": "reprocess",
                            "status": "running",
                            "created_at": datetime(2026, 5, 11, tzinfo=UTC),
                        },
                    ],
                )

                await conn.run_sync(
                    lambda sync_conn: _run_migration_upgrade(sync_conn, migration)
                )

                jobs = (
                    (
                        await conn.execute(
                            text(
                                """
                                SELECT id, job_type, extraction_profile_id
                                FROM jobs
                                ORDER BY created_at ASC, id ASC
                                """
                            )
                        )
                    )
                    .mappings()
                    .all()
                )
                assert [dict(row) for row in jobs] == [
                    {
                        "id": ingest_job_id,
                        "job_type": "ingest",
                        "extraction_profile_id": profile_id,
                    },
                    {
                        "id": reprocess_job_id,
                        "job_type": "reprocess",
                        "extraction_profile_id": profile_id,
                    },
                ]

                constraints = await _get_job_check_constraints(conn, schema_name)
                assert constraints == {
                    "ck_jobs_error_code_valid": True,
                    "ck_jobs_ingest_extraction_profile_required": True,
                    "ck_jobs_job_type_valid": True,
                    "ck_jobs_status_valid": True,
                }
        finally:
            async with engine.begin() as conn:
                await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))

    async def test_job_constraints_migration_upgrade_fails_when_backfill_leaves_null_profiles(
        self,
    ) -> None:
        """Upgrade should fail loudly when legacy required-profile jobs cannot be backfilled."""
        _ = self

        engine = session_module.get_engine()
        assert engine is not None

        schema_name = f"test_job_constraints_fail_{uuid.uuid4().hex}"
        migration = _load_job_constraints_migration()
        project_id = uuid.uuid4()
        file_id = uuid.uuid4()
        ingest_job_id = uuid.uuid4()

        try:
            async with engine.begin() as conn:
                await conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))

            with pytest.raises(
                RuntimeError,
                match="still have NULL extraction_profile_id after backfill",
            ):
                async with engine.begin() as conn:
                    await conn.execute(text(f'SET search_path TO "{schema_name}"'))
                    await conn.run_sync(_install_pre_0007_jobs_schema)

                    await conn.execute(
                        text("INSERT INTO projects (id) VALUES (:id)"),
                        {"id": project_id},
                    )
                    await conn.execute(
                        text(
                            """
                            INSERT INTO files (id, project_id, initial_extraction_profile_id)
                            VALUES (:id, :project_id, NULL)
                            """
                        ),
                        {"id": file_id, "project_id": project_id},
                    )
                    await conn.execute(
                        text(
                            """
                            INSERT INTO jobs (
                                id,
                                project_id,
                                file_id,
                                extraction_profile_id,
                                job_type,
                                status,
                                attempts,
                                max_attempts,
                                cancel_requested,
                                created_at
                            ) VALUES (
                                :id,
                                :project_id,
                                :file_id,
                                NULL,
                                'ingest',
                                'pending',
                                0,
                                3,
                                false,
                                :created_at
                            )
                            """
                        ),
                        {
                            "id": ingest_job_id,
                            "project_id": project_id,
                            "file_id": file_id,
                            "created_at": datetime(2026, 5, 12, tzinfo=UTC),
                        },
                    )

                    await conn.run_sync(
                        lambda sync_conn: _run_migration_upgrade(
                            sync_conn,
                            migration,
                        )
                    )
        finally:
            async with engine.begin() as conn:
                await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))

    async def test_job_constraints_migration_downgrade_drops_constraints(
        self,
    ) -> None:
        """Downgrade should remove the 0007 job check constraints."""
        _ = self

        engine = session_module.get_engine()
        assert engine is not None

        schema_name = f"test_job_constraints_down_{uuid.uuid4().hex}"
        migration = _load_job_constraints_migration()
        project_id = uuid.uuid4()
        file_id = uuid.uuid4()
        profile_id = uuid.uuid4()
        ingest_job_id = uuid.uuid4()

        try:
            async with engine.begin() as conn:
                await conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))

            async with engine.begin() as conn:
                await conn.execute(text(f'SET search_path TO "{schema_name}"'))
                await conn.run_sync(_install_pre_0007_jobs_schema)

                await conn.execute(
                    text("INSERT INTO projects (id) VALUES (:id)"),
                    {"id": project_id},
                )
                await conn.execute(
                    text(
                        """
                        INSERT INTO files (id, project_id, initial_extraction_profile_id)
                        VALUES (:id, :project_id, :initial_extraction_profile_id)
                        """
                    ),
                    {
                        "id": file_id,
                        "project_id": project_id,
                        "initial_extraction_profile_id": profile_id,
                    },
                )
                await conn.execute(
                    text(
                        """
                        INSERT INTO jobs (
                            id,
                            project_id,
                            file_id,
                            extraction_profile_id,
                            job_type,
                            status,
                            attempts,
                            max_attempts,
                            cancel_requested,
                            created_at
                        ) VALUES (
                            :id,
                            :project_id,
                            :file_id,
                            NULL,
                            'ingest',
                            'pending',
                            0,
                            3,
                            false,
                            :created_at
                        )
                        """
                    ),
                    {
                        "id": ingest_job_id,
                        "project_id": project_id,
                        "file_id": file_id,
                        "created_at": datetime(2026, 5, 13, tzinfo=UTC),
                    },
                )

                await conn.run_sync(
                    lambda sync_conn: _run_migration_upgrade(sync_conn, migration)
                )
                assert await _get_job_check_constraints(conn, schema_name) == {
                    "ck_jobs_error_code_valid": True,
                    "ck_jobs_ingest_extraction_profile_required": True,
                    "ck_jobs_job_type_valid": True,
                    "ck_jobs_status_valid": True,
                }

                await conn.run_sync(
                    lambda sync_conn: _run_migration_downgrade(
                        sync_conn,
                        migration,
                    )
                )
                assert await _get_job_check_constraints(conn, schema_name) == {}
        finally:
            async with engine.begin() as conn:
                await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))

    async def test_migration_upgrade_backfills_profiles_and_initial_lineage(
        self,
    ) -> None:
        """Upgrade should backfill profiles now while leaving NOT NULL for a later contract step."""
        _ = self

        engine = session_module.get_engine()
        assert engine is not None

        schema_name = f"test_extract_profiles_{uuid.uuid4().hex}"
        migration = _load_extraction_profiles_migration()
        project_id = uuid.uuid4()
        file_id = uuid.uuid4()
        first_ingest_job_id = uuid.uuid4()
        second_ingest_job_id = uuid.uuid4()
        export_job_id = uuid.uuid4()

        try:
            async with engine.begin() as conn:
                await conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))

            async with engine.begin() as conn:
                await conn.execute(text(f'SET search_path TO "{schema_name}"'))
                await conn.run_sync(_install_pre_0004_schema)

                await conn.execute(
                    text("INSERT INTO projects (id) VALUES (:id)"),
                    {"id": project_id},
                )
                await conn.execute(
                    text("INSERT INTO files (id, project_id) VALUES (:id, :project_id)"),
                    {"id": file_id, "project_id": project_id},
                )
                await conn.execute(
                    text(
                        """
                        INSERT INTO jobs (
                            id,
                            project_id,
                            file_id,
                            job_type,
                            status,
                            attempts,
                            max_attempts,
                            cancel_requested,
                            created_at
                        ) VALUES (
                            :id,
                            :project_id,
                            :file_id,
                            :job_type,
                            :status,
                            0,
                            3,
                            false,
                            :created_at
                        )
                        """
                    ),
                    [
                        {
                            "id": first_ingest_job_id,
                            "project_id": project_id,
                            "file_id": file_id,
                            "job_type": "ingest",
                            "status": "succeeded",
                            "created_at": datetime(2026, 5, 1, tzinfo=UTC),
                        },
                        {
                            "id": second_ingest_job_id,
                            "project_id": project_id,
                            "file_id": file_id,
                            "job_type": "ingest",
                            "status": "pending",
                            "created_at": datetime(2026, 5, 2, tzinfo=UTC),
                        },
                        {
                            "id": export_job_id,
                            "project_id": project_id,
                            "file_id": file_id,
                            "job_type": "export",
                            "status": "pending",
                            "created_at": datetime(2026, 5, 3, tzinfo=UTC),
                        },
                    ],
                )

                await conn.run_sync(lambda sync_conn: _run_migration_upgrade(sync_conn, migration))

                jobs = (
                    (
                        await conn.execute(
                            text(
                                """
                                SELECT id, extraction_profile_id
                                FROM jobs
                                ORDER BY created_at ASC, id ASC
                                """
                            )
                        )
                    )
                    .mappings()
                    .all()
                )
                assert len(jobs) == 3
                profile_ids = {row["extraction_profile_id"] for row in jobs}
                assert len(profile_ids) == 1
                assert None not in profile_ids
                profile_id = next(iter(profile_ids))

                extraction_profiles = (
                    (
                        await conn.execute(
                            text(
                                """
                                SELECT id, project_id, profile_version, layout_mode, xref_handling,
                                       block_handling, text_extraction, dimension_extraction,
                                       pdf_page_range, raster_calibration, confidence_threshold
                                FROM extraction_profiles
                                """
                            )
                        )
                    )
                    .mappings()
                    .all()
                )
                assert len(extraction_profiles) == 1
                assert extraction_profiles[0]["id"] == profile_id
                assert extraction_profiles[0]["project_id"] == project_id
                assert extraction_profiles[0]["profile_version"] == "v0.1"
                assert extraction_profiles[0]["layout_mode"] == "auto"
                assert extraction_profiles[0]["xref_handling"] == "preserve"
                assert extraction_profiles[0]["block_handling"] == "expand"
                assert extraction_profiles[0]["text_extraction"] is True
                assert extraction_profiles[0]["dimension_extraction"] is True
                assert extraction_profiles[0]["pdf_page_range"] is None
                assert extraction_profiles[0]["raster_calibration"] is None
                assert extraction_profiles[0]["confidence_threshold"] == pytest.approx(0.6)

                file_row = (
                    (
                        await conn.execute(
                            text(
                                """
                                SELECT initial_job_id, initial_extraction_profile_id
                                FROM files
                                WHERE id = :file_id
                                """
                            ),
                            {"file_id": file_id},
                        )
                    )
                    .mappings()
                    .one()
                )
                assert file_row == {
                    "initial_job_id": first_ingest_job_id,
                    "initial_extraction_profile_id": profile_id,
                }
        finally:
            async with engine.begin() as conn:
                await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))


def _load_extraction_profiles_migration() -> Any:
    """Load the extraction profiles migration module directly from disk."""
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "2026_05_05_0004_add_extraction_profiles.py"
    )
    spec = importlib.util.spec_from_file_location(
        "migration_2026_05_05_0004_add_extraction_profiles",
        migration_path,
    )
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_job_constraints_migration() -> Any:
    """Load the job-constraint migration module directly from disk."""
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "2026_05_10_0007_constrain_job_fields_and_ingest_profile.py"
    )
    spec = importlib.util.spec_from_file_location(
        "migration_2026_05_10_0007_constrain_job_fields_and_ingest_profile",
        migration_path,
    )
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _install_pre_0004_schema(sync_conn: sa.Connection) -> None:
    """Create the minimal pre-0004 schema needed to exercise the migration."""
    metadata = sa.MetaData()
    sa.Table(
        "projects",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
    )
    sa.Table(
        "files",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("project_id", sa.Uuid(), nullable=False),
    )
    sa.Table(
        "jobs",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("file_id", sa.Uuid(), nullable=False),
        sa.Column("job_type", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default=sa.text("3")),
        sa.Column(
            "cancel_requested",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column("error_code", sa.String(length=128), nullable=True),
        sa.Column("error_message", sa.String(length=2048), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    metadata.create_all(sync_conn)


def _install_pre_0007_jobs_schema(sync_conn: sa.Connection) -> None:
    """Create the minimal pre-0007 schema needed to exercise job constraints."""
    metadata = sa.MetaData()
    sa.Table(
        "projects",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
    )
    sa.Table(
        "files",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("initial_extraction_profile_id", sa.Uuid(), nullable=True),
    )
    sa.Table(
        "jobs",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("file_id", sa.Uuid(), nullable=False),
        sa.Column("extraction_profile_id", sa.Uuid(), nullable=True),
        sa.Column("job_type", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default=sa.text("3")),
        sa.Column(
            "cancel_requested",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column("error_code", sa.String(length=128), nullable=True),
        sa.Column("error_message", sa.String(length=2048), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    metadata.create_all(sync_conn)


def _run_migration_upgrade(sync_conn: sa.Connection, migration: Any) -> None:
    """Run the extraction-profile upgrade against a live connection."""
    migration_context = MigrationContext.configure(sync_conn)
    migration.op = Operations(migration_context)
    migration.upgrade()


def _run_migration_downgrade(sync_conn: sa.Connection, migration: Any) -> None:
    """Run the corresponding downgrade against a live connection."""
    migration_context = MigrationContext.configure(sync_conn)
    migration.op = Operations(migration_context)
    migration.downgrade()


async def _get_job_check_constraints(conn: Any, schema_name: str) -> dict[str, bool]:
    """Return job-table check constraints and validation state for a schema."""
    rows = (
        (
            await conn.execute(
                text(
                    """
                    SELECT con.conname, con.convalidated
                    FROM pg_constraint AS con
                    JOIN pg_class AS rel
                      ON rel.oid = con.conrelid
                    JOIN pg_namespace AS nsp
                      ON nsp.oid = rel.relnamespace
                    WHERE nsp.nspname = :schema_name
                      AND rel.relname = 'jobs'
                      AND con.contype = 'c'
                    ORDER BY con.conname ASC
                    """
                ),
                {"schema_name": schema_name},
            )
        )
        .mappings()
        .all()
    )
    return {
        cast(str, row["conname"]): cast(bool, row["convalidated"])
        for row in rows
    }


class _FakeScalarResult:
    """Small scalar result stand-in for migration guard tests."""

    def __init__(self, value: bool) -> None:
        self._value = value

    def scalar(self) -> bool:
        """Return the configured scalar value."""
        return self._value


class _FakeBind:
    """Minimal Alembic bind double for downgrade guard tests."""

    def __init__(self, *, profile_rows_exist: bool) -> None:
        self.profile_rows_exist = profile_rows_exist

    def execute(self, *_: Any, **__: Any) -> _FakeScalarResult:
        """Return the configured extraction-profile existence result."""
        return _FakeScalarResult(self.profile_rows_exist)


class _FakeOp:
    """Minimal Alembic op double for downgrade guard tests."""

    def __init__(self, *, profile_rows_exist: bool) -> None:
        self._bind = _FakeBind(profile_rows_exist=profile_rows_exist)
        self.drop_calls: list[tuple[str, str]] = []

    def get_bind(self) -> _FakeBind:
        """Return the fake bind used by the downgrade guard."""
        return self._bind

    def drop_constraint(self, name: str, table_name: str, *, type_: str) -> None:
        """Record drop calls that would mutate schema state."""
        _ = type_
        self.drop_calls.append(("drop_constraint", f"{table_name}:{name}"))

    def drop_index(self, name: str, *, table_name: str) -> None:
        """Record drop index calls."""
        self.drop_calls.append(("drop_index", f"{table_name}:{name}"))

    def drop_column(self, table_name: str, column_name: str) -> None:
        """Record drop column calls."""
        self.drop_calls.append(("drop_column", f"{table_name}:{column_name}"))

    def drop_table(self, table_name: str) -> None:
        """Record drop table calls."""
        self.drop_calls.append(("drop_table", table_name))

    def f(self, name: str) -> str:
        """Mimic Alembic naming helper passthrough."""
        return name


def test_extraction_profiles_migration_downgrade_raises_when_rows_exist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Downgrade should refuse to drop profile lineage when rows already exist."""
    migration = _load_extraction_profiles_migration()
    fake_op = _FakeOp(profile_rows_exist=True)
    monkeypatch.setattr(migration, "op", fake_op)

    with pytest.raises(RuntimeError, match="Manual data-preserving rollback is required"):
        migration.downgrade()

    assert fake_op.drop_calls == []


def test_job_read_accepts_null_extraction_profile_id_during_rollback_window() -> None:
    """Read schema should tolerate historical jobs until the contract migration lands."""
    job = JobRead.model_validate(
        {
            "id": uuid.uuid4(),
            "project_id": uuid.uuid4(),
            "file_id": uuid.uuid4(),
            "extraction_profile_id": None,
            "job_type": "ingest",
            "status": "pending",
            "attempts": 0,
            "max_attempts": 3,
            "cancel_requested": False,
            "error_code": None,
            "error_message": None,
            "started_at": None,
            "finished_at": None,
            "created_at": datetime(2026, 5, 5, tzinfo=UTC),
        }
    )

    assert job.extraction_profile_id is None


@pytest.mark.parametrize(
    "value",
    ["1--3", "0", "-1", "1,", ",1", "1,,2", "3-1", "1-0"],
)
def test_extraction_profile_create_rejects_malformed_pdf_page_ranges(value: str) -> None:
    """Schema should reject malformed page selectors before reprocess jobs are created."""
    with pytest.raises(ValidationError, match="pdf_page_range"):
        ExtractionProfileCreate(pdf_page_range=value)


def test_extraction_profile_create_accepts_csv_page_ranges_and_strict_raster_calibration() -> None:
    """Schema should accept valid page ranges and typed raster calibration payloads."""
    profile = ExtractionProfileCreate.model_validate(
        {
            "pdf_page_range": "1, 2-4",
            "raster_calibration": {"scale": 48, "unit": "inch"},
        }
    )

    assert profile.pdf_page_range == "1, 2-4"
    assert profile.raster_calibration is not None
    assert profile.raster_calibration.model_dump() == {"scale": 48, "unit": "inch"}


@pytest.mark.parametrize(
    ("value", "match"),
    [
        ({"scale": 48, "unit": "inch", "extra": True}, "extra_forbidden"),
        ({"scale": 0, "unit": "inch"}, "raster_calibration.scale"),
        ({"scale": -1, "unit": "inch"}, "raster_calibration.scale"),
        ({"scale": math.inf, "unit": "inch"}, "raster_calibration.scale"),
        ({"scale": -math.inf, "unit": "inch"}, "raster_calibration.scale"),
        ({"scale": math.nan, "unit": "inch"}, "raster_calibration.scale"),
        ({"scale": "48", "unit": "inch"}, "raster_calibration.scale"),
        ({"scale": 48, "unit": "yards"}, "literal_error"),
    ],
)
def test_extraction_profile_create_rejects_invalid_raster_calibration(
    value: dict[str, Any],
    match: str,
) -> None:
    """Schema should enforce strict raster calibration shape and values."""
    with pytest.raises(ValidationError, match=match):
        ExtractionProfileCreate.model_validate({"raster_calibration": value})
