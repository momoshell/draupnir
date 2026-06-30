"""Session-backed execution-input adapters used by worker job attempts."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.ingestion.contracts import InputFamily
from app.ingestion.runner import IngestionRunRequest
from app.jobs.execution_inputs import _ExportExecutionInput, _QuantityTakeoffExecutionInput
from app.jobs.export_execution_input import (
    BuildArtifactName,
    ResolveExportSpec,
)
from app.jobs.export_execution_input import (
    build_export_execution_input as validate_export_execution_input,
)
from app.jobs.quantity_execution_input import (
    build_quantity_takeoff_execution_input as validate_quantity_takeoff_execution_input,
)
from app.models.drawing_revision import DrawingRevision
from app.models.estimate_version import EstimateVersion
from app.models.export_job_input import ExportJobInput
from app.models.job import Job
from app.models.quantity_takeoff import QuantityTakeoff
from app.models.revision_materialization import RevisionEntity, RevisionEntityManifest
from app.models.validation_report import ValidationReport


def requested_input_family_from_pdf_input_mode(pdf_input_mode: str | None) -> InputFamily | None:
    """Map persisted PDF input mode to an explicit runner input family override."""
    if pdf_input_mode == "vector":
        return InputFamily.PDF_VECTOR
    if pdf_input_mode == "raster":
        return InputFamily.PDF_RASTER
    return None


async def build_ingestion_run_request(
    job_id: UUID,
    *,
    attempt_token: UUID,
    session_maker_factory: Callable[[], Any],
    get_job_lock_bootstrap: Callable[..., Any],
    get_project: Callable[..., Any],
    get_job_for_update_with_metadata: Callable[..., Any],
    job_attempt_is_current: Callable[..., bool],
    assert_job_base_revision_invariants: Callable[[Job], None],
    cancel_job_for_inactive_source: Callable[..., Any],
    get_source_file: Callable[..., Any],
    get_extraction_profile: Callable[..., Any],
    inactive_source_error_type: type[Exception],
    stale_job_attempt_error_type: type[Exception],
) -> IngestionRunRequest:
    """Load persisted job and file metadata for the ingestion runner."""
    session_maker = session_maker_factory()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        bootstrap = await get_job_lock_bootstrap(session, job_id)
        if bootstrap is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        project = await get_project(session, bootstrap.project_id, for_update=True)
        if project is None:
            raise LookupError(
                f"Project with identifier '{bootstrap.project_id}' for job '{job_id}' not found"
            )

        job = await get_job_for_update_with_metadata(
            session,
            job_id,
            expected_project_id=bootstrap.project_id,
            expected_file_id=bootstrap.file_id,
        )
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")
        if not job_attempt_is_current(job, attempt_token=attempt_token):
            raise stale_job_attempt_error_type(
                f"Job attempt for '{job_id}' no longer owns the lease"
            )
        assert_job_base_revision_invariants(job)

        if project.deleted_at is not None:
            cancelled = await cancel_job_for_inactive_source(
                session,
                job,
                reason="source_deleted",
                attempt_token=attempt_token,
            )
            if not cancelled:
                raise stale_job_attempt_error_type(
                    f"Job attempt for '{job_id}' no longer owns the lease"
                )
            raise inactive_source_error_type(
                f"Project with identifier '{job.project_id}' for job '{job_id}' is no longer active"
            )

        source_file = await get_source_file(
            session,
            project_id=job.project_id,
            file_id=job.file_id,
            for_update=True,
        )
        if source_file is None or source_file.deleted_at is not None:
            cancelled = await cancel_job_for_inactive_source(
                session,
                job,
                reason="source_deleted",
                attempt_token=attempt_token,
            )
            if not cancelled:
                raise stale_job_attempt_error_type(
                    f"Job attempt for '{job_id}' no longer owns the lease"
                )
            raise inactive_source_error_type(
                f"File with identifier '{job.file_id}' for job '{job_id}' is no longer active"
            )

        requested_input_family = None
        if job.extraction_profile_id is not None:
            extraction_profile = await get_extraction_profile(
                session,
                extraction_profile_id=job.extraction_profile_id,
            )
            requested_input_family = requested_input_family_from_pdf_input_mode(
                extraction_profile.pdf_input_mode if extraction_profile is not None else None
            )

        return IngestionRunRequest(
            job_id=job.id,
            file_id=source_file.id,
            checksum_sha256=source_file.checksum_sha256,
            detected_format=source_file.detected_format,
            media_type=source_file.media_type,
            original_name=source_file.original_filename,
            extraction_profile_id=job.extraction_profile_id,
            initial_job_id=source_file.initial_job_id,
            requested_input_family=requested_input_family,
        )


class SessionExportRowLoader:
    """Session-backed ``ExportRowLoader`` used by the worker export path."""

    def __init__(
        self,
        session: AsyncSession,
        *,
        get_drawing_revision: Callable[..., Any],
    ) -> None:
        self._session = session
        self._get_drawing_revision = get_drawing_revision

    async def get_job(self, job_id: UUID) -> Job | None:
        return await self._session.get(Job, job_id)

    async def get_export_job_input(self, job_id: UUID) -> ExportJobInput | None:
        return await self._session.get(ExportJobInput, job_id)

    async def get_drawing_revision(self, revision_id: UUID) -> DrawingRevision | None:
        return cast(
            DrawingRevision | None,
            await self._get_drawing_revision(self._session, revision_id=revision_id),
        )

    async def get_quantity_takeoff(self, takeoff_id: UUID) -> QuantityTakeoff | None:
        return await self._session.get(QuantityTakeoff, takeoff_id)

    async def get_estimate_version(self, version_id: UUID) -> EstimateVersion | None:
        return await self._session.get(EstimateVersion, version_id)


async def build_export_execution_input(
    job_id: UUID,
    *,
    attempt_token: UUID,
    session_maker_factory: Callable[[], Any],
    row_loader_factory: Callable[[AsyncSession], Any],
    resolve_export_spec: ResolveExportSpec,
    build_artifact_name: BuildArtifactName,
) -> _ExportExecutionInput:
    """Load deterministic persisted inputs for a claimed export job."""
    session_maker = session_maker_factory()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        return await validate_export_execution_input(
            job_id,
            attempt_token=attempt_token,
            loader=row_loader_factory(session),
            resolve_export_spec=resolve_export_spec,
            build_artifact_name=build_artifact_name,
        )


class SessionQuantityRowLoader:
    """Session-backed ``QuantityRowLoader`` used by the worker quantity path."""

    def __init__(
        self,
        session: AsyncSession,
        *,
        get_drawing_revision: Callable[..., Any],
        get_validation_report_for_revision: Callable[..., Any],
        get_revision_entity_manifest_for_revision: Callable[..., Any],
        get_revision_entities_for_revision: Callable[..., Any],
    ) -> None:
        self._session = session
        self._get_drawing_revision = get_drawing_revision
        self._get_validation_report_for_revision = get_validation_report_for_revision
        self._get_revision_entity_manifest_for_revision = get_revision_entity_manifest_for_revision
        self._get_revision_entities_for_revision = get_revision_entities_for_revision

    async def get_job(self, job_id: UUID) -> Job | None:
        return await self._session.get(Job, job_id)

    async def get_drawing_revision(self, revision_id: UUID) -> DrawingRevision | None:
        return cast(
            DrawingRevision | None,
            await self._get_drawing_revision(self._session, revision_id=revision_id),
        )

    async def get_validation_report(
        self, *, project_id: UUID, drawing_revision_id: UUID
    ) -> ValidationReport | None:
        return cast(
            ValidationReport | None,
            await self._get_validation_report_for_revision(
                self._session,
                project_id=project_id,
                drawing_revision_id=drawing_revision_id,
            ),
        )

    async def get_entity_manifest(
        self, *, project_id: UUID, source_file_id: UUID, drawing_revision_id: UUID
    ) -> RevisionEntityManifest | None:
        return cast(
            RevisionEntityManifest | None,
            await self._get_revision_entity_manifest_for_revision(
                self._session,
                project_id=project_id,
                source_file_id=source_file_id,
                drawing_revision_id=drawing_revision_id,
            ),
        )

    async def get_revision_entities(
        self, *, project_id: UUID, source_file_id: UUID, drawing_revision_id: UUID
    ) -> list[RevisionEntity]:
        return cast(
            list[RevisionEntity],
            await self._get_revision_entities_for_revision(
                self._session,
                project_id=project_id,
                source_file_id=source_file_id,
                drawing_revision_id=drawing_revision_id,
            ),
        )


async def build_quantity_takeoff_execution_input(
    job_id: UUID,
    *,
    attempt_token: UUID,
    session_maker_factory: Callable[[], Any],
    row_loader_factory: Callable[[AsyncSession], Any],
) -> _QuantityTakeoffExecutionInput:
    """Load unlocked quantity engine inputs for a claimed persisted job."""
    session_maker = session_maker_factory()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        return await validate_quantity_takeoff_execution_input(
            job_id,
            attempt_token=attempt_token,
            loader=row_loader_factory(session),
        )
