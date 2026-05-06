"""Celery worker application and persisted job handlers."""

import asyncio
import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from celery import Celery
from celery.signals import worker_ready
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.errors import ErrorCode
from app.core.logging import get_logger
from app.db.session import get_session_maker
from app.models.adapter_run_output import AdapterRunOutput
from app.models.drawing_revision import DrawingRevision
from app.models.file import File
from app.models.job import Job
from app.models.job_event import JobEvent
from app.models.validation_report import ValidationReport

logger = get_logger(__name__)

_INGEST_NOOP_DELAY_SECONDS = 0.01
_INCOMPLETE_JOB_STATUSES = ("pending", "running")
_TERMINAL_JOB_STATUSES = {"failed", "succeeded", "cancelled"}
_DEFAULT_ADAPTER_TIMEOUT = timedelta(minutes=5)
_RUNNING_JOB_STALE_AFTER = _DEFAULT_ADAPTER_TIMEOUT * 2
_JOB_CANCELLED_ERROR_CODE = ErrorCode.JOB_CANCELLED.value
_ENQUEUE_INGEST_JOB_ERROR_MESSAGE = "Failed to enqueue ingest job"
_FINALIZE_INGEST_JOB_ERROR_MESSAGE = "Failed to finalize ingest job"
_NOOP_ADAPTER_KEY = "noop.ingest"
_NOOP_ADAPTER_VERSION = "0.1"
_CANONICAL_ENTITY_SCHEMA_VERSION = "0.1"
_VALIDATION_REPORT_SCHEMA_VERSION = "0.1"
_NOOP_REVIEW_STATE = "review_required"
_NOOP_VALIDATION_STATUS = "needs_review"
_NOOP_QUANTITY_GATE = "review_gated"
_NOOP_CONFIDENCE_SCORE = 0.0
_INITIAL_INGEST_REVISION_KIND = "ingest"
_REPROCESS_REVISION_KIND = "reprocess"
_NOOP_VALIDATOR_NAME = "noop.ingest"
_NOOP_VALIDATOR_VERSION = "0.1"

celery_app = Celery(
    "draupnir",
    broker=settings.broker_url,
)
celery_app.conf.update(
    task_ignore_result=True,
    task_store_eager_result=False,
    task_publish_retry=False,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
)

# Auto-discover tasks from the jobs module
celery_app.autodiscover_tasks(["app.jobs"], force=True)


def _utcnow() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class NoopIngestPayloadSource:
    """Immutable source snapshot used to build no-op ingest payloads."""

    file_id: UUID
    extraction_profile_id: UUID | None
    initial_job_id: UUID | None
    detected_format: str | None
    media_type: str


@dataclass(frozen=True, slots=True)
class NoopIngestOutputPayload:
    """Prepared no-op ingest payload inserted during finalization."""

    revision_kind: str
    adapter_key: str
    adapter_version: str
    input_family: str
    canonical_entity_schema_version: str
    canonical_json: dict[str, Any]
    provenance_json: dict[str, Any]
    confidence_json: dict[str, Any]
    confidence_score: float
    warnings_json: list[Any]
    diagnostics_json: dict[str, Any]
    result_checksum_sha256: str
    validation_report_schema_version: str
    validation_status: str
    review_state: str
    quantity_gate: str
    effective_confidence: float
    validator_name: str
    validator_version: str
    report_json: dict[str, Any]
    generated_at: datetime


def _is_stale_running_job(job: Job, *, now: datetime) -> bool:
    """Return whether a running job is old enough to treat as orphaned."""
    if job.started_at is None:
        return True

    started_at = job.started_at
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=UTC)

    return started_at <= now - _RUNNING_JOB_STALE_AFTER


async def _get_job_for_update(session: AsyncSession, job_id: UUID) -> Job | None:
    """Load and lock a persisted job row."""
    result = await session.execute(select(Job).where(Job.id == job_id).with_for_update())
    return result.scalar_one_or_none()


async def _get_source_file(
    session: AsyncSession,
    *,
    project_id: UUID,
    file_id: UUID,
    for_update: bool = False,
) -> File | None:
    """Load a source file row, optionally under a row lock."""
    statement = select(File).where((File.project_id == project_id) & (File.id == file_id))
    if for_update:
        statement = statement.with_for_update()

    result = await session.execute(statement)
    return result.scalar_one_or_none()


async def _get_existing_adapter_run_output(
    session: AsyncSession,
    *,
    source_job_id: UUID,
) -> AdapterRunOutput | None:
    """Load an existing committed adapter output for a job."""
    result = await session.execute(
        select(AdapterRunOutput).where(AdapterRunOutput.source_job_id == source_job_id)
    )
    return result.scalar_one_or_none()


async def _get_latest_drawing_revision(
    session: AsyncSession,
    *,
    project_id: UUID,
    source_file_id: UUID,
) -> DrawingRevision | None:
    """Load the latest drawing revision for a source file."""
    result = await session.execute(
        select(DrawingRevision)
        .where(
            (DrawingRevision.project_id == project_id)
            & (DrawingRevision.source_file_id == source_file_id)
        )
        .order_by(DrawingRevision.revision_sequence.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


def _normalize_input_family(source: NoopIngestPayloadSource) -> str:
    """Map immutable file metadata to a stable no-op input family."""
    if source.detected_format is not None:
        return source.detected_format.lower()

    media_type = source.media_type.lower()
    if media_type == "application/pdf":
        return "pdf"

    return "unknown"


def _compute_adapter_result_checksum(result_envelope: dict[str, Any]) -> str:
    """Return a stable SHA-256 checksum for a committed result envelope."""
    payload = json.dumps(result_envelope, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha256(payload).hexdigest()


def _resolve_revision_kind(job_id: UUID, *, initial_job_id: UUID | None) -> str:
    """Map file linkage to the correct ingest revision kind."""
    if initial_job_id == job_id:
        return _INITIAL_INGEST_REVISION_KIND

    return _REPROCESS_REVISION_KIND


def _resolve_noop_revision_kind(job_id: UUID, *, source: NoopIngestPayloadSource) -> str:
    """Map the source file linkage to the correct ingest revision kind."""
    return _resolve_revision_kind(job_id, initial_job_id=source.initial_job_id)


def _build_noop_entity_counts() -> dict[str, int]:
    """Return stable empty entity counts for no-op payloads."""
    return {
        "layouts": 0,
        "layers": 0,
        "blocks": 0,
        "entities": 0,
    }


def _assert_revision_invariants(
    job_id: UUID,
    *,
    source_file: File,
    predecessor_revision: DrawingRevision | None,
    payload_revision_kind: str,
) -> None:
    """Reject finalization when revision lineage invariants are violated."""
    expected_revision_kind = _resolve_revision_kind(
        job_id,
        initial_job_id=source_file.initial_job_id,
    )
    if payload_revision_kind != expected_revision_kind:
        raise ValueError("Ingest job revision kind changed before finalization")

    if expected_revision_kind == _INITIAL_INGEST_REVISION_KIND:
        if predecessor_revision is not None:
            raise ValueError("Initial ingest cannot finalize after a predecessor revision exists")
        return

    if predecessor_revision is None:
        raise ValueError(
            "Reprocess ingest job cannot finalize before a predecessor revision exists"
        )


async def _load_noop_ingest_payload_source(job_id: UUID) -> NoopIngestPayloadSource:
    """Load immutable file metadata needed for no-op payload construction."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        result = await session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one_or_none()
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        source_file = await _get_source_file(
            session,
            project_id=job.project_id,
            file_id=job.file_id,
        )
        if source_file is None:
            raise LookupError(
                f"File with identifier '{job.file_id}' for job '{job_id}' not found"
            )

        return NoopIngestPayloadSource(
            file_id=source_file.id,
            extraction_profile_id=job.extraction_profile_id,
            initial_job_id=source_file.initial_job_id,
            detected_format=source_file.detected_format,
            media_type=source_file.media_type,
        )


def _build_noop_ingest_output_payload(
    job_id: UUID,
    *,
    source: NoopIngestPayloadSource,
) -> NoopIngestOutputPayload:
    """Construct the immutable no-op ingest payload outside finalization."""
    generated_at = _utcnow()
    input_family = _normalize_input_family(source)
    revision_kind = _resolve_noop_revision_kind(job_id, source=source)
    entity_counts = _build_noop_entity_counts()
    canonical_json: dict[str, Any] = {
        "canonical_entity_schema_version": _CANONICAL_ENTITY_SCHEMA_VERSION,
        "schema_version": _CANONICAL_ENTITY_SCHEMA_VERSION,
        "layouts": [],
        "layers": [],
        "blocks": [],
        "entities": [],
        "entity_counts": entity_counts,
    }
    provenance_json = {
        "schema_version": _CANONICAL_ENTITY_SCHEMA_VERSION,
        "bridge": "noop_ingest",
        "adapter": {
            "key": _NOOP_ADAPTER_KEY,
            "version": _NOOP_ADAPTER_VERSION,
        },
        "source": {
            "file_id": str(source.file_id),
            "job_id": str(job_id),
            "extraction_profile_id": (
                str(source.extraction_profile_id)
                if source.extraction_profile_id is not None
                else None
            ),
            "input_family": input_family,
            "revision_kind": revision_kind,
        },
        "generated_at": generated_at.isoformat(),
    }
    noop_warning = {
        "code": "NOOP_INGEST_BRIDGE",
        "severity": "warning",
        "message": "No real adapter executed; review is required.",
    }
    confidence_json = {
        "score": _NOOP_CONFIDENCE_SCORE,
        "effective_confidence": _NOOP_CONFIDENCE_SCORE,
        "review_state": _NOOP_REVIEW_STATE,
    }
    warnings_json: list[Any] = [noop_warning]
    diagnostics_json = {
        "adapter": _NOOP_ADAPTER_KEY,
        "adapter_version": _NOOP_ADAPTER_VERSION,
        "bridge": "noop_ingest",
        "timing": {"sleep_seconds": _INGEST_NOOP_DELAY_SECONDS},
    }
    report_json = {
        "validation_report_schema_version": _VALIDATION_REPORT_SCHEMA_VERSION,
        "canonical_entity_schema_version": _CANONICAL_ENTITY_SCHEMA_VERSION,
        "validator": {
            "name": _NOOP_VALIDATOR_NAME,
            "version": _NOOP_VALIDATOR_VERSION,
        },
        "summary": {
            "validation_status": _NOOP_VALIDATION_STATUS,
            "review_state": _NOOP_REVIEW_STATE,
            "quantity_gate": _NOOP_QUANTITY_GATE,
            "effective_confidence": _NOOP_CONFIDENCE_SCORE,
            "entity_counts": entity_counts,
        },
        "checks": [],
        "findings": [noop_warning],
        "adapter_warnings": warnings_json,
        "provenance": provenance_json,
    }
    result_envelope = {
        "adapter_key": _NOOP_ADAPTER_KEY,
        "adapter_version": _NOOP_ADAPTER_VERSION,
        "input_family": input_family,
        "canonical_entity_schema_version": _CANONICAL_ENTITY_SCHEMA_VERSION,
        "canonical_json": canonical_json,
        "provenance_json": provenance_json,
        "confidence_json": confidence_json,
        "confidence_score": _NOOP_CONFIDENCE_SCORE,
        "warnings_json": warnings_json,
        "diagnostics_json": diagnostics_json,
    }

    return NoopIngestOutputPayload(
        revision_kind=revision_kind,
        adapter_key=_NOOP_ADAPTER_KEY,
        adapter_version=_NOOP_ADAPTER_VERSION,
        input_family=input_family,
        canonical_entity_schema_version=_CANONICAL_ENTITY_SCHEMA_VERSION,
        canonical_json=canonical_json,
        provenance_json=provenance_json,
        confidence_json=confidence_json,
        confidence_score=_NOOP_CONFIDENCE_SCORE,
        warnings_json=warnings_json,
        diagnostics_json=diagnostics_json,
        result_checksum_sha256=_compute_adapter_result_checksum(result_envelope),
        validation_report_schema_version=_VALIDATION_REPORT_SCHEMA_VERSION,
        validation_status=_NOOP_VALIDATION_STATUS,
        review_state=_NOOP_REVIEW_STATE,
        quantity_gate=_NOOP_QUANTITY_GATE,
        effective_confidence=_NOOP_CONFIDENCE_SCORE,
        validator_name=_NOOP_VALIDATOR_NAME,
        validator_version=_NOOP_VALIDATOR_VERSION,
        report_json=report_json,
        generated_at=generated_at,
    )


async def _finalize_ingest_job(job_id: UUID, *, payload: NoopIngestOutputPayload) -> bool:
    """Atomically publish durable no-op outputs and terminal job success."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        job = await _get_job_for_update(session, job_id)
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        if job.status in _TERMINAL_JOB_STATUSES:
            logger.info(
                "ingest_job_completion_skipped_terminal_status",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        if job.cancel_requested:
            _finalize_job_cancelled(job)
            await emit_job_event(
                job.id,
                level="warning",
                message="Job cancelled",
                data_json={"status": "cancelled"},
                session=session,
            )
            await session.commit()
            logger.info("ingest_job_cancelled", job_id=str(job_id))
            return False

        if job.status != "running":
            logger.info(
                "ingest_job_completion_skipped_non_running_status",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        existing_output = await _get_existing_adapter_run_output(session, source_job_id=job.id)
        if existing_output is not None:
            logger.info(
                "ingest_job_completion_skipped_existing_output",
                job_id=str(job_id),
                adapter_run_output_id=str(existing_output.id),
            )
            return False

        if job.extraction_profile_id is None:
            raise ValueError("Ingest job missing extraction profile during finalization")

        source_file = await _get_source_file(
            session,
            project_id=job.project_id,
            file_id=job.file_id,
            for_update=True,
        )
        if source_file is None:
            raise LookupError(
                f"File with identifier '{job.file_id}' for job '{job_id}' not found"
            )

        predecessor_revision = await _get_latest_drawing_revision(
            session,
            project_id=job.project_id,
            source_file_id=source_file.id,
        )
        _assert_revision_invariants(
            job.id,
            source_file=source_file,
            predecessor_revision=predecessor_revision,
            payload_revision_kind=payload.revision_kind,
        )
        revision_sequence = 1
        predecessor_revision_id: UUID | None = None
        if predecessor_revision is not None:
            revision_sequence = predecessor_revision.revision_sequence + 1
            predecessor_revision_id = predecessor_revision.id

        adapter_run_output_id = uuid.uuid4()
        drawing_revision_id = uuid.uuid4()
        validation_report_id = uuid.uuid4()
        finished_at = _utcnow()

        session.add(
            AdapterRunOutput(
                id=adapter_run_output_id,
                project_id=job.project_id,
                source_file_id=source_file.id,
                extraction_profile_id=job.extraction_profile_id,
                source_job_id=job.id,
                adapter_key=payload.adapter_key,
                adapter_version=payload.adapter_version,
                input_family=payload.input_family,
                canonical_entity_schema_version=payload.canonical_entity_schema_version,
                canonical_json=payload.canonical_json,
                provenance_json=payload.provenance_json,
                confidence_json=payload.confidence_json,
                confidence_score=payload.confidence_score,
                warnings_json=payload.warnings_json,
                diagnostics_json=payload.diagnostics_json,
                result_checksum_sha256=payload.result_checksum_sha256,
            )
        )
        session.add(
            DrawingRevision(
                id=drawing_revision_id,
                project_id=job.project_id,
                source_file_id=source_file.id,
                extraction_profile_id=job.extraction_profile_id,
                source_job_id=job.id,
                adapter_run_output_id=adapter_run_output_id,
                predecessor_revision_id=predecessor_revision_id,
                revision_sequence=revision_sequence,
                revision_kind=payload.revision_kind,
                review_state=payload.review_state,
                canonical_entity_schema_version=payload.canonical_entity_schema_version,
                confidence_score=payload.confidence_score,
            )
        )
        session.add(
            ValidationReport(
                id=validation_report_id,
                project_id=job.project_id,
                drawing_revision_id=drawing_revision_id,
                source_job_id=job.id,
                validation_report_schema_version=payload.validation_report_schema_version,
                canonical_entity_schema_version=payload.canonical_entity_schema_version,
                validation_status=payload.validation_status,
                review_state=payload.review_state,
                quantity_gate=payload.quantity_gate,
                effective_confidence=payload.effective_confidence,
                validator_name=payload.validator_name,
                validator_version=payload.validator_version,
                report_json=payload.report_json,
                generated_at=payload.generated_at,
            )
        )

        job.status = "succeeded"
        job.finished_at = finished_at
        job.error_code = None
        job.error_message = None
        await emit_job_event(
            job.id,
            level="info",
            message="Job succeeded",
            data_json={
                "status": "succeeded",
                "attempts": job.attempts,
                "adapter_run_output_id": str(adapter_run_output_id),
                "drawing_revision_id": str(drawing_revision_id),
                "validation_report_id": str(validation_report_id),
            },
            session=session,
        )
        await session.commit()

    return True


async def emit_job_event(
    job_id: UUID,
    *,
    level: str,
    message: str,
    data_json: dict[str, Any] | None = None,
    session: AsyncSession | None = None,
) -> None:
    """Persist a job lifecycle event."""
    event = JobEvent(
        job_id=job_id,
        level=level,
        message=message,
        data_json=data_json,
    )
    if session is not None:
        session.add(event)
        return

    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as managed_session:
        managed_session.add(event)
        await managed_session.commit()


async def _mark_job_failed(
    job_id: UUID,
    *,
    error_message: str,
    error_code: ErrorCode = ErrorCode.INTERNAL_ERROR,
) -> None:
    """Persist a failed job state with the supplied message."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        job = await _get_job_for_update(session, job_id)
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        if job.status in _TERMINAL_JOB_STATUSES:
            logger.info(
                "ingest_job_failure_mark_skipped_terminal_status",
                job_id=str(job_id),
                status=job.status,
            )
            return
        job.status = "failed"
        job.error_code = error_code.value
        job.error_message = error_message
        job.finished_at = _utcnow()
        await emit_job_event(
            job_id,
            level="error",
            message="Job failed",
            data_json={
                "status": "failed",
                "error_code": error_code.value,
                "error_message": error_message,
            },
            session=session,
        )
        await session.commit()


async def _mark_recovery_enqueue_failed(job_id: UUID) -> None:
    """Persist and log a sanitized worker-recovery enqueue failure."""
    await _mark_job_failed(job_id, error_message=_ENQUEUE_INGEST_JOB_ERROR_MESSAGE)
    logger.error(
        "ingest_job_recovery_enqueue_failed",
        job_id=str(job_id),
        error_code=ErrorCode.INTERNAL_ERROR.value,
        recovery_action="mark_failed",
    )


def _finalize_job_cancelled(job: Job) -> None:
    """Apply the persisted cancelled terminal state to a job."""
    job.status = "cancelled"
    job.error_code = _JOB_CANCELLED_ERROR_CODE
    job.error_message = None
    job.finished_at = _utcnow()


async def _begin_or_resume_ingest_job(job_id: UUID) -> bool:
    """Claim, resume, or cancel a persisted ingest job under a row lock."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    now = _utcnow()

    async with session_maker() as session:
        job = await _get_job_for_update(session, job_id)
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        if job.status in _TERMINAL_JOB_STATUSES:
            logger.info(
                "ingest_job_cancel_skipped_terminal_status",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        if not job.cancel_requested:
            if job.status == "running":
                if _is_stale_running_job(job, now=now):
                    job.attempts += 1
                    job.started_at = now
                    job.finished_at = None
                    job.error_code = None
                    job.error_message = None
                    await emit_job_event(
                        job.id,
                        level="info",
                        message="Job started",
                        data_json={
                            "status": "running",
                            "attempts": job.attempts,
                            "reclaimed": True,
                        },
                        session=session,
                    )
                    await session.commit()
                    logger.warning(
                        "ingest_job_reclaimed_stale_running_status",
                        job_id=str(job_id),
                        status=job.status,
                    )
                    return True

                logger.info(
                    "ingest_job_continuing_running_status",
                    job_id=str(job_id),
                    status=job.status,
                )
                return True

            job.attempts += 1
            job.status = "running"
            job.started_at = now
            job.finished_at = None
            job.error_code = None
            job.error_message = None
            await emit_job_event(
                job.id,
                level="info",
                message="Job started",
                data_json={"status": "running", "attempts": job.attempts, "reclaimed": False},
                session=session,
            )
            await session.commit()
            return True

        _finalize_job_cancelled(job)
        await emit_job_event(
            job.id,
            level="warning",
            message="Job cancelled",
            data_json={"status": "cancelled"},
            session=session,
        )
        await session.commit()

    logger.info("ingest_job_cancelled", job_id=str(job_id))
    return False


async def process_ingest_job(job_id: UUID) -> None:
    """Load a persisted ingest job, perform a no-op, and persist state transitions."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    if not await _begin_or_resume_ingest_job(job_id):
        return

    try:
        await asyncio.sleep(_INGEST_NOOP_DELAY_SECONDS)
    except Exception as exc:
        await _mark_job_failed(job_id, error_message=str(exc))
        logger.error("ingest_job_failed", job_id=str(job_id), error=str(exc), exc_info=True)
        raise

    try:
        payload_source = await _load_noop_ingest_payload_source(job_id)
        payload = _build_noop_ingest_output_payload(job_id, source=payload_source)
        finalized = await _finalize_ingest_job(job_id, payload=payload)
    except Exception as exc:
        await _mark_job_failed(job_id, error_message=_FINALIZE_INGEST_JOB_ERROR_MESSAGE)
        logger.error(
            "ingest_job_finalization_failed",
            job_id=str(job_id),
            error=str(exc),
            exc_info=True,
        )
        raise

    if finalized:
        logger.info("ingest_job_succeeded", job_id=str(job_id))



async def recover_incomplete_ingest_jobs() -> list[UUID]:
    """Requeue incomplete persisted ingest jobs on worker startup."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    now = _utcnow()

    async with session_maker() as session:
        result = await session.execute(
            select(Job)
            .where(
                (Job.job_type == "ingest")
                & (Job.status.in_(_INCOMPLETE_JOB_STATUSES))
            )
            .order_by(Job.created_at.asc(), Job.id.asc())
        )
        jobs = result.scalars().all()

        recovered_job_ids: list[UUID] = []
        for job in jobs:
            if job.status == "running":
                if not _is_stale_running_job(job, now=now):
                    continue
                job.status = "pending"
                job.started_at = None
                job.finished_at = None
                job.error_code = None
                job.error_message = None
            recovered_job_ids.append(job.id)

        await session.commit()

    enqueued_job_ids: list[UUID] = []
    for job_id in recovered_job_ids:
        try:
            enqueue_ingest_job(job_id)
        except Exception:
            await _mark_recovery_enqueue_failed(job_id)
        else:
            enqueued_job_ids.append(job_id)

    return enqueued_job_ids


def recover_incomplete_ingest_jobs_on_worker_start(**_: object) -> None:
    """Requeue incomplete ingest jobs when a worker starts."""
    try:
        recovered_job_ids = asyncio.run(recover_incomplete_ingest_jobs())
    except Exception as exc:
        logger.error("ingest_job_recovery_failed", error=str(exc), exc_info=True)
        return

    if recovered_job_ids:
        logger.info(
            "ingest_job_recovery_completed",
            recovered_job_ids=[str(job_id) for job_id in recovered_job_ids],
        )


worker_ready.connect(recover_incomplete_ingest_jobs_on_worker_start)


@celery_app.task(
    name="app.jobs.worker.run_ingest_job",
    ignore_result=True,
    acks_late=True,
    reject_on_worker_lost=True,
)
def run_ingest_job(job_id: str) -> None:
    """Celery task wrapper for the persisted ingest job processor."""
    asyncio.run(process_ingest_job(UUID(job_id)))


def enqueue_ingest_job(job_id: UUID) -> None:
    """Publish a persisted ingest job to Celery."""
    run_ingest_job.apply_async(args=(str(job_id),), task_id=str(job_id), retry=False)
