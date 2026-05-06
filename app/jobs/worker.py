"""Celery worker application and persisted job handlers."""

import asyncio
import inspect
import uuid
from copy import deepcopy
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
from app.ingestion.contracts import AdapterTimeout, ProgressUpdate
from app.ingestion.finalization import IngestFinalizationPayload
from app.ingestion.runner import IngestionRunnerError, IngestionRunRequest, run_ingestion
from app.models.adapter_run_output import AdapterRunOutput
from app.models.drawing_revision import DrawingRevision
from app.models.file import File
from app.models.job import Job
from app.models.job_event import JobEvent
from app.models.validation_report import ValidationReport

logger = get_logger(__name__)

_INCOMPLETE_JOB_STATUSES = ("pending", "running")
_TERMINAL_JOB_STATUSES = {"failed", "succeeded", "cancelled"}
_DEFAULT_ADAPTER_TIMEOUT = timedelta(minutes=5)
_RUNNING_JOB_STALE_AFTER = _DEFAULT_ADAPTER_TIMEOUT * 2
_JOB_CANCELLATION_POLL_INTERVAL_SECONDS = 0.1
_JOB_CANCELLED_ERROR_CODE = ErrorCode.JOB_CANCELLED.value
_ENQUEUE_INGEST_JOB_ERROR_MESSAGE = "Failed to enqueue ingest job"
_FINALIZE_INGEST_JOB_ERROR_MESSAGE = "Failed to finalize ingest job"
_PROCESS_INGEST_JOB_ERROR_MESSAGE = "Ingest job failed unexpectedly."
_INITIAL_INGEST_REVISION_KIND = "ingest"
_REPROCESS_REVISION_KIND = "reprocess"
_SAFE_RUNNER_ERROR_DETAIL_KEYS = (
    "adapter_key",
    "input_family",
    "reason",
    "stage",
    "detected_format",
    "media_type",
)


@dataclass(frozen=True, slots=True)
class _QueuedJobEvent:
    """Buffered job event persisted by the progress drain."""

    level: str
    message: str
    data_json: dict[str, Any]


class _PersistedJobCancellationHandle:
    """Cancellation handle backed by worker polling."""

    def __init__(self) -> None:
        self._cancel_requested = False

    def is_cancelled(self) -> bool:
        return self._cancel_requested

    def mark_cancelled(self) -> None:
        self._cancel_requested = True


class _JobProgressEventBridge:
    """Synchronous progress callback with async DB draining."""

    _STOP = object()

    def __init__(self, job_id: UUID) -> None:
        self._job_id = job_id
        self._queue: asyncio.Queue[_QueuedJobEvent | object] = asyncio.Queue()
        self._drain_task = asyncio.create_task(self._drain())
        self._closed = False

    def callback(self, update: ProgressUpdate) -> None:
        if self._closed:
            raise RuntimeError("Progress callback received update after bridge closed.")

        self._queue.put_nowait(
            _QueuedJobEvent(
                level="info",
                message=update.message or f"Job progress: {update.stage}",
                data_json=_progress_event_data(update),
            )
        )

    async def flush(self) -> None:
        if self._closed:
            await self._drain_task
            return

        self._closed = True
        self._queue.put_nowait(self._STOP)
        await self._drain_task

    async def _drain(self) -> None:
        while True:
            queued = await self._queue.get()
            try:
                if queued is self._STOP:
                    return

                assert isinstance(queued, _QueuedJobEvent)
                await emit_job_event(
                    self._job_id,
                    level=queued.level,
                    message=queued.message,
                    data_json=queued.data_json,
                )
            finally:
                self._queue.task_done()

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


def _resolve_revision_kind(job_id: UUID, *, initial_job_id: UUID | None) -> str:
    """Map file linkage to the correct ingest revision kind."""
    if initial_job_id == job_id:
        return _INITIAL_INGEST_REVISION_KIND

    return _REPROCESS_REVISION_KIND


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


def _build_persisted_validation_report_json(
    payload: IngestFinalizationPayload,
    *,
    drawing_revision_id: UUID,
    source_job_id: UUID,
    validation_report_id: UUID,
) -> dict[str, Any]:
    """Copy the canonical report JSON and enrich it with persisted identities."""
    report_json = deepcopy(payload.report_json)

    validator_json = report_json.get("validator")
    validator = dict(validator_json) if isinstance(validator_json, dict) else {}
    validator["name"] = payload.validator_name
    validator["version"] = payload.validator_version

    confidence = dict(payload.confidence_json)
    confidence["effective_confidence"] = payload.effective_confidence
    confidence["review_state"] = payload.review_state
    confidence["review_required"] = payload.review_state == "review_required"

    summary_json = report_json.get("summary")
    summary = dict(summary_json) if isinstance(summary_json, dict) else {}
    summary["validation_status"] = payload.validation_status
    summary["review_state"] = payload.review_state
    summary["quantity_gate"] = payload.quantity_gate
    summary["effective_confidence"] = payload.effective_confidence

    checks_json = report_json.get("checks")
    checks = list(checks_json) if isinstance(checks_json, list) else []
    if not checks:
        checks.append(
            {
                "code": "validation_report_persisted",
                "status": "passed",
                "message": (
                    "Persisted validation report columns are attached to the "
                    "canonical payload."
                ),
            }
        )

    report_json["validation_report_id"] = str(validation_report_id)
    report_json["drawing_revision_id"] = str(drawing_revision_id)
    report_json["source_job_id"] = str(source_job_id)
    report_json["validation_report_schema_version"] = payload.validation_report_schema_version
    report_json["canonical_entity_schema_version"] = payload.canonical_entity_schema_version
    report_json["validation_status"] = payload.validation_status
    report_json["review_state"] = payload.review_state
    report_json["quantity_gate"] = payload.quantity_gate
    report_json["effective_confidence"] = payload.effective_confidence
    report_json["validator"] = validator
    report_json["confidence"] = confidence
    report_json["provenance"] = deepcopy(payload.provenance_json)
    report_json["generated_at"] = payload.generated_at.isoformat()
    report_json["summary"] = summary
    report_json["checks"] = checks

    return report_json


async def _build_ingestion_run_request(job_id: UUID) -> IngestionRunRequest:
    """Load persisted job and file metadata for the ingestion runner."""
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

        return IngestionRunRequest(
            job_id=job.id,
            file_id=source_file.id,
            checksum_sha256=source_file.checksum_sha256,
            detected_format=source_file.detected_format,
            media_type=source_file.media_type,
            original_name=source_file.original_filename,
            extraction_profile_id=job.extraction_profile_id,
            initial_job_id=source_file.initial_job_id,
        )


async def _finalize_ingest_job(job_id: UUID, *, payload: IngestFinalizationPayload) -> bool:
    """Atomically publish durable ingest outputs and terminal job success."""
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
                report_json=_build_persisted_validation_report_json(
                    payload,
                    drawing_revision_id=drawing_revision_id,
                    source_job_id=job.id,
                    validation_report_id=validation_report_id,
                ),
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


def _progress_event_data(update: ProgressUpdate) -> dict[str, Any]:
    """Build a stable persisted progress event payload."""
    data_json: dict[str, Any] = {
        "status": "running",
        "event": "progress",
        "stage": update.stage,
    }
    if update.message is not None:
        data_json["detail"] = update.message
    if update.completed is not None:
        data_json["completed"] = update.completed
    if update.total is not None:
        data_json["total"] = update.total
    if update.percent is not None:
        data_json["percent"] = update.percent
    return data_json


def _runner_error_log_fields(exc: IngestionRunnerError) -> dict[str, Any]:
    """Return whitelisted structured fields for expected runner failures."""
    data_json: dict[str, Any] = {
        "error_code": exc.error_code.value,
        "failure_kind": exc.failure_kind.value,
        "error_message": exc.message,
    }
    for key in _SAFE_RUNNER_ERROR_DETAIL_KEYS:
        value = exc.details.get(key)
        if value is not None:
            data_json[key] = value
    return data_json


def _runner_supports_keyword(runner: Any, keyword: str) -> bool:
    """Return whether a runner callable accepts a given keyword."""
    signature = inspect.signature(runner)
    parameters = signature.parameters.values()
    if any(parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters):
        return True
    return keyword in signature.parameters


async def _invoke_ingestion_runner(
    request: IngestionRunRequest,
    *,
    timeout: AdapterTimeout,
    cancellation: _PersistedJobCancellationHandle,
    on_progress: Any,
) -> IngestFinalizationPayload:
    """Call the runner while remaining compatible with patched test doubles."""
    kwargs: dict[str, Any] = {}
    if _runner_supports_keyword(run_ingestion, "timeout"):
        kwargs["timeout"] = timeout
    if _runner_supports_keyword(run_ingestion, "cancellation"):
        kwargs["cancellation"] = cancellation
    if _runner_supports_keyword(run_ingestion, "on_progress"):
        kwargs["on_progress"] = on_progress
    return await run_ingestion(request, **kwargs)


async def _poll_job_cancellation(
    job_id: UUID,
    *,
    cancellation: _PersistedJobCancellationHandle,
    run_task: asyncio.Task[IngestFinalizationPayload],
    stop_event: asyncio.Event,
) -> None:
    """Poll persisted cancellation without holding DB locks during execution."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    while not stop_event.is_set() and not cancellation.is_cancelled():
        async with session_maker() as session:
            result = await session.execute(select(Job.cancel_requested).where(Job.id == job_id))
            cancel_requested = result.scalar_one_or_none()

        if cancel_requested is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        if cancel_requested:
            cancellation.mark_cancelled()
            run_task.cancel()
            return

        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=_JOB_CANCELLATION_POLL_INTERVAL_SECONDS,
            )
        except TimeoutError:
            continue


async def _stop_job_execution_monitor(
    *,
    progress_bridge: _JobProgressEventBridge,
    stop_event: asyncio.Event,
    cancellation_task: asyncio.Task[None],
) -> None:
    """Flush queued progress and stop background execution monitors."""
    stop_event.set()
    try:
        await cancellation_task
    finally:
        await progress_bridge.flush()


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


async def _mark_job_cancelled(job_id: UUID) -> None:
    """Persist a cancelled job state."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        job = await _get_job_for_update(session, job_id)
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        if job.status in _TERMINAL_JOB_STATUSES:
            logger.info(
                "ingest_job_cancel_mark_skipped_terminal_status",
                job_id=str(job_id),
                status=job.status,
            )
            return

        _finalize_job_cancelled(job)
        await emit_job_event(
            job_id,
            level="warning",
            message="Job cancelled",
            data_json={"status": "cancelled"},
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
    """Load a persisted ingest job, run ingestion, and persist state transitions."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    if not await _begin_or_resume_ingest_job(job_id):
        return

    try:
        request = await _build_ingestion_run_request(job_id)
        progress_bridge = _JobProgressEventBridge(job_id)
        cancellation = _PersistedJobCancellationHandle()
        stop_event = asyncio.Event()
        run_task = asyncio.create_task(
            _invoke_ingestion_runner(
                request,
                timeout=AdapterTimeout(seconds=_DEFAULT_ADAPTER_TIMEOUT.total_seconds()),
                cancellation=cancellation,
                on_progress=progress_bridge.callback,
            )
        )
        cancellation_task = asyncio.create_task(
            _poll_job_cancellation(
                job_id,
                cancellation=cancellation,
                run_task=run_task,
                stop_event=stop_event,
            )
        )
        try:
            payload = await run_task
        finally:
            await _stop_job_execution_monitor(
                progress_bridge=progress_bridge,
                stop_event=stop_event,
                cancellation_task=cancellation_task,
            )
    except IngestionRunnerError as exc:
        if exc.error_code is ErrorCode.JOB_CANCELLED:
            await _mark_job_cancelled(job_id)
            logger.info(
                "ingest_job_cancelled_during_execution",
                job_id=str(job_id),
                error_code=exc.error_code.value,
            )
        else:
            await _mark_job_failed(job_id, error_message=exc.message, error_code=exc.error_code)
            logger.error("ingest_job_failed", job_id=str(job_id), **_runner_error_log_fields(exc))
        raise
    except asyncio.CancelledError:
        await _mark_job_cancelled(job_id)
        logger.info("ingest_job_cancelled_during_execution", job_id=str(job_id))
        raise
    except Exception:
        await _mark_job_failed(job_id, error_message=_PROCESS_INGEST_JOB_ERROR_MESSAGE)
        logger.error(
            "ingest_job_failed",
            job_id=str(job_id),
            error_code=ErrorCode.INTERNAL_ERROR.value,
            error_message=_PROCESS_INGEST_JOB_ERROR_MESSAGE,
            exc_info=True,
        )
        raise

    try:
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
