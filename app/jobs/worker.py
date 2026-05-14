"""Celery worker application and persisted job handlers."""

import asyncio
import heapq
import inspect
import uuid
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from celery import Celery
from celery.signals import worker_ready
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.errors import ErrorCode
from app.core.logging import get_logger
from app.db.session import get_session_maker
from app.ingestion.contracts import AdapterTimeout, ProgressUpdate
from app.ingestion.debug_overlay import plan_svg_debug_overlay
from app.ingestion.finalization import IngestFinalizationPayload
from app.ingestion.runner import IngestionRunnerError, IngestionRunRequest, run_ingestion
from app.models.adapter_run_output import AdapterRunOutput
from app.models.drawing_revision import DrawingRevision
from app.models.file import File
from app.models.generated_artifact import GeneratedArtifact
from app.models.job import Job, JobType
from app.models.job_event import JobEvent
from app.models.project import Project
from app.models.revision_materialization import (
    RevisionBlock,
    RevisionEntity,
    RevisionEntityManifest,
    RevisionLayer,
    RevisionLayout,
)
from app.models.validation_report import ValidationReport
from app.storage import get_storage
from app.storage.keys import build_generated_artifact_storage_key

logger = get_logger(__name__)

_RECOVERABLE_INGEST_JOB_TYPES = (JobType.INGEST.value, JobType.REPROCESS.value)
_TERMINAL_JOB_STATUSES = {"failed", "succeeded", "cancelled"}
_ENQUEUE_STATUS_PENDING = "pending"
_ENQUEUE_STATUS_PUBLISHING = "publishing"
_ENQUEUE_STATUS_PUBLISHED = "published"
_DEFAULT_ADAPTER_TIMEOUT = timedelta(minutes=5)
_RUNNING_JOB_STALE_AFTER = _DEFAULT_ADAPTER_TIMEOUT * 2
_ENQUEUE_LEASE_DURATION = timedelta(minutes=1)
_JOB_CANCELLATION_POLL_INTERVAL_SECONDS = 0.1
_JOB_CANCELLED_ERROR_CODE = ErrorCode.JOB_CANCELLED.value
_ENQUEUE_INGEST_JOB_ERROR_MESSAGE = "Failed to enqueue ingest job"
_FINALIZE_INGEST_JOB_ERROR_MESSAGE = "Failed to finalize ingest job"
_PROCESS_INGEST_JOB_ERROR_MESSAGE = "Ingest job failed unexpectedly."
_INITIAL_INGEST_REVISION_KIND = "ingest"
_REPROCESS_REVISION_KIND = "reprocess"
_DEBUG_OVERLAY_ARTIFACT_KIND = "debug_overlay"
_DEBUG_OVERLAY_ARTIFACT_FORMAT = "svg"
_DEBUG_OVERLAY_GENERATOR_NAME = "app.ingestion.debug_overlay"
_DEBUG_OVERLAY_GENERATOR_VERSION = "1"
_NORMALIZED_ENTITY_INSERT_CHUNK_SIZE = 500
_CANONICAL_ENTITY_PROVENANCE_ORIGINS = frozenset(
    {
        "source_direct",
        "adapter_normalized",
        "inferred",
        "user_created",
        "agent_proposed",
        "generated_export",
    }
)
_SAFE_RUNNER_ERROR_DETAIL_KEYS = (
    "adapter_key",
    "input_family",
    "reason",
    "stage",
    "detected_format",
    "media_type",
)


class _InactiveSourceError(Exception):
    """Raised when a job source project or file is no longer active."""


class _StaleJobAttemptError(Exception):
    """Raised when a worker attempt no longer owns the job lease."""


@dataclass(frozen=True, slots=True)
class _RevisionConflictError(Exception):
    """Raised when a persisted ingest/reprocess revision base is invalid or stale."""

    message: str
    details: dict[str, Any]

    def __str__(self) -> str:
        return self.message


@dataclass(frozen=True, slots=True)
class _QueuedJobEvent:
    """Buffered job event persisted by the progress drain."""

    level: str
    message: str
    data_json: dict[str, Any]


@dataclass(frozen=True, slots=True)
class _RevisionMaterializationRows:
    """Prepared normalized revision payload rows for DB insertion."""

    counts_json: dict[str, int]
    layouts: list[dict[str, Any]]
    layers: list[dict[str, Any]]
    blocks: list[dict[str, Any]]
    entities: list[dict[str, Any]]


@dataclass(frozen=True, slots=True)
class _JobAttemptLease:
    """Persisted ownership token for a claimed job attempt."""

    token: UUID
    lease_expires_at: datetime


@dataclass(frozen=True, slots=True)
class _EnqueueIntentLease:
    """Persisted ownership token for a claimed durable enqueue intent."""

    token: UUID
    lease_expires_at: datetime


@dataclass(frozen=True, slots=True)
class _JobLockBootstrap:
    """Non-locking job metadata needed to acquire ordered row locks."""

    project_id: UUID
    file_id: UUID


@dataclass(frozen=True, slots=True)
class _LockedJobSource:
    """Project/job/file rows locked in the approved terminal-mutation order."""

    project: Project
    job: Job
    source_file: File | None


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

    def __init__(self, job_id: UUID, *, attempt_token: UUID) -> None:
        self._job_id = job_id
        self._attempt_token = attempt_token
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
                    attempt_token=self._attempt_token,
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


def _clear_job_attempt_lease(job: Job) -> None:
    """Clear persisted ownership fencing for a job attempt."""
    job.attempt_token = None
    job.attempt_lease_expires_at = None


def _clear_enqueue_intent_lease(job: Job) -> None:
    """Clear persisted ownership fencing for a durable enqueue intent."""
    job.enqueue_owner_token = None
    job.enqueue_lease_expires_at = None


def prepare_job_enqueue_intent(job: Job) -> None:
    """Reset a job's durable enqueue intent to the pending outbox state."""
    job.enqueue_status = _ENQUEUE_STATUS_PENDING
    job.enqueue_attempts = 0
    job.enqueue_last_attempted_at = None
    job.enqueue_published_at = None
    _clear_enqueue_intent_lease(job)


def _claim_job_attempt_lease(
    job: Job,
    *,
    now: datetime,
    increment_attempt: bool,
) -> _JobAttemptLease:
    """Mint and persist a fresh job-attempt ownership lease."""
    attempt_token = uuid.uuid4()
    lease_expires_at = now + _RUNNING_JOB_STALE_AFTER

    if increment_attempt:
        job.attempts += 1

    job.status = "running"
    job.started_at = now
    job.finished_at = None
    job.error_code = None
    job.error_message = None
    job.attempt_token = attempt_token
    job.attempt_lease_expires_at = lease_expires_at

    return _JobAttemptLease(token=attempt_token, lease_expires_at=lease_expires_at)


def _job_attempt_is_current(job: Job, *, attempt_token: UUID) -> bool:
    """Return whether a worker still owns the persisted job attempt lease."""
    return job.status == "running" and job.attempt_token == attempt_token


def _job_is_safe_recovery_failure_target(job: Job) -> bool:
    """Return whether recovery can still safely mark the job failed."""
    return (
        job.status == "pending"
        and job.attempt_token is None
        and job.attempt_lease_expires_at is None
        and job.enqueue_status == _ENQUEUE_STATUS_PENDING
        and job.enqueue_owner_token is None
        and job.enqueue_lease_expires_at is None
    )


def _is_stale_running_job(job: Job, *, now: datetime) -> bool:
    """Return whether a running job is old enough to treat as orphaned."""
    lease_expires_at = job.attempt_lease_expires_at
    if lease_expires_at is not None:
        if lease_expires_at.tzinfo is None:
            lease_expires_at = lease_expires_at.replace(tzinfo=UTC)
        return lease_expires_at <= now

    if job.started_at is None:
        return True

    started_at = job.started_at
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=UTC)

    return started_at <= now - _RUNNING_JOB_STALE_AFTER


def _is_stale_enqueue_intent(job: Job, *, now: datetime) -> bool:
    """Return whether an in-flight enqueue publish claim can be reclaimed."""
    lease_expires_at = job.enqueue_lease_expires_at
    if lease_expires_at is None:
        return True
    if lease_expires_at.tzinfo is None:
        lease_expires_at = lease_expires_at.replace(tzinfo=UTC)
    return lease_expires_at <= now


def _claim_enqueue_intent_lease(job: Job, *, now: datetime) -> _EnqueueIntentLease:
    """Mint and persist a fresh ownership lease for broker publication."""
    token = uuid.uuid4()
    lease_expires_at = now + _ENQUEUE_LEASE_DURATION
    job.enqueue_status = _ENQUEUE_STATUS_PUBLISHING
    job.enqueue_attempts += 1
    job.enqueue_owner_token = token
    job.enqueue_lease_expires_at = lease_expires_at
    job.enqueue_last_attempted_at = now
    return _EnqueueIntentLease(token=token, lease_expires_at=lease_expires_at)


async def _get_job_for_update(session: AsyncSession, job_id: UUID) -> Job | None:
    """Load and lock a persisted job row."""
    return await _get_job_for_update_with_metadata(session, job_id)


async def _get_job_lock_bootstrap(
    session: AsyncSession,
    job_id: UUID,
) -> _JobLockBootstrap | None:
    """Load job metadata without taking locks."""
    result = await session.execute(
        select(Job.project_id, Job.file_id).where(Job.id == job_id)
    )
    row = result.one_or_none()
    if row is None:
        return None

    project_id, file_id = row
    return _JobLockBootstrap(project_id=project_id, file_id=file_id)


async def _get_project(
    session: AsyncSession,
    project_id: UUID,
    *,
    for_update: bool = False,
) -> Project | None:
    """Load a persisted project row, optionally under a row lock."""
    statement = select(Project).where(Project.id == project_id)
    if for_update:
        statement = statement.with_for_update(of=Project)

    result = await session.execute(statement)
    return result.scalar_one_or_none()


async def _get_job_for_update_with_metadata(
    session: AsyncSession,
    job_id: UUID,
    *,
    expected_project_id: UUID | None = None,
    expected_file_id: UUID | None = None,
) -> Job | None:
    """Load and lock a persisted job row, revalidating stable metadata."""
    result = await session.execute(
        select(Job)
        .where(Job.id == job_id)
        .with_for_update(of=Job)
    )
    job = result.scalar_one_or_none()
    if job is None:
        return None
    if expected_project_id is not None and job.project_id != expected_project_id:
        return None
    if expected_file_id is not None and job.file_id != expected_file_id:
        return None

    return job


async def _get_source_file(
    session: AsyncSession,
    *,
    project_id: UUID,
    file_id: UUID,
    for_update: bool = False,
) -> File | None:
    """Load a source file row, optionally under a row lock."""
    statement = select(File).where(
        (File.project_id == project_id)
        & (File.id == file_id)
    )
    if for_update:
        statement = statement.with_for_update(of=File)

    result = await session.execute(statement)
    return result.scalar_one_or_none()


async def _lock_job_source_for_terminal_mutation(
    session: AsyncSession,
    job_id: UUID,
) -> _LockedJobSource:
    """Lock project/job/file rows in the approved order for terminal writes."""
    bootstrap = await _get_job_lock_bootstrap(session, job_id)
    if bootstrap is None:
        raise LookupError(f"Job with identifier '{job_id}' not found")

    project = await _get_project(session, bootstrap.project_id, for_update=True)
    if project is None:
        raise LookupError(
            f"Project with identifier '{bootstrap.project_id}' for job '{job_id}' not found"
        )

    job = await _get_job_for_update_with_metadata(
        session,
        job_id,
        expected_project_id=bootstrap.project_id,
        expected_file_id=bootstrap.file_id,
    )
    if job is None:
        raise LookupError(f"Job with identifier '{job_id}' not found")

    source_file = await _get_source_file(
        session,
        project_id=bootstrap.project_id,
        file_id=bootstrap.file_id,
        for_update=True,
    )
    return _LockedJobSource(project=project, job=job, source_file=source_file)


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


async def _get_drawing_revision(
    session: AsyncSession,
    *,
    revision_id: UUID,
) -> DrawingRevision | None:
    """Load a drawing revision row by identifier."""

    return await session.get(DrawingRevision, revision_id)


def _expected_revision_kind_for_job(job: Job) -> str:
    """Return the expected persisted revision kind for a job type."""

    if job.job_type == JobType.INGEST.value:
        return _INITIAL_INGEST_REVISION_KIND
    if job.job_type == JobType.REPROCESS.value:
        return _REPROCESS_REVISION_KIND
    raise ValueError(f"Unsupported ingest job type '{job.job_type}'")


def _assert_job_base_revision_invariants(job: Job) -> None:
    """Reject persisted jobs whose job_type/base_revision_id pairing is invalid."""

    if job.job_type == JobType.INGEST.value:
        if job.base_revision_id is not None:
            raise ValueError("Initial ingest job cannot retain a base revision")
        return

    if job.job_type == JobType.REPROCESS.value:
        if job.base_revision_id is None:
            raise _RevisionConflictError(
                message="Reprocess job is missing its finalized base revision.",
                details={
                    "base_revision_id": None,
                    "base_revision_sequence": None,
                    "current_revision_id": None,
                    "current_revision_sequence": None,
                },
            )
        return

    raise ValueError(f"Unsupported ingest job type '{job.job_type}'")


def _revision_reference(
    revision: DrawingRevision | None,
) -> tuple[str | None, int | None]:
    """Return stable revision identifier/sequence details for conflict payloads."""

    if revision is None:
        return None, None

    return str(revision.id), revision.revision_sequence


def _build_revision_conflict_details(
    *,
    base_revision: DrawingRevision | None,
    current_revision: DrawingRevision | None,
) -> dict[str, str | int | None]:
    """Build structured stale-base details for durable job events."""

    base_revision_id, base_revision_sequence = _revision_reference(base_revision)
    current_revision_id, current_revision_sequence = _revision_reference(current_revision)
    return {
        "base_revision_id": base_revision_id,
        "base_revision_sequence": base_revision_sequence,
        "current_revision_id": current_revision_id,
        "current_revision_sequence": current_revision_sequence,
    }


async def _resolve_finalization_predecessor_revision(
    session: AsyncSession,
    *,
    job: Job,
    source_file: File,
    payload_revision_kind: str,
) -> DrawingRevision | None:
    """Validate job lineage invariants and return the predecessor revision to append to."""

    _assert_job_base_revision_invariants(job)
    expected_revision_kind = _expected_revision_kind_for_job(job)
    if payload_revision_kind != expected_revision_kind:
        raise _RevisionConflictError(
            message="Ingest job revision kind changed before finalization.",
            details={
                "expected_revision_kind": expected_revision_kind,
                "payload_revision_kind": payload_revision_kind,
            },
        )

    current_revision = await _get_latest_drawing_revision(
        session,
        project_id=job.project_id,
        source_file_id=source_file.id,
    )
    if expected_revision_kind == _INITIAL_INGEST_REVISION_KIND:
        if current_revision is not None:
            raise _RevisionConflictError(
                message="Initial ingest cannot finalize after another revision already exists.",
                details=_build_revision_conflict_details(
                    base_revision=None,
                    current_revision=current_revision,
                ),
            )
        return None

    assert job.base_revision_id is not None
    base_revision = await _get_drawing_revision(session, revision_id=job.base_revision_id)
    if base_revision is None:
        raise _RevisionConflictError(
            message="Reprocess job base revision no longer exists.",
            details=_build_revision_conflict_details(
                base_revision=None,
                current_revision=current_revision,
            ),
        )
    if base_revision.project_id != job.project_id or base_revision.source_file_id != source_file.id:
        raise ValueError("Reprocess job base revision does not belong to the source file")
    if current_revision is None or current_revision.id != base_revision.id:
        raise _RevisionConflictError(
            message="Reprocess base revision became stale before finalization.",
            details=_build_revision_conflict_details(
                base_revision=base_revision,
                current_revision=current_revision,
            ),
        )

    return base_revision


async def _get_generated_artifact_for_revision(
    session: AsyncSession,
    *,
    project_id: UUID,
    drawing_revision_id: UUID,
    artifact_kind: str,
) -> GeneratedArtifact | None:
    """Load a committed artifact of a given kind for a drawing revision."""
    result = await session.execute(
        select(GeneratedArtifact)
        .where(
            (GeneratedArtifact.project_id == project_id)
            & (GeneratedArtifact.drawing_revision_id == drawing_revision_id)
            & (GeneratedArtifact.artifact_kind == artifact_kind)
            & (GeneratedArtifact.deleted_at.is_(None))
        )
        .limit(1)
    )
    return result.scalar_one_or_none()


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


def _build_debug_overlay_generator_config(
    *,
    title: str,
    source_label: str,
    review_state: str,
    confidence_score: float,
) -> dict[str, Any]:
    """Build persisted generator settings for the debug overlay artifact."""
    return {
        "title": title,
        "source_label": source_label,
        "review_state": review_state,
        "confidence_score": confidence_score,
    }


def _materialized_payload_json(value: Any) -> dict[str, Any]:
    """Coerce a canonical collection item into a persisted JSON object payload."""
    if isinstance(value, dict):
        return deepcopy(value)

    return {"value": deepcopy(value)}


def _canonical_payload_list(payload: IngestFinalizationPayload, key: str) -> list[Any]:
    """Return a canonical collection list or an empty list when absent."""
    raw_value = payload.canonical_json.get(key)
    return list(raw_value) if isinstance(raw_value, list) else []


def _string_ref(value: Any) -> str | None:
    """Normalize a persisted ref string extracted from canonical payloads."""
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _hash_ref(value: Any) -> str | None:
    """Normalize persisted hash refs to lowercase SHA-256 strings when valid."""
    normalized = _string_ref(value)
    if normalized is None:
        return None

    lowered = normalized.lower()
    return lowered if len(lowered) == 64 else None


def _first_string_ref(*values: Any) -> str | None:
    """Return the first non-empty normalized string from candidate values."""
    for value in values:
        normalized = _string_ref(value)
        if normalized is not None:
            return normalized

    return None


def _first_hash_ref(*values: Any) -> str | None:
    """Return the first valid normalized hash from candidate values."""
    for value in values:
        normalized = _hash_ref(value)
        if normalized is not None:
            return normalized

    return None


def _json_object(value: Any) -> dict[str, Any]:
    """Return a deep-copied JSON object or an empty object."""
    if isinstance(value, dict):
        return deepcopy(value)

    return {}


def _json_array(value: Any) -> list[Any]:
    """Return a deep-copied JSON array-like value or an empty list."""
    if isinstance(value, list):
        return deepcopy(value)
    if isinstance(value, tuple):
        return deepcopy(list(value))

    return []


def _float_value(value: Any) -> float | None:
    """Normalize persisted numeric fields to floats when possible."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)

    return None


def _allocate_unique_ref(
    *,
    candidates: list[Any],
    prefix: str,
    sequence_index: int,
    used_values: set[str],
) -> str:
    """Allocate a deterministic unique ref from preferred candidates or a sequence fallback."""
    for candidate in candidates:
        normalized = _string_ref(candidate)
        if normalized is not None and normalized not in used_values:
            used_values.add(normalized)
            return normalized

    fallback_base = f"{prefix}-{sequence_index:06d}"
    fallback = fallback_base
    suffix = 1
    while fallback in used_values:
        fallback = f"{fallback_base}-{suffix}"
        suffix += 1

    used_values.add(fallback)
    return fallback


def _resolve_collection_ref(
    payload_json: dict[str, Any],
    *,
    explicit_key: str,
    fallback_keys: tuple[str, ...],
    prefix: str,
    sequence_index: int,
    used_values: set[str],
) -> str:
    """Resolve a stable non-null unique collection ref for a materialized row."""
    return _allocate_unique_ref(
        candidates=[
            payload_json.get(explicit_key),
            *[payload_json.get(key) for key in fallback_keys],
        ],
        prefix=prefix,
        sequence_index=sequence_index,
        used_values=used_values,
    )


def _entity_provenance_json(entity_payload_json: dict[str, Any]) -> dict[str, Any]:
    """Return canonical entity provenance JSON from contract or legacy payloads."""
    provenance_json = entity_payload_json.get("provenance_json")
    provenance = provenance_json if isinstance(provenance_json, dict) else None
    if provenance is None:
        legacy_provenance = entity_payload_json.get("provenance")
        provenance = legacy_provenance if isinstance(legacy_provenance, dict) else {}

    origin = _first_string_ref(provenance.get("origin"), entity_payload_json.get("origin"))
    if origin is not None and origin not in _CANONICAL_ENTITY_PROVENANCE_ORIGINS:
        raise ValueError(f"Invalid entity provenance origin '{origin}'")

    extraction_path_value = (
        provenance.get("extraction_path")
        if "extraction_path" in provenance
        else entity_payload_json.get("extraction_path")
    )
    notes_value = (
        provenance.get("notes")
        if "notes" in provenance
        else entity_payload_json.get("notes")
    )
    adapter_json = provenance.get("adapter")
    if isinstance(adapter_json, dict):
        adapter = deepcopy(adapter_json)
    elif adapter_json is None:
        adapter = {}
    else:
        adapter = {"value": deepcopy(adapter_json)}

    return {
        "origin": origin or "adapter_normalized",
        "adapter": adapter,
        "source_ref": _first_string_ref(
            provenance.get("source_ref"),
            entity_payload_json.get("source_ref"),
            provenance.get("source_entity_ref"),
            entity_payload_json.get("source_entity_ref"),
        ),
        "source_identity": _first_string_ref(
            provenance.get("source_identity"),
            entity_payload_json.get("source_identity"),
            provenance.get("source_handle"),
            entity_payload_json.get("source_handle"),
            provenance.get("dxf_handle"),
            entity_payload_json.get("dxf_handle"),
            provenance.get("source_entity_handle"),
            entity_payload_json.get("source_entity_handle"),
            provenance.get("native_handle"),
            entity_payload_json.get("native_handle"),
            provenance.get("source_id"),
            entity_payload_json.get("source_id"),
        ),
        "source_hash": _first_hash_ref(
            provenance.get("source_hash"),
            entity_payload_json.get("source_hash"),
            provenance.get("normalized_source_hash"),
            entity_payload_json.get("normalized_source_hash"),
            provenance.get("record_hash"),
            entity_payload_json.get("record_hash"),
        ),
        "extraction_path": _json_array(extraction_path_value),
        "notes": _json_array(notes_value),
    }


def _resolve_entity_source_identity(entity_payload_json: dict[str, Any]) -> str | None:
    """Resolve the best-effort stable source identity for a materialized entity row."""
    provenance = _entity_provenance_json(entity_payload_json)
    return _string_ref(provenance.get("source_identity"))


def _resolve_entity_source_hash(entity_payload_json: dict[str, Any]) -> str | None:
    """Resolve the best-effort stable source hash for a materialized entity row."""
    provenance = _entity_provenance_json(entity_payload_json)
    return _hash_ref(provenance.get("source_hash"))


def _resolve_entity_ref(
    entity_payload_json: dict[str, Any],
    *,
    explicit_key: str,
    legacy_key: str,
) -> str | None:
    """Resolve a raw entity relationship ref from contract or legacy payloads."""
    explicit_ref = _string_ref(entity_payload_json.get(explicit_key))
    if explicit_ref is not None:
        return explicit_ref

    legacy_ref = _string_ref(entity_payload_json.get(legacy_key))
    if legacy_ref is not None:
        return legacy_ref

    provenance = _entity_provenance_json(entity_payload_json)
    if not provenance:
        return None

    return _string_ref(provenance.get(explicit_key))


def _resolve_entity_parent_ref(entity_payload_json: dict[str, Any]) -> str | None:
    """Resolve a raw parent entity reference from contract or legacy payloads."""
    parent_entity_ref = _string_ref(entity_payload_json.get("parent_entity_ref"))
    if parent_entity_ref is not None:
        return parent_entity_ref

    parent_id = _string_ref(entity_payload_json.get("parent_id"))
    if parent_id is not None:
        return parent_id

    provenance = _entity_provenance_json(entity_payload_json)
    if not provenance:
        return None

    return _string_ref(provenance.get("parent_entity_ref")) or _string_ref(
        provenance.get("parent_source_id")
    )


def _resolve_entity_id(
    entity_payload_json: dict[str, Any],
    *,
    sequence_index: int,
    used_values: set[str],
) -> str:
    """Resolve a stable non-null unique entity id for a materialized row."""
    provenance = _entity_provenance_json(entity_payload_json)
    return _allocate_unique_ref(
        candidates=[
            entity_payload_json.get("entity_id"),
            entity_payload_json.get("id"),
            entity_payload_json.get("source_identity"),
            provenance.get("source_identity"),
            provenance.get("source_ref"),
        ],
        prefix="entity",
        sequence_index=sequence_index,
        used_values=used_values,
    )


def _resolve_entity_type(entity_payload_json: dict[str, Any]) -> str:
    """Resolve a stable non-null entity type from contract or legacy payloads."""
    return (
        _string_ref(entity_payload_json.get("entity_type"))
        or _string_ref(entity_payload_json.get("kind"))
        or "unknown"
    )


def _resolve_entity_schema_version(
    entity_payload_json: dict[str, Any],
    *,
    default_schema_version: str,
) -> str:
    """Resolve the entity schema version from the payload or manifest default."""
    return _string_ref(entity_payload_json.get("entity_schema_version")) or default_schema_version


def _resolve_entity_confidence_score(
    entity_payload_json: dict[str, Any],
    *,
    default_score: float,
) -> float:
    """Resolve the entity confidence score from contract payloads or a payload default."""
    confidence_score = _float_value(entity_payload_json.get("confidence_score"))
    if confidence_score is not None:
        return confidence_score

    for key in ("confidence_json", "confidence"):
        confidence_payload = entity_payload_json.get(key)
        nested_score = _float_value(confidence_payload)
        if nested_score is not None:
            return nested_score

        confidence_json = _json_object(confidence_payload)
        nested_score = _float_value(confidence_json.get("score"))
        if nested_score is not None:
            return nested_score

    return default_score


def _resolve_entity_confidence_json(
    entity_payload_json: dict[str, Any],
    *,
    confidence_score: float,
) -> dict[str, Any]:
    """Resolve the entity confidence payload from contract or legacy payloads."""
    for key in ("confidence_json", "confidence"):
        confidence_payload = entity_payload_json.get(key)
        confidence_json = _json_object(confidence_payload)
        if confidence_json:
            return confidence_json

        numeric_confidence = _float_value(confidence_payload)
        if numeric_confidence is not None:
            return {"score": numeric_confidence}

    return {"score": confidence_score}


def _build_revision_materialization_rows(
    payload: IngestFinalizationPayload,
) -> _RevisionMaterializationRows:
    """Build revision-scoped normalized payload rows from canonical JSON."""
    layouts: list[dict[str, Any]] = []
    used_layout_refs: set[str] = set()
    for index, layout in enumerate(_canonical_payload_list(payload, "layouts")):
        payload_json = _materialized_payload_json(layout)
        layouts.append(
            {
                "id": uuid.uuid4(),
                "sequence_index": index,
                "payload_json": payload_json,
                "layout_ref": _resolve_collection_ref(
                    payload_json,
                    explicit_key="layout_ref",
                    fallback_keys=("name", "ref", "id"),
                    prefix="layout",
                    sequence_index=index,
                    used_values=used_layout_refs,
                ),
            }
        )

    layers: list[dict[str, Any]] = []
    used_layer_refs: set[str] = set()
    for index, layer in enumerate(_canonical_payload_list(payload, "layers")):
        payload_json = _materialized_payload_json(layer)
        layers.append(
            {
                "id": uuid.uuid4(),
                "sequence_index": index,
                "payload_json": payload_json,
                "layer_ref": _resolve_collection_ref(
                    payload_json,
                    explicit_key="layer_ref",
                    fallback_keys=("name", "ref", "id"),
                    prefix="layer",
                    sequence_index=index,
                    used_values=used_layer_refs,
                ),
            }
        )

    blocks: list[dict[str, Any]] = []
    used_block_refs: set[str] = set()
    for index, block in enumerate(_canonical_payload_list(payload, "blocks")):
        payload_json = _materialized_payload_json(block)
        blocks.append(
            {
                "id": uuid.uuid4(),
                "sequence_index": index,
                "payload_json": payload_json,
                "block_ref": _resolve_collection_ref(
                    payload_json,
                    explicit_key="block_ref",
                    fallback_keys=("name", "ref", "id"),
                    prefix="block",
                    sequence_index=index,
                    used_values=used_block_refs,
                ),
            }
        )

    layout_ids_by_ref = {row["layout_ref"]: row["id"] for row in layouts}
    layer_ids_by_ref = {row["layer_ref"]: row["id"] for row in layers}
    block_ids_by_ref = {row["block_ref"]: row["id"] for row in blocks}

    entities: list[dict[str, Any]] = []
    used_entity_ids: set[str] = set()
    for index, entity in enumerate(_canonical_payload_list(payload, "entities")):
        payload_json = _materialized_payload_json(entity)
        entity_id = _resolve_entity_id(
            payload_json,
            sequence_index=index,
            used_values=used_entity_ids,
        )
        entity_type = _resolve_entity_type(payload_json)
        entity_schema_version = _resolve_entity_schema_version(
            payload_json,
            default_schema_version=payload.canonical_entity_schema_version,
        )
        parent_entity_ref = _resolve_entity_parent_ref(payload_json)
        confidence_score = _resolve_entity_confidence_score(
            payload_json,
            default_score=payload.confidence_score,
        )
        confidence_json = _resolve_entity_confidence_json(
            payload_json,
            confidence_score=confidence_score,
        )
        provenance_json = _entity_provenance_json(payload_json)
        layout_ref = _resolve_entity_ref(
            payload_json,
            explicit_key="layout_ref",
            legacy_key="layout",
        )
        layer_ref = _resolve_entity_ref(
            payload_json,
            explicit_key="layer_ref",
            legacy_key="layer",
        )
        block_ref = _resolve_entity_ref(
            payload_json,
            explicit_key="block_ref",
            legacy_key="block",
        )
        entities.append(
            {
                "id": uuid.uuid4(),
                "sequence_index": index,
                "entity_id": entity_id,
                "entity_type": entity_type,
                "entity_schema_version": entity_schema_version,
                "parent_entity_ref": parent_entity_ref,
                "confidence_score": confidence_score,
                "confidence_json": confidence_json,
                "geometry_json": _json_object(
                    payload_json.get("geometry_json")
                    if "geometry_json" in payload_json
                    else payload_json.get("geometry")
                ),
                "properties_json": _json_object(
                    payload_json.get("properties_json")
                    if "properties_json" in payload_json
                    else payload_json.get("properties")
                ),
                "provenance_json": provenance_json,
                "canonical_entity_json": payload_json,
                "layout_ref": layout_ref,
                "layer_ref": layer_ref,
                "block_ref": block_ref,
                "source_identity": _resolve_entity_source_identity(payload_json),
                "source_hash": _resolve_entity_source_hash(payload_json),
                "layout_id": layout_ids_by_ref.get(layout_ref) if layout_ref is not None else None,
                "layer_id": layer_ids_by_ref.get(layer_ref) if layer_ref is not None else None,
                "block_id": block_ids_by_ref.get(block_ref) if block_ref is not None else None,
            }
        )

    entity_row_ids_by_entity_id = {row["entity_id"]: row["id"] for row in entities}
    for row in entities:
        parent_entity_ref = row["parent_entity_ref"]
        row["parent_entity_row_id"] = (
            entity_row_ids_by_entity_id.get(parent_entity_ref)
            if parent_entity_ref is not None
            else None
        )

    counts_json = {
        "layouts": len(layouts),
        "layers": len(layers),
        "blocks": len(blocks),
        "entities": len(entities),
    }
    return _RevisionMaterializationRows(
        counts_json=counts_json,
        layouts=layouts,
        layers=layers,
        blocks=blocks,
        entities=entities,
    )


def _order_revision_entity_insert_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Order entity rows so parent self-FKs insert before children when possible."""
    if len(rows) < 2:
        return rows

    row_by_id = {row["id"]: row for row in rows}
    pending_parent_counts = {row["id"]: 0 for row in rows}
    child_ids_by_parent: dict[UUID, list[UUID]] = {}
    original_order_by_id = {row["id"]: index for index, row in enumerate(rows)}

    for row in rows:
        row_id = row["id"]
        parent_row_id = row.get("parent_entity_row_id")
        if isinstance(parent_row_id, UUID) and parent_row_id in row_by_id:
            pending_parent_counts[row_id] += 1
            child_ids_by_parent.setdefault(parent_row_id, []).append(row_id)

    ready: list[tuple[int, int, UUID]] = []
    for row in rows:
        row_id = row["id"]
        if pending_parent_counts[row_id] == 0:
            heapq.heappush(
                ready,
                (int(row["sequence_index"]), original_order_by_id[row_id], row_id),
            )

    ordered_rows: list[dict[str, Any]] = []
    while ready:
        _, _, row_id = heapq.heappop(ready)
        ordered_rows.append(row_by_id[row_id])
        for child_row_id in child_ids_by_parent.get(row_id, []):
            pending_parent_counts[child_row_id] -= 1
            if pending_parent_counts[child_row_id] == 0:
                child_row = row_by_id[child_row_id]
                heapq.heappush(
                    ready,
                    (
                        int(child_row["sequence_index"]),
                        original_order_by_id[child_row_id],
                        child_row_id,
                    ),
                )

    if len(ordered_rows) == len(rows):
        return ordered_rows

    ordered_row_ids = {row["id"] for row in ordered_rows}
    return [*ordered_rows, *[row for row in rows if row["id"] not in ordered_row_ids]]


async def _bulk_insert_model_rows(
    session: AsyncSession,
    model: Any,
    rows: list[dict[str, Any]],
) -> None:
    """Insert prepared mappings in deterministic chunks."""
    if not rows:
        return

    for start in range(0, len(rows), _NORMALIZED_ENTITY_INSERT_CHUNK_SIZE):
        chunk = rows[start : start + _NORMALIZED_ENTITY_INSERT_CHUNK_SIZE]
        await session.execute(insert(model), chunk)


async def _persist_revision_materialization(
    session: AsyncSession,
    *,
    job: Job,
    source_file: File,
    payload: IngestFinalizationPayload,
    drawing_revision_id: UUID,
    adapter_run_output_id: UUID,
) -> UUID:
    """Persist revision-scoped normalized entity rows and manifest atomically."""
    if job.extraction_profile_id is None:
        raise RuntimeError("Persisted ingest outputs require an extraction profile id.")

    materialization_rows = _build_revision_materialization_rows(payload)
    manifest_id = uuid.uuid4()
    base_row = {
        "project_id": job.project_id,
        "source_file_id": source_file.id,
        "extraction_profile_id": job.extraction_profile_id,
        "source_job_id": job.id,
        "drawing_revision_id": drawing_revision_id,
        "adapter_run_output_id": adapter_run_output_id,
        "canonical_entity_schema_version": payload.canonical_entity_schema_version,
    }

    session.add(
        RevisionEntityManifest(
            id=manifest_id,
            project_id=job.project_id,
            source_file_id=source_file.id,
            extraction_profile_id=job.extraction_profile_id,
            source_job_id=job.id,
            drawing_revision_id=drawing_revision_id,
            adapter_run_output_id=adapter_run_output_id,
            canonical_entity_schema_version=payload.canonical_entity_schema_version,
            counts_json=materialization_rows.counts_json,
        )
    )

    await _bulk_insert_model_rows(
        session,
        RevisionLayout,
        [
            {
                **base_row,
                **row,
            }
            for row in materialization_rows.layouts
        ],
    )
    await _bulk_insert_model_rows(
        session,
        RevisionLayer,
        [
            {
                **base_row,
                **row,
            }
            for row in materialization_rows.layers
        ],
    )
    await _bulk_insert_model_rows(
        session,
        RevisionBlock,
        [
            {
                **base_row,
                **row,
            }
            for row in materialization_rows.blocks
        ],
    )
    await _bulk_insert_model_rows(
        session,
        RevisionEntity,
        _order_revision_entity_insert_rows(
            [
                {
                    **base_row,
                    **row,
                }
                for row in materialization_rows.entities
            ]
        ),
    )

    return manifest_id


def _build_debug_overlay_lineage_json(
    *,
    source_file: File,
    job: Job,
    payload: IngestFinalizationPayload,
    drawing_revision_id: UUID,
    revision_sequence: int,
    predecessor_revision_id: UUID | None,
    adapter_run_output_id: UUID,
) -> dict[str, Any]:
    """Build lineage metadata for a persisted debug overlay artifact."""
    entity_counts_json = payload.canonical_json.get("entity_counts")
    entity_counts = deepcopy(entity_counts_json) if isinstance(entity_counts_json, dict) else {}

    entities_json = payload.canonical_json.get("entities")
    entity_total = len(entities_json) if isinstance(entities_json, list) else None

    options_json = payload.provenance_json.get("options")
    options = deepcopy(options_json) if isinstance(options_json, dict) else {}

    return {
        "source_file": {
            "id": str(source_file.id),
            "original_filename": source_file.original_filename,
            "detected_format": source_file.detected_format,
            "media_type": source_file.media_type,
            "checksum_sha256": source_file.checksum_sha256,
        },
        "job": {
            "id": str(job.id),
            "extraction_profile_id": str(job.extraction_profile_id),
            "attempts": job.attempts,
        },
        "drawing_revision": {
            "id": str(drawing_revision_id),
            "revision_sequence": revision_sequence,
            "revision_kind": payload.revision_kind,
            "predecessor_revision_id": (
                str(predecessor_revision_id) if predecessor_revision_id is not None else None
            ),
        },
        "adapter": {
            "id": str(adapter_run_output_id),
            "key": payload.adapter_key,
            "version": payload.adapter_version,
            "input_family": payload.input_family,
            "result_checksum_sha256": payload.result_checksum_sha256,
        },
        "entities": {
            "schema_version": payload.canonical_entity_schema_version,
            "counts": entity_counts,
            "total": entity_total,
        },
        "options": options,
    }


async def _cleanup_failed_storage_writes(
    storage: Any,
    writes: list[tuple[str, str]],
    *,
    job_id: UUID,
) -> None:
    """Best-effort cleanup for pre-commit immutable storage writes."""
    for key, storage_uri in reversed(writes):
        try:
            await storage.delete_failed_put(key, storage_uri=storage_uri)
        except Exception:
            logger.warning(
                "generated_artifact_cleanup_failed",
                job_id=str(job_id),
                storage_key=key,
                storage_uri=storage_uri,
                exc_info=True,
            )


async def _build_ingestion_run_request(job_id: UUID, *, attempt_token: UUID) -> IngestionRunRequest:
    """Load persisted job and file metadata for the ingestion runner."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        bootstrap = await _get_job_lock_bootstrap(session, job_id)
        if bootstrap is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        project = await _get_project(session, bootstrap.project_id, for_update=True)
        if project is None:
            raise LookupError(
                f"Project with identifier '{bootstrap.project_id}' for job '{job_id}' not found"
            )

        job = await _get_job_for_update_with_metadata(
            session,
            job_id,
            expected_project_id=bootstrap.project_id,
            expected_file_id=bootstrap.file_id,
        )
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")
        if not _job_attempt_is_current(job, attempt_token=attempt_token):
            raise _StaleJobAttemptError(f"Job attempt for '{job_id}' no longer owns the lease")
        _assert_job_base_revision_invariants(job)

        if project.deleted_at is not None:
            cancelled = await _cancel_job_for_inactive_source(
                session,
                job,
                reason="source_deleted",
                attempt_token=attempt_token,
            )
            if not cancelled:
                raise _StaleJobAttemptError(f"Job attempt for '{job_id}' no longer owns the lease")
            raise _InactiveSourceError(
                f"Project with identifier '{job.project_id}' for job '{job_id}' is no longer active"
            )

        source_file = await _get_source_file(
            session,
            project_id=job.project_id,
            file_id=job.file_id,
            for_update=True,
        )
        if source_file is None or source_file.deleted_at is not None:
            cancelled = await _cancel_job_for_inactive_source(
                session,
                job,
                reason="source_deleted",
                attempt_token=attempt_token,
            )
            if not cancelled:
                raise _StaleJobAttemptError(f"Job attempt for '{job_id}' no longer owns the lease")
            raise _InactiveSourceError(
                f"File with identifier '{job.file_id}' for job '{job_id}' is no longer active"
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


async def _finalize_ingest_job(
    job_id: UUID,
    *,
    attempt_token: UUID,
    payload: IngestFinalizationPayload,
) -> bool:
    """Atomically publish durable ingest outputs and terminal job success."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        bootstrap = await _get_job_lock_bootstrap(session, job_id)
        if bootstrap is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        project = await _get_project(session, bootstrap.project_id, for_update=True)
        if project is None:
            raise LookupError(
                f"Project with identifier '{bootstrap.project_id}' for job '{job_id}' not found"
            )

        job = await _get_job_for_update_with_metadata(
            session,
            job_id,
            expected_project_id=bootstrap.project_id,
            expected_file_id=bootstrap.file_id,
        )
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        if job.status in _TERMINAL_JOB_STATUSES:
            logger.info(
                "ingest_job_completion_skipped_terminal_status",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        if not _job_attempt_is_current(job, attempt_token=attempt_token):
            logger.info(
                "ingest_job_completion_skipped_stale_attempt",
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

        if project.deleted_at is not None:
            await _cancel_job_for_inactive_source(
                session,
                job,
                reason="source_deleted",
                attempt_token=attempt_token,
            )
            logger.info("ingest_job_cancelled_inactive_source", job_id=str(job_id))
            return False

        source_file = await _get_source_file(
            session,
            project_id=job.project_id,
            file_id=job.file_id,
            for_update=True,
        )
        if source_file is None or source_file.deleted_at is not None:
            await _cancel_job_for_inactive_source(
                session,
                job,
                reason="source_deleted",
                attempt_token=attempt_token,
            )
            logger.info("ingest_job_cancelled_inactive_source", job_id=str(job_id))
            return False

        predecessor_revision = await _resolve_finalization_predecessor_revision(
            session,
            job=job,
            source_file=source_file,
            payload_revision_kind=payload.revision_kind,
        )
        revision_sequence = 1
        predecessor_revision_id: UUID | None = None
        if predecessor_revision is not None:
            revision_sequence = predecessor_revision.revision_sequence + 1
            predecessor_revision_id = predecessor_revision.id

        predecessor_debug_overlay = None
        if predecessor_revision_id is not None:
            predecessor_debug_overlay = await _get_generated_artifact_for_revision(
                session,
                project_id=job.project_id,
                drawing_revision_id=predecessor_revision_id,
                artifact_kind=_DEBUG_OVERLAY_ARTIFACT_KIND,
            )

        adapter_run_output_id = uuid.uuid4()
        drawing_revision_id = uuid.uuid4()
        validation_report_id = uuid.uuid4()
        revision_entity_manifest_id = uuid.uuid4()
        debug_overlay_artifact_id = uuid.uuid4()
        finished_at = _utcnow()
        overlay_source_label = source_file.original_filename
        overlay_title = f"{overlay_source_label} revision {revision_sequence}"
        overlay_plan = plan_svg_debug_overlay(
            payload.canonical_json,
            title=overlay_title,
            source_label=overlay_source_label,
            review_state=payload.review_state,
            confidence_score=payload.confidence_score,
        )
        overlay_storage_key = build_generated_artifact_storage_key(
            debug_overlay_artifact_id,
            overlay_plan.filename,
        )
        overlay_generator_config = _build_debug_overlay_generator_config(
            title=overlay_title,
            source_label=overlay_source_label,
            review_state=payload.review_state,
            confidence_score=payload.confidence_score,
        )
        overlay_lineage_json = _build_debug_overlay_lineage_json(
            source_file=source_file,
            job=job,
            payload=payload,
            drawing_revision_id=drawing_revision_id,
            revision_sequence=revision_sequence,
            predecessor_revision_id=predecessor_revision_id,
            adapter_run_output_id=adapter_run_output_id,
        )
        storage = get_storage()
        written_storage_objects: list[tuple[str, str]] = []
        commit_started = False

        try:
            stored_overlay = await storage.put(
                overlay_storage_key,
                overlay_plan.payload,
                immutable=True,
            )
            written_storage_objects.append((stored_overlay.key, stored_overlay.storage_uri))

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
            session.add(
                GeneratedArtifact(
                    id=debug_overlay_artifact_id,
                    project_id=job.project_id,
                    source_file_id=source_file.id,
                    job_id=job.id,
                    drawing_revision_id=drawing_revision_id,
                    adapter_run_output_id=adapter_run_output_id,
                    artifact_kind=_DEBUG_OVERLAY_ARTIFACT_KIND,
                    name=overlay_plan.filename,
                    format=_DEBUG_OVERLAY_ARTIFACT_FORMAT,
                    media_type=overlay_plan.media_type,
                    size_bytes=stored_overlay.size_bytes,
                    checksum_sha256=stored_overlay.checksum_sha256,
                    generator_name=_DEBUG_OVERLAY_GENERATOR_NAME,
                    generator_version=_DEBUG_OVERLAY_GENERATOR_VERSION,
                    generator_config_json=overlay_generator_config,
                    storage_key=stored_overlay.key,
                    storage_uri=stored_overlay.storage_uri,
                    lineage_json=overlay_lineage_json,
                    predecessor_artifact_id=(
                        predecessor_debug_overlay.id
                        if predecessor_debug_overlay is not None
                        else None
                    ),
                )
            )
            await session.flush()
            revision_entity_manifest_id = await _persist_revision_materialization(
                session,
                job=job,
                source_file=source_file,
                payload=payload,
                drawing_revision_id=drawing_revision_id,
                adapter_run_output_id=adapter_run_output_id,
            )

            job.status = "succeeded"
            job.finished_at = finished_at
            job.error_code = None
            job.error_message = None
            _clear_job_attempt_lease(job)
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
                    "revision_entity_manifest_id": str(revision_entity_manifest_id),
                    "generated_artifact_id": str(debug_overlay_artifact_id),
                },
                session=session,
            )
            commit_started = True
            await session.commit()
        except asyncio.CancelledError:
            if not commit_started:
                await session.rollback()
                await _cleanup_failed_storage_writes(
                    storage,
                    written_storage_objects,
                    job_id=job_id,
                )
            raise
        except Exception:
            if not commit_started:
                await session.rollback()
                await _cleanup_failed_storage_writes(
                    storage,
                    written_storage_objects,
                    job_id=job_id,
                )
            raise

    return True


async def emit_job_event(
    job_id: UUID,
    *,
    level: str,
    message: str,
    data_json: dict[str, Any] | None = None,
    attempt_token: UUID | None = None,
    session: AsyncSession | None = None,
) -> bool:
    """Persist a job lifecycle event."""
    event = JobEvent(
        job_id=job_id,
        level=level,
        message=message,
        data_json=data_json,
    )
    if session is not None:
        if attempt_token is not None:
            job = await _get_job_for_update(session, job_id)
            if job is None:
                raise LookupError(f"Job with identifier '{job_id}' not found")
            if not _job_attempt_is_current(job, attempt_token=attempt_token):
                return False
        session.add(event)
        return True

    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as managed_session:
        if attempt_token is not None:
            job = await _get_job_for_update(managed_session, job_id)
            if job is None:
                raise LookupError(f"Job with identifier '{job_id}' not found")
            if not _job_attempt_is_current(job, attempt_token=attempt_token):
                return False
        managed_session.add(event)
        await managed_session.commit()

    return True


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
    attempt_token: UUID,
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
            job = await session.get(Job, job_id)

        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        if job.cancel_requested:
            cancellation.mark_cancelled()
            run_task.cancel()
            return

        if job.status == "cancelled":
            cancellation.mark_cancelled()
            run_task.cancel()
            return

        if not _job_attempt_is_current(job, attempt_token=attempt_token):
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


async def _persist_job_failed(
    session: AsyncSession,
    job: Job,
    *,
    error_message: str,
    error_code: ErrorCode,
    error_details: dict[str, Any] | None = None,
) -> None:
    """Persist a failed job state and matching event within an active session."""
    job.status = "failed"
    job.error_code = error_code.value
    job.error_message = error_message
    job.finished_at = _utcnow()
    _clear_job_attempt_lease(job)
    await emit_job_event(
        job.id,
        level="error",
        message="Job failed",
        data_json={
            "status": "failed",
            "error_code": error_code.value,
            "error_message": error_message,
            **({"details": error_details} if error_details is not None else {}),
        },
        session=session,
    )


async def _mark_job_failed(
    job_id: UUID,
    *,
    error_message: str,
    error_code: ErrorCode = ErrorCode.INTERNAL_ERROR,
    attempt_token: UUID | None = None,
    error_details: dict[str, Any] | None = None,
) -> bool:
    """Persist a failed job state with the supplied message."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        locked_source = await _lock_job_source_for_terminal_mutation(session, job_id)
        job = locked_source.job

        if job.status in _TERMINAL_JOB_STATUSES:
            logger.info(
                "ingest_job_failure_mark_skipped_terminal_status",
                job_id=str(job_id),
                status=job.status,
            )
            return False
        if attempt_token is not None and not _job_attempt_is_current(
            job,
            attempt_token=attempt_token,
        ):
            logger.info(
                "ingest_job_failure_mark_skipped_stale_attempt",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        if (
            locked_source.project.deleted_at is not None
            or locked_source.source_file is None
            or locked_source.source_file.deleted_at is not None
        ):
            await _cancel_job_for_inactive_source(
                session,
                job,
                reason="source_deleted",
                attempt_token=attempt_token,
            )
            logger.info(
                "ingest_job_failure_mark_skipped_inactive_source",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        await _persist_job_failed(
            session,
            job,
            error_message=error_message,
            error_code=error_code,
            error_details=error_details,
        )
        await session.commit()

    return True


async def _mark_job_cancelled(job_id: UUID, *, attempt_token: UUID | None = None) -> bool:
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
            return False

        if attempt_token is not None and not _job_attempt_is_current(
            job,
            attempt_token=attempt_token,
        ):
            logger.info(
                "ingest_job_cancel_mark_skipped_stale_attempt",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        _finalize_job_cancelled(job)
        await emit_job_event(
            job_id,
            level="warning",
            message="Job cancelled",
            data_json={"status": "cancelled"},
            session=session,
        )
        await session.commit()

    return True


async def _mark_job_failed_if_recovery_safe(
    job_id: UUID,
    *,
    error_message: str,
    error_code: ErrorCode = ErrorCode.INTERNAL_ERROR,
    error_details: dict[str, Any] | None = None,
) -> bool:
    """Fail a recovered job only if it is still pending and unowned."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        locked_source = await _lock_job_source_for_terminal_mutation(session, job_id)
        job = locked_source.job

        if (
            locked_source.project.deleted_at is not None
            or locked_source.source_file is None
            or locked_source.source_file.deleted_at is not None
        ):
            await _cancel_job_for_inactive_source(
                session,
                job,
                reason="source_deleted",
            )
            logger.info(
                "ingest_job_recovery_enqueue_failure_mark_skipped_inactive_source",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        if not _job_is_safe_recovery_failure_target(job):
            logger.info(
                "ingest_job_recovery_enqueue_failure_mark_skipped_changed_state",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        await _persist_job_failed(
            session,
            job,
            error_message=error_message,
            error_code=error_code,
            error_details=error_details,
        )
        await session.commit()

    return True


async def _claim_job_enqueue_intent(job_id: UUID) -> _EnqueueIntentLease | None:
    """Claim a durable enqueue intent for best-effort or recovery publication."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    now = _utcnow()
    async with session_maker() as session:
        job = await _get_job_for_update(session, job_id)
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        if job.job_type not in _RECOVERABLE_INGEST_JOB_TYPES or job.status != "pending":
            return None

        if job.enqueue_status == _ENQUEUE_STATUS_PUBLISHED:
            return None

        if job.enqueue_status == _ENQUEUE_STATUS_PUBLISHING and not _is_stale_enqueue_intent(
            job,
            now=now,
        ):
            return None

        lease = _claim_enqueue_intent_lease(job, now=now)
        await session.commit()
        return lease


async def _release_job_enqueue_intent(job_id: UUID, *, lease_token: UUID) -> bool:
    """Release a claimed durable enqueue intent after a publish failure."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        job = await _get_job_for_update(session, job_id)
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        if (
            job.enqueue_status != _ENQUEUE_STATUS_PUBLISHING
            or job.enqueue_owner_token != lease_token
        ):
            return False

        if job.status == "pending":
            job.enqueue_status = _ENQUEUE_STATUS_PENDING
            _clear_enqueue_intent_lease(job)
            await session.commit()
            return True

        job.enqueue_status = _ENQUEUE_STATUS_PUBLISHED
        job.enqueue_published_at = _utcnow()
        _clear_enqueue_intent_lease(job)
        await session.commit()
        return True


async def _mark_job_enqueue_published(job_id: UUID, *, lease_token: UUID) -> bool:
    """Finalize a claimed durable enqueue intent after broker publication."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        job = await _get_job_for_update(session, job_id)
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        if (
            job.enqueue_status != _ENQUEUE_STATUS_PUBLISHING
            or job.enqueue_owner_token != lease_token
        ):
            return False

        job.enqueue_status = _ENQUEUE_STATUS_PUBLISHED
        job.enqueue_published_at = _utcnow()
        _clear_enqueue_intent_lease(job)
        await session.commit()
        return True


async def publish_job_enqueue_intent(
    job_id: UUID,
    *,
    recovery: bool = False,
    publisher: Callable[[UUID], None] | None = None,
    suppress_exceptions: bool = False,
) -> bool:
    """Best-effort publish for a durable enqueue intent recorded in Postgres."""
    try:
        lease = await _claim_job_enqueue_intent(job_id)
        if lease is None:
            return False

        publish = publisher or enqueue_ingest_job
        try:
            publish(job_id)
        except Exception:
            await _release_job_enqueue_intent(job_id, lease_token=lease.token)
            if recovery:
                await _mark_recovery_enqueue_failed(job_id)
            else:
                logger.warning(
                    "ingest_job_enqueue_deferred",
                    job_id=str(job_id),
                    recovery_action="worker_start_recovery",
                )
            return False

        await _mark_job_enqueue_published(job_id, lease_token=lease.token)
        return True
    except Exception:
        if not suppress_exceptions:
            raise

        logger.warning(
            "ingest_job_enqueue_deferred",
            job_id=str(job_id),
            recovery_action="worker_start_recovery",
        )
        return False


async def _mark_recovery_enqueue_failed(job_id: UUID) -> bool:
    """Persist and log a sanitized worker-recovery enqueue failure."""
    marked_failed = await _mark_job_failed_if_recovery_safe(
        job_id,
        error_message=_ENQUEUE_INGEST_JOB_ERROR_MESSAGE,
    )
    if not marked_failed:
        return False

    logger.error(
        "ingest_job_recovery_enqueue_failed",
        job_id=str(job_id),
        error_code=ErrorCode.INTERNAL_ERROR.value,
        recovery_action="mark_failed",
    )
    return True


def _finalize_job_cancelled(job: Job) -> None:
    """Apply the persisted cancelled terminal state to a job."""
    job.status = "cancelled"
    job.error_code = _JOB_CANCELLED_ERROR_CODE
    job.error_message = None
    job.finished_at = _utcnow()
    _clear_job_attempt_lease(job)


async def _cancel_job_for_inactive_source(
    session: AsyncSession,
    job: Job,
    *,
    reason: str,
    attempt_token: UUID | None = None,
) -> bool:
    """Persist cancellation when a job source project/file is no longer active."""
    if job.status in _TERMINAL_JOB_STATUSES:
        return False

    if attempt_token is not None and not _job_attempt_is_current(job, attempt_token=attempt_token):
        return False

    job.cancel_requested = True
    _finalize_job_cancelled(job)
    await emit_job_event(
        job.id,
        level="warning",
        message="Job cancelled",
        data_json={"status": "cancelled", "reason": reason},
        session=session,
    )
    await session.commit()

    return True


async def _begin_or_resume_ingest_job(job_id: UUID) -> _JobAttemptLease | None:
    """Claim, resume, or cancel a persisted ingest job under a row lock."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    now = _utcnow()

    async with session_maker() as session:
        bootstrap = await _get_job_lock_bootstrap(session, job_id)
        if bootstrap is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        project = await _get_project(session, bootstrap.project_id, for_update=True)
        if project is None:
            raise LookupError(
                f"Project with identifier '{bootstrap.project_id}' for job '{job_id}' not found"
            )

        job = await _get_job_for_update_with_metadata(
            session,
            job_id,
            expected_project_id=bootstrap.project_id,
            expected_file_id=bootstrap.file_id,
        )
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        if job.status in _TERMINAL_JOB_STATUSES:
            logger.info(
                "ingest_job_cancel_skipped_terminal_status",
                job_id=str(job_id),
                status=job.status,
            )
            return None

        if project.deleted_at is not None:
            await _cancel_job_for_inactive_source(
                session,
                job,
                reason="source_deleted",
            )
            logger.info("ingest_job_cancelled_inactive_source", job_id=str(job_id))
            return None

        source_file = await _get_source_file(
            session,
            project_id=job.project_id,
            file_id=job.file_id,
            for_update=True,
        )
        if source_file is None or source_file.deleted_at is not None:
            await _cancel_job_for_inactive_source(
                session,
                job,
                reason="source_deleted",
            )
            logger.info("ingest_job_cancelled_inactive_source", job_id=str(job_id))
            return None

        if not job.cancel_requested:
            if job.status == "running":
                if _is_stale_running_job(job, now=now):
                    lease = _claim_job_attempt_lease(job, now=now, increment_attempt=True)
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
                    return lease

                logger.info(
                    "ingest_job_duplicate_delivery_skipped_running_attempt",
                    job_id=str(job_id),
                    status=job.status,
                )
                return None

            lease = _claim_job_attempt_lease(job, now=now, increment_attempt=True)
            await emit_job_event(
                job.id,
                level="info",
                message="Job started",
                data_json={"status": "running", "attempts": job.attempts, "reclaimed": False},
                session=session,
            )
            await session.commit()
            return lease

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
    return None


async def process_ingest_job(job_id: UUID) -> None:
    """Load a persisted ingest job, run ingestion, and persist state transitions."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    lease = await _begin_or_resume_ingest_job(job_id)
    if lease is None:
        return

    try:
        request = await _build_ingestion_run_request(job_id, attempt_token=lease.token)
        progress_bridge = _JobProgressEventBridge(job_id, attempt_token=lease.token)
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
                attempt_token=lease.token,
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
    except _InactiveSourceError:
        logger.info("ingest_job_cancelled_inactive_source", job_id=str(job_id))
        return
    except _StaleJobAttemptError:
        logger.info("ingest_job_stale_attempt_skipped", job_id=str(job_id))
        return
    except _RevisionConflictError as exc:
        await _mark_job_failed(
            job_id,
            error_message=exc.message,
            error_code=ErrorCode.REVISION_CONFLICT,
            attempt_token=lease.token,
            error_details=exc.details,
        )
        logger.warning(
            "ingest_job_revision_conflict",
            job_id=str(job_id),
            error_code=ErrorCode.REVISION_CONFLICT.value,
            **exc.details,
        )
        raise
    except IngestionRunnerError as exc:
        if exc.error_code is ErrorCode.JOB_CANCELLED:
            await _mark_job_cancelled(job_id, attempt_token=lease.token)
            logger.info(
                "ingest_job_cancelled_during_execution",
                job_id=str(job_id),
                error_code=exc.error_code.value,
            )
        else:
            await _mark_job_failed(
                job_id,
                error_message=exc.message,
                error_code=exc.error_code,
                attempt_token=lease.token,
            )
            logger.error("ingest_job_failed", job_id=str(job_id), **_runner_error_log_fields(exc))
        raise
    except asyncio.CancelledError:
        await _mark_job_cancelled(job_id, attempt_token=lease.token)
        logger.info("ingest_job_cancelled_during_execution", job_id=str(job_id))
        raise
    except Exception:
        await _mark_job_failed(
            job_id,
            error_message=_PROCESS_INGEST_JOB_ERROR_MESSAGE,
            attempt_token=lease.token,
        )
        logger.error(
            "ingest_job_failed",
            job_id=str(job_id),
            error_code=ErrorCode.INTERNAL_ERROR.value,
            error_message=_PROCESS_INGEST_JOB_ERROR_MESSAGE,
            exc_info=True,
        )
        raise

    try:
        finalized = await _finalize_ingest_job(
            job_id,
            attempt_token=lease.token,
            payload=payload,
        )
    except asyncio.CancelledError:
        await _mark_job_cancelled(job_id, attempt_token=lease.token)
        logger.info("ingest_job_cancelled_during_finalization", job_id=str(job_id))
        raise
    except _RevisionConflictError as exc:
        await _mark_job_failed(
            job_id,
            error_message=exc.message,
            error_code=ErrorCode.REVISION_CONFLICT,
            attempt_token=lease.token,
            error_details=exc.details,
        )
        logger.warning(
            "ingest_job_revision_conflict",
            job_id=str(job_id),
            error_code=ErrorCode.REVISION_CONFLICT.value,
            **exc.details,
        )
        raise
    except Exception as exc:
        await _mark_job_failed(
            job_id,
            error_message=_FINALIZE_INGEST_JOB_ERROR_MESSAGE,
            attempt_token=lease.token,
        )
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
    """Requeue incomplete persisted ingest/reprocess jobs on worker startup."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    now = _utcnow()

    async with session_maker() as session:
        result = await session.execute(
            select(Job)
            .where(
                (Job.job_type.in_(_RECOVERABLE_INGEST_JOB_TYPES))
                & (
                    (Job.status == "running")
                    | (
                        (Job.status == "pending")
                        & (
                            Job.enqueue_status.in_(
                                (_ENQUEUE_STATUS_PENDING, _ENQUEUE_STATUS_PUBLISHING)
                            )
                        )
                    )
                )
            )
            .order_by(Job.created_at.asc(), Job.id.asc())
            .with_for_update(skip_locked=True)
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
                _clear_job_attempt_lease(job)
                prepare_job_enqueue_intent(job)
                recovered_job_ids.append(job.id)
                continue

            if job.enqueue_status == _ENQUEUE_STATUS_PUBLISHING and not _is_stale_enqueue_intent(
                job,
                now=now,
            ):
                continue

            if job.enqueue_status == _ENQUEUE_STATUS_PUBLISHING:
                job.enqueue_status = _ENQUEUE_STATUS_PENDING
                _clear_enqueue_intent_lease(job)

            if job.enqueue_status != _ENQUEUE_STATUS_PENDING:
                continue

            recovered_job_ids.append(job.id)

        await session.commit()

    enqueued_job_ids: list[UUID] = []
    for job_id in recovered_job_ids:
        if await publish_job_enqueue_intent(job_id, recovery=True):
            enqueued_job_ids.append(job_id)

    return enqueued_job_ids


def recover_incomplete_ingest_jobs_on_worker_start(**_: object) -> None:
    """Requeue incomplete ingest/reprocess jobs when a worker starts."""
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
