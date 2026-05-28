"""Celery worker application and persisted job handlers."""

# ruff: noqa: SLF001

import asyncio
import atexit
import hashlib
import heapq
import inspect
import json
import math
import threading
import uuid
from collections.abc import Callable, Coroutine, Sequence
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, cast
from uuid import UUID

from celery import Celery
from celery.signals import worker_ready
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.errors import ErrorCode
from app.core.logging import get_logger
from app.db.session import get_session_maker
from app.estimating.catalog import CatalogFormulaRef, CatalogMaterialRef, CatalogRateRef
from app.estimating.catalog.resolver import resolve_formula, resolve_material, resolve_rate
from app.estimating.catalog.selection import CatalogSelectionError
from app.estimating.engine import formula_definition_from_selected_formula
from app.estimating.engine.contracts import (
    EstimateEngineInput,
    EstimateEngineOutput,
    EstimateFormulaEntryInput,
    EstimateLineInputSpec,
    EstimateMaterialEntryInput,
    EstimateQuantityEntryInput,
    EstimateRateEntryInput,
)
from app.estimating.engine.errors import EstimateEngineError
from app.estimating.engine.service import compose_estimate
from app.estimating.quantities.contracts import (
    GateStatus,
    QuantityEngineResult,
    RevisionEntityInput,
    RevisionGateMetadata,
)
from app.estimating.quantities.engine import compute_quantities
from app.exports.csv import (
    CsvExportResult,
    EstimateCsvExportError,
    QuantityCsvExportError,
    render_estimate_csv_export,
    render_quantity_csv_export,
)
from app.exports.estimate_pdf import (
    EstimatePdfExportError,
    EstimatePdfExportResult,
    render_estimate_pdf_export,
)
from app.exports.revision_json import (
    RevisionJsonExportError,
    RevisionJsonExportResult,
    render_revision_json_export,
)
from app.ingestion.contracts import AdapterTimeout, ProgressUpdate
from app.ingestion.debug_overlay import plan_svg_debug_overlay
from app.ingestion.finalization import IngestFinalizationPayload
from app.ingestion.runner import IngestionRunnerError, IngestionRunRequest, run_ingestion
from app.jobs import lifecycle as job_lifecycle
from app.models.adapter_run_output import AdapterRunOutput
from app.models.drawing_revision import DrawingRevision
from app.models.estimate_job_input import EstimateJobInput, EstimateJobInputCatalogRef
from app.models.estimate_version import EstimateItem, EstimateSnapshotEntry, EstimateVersion
from app.models.export_job_input import ExportJobInput
from app.models.file import File
from app.models.generated_artifact import GeneratedArtifact
from app.models.job import Job, JobType
from app.models.quantity_takeoff import QuantityItem, QuantityItemKind, QuantityTakeoff
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
_RECOVERABLE_ENQUEUE_JOB_TYPES = (
    JobType.INGEST.value,
    JobType.REPROCESS.value,
    JobType.QUANTITY_TAKEOFF.value,
    JobType.ESTIMATE.value,
    JobType.EXPORT.value,
)
_KNOWN_ENQUEUE_JOB_TYPES_WITHOUT_PUBLISHER: frozenset[str] = frozenset()
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
_ENQUEUE_REPROCESS_JOB_ERROR_MESSAGE = "Failed to enqueue reprocess job"
_ENQUEUE_QUANTITY_TAKEOFF_JOB_ERROR_MESSAGE = "Failed to enqueue quantity takeoff job"
_ENQUEUE_ESTIMATE_JOB_ERROR_MESSAGE = "Failed to enqueue estimate job"
_ENQUEUE_EXPORT_JOB_ERROR_MESSAGE = "Failed to enqueue export job"
_FINALIZE_INGEST_JOB_ERROR_MESSAGE = "Failed to finalize ingest job"
_PROCESS_INGEST_JOB_ERROR_MESSAGE = "Ingest job failed unexpectedly."
_FINALIZE_QUANTITY_TAKEOFF_JOB_ERROR_MESSAGE = "Failed to finalize quantity takeoff job"
_FINALIZE_ESTIMATE_JOB_ERROR_MESSAGE = "Failed to finalize estimate job"
_FINALIZE_EXPORT_JOB_ERROR_MESSAGE = "Failed to finalize export job"
_PROCESS_QUANTITY_TAKEOFF_JOB_ERROR_MESSAGE = "Quantity takeoff job failed unexpectedly."
_PROCESS_ESTIMATE_JOB_ERROR_MESSAGE = "Estimate job failed unexpectedly."
_PROCESS_EXPORT_JOB_ERROR_MESSAGE = "Export job failed unexpectedly."
_INITIAL_INGEST_REVISION_KIND = "ingest"
_REPROCESS_REVISION_KIND = "reprocess"
_DEBUG_OVERLAY_ARTIFACT_KIND = "debug_overlay"
_DEBUG_OVERLAY_ARTIFACT_FORMAT = "svg"
_DEBUG_OVERLAY_GENERATOR_NAME = "app.ingestion.debug_overlay"
_DEBUG_OVERLAY_GENERATOR_VERSION = "1"
_NORMALIZED_ENTITY_INSERT_CHUNK_SIZE = 500
_QUANTITY_TAKEOFF_GATE_BLOCKED_ERROR_MESSAGE = (
    "Quantity takeoff is blocked by the revision validation gate."
)
_QUANTITY_TAKEOFF_CONFLICT_ERROR_MESSAGE = (
    "Quantity takeoff detected conflicting contributor inputs."
)
_QUANTITY_TAKEOFF_VALIDATION_REPORT_MISSING_ERROR_MESSAGE = (
    "Quantity takeoff base revision is missing its validation report."
)
_QUANTITY_TAKEOFF_MATERIALIZATION_MISSING_ERROR_MESSAGE = (
    "Quantity takeoff base revision is missing normalized entities."
)
_ESTIMATE_JOB_INPUT_INVALID_ERROR_MESSAGE = "Estimate job input mapping is invalid."
_ESTIMATE_WORKER_MAPPING_VERSION = "estimate-line-v1"
_QUANTITY_CONFLICT_SUMMARY_LIMIT = 5
_QUANTITY_CONFLICT_ENTITY_ID_LIMIT = 10
_QUANTITY_CONFLICT_DETAIL_ITEM_LIMIT = 10
_QUANTITY_CONFLICT_DETAIL_DEPTH_LIMIT = 3
_QUANTITY_CONFLICT_TEXT_LIMIT = 200
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

_WORKER_LOOP_RUNNER: asyncio.Runner | None = None
_WORKER_LOOP_RUNNER_LOCK = threading.Lock()


def is_ingest_worker_job_type(job_type: JobType | str) -> bool:
    """Return whether a job type is published to the ingest worker."""
    normalized_job_type = job_type.value if isinstance(job_type, JobType) else job_type
    return normalized_job_type in _RECOVERABLE_INGEST_JOB_TYPES


def is_recoverable_enqueue_job_type(job_type: JobType | str) -> bool:
    """Return whether a job type participates in durable queue publication/recovery."""
    normalized_job_type = job_type.value if isinstance(job_type, JobType) else job_type
    return normalized_job_type in _RECOVERABLE_ENQUEUE_JOB_TYPES


def get_job_enqueue_publisher(job_type: JobType | str) -> Callable[[UUID], None] | None:
    """Return the queue publisher registered for a persisted worker job type."""
    normalized_job_type = job_type.value if isinstance(job_type, JobType) else job_type
    if normalized_job_type in _KNOWN_ENQUEUE_JOB_TYPES_WITHOUT_PUBLISHER:
        return None

    registry: dict[str, Callable[[UUID], None]] = {
        JobType.INGEST.value: enqueue_ingest_job,
        JobType.REPROCESS.value: enqueue_ingest_job,
        JobType.QUANTITY_TAKEOFF.value: enqueue_quantity_takeoff_job,
        JobType.ESTIMATE.value: enqueue_estimate_job,
        JobType.EXPORT.value: enqueue_export_job,
    }
    return registry.get(normalized_job_type)


_InactiveSourceError = job_lifecycle._InactiveSourceError
_StaleJobAttemptError = job_lifecycle._StaleJobAttemptError


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
class _QuantityTakeoffJobError(Exception):
    """Raised for deterministic quantity takeoff failures."""

    error_code: ErrorCode
    message: str
    details: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.message


@dataclass(frozen=True, slots=True)
class _EstimateJobInputError(Exception):
    """Raised for deterministic estimate input mapping failures."""

    error_code: ErrorCode
    message: str
    details: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.message


@dataclass(frozen=True, slots=True)
class _ExportJobInputError(Exception):
    """Raised for deterministic export job input failures."""

    error_code: ErrorCode
    message: str
    details: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.message


@dataclass(frozen=True, slots=True)
class _ExportExecutionInput:
    """Resolved persisted inputs for a supported export job."""

    drawing_revision_id: UUID
    export_kind: str
    export_format: str
    media_type: str
    artifact_name: str
    options_json: dict[str, Any]
    quantity_takeoff_id: UUID | None = None
    estimate_version_id: UUID | None = None


@dataclass(frozen=True, slots=True)
class _RenderedExportArtifact:
    """Deterministic rendered artifact bytes and metadata."""

    content_bytes: bytes
    checksum_sha256: str
    size_bytes: int
    media_type: str
    generator_name: str
    generator_version: str


_JobAttemptLease = job_lifecycle._JobAttemptLease


@dataclass(frozen=True, slots=True)
class _EnqueueIntentLease:
    """Persisted ownership token for a claimed durable enqueue intent."""

    token: UUID
    lease_expires_at: datetime


@dataclass(frozen=True, slots=True)
class _ClaimedJobEnqueueIntent:
    """Persisted enqueue claim bundled with the routed worker job type."""

    lease: _EnqueueIntentLease
    job_type: str


@dataclass(frozen=True, slots=True)
class _QuantityTakeoffExecutionInput:
    """Loaded immutable quantity takeoff execution inputs."""

    drawing_revision_id: UUID
    review_state: str
    validation_status: str
    quantity_gate: str
    gate: RevisionGateMetadata
    entities: list[RevisionEntityInput]


@dataclass(frozen=True, slots=True)
class _EstimateWorkerQuantityEntry:
    """Deduped quantity entry dependency for estimate worker assembly."""

    entry_key: str
    quantity_item_id: UUID


@dataclass(frozen=True, slots=True)
class _EstimateWorkerLineInput:
    """Normalized worker line assembled from one explicit catalog ref."""

    line_key: str
    line_type: str
    description: str
    ref_type: str
    selection_key: str
    catalog_entry_key: str
    ref_order: int
    catalog_checksum_sha256: str
    rate_catalog_entry_id: UUID | None
    material_catalog_entry_id: UUID | None
    formula_definition_id: UUID | None
    quantity_entry_key: str | None
    quantity_item_id: UUID | None
    formula_inputs: dict[str, Any] | None


@dataclass(frozen=True, slots=True)
class _EstimateWorkerAssemblyInput:
    """Estimate worker-ready line and quantity-entry inputs."""

    lines: list[_EstimateWorkerLineInput]
    quantity_entries: list[_EstimateWorkerQuantityEntry]


_JobLockBootstrap = job_lifecycle._JobLockBootstrap
_LockedJobSource = job_lifecycle._LockedJobSource


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


def _get_worker_loop_runner() -> asyncio.Runner:
    """Return the reusable asyncio runner for sync Celery entrypoints."""
    global _WORKER_LOOP_RUNNER
    if _WORKER_LOOP_RUNNER is None:
        _WORKER_LOOP_RUNNER = asyncio.Runner()
    return _WORKER_LOOP_RUNNER


def _close_worker_loop_runner() -> None:
    """Close and clear the reusable asyncio runner."""
    global _WORKER_LOOP_RUNNER
    with _WORKER_LOOP_RUNNER_LOCK:
        runner = _WORKER_LOOP_RUNNER
        if runner is None:
            return
        runner.close()
        _WORKER_LOOP_RUNNER = None


def _run_worker_loop[WorkerLoopResultT](
    coro_factory: Callable[[], Coroutine[Any, Any, WorkerLoopResultT]],
) -> WorkerLoopResultT:
    """Run a worker coroutine on the process-local reusable event loop."""
    with _WORKER_LOOP_RUNNER_LOCK:
        return _get_worker_loop_runner().run(coro_factory())


atexit.register(_close_worker_loop_runner)


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
    stale_after: timedelta = _RUNNING_JOB_STALE_AFTER,
) -> _JobAttemptLease:
    """Mint and persist a fresh job-attempt ownership lease."""
    return job_lifecycle._claim_job_attempt_lease(
        job,
        now=now,
        increment_attempt=increment_attempt,
        stale_after=stale_after,
    )


_job_attempt_is_current = job_lifecycle._job_attempt_is_current


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


def _is_stale_running_job(
    job: Job,
    *,
    now: datetime,
    stale_after: timedelta = _RUNNING_JOB_STALE_AFTER,
) -> bool:
    """Return whether a running job is old enough to treat as orphaned."""
    return job_lifecycle._is_stale_running_job(
        job,
        now=now,
        stale_after=stale_after,
    )


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


_get_job_for_update = job_lifecycle._get_job_for_update
_get_job_lock_bootstrap = job_lifecycle._get_job_lock_bootstrap
_get_project = job_lifecycle._get_project
_get_job_for_update_with_metadata = job_lifecycle._get_job_for_update_with_metadata
_get_source_file = job_lifecycle._get_source_file


async def _lock_job_source_for_terminal_mutation(
    session: AsyncSession,
    job_id: UUID,
) -> _LockedJobSource:
    """Lock project/job/file rows in the approved order for terminal writes."""
    return await job_lifecycle._lock_job_source_for_terminal_mutation(
        session,
        job_id,
        get_job_lock_bootstrap_func=_get_job_lock_bootstrap,
        get_project_func=_get_project,
        get_job_for_update_with_metadata_func=_get_job_for_update_with_metadata,
        get_source_file_func=_get_source_file,
    )


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


async def _get_existing_quantity_takeoff(
    session: AsyncSession,
    *,
    source_job_id: UUID,
) -> QuantityTakeoff | None:
    """Load an existing committed quantity takeoff for a job."""
    result = await session.execute(
        select(QuantityTakeoff).where(QuantityTakeoff.source_job_id == source_job_id)
    )
    return result.scalar_one_or_none()


async def _get_existing_estimate_version(
    session: AsyncSession,
    *,
    source_job_id: UUID,
) -> EstimateVersion | None:
    """Load an existing committed estimate version for a job."""
    result = await session.execute(
        select(EstimateVersion).where(EstimateVersion.source_job_id == source_job_id)
    )
    return result.scalar_one_or_none()


async def _get_existing_generated_artifact(
    session: AsyncSession,
    *,
    source_job_id: UUID,
) -> GeneratedArtifact | None:
    """Load an existing committed generated artifact for a job."""
    result = await session.execute(
        select(GeneratedArtifact)
        .where(
            (GeneratedArtifact.job_id == source_job_id) & (GeneratedArtifact.deleted_at.is_(None))
        )
        .order_by(GeneratedArtifact.created_at.asc(), GeneratedArtifact.id.asc())
        .limit(1)
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


async def _get_validation_report_for_revision(
    session: AsyncSession,
    *,
    project_id: UUID,
    drawing_revision_id: UUID,
) -> ValidationReport | None:
    """Load the canonical validation report for a drawing revision."""
    result = await session.execute(
        select(ValidationReport).where(
            (ValidationReport.project_id == project_id)
            & (ValidationReport.drawing_revision_id == drawing_revision_id)
        )
    )
    return result.scalar_one_or_none()


async def _get_revision_entity_manifest_for_revision(
    session: AsyncSession,
    *,
    project_id: UUID,
    source_file_id: UUID,
    drawing_revision_id: UUID,
) -> RevisionEntityManifest | None:
    """Load the revision entity manifest for a drawing revision."""
    result = await session.execute(
        select(RevisionEntityManifest).where(
            (RevisionEntityManifest.project_id == project_id)
            & (RevisionEntityManifest.source_file_id == source_file_id)
            & (RevisionEntityManifest.drawing_revision_id == drawing_revision_id)
        )
    )
    return result.scalar_one_or_none()


async def _get_revision_entities_for_revision(
    session: AsyncSession,
    *,
    project_id: UUID,
    source_file_id: UUID,
    drawing_revision_id: UUID,
) -> list[RevisionEntity]:
    """Load deterministic materialized entities for a drawing revision."""
    result = await session.execute(
        select(RevisionEntity)
        .where(
            (RevisionEntity.project_id == project_id)
            & (RevisionEntity.source_file_id == source_file_id)
            & (RevisionEntity.drawing_revision_id == drawing_revision_id)
        )
        .order_by(RevisionEntity.sequence_index.asc(), RevisionEntity.id.asc())
    )
    return list(result.scalars().all())


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
                    "Persisted validation report columns are attached to the canonical payload."
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


def _build_estimate_job_input_error(
    reason: str,
    *,
    ref_index: int | None = None,
    ref_type: str | None = None,
    selection_key: str | None = None,
    line_key: str | None = None,
    extra_details: dict[str, Any] | None = None,
) -> _EstimateJobInputError:
    """Build a sanitized deterministic estimate input mapping error."""
    details: dict[str, Any] = {"reason": reason}
    if ref_index is not None:
        details["ref_index"] = ref_index
    if ref_type is not None:
        details["ref_type"] = ref_type
    if selection_key is not None:
        details["selection_key"] = selection_key
    if line_key is not None:
        details["line_key"] = line_key
    if extra_details:
        details.update(extra_details)
    return _EstimateJobInputError(
        error_code=ErrorCode.INPUT_INVALID,
        message=_ESTIMATE_JOB_INPUT_INVALID_ERROR_MESSAGE,
        details=details,
    )


def _estimate_mapping_uuid(
    value: Any,
    *,
    reason: str,
    ref_index: int,
    ref_type: str,
    selection_key: str,
    line_key: str,
    field_name: str,
) -> UUID:
    """Parse one required UUID field from estimate mapping context."""
    normalized = _string_ref(value)
    if normalized is None:
        raise _build_estimate_job_input_error(
            reason,
            ref_index=ref_index,
            ref_type=ref_type,
            selection_key=selection_key,
            line_key=line_key,
            extra_details={"field": field_name},
        )
    try:
        return UUID(normalized)
    except ValueError as exc:
        raise _build_estimate_job_input_error(
            reason,
            ref_index=ref_index,
            ref_type=ref_type,
            selection_key=selection_key,
            line_key=line_key,
            extra_details={"field": field_name},
        ) from exc


def _build_estimate_worker_mapping_v1(
    catalog_refs: Sequence[Any],
) -> _EstimateWorkerAssemblyInput:
    """Assemble deterministic estimate worker mapping inputs from catalog refs."""
    pending_lines: list[_EstimateWorkerLineInput] = []
    quantity_entries_by_key: dict[str, _EstimateWorkerQuantityEntry] = {}
    seen_line_keys: set[str] = set()

    for ref_index, catalog_ref in enumerate(catalog_refs):
        ref_type = _string_ref(getattr(catalog_ref, "ref_type", None))
        selection_key = _string_ref(getattr(catalog_ref, "selection_key", None))
        context = _json_object(getattr(catalog_ref, "selection_context_json", None))

        if context.get("worker_mapping_version") != _ESTIMATE_WORKER_MAPPING_VERSION:
            raise _build_estimate_job_input_error(
                "missing_worker_mapping_version",
                ref_index=ref_index,
                ref_type=ref_type,
                selection_key=selection_key,
                extra_details={
                    "expected_worker_mapping_version": _ESTIMATE_WORKER_MAPPING_VERSION,
                },
            )

        line_key = _string_ref(context.get("line_key"))
        line_type = _string_ref(context.get("line_type"))
        description = _string_ref(context.get("description"))
        if (
            line_key is None
            or line_type is None
            or description is None
            or ref_type is None
            or selection_key is None
        ):
            raise _build_estimate_job_input_error(
                "missing_required_mapping_field",
                ref_index=ref_index,
                ref_type=ref_type,
                selection_key=selection_key,
                line_key=line_key,
            )

        if line_type != ref_type:
            raise _build_estimate_job_input_error(
                "mismatched_line_ref_type",
                ref_index=ref_index,
                ref_type=ref_type,
                selection_key=selection_key,
                line_key=line_key,
                extra_details={"line_type": line_type},
            )

        raw_ref_order = getattr(catalog_ref, "ref_order", None)
        if isinstance(raw_ref_order, bool) or not isinstance(raw_ref_order, int):
            raise _build_estimate_job_input_error(
                "invalid_ref_order",
                ref_index=ref_index,
                ref_type=ref_type,
                selection_key=selection_key,
                line_key=line_key,
            )

        if line_key in seen_line_keys:
            raise _build_estimate_job_input_error(
                "duplicate_line_key",
                ref_index=ref_index,
                ref_type=ref_type,
                selection_key=selection_key,
                line_key=line_key,
            )
        seen_line_keys.add(line_key)

        quantity_entry_key = _string_ref(context.get("quantity_entry_key"))
        catalog_checksum_sha256 = _hash_ref(getattr(catalog_ref, "catalog_checksum_sha256", None))
        if catalog_checksum_sha256 is None:
            raise _build_estimate_job_input_error(
                "invalid_catalog_checksum",
                ref_index=ref_index,
                ref_type=ref_type,
                selection_key=selection_key,
                line_key=line_key,
            )
        catalog_entry_key = _first_string_ref(
            context.get("catalog_entry_key"),
            f"{ref_type}:{selection_key}",
        )
        assert catalog_entry_key is not None
        rate_catalog_entry_id: UUID | None = None
        material_catalog_entry_id: UUID | None = None
        formula_definition_id: UUID | None = None
        quantity_item_id: UUID | None = None
        formula_inputs: dict[str, Any] | None = None

        if ref_type in {"rate", "material"}:
            quantity_item_id = _estimate_mapping_uuid(
                context.get("quantity_item_id"),
                reason="missing_quantity_item_id",
                ref_index=ref_index,
                ref_type=ref_type,
                selection_key=selection_key,
                line_key=line_key,
                field_name="quantity_item_id",
            )
            if quantity_entry_key is None:
                quantity_entry_key = f"quantity:{quantity_item_id}"
            existing_quantity_entry = quantity_entries_by_key.get(quantity_entry_key)
            if existing_quantity_entry is None:
                quantity_entries_by_key[quantity_entry_key] = _EstimateWorkerQuantityEntry(
                    entry_key=quantity_entry_key,
                    quantity_item_id=quantity_item_id,
                )
            elif existing_quantity_entry.quantity_item_id != quantity_item_id:
                raise _build_estimate_job_input_error(
                    "mismatched_quantity_entry",
                    ref_index=ref_index,
                    ref_type=ref_type,
                    selection_key=selection_key,
                    line_key=line_key,
                    extra_details={"quantity_entry_key": quantity_entry_key},
                )
            if ref_type == "rate":
                rate_catalog_entry_id = _estimate_mapping_uuid(
                    getattr(catalog_ref, "rate_catalog_entry_id", None),
                    reason="missing_catalog_entry_id",
                    ref_index=ref_index,
                    ref_type=ref_type,
                    selection_key=selection_key,
                    line_key=line_key,
                    field_name="rate_catalog_entry_id",
                )
            else:
                material_catalog_entry_id = _estimate_mapping_uuid(
                    getattr(catalog_ref, "material_catalog_entry_id", None),
                    reason="missing_catalog_entry_id",
                    ref_index=ref_index,
                    ref_type=ref_type,
                    selection_key=selection_key,
                    line_key=line_key,
                    field_name="material_catalog_entry_id",
                )
        elif ref_type == "formula":
            if not isinstance(context.get("formula_inputs"), dict):
                raise _build_estimate_job_input_error(
                    "missing_formula_inputs",
                    ref_index=ref_index,
                    ref_type=ref_type,
                    selection_key=selection_key,
                    line_key=line_key,
                )
            formula_inputs = _json_object(context.get("formula_inputs"))
            formula_definition_id = _estimate_mapping_uuid(
                getattr(catalog_ref, "formula_definition_id", None),
                reason="missing_catalog_entry_id",
                ref_index=ref_index,
                ref_type=ref_type,
                selection_key=selection_key,
                line_key=line_key,
                field_name="formula_definition_id",
            )
        else:
            raise _build_estimate_job_input_error(
                "unsupported_ref_type",
                ref_index=ref_index,
                ref_type=ref_type,
                selection_key=selection_key,
                line_key=line_key,
            )

        pending_lines.append(
            _EstimateWorkerLineInput(
                line_key=line_key,
                line_type=line_type,
                description=description,
                ref_type=ref_type,
                selection_key=selection_key,
                catalog_entry_key=catalog_entry_key,
                ref_order=raw_ref_order,
                catalog_checksum_sha256=catalog_checksum_sha256,
                rate_catalog_entry_id=rate_catalog_entry_id,
                material_catalog_entry_id=material_catalog_entry_id,
                formula_definition_id=formula_definition_id,
                quantity_entry_key=quantity_entry_key,
                quantity_item_id=quantity_item_id,
                formula_inputs=formula_inputs,
            )
        )

    lines = sorted(
        pending_lines,
        key=lambda line: (line.ref_order, line.ref_type, line.selection_key),
    )
    quantity_entries: list[_EstimateWorkerQuantityEntry] = []
    emitted_quantity_entry_keys: set[str] = set()
    for line in lines:
        if (
            line.quantity_entry_key is None
            or line.quantity_entry_key in emitted_quantity_entry_keys
        ):
            continue
        quantity_entries.append(quantity_entries_by_key[line.quantity_entry_key])
        emitted_quantity_entry_keys.add(line.quantity_entry_key)

    return _EstimateWorkerAssemblyInput(lines=lines, quantity_entries=quantity_entries)


def _estimate_input_checksum(payload: dict[str, Any]) -> str:
    """Build a deterministic checksum for worker-synthesized estimate inputs."""
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _estimate_decimal(
    value: Any,
    *,
    reason: str,
    extra_details: dict[str, Any] | None = None,
) -> Decimal:
    """Parse one persisted estimate numeric input as a finite Decimal."""
    if isinstance(value, bool):
        raise _build_estimate_job_input_error(reason, extra_details=extra_details)
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise _build_estimate_job_input_error(reason, extra_details=extra_details) from exc
    if not decimal_value.is_finite():
        raise _build_estimate_job_input_error(reason, extra_details=extra_details)
    return decimal_value


def _estimate_tax_rate(assumptions_json: dict[str, Any]) -> Decimal:
    """Load the persisted estimate tax rate with a default of zero."""
    raw_tax_rate = assumptions_json.get("tax_rate", 0)
    tax_rate = _estimate_decimal(
        raw_tax_rate,
        reason="invalid_tax_rate",
        extra_details={"field": "tax_rate"},
    )
    if tax_rate < 0:
        raise _build_estimate_job_input_error(
            "invalid_tax_rate",
            extra_details={"field": "tax_rate"},
        )
    return tax_rate


def _estimate_formula_binding_tokens(
    raw_bindings: dict[str, Any],
    *,
    line: _EstimateWorkerLineInput,
    declared_input_names: tuple[str, ...],
) -> dict[str, str]:
    """Normalize persisted formula-binding payloads into declared-input tokens."""
    bindings_payload = raw_bindings.get("bindings")
    if isinstance(bindings_payload, dict):
        if not all(
            isinstance(key, str) and key and isinstance(value, str) and value
            for key, value in bindings_payload.items()
        ):
            raise _build_estimate_job_input_error(
                "invalid_formula_input_binding",
                ref_type=line.ref_type,
                selection_key=line.selection_key,
                line_key=line.line_key,
            )
        return cast(dict[str, str], dict(bindings_payload))

    if raw_bindings and all(
        isinstance(key, str) and key and isinstance(value, str) and value
        for key, value in raw_bindings.items()
    ):
        return cast(dict[str, str], dict(raw_bindings))

    operand_line_keys = raw_bindings.get("operand_line_keys")
    if isinstance(operand_line_keys, list) and len(operand_line_keys) == len(declared_input_names):
        if not all(isinstance(value, str) and value for value in operand_line_keys):
            raise _build_estimate_job_input_error(
                "invalid_formula_input_binding",
                ref_type=line.ref_type,
                selection_key=line.selection_key,
                line_key=line.line_key,
            )
        return dict(zip(declared_input_names, operand_line_keys, strict=True))

    raise _build_estimate_job_input_error(
        "invalid_formula_input_binding",
        ref_type=line.ref_type,
        selection_key=line.selection_key,
        line_key=line.line_key,
    )


def _resolve_formula_binding_snapshot_key(
    token: str,
    *,
    line: _EstimateWorkerLineInput,
    contract_kind: str,
    lines_by_key: dict[str, _EstimateWorkerLineInput],
    quantity_entry_keys: set[str],
    rate_entry_keys: set[str],
    material_entry_keys: set[str],
) -> str:
    """Resolve one persisted formula-binding token into an engine snapshot entry key."""
    if contract_kind == "quantity" and token in quantity_entry_keys:
        return token
    if contract_kind == "rate" and token in rate_entry_keys | material_entry_keys:
        return token

    bound_line = lines_by_key.get(token)
    if bound_line is None:
        raise _build_estimate_job_input_error(
            "invalid_formula_input_binding",
            ref_type=line.ref_type,
            selection_key=line.selection_key,
            line_key=line.line_key,
            extra_details={"binding": token, "contract_kind": contract_kind},
        )

    if contract_kind == "quantity" and bound_line.quantity_entry_key is not None:
        return bound_line.quantity_entry_key
    if contract_kind == "rate":
        if bound_line.rate_catalog_entry_id is not None:
            return bound_line.catalog_entry_key
        if bound_line.material_catalog_entry_id is not None:
            return bound_line.catalog_entry_key

    raise _build_estimate_job_input_error(
        "invalid_formula_input_binding",
        ref_type=line.ref_type,
        selection_key=line.selection_key,
        line_key=line.line_key,
        extra_details={"binding": token, "contract_kind": contract_kind},
    )


async def _build_estimate_engine_input(
    job_id: UUID,
    *,
    attempt_token: UUID,
) -> EstimateEngineInput:
    """Load deterministic engine inputs for a claimed persisted estimate job."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        job = await session.get(Job, job_id)
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")
        if not _job_attempt_is_current(job, attempt_token=attempt_token):
            raise _StaleJobAttemptError(f"Job attempt for '{job_id}' no longer owns the lease")
        if job.job_type != JobType.ESTIMATE.value:
            raise ValueError(f"Unsupported estimate job type '{job.job_type}'")
        if job.base_revision_id is None:
            raise _RevisionConflictError(
                message="Estimate job is missing its finalized base revision.",
                details={
                    "base_revision_id": None,
                    "current_revision_id": None,
                },
            )

        estimate_input = await session.get(EstimateJobInput, job.id)
        if estimate_input is None:
            raise _build_estimate_job_input_error("missing_estimate_job_input")
        if (
            estimate_input.project_id != job.project_id
            or estimate_input.source_file_id != job.file_id
            or estimate_input.drawing_revision_id != job.base_revision_id
        ):
            raise _build_estimate_job_input_error(
                "estimate_input_lineage_mismatch",
                extra_details={
                    "drawing_revision_id": str(estimate_input.drawing_revision_id),
                    "base_revision_id": str(job.base_revision_id),
                },
            )

        quantity_takeoff = await session.get(QuantityTakeoff, estimate_input.quantity_takeoff_id)
        if quantity_takeoff is None:
            raise _build_estimate_job_input_error(
                "missing_quantity_takeoff",
                extra_details={"quantity_takeoff_id": str(estimate_input.quantity_takeoff_id)},
            )
        if (
            quantity_takeoff.project_id != job.project_id
            or quantity_takeoff.source_file_id != job.file_id
            or quantity_takeoff.drawing_revision_id != job.base_revision_id
            or quantity_takeoff.id != estimate_input.quantity_takeoff_id
            or quantity_takeoff.quantity_gate != estimate_input.quantity_gate
            or quantity_takeoff.trusted_totals is not estimate_input.trusted_totals
            or quantity_takeoff.source_job_type != JobType.QUANTITY_TAKEOFF.value
            or estimate_input.source_job_type != JobType.ESTIMATE.value
        ):
            raise _build_estimate_job_input_error(
                "quantity_takeoff_lineage_mismatch",
                extra_details={
                    "quantity_takeoff_id": str(quantity_takeoff.id),
                    "drawing_revision_id": str(quantity_takeoff.drawing_revision_id),
                },
            )

        catalog_refs_result = await session.execute(
            select(EstimateJobInputCatalogRef)
            .where(EstimateJobInputCatalogRef.estimate_job_id == job.id)
            .order_by(
                EstimateJobInputCatalogRef.ref_order.asc(),
                EstimateJobInputCatalogRef.ref_type.asc(),
                EstimateJobInputCatalogRef.selection_key.asc(),
            )
        )
        catalog_refs = list(catalog_refs_result.scalars().all())
        assembly = _build_estimate_worker_mapping_v1(catalog_refs)

        quantity_items_result = await session.execute(
            select(QuantityItem)
            .where(QuantityItem.quantity_takeoff_id == quantity_takeoff.id)
            .order_by(QuantityItem.created_at.asc(), QuantityItem.id.asc())
        )
        quantity_items = list(quantity_items_result.scalars().all())
        quantity_items_by_id = {item.id: item for item in quantity_items}

        next_snapshot_sort_order = 1

        def _next_snapshot_sort_order() -> int:
            nonlocal next_snapshot_sort_order
            sort_order = next_snapshot_sort_order
            next_snapshot_sort_order += 1
            return sort_order

        quantity_entries: list[EstimateQuantityEntryInput] = []
        for quantity_dependency in assembly.quantity_entries:
            quantity_item = quantity_items_by_id.get(quantity_dependency.quantity_item_id)
            if quantity_item is None:
                raise _build_estimate_job_input_error(
                    "missing_quantity_item",
                    extra_details={
                        "quantity_item_id": str(quantity_dependency.quantity_item_id),
                        "quantity_entry_key": quantity_dependency.entry_key,
                    },
                )
            if quantity_item.item_kind in {
                QuantityItemKind.EXCLUSION.value,
                QuantityItemKind.CONFLICT.value,
            }:
                raise _build_estimate_job_input_error(
                    "invalid_quantity_item_kind",
                    extra_details={
                        "quantity_item_id": str(quantity_item.id),
                        "item_kind": quantity_item.item_kind,
                    },
                )
            if quantity_item.quantity_gate != "allowed":
                raise _build_estimate_job_input_error(
                    "quantity_item_gate_not_allowed",
                    extra_details={
                        "quantity_item_id": str(quantity_item.id),
                        "quantity_gate": quantity_item.quantity_gate,
                    },
                )
            if quantity_item.value is None or not math.isfinite(quantity_item.value):
                raise _build_estimate_job_input_error(
                    "invalid_quantity_value",
                    extra_details={"quantity_item_id": str(quantity_item.id)},
                )

            quantity_payload = {
                "quantity_takeoff_id": str(quantity_takeoff.id),
                "quantity_item_id": str(quantity_item.id),
                "item_kind": quantity_item.item_kind,
                "quantity_type": quantity_item.quantity_type,
                "value": quantity_item.value,
                "unit": quantity_item.unit,
                "review_state": quantity_item.review_state,
                "validation_status": quantity_item.validation_status,
                "quantity_gate": quantity_item.quantity_gate,
                "source_entity_id": quantity_item.source_entity_id,
                "excluded_source_entity_ids_json": deepcopy(
                    quantity_item.excluded_source_entity_ids_json
                ),
            }
            quantity_entries.append(
                EstimateQuantityEntryInput(
                    entry_key=quantity_dependency.entry_key,
                    entry_label=quantity_item.quantity_type,
                    sort_order=_next_snapshot_sort_order(),
                    source_quantity_item_id=quantity_item.id,
                    source_checksum_sha256=_estimate_input_checksum(quantity_payload),
                    quantity_value=Decimal(str(quantity_item.value)),
                    unit=quantity_item.unit,
                    source_quantity_takeoff_id=quantity_takeoff.id,
                    source_payload=quantity_payload,
                )
            )

        lines_by_key = {line.line_key: line for line in assembly.lines}
        quantity_entry_keys = {entry.entry_key for entry in quantity_entries}
        rate_entry_keys = {
            line.catalog_entry_key
            for line in assembly.lines
            if line.rate_catalog_entry_id is not None
        }
        material_entry_keys = {
            line.catalog_entry_key
            for line in assembly.lines
            if line.material_catalog_entry_id is not None
        }
        rate_entries: list[EstimateRateEntryInput] = []
        material_entries: list[EstimateMaterialEntryInput] = []
        formula_entries: list[EstimateFormulaEntryInput] = []
        line_inputs: list[EstimateLineInputSpec] = []
        emitted_catalog_entry_sources: dict[str, tuple[str, UUID, str]] = {}

        def _register_catalog_entry_source(
            *,
            line: _EstimateWorkerLineInput,
            source_id: UUID,
            source_checksum_sha256: str,
        ) -> bool:
            candidate = (line.ref_type, source_id, source_checksum_sha256)
            existing = emitted_catalog_entry_sources.get(line.catalog_entry_key)
            if existing is None:
                emitted_catalog_entry_sources[line.catalog_entry_key] = candidate
                return True
            if existing != candidate:
                existing_ref_type, existing_source_id, existing_checksum = existing
                raise _build_estimate_job_input_error(
                    "colliding_catalog_entry_key",
                    ref_type=line.ref_type,
                    selection_key=line.selection_key,
                    line_key=line.line_key,
                    extra_details={
                        "catalog_entry_key": line.catalog_entry_key,
                        "existing_ref_type": existing_ref_type,
                        "existing_source_id": str(existing_source_id),
                        "existing_checksum_sha256": existing_checksum,
                    },
                )
            return False

        for line in assembly.lines:
            if line.ref_type == "rate":
                assert line.rate_catalog_entry_id is not None
                try:
                    matched_rate = await resolve_rate(
                        session,
                        ref=CatalogRateRef(
                            id=line.rate_catalog_entry_id,
                            checksum_sha256=line.catalog_checksum_sha256,
                        ),
                    )
                except CatalogSelectionError as exc:
                    raise _build_estimate_job_input_error(
                        "catalog_ref_unresolved",
                        ref_type=line.ref_type,
                        selection_key=line.selection_key,
                        line_key=line.line_key,
                        extra_details={
                            "conflict_count": len(exc.conflicting_candidate_ids),
                        },
                    ) from exc

                if _register_catalog_entry_source(
                    line=line,
                    source_id=matched_rate.id,
                    source_checksum_sha256=matched_rate.checksum_sha256,
                ):
                    rate_entries.append(
                        EstimateRateEntryInput(
                            entry_key=line.catalog_entry_key,
                            entry_label=line.description,
                            sort_order=_next_snapshot_sort_order(),
                            source_rate_id=matched_rate.id,
                            source_checksum_sha256=matched_rate.checksum_sha256,
                            unit=matched_rate.unit,
                            effective_date=matched_rate.effective_start,
                            unit_amount=matched_rate.value,
                            source_payload={
                                "selection_key": line.selection_key,
                                "rate_key": matched_rate.rate_key,
                                "item_type": matched_rate.item_type,
                                "metadata": deepcopy(matched_rate.metadata or {}),
                            },
                            currency=cast(Any, matched_rate.currency),
                        )
                    )

                line_inputs.append(
                    EstimateLineInputSpec(
                        line_key=line.line_key,
                        line_type=cast(Any, line.line_type),
                        description=line.description,
                        quantity_entry_key=line.quantity_entry_key,
                        rate_entry_key=line.catalog_entry_key,
                    )
                )
                continue

            if line.ref_type == "material":
                assert line.material_catalog_entry_id is not None
                try:
                    matched_material = await resolve_material(
                        session,
                        ref=CatalogMaterialRef(
                            id=line.material_catalog_entry_id,
                            checksum_sha256=line.catalog_checksum_sha256,
                        ),
                    )
                except CatalogSelectionError as exc:
                    raise _build_estimate_job_input_error(
                        "catalog_ref_unresolved",
                        ref_type=line.ref_type,
                        selection_key=line.selection_key,
                        line_key=line.line_key,
                        extra_details={
                            "conflict_count": len(exc.conflicting_candidate_ids),
                        },
                    ) from exc

                if _register_catalog_entry_source(
                    line=line,
                    source_id=matched_material.id,
                    source_checksum_sha256=matched_material.checksum_sha256,
                ):
                    material_entries.append(
                        EstimateMaterialEntryInput(
                            entry_key=line.catalog_entry_key,
                            entry_label=line.description,
                            sort_order=_next_snapshot_sort_order(),
                            source_material_id=matched_material.id,
                            source_checksum_sha256=matched_material.checksum_sha256,
                            unit=matched_material.unit,
                            effective_date=matched_material.effective_start,
                            unit_amount=matched_material.value,
                            source_payload={
                                "selection_key": line.selection_key,
                                "material_key": matched_material.material_key,
                                "metadata": deepcopy(matched_material.metadata or {}),
                            },
                            currency=cast(Any, matched_material.currency),
                        )
                    )

                line_inputs.append(
                    EstimateLineInputSpec(
                        line_key=line.line_key,
                        line_type=cast(Any, line.line_type),
                        description=line.description,
                        quantity_entry_key=line.quantity_entry_key,
                        material_entry_key=line.catalog_entry_key,
                    )
                )
                continue

            assert line.formula_definition_id is not None
            try:
                selected_formula = await resolve_formula(
                    session,
                    ref=CatalogFormulaRef(
                        id=line.formula_definition_id,
                        checksum_sha256=line.catalog_checksum_sha256,
                    ),
                )
            except CatalogSelectionError as exc:
                raise _build_estimate_job_input_error(
                    "catalog_ref_unresolved",
                    ref_type=line.ref_type,
                    selection_key=line.selection_key,
                    line_key=line.line_key,
                    extra_details={
                        "conflict_count": len(exc.conflicting_candidate_ids),
                    },
                ) from exc

            try:
                formula_definition = formula_definition_from_selected_formula(selected_formula)
            except Exception as exc:
                raise _build_estimate_job_input_error(
                    "invalid_formula_definition",
                    ref_type=line.ref_type,
                    selection_key=line.selection_key,
                    line_key=line.line_key,
                ) from exc
            if _register_catalog_entry_source(
                line=line,
                source_id=selected_formula.definition_id,
                source_checksum_sha256=selected_formula.checksum_sha256,
            ):
                formula_entries.append(
                    EstimateFormulaEntryInput(
                        entry_key=line.catalog_entry_key,
                        entry_label=line.description,
                        sort_order=_next_snapshot_sort_order(),
                        source_formula_id=selected_formula.definition_id,
                        source_checksum_sha256=selected_formula.checksum_sha256,
                        definition=formula_definition,
                        source_payload={
                            "selection_key": line.selection_key,
                            "formula_id": selected_formula.formula_id,
                            "formula_version": selected_formula.version,
                        },
                    )
                )

            raw_formula_inputs = line.formula_inputs or {}
            declared_input_names = tuple(
                declared_input.name for declared_input in formula_definition.declared_inputs
            )
            binding_tokens = _estimate_formula_binding_tokens(
                raw_formula_inputs,
                line=line,
                declared_input_names=declared_input_names,
            )
            for declared_input_name in declared_input_names:
                if declared_input_name not in binding_tokens:
                    raise _build_estimate_job_input_error(
                        "missing_formula_input_binding",
                        ref_type=line.ref_type,
                        selection_key=line.selection_key,
                        line_key=line.line_key,
                        extra_details={"input": declared_input_name},
                    )
            resolved_formula_inputs = {
                declared_input.name: _resolve_formula_binding_snapshot_key(
                    binding_tokens[declared_input.name],
                    line=line,
                    contract_kind=declared_input.contract.kind,
                    lines_by_key=lines_by_key,
                    quantity_entry_keys=quantity_entry_keys,
                    rate_entry_keys=rate_entry_keys,
                    material_entry_keys=material_entry_keys,
                )
                for declared_input in formula_definition.declared_inputs
            }
            line_inputs.append(
                EstimateLineInputSpec(
                    line_key=line.line_key,
                    line_type=cast(Any, line.line_type),
                    description=line.description,
                    formula_entry_key=line.catalog_entry_key,
                    formula_inputs=resolved_formula_inputs,
                )
            )

        return EstimateEngineInput(
            estimate_job_id=job.id,
            project_id=job.project_id,
            file_id=job.file_id,
            source_job_id=job.id,
            drawing_revision_id=estimate_input.drawing_revision_id,
            quantity_takeoff_id=quantity_takeoff.id,
            currency=cast(Any, estimate_input.currency),
            quantity_gate=cast(Any, estimate_input.quantity_gate),
            trusted_totals=estimate_input.trusted_totals,
            tax_rate=_estimate_tax_rate(estimate_input.assumptions_json),
            quantity_entries=tuple(quantity_entries),
            rate_entries=tuple(rate_entries),
            material_entries=tuple(material_entries),
            formula_entries=tuple(formula_entries),
            line_inputs=tuple(line_inputs),
        )


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
        provenance.get("notes") if "notes" in provenance else entity_payload_json.get("notes")
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


def _get_enqueue_job_error_message(job_type: str) -> str:
    """Return the persisted enqueue failure message for a worker job type."""
    return {
        JobType.INGEST.value: _ENQUEUE_INGEST_JOB_ERROR_MESSAGE,
        JobType.REPROCESS.value: _ENQUEUE_REPROCESS_JOB_ERROR_MESSAGE,
        JobType.QUANTITY_TAKEOFF.value: _ENQUEUE_QUANTITY_TAKEOFF_JOB_ERROR_MESSAGE,
        JobType.ESTIMATE.value: _ENQUEUE_ESTIMATE_JOB_ERROR_MESSAGE,
        JobType.EXPORT.value: _ENQUEUE_EXPORT_JOB_ERROR_MESSAGE,
    }.get(job_type, "Failed to enqueue job")


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


def _quantity_gate_details(
    *,
    drawing_revision_id: UUID,
    review_state: str,
    validation_status: str,
    quantity_gate: str,
) -> dict[str, Any]:
    """Build stable structured metadata for quantity gate failures."""
    return {
        "drawing_revision_id": str(drawing_revision_id),
        "review_state": review_state,
        "validation_status": validation_status,
        "quantity_gate": quantity_gate,
    }


def _manifest_entity_count(manifest: RevisionEntityManifest) -> int | None:
    """Return the expected entity count when recorded on the manifest."""
    raw_count = (
        manifest.counts_json.get("entities") if isinstance(manifest.counts_json, dict) else None
    )
    return raw_count if isinstance(raw_count, int) else None


def _build_quantity_gate_metadata(report: ValidationReport) -> RevisionGateMetadata:
    """Build quantity engine gate metadata from the persisted validation report."""
    return RevisionGateMetadata(
        status=cast(GateStatus, report.quantity_gate),
        validation_status=report.validation_status,
        reason=report.review_state if report.quantity_gate in {"review_gated", "blocked"} else None,
        details={
            "drawing_revision_id": str(report.drawing_revision_id),
            "review_state": report.review_state,
            "effective_confidence": report.effective_confidence,
        },
    )


def _build_revision_entity_input(entity: RevisionEntity) -> RevisionEntityInput:
    """Map a materialized revision entity row to the quantity engine contract."""
    return RevisionEntityInput(
        entity_id=entity.entity_id,
        entity_type=entity.entity_type,
        sequence_index=entity.sequence_index,
        geometry_json=entity.geometry_json,
        properties_json=entity.properties_json,
        provenance_json=entity.provenance_json,
        canonical_entity_json=(
            entity.canonical_entity_json if entity.canonical_entity_json is not None else {}
        ),
        source_identity=entity.source_identity,
        source_hash=entity.source_hash,
    )


def _nonempty_quantity_type(value: str | None) -> str:
    """Normalize persisted quantity type labels to non-empty strings."""
    if value is None:
        return "unknown"
    normalized = value.strip()
    return normalized or "unknown"


def _nonempty_quantity_unit(value: str | None) -> str:
    """Normalize persisted quantity units to non-empty strings."""
    if value is None:
        return "unknown"
    normalized = value.strip()
    return normalized or "unknown"


def _duplicate_entity_ids_json(values: tuple[str, ...]) -> list[str]:
    """Copy duplicate contributor lineage ids into a JSON-safe list."""
    return [value for value in values if value]


def _serialize_quantity_context(context: Any) -> str | None:
    """Render quantity context as a deterministic persisted label suffix."""
    if context is None:
        return None
    if isinstance(context, str):
        normalized = context.strip()
        return normalized or None

    try:
        serialized = json.dumps(context, sort_keys=True, separators=(",", ":"))
    except TypeError:
        serialized = str(context).strip()

    return serialized or None


def _quantity_item_type_label(quantity_type: str | None, context: Any) -> str:
    """Persist the quantity type with stable quantity-context disambiguation."""
    normalized_quantity_type = _nonempty_quantity_type(quantity_type)
    serialized_context = _serialize_quantity_context(context)
    if serialized_context is None:
        return normalized_quantity_type

    return f"{normalized_quantity_type}:{serialized_context}"


def _bounded_conflict_text(value: Any) -> str | None:
    """Return a bounded string payload for persisted conflict metadata."""
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    return normalized[:_QUANTITY_CONFLICT_TEXT_LIMIT]


def _bounded_conflict_json(value: Any, *, depth: int = 0) -> Any:
    """Bound persisted conflict details to stable JSON-safe payloads."""
    if value is None or isinstance(value, bool | int | float):
        return value
    if isinstance(value, str):
        return value[:_QUANTITY_CONFLICT_TEXT_LIMIT]
    if depth >= _QUANTITY_CONFLICT_DETAIL_DEPTH_LIMIT:
        return _bounded_conflict_text(value)
    if isinstance(value, dict):
        bounded: dict[str, Any] = {}
        for key, nested_value in list(value.items())[:_QUANTITY_CONFLICT_DETAIL_ITEM_LIMIT]:
            normalized_key = _bounded_conflict_text(key)
            if normalized_key is None:
                continue
            bounded[normalized_key] = _bounded_conflict_json(nested_value, depth=depth + 1)
        return bounded
    if isinstance(value, list | tuple):
        return [
            _bounded_conflict_json(item, depth=depth + 1)
            for item in list(value)[:_QUANTITY_CONFLICT_DETAIL_ITEM_LIMIT]
        ]
    return _bounded_conflict_text(value)


def _bounded_conflict_entity_ids(entity_ids: Any) -> list[str]:
    """Copy contributor conflict entity ids into a bounded JSON-safe list."""
    if not isinstance(entity_ids, tuple | list):
        return []

    bounded_ids: list[str] = []
    for entity_id in entity_ids[:_QUANTITY_CONFLICT_ENTITY_ID_LIMIT]:
        normalized = _bounded_conflict_text(entity_id)
        if normalized is not None:
            bounded_ids.append(normalized)
    return bounded_ids


def _build_quantity_conflict_summaries(conflicts: Sequence[Any]) -> list[dict[str, Any]]:
    """Build bounded persisted summaries for deterministic quantity conflicts."""
    summaries: list[dict[str, Any]] = []
    for conflict in conflicts[:_QUANTITY_CONFLICT_SUMMARY_LIMIT]:
        summaries.append(
            {
                "dedup_key": _bounded_conflict_text(getattr(conflict, "dedup_key", None)),
                "entity_ids": _bounded_conflict_entity_ids(getattr(conflict, "entity_ids", ())),
                "reason": _bounded_conflict_text(getattr(conflict, "reason", None)),
                "details": _bounded_conflict_json(getattr(conflict, "details", None)),
            }
        )
    return summaries


def _build_quantity_items(
    *,
    quantity_takeoff_id: UUID,
    project_id: UUID,
    drawing_revision_id: UUID,
    review_state: str,
    validation_status: str,
    quantity_gate: str,
    result: QuantityEngineResult,
) -> list[QuantityItem]:
    """Build immutable quantity item rows for a takeoff result."""
    items: list[QuantityItem] = []

    for contributor in result.contributors:
        items.append(
            QuantityItem(
                id=uuid.uuid4(),
                quantity_takeoff_id=quantity_takeoff_id,
                project_id=project_id,
                drawing_revision_id=drawing_revision_id,
                item_kind=QuantityItemKind.CONTRIBUTOR.value,
                quantity_type=_quantity_item_type_label(
                    contributor.quantity_type,
                    getattr(contributor, "context", None),
                ),
                value=contributor.value,
                unit=_nonempty_quantity_unit(contributor.unit),
                review_state=review_state,
                validation_status=validation_status,
                quantity_gate=quantity_gate,
                source_entity_id=contributor.entity_id,
                excluded_source_entity_ids_json=_duplicate_entity_ids_json(
                    contributor.duplicate_entity_ids
                ),
            )
        )

    for aggregate in result.aggregates:
        items.append(
            QuantityItem(
                id=uuid.uuid4(),
                quantity_takeoff_id=quantity_takeoff_id,
                project_id=project_id,
                drawing_revision_id=drawing_revision_id,
                item_kind=QuantityItemKind.AGGREGATE.value,
                quantity_type=_quantity_item_type_label(
                    aggregate.quantity_type,
                    getattr(aggregate, "context", None),
                ),
                value=aggregate.total,
                unit=_nonempty_quantity_unit(aggregate.unit),
                review_state=review_state,
                validation_status=validation_status,
                quantity_gate=quantity_gate,
                source_entity_id=None,
                excluded_source_entity_ids_json=[],
            )
        )

    for exclusion in result.exclusions:
        items.append(
            QuantityItem(
                id=uuid.uuid4(),
                quantity_takeoff_id=quantity_takeoff_id,
                project_id=project_id,
                drawing_revision_id=drawing_revision_id,
                item_kind=QuantityItemKind.EXCLUSION.value,
                quantity_type=_quantity_item_type_label(
                    exclusion.quantity_type,
                    getattr(exclusion, "context", None),
                ),
                value=None,
                unit="unknown",
                review_state=review_state,
                validation_status=validation_status,
                quantity_gate=quantity_gate,
                source_entity_id=exclusion.entity_id,
                excluded_source_entity_ids_json=[],
            )
        )

    return items


def _build_export_artifact_name(
    *,
    export_kind: str,
    export_format: str,
    drawing_revision_id: UUID,
    quantity_takeoff_id: UUID | None = None,
    estimate_version_id: UUID | None = None,
) -> str:
    """Build a deterministic filename for a generated export artifact."""
    if export_kind == "revision_json":
        return f"revision-{drawing_revision_id}.{export_format}"
    if export_kind == "quantity_csv":
        assert quantity_takeoff_id is not None
        return f"quantity-takeoff-{quantity_takeoff_id}.{export_format}"
    if export_kind in {"estimate_csv", "estimate_pdf"}:
        assert estimate_version_id is not None
        return f"estimate-{estimate_version_id}.{export_format}"
    return f"export-{drawing_revision_id}.{export_format}"


def _build_export_job_input_error(
    message: str,
    *,
    error_code: ErrorCode = ErrorCode.INPUT_INVALID,
    details: dict[str, Any] | None = None,
) -> _ExportJobInputError:
    """Build a deterministic export job input failure."""
    return _ExportJobInputError(error_code=error_code, message=message, details=details)


def _build_export_artifact_lineage_json(
    *,
    source_file: File,
    job: Job,
    execution: _ExportExecutionInput,
) -> dict[str, Any]:
    """Build lineage metadata for a persisted export artifact."""
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
            "attempts": job.attempts,
            "base_revision_id": (
                str(job.base_revision_id) if job.base_revision_id is not None else None
            ),
        },
        "drawing_revision": {"id": str(execution.drawing_revision_id)},
        "export": {
            "kind": execution.export_kind,
            "format": execution.export_format,
            "media_type": execution.media_type,
            "quantity_takeoff_id": (
                str(execution.quantity_takeoff_id)
                if execution.quantity_takeoff_id is not None
                else None
            ),
            "estimate_version_id": (
                str(execution.estimate_version_id)
                if execution.estimate_version_id is not None
                else None
            ),
            "options": deepcopy(execution.options_json),
        },
    }


async def _render_export_artifact(
    session: AsyncSession,
    execution: _ExportExecutionInput,
) -> _RenderedExportArtifact:
    """Render bytes for a supported export job."""
    try:
        result: RevisionJsonExportResult | CsvExportResult | EstimatePdfExportResult
        if execution.export_kind == "revision_json":
            result = await render_revision_json_export(
                session,
                execution.drawing_revision_id,
                options=execution.options_json,
            )
        elif execution.export_kind == "quantity_csv":
            assert execution.quantity_takeoff_id is not None
            result = await render_quantity_csv_export(session, execution.quantity_takeoff_id)
        elif execution.export_kind == "estimate_csv":
            assert execution.estimate_version_id is not None
            result = await render_estimate_csv_export(session, execution.estimate_version_id)
        elif execution.export_kind == "estimate_pdf":
            assert execution.estimate_version_id is not None
            result = await render_estimate_pdf_export(
                session,
                execution.estimate_version_id,
                options=execution.options_json,
            )
        else:
            raise _build_export_job_input_error(
                "Export job kind is not supported by the worker.",
                details={"export_kind": execution.export_kind},
            )
    except RevisionJsonExportError as exc:
        raise _build_export_job_input_error(
            str(exc),
            error_code=ErrorCode.NOT_FOUND,
            details={"drawing_revision_id": str(execution.drawing_revision_id)},
        ) from exc
    except QuantityCsvExportError as exc:
        raise _build_export_job_input_error(
            str(exc),
            error_code=ErrorCode.NOT_FOUND,
            details={
                "drawing_revision_id": str(execution.drawing_revision_id),
                "quantity_takeoff_id": str(execution.quantity_takeoff_id),
            },
        ) from exc
    except EstimateCsvExportError as exc:
        raise _build_export_job_input_error(
            str(exc),
            error_code=ErrorCode.NOT_FOUND,
            details={
                "drawing_revision_id": str(execution.drawing_revision_id),
                "estimate_version_id": str(execution.estimate_version_id),
            },
        ) from exc
    except EstimatePdfExportError as exc:
        raise _build_export_job_input_error(
            str(exc),
            error_code=ErrorCode.NOT_FOUND,
            details={
                "drawing_revision_id": str(execution.drawing_revision_id),
                "estimate_version_id": str(execution.estimate_version_id),
            },
        ) from exc

    if result.media_type != execution.media_type:
        raise ValueError("Rendered export media type does not match the persisted export job input")

    computed_checksum = hashlib.sha256(result.content_bytes).hexdigest()
    if computed_checksum != result.checksum_sha256:
        raise ValueError("Rendered export checksum does not match the generated bytes")

    if len(result.content_bytes) != result.size_bytes:
        raise ValueError("Rendered export size does not match the generated bytes")

    return _RenderedExportArtifact(
        content_bytes=result.content_bytes,
        checksum_sha256=result.checksum_sha256,
        size_bytes=result.size_bytes,
        media_type=result.media_type,
        generator_name=result.generator_name,
        generator_version=result.generator_version,
    )


async def _build_export_execution_input(
    job_id: UUID,
    *,
    attempt_token: UUID,
) -> _ExportExecutionInput:
    """Load deterministic persisted inputs for a claimed export job."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        job = await session.get(Job, job_id)
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")
        if not _job_attempt_is_current(job, attempt_token=attempt_token):
            raise _StaleJobAttemptError(f"Job attempt for '{job_id}' no longer owns the lease")
        if job.job_type != JobType.EXPORT.value:
            raise ValueError(f"Unsupported export job type '{job.job_type}'")
        if job.base_revision_id is None:
            raise _RevisionConflictError(
                message="Export job is missing its finalized base revision.",
                details={
                    "base_revision_id": None,
                    "current_revision_id": None,
                },
            )

        export_input = await session.get(ExportJobInput, job.id)
        if export_input is None:
            raise _build_export_job_input_error(
                "Export job input is missing.",
                error_code=ErrorCode.NOT_FOUND,
                details={"job_id": str(job.id)},
            )
        if (
            export_input.project_id != job.project_id
            or export_input.source_file_id != job.file_id
            or export_input.drawing_revision_id != job.base_revision_id
            or export_input.source_job_id != job.id
            or export_input.source_job_type != JobType.EXPORT.value
        ):
            raise _build_export_job_input_error(
                "Export job input lineage does not match the persisted job.",
                details={
                    "drawing_revision_id": str(export_input.drawing_revision_id),
                    "base_revision_id": str(job.base_revision_id),
                    "source_job_type": export_input.source_job_type,
                },
            )

        drawing_revision = await _get_drawing_revision(session, revision_id=job.base_revision_id)
        if drawing_revision is None:
            raise _RevisionConflictError(
                message="Export job base revision no longer exists.",
                details={
                    "base_revision_id": str(job.base_revision_id),
                    "current_revision_id": None,
                },
            )
        if (
            drawing_revision.project_id != job.project_id
            or drawing_revision.source_file_id != job.file_id
        ):
            raise ValueError("Export job base revision does not belong to the source file")

        supported_exports: dict[str, tuple[str, str]] = {
            "revision_json": ("json", "application/json"),
            "quantity_csv": ("csv", "text/csv"),
            "estimate_csv": ("csv", "text/csv"),
            "estimate_pdf": ("pdf", "application/pdf"),
        }
        expected_export_metadata = supported_exports.get(export_input.export_kind)
        if expected_export_metadata is None:
            raise _build_export_job_input_error(
                "Export job kind is not supported by the worker.",
                details={"export_kind": export_input.export_kind},
            )

        expected_format, expected_media_type = expected_export_metadata
        if (
            export_input.export_format != expected_format
            or export_input.media_type != expected_media_type
        ):
            raise _build_export_job_input_error(
                "Export job metadata does not match the supported export kind.",
                details={
                    "export_kind": export_input.export_kind,
                    "export_format": export_input.export_format,
                    "media_type": export_input.media_type,
                },
            )

        options_json = deepcopy(export_input.options_json)
        if export_input.export_kind == "revision_json":
            if (
                export_input.quantity_takeoff_id is not None
                or export_input.estimate_version_id is not None
                or export_input.quantity_gate is not None
                or export_input.trusted_totals is not None
            ):
                raise _build_export_job_input_error(
                    "Revision JSON export input contains unexpected quantity or estimate lineage.",
                    details={"export_kind": export_input.export_kind},
                )
            return _ExportExecutionInput(
                drawing_revision_id=drawing_revision.id,
                export_kind=export_input.export_kind,
                export_format=export_input.export_format,
                media_type=export_input.media_type,
                artifact_name=_build_export_artifact_name(
                    export_kind=export_input.export_kind,
                    export_format=export_input.export_format,
                    drawing_revision_id=drawing_revision.id,
                ),
                options_json=options_json,
            )

        quantity_takeoff_id = export_input.quantity_takeoff_id
        if quantity_takeoff_id is None:
            raise _build_export_job_input_error(
                "Export job input is missing its quantity takeoff linkage.",
                details={"export_kind": export_input.export_kind},
            )
        if export_input.quantity_gate != "allowed" or export_input.trusted_totals is not True:
            raise _build_export_job_input_error(
                "Export job input requires a trusted quantity takeoff with allowed gate.",
                details={
                    "quantity_takeoff_id": str(quantity_takeoff_id),
                    "quantity_gate": export_input.quantity_gate,
                    "trusted_totals": export_input.trusted_totals,
                },
            )

        quantity_takeoff = await session.get(QuantityTakeoff, quantity_takeoff_id)
        if quantity_takeoff is None:
            raise _build_export_job_input_error(
                "Export job quantity takeoff was not found.",
                error_code=ErrorCode.NOT_FOUND,
                details={"quantity_takeoff_id": str(quantity_takeoff_id)},
            )
        if (
            quantity_takeoff.project_id != job.project_id
            or quantity_takeoff.source_file_id != job.file_id
            or quantity_takeoff.drawing_revision_id != drawing_revision.id
            or quantity_takeoff.quantity_gate != export_input.quantity_gate
            or quantity_takeoff.trusted_totals is not export_input.trusted_totals
            or quantity_takeoff.source_job_type != JobType.QUANTITY_TAKEOFF.value
        ):
            raise _build_export_job_input_error(
                "Export job quantity takeoff lineage does not match the persisted job input.",
                details={
                    "quantity_takeoff_id": str(quantity_takeoff.id),
                    "drawing_revision_id": str(quantity_takeoff.drawing_revision_id),
                    "quantity_gate": quantity_takeoff.quantity_gate,
                    "trusted_totals": quantity_takeoff.trusted_totals,
                },
            )

        if export_input.export_kind == "quantity_csv":
            if export_input.estimate_version_id is not None:
                raise _build_export_job_input_error(
                    "Quantity CSV export input contains unexpected estimate linkage.",
                    details={
                        "quantity_takeoff_id": str(quantity_takeoff.id),
                        "estimate_version_id": str(export_input.estimate_version_id),
                    },
                )
            return _ExportExecutionInput(
                drawing_revision_id=drawing_revision.id,
                export_kind=export_input.export_kind,
                export_format=export_input.export_format,
                media_type=export_input.media_type,
                artifact_name=_build_export_artifact_name(
                    export_kind=export_input.export_kind,
                    export_format=export_input.export_format,
                    drawing_revision_id=drawing_revision.id,
                    quantity_takeoff_id=quantity_takeoff.id,
                ),
                options_json=options_json,
                quantity_takeoff_id=quantity_takeoff.id,
            )

        estimate_version_id = export_input.estimate_version_id
        if estimate_version_id is None:
            raise _build_export_job_input_error(
                "Estimate export input is missing its estimate version linkage.",
                details={"quantity_takeoff_id": str(quantity_takeoff.id)},
            )

        estimate_version = await session.get(EstimateVersion, estimate_version_id)
        if estimate_version is None:
            raise _build_export_job_input_error(
                "Export job estimate version was not found.",
                error_code=ErrorCode.NOT_FOUND,
                details={"estimate_version_id": str(estimate_version_id)},
            )
        if (
            estimate_version.project_id != job.project_id
            or estimate_version.source_file_id != job.file_id
            or estimate_version.drawing_revision_id != drawing_revision.id
            or estimate_version.quantity_takeoff_id != quantity_takeoff.id
            or estimate_version.quantity_gate != export_input.quantity_gate
            or estimate_version.trusted_totals is not export_input.trusted_totals
        ):
            raise _build_export_job_input_error(
                "Export job estimate lineage does not match the persisted job input.",
                details={
                    "estimate_version_id": str(estimate_version.id),
                    "drawing_revision_id": str(estimate_version.drawing_revision_id),
                    "quantity_takeoff_id": str(estimate_version.quantity_takeoff_id),
                    "quantity_gate": estimate_version.quantity_gate,
                    "trusted_totals": estimate_version.trusted_totals,
                },
            )

        return _ExportExecutionInput(
            drawing_revision_id=drawing_revision.id,
            export_kind=export_input.export_kind,
            export_format=export_input.export_format,
            media_type=export_input.media_type,
            artifact_name=_build_export_artifact_name(
                export_kind=export_input.export_kind,
                export_format=export_input.export_format,
                drawing_revision_id=drawing_revision.id,
                quantity_takeoff_id=quantity_takeoff.id,
                estimate_version_id=estimate_version.id,
            ),
            options_json=options_json,
            quantity_takeoff_id=quantity_takeoff.id,
            estimate_version_id=estimate_version.id,
        )


async def _finalize_export_job(
    job_id: UUID,
    *,
    attempt_token: UUID,
    execution: _ExportExecutionInput,
    rendered: _RenderedExportArtifact,
) -> bool:
    """Atomically publish one export artifact and terminal job success."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        locked_source = await _lock_job_source_for_terminal_mutation(session, job_id)
        job = locked_source.job
        source_file = locked_source.source_file

        if job.status in _TERMINAL_JOB_STATUSES:
            logger.info(
                "export_job_completion_skipped_terminal_status",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        if not _job_attempt_is_current(job, attempt_token=attempt_token):
            logger.info(
                "export_job_completion_skipped_stale_attempt",
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
            logger.info("export_job_cancelled", job_id=str(job_id))
            return False

        if job.status != "running":
            logger.info(
                "export_job_completion_skipped_non_running_status",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        if (
            locked_source.project.deleted_at is not None
            or source_file is None
            or source_file.deleted_at is not None
        ):
            await _cancel_job_for_inactive_source(
                session,
                job,
                reason="source_deleted",
                attempt_token=attempt_token,
            )
            logger.info("export_job_cancelled_inactive_source", job_id=str(job_id))
            return False

        if job.base_revision_id != execution.drawing_revision_id:
            raise _RevisionConflictError(
                message="Export base revision changed before finalization.",
                details={
                    "base_revision_id": (
                        str(job.base_revision_id) if job.base_revision_id is not None else None
                    ),
                    "drawing_revision_id": str(execution.drawing_revision_id),
                },
            )

        existing_artifact = await _get_existing_generated_artifact(session, source_job_id=job.id)
        if existing_artifact is not None:
            logger.info(
                "export_job_completion_skipped_existing_artifact",
                job_id=str(job_id),
                generated_artifact_id=str(existing_artifact.id),
            )
            return False

        generated_artifact_id = uuid.uuid4()
        storage_key = build_generated_artifact_storage_key(
            generated_artifact_id,
            execution.artifact_name,
        )
        lineage_json = _build_export_artifact_lineage_json(
            source_file=source_file,
            job=job,
            execution=execution,
        )
        storage = get_storage()
        written_storage_objects: list[tuple[str, str]] = []
        commit_started = False

        try:
            stored_object = await storage.put(storage_key, rendered.content_bytes, immutable=True)
            written_storage_objects.append((stored_object.key, stored_object.storage_uri))

            if stored_object.checksum_sha256 != rendered.checksum_sha256:
                raise ValueError(
                    "Stored export artifact checksum does not match the rendered bytes"
                )
            if stored_object.size_bytes != rendered.size_bytes:
                raise ValueError("Stored export artifact size does not match the rendered bytes")

            session.add(
                GeneratedArtifact(
                    id=generated_artifact_id,
                    project_id=job.project_id,
                    source_file_id=source_file.id,
                    job_id=job.id,
                    drawing_revision_id=execution.drawing_revision_id,
                    artifact_kind=execution.export_kind,
                    name=execution.artifact_name,
                    format=execution.export_format,
                    media_type=rendered.media_type,
                    size_bytes=stored_object.size_bytes,
                    checksum_sha256=stored_object.checksum_sha256,
                    generator_name=rendered.generator_name,
                    generator_version=rendered.generator_version,
                    generator_config_json=deepcopy(execution.options_json),
                    storage_key=stored_object.key,
                    storage_uri=stored_object.storage_uri,
                    lineage_json=lineage_json,
                )
            )

            job.status = "succeeded"
            job.finished_at = _utcnow()
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
                    "generated_artifact_id": str(generated_artifact_id),
                    "export_kind": execution.export_kind,
                },
                session=session,
            )
            await session.flush()
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


async def _build_quantity_takeoff_execution_input(
    job_id: UUID,
    *,
    attempt_token: UUID,
) -> _QuantityTakeoffExecutionInput:
    """Load unlocked quantity engine inputs for a claimed persisted job."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        job = await session.get(Job, job_id)
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")
        if not _job_attempt_is_current(job, attempt_token=attempt_token):
            raise _StaleJobAttemptError(f"Job attempt for '{job_id}' no longer owns the lease")
        if job.job_type != JobType.QUANTITY_TAKEOFF.value:
            raise ValueError(f"Unsupported quantity takeoff job type '{job.job_type}'")
        if job.base_revision_id is None:
            raise _RevisionConflictError(
                message="Quantity takeoff job is missing its finalized base revision.",
                details={
                    "base_revision_id": None,
                    "current_revision_id": None,
                },
            )

        drawing_revision = await _get_drawing_revision(session, revision_id=job.base_revision_id)
        if drawing_revision is None:
            raise _RevisionConflictError(
                message="Quantity takeoff base revision no longer exists.",
                details={
                    "base_revision_id": str(job.base_revision_id),
                    "current_revision_id": None,
                },
            )
        if (
            drawing_revision.project_id != job.project_id
            or drawing_revision.source_file_id != job.file_id
        ):
            raise ValueError("Quantity takeoff base revision does not belong to the source file")

        report = await _get_validation_report_for_revision(
            session,
            project_id=job.project_id,
            drawing_revision_id=drawing_revision.id,
        )
        if report is None:
            raise _QuantityTakeoffJobError(
                error_code=ErrorCode.NOT_FOUND,
                message=_QUANTITY_TAKEOFF_VALIDATION_REPORT_MISSING_ERROR_MESSAGE,
                details={"drawing_revision_id": str(drawing_revision.id)},
            )

        if report.quantity_gate in {"review_gated", "blocked"}:
            return _QuantityTakeoffExecutionInput(
                drawing_revision_id=drawing_revision.id,
                review_state=report.review_state,
                validation_status=report.validation_status,
                quantity_gate=report.quantity_gate,
                gate=_build_quantity_gate_metadata(report),
                entities=[],
            )

        manifest = await _get_revision_entity_manifest_for_revision(
            session,
            project_id=job.project_id,
            source_file_id=job.file_id,
            drawing_revision_id=drawing_revision.id,
        )
        if manifest is None:
            raise _QuantityTakeoffJobError(
                error_code=ErrorCode.NORMALIZED_ENTITIES_NOT_MATERIALIZED,
                message=_QUANTITY_TAKEOFF_MATERIALIZATION_MISSING_ERROR_MESSAGE,
                details={"drawing_revision_id": str(drawing_revision.id)},
            )

        entities = await _get_revision_entities_for_revision(
            session,
            project_id=job.project_id,
            source_file_id=job.file_id,
            drawing_revision_id=drawing_revision.id,
        )
        expected_entity_count = _manifest_entity_count(manifest)
        if expected_entity_count is not None and expected_entity_count != len(entities):
            raise _QuantityTakeoffJobError(
                error_code=ErrorCode.NORMALIZED_ENTITIES_NOT_MATERIALIZED,
                message=_QUANTITY_TAKEOFF_MATERIALIZATION_MISSING_ERROR_MESSAGE,
                details={
                    "drawing_revision_id": str(drawing_revision.id),
                    "expected_entities": expected_entity_count,
                    "loaded_entities": len(entities),
                },
            )

        return _QuantityTakeoffExecutionInput(
            drawing_revision_id=drawing_revision.id,
            review_state=report.review_state,
            validation_status=report.validation_status,
            quantity_gate=report.quantity_gate,
            gate=_build_quantity_gate_metadata(report),
            entities=[_build_revision_entity_input(entity) for entity in entities],
        )


async def _finalize_quantity_takeoff_job(
    job_id: UUID,
    *,
    attempt_token: UUID,
    execution: _QuantityTakeoffExecutionInput,
    result: QuantityEngineResult,
) -> bool:
    """Atomically publish quantity takeoff rows and terminal job success."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    if result.conflicts:
        raise ValueError("Conflicting quantity results cannot be finalized")

    async with session_maker() as session:
        locked_source = await _lock_job_source_for_terminal_mutation(session, job_id)
        job = locked_source.job

        if job.status in _TERMINAL_JOB_STATUSES:
            logger.info(
                "quantity_takeoff_job_completion_skipped_terminal_status",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        if not _job_attempt_is_current(job, attempt_token=attempt_token):
            logger.info(
                "quantity_takeoff_job_completion_skipped_stale_attempt",
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
            logger.info("quantity_takeoff_job_cancelled", job_id=str(job_id))
            return False

        if job.status != "running":
            logger.info(
                "quantity_takeoff_job_completion_skipped_non_running_status",
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
            logger.info("quantity_takeoff_job_cancelled_inactive_source", job_id=str(job_id))
            return False

        if job.base_revision_id != execution.drawing_revision_id:
            raise _RevisionConflictError(
                message="Quantity takeoff base revision changed before finalization.",
                details={
                    "base_revision_id": (
                        str(job.base_revision_id) if job.base_revision_id is not None else None
                    ),
                    "drawing_revision_id": str(execution.drawing_revision_id),
                },
            )

        existing_takeoff = await _get_existing_quantity_takeoff(session, source_job_id=job.id)
        if existing_takeoff is not None:
            logger.info(
                "quantity_takeoff_job_completion_skipped_existing_takeoff",
                job_id=str(job_id),
                quantity_takeoff_id=str(existing_takeoff.id),
            )
            return False

        quantity_takeoff_id = uuid.uuid4()
        quantity_takeoff = QuantityTakeoff(
            id=quantity_takeoff_id,
            project_id=job.project_id,
            source_file_id=job.file_id,
            drawing_revision_id=execution.drawing_revision_id,
            source_job_id=job.id,
            source_job_type=JobType.QUANTITY_TAKEOFF.value,
            review_state=execution.review_state,
            validation_status=execution.validation_status,
            quantity_gate=execution.quantity_gate,
            trusted_totals=result.trusted_totals,
        )
        quantity_items = _build_quantity_items(
            quantity_takeoff_id=quantity_takeoff_id,
            project_id=job.project_id,
            drawing_revision_id=execution.drawing_revision_id,
            review_state=execution.review_state,
            validation_status=execution.validation_status,
            quantity_gate=execution.quantity_gate,
            result=result,
        )

        session.add(quantity_takeoff)
        await session.flush()
        session.add_all(quantity_items)

        job.status = "succeeded"
        job.finished_at = _utcnow()
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
                "quantity_takeoff_id": str(quantity_takeoff_id),
                "quantity_item_count": len(quantity_items),
                "trusted_totals": result.trusted_totals,
                "quantity_gate": execution.quantity_gate,
            },
            session=session,
        )
        await session.commit()

    return True


async def _finalize_estimate_job(
    job_id: UUID,
    *,
    attempt_token: UUID,
    output: EstimateEngineOutput,
) -> bool:
    """Atomically publish estimate rows and terminal job success."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    estimate_version_kwargs = output.estimate_version_model_kwargs()
    snapshot_entry_kwargs = list(output.snapshot_entry_model_kwargs())
    line_item_kwargs = list(output.line_item_model_kwargs())

    async with session_maker() as session:
        locked_source = await _lock_job_source_for_terminal_mutation(session, job_id)
        job = locked_source.job

        if job.status in _TERMINAL_JOB_STATUSES:
            logger.info(
                "estimate_job_completion_skipped_terminal_status",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        if not _job_attempt_is_current(job, attempt_token=attempt_token):
            logger.info(
                "estimate_job_completion_skipped_stale_attempt",
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
            logger.info("estimate_job_cancelled", job_id=str(job_id))
            return False

        if job.status != "running":
            logger.info(
                "estimate_job_completion_skipped_non_running_status",
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
            logger.info("estimate_job_cancelled_inactive_source", job_id=str(job_id))
            return False

        output_revision_id = cast(UUID | None, estimate_version_kwargs.get("drawing_revision_id"))
        if job.base_revision_id != output_revision_id:
            raise _RevisionConflictError(
                message="Estimate base revision changed before finalization.",
                details={
                    "base_revision_id": (
                        str(job.base_revision_id) if job.base_revision_id is not None else None
                    ),
                    "drawing_revision_id": str(output_revision_id) if output_revision_id else None,
                },
            )

        existing_version = await _get_existing_estimate_version(session, source_job_id=job.id)
        if existing_version is not None:
            logger.info(
                "estimate_job_completion_skipped_existing_version",
                job_id=str(job_id),
                estimate_version_id=str(existing_version.id),
            )
            return False

        estimate_version = EstimateVersion(**estimate_version_kwargs)
        snapshot_entries = [EstimateSnapshotEntry(**kwargs) for kwargs in snapshot_entry_kwargs]
        line_items = [EstimateItem(**kwargs) for kwargs in line_item_kwargs]

        session.add(estimate_version)
        await session.flush()
        session.add_all(snapshot_entries)
        await session.flush()
        session.add_all(line_items)

        job.status = "succeeded"
        job.finished_at = _utcnow()
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
                "estimate_version_id": str(estimate_version.id),
                "snapshot_entry_count": len(snapshot_entries),
                "line_item_count": len(line_items),
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
    attempt_token: UUID | None = None,
    session: AsyncSession | None = None,
) -> bool:
    """Persist a job lifecycle event."""
    return await job_lifecycle.emit_job_event(
        job_id,
        level=level,
        message=message,
        data_json=data_json,
        attempt_token=attempt_token,
        session=session,
        get_job_for_update_func=_get_job_for_update,
        job_attempt_is_current_func=_job_attempt_is_current,
        session_maker_factory=get_session_maker,
    )


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
    await job_lifecycle._persist_job_failed(
        session,
        job,
        error_message=error_message,
        error_code=error_code,
        error_details=error_details,
        utcnow_func=_utcnow,
        clear_job_attempt_lease_func=_clear_job_attempt_lease,
        emit_job_event_func=emit_job_event,
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
    return await job_lifecycle._mark_job_failed(
        job_id,
        error_message=error_message,
        error_code=error_code,
        terminal_job_statuses=_TERMINAL_JOB_STATUSES,
        attempt_token=attempt_token,
        error_details=error_details,
        session_maker_factory=get_session_maker,
        lock_job_source_for_terminal_mutation_func=_lock_job_source_for_terminal_mutation,
        job_attempt_is_current_func=_job_attempt_is_current,
        cancel_job_for_inactive_source_func=_cancel_job_for_inactive_source,
        finalize_job_cancelled_func=_finalize_job_cancelled,
        persist_job_failed_func=_persist_job_failed,
        emit_job_event_func=emit_job_event,
        logger_instance=logger,
    )


async def _mark_job_cancelled(job_id: UUID, *, attempt_token: UUID | None = None) -> bool:
    """Persist a cancelled job state."""
    return await job_lifecycle._mark_job_cancelled(
        job_id,
        terminal_job_statuses=_TERMINAL_JOB_STATUSES,
        attempt_token=attempt_token,
        session_maker_factory=get_session_maker,
        get_job_for_update_func=_get_job_for_update,
        job_attempt_is_current_func=_job_attempt_is_current,
        finalize_job_cancelled_func=_finalize_job_cancelled,
        emit_job_event_func=emit_job_event,
        logger_instance=logger,
    )


def _ensure_worker_database_configured() -> None:
    """Raise when persisted worker processing is invoked without DB access."""
    if get_session_maker() is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")


async def _mark_job_cancelled_with_log(
    job_id: UUID,
    *,
    attempt_token: UUID,
    log_event: str,
    log_fields: dict[str, Any] | None = None,
) -> None:
    """Persist a cancelled terminal state and emit the matching worker log."""
    await _mark_job_cancelled(job_id, attempt_token=attempt_token)
    logger.info(log_event, job_id=str(job_id), **(log_fields or {}))


async def _mark_job_failed_for_revision_conflict(
    job_id: UUID,
    *,
    attempt_token: UUID,
    log_event: str,
    exc: _RevisionConflictError,
) -> None:
    """Persist and log the shared revision-conflict failure contract."""
    await _mark_job_failed(
        job_id,
        error_message=exc.message,
        error_code=ErrorCode.REVISION_CONFLICT,
        attempt_token=attempt_token,
        error_details=exc.details,
    )
    logger.warning(
        log_event,
        job_id=str(job_id),
        error_code=ErrorCode.REVISION_CONFLICT.value,
        **exc.details,
    )


async def _mark_job_failed_with_internal_error_log(
    job_id: UUID,
    *,
    attempt_token: UUID,
    error_message: str,
    log_event: str,
    log_fields: dict[str, Any] | None = None,
) -> None:
    """Persist and log unexpected internal worker failures."""
    await _mark_job_failed(
        job_id,
        error_message=error_message,
        attempt_token=attempt_token,
    )
    logger.error(
        log_event,
        job_id=str(job_id),
        **(
            log_fields
            if log_fields is not None
            else {
                "error_code": ErrorCode.INTERNAL_ERROR.value,
                "error_message": error_message,
            }
        ),
        exc_info=True,
    )


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
                "job_recovery_enqueue_failure_mark_skipped_inactive_source",
                job_id=str(job_id),
                job_type=job.job_type,
                status=job.status,
            )
            return False

        if not _job_is_safe_recovery_failure_target(job):
            logger.info(
                "job_recovery_enqueue_failure_mark_skipped_changed_state",
                job_id=str(job_id),
                job_type=job.job_type,
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


async def _claim_job_enqueue_intent(job_id: UUID) -> _ClaimedJobEnqueueIntent | None:
    """Claim a durable enqueue intent for best-effort or recovery publication."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    now = _utcnow()
    async with session_maker() as session:
        job = await _get_job_for_update(session, job_id)
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        if not is_recoverable_enqueue_job_type(job.job_type) or job.status != "pending":
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
        return _ClaimedJobEnqueueIntent(lease=lease, job_type=job.job_type)


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
    claimed_intent: _ClaimedJobEnqueueIntent | None = None
    try:
        claimed_intent = await _claim_job_enqueue_intent(job_id)
        if claimed_intent is None:
            return False

        publish = publisher or get_job_enqueue_publisher(claimed_intent.job_type)
        if publish is None:
            await _release_job_enqueue_intent(job_id, lease_token=claimed_intent.lease.token)
            return False
        try:
            publish(job_id)
        except Exception:
            await _release_job_enqueue_intent(job_id, lease_token=claimed_intent.lease.token)
            if recovery:
                await _mark_recovery_enqueue_failed(job_id, job_type=claimed_intent.job_type)
            else:
                logger.warning(
                    "job_enqueue_deferred",
                    job_id=str(job_id),
                    job_type=claimed_intent.job_type,
                    recovery_action="worker_start_recovery",
                )
            return False

        await _mark_job_enqueue_published(job_id, lease_token=claimed_intent.lease.token)
        return True
    except Exception:
        if not suppress_exceptions:
            raise

        logger.warning(
            "job_enqueue_deferred",
            job_id=str(job_id),
            job_type=claimed_intent.job_type if claimed_intent is not None else None,
            recovery_action="worker_start_recovery",
        )
        return False


async def _mark_recovery_enqueue_failed(job_id: UUID, *, job_type: str) -> bool:
    """Persist and log a sanitized worker-recovery enqueue failure."""
    marked_failed = await _mark_job_failed_if_recovery_safe(
        job_id,
        error_message=_get_enqueue_job_error_message(job_type),
    )
    if not marked_failed:
        return False

    logger.error(
        "job_recovery_enqueue_failed",
        job_id=str(job_id),
        job_type=job_type,
        error_code=ErrorCode.INTERNAL_ERROR.value,
        recovery_action="mark_failed",
    )
    return True


def _finalize_job_cancelled(job: Job) -> None:
    """Apply the persisted cancelled terminal state to a job."""
    job_lifecycle._finalize_job_cancelled(
        job,
        cancelled_error_code=_JOB_CANCELLED_ERROR_CODE,
        utcnow_func=_utcnow,
        clear_job_attempt_lease_func=_clear_job_attempt_lease,
    )


async def _cancel_job_for_inactive_source(
    session: AsyncSession,
    job: Job,
    *,
    reason: str,
    attempt_token: UUID | None = None,
) -> bool:
    """Persist cancellation when a job source project/file is no longer active."""
    return await job_lifecycle._cancel_job_for_inactive_source(
        session,
        job,
        reason=reason,
        attempt_token=attempt_token,
        terminal_job_statuses=_TERMINAL_JOB_STATUSES,
        job_attempt_is_current_func=_job_attempt_is_current,
        finalize_job_cancelled_func=_finalize_job_cancelled,
        emit_job_event_func=emit_job_event,
    )


async def _begin_or_resume_ingest_job(job_id: UUID) -> _JobAttemptLease | None:
    """Claim, resume, or cancel a persisted ingest job under a row lock."""
    return await job_lifecycle._begin_or_resume_ingest_job(
        job_id,
        terminal_job_statuses=_TERMINAL_JOB_STATUSES,
        stale_after=_RUNNING_JOB_STALE_AFTER,
        session_maker_factory=get_session_maker,
        lock_job_source_for_terminal_mutation_func=_lock_job_source_for_terminal_mutation,
        cancel_job_for_inactive_source_func=_cancel_job_for_inactive_source,
        claim_job_attempt_lease_func=_claim_job_attempt_lease,
        is_stale_running_job_func=_is_stale_running_job,
        finalize_job_cancelled_func=_finalize_job_cancelled,
        emit_job_event_func=emit_job_event,
        logger_instance=logger,
    )


async def _begin_or_resume_quantity_takeoff_job(job_id: UUID) -> _JobAttemptLease | None:
    """Claim, resume, or cancel a persisted quantity takeoff job under a row lock."""
    return await job_lifecycle._begin_or_resume_quantity_takeoff_job(
        job_id,
        terminal_job_statuses=_TERMINAL_JOB_STATUSES,
        stale_after=_RUNNING_JOB_STALE_AFTER,
        session_maker_factory=get_session_maker,
        lock_job_source_for_terminal_mutation_func=_lock_job_source_for_terminal_mutation,
        cancel_job_for_inactive_source_func=_cancel_job_for_inactive_source,
        claim_job_attempt_lease_func=_claim_job_attempt_lease,
        is_stale_running_job_func=_is_stale_running_job,
        finalize_job_cancelled_func=_finalize_job_cancelled,
        emit_job_event_func=emit_job_event,
        logger_instance=logger,
    )


async def _begin_or_resume_estimate_job(job_id: UUID) -> _JobAttemptLease | None:
    """Claim, resume, or cancel a persisted estimate job under a row lock."""
    return await job_lifecycle._begin_or_resume_estimate_job(
        job_id,
        terminal_job_statuses=_TERMINAL_JOB_STATUSES,
        stale_after=_RUNNING_JOB_STALE_AFTER,
        session_maker_factory=get_session_maker,
        lock_job_source_for_terminal_mutation_func=_lock_job_source_for_terminal_mutation,
        cancel_job_for_inactive_source_func=_cancel_job_for_inactive_source,
        claim_job_attempt_lease_func=_claim_job_attempt_lease,
        is_stale_running_job_func=_is_stale_running_job,
        finalize_job_cancelled_func=_finalize_job_cancelled,
        emit_job_event_func=emit_job_event,
        logger_instance=logger,
    )


async def _begin_or_resume_export_job(job_id: UUID) -> _JobAttemptLease | None:
    """Claim, resume, or cancel a persisted export job under a row lock."""
    return await job_lifecycle._begin_or_resume_export_job(
        job_id,
        terminal_job_statuses=_TERMINAL_JOB_STATUSES,
        stale_after=_RUNNING_JOB_STALE_AFTER,
        session_maker_factory=get_session_maker,
        lock_job_source_for_terminal_mutation_func=_lock_job_source_for_terminal_mutation,
        cancel_job_for_inactive_source_func=_cancel_job_for_inactive_source,
        claim_job_attempt_lease_func=_claim_job_attempt_lease,
        is_stale_running_job_func=_is_stale_running_job,
        finalize_job_cancelled_func=_finalize_job_cancelled,
        emit_job_event_func=emit_job_event,
        logger_instance=logger,
    )


async def _execute_ingest_job_attempt(
    job_id: UUID,
    *,
    attempt_token: UUID,
) -> IngestFinalizationPayload:
    """Build inputs, run ingestion, and drain progress/cancellation monitors."""
    request = await _build_ingestion_run_request(job_id, attempt_token=attempt_token)
    progress_bridge = _JobProgressEventBridge(job_id, attempt_token=attempt_token)
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
            attempt_token=attempt_token,
            cancellation=cancellation,
            run_task=run_task,
            stop_event=stop_event,
        )
    )
    try:
        return await run_task
    finally:
        await _stop_job_execution_monitor(
            progress_bridge=progress_bridge,
            stop_event=stop_event,
            cancellation_task=cancellation_task,
        )


async def process_ingest_job(job_id: UUID) -> None:
    """Load a persisted ingest job, run ingestion, and persist state transitions."""
    _ensure_worker_database_configured()

    lease = await _begin_or_resume_ingest_job(job_id)
    if lease is None:
        return

    try:
        payload = await _execute_ingest_job_attempt(job_id, attempt_token=lease.token)
    except _InactiveSourceError:
        logger.info("ingest_job_cancelled_inactive_source", job_id=str(job_id))
        return
    except _StaleJobAttemptError:
        logger.info("ingest_job_stale_attempt_skipped", job_id=str(job_id))
        return
    except _RevisionConflictError as exc:
        await _mark_job_failed_for_revision_conflict(
            job_id,
            attempt_token=lease.token,
            log_event="ingest_job_revision_conflict",
            exc=exc,
        )
        raise
    except IngestionRunnerError as exc:
        if exc.error_code is ErrorCode.JOB_CANCELLED:
            await _mark_job_cancelled_with_log(
                job_id,
                attempt_token=lease.token,
                log_event="ingest_job_cancelled_during_execution",
                log_fields={"error_code": exc.error_code.value},
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
        await _mark_job_cancelled_with_log(
            job_id,
            attempt_token=lease.token,
            log_event="ingest_job_cancelled_during_execution",
        )
        raise
    except Exception:
        await _mark_job_failed_with_internal_error_log(
            job_id,
            attempt_token=lease.token,
            error_message=_PROCESS_INGEST_JOB_ERROR_MESSAGE,
            log_event="ingest_job_failed",
        )
        raise

    try:
        finalized = await _finalize_ingest_job(
            job_id,
            attempt_token=lease.token,
            payload=payload,
        )
    except asyncio.CancelledError:
        await _mark_job_cancelled_with_log(
            job_id,
            attempt_token=lease.token,
            log_event="ingest_job_cancelled_during_finalization",
        )
        raise
    except _RevisionConflictError as exc:
        await _mark_job_failed_for_revision_conflict(
            job_id,
            attempt_token=lease.token,
            log_event="ingest_job_revision_conflict",
            exc=exc,
        )
        raise
    except Exception as exc:
        await _mark_job_failed_with_internal_error_log(
            job_id,
            attempt_token=lease.token,
            error_message=_FINALIZE_INGEST_JOB_ERROR_MESSAGE,
            log_event="ingest_job_finalization_failed",
            log_fields={"error": str(exc)},
        )
        raise

    if finalized:
        logger.info("ingest_job_succeeded", job_id=str(job_id))


async def recover_incomplete_ingest_jobs() -> list[UUID]:
    """Requeue incomplete persisted ingest/reprocess/quantity/estimate jobs on worker startup."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    now = _utcnow()

    async with session_maker() as session:
        result = await session.execute(
            select(Job)
            .where(
                (Job.job_type.in_(_RECOVERABLE_ENQUEUE_JOB_TYPES))
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
    """Requeue incomplete ingest/reprocess/quantity/estimate jobs when a worker starts."""
    try:
        recovered_job_ids = _run_worker_loop(recover_incomplete_ingest_jobs)
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
    _run_worker_loop(lambda: process_ingest_job(UUID(job_id)))


def enqueue_ingest_job(job_id: UUID) -> None:
    """Publish a persisted ingest job to Celery."""
    run_ingest_job.apply_async(args=(str(job_id),), task_id=str(job_id), retry=False)


async def process_quantity_takeoff_job(job_id: UUID) -> None:
    """Load a persisted quantity takeoff job and atomically persist its result."""
    _ensure_worker_database_configured()

    lease = await _begin_or_resume_quantity_takeoff_job(job_id)
    if lease is None:
        return

    try:
        execution = await _build_quantity_takeoff_execution_input(job_id, attempt_token=lease.token)
        if execution.quantity_gate in {"review_gated", "blocked"}:
            error_details = _quantity_gate_details(
                drawing_revision_id=execution.drawing_revision_id,
                review_state=execution.review_state,
                validation_status=execution.validation_status,
                quantity_gate=execution.quantity_gate,
            )
            await _mark_job_failed(
                job_id,
                error_message=_QUANTITY_TAKEOFF_GATE_BLOCKED_ERROR_MESSAGE,
                error_code=ErrorCode.INPUT_INVALID,
                attempt_token=lease.token,
                error_details=error_details,
            )
            logger.warning(
                "quantity_takeoff_job_gate_blocked",
                job_id=str(job_id),
                error_code=ErrorCode.INPUT_INVALID.value,
                **error_details,
            )
            return

        result = compute_quantities(execution.gate, execution.entities)
        if result.conflicts:
            error_details = {
                "drawing_revision_id": str(execution.drawing_revision_id),
                "quantity_gate": execution.quantity_gate,
                "conflict_count": len(result.conflicts),
                "conflicts": _build_quantity_conflict_summaries(result.conflicts),
            }
            await _mark_job_failed(
                job_id,
                error_message=_QUANTITY_TAKEOFF_CONFLICT_ERROR_MESSAGE,
                error_code=ErrorCode.INPUT_INVALID,
                attempt_token=lease.token,
                error_details=error_details,
            )
            logger.warning(
                "quantity_takeoff_job_conflicts_detected",
                job_id=str(job_id),
                error_code=ErrorCode.INPUT_INVALID.value,
                **error_details,
            )
            return
    except _StaleJobAttemptError:
        logger.info("quantity_takeoff_job_stale_attempt_skipped", job_id=str(job_id))
        return
    except _RevisionConflictError as exc:
        await _mark_job_failed_for_revision_conflict(
            job_id,
            attempt_token=lease.token,
            log_event="quantity_takeoff_job_revision_conflict",
            exc=exc,
        )
        return
    except _QuantityTakeoffJobError as exc:
        failure_details: dict[str, Any] | None = exc.details
        await _mark_job_failed(
            job_id,
            error_message=exc.message,
            error_code=exc.error_code,
            attempt_token=lease.token,
            error_details=failure_details,
        )
        logger.warning(
            "quantity_takeoff_job_input_failed",
            job_id=str(job_id),
            error_code=exc.error_code.value,
            **(failure_details or {}),
        )
        return
    except asyncio.CancelledError:
        await _mark_job_cancelled_with_log(
            job_id,
            attempt_token=lease.token,
            log_event="quantity_takeoff_job_cancelled_during_execution",
        )
        raise
    except Exception:
        await _mark_job_failed_with_internal_error_log(
            job_id,
            attempt_token=lease.token,
            error_message=_PROCESS_QUANTITY_TAKEOFF_JOB_ERROR_MESSAGE,
            log_event="quantity_takeoff_job_failed",
        )
        raise

    try:
        finalized = await _finalize_quantity_takeoff_job(
            job_id,
            attempt_token=lease.token,
            execution=execution,
            result=result,
        )
    except asyncio.CancelledError:
        await _mark_job_cancelled_with_log(
            job_id,
            attempt_token=lease.token,
            log_event="quantity_takeoff_job_cancelled_during_finalization",
        )
        raise
    except _RevisionConflictError as exc:
        await _mark_job_failed_for_revision_conflict(
            job_id,
            attempt_token=lease.token,
            log_event="quantity_takeoff_job_revision_conflict",
            exc=exc,
        )
        return
    except Exception:
        await _mark_job_failed_with_internal_error_log(
            job_id,
            attempt_token=lease.token,
            error_message=_FINALIZE_QUANTITY_TAKEOFF_JOB_ERROR_MESSAGE,
            log_event="quantity_takeoff_job_finalization_failed",
        )
        raise

    if finalized:
        logger.info("quantity_takeoff_job_succeeded", job_id=str(job_id))


@celery_app.task(
    name="app.jobs.worker.run_quantity_takeoff_job",
    ignore_result=True,
    acks_late=True,
    reject_on_worker_lost=True,
)
def run_quantity_takeoff_job(job_id: str) -> None:
    """Celery task wrapper for persisted quantity takeoff jobs."""
    _run_worker_loop(lambda: process_quantity_takeoff_job(UUID(job_id)))


def enqueue_quantity_takeoff_job(job_id: UUID) -> None:
    """Publish a persisted quantity takeoff job to Celery."""
    run_quantity_takeoff_job.apply_async(args=(str(job_id),), task_id=str(job_id), retry=False)


async def process_estimate_job(job_id: UUID) -> None:
    """Load a persisted estimate job and assemble deterministic engine inputs."""
    _ensure_worker_database_configured()

    lease = await _begin_or_resume_estimate_job(job_id)
    if lease is None:
        return

    try:
        engine_input = await _build_estimate_engine_input(job_id, attempt_token=lease.token)
        estimate_output = compose_estimate(engine_input)
    except _StaleJobAttemptError:
        logger.info("estimate_job_stale_attempt_skipped", job_id=str(job_id))
        return
    except _RevisionConflictError as exc:
        await _mark_job_failed_for_revision_conflict(
            job_id,
            attempt_token=lease.token,
            log_event="estimate_job_revision_conflict",
            exc=exc,
        )
        return
    except _EstimateJobInputError as exc:
        failure_details = exc.details
        await _mark_job_failed(
            job_id,
            error_message=exc.message,
            error_code=exc.error_code,
            attempt_token=lease.token,
            error_details=failure_details,
        )
        logger.warning(
            "estimate_job_input_failed",
            job_id=str(job_id),
            error_code=exc.error_code.value,
            **(failure_details or {}),
        )
        return
    except EstimateEngineError as exc:
        failure_details = {"reason": exc.reason}
        await _mark_job_failed(
            job_id,
            error_message=_ESTIMATE_JOB_INPUT_INVALID_ERROR_MESSAGE,
            error_code=ErrorCode.INPUT_INVALID,
            attempt_token=lease.token,
            error_details=failure_details,
        )
        logger.warning(
            "estimate_job_input_failed",
            job_id=str(job_id),
            error_code=ErrorCode.INPUT_INVALID.value,
            **failure_details,
        )
        return
    except asyncio.CancelledError:
        await _mark_job_cancelled_with_log(
            job_id,
            attempt_token=lease.token,
            log_event="estimate_job_cancelled_during_execution",
        )
        raise
    except Exception:
        await _mark_job_failed_with_internal_error_log(
            job_id,
            attempt_token=lease.token,
            error_message=_PROCESS_ESTIMATE_JOB_ERROR_MESSAGE,
            log_event="estimate_job_failed",
        )
        raise

    logger.info(
        "estimate_job_composed_pending_finalization",
        job_id=str(job_id),
        quantity_entry_count=len(engine_input.quantity_entries),
        rate_entry_count=len(engine_input.rate_entries),
        material_entry_count=len(engine_input.material_entries),
        formula_entry_count=len(engine_input.formula_entries),
        line_count=len(engine_input.line_inputs),
    )

    try:
        finalized = await _finalize_estimate_job(
            job_id,
            attempt_token=lease.token,
            output=estimate_output,
        )
    except asyncio.CancelledError:
        await _mark_job_cancelled_with_log(
            job_id,
            attempt_token=lease.token,
            log_event="estimate_job_cancelled_during_finalization",
        )
        raise
    except _RevisionConflictError as exc:
        await _mark_job_failed_for_revision_conflict(
            job_id,
            attempt_token=lease.token,
            log_event="estimate_job_revision_conflict",
            exc=exc,
        )
        return
    except Exception:
        await _mark_job_failed_with_internal_error_log(
            job_id,
            attempt_token=lease.token,
            error_message=_FINALIZE_ESTIMATE_JOB_ERROR_MESSAGE,
            log_event="estimate_job_finalization_failed",
        )
        raise

    if finalized:
        logger.info("estimate_job_succeeded", job_id=str(job_id))


@celery_app.task(
    name="app.jobs.worker.run_estimate_job",
    ignore_result=True,
    acks_late=True,
    reject_on_worker_lost=True,
)
def run_estimate_job(job_id: str) -> None:
    """Celery task wrapper for persisted estimate jobs."""
    _run_worker_loop(lambda: process_estimate_job(UUID(job_id)))


def enqueue_estimate_job(job_id: UUID) -> None:
    """Publish a persisted estimate job to Celery."""
    run_estimate_job.apply_async(args=(str(job_id),), task_id=str(job_id), retry=False)


async def process_export_job(job_id: UUID) -> None:
    """Load a persisted export job and atomically persist its generated artifact."""
    _ensure_worker_database_configured()

    lease = await _begin_or_resume_export_job(job_id)
    if lease is None:
        return

    try:
        execution = await _build_export_execution_input(job_id, attempt_token=lease.token)
        session_maker = get_session_maker()
        if session_maker is None:
            raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")
        async with session_maker() as session:
            rendered = await _render_export_artifact(session, execution)
    except _StaleJobAttemptError:
        logger.info("export_job_stale_attempt_skipped", job_id=str(job_id))
        return
    except _RevisionConflictError as exc:
        await _mark_job_failed_for_revision_conflict(
            job_id,
            attempt_token=lease.token,
            log_event="export_job_revision_conflict",
            exc=exc,
        )
        return
    except _ExportJobInputError as exc:
        failure_details = exc.details
        await _mark_job_failed(
            job_id,
            error_message=exc.message,
            error_code=exc.error_code,
            attempt_token=lease.token,
            error_details=failure_details,
        )
        logger.warning(
            "export_job_input_failed",
            job_id=str(job_id),
            error_code=exc.error_code.value,
            **(failure_details or {}),
        )
        return
    except asyncio.CancelledError:
        await _mark_job_cancelled_with_log(
            job_id,
            attempt_token=lease.token,
            log_event="export_job_cancelled_during_execution",
        )
        raise
    except Exception:
        await _mark_job_failed_with_internal_error_log(
            job_id,
            attempt_token=lease.token,
            error_message=_PROCESS_EXPORT_JOB_ERROR_MESSAGE,
            log_event="export_job_failed",
        )
        raise

    try:
        finalized = await _finalize_export_job(
            job_id,
            attempt_token=lease.token,
            execution=execution,
            rendered=rendered,
        )
    except asyncio.CancelledError:
        await _mark_job_cancelled_with_log(
            job_id,
            attempt_token=lease.token,
            log_event="export_job_cancelled_during_finalization",
        )
        raise
    except _RevisionConflictError as exc:
        await _mark_job_failed_for_revision_conflict(
            job_id,
            attempt_token=lease.token,
            log_event="export_job_revision_conflict",
            exc=exc,
        )
        return
    except Exception:
        await _mark_job_failed_with_internal_error_log(
            job_id,
            attempt_token=lease.token,
            error_message=_FINALIZE_EXPORT_JOB_ERROR_MESSAGE,
            log_event="export_job_finalization_failed",
        )
        raise

    if finalized:
        logger.info("export_job_succeeded", job_id=str(job_id))


@celery_app.task(
    name="app.jobs.worker.run_export_job",
    ignore_result=True,
    acks_late=True,
    reject_on_worker_lost=True,
)
def run_export_job(job_id: str) -> None:
    """Celery task wrapper for persisted export jobs."""
    _run_worker_loop(lambda: process_export_job(UUID(job_id)))


def enqueue_export_job(job_id: UUID) -> None:
    """Publish a persisted export job to Celery."""

    run_export_job.apply_async(args=(str(job_id),), task_id=str(job_id), retry=False)
