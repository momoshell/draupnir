"""Per-type job finalizers extracted from app.jobs.worker (issue #387).

Each finalizer atomically publishes one job type's durable outputs and marks the
job succeeded. Collaborators that tests patch on the worker module are received
via the injected WorkerDeps; shared queries/primitives come from sibling modules.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from copy import deepcopy
from typing import Any, cast
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.db.session import get_session_maker
from app.estimating.engine.contracts import (
    EstimateEngineOutput,
)
from app.estimating.quantities.contracts import (
    QuantityEngineResult,
)
from app.exports._base import ExportArtifact
from app.ingestion.debug_overlay import plan_svg_debug_overlay
from app.ingestion.finalization import IngestFinalizationPayload
from app.jobs.db_write import _bulk_insert_model_rows
from app.jobs.estimate_assembly import (
    _build_estimate_engine_input as _build_estimate_engine_input,
)
from app.jobs.execution_inputs import (
    _ExportExecutionInput as _ExportExecutionInput,
)
from app.jobs.execution_inputs import (
    _QuantityTakeoffExecutionInput as _QuantityTakeoffExecutionInput,
)
from app.jobs.lifecycle import (
    _TERMINAL_JOB_STATUSES,
    _clear_job_attempt_lease,
    _get_job_for_update_with_metadata,
    _get_source_file,
    _job_attempt_is_current,
    _RevisionConflictError,
    _utcnow,
)
from app.jobs.report_lineage import (
    _build_debug_overlay_lineage_json,
    _build_export_artifact_lineage_json,
    _build_persisted_validation_report_json,
)
from app.jobs.revision_materialization import (
    _build_revision_materialization_rows,
    _order_revision_entity_insert_rows,
)
from app.jobs.revision_queries import _assert_job_base_revision_invariants
from app.jobs.revision_queries import (
    _build_revision_conflict_details as _build_revision_conflict_details,
)
from app.jobs.revision_queries import (
    _get_drawing_revision as _get_drawing_revision,
)
from app.jobs.revision_queries import (
    _get_latest_drawing_revision as _get_latest_drawing_revision,
)
from app.jobs.revision_queries import (
    _get_revision_blocks_for_revision as _get_revision_blocks_for_revision,
)
from app.jobs.revision_queries import (
    _get_revision_entities_for_revision as _get_revision_entities_for_revision,
)
from app.jobs.revision_queries import (
    _get_revision_entity_manifest_for_revision as _get_revision_entity_manifest_for_revision,
)
from app.jobs.revision_queries import (
    _get_revision_layers_for_revision as _get_revision_layers_for_revision,
)
from app.jobs.revision_queries import (
    _get_revision_layouts_for_revision as _get_revision_layouts_for_revision,
)
from app.jobs.revision_queries import (
    _get_validation_report_for_revision as _get_validation_report_for_revision,
)
from app.jobs.revision_queries import (
    _revision_reference as _revision_reference,
)
from app.jobs.worker_deps import WorkerDeps
from app.models.adapter_run_output import AdapterRunOutput
from app.models.drawing_revision import DrawingRevision
from app.models.estimate_version import EstimateItem, EstimateSnapshotEntry, EstimateVersion
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
from app.storage.keys import build_generated_artifact_storage_key

logger = get_logger(__name__)


_INITIAL_INGEST_REVISION_KIND = "ingest"

_REPROCESS_REVISION_KIND = "reprocess"

_DEBUG_OVERLAY_ARTIFACT_KIND = "debug_overlay"

_DEBUG_OVERLAY_ARTIFACT_FORMAT = "svg"

_DEBUG_OVERLAY_GENERATOR_NAME = "app.ingestion.debug_overlay"

_DEBUG_OVERLAY_GENERATOR_VERSION = "1"


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


def _expected_revision_kind_for_job(job: Job) -> str:
    """Return the expected persisted revision kind for a job type."""

    if job.job_type == JobType.INGEST.value:
        return _INITIAL_INGEST_REVISION_KIND
    if job.job_type == JobType.REPROCESS.value:
        return _REPROCESS_REVISION_KIND
    raise ValueError(f"Unsupported ingest job type '{job.job_type}'")


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


async def _finalize_ingest_job(
    job_id: UUID,
    *,
    attempt_token: UUID,
    deps: WorkerDeps,
    payload: IngestFinalizationPayload,
) -> bool:
    """Atomically publish durable ingest outputs and terminal job success."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        bootstrap = await deps.get_job_lock_bootstrap(session, job_id)
        if bootstrap is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        project = await deps.get_project(session, bootstrap.project_id, for_update=True)
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
            deps.finalize_job_cancelled(job)
            await deps.emit_job_event(
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
            await deps.cancel_job_for_inactive_source(
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
            await deps.cancel_job_for_inactive_source(
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
        storage = deps.get_storage()
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
            await deps.emit_job_event(
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


async def _finalize_export_job(
    job_id: UUID,
    *,
    attempt_token: UUID,
    deps: WorkerDeps,
    execution: _ExportExecutionInput,
    rendered: ExportArtifact,
) -> bool:
    """Atomically publish one export artifact and terminal job success."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        locked_source = await deps.lock_job_source(session, job_id)
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
            deps.finalize_job_cancelled(job)
            await deps.emit_job_event(
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
            await deps.cancel_job_for_inactive_source(
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
        storage = deps.get_storage()
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
                    changeset_id=execution.changeset_id,
                    quantity_takeoff_id=execution.quantity_takeoff_id,
                    estimate_version_id=execution.estimate_version_id,
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
            await deps.emit_job_event(
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


async def _finalize_quantity_takeoff_job(
    job_id: UUID,
    *,
    attempt_token: UUID,
    deps: WorkerDeps,
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
        locked_source = await deps.lock_job_source(session, job_id)
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
            deps.finalize_job_cancelled(job)
            await deps.emit_job_event(
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
            await deps.cancel_job_for_inactive_source(
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
        await deps.emit_job_event(
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
    deps: WorkerDeps,
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
        locked_source = await deps.lock_job_source(session, job_id)
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
            deps.finalize_job_cancelled(job)
            await deps.emit_job_event(
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
            await deps.cancel_job_for_inactive_source(
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
        await deps.emit_job_event(
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
