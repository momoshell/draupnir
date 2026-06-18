"""Quantity-takeoff execution-input validation and building (issue D7b).

Mirrors the export seam from D7a: the quantity-takeoff path turns a claimed job
into a deterministic ``_QuantityTakeoffExecutionInput`` by loading several
persisted rows and validating lineage/gate/materialization invariants. This
module isolates that contract checking from database access through the
``QuantityRowLoader`` protocol, so the validation is unit-testable with in-memory
fakes. ``worker`` supplies a session-backed loader and delegates.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

from app.core.errors import ErrorCode
from app.estimating.quantities.contracts import (
    RevisionEntityInput,
    RevisionGateMetadata,
)
from app.jobs.execution_inputs import _QuantityTakeoffExecutionInput
from app.jobs.lifecycle import (
    _job_attempt_is_current,
    _RevisionConflictError,
    _StaleJobAttemptError,
)
from app.models.drawing_revision import DrawingRevision
from app.models.job import Job, JobType
from app.models.revision_materialization import RevisionEntity, RevisionEntityManifest
from app.models.validation_report import ValidationReport

_QUANTITY_TAKEOFF_VALIDATION_REPORT_MISSING_ERROR_MESSAGE = (
    "Quantity takeoff base revision is missing its validation report."
)
_QUANTITY_TAKEOFF_MATERIALIZATION_MISSING_ERROR_MESSAGE = (
    "Quantity takeoff base revision is missing normalized entities."
)


@dataclass(frozen=True, slots=True)
class _QuantityTakeoffJobError(Exception):
    """Raised for deterministic quantity takeoff failures."""

    error_code: ErrorCode
    message: str
    details: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.message


def _manifest_entity_count(manifest: RevisionEntityManifest) -> int | None:
    """Return the expected entity count when recorded on the manifest."""
    raw_count = (
        manifest.counts_json.get("entities") if isinstance(manifest.counts_json, dict) else None
    )
    return raw_count if isinstance(raw_count, int) else None


def _build_quantity_gate_metadata(report: ValidationReport) -> RevisionGateMetadata:
    """Build quantity engine gate metadata from the persisted validation report.

    Path B 6: the gate/review/confidence columns are gone; quantities are always
    computed, so the gate is recorded as informational provenance with a fixed
    ``allowed`` status carrying only the technical validation status.
    """
    return RevisionGateMetadata(
        status="allowed",
        validation_status=report.validation_status,
        reason=None,
        details={
            "drawing_revision_id": str(report.drawing_revision_id),
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


class QuantityRowLoader(Protocol):
    """Database seam for the rows a quantity-takeoff execution-input depends on.

    Implementations own all DB access; the builder stays pure over the rows they
    return.
    """

    async def get_job(self, job_id: UUID) -> Job | None: ...

    async def get_drawing_revision(self, revision_id: UUID) -> DrawingRevision | None: ...

    async def get_validation_report(
        self, *, project_id: UUID, drawing_revision_id: UUID
    ) -> ValidationReport | None: ...

    async def get_entity_manifest(
        self, *, project_id: UUID, source_file_id: UUID, drawing_revision_id: UUID
    ) -> RevisionEntityManifest | None: ...

    async def get_revision_entities(
        self, *, project_id: UUID, source_file_id: UUID, drawing_revision_id: UUID
    ) -> Sequence[RevisionEntity]: ...


async def build_quantity_takeoff_execution_input(
    job_id: UUID,
    *,
    attempt_token: UUID,
    loader: QuantityRowLoader,
) -> _QuantityTakeoffExecutionInput:
    """Validate lineage/gate/materialization rules and build the quantity input.

    Pure over the rows returned by ``loader``: all database access is delegated to
    the loader, so the validation rules are unit-testable with fakes. The
    load/validate ordering (and therefore error precedence) matches the original
    inline implementation in ``worker``.
    """
    job = await loader.get_job(job_id)
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

    drawing_revision = await loader.get_drawing_revision(job.base_revision_id)
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

    report = await loader.get_validation_report(
        project_id=job.project_id,
        drawing_revision_id=drawing_revision.id,
    )
    if report is None:
        raise _QuantityTakeoffJobError(
            error_code=ErrorCode.NOT_FOUND,
            message=_QUANTITY_TAKEOFF_VALIDATION_REPORT_MISSING_ERROR_MESSAGE,
            details={"drawing_revision_id": str(drawing_revision.id)},
        )

    manifest = await loader.get_entity_manifest(
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

    entities = await loader.get_revision_entities(
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
        validation_status=report.validation_status,
        gate=_build_quantity_gate_metadata(report),
        entities=[_build_revision_entity_input(entity) for entity in entities],
    )
