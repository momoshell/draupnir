"""Export execution-input validation and building (issue D7a).

The export path must turn a claimed job into a deterministic ``_ExportExecutionInput``
by validating lineage/gate/trusted-totals invariants across several persisted rows.
This module isolates that contract checking from database access: row loading is
expressed through the ``ExportRowLoader`` protocol, so the builder can be exercised
with in-memory fakes and no session. ``worker`` supplies a session-backed loader and
the spec/artifact-name collaborators that remain tied to the render registry.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

from app.core.errors import ErrorCode
from app.jobs.execution_inputs import _ExportExecutionInput
from app.jobs.lifecycle import (
    _job_attempt_is_current,
    _RevisionConflictError,
    _StaleJobAttemptError,
)
from app.models.drawing_revision import DrawingRevision
from app.models.estimate_version import EstimateVersion
from app.models.export_job_input import ExportJobInput
from app.models.job import Job, JobType
from app.models.quantity_takeoff import QuantityTakeoff

_EXPORT_LINEAGE_ANCHOR_REVISION = "revision"
_EXPORT_LINEAGE_ANCHOR_CHANGESET = "changeset"
_EXPORT_LINEAGE_ANCHOR_QUANTITY_TAKEOFF = "quantity_takeoff"
_EXPORT_LINEAGE_ANCHOR_ESTIMATE_VERSION = "estimate_version"


@dataclass(frozen=True, slots=True)
class _ExportJobInputError(Exception):
    """Raised for deterministic export job input failures."""

    error_code: ErrorCode
    message: str
    details: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.message


def _build_export_job_input_error(
    message: str,
    *,
    error_code: ErrorCode = ErrorCode.INPUT_INVALID,
    details: dict[str, Any] | None = None,
) -> _ExportJobInputError:
    """Build a deterministic export job input failure."""
    return _ExportJobInputError(error_code=error_code, message=message, details=details)


class ExportKindContract(Protocol):
    """The export-kind spec fields the execution-input builder relies on."""

    @property
    def format(self) -> str: ...

    @property
    def media_type(self) -> str: ...

    @property
    def lineage_anchor(self) -> str: ...


class ExportRowLoader(Protocol):
    """Database seam for the rows an export execution-input depends on.

    Each method loads one persisted row by identity (or ``None`` when absent),
    mirroring ``AsyncSession.get`` semantics. Implementations own all DB access;
    the builder stays pure over the rows they return.
    """

    async def get_job(self, job_id: UUID) -> Job | None: ...

    async def get_export_job_input(self, job_id: UUID) -> ExportJobInput | None: ...

    async def get_drawing_revision(self, revision_id: UUID) -> DrawingRevision | None: ...

    async def get_quantity_takeoff(self, takeoff_id: UUID) -> QuantityTakeoff | None: ...

    async def get_estimate_version(self, version_id: UUID) -> EstimateVersion | None: ...


class ResolveExportSpec(Protocol):
    def __call__(self, export_kind: str) -> ExportKindContract: ...


class BuildArtifactName(Protocol):
    def __call__(
        self,
        *,
        export_kind: str,
        export_format: str,
        drawing_revision_id: UUID,
        changeset_id: UUID | None = ...,
        quantity_takeoff_id: UUID | None = ...,
        estimate_version_id: UUID | None = ...,
    ) -> str: ...


class ExecutionInputValidator(Protocol):
    """Validate persisted rows and build a deterministic export execution input."""

    async def __call__(
        self,
        job_id: UUID,
        *,
        attempt_token: UUID,
        loader: ExportRowLoader,
        resolve_export_spec: ResolveExportSpec,
        build_artifact_name: BuildArtifactName,
    ) -> _ExportExecutionInput: ...


async def build_export_execution_input(
    job_id: UUID,
    *,
    attempt_token: UUID,
    loader: ExportRowLoader,
    resolve_export_spec: ResolveExportSpec,
    build_artifact_name: BuildArtifactName,
) -> _ExportExecutionInput:
    """Validate lineage/gate/trusted-totals rules and build the export execution input.

    Pure over the rows returned by ``loader``: all database access is delegated to
    the loader, so the validation rules below are unit-testable with fakes. The
    load/validate ordering (and therefore error precedence) matches the original
    inline implementation in ``worker``.
    """
    job = await loader.get_job(job_id)
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

    export_input = await loader.get_export_job_input(job.id)
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

    drawing_revision = await loader.get_drawing_revision(job.base_revision_id)
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

    export_spec = resolve_export_spec(export_input.export_kind)
    if (
        export_input.export_format != export_spec.format
        or export_input.media_type != export_spec.media_type
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
    if export_spec.lineage_anchor in {
        _EXPORT_LINEAGE_ANCHOR_REVISION,
        _EXPORT_LINEAGE_ANCHOR_CHANGESET,
    }:
        if (
            export_input.quantity_takeoff_id is not None
            or export_input.estimate_version_id is not None
            or export_input.quantity_gate is not None
            or export_input.trusted_totals is not None
        ):
            raise _build_export_job_input_error(
                "Revision-scoped export input contains unexpected quantity or estimate lineage.",
                details={"export_kind": export_input.export_kind},
            )
        if export_spec.lineage_anchor == _EXPORT_LINEAGE_ANCHOR_CHANGESET:
            if (
                drawing_revision.revision_kind != "changeset"
                or drawing_revision.changeset_id is None
            ):
                raise _build_export_job_input_error(
                    "Revised DXF export requires a changeset-origin drawing revision.",
                    details={"drawing_revision_id": str(drawing_revision.id)},
                )
            return _ExportExecutionInput(
                drawing_revision_id=drawing_revision.id,
                changeset_id=drawing_revision.changeset_id,
                export_kind=export_input.export_kind,
                export_format=export_input.export_format,
                media_type=export_input.media_type,
                artifact_name=build_artifact_name(
                    export_kind=export_input.export_kind,
                    export_format=export_input.export_format,
                    drawing_revision_id=drawing_revision.id,
                    changeset_id=drawing_revision.changeset_id,
                ),
                options_json=options_json,
            )
        return _ExportExecutionInput(
            drawing_revision_id=drawing_revision.id,
            export_kind=export_input.export_kind,
            export_format=export_input.export_format,
            media_type=export_input.media_type,
            artifact_name=build_artifact_name(
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

    quantity_takeoff = await loader.get_quantity_takeoff(quantity_takeoff_id)
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

    if export_spec.lineage_anchor == _EXPORT_LINEAGE_ANCHOR_QUANTITY_TAKEOFF:
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
            artifact_name=build_artifact_name(
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

    estimate_version = await loader.get_estimate_version(estimate_version_id)
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
        artifact_name=build_artifact_name(
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
