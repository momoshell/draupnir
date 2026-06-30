"""Export artifact registry, naming, and rendering helpers."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Coroutine
from copy import deepcopy
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import ErrorCode
from app.exports._base import ExportArtifact
from app.exports.csv import (
    EstimateCsvExportError,
    QuantityCsvExportError,
    render_estimate_csv_export,
    render_quantity_csv_export,
)
from app.exports.estimate_pdf import EstimatePdfExportError, render_estimate_pdf_export
from app.exports.revised_dxf import RevisedDxfExportError, render_revised_dxf_export
from app.exports.revision_json import RevisionJsonExportError, render_revision_json_export
from app.jobs.execution_inputs import _ExportExecutionInput
from app.jobs.export_execution_input import (
    _EXPORT_LINEAGE_ANCHOR_CHANGESET,
    _EXPORT_LINEAGE_ANCHOR_ESTIMATE_VERSION,
    _EXPORT_LINEAGE_ANCHOR_QUANTITY_TAKEOFF,
    _EXPORT_LINEAGE_ANCHOR_REVISION,
    _build_export_job_input_error,
    _ExportJobInputError,
)

EXPORT_LINEAGE_ANCHOR_REVISION = _EXPORT_LINEAGE_ANCHOR_REVISION
EXPORT_LINEAGE_ANCHOR_CHANGESET = _EXPORT_LINEAGE_ANCHOR_CHANGESET
EXPORT_LINEAGE_ANCHOR_QUANTITY_TAKEOFF = _EXPORT_LINEAGE_ANCHOR_QUANTITY_TAKEOFF
EXPORT_LINEAGE_ANCHOR_ESTIMATE_VERSION = _EXPORT_LINEAGE_ANCHOR_ESTIMATE_VERSION

ExportRenderFn = Callable[
    [AsyncSession, _ExportExecutionInput],
    Coroutine[Any, Any, ExportArtifact],
]
ExportErrorDetailsFn = Callable[[_ExportExecutionInput], dict[str, Any]]
ExportErrorMapperFn = Callable[[Exception, _ExportExecutionInput], _ExportJobInputError]

_REVISED_DXF_INPUT_ERROR_CODES = frozenset(
    {
        "INPUT_INVALID",
        "MANIFEST_NOT_FOUND",
        "MATERIALIZATION_MISSING",
        "MISSING_LAYER",
        "MISSING_LAYOUT",
        "NONFINITE_COORDINATE",
        "NONZERO_Z_COORDINATE",
        "REVISION_NOT_FOUND",
    }
)


@dataclass(frozen=True, slots=True)
class ExportKindSpec:
    """Registry entry for one supported export worker kind."""

    format: str
    media_type: str
    render_fn: ExportRenderFn
    error_type: type[Exception]
    lineage_anchor: str
    error_details_fn: ExportErrorDetailsFn
    error_mapper_fn: ExportErrorMapperFn | None = None


def _export_revision_error_details(execution: _ExportExecutionInput) -> dict[str, Any]:
    """Build not-found details for revision-scoped exports."""
    return {"drawing_revision_id": str(execution.drawing_revision_id)}


def _export_changeset_error_details(execution: _ExportExecutionInput) -> dict[str, Any]:
    """Build details for changeset-scoped exports."""
    return {
        "drawing_revision_id": str(execution.drawing_revision_id),
        "changeset_id": str(execution.changeset_id) if execution.changeset_id is not None else None,
    }


def _export_quantity_error_details(execution: _ExportExecutionInput) -> dict[str, Any]:
    """Build not-found details for quantity-scoped exports."""
    assert execution.quantity_takeoff_id is not None
    return {
        "drawing_revision_id": str(execution.drawing_revision_id),
        "quantity_takeoff_id": str(execution.quantity_takeoff_id),
    }


def _export_estimate_error_details(execution: _ExportExecutionInput) -> dict[str, Any]:
    """Build not-found details for estimate-scoped exports."""
    assert execution.estimate_version_id is not None
    return {
        "drawing_revision_id": str(execution.drawing_revision_id),
        "estimate_version_id": str(execution.estimate_version_id),
    }


async def _render_revision_json_export_artifact(
    session: AsyncSession,
    execution: _ExportExecutionInput,
) -> ExportArtifact:
    """Render a revision JSON export artifact."""
    return await render_revision_json_export(
        session,
        execution.drawing_revision_id,
        options=execution.options_json,
    )


def _map_revised_dxf_export_error(
    exc: Exception,
    execution: _ExportExecutionInput,
) -> _ExportJobInputError:
    """Map revised-DXF renderer failures to deterministic job errors."""
    assert isinstance(exc, RevisedDxfExportError)
    details = _export_changeset_error_details(execution)
    details.update(deepcopy(exc.details or {}))
    details["renderer_error_code"] = exc.code
    return _build_export_job_input_error(
        str(exc),
        error_code=(
            ErrorCode.ADAPTER_UNAVAILABLE
            if exc.code in {"ADAPTER_UNAVAILABLE", "ADAPTER_LOAD_FAILED"}
            else (
                ErrorCode.INPUT_INVALID
                if _is_revised_dxf_input_error_code(exc.code)
                else ErrorCode.ADAPTER_FAILED
            )
        ),
        details=details,
    )


def _is_revised_dxf_input_error_code(code: str) -> bool:
    return code in _REVISED_DXF_INPUT_ERROR_CODES or code.startswith(("INVALID_", "UNSUPPORTED_"))


async def _render_revised_dxf_export_artifact(
    session: AsyncSession,
    execution: _ExportExecutionInput,
) -> ExportArtifact:
    """Render a revised DXF export artifact."""
    return await render_revised_dxf_export(
        session,
        execution.drawing_revision_id,
        options=execution.options_json,
    )


async def _render_dxf_export_artifact(
    session: AsyncSession,
    execution: _ExportExecutionInput,
) -> ExportArtifact:
    """Render a base-revision DXF export artifact (no changeset required)."""
    return await render_revised_dxf_export(
        session,
        execution.drawing_revision_id,
        options=execution.options_json,
        require_changeset_origin=False,
    )


async def _render_quantity_csv_export_artifact(
    session: AsyncSession,
    execution: _ExportExecutionInput,
) -> ExportArtifact:
    """Render a quantity CSV export artifact."""
    assert execution.quantity_takeoff_id is not None
    return await render_quantity_csv_export(session, execution.quantity_takeoff_id)


async def _render_estimate_csv_export_artifact(
    session: AsyncSession,
    execution: _ExportExecutionInput,
) -> ExportArtifact:
    """Render an estimate CSV export artifact."""
    assert execution.estimate_version_id is not None
    return await render_estimate_csv_export(session, execution.estimate_version_id)


async def _render_estimate_pdf_export_artifact(
    session: AsyncSession,
    execution: _ExportExecutionInput,
) -> ExportArtifact:
    """Render an estimate PDF export artifact."""
    assert execution.estimate_version_id is not None
    return await render_estimate_pdf_export(
        session,
        execution.estimate_version_id,
        options=execution.options_json,
    )


EXPORT_KIND_SPECS: dict[str, ExportKindSpec] = {
    "revision_json": ExportKindSpec(
        format="json",
        media_type="application/json",
        render_fn=_render_revision_json_export_artifact,
        error_type=RevisionJsonExportError,
        lineage_anchor=_EXPORT_LINEAGE_ANCHOR_REVISION,
        error_details_fn=_export_revision_error_details,
    ),
    "quantity_csv": ExportKindSpec(
        format="csv",
        media_type="text/csv",
        render_fn=_render_quantity_csv_export_artifact,
        error_type=QuantityCsvExportError,
        lineage_anchor=_EXPORT_LINEAGE_ANCHOR_QUANTITY_TAKEOFF,
        error_details_fn=_export_quantity_error_details,
    ),
    "estimate_csv": ExportKindSpec(
        format="csv",
        media_type="text/csv",
        render_fn=_render_estimate_csv_export_artifact,
        error_type=EstimateCsvExportError,
        lineage_anchor=_EXPORT_LINEAGE_ANCHOR_ESTIMATE_VERSION,
        error_details_fn=_export_estimate_error_details,
    ),
    "estimate_pdf": ExportKindSpec(
        format="pdf",
        media_type="application/pdf",
        render_fn=_render_estimate_pdf_export_artifact,
        error_type=EstimatePdfExportError,
        lineage_anchor=_EXPORT_LINEAGE_ANCHOR_ESTIMATE_VERSION,
        error_details_fn=_export_estimate_error_details,
    ),
    "revised_dxf": ExportKindSpec(
        format="dxf",
        media_type="application/dxf",
        render_fn=_render_revised_dxf_export_artifact,
        error_type=RevisedDxfExportError,
        lineage_anchor=_EXPORT_LINEAGE_ANCHOR_CHANGESET,
        error_details_fn=_export_changeset_error_details,
        error_mapper_fn=_map_revised_dxf_export_error,
    ),
    "dxf": ExportKindSpec(
        format="dxf",
        media_type="application/dxf",
        render_fn=_render_dxf_export_artifact,
        error_type=RevisedDxfExportError,
        lineage_anchor=_EXPORT_LINEAGE_ANCHOR_REVISION,
        error_details_fn=_export_revision_error_details,
        error_mapper_fn=_map_revised_dxf_export_error,
    ),
}


def get_export_kind_spec(export_kind: str) -> ExportKindSpec:
    """Return the worker registry entry for a supported export kind."""
    export_spec = EXPORT_KIND_SPECS.get(export_kind)
    if export_spec is None:
        raise _build_export_job_input_error(
            "Export job kind is not supported by the worker.",
            details={"export_kind": export_kind},
        )
    return export_spec


def build_export_artifact_name(
    *,
    export_kind: str,
    export_format: str,
    drawing_revision_id: UUID,
    changeset_id: UUID | None = None,
    quantity_takeoff_id: UUID | None = None,
    estimate_version_id: UUID | None = None,
) -> str:
    """Build a deterministic filename for a generated export artifact."""
    export_spec = get_export_kind_spec(export_kind)
    if export_spec.lineage_anchor == _EXPORT_LINEAGE_ANCHOR_REVISION:
        return f"revision-{drawing_revision_id}.{export_format}"
    if export_spec.lineage_anchor == _EXPORT_LINEAGE_ANCHOR_CHANGESET:
        assert changeset_id is not None
        return f"changeset-{changeset_id}.{export_format}"
    if export_spec.lineage_anchor == _EXPORT_LINEAGE_ANCHOR_QUANTITY_TAKEOFF:
        assert quantity_takeoff_id is not None
        return f"quantity-takeoff-{quantity_takeoff_id}.{export_format}"
    assert export_spec.lineage_anchor == _EXPORT_LINEAGE_ANCHOR_ESTIMATE_VERSION
    assert estimate_version_id is not None
    return f"estimate-{estimate_version_id}.{export_format}"


async def render_export_artifact(
    session: AsyncSession,
    execution: _ExportExecutionInput,
) -> ExportArtifact:
    """Render bytes for a supported export job."""
    export_spec = get_export_kind_spec(execution.export_kind)
    try:
        result = await export_spec.render_fn(session, execution)
    except Exception as exc:
        if isinstance(exc, export_spec.error_type):
            if export_spec.error_mapper_fn is not None:
                raise export_spec.error_mapper_fn(exc, execution) from exc
            raise _build_export_job_input_error(
                str(exc),
                error_code=ErrorCode.NOT_FOUND,
                details=export_spec.error_details_fn(execution),
            ) from exc
        raise

    if result.media_type != execution.media_type:
        raise ValueError("Rendered export media type does not match the persisted export job input")

    computed_checksum = hashlib.sha256(result.content_bytes).hexdigest()
    if computed_checksum != result.checksum_sha256:
        raise ValueError("Rendered export checksum does not match the generated bytes")

    if len(result.content_bytes) != result.size_bytes:
        raise ValueError("Rendered export size does not match the generated bytes")

    return result
