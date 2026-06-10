"""Pure builders for persisted validation-report and artifact-lineage JSON.

Extracted from ``worker.py``: shape deterministic JSON for validation reports
(ingest + changeset) and generated-artifact / debug-overlay lineage. Pure
transformations over their arguments — no DB access, no worker-module state,
no monkeypatch seams.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

from app.ingestion.finalization import IngestFinalizationPayload
from app.models.file import File
from app.models.job import Job
from app.models.validation_report import ValidationReport

if TYPE_CHECKING:
    # Type-only: the annotation is a string (PEP 563), so this import is never
    # executed at runtime and introduces no import cycle with worker.py.
    from app.jobs.worker import _ExportExecutionInput


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


def _build_changeset_validation_report_json(
    base_report: ValidationReport,
    *,
    change_set_id: UUID,
    drawing_revision_id: UUID,
    predecessor_revision_id: UUID,
    pinned_validation_result_id: UUID | None,
    pinned_validation_status: str | None,
    source_job_id: UUID,
    validation_report_id: UUID,
    generated_at: datetime,
) -> dict[str, Any]:
    """Copy a predecessor validation report onto a changeset-origin revision."""
    base_json = base_report.report_json
    report_json = deepcopy(base_json) if isinstance(base_json, dict) else {}

    validator_json = report_json.get("validator")
    validator = dict(validator_json) if isinstance(validator_json, dict) else {}
    validator["name"] = base_report.validator_name
    validator["version"] = base_report.validator_version

    summary_json = report_json.get("summary")
    summary = dict(summary_json) if isinstance(summary_json, dict) else {}
    summary["validation_status"] = base_report.validation_status
    summary["review_state"] = base_report.review_state
    summary["quantity_gate"] = base_report.quantity_gate
    summary["effective_confidence"] = base_report.effective_confidence

    checks_json = report_json.get("checks")
    checks = list(checks_json) if isinstance(checks_json, list) else []
    if not checks:
        checks.append(
            {
                "code": "changeset_validation_report_persisted",
                "status": "passed",
                "message": (
                    "Changeset-origin revisions inherit the predecessor validation gate "
                    "for quantity workflow compatibility."
                ),
            }
        )

    provenance_json = report_json.get("provenance")
    provenance = dict(provenance_json) if isinstance(provenance_json, dict) else {}
    provenance["source_validation_report_id"] = str(base_report.id)
    provenance["source_drawing_revision_id"] = str(predecessor_revision_id)
    provenance["change_set_id"] = str(change_set_id)
    if pinned_validation_result_id is not None:
        provenance["pinned_validation_result_id"] = str(pinned_validation_result_id)
    if pinned_validation_status is not None:
        provenance["pinned_validation_status"] = pinned_validation_status

    report_json["validation_report_id"] = str(validation_report_id)
    report_json["drawing_revision_id"] = str(drawing_revision_id)
    report_json["source_job_id"] = str(source_job_id)
    report_json["validation_report_schema_version"] = base_report.validation_report_schema_version
    report_json["canonical_entity_schema_version"] = base_report.canonical_entity_schema_version
    report_json["validation_status"] = base_report.validation_status
    report_json["review_state"] = base_report.review_state
    report_json["quantity_gate"] = base_report.quantity_gate
    report_json["effective_confidence"] = base_report.effective_confidence
    report_json["validator"] = validator
    report_json["summary"] = summary
    report_json["checks"] = checks
    report_json["provenance"] = provenance
    report_json["generated_at"] = generated_at.isoformat()
    report_json["change_set_id"] = str(change_set_id)
    report_json["predecessor_revision_id"] = str(predecessor_revision_id)

    return report_json


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


def _build_export_artifact_lineage_json(
    *,
    source_file: File,
    job: Job,
    execution: _ExportExecutionInput,
) -> dict[str, Any]:
    """Build lineage metadata for a persisted export artifact."""
    drawing_revision_lineage: dict[str, Any] = {"id": str(execution.drawing_revision_id)}
    if execution.changeset_id is not None:
        drawing_revision_lineage["changeset_id"] = str(execution.changeset_id)

    export_lineage: dict[str, Any] = {
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
    }
    if execution.changeset_id is not None:
        export_lineage["changeset_id"] = str(execution.changeset_id)

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
        "drawing_revision": drawing_revision_lineage,
        "export": export_lineage,
    }
