"""Shared immutable execution-input records for worker job processing.

Extracted from ``app.jobs.worker`` (issue #387). The execute stage builds these
and the per-type finalizers consume them, so they live in a neutral module both
can import without a circular dependency on worker.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from app.estimating.quantities.contracts import RevisionEntityInput, RevisionGateMetadata


@dataclass(frozen=True, slots=True)
class _ExportExecutionInput:
    """Resolved persisted inputs for a supported export job."""

    drawing_revision_id: UUID
    export_kind: str
    export_format: str
    media_type: str
    artifact_name: str
    options_json: dict[str, Any]
    changeset_id: UUID | None = None
    quantity_takeoff_id: UUID | None = None
    estimate_version_id: UUID | None = None


@dataclass(frozen=True, slots=True)
class _QuantityTakeoffExecutionInput:
    """Loaded immutable quantity takeoff execution inputs."""

    drawing_revision_id: UUID
    # Path B 5b: review_state / quantity_gate are vestigial (sourced from the now-NULL
    # validation report); removed entirely in 5c. validation_status is kept.
    review_state: str | None
    validation_status: str
    quantity_gate: str | None
    gate: RevisionGateMetadata
    entities: list[RevisionEntityInput]
