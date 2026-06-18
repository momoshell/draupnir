"""Pure unit tests for quantity-takeoff execution-input validation (issue D7b).

These exercise the lineage/gate/materialization rules with an in-memory
``QuantityRowLoader`` fake — no database, no session. The fake returns duck-typed
rows (cast to the model types); the builder only reads attributes off them.
"""
# The fake loader's keyword-only params mirror the protocol exactly (the builder
# calls them by keyword), so they can't be underscore-prefixed; ignore unused-arg.
# ruff: noqa: ARG002

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest

from app.core.errors import ErrorCode
from app.jobs.lifecycle import _RevisionConflictError, _StaleJobAttemptError
from app.jobs.quantity_execution_input import (
    _QuantityTakeoffJobError,
    build_quantity_takeoff_execution_input,
)
from app.models.drawing_revision import DrawingRevision
from app.models.job import JobType
from app.models.revision_materialization import RevisionEntity, RevisionEntityManifest
from app.models.validation_report import ValidationReport

PROJECT_ID = uuid.uuid4()
FILE_ID = uuid.uuid4()
JOB_ID = uuid.uuid4()
REVISION_ID = uuid.uuid4()
TOKEN = uuid.uuid4()


class _FakeLoader:
    """In-memory QuantityRowLoader returning preconfigured rows."""

    def __init__(
        self,
        *,
        job: Any = None,
        drawing_revision: Any = None,
        report: Any = None,
        manifest: Any = None,
        entities: Any = None,
    ) -> None:
        self._job = job
        self._drawing_revision = drawing_revision
        self._report = report
        self._manifest = manifest
        self._entities = entities if entities is not None else []

    async def get_job(self, _job_id: UUID) -> Any:
        return self._job

    async def get_drawing_revision(self, _revision_id: UUID) -> Any:
        return self._drawing_revision

    async def get_validation_report(self, *, project_id: UUID, drawing_revision_id: UUID) -> Any:
        return self._report

    async def get_entity_manifest(
        self, *, project_id: UUID, source_file_id: UUID, drawing_revision_id: UUID
    ) -> Any:
        return self._manifest

    async def get_revision_entities(
        self, *, project_id: UUID, source_file_id: UUID, drawing_revision_id: UUID
    ) -> Any:
        return self._entities


def _job(**overrides: Any) -> Any:
    base = {
        "status": "running",
        "attempt_token": TOKEN,
        "job_type": JobType.QUANTITY_TAKEOFF.value,
        "base_revision_id": REVISION_ID,
        "project_id": PROJECT_ID,
        "file_id": FILE_ID,
        "id": JOB_ID,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def _revision(**overrides: Any) -> DrawingRevision:
    base = {"id": REVISION_ID, "project_id": PROJECT_ID, "source_file_id": FILE_ID}
    base.update(overrides)
    return cast(DrawingRevision, SimpleNamespace(**base))


def _report(**overrides: Any) -> ValidationReport:
    base = {
        "drawing_revision_id": REVISION_ID,
        "review_state": "approved",
        "validation_status": "valid",
        "quantity_gate": "allowed",
        "effective_confidence": 0.9,
    }
    base.update(overrides)
    return cast(ValidationReport, SimpleNamespace(**base))


def _manifest(entities: int | None) -> RevisionEntityManifest:
    counts = {"entities": entities} if entities is not None else {}
    return cast(RevisionEntityManifest, SimpleNamespace(counts_json=counts))


def _entity(entity_id: str) -> RevisionEntity:
    return cast(
        RevisionEntity,
        SimpleNamespace(
            entity_id=entity_id,
            entity_type="line",
            sequence_index=0,
            geometry_json={},
            properties_json={},
            provenance_json={},
            canonical_entity_json=None,
            source_identity=None,
            source_hash=None,
        ),
    )


async def _build(loader: _FakeLoader) -> Any:
    return await build_quantity_takeoff_execution_input(JOB_ID, attempt_token=TOKEN, loader=loader)


async def test_allowed_gate_builds_input_with_mapped_entities() -> None:
    loader = _FakeLoader(
        job=_job(),
        drawing_revision=_revision(),
        report=_report(),
        manifest=_manifest(2),
        entities=[_entity("e1"), _entity("e2")],
    )
    execution = await _build(loader)
    assert execution.drawing_revision_id == REVISION_ID
    assert execution.validation_status == "valid"
    assert [e.entity_id for e in execution.entities] == ["e1", "e2"]
    assert execution.gate.status == "allowed"


async def test_missing_job_raises_lookup_error() -> None:
    with pytest.raises(LookupError):
        await _build(_FakeLoader(job=None))


async def test_stale_attempt_token_raises() -> None:
    with pytest.raises(_StaleJobAttemptError):
        await _build(_FakeLoader(job=_job(attempt_token=uuid.uuid4())))


async def test_missing_base_revision_raises_conflict() -> None:
    with pytest.raises(_RevisionConflictError):
        await _build(_FakeLoader(job=_job(base_revision_id=None)))


async def test_missing_validation_report_is_not_found() -> None:
    loader = _FakeLoader(job=_job(), drawing_revision=_revision(), report=None)
    with pytest.raises(_QuantityTakeoffJobError) as exc:
        await _build(loader)
    assert exc.value.error_code == ErrorCode.NOT_FOUND


async def test_gated_report_still_loads_entities_and_records_gate() -> None:
    # A review-gated/blocked report no longer short-circuits: it loads the manifest
    # and entities like an allowed gate, recording the gate as informational metadata.
    loader = _FakeLoader(
        job=_job(),
        drawing_revision=_revision(),
        report=_report(quantity_gate="review_gated", review_state="review_required"),
        manifest=_manifest(2),
        entities=[_entity("e1"), _entity("e2")],
    )
    execution = await _build(loader)
    assert [e.entity_id for e in execution.entities] == ["e1", "e2"]
    # quantity_gate is no longer carried on the execution input (Path B 5c); the
    # informational gate metadata still reflects the (now-vestigial) report values.
    assert execution.gate.status == "review_gated"
    assert execution.gate.reason == "review_required"


async def test_missing_manifest_is_materialization_error() -> None:
    loader = _FakeLoader(job=_job(), drawing_revision=_revision(), report=_report(), manifest=None)
    with pytest.raises(_QuantityTakeoffJobError) as exc:
        await _build(loader)
    assert exc.value.error_code == ErrorCode.NORMALIZED_ENTITIES_NOT_MATERIALIZED


async def test_entity_count_mismatch_is_materialization_error() -> None:
    loader = _FakeLoader(
        job=_job(),
        drawing_revision=_revision(),
        report=_report(),
        manifest=_manifest(3),  # expects 3
        entities=[_entity("e1")],  # only 1 loaded
    )
    with pytest.raises(_QuantityTakeoffJobError) as exc:
        await _build(loader)
    assert exc.value.error_code == ErrorCode.NORMALIZED_ENTITIES_NOT_MATERIALIZED
    assert exc.value.details == {
        "drawing_revision_id": str(REVISION_ID),
        "expected_entities": 3,
        "loaded_entities": 1,
    }


async def test_revision_belongs_to_other_file_is_rejected() -> None:
    loader = _FakeLoader(job=_job(), drawing_revision=_revision(source_file_id=uuid.uuid4()))
    with pytest.raises(ValueError, match="does not belong to the source file"):
        await _build(loader)
