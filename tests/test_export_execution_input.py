"""Pure unit tests for export execution-input validation (issue D7a).

These exercise the lineage/gate/trusted-totals rules with an in-memory
``ExportRowLoader`` fake — no database, no session. The fake returns duck-typed
rows (cast to the model types); the builder only reads attributes off them.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest

from app.core.errors import ErrorCode
from app.jobs.export_execution_input import (
    _EXPORT_LINEAGE_ANCHOR_CHANGESET,
    _EXPORT_LINEAGE_ANCHOR_ESTIMATE_VERSION,
    _EXPORT_LINEAGE_ANCHOR_QUANTITY_TAKEOFF,
    _EXPORT_LINEAGE_ANCHOR_REVISION,
    _ExportJobInputError,
    build_export_execution_input,
)
from app.jobs.lifecycle import _RevisionConflictError, _StaleJobAttemptError
from app.models.drawing_revision import DrawingRevision
from app.models.estimate_version import EstimateVersion
from app.models.export_job_input import ExportJobInput
from app.models.job import Job, JobType
from app.models.quantity_takeoff import QuantityTakeoff

PROJECT_ID = uuid.uuid4()
FILE_ID = uuid.uuid4()
JOB_ID = uuid.uuid4()
REVISION_ID = uuid.uuid4()
TOKEN = uuid.uuid4()

_SPECS = {
    "revision_json": SimpleNamespace(
        format="json", media_type="application/json", lineage_anchor=_EXPORT_LINEAGE_ANCHOR_REVISION
    ),
    "revised_dxf": SimpleNamespace(
        format="dxf", media_type="application/dxf", lineage_anchor=_EXPORT_LINEAGE_ANCHOR_CHANGESET
    ),
    "quantity_csv": SimpleNamespace(
        format="csv",
        media_type="text/csv",
        lineage_anchor=_EXPORT_LINEAGE_ANCHOR_QUANTITY_TAKEOFF,
    ),
    "estimate_csv": SimpleNamespace(
        format="csv",
        media_type="text/csv",
        lineage_anchor=_EXPORT_LINEAGE_ANCHOR_ESTIMATE_VERSION,
    ),
}


def _resolve_spec(export_kind: str) -> Any:
    return _SPECS[export_kind]


def _artifact_name(*, export_kind: str, export_format: str, **_: object) -> str:
    return f"{export_kind}.{export_format}"


class _FakeLoader:
    """In-memory ExportRowLoader returning preconfigured rows."""

    def __init__(
        self,
        *,
        job: Any = None,
        export_input: Any = None,
        drawing_revision: Any = None,
        quantity_takeoff: Any = None,
        estimate_version: Any = None,
    ) -> None:
        self._job = job
        self._export_input = export_input
        self._drawing_revision = drawing_revision
        self._quantity_takeoff = quantity_takeoff
        self._estimate_version = estimate_version

    async def get_job(self, _job_id: UUID) -> Any:
        return self._job

    async def get_export_job_input(self, _job_id: UUID) -> Any:
        return self._export_input

    async def get_drawing_revision(self, _revision_id: UUID) -> Any:
        return self._drawing_revision

    async def get_quantity_takeoff(self, _takeoff_id: UUID) -> Any:
        return self._quantity_takeoff

    async def get_estimate_version(self, _version_id: UUID) -> Any:
        return self._estimate_version


def _job(**overrides: Any) -> Job:
    base = {
        "status": "running",
        "attempt_token": TOKEN,
        "job_type": JobType.EXPORT.value,
        "base_revision_id": REVISION_ID,
        "project_id": PROJECT_ID,
        "file_id": FILE_ID,
        "id": JOB_ID,
    }
    base.update(overrides)
    return cast(Job, SimpleNamespace(**base))


def _export_input(**overrides: Any) -> ExportJobInput:
    base = {
        "project_id": PROJECT_ID,
        "source_file_id": FILE_ID,
        "drawing_revision_id": REVISION_ID,
        "source_job_id": JOB_ID,
        "source_job_type": JobType.EXPORT.value,
        "export_kind": "revision_json",
        "export_format": "json",
        "media_type": "application/json",
        "options_json": {"k": "v"},
        "quantity_takeoff_id": None,
        "estimate_version_id": None,
        "quantity_gate": None,
        "trusted_totals": None,
    }
    base.update(overrides)
    return cast(ExportJobInput, SimpleNamespace(**base))


def _revision(**overrides: Any) -> DrawingRevision:
    base = {
        "id": REVISION_ID,
        "project_id": PROJECT_ID,
        "source_file_id": FILE_ID,
        "revision_kind": "ingest",
        "changeset_id": None,
    }
    base.update(overrides)
    return cast(DrawingRevision, SimpleNamespace(**base))


async def _build(loader: _FakeLoader) -> Any:
    return await build_export_execution_input(
        JOB_ID,
        attempt_token=TOKEN,
        loader=loader,
        resolve_export_spec=_resolve_spec,
        build_artifact_name=_artifact_name,
    )


async def test_revision_scoped_export_builds_execution_input() -> None:
    loader = _FakeLoader(job=_job(), export_input=_export_input(), drawing_revision=_revision())
    execution = await _build(loader)
    assert execution.drawing_revision_id == REVISION_ID
    assert execution.export_kind == "revision_json"
    assert execution.artifact_name == "revision_json.json"
    assert execution.quantity_takeoff_id is None
    assert execution.estimate_version_id is None


async def test_missing_job_raises_lookup_error() -> None:
    with pytest.raises(LookupError):
        await _build(_FakeLoader(job=None))


async def test_stale_attempt_token_raises() -> None:
    loader = _FakeLoader(job=_job(attempt_token=uuid.uuid4()))
    with pytest.raises(_StaleJobAttemptError):
        await _build(loader)


async def test_missing_base_revision_raises_conflict() -> None:
    loader = _FakeLoader(job=_job(base_revision_id=None))
    with pytest.raises(_RevisionConflictError):
        await _build(loader)


async def test_revision_input_with_unexpected_quantity_lineage_is_rejected() -> None:
    loader = _FakeLoader(
        job=_job(),
        export_input=_export_input(quantity_takeoff_id=uuid.uuid4()),
        drawing_revision=_revision(),
    )
    with pytest.raises(_ExportJobInputError) as exc:
        await _build(loader)
    assert exc.value.error_code == ErrorCode.INPUT_INVALID


async def test_changeset_export_requires_changeset_origin_revision() -> None:
    loader = _FakeLoader(
        job=_job(),
        export_input=_export_input(
            export_kind="revised_dxf", export_format="dxf", media_type="application/dxf"
        ),
        drawing_revision=_revision(revision_kind="ingest", changeset_id=None),
    )
    with pytest.raises(_ExportJobInputError):
        await _build(loader)


async def test_changeset_export_succeeds_with_changeset_revision() -> None:
    changeset_id = uuid.uuid4()
    loader = _FakeLoader(
        job=_job(),
        export_input=_export_input(
            export_kind="revised_dxf", export_format="dxf", media_type="application/dxf"
        ),
        drawing_revision=_revision(revision_kind="changeset", changeset_id=changeset_id),
    )
    execution = await _build(loader)
    assert execution.changeset_id == changeset_id


async def test_quantity_export_builds_from_gated_untrusted_takeoff() -> None:
    # Path B 4: exports are no longer gated on quantity_gate / trusted_totals; a
    # review-gated, untrusted takeoff builds as long as its lineage is consistent.
    takeoff_id = uuid.uuid4()
    takeoff = cast(
        QuantityTakeoff,
        SimpleNamespace(
            id=takeoff_id,
            project_id=PROJECT_ID,
            source_file_id=FILE_ID,
            drawing_revision_id=REVISION_ID,
            quantity_gate="blocked",
            trusted_totals=False,
            source_job_type=JobType.QUANTITY_TAKEOFF.value,
        ),
    )
    loader = _FakeLoader(
        job=_job(),
        export_input=_export_input(
            export_kind="quantity_csv",
            export_format="csv",
            media_type="text/csv",
            quantity_takeoff_id=takeoff_id,
            quantity_gate="blocked",
            trusted_totals=False,
        ),
        drawing_revision=_revision(),
        quantity_takeoff=takeoff,
    )
    execution = await _build(loader)
    assert execution.quantity_takeoff_id == takeoff_id


async def test_quantity_export_missing_takeoff_row_is_not_found() -> None:
    takeoff_id = uuid.uuid4()
    loader = _FakeLoader(
        job=_job(),
        export_input=_export_input(
            export_kind="quantity_csv",
            export_format="csv",
            media_type="text/csv",
            quantity_takeoff_id=takeoff_id,
            quantity_gate="allowed",
            trusted_totals=True,
        ),
        drawing_revision=_revision(),
        quantity_takeoff=None,
    )
    with pytest.raises(_ExportJobInputError) as exc:
        await _build(loader)
    assert exc.value.error_code == ErrorCode.NOT_FOUND


async def test_quantity_export_builds_with_valid_takeoff() -> None:
    takeoff_id = uuid.uuid4()
    takeoff = cast(
        QuantityTakeoff,
        SimpleNamespace(
            id=takeoff_id,
            project_id=PROJECT_ID,
            source_file_id=FILE_ID,
            drawing_revision_id=REVISION_ID,
            quantity_gate="allowed",
            trusted_totals=True,
            source_job_type=JobType.QUANTITY_TAKEOFF.value,
        ),
    )
    loader = _FakeLoader(
        job=_job(),
        export_input=_export_input(
            export_kind="quantity_csv",
            export_format="csv",
            media_type="text/csv",
            quantity_takeoff_id=takeoff_id,
            quantity_gate="allowed",
            trusted_totals=True,
        ),
        drawing_revision=_revision(),
        quantity_takeoff=takeoff,
    )
    execution = await _build(loader)
    assert execution.quantity_takeoff_id == takeoff_id
    assert execution.estimate_version_id is None


async def test_estimate_export_lineage_mismatch_is_rejected() -> None:
    takeoff_id = uuid.uuid4()
    version_id = uuid.uuid4()
    takeoff = cast(
        QuantityTakeoff,
        SimpleNamespace(
            id=takeoff_id,
            project_id=PROJECT_ID,
            source_file_id=FILE_ID,
            drawing_revision_id=REVISION_ID,
            quantity_gate="allowed",
            trusted_totals=True,
            source_job_type=JobType.QUANTITY_TAKEOFF.value,
        ),
    )
    version = cast(
        EstimateVersion,
        SimpleNamespace(
            id=version_id,
            project_id=PROJECT_ID,
            source_file_id=FILE_ID,
            drawing_revision_id=uuid.uuid4(),  # mismatched revision
            quantity_takeoff_id=takeoff_id,
            quantity_gate="allowed",
            trusted_totals=True,
        ),
    )
    loader = _FakeLoader(
        job=_job(),
        export_input=_export_input(
            export_kind="estimate_csv",
            export_format="csv",
            media_type="text/csv",
            quantity_takeoff_id=takeoff_id,
            estimate_version_id=version_id,
            quantity_gate="allowed",
            trusted_totals=True,
        ),
        drawing_revision=_revision(),
        quantity_takeoff=takeoff,
        estimate_version=version,
    )
    with pytest.raises(_ExportJobInputError):
        await _build(loader)
