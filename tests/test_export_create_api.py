from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select

import app.db.session as session_module
from app.api.idempotency import hash_idempotency_key
from app.api.v1.revision_routes import exports as export_routes
from app.models.adapter_run_output import AdapterRunOutput
from app.models.cad_changeset import CadChangeSet
from app.models.drawing_revision import DrawingRevision
from app.models.estimate_version import EstimateVersion
from app.models.export_job_input import ExportJobInput
from app.models.extraction_profile import ExtractionProfile
from app.models.file import File
from app.models.generated_artifact import GeneratedArtifact
from app.models.idempotency_key import IdempotencyKey
from app.models.job import Job, JobType
from app.models.project import Project
from app.models.quantity_takeoff import (
    QuantityGate,
    QuantityReviewState,
    QuantityTakeoff,
    QuantityValidationStatus,
)
from app.schemas.export import EXPORT_KIND_MATRIX, ExportKind
from tests.conftest import requires_database

pytestmark = requires_database


@dataclass(slots=True)
class ExportLineage:
    project_id: UUID
    file_id: UUID
    revision_id: UUID
    changeset_revision_id: UUID
    takeoff_id: UUID
    estimate_version_id: UUID
    revision_source_job_id: UUID
    changeset_revision_source_job_id: UUID
    takeoff_source_job_id: UUID
    estimate_source_job_id: UUID


@dataclass(frozen=True, slots=True)
class ExportCreateCase:
    name: str
    export_kind: ExportKind
    body: dict[str, object]


EXPORT_CREATE_CASES = [
    ExportCreateCase(
        name="revision_json",
        export_kind=ExportKind.REVISION_JSON,
        body={"options": {"include_manifest": True, "layout_ids": ["sheet-a"]}},
    ),
    ExportCreateCase(
        name="quantity_csv",
        export_kind=ExportKind.QUANTITY_CSV,
        body={"options": {"dialect": "excel", "include_units": True}},
    ),
    ExportCreateCase(
        name="estimate_csv",
        export_kind=ExportKind.ESTIMATE_CSV,
        body={
            "export_kind": ExportKind.ESTIMATE_CSV.value,
            "options": {"template": "default", "include_assumptions": True},
        },
    ),
    ExportCreateCase(
        name="estimate_pdf",
        export_kind=ExportKind.ESTIMATE_PDF,
        body={
            "export_kind": ExportKind.ESTIMATE_PDF.value,
            "options": {"template": "default", "show_totals": True},
        },
    ),
    # Appended (not inserted) so existing positional EXPORT_CREATE_CASES[...] references hold.
    ExportCreateCase(
        name="dxf",
        export_kind=ExportKind.DXF,
        body={"options": {}},
    ),
]


async def _seed_export_lineage() -> ExportLineage:
    assert session_module.AsyncSessionLocal is not None

    async with session_module.AsyncSessionLocal() as session:
        project = Project(name="Export API Project")
        session.add(project)
        await session.flush()

        extraction_profile = ExtractionProfile(
            project_id=project.id,
            profile_version="1.0",
            layout_mode="model_space",
            xref_handling="bind",
            block_handling="expand",
        )
        session.add(extraction_profile)
        await session.flush()

        source_file = File(
            project_id=project.id,
            original_filename="plan.dxf",
            media_type="image/vnd.dxf",
            detected_format="dxf",
            storage_uri="file:///tmp/originals/plan.dxf",
            size_bytes=128,
            checksum_sha256="1" * 64,
            immutable=True,
            initial_extraction_profile_id=extraction_profile.id,
        )
        session.add(source_file)
        await session.flush()

        revision_source_job = Job(
            project_id=project.id,
            file_id=source_file.id,
            extraction_profile_id=extraction_profile.id,
            base_revision_id=None,
            parent_job_id=None,
            job_type=JobType.INGEST.value,
            status="succeeded",
            attempts=1,
            max_attempts=3,
            enqueue_status="pending",
            enqueue_attempts=0,
            cancel_requested=False,
        )
        session.add(revision_source_job)
        await session.flush()

        adapter_run_output = AdapterRunOutput(
            project_id=project.id,
            source_file_id=source_file.id,
            extraction_profile_id=extraction_profile.id,
            source_job_id=revision_source_job.id,
            adapter_key="fake-dxf",
            adapter_version="1.0",
            input_family="dxf",
            canonical_entity_schema_version="1.0",
            canonical_json={"entities": []},
            provenance_json={"origin": "test"},
            confidence_json={"entities": []},
            confidence_score=0.99,
            warnings_json=[],
            diagnostics_json={},
            result_checksum_sha256="2" * 64,
        )
        session.add(adapter_run_output)
        await session.flush()

        revision = DrawingRevision(
            project_id=project.id,
            source_file_id=source_file.id,
            extraction_profile_id=extraction_profile.id,
            source_job_id=revision_source_job.id,
            adapter_run_output_id=adapter_run_output.id,
            predecessor_revision_id=None,
            revision_sequence=1,
            revision_kind="ingest",
            review_state="approved",
            canonical_entity_schema_version="1.0",
            confidence_score=0.99,
        )
        session.add(revision)
        await session.flush()

        change_set = CadChangeSet(
            project_id=project.id,
            base_revision_id=revision.id,
            status="approved",
            created_by="test-suite",
        )
        session.add(change_set)
        await session.flush()

        changeset_revision_source_job = Job(
            project_id=project.id,
            file_id=source_file.id,
            extraction_profile_id=None,
            base_revision_id=revision.id,
            parent_job_id=revision_source_job.id,
            job_type=getattr(JobType, "CHANGESET_APPLY", JobType.EXPORT).value,
            status="succeeded",
            attempts=1,
            max_attempts=3,
            enqueue_status="pending",
            enqueue_attempts=0,
            cancel_requested=False,
        )
        session.add(changeset_revision_source_job)
        await session.flush()

        changeset_revision = DrawingRevision(
            project_id=project.id,
            source_file_id=source_file.id,
            extraction_profile_id=None,
            source_job_id=changeset_revision_source_job.id,
            adapter_run_output_id=None,
            predecessor_revision_id=revision.id,
            revision_sequence=2,
            revision_kind="changeset",
            review_state="approved",
            canonical_entity_schema_version="1.0",
            confidence_score=0.99,
            changeset_id=change_set.id,
        )
        session.add(changeset_revision)
        await session.flush()

        takeoff_source_job = Job(
            project_id=project.id,
            file_id=source_file.id,
            extraction_profile_id=None,
            base_revision_id=revision.id,
            parent_job_id=revision_source_job.id,
            job_type=JobType.QUANTITY_TAKEOFF.value,
            status="succeeded",
            attempts=1,
            max_attempts=3,
            enqueue_status="pending",
            enqueue_attempts=0,
            cancel_requested=False,
        )
        session.add(takeoff_source_job)
        await session.flush()

        takeoff = QuantityTakeoff(
            project_id=project.id,
            source_file_id=source_file.id,
            drawing_revision_id=revision.id,
            source_job_id=takeoff_source_job.id,
            review_state=QuantityReviewState.APPROVED.value,
            validation_status=QuantityValidationStatus.VALID.value,
            quantity_gate=QuantityGate.ALLOWED.value,
            trusted_totals=True,
        )
        session.add(takeoff)
        await session.flush()

        estimate_source_job = Job(
            project_id=project.id,
            file_id=source_file.id,
            extraction_profile_id=None,
            base_revision_id=revision.id,
            parent_job_id=takeoff_source_job.id,
            job_type=JobType.ESTIMATE.value,
            status="succeeded",
            attempts=1,
            max_attempts=3,
            enqueue_status="pending",
            enqueue_attempts=0,
            cancel_requested=False,
        )
        session.add(estimate_source_job)
        await session.flush()

        estimate_version = EstimateVersion(
            project_id=project.id,
            source_file_id=source_file.id,
            drawing_revision_id=revision.id,
            quantity_takeoff_id=takeoff.id,
            source_job_id=estimate_source_job.id,
            quantity_gate=QuantityGate.ALLOWED.value,
            trusted_totals=True,
            currency="GBP",
            subtotal_amount=Decimal("100.00"),
            tax_amount=Decimal("20.00"),
            total_amount=Decimal("120.00"),
        )
        session.add(estimate_version)
        await session.commit()

        return ExportLineage(
            project_id=project.id,
            file_id=source_file.id,
            revision_id=revision.id,
            changeset_revision_id=changeset_revision.id,
            takeoff_id=takeoff.id,
            estimate_version_id=estimate_version.id,
            revision_source_job_id=revision_source_job.id,
            changeset_revision_source_job_id=changeset_revision_source_job.id,
            takeoff_source_job_id=takeoff_source_job.id,
            estimate_source_job_id=estimate_source_job.id,
        )


def _route_path(case: ExportCreateCase, lineage: ExportLineage) -> str:
    if case.export_kind == ExportKind.REVISION_JSON:
        return f"/v1/revisions/{lineage.revision_id}/exports/revision-json"
    if case.export_kind == ExportKind.DXF:
        return f"/v1/revisions/{lineage.revision_id}/exports/dxf"
    if case.export_kind == ExportKind.QUANTITY_CSV:
        return (
            f"/v1/revisions/{lineage.revision_id}"
            f"/quantity-takeoffs/{lineage.takeoff_id}/exports/quantity-csv"
        )
    return (
        f"/v1/revisions/{lineage.revision_id}"
        f"/quantity-takeoffs/{lineage.takeoff_id}"
        f"/estimates/{lineage.estimate_version_id}/exports"
    )


def _expected_parent_job_id(case: ExportCreateCase, lineage: ExportLineage) -> UUID:
    if case.export_kind in (ExportKind.REVISION_JSON, ExportKind.DXF):
        return lineage.revision_source_job_id
    if case.export_kind == ExportKind.QUANTITY_CSV:
        return lineage.takeoff_source_job_id
    return lineage.estimate_source_job_id


async def _count_export_side_effects(lineage: ExportLineage) -> tuple[int, int, int]:
    return await _count_export_side_effects_for_revision(lineage, lineage.revision_id)


async def _count_export_side_effects_for_revision(
    lineage: ExportLineage,
    revision_id: UUID,
) -> tuple[int, int, int]:
    assert session_module.AsyncSessionLocal is not None

    async with session_module.AsyncSessionLocal() as session:
        export_job_count = await session.scalar(
            select(func.count())
            .select_from(Job)
            .where(
                Job.job_type == JobType.EXPORT.value,
                Job.project_id == lineage.project_id,
                Job.file_id == lineage.file_id,
                Job.base_revision_id == revision_id,
            )
        )
        export_input_count = await session.scalar(
            select(func.count())
            .select_from(ExportJobInput)
            .where(
                ExportJobInput.project_id == lineage.project_id,
                ExportJobInput.source_file_id == lineage.file_id,
                ExportJobInput.drawing_revision_id == revision_id,
            )
        )
        generated_artifact_count = await session.scalar(
            select(func.count())
            .select_from(GeneratedArtifact)
            .where(
                GeneratedArtifact.project_id == lineage.project_id,
                GeneratedArtifact.source_file_id == lineage.file_id,
                GeneratedArtifact.drawing_revision_id == revision_id,
            )
        )

    assert export_job_count is not None
    assert export_input_count is not None
    assert generated_artifact_count is not None
    return export_job_count, export_input_count, generated_artifact_count


async def _count_idempotency_rows(idempotency_key: str) -> int:
    assert session_module.AsyncSessionLocal is not None

    async with session_module.AsyncSessionLocal() as session:
        row_count = await session.scalar(
            select(func.count())
            .select_from(IdempotencyKey)
            .where(IdempotencyKey.key_hash == hash_idempotency_key(idempotency_key))
        )

    assert row_count is not None
    return row_count


async def _set_source_file_deleted(lineage: ExportLineage) -> None:
    assert session_module.AsyncSessionLocal is not None

    async with session_module.AsyncSessionLocal() as session:
        source_file = await session.get(File, lineage.file_id)
        assert source_file is not None
        source_file.deleted_at = datetime.now(UTC)
        await session.commit()


async def _set_project_deleted(lineage: ExportLineage) -> None:
    assert session_module.AsyncSessionLocal is not None

    async with session_module.AsyncSessionLocal() as session:
        project = await session.get(Project, lineage.project_id)
        assert project is not None
        project.deleted_at = datetime.now(UTC)
        await session.commit()


async def _create_takeoff(
    lineage: ExportLineage,
    *,
    trusted_totals: bool,
    quantity_gate: str,
) -> UUID:
    assert session_module.AsyncSessionLocal is not None

    async with session_module.AsyncSessionLocal() as session:
        takeoff_source_job = Job(
            project_id=lineage.project_id,
            file_id=lineage.file_id,
            extraction_profile_id=None,
            base_revision_id=lineage.revision_id,
            parent_job_id=lineage.revision_source_job_id,
            job_type=JobType.QUANTITY_TAKEOFF.value,
            status="succeeded",
            attempts=1,
            max_attempts=3,
            enqueue_status="pending",
            enqueue_attempts=0,
            cancel_requested=False,
        )
        session.add(takeoff_source_job)
        await session.flush()

        takeoff = QuantityTakeoff(
            project_id=lineage.project_id,
            source_file_id=lineage.file_id,
            drawing_revision_id=lineage.revision_id,
            source_job_id=takeoff_source_job.id,
            review_state=QuantityReviewState.APPROVED.value,
            validation_status=QuantityValidationStatus.VALID.value,
            quantity_gate=quantity_gate,
            trusted_totals=trusted_totals,
        )
        session.add(takeoff)
        await session.commit()

        return takeoff.id


async def _create_alternate_takeoff_and_estimate(lineage: ExportLineage) -> tuple[UUID, UUID]:
    assert session_module.AsyncSessionLocal is not None

    async with session_module.AsyncSessionLocal() as session:
        alternate_takeoff_source_job = Job(
            project_id=lineage.project_id,
            file_id=lineage.file_id,
            extraction_profile_id=None,
            base_revision_id=lineage.revision_id,
            parent_job_id=lineage.revision_source_job_id,
            job_type=JobType.QUANTITY_TAKEOFF.value,
            status="succeeded",
            attempts=1,
            max_attempts=3,
            enqueue_status="pending",
            enqueue_attempts=0,
            cancel_requested=False,
        )
        session.add(alternate_takeoff_source_job)
        await session.flush()

        alternate_takeoff = QuantityTakeoff(
            project_id=lineage.project_id,
            source_file_id=lineage.file_id,
            drawing_revision_id=lineage.revision_id,
            source_job_id=alternate_takeoff_source_job.id,
            review_state=QuantityReviewState.APPROVED.value,
            validation_status=QuantityValidationStatus.VALID.value,
            quantity_gate=QuantityGate.ALLOWED.value,
            trusted_totals=True,
        )
        session.add(alternate_takeoff)
        await session.flush()

        alternate_estimate_source_job = Job(
            project_id=lineage.project_id,
            file_id=lineage.file_id,
            extraction_profile_id=None,
            base_revision_id=lineage.revision_id,
            parent_job_id=alternate_takeoff_source_job.id,
            job_type=JobType.ESTIMATE.value,
            status="succeeded",
            attempts=1,
            max_attempts=3,
            enqueue_status="pending",
            enqueue_attempts=0,
            cancel_requested=False,
        )
        session.add(alternate_estimate_source_job)
        await session.flush()

        alternate_estimate_version = EstimateVersion(
            project_id=lineage.project_id,
            source_file_id=lineage.file_id,
            drawing_revision_id=lineage.revision_id,
            quantity_takeoff_id=alternate_takeoff.id,
            source_job_id=alternate_estimate_source_job.id,
            quantity_gate=QuantityGate.ALLOWED.value,
            trusted_totals=True,
            currency="GBP",
            subtotal_amount=Decimal("50.00"),
            tax_amount=Decimal("10.00"),
            total_amount=Decimal("60.00"),
        )
        session.add(alternate_estimate_version)
        await session.commit()

        return alternate_takeoff.id, alternate_estimate_version.id


@pytest.mark.parametrize(
    "case",
    EXPORT_CREATE_CASES,
    ids=[case.name for case in EXPORT_CREATE_CASES],
)
async def test_create_export_persists_pending_job_and_input(
    async_client: AsyncClient,
    case: ExportCreateCase,
) -> None:
    lineage = await _seed_export_lineage()

    response = await async_client.post(_route_path(case, lineage), json=case.body)

    assert response.status_code == 202
    body = response.json()
    job_id = UUID(body["id"])
    assert body["project_id"] == str(lineage.project_id)
    assert body["file_id"] == str(lineage.file_id)
    assert body["base_revision_id"] == str(lineage.revision_id)
    assert body["parent_job_id"] == str(_expected_parent_job_id(case, lineage))
    assert body["job_type"] == JobType.EXPORT.value
    assert body["status"] == "pending"
    assert body["attempts"] == 0
    assert body["max_attempts"] == 3
    assert body["cancel_requested"] is False

    expected_format, expected_media_type = EXPORT_KIND_MATRIX[case.export_kind]

    assert session_module.AsyncSessionLocal is not None
    async with session_module.AsyncSessionLocal() as session:
        export_jobs = list(
            await session.scalars(
                select(Job).where(
                    Job.job_type == JobType.EXPORT.value,
                    Job.project_id == lineage.project_id,
                    Job.file_id == lineage.file_id,
                    Job.base_revision_id == lineage.revision_id,
                )
            )
        )
        assert len(export_jobs) == 1
        export_job = export_jobs[0]
        assert export_job.id == job_id
        assert export_job.project_id == lineage.project_id
        assert export_job.file_id == lineage.file_id
        assert export_job.base_revision_id == lineage.revision_id
        assert export_job.parent_job_id == _expected_parent_job_id(case, lineage)
        assert export_job.status == "pending"
        assert export_job.enqueue_status == "pending"

        export_inputs = list(
            await session.scalars(
                select(ExportJobInput).where(
                    ExportJobInput.project_id == lineage.project_id,
                    ExportJobInput.source_file_id == lineage.file_id,
                    ExportJobInput.drawing_revision_id == lineage.revision_id,
                )
            )
        )
        assert len(export_inputs) == 1
        export_input = export_inputs[0]
        assert export_input.source_job_id == export_job.id
        assert export_input.project_id == lineage.project_id
        assert export_input.source_file_id == lineage.file_id
        assert export_input.drawing_revision_id == lineage.revision_id
        assert export_input.source_job_type == JobType.EXPORT.value
        assert export_input.export_kind == case.export_kind.value
        assert export_input.export_format == expected_format.value
        assert export_input.media_type == expected_media_type
        assert export_input.options_json == case.body["options"]

        if case.export_kind in (ExportKind.REVISION_JSON, ExportKind.DXF):
            assert export_input.quantity_takeoff_id is None
            assert export_input.quantity_gate is None
            assert export_input.trusted_totals is None
            assert export_input.estimate_version_id is None
        elif case.export_kind == ExportKind.QUANTITY_CSV:
            assert export_input.quantity_takeoff_id == lineage.takeoff_id
            # Path B 5c: quantity_gate / trusted_totals are no longer copied onto exports.
            assert export_input.quantity_gate is None
            assert export_input.trusted_totals is None
            assert export_input.estimate_version_id is None
        else:
            assert export_input.quantity_takeoff_id == lineage.takeoff_id
            # Path B 5c: quantity_gate / trusted_totals are no longer copied onto exports.
            assert export_input.quantity_gate is None
            assert export_input.trusted_totals is None
            assert export_input.estimate_version_id == lineage.estimate_version_id

        generated_artifact_count = await session.scalar(
            select(func.count())
            .select_from(GeneratedArtifact)
            .where(
                GeneratedArtifact.project_id == lineage.project_id,
                GeneratedArtifact.source_file_id == lineage.file_id,
                GeneratedArtifact.drawing_revision_id == lineage.revision_id,
            )
        )
        assert generated_artifact_count == 0


async def test_create_revised_dxf_export_persists_pending_job_and_input(
    async_client: AsyncClient,
) -> None:
    lineage = await _seed_export_lineage()

    response = await async_client.post(
        f"/v1/revisions/{lineage.changeset_revision_id}/exports/revised-dxf",
        json={"options": {"target_version": "R2010", "include_review_flags": True}},
    )

    assert response.status_code == 202
    body = response.json()
    job_id = UUID(body["id"])
    assert body["project_id"] == str(lineage.project_id)
    assert body["file_id"] == str(lineage.file_id)
    assert body["base_revision_id"] == str(lineage.changeset_revision_id)
    assert body["parent_job_id"] == str(lineage.changeset_revision_source_job_id)
    assert body["job_type"] == JobType.EXPORT.value
    assert body["status"] == "pending"

    expected_format, expected_media_type = EXPORT_KIND_MATRIX[ExportKind.REVISED_DXF]

    assert session_module.AsyncSessionLocal is not None
    async with session_module.AsyncSessionLocal() as session:
        export_jobs = list(
            await session.scalars(
                select(Job).where(
                    Job.job_type == JobType.EXPORT.value,
                    Job.project_id == lineage.project_id,
                    Job.file_id == lineage.file_id,
                    Job.base_revision_id == lineage.changeset_revision_id,
                )
            )
        )
        assert len(export_jobs) == 1
        export_job = export_jobs[0]
        assert export_job.id == job_id
        assert export_job.parent_job_id == lineage.changeset_revision_source_job_id

        export_inputs = list(
            await session.scalars(
                select(ExportJobInput).where(
                    ExportJobInput.project_id == lineage.project_id,
                    ExportJobInput.source_file_id == lineage.file_id,
                    ExportJobInput.drawing_revision_id == lineage.changeset_revision_id,
                )
            )
        )
        assert len(export_inputs) == 1
        export_input = export_inputs[0]
        assert export_input.source_job_id == export_job.id
        assert export_input.export_kind == ExportKind.REVISED_DXF.value
        assert export_input.export_format == expected_format.value
        assert export_input.media_type == expected_media_type
        assert export_input.options_json == {
            "target_version": "R2010",
            "include_review_flags": True,
        }
        assert export_input.quantity_takeoff_id is None
        assert export_input.quantity_gate is None
        assert export_input.trusted_totals is None
        assert export_input.estimate_version_id is None

    assert await _count_export_side_effects_for_revision(
        lineage,
        lineage.changeset_revision_id,
    ) == (1, 1, 0)


async def test_create_revised_dxf_export_rejects_non_changeset_revision_before_claim(
    async_client: AsyncClient,
) -> None:
    lineage = await _seed_export_lineage()
    idempotency_key = f"revised-dxf-ingest-{uuid4().hex}"

    response = await async_client.post(
        f"/v1/revisions/{lineage.revision_id}/exports/revised-dxf",
        json={"options": {"target_version": "R2010"}},
        headers={"Idempotency-Key": idempotency_key},
    )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "INPUT_INVALID"
    assert await _count_export_side_effects(lineage) == (0, 0, 0)
    assert await _count_idempotency_rows(idempotency_key) == 0


@pytest.mark.parametrize(
    ("case", "lineage_extras"),
    [
        (
            EXPORT_CREATE_CASES[0],
            {"project_id": "ignored", "file_id": "ignored", "revision_id": "ignored"},
        ),
        (
            EXPORT_CREATE_CASES[1],
            {
                "project_id": "ignored",
                "file_id": "ignored",
                "revision_id": "ignored",
                "takeoff_id": "ignored",
            },
        ),
        (
            EXPORT_CREATE_CASES[2],
            {
                "project_id": "ignored",
                "file_id": "ignored",
                "revision_id": "ignored",
                "takeoff_id": "ignored",
                "estimate_version_id": "ignored",
            },
        ),
    ],
    ids=["revision_json", "quantity_csv", "estimate_csv"],
)
async def test_create_export_rejects_body_lineage_extras(
    async_client: AsyncClient,
    case: ExportCreateCase,
    lineage_extras: dict[str, object],
) -> None:
    lineage = await _seed_export_lineage()

    response = await async_client.post(
        _route_path(case, lineage),
        json={**case.body, **lineage_extras},
    )

    assert response.status_code == 422
    assert await _count_export_side_effects(lineage) == (0, 0, 0)


async def test_create_estimate_export_rejects_invalid_export_kind(
    async_client: AsyncClient,
) -> None:
    lineage = await _seed_export_lineage()

    response = await async_client.post(
        _route_path(EXPORT_CREATE_CASES[2], lineage),
        json={
            "export_kind": ExportKind.QUANTITY_CSV.value,
            "options": {"template": "default"},
        },
    )

    assert response.status_code == 422
    assert await _count_export_side_effects(lineage) == (0, 0, 0)


async def test_create_export_rejects_non_finite_options_before_idempotency_claim(
    async_client: AsyncClient,
) -> None:
    lineage = await _seed_export_lineage()
    idempotency_key = f"export-create-non-finite-{uuid4().hex}"

    response = await async_client.post(
        _route_path(EXPORT_CREATE_CASES[0], lineage),
        content='{"options":{"nested":[NaN]}}',
        headers={
            "Content-Type": "application/json",
            "Idempotency-Key": idempotency_key,
        },
    )

    assert response.status_code == 422
    assert await _count_export_side_effects(lineage) == (0, 0, 0)
    assert await _count_idempotency_rows(idempotency_key) == 0


@pytest.mark.parametrize("state", ["inactive", "deleted"])
async def test_create_export_rejects_inactive_or_deleted_revision(
    async_client: AsyncClient,
    state: str,
) -> None:
    lineage = await _seed_export_lineage()

    if state == "inactive":
        await _set_project_deleted(lineage)
    else:
        await _set_source_file_deleted(lineage)

    response = await async_client.post(
        _route_path(EXPORT_CREATE_CASES[0], lineage),
        json=EXPORT_CREATE_CASES[0].body,
    )

    assert response.status_code == 404
    assert await _count_export_side_effects(lineage) == (0, 0, 0)


@pytest.mark.parametrize(
    ("trusted_totals", "quantity_gate"),
    [(False, QuantityGate.ALLOWED.value), (False, "blocked")],
    ids=["untrusted", "non_allowed_gate"],
)
async def test_create_export_allows_gated_or_untrusted_takeoff(
    async_client: AsyncClient,
    trusted_totals: bool,
    quantity_gate: str,
) -> None:
    # Path B 4: quantity/estimate exports are no longer gated on
    # quantity_gate / trusted_totals; any persisted takeoff is exportable.
    lineage = await _seed_export_lineage()
    takeoff_id = await _create_takeoff(
        lineage,
        trusted_totals=trusted_totals,
        quantity_gate=quantity_gate,
    )

    response = await async_client.post(
        (
            f"/v1/revisions/{lineage.revision_id}"
            f"/quantity-takeoffs/{takeoff_id}/exports/quantity-csv"
        ),
        json=EXPORT_CREATE_CASES[1].body,
    )

    assert response.status_code == 202
    # one export job + one export input persisted (no artifact until the worker runs)
    assert await _count_export_side_effects(lineage) == (1, 1, 0)


async def test_create_estimate_export_rejects_estimate_version_outside_path_takeoff(
    async_client: AsyncClient,
) -> None:
    lineage = await _seed_export_lineage()
    _, alternate_estimate_version_id = await _create_alternate_takeoff_and_estimate(lineage)

    response = await async_client.post(
        (
            f"/v1/revisions/{lineage.revision_id}"
            f"/quantity-takeoffs/{lineage.takeoff_id}"
            f"/estimates/{alternate_estimate_version_id}/exports"
        ),
        json=EXPORT_CREATE_CASES[2].body,
    )

    assert response.status_code == 404
    assert await _count_export_side_effects(lineage) == (0, 0, 0)


async def test_create_export_replays_idempotent_request(
    async_client: AsyncClient,
) -> None:
    lineage = await _seed_export_lineage()
    idempotency_key = f"export-create-replay-{uuid4().hex}"
    headers = {"Idempotency-Key": idempotency_key}

    first_response = await async_client.post(
        _route_path(EXPORT_CREATE_CASES[1], lineage),
        json=EXPORT_CREATE_CASES[1].body,
        headers=headers,
    )
    second_response = await async_client.post(
        _route_path(EXPORT_CREATE_CASES[1], lineage),
        json=EXPORT_CREATE_CASES[1].body,
        headers=headers,
    )

    assert first_response.status_code == 202
    assert second_response.status_code == 202
    assert second_response.json() == first_response.json()
    assert await _count_export_side_effects(lineage) == (1, 1, 0)

    assert session_module.AsyncSessionLocal is not None
    async with session_module.AsyncSessionLocal() as session:
        idempotency_rows = list(
            await session.scalars(
                select(IdempotencyKey).where(
                    IdempotencyKey.key_hash == hash_idempotency_key(idempotency_key)
                )
            )
        )

    assert len(idempotency_rows) == 1


async def test_create_export_rejects_same_idempotency_key_with_different_payload(
    async_client: AsyncClient,
) -> None:
    lineage = await _seed_export_lineage()
    idempotency_key = f"export-create-conflict-{uuid4().hex}"
    headers = {"Idempotency-Key": idempotency_key}

    first_response = await async_client.post(
        _route_path(EXPORT_CREATE_CASES[1], lineage),
        json=EXPORT_CREATE_CASES[1].body,
        headers=headers,
    )
    conflicting_response = await async_client.post(
        _route_path(EXPORT_CREATE_CASES[1], lineage),
        json={"options": {"dialect": "unix", "include_units": True}},
        headers=headers,
    )

    assert first_response.status_code == 202
    assert conflicting_response.status_code == 409
    assert await _count_export_side_effects(lineage) == (1, 1, 0)

    assert await _count_idempotency_rows(idempotency_key) == 1


async def test_create_export_revalidates_active_revision_after_idempotency_claim(
    async_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lineage = await _seed_export_lineage()
    idempotency_key = f"export-create-revalidate-{uuid4().hex}"
    original_claim: Any = export_routes._claim_idempotency_response

    async def claim_and_delete_revision(*args: object, **kwargs: object) -> object:
        claim = await original_claim(*args, **kwargs)
        await _set_source_file_deleted(lineage)
        return claim

    monkeypatch.setattr(export_routes, "_claim_idempotency_response", claim_and_delete_revision)

    response = await async_client.post(
        _route_path(EXPORT_CREATE_CASES[0], lineage),
        json=EXPORT_CREATE_CASES[0].body,
        headers={"Idempotency-Key": idempotency_key},
    )

    assert response.status_code == 404
    assert await _count_export_side_effects(lineage) == (0, 0, 0)
    assert await _count_idempotency_rows(idempotency_key) == 1


async def test_publish_export_enqueue_uses_route_local_publisher(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = uuid4()
    published: dict[str, object] = {}

    async def fake_publish_job_enqueue_intent(
        published_job_id: UUID,
        *,
        publisher: object,
        suppress_exceptions: bool,
    ) -> bool:
        published["job_id"] = published_job_id
        published["publisher"] = publisher
        published["suppress_exceptions"] = suppress_exceptions
        return True

    def fake_enqueue_export_job(enqueued_job_id: UUID) -> None:
        published["enqueued_job_id"] = enqueued_job_id

    monkeypatch.setattr(
        export_routes,
        "_publish_job_enqueue_intent",
        fake_publish_job_enqueue_intent,
    )
    monkeypatch.setattr(export_routes, "_enqueue_export_job", fake_enqueue_export_job)

    await export_routes._publish_export_enqueue(job_id)

    assert published == {
        "job_id": job_id,
        "publisher": fake_enqueue_export_job,
        "suppress_exceptions": True,
    }
