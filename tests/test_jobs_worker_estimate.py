"""Estimate worker helper and dispatch tests split from test_jobs.py."""

import types
import uuid
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any, cast

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

import app.db.session as session_module
import app.jobs.worker as worker_module
from app.core.errors import ErrorCode
from app.estimating.engine.errors import EstimateEngineError
from app.estimating.engine.service import compose_estimate as real_compose_estimate
from app.ingestion.finalization import IngestFinalizationPayload
from app.ingestion.runner import IngestionRunRequest
from app.models.drawing_revision import DrawingRevision
from app.models.estimate_job_input import EstimateJobInput, EstimateJobInputCatalogRef
from app.models.estimate_version import EstimateItem, EstimateSnapshotEntry, EstimateVersion
from app.models.estimation_catalog import (
    CatalogScopeType,
    EstimationFormula,
    EstimationMaterial,
    EstimationRate,
)
from app.models.job import Job, JobType
from app.models.quantity_takeoff import QuantityItem, QuantityTakeoff
from tests.conftest import requires_database
from tests.jobs_test_helpers import (
    _build_fake_ingest_payload,
    _create_project,
    _get_job,
    _get_job_for_file,
    _mark_source_deleted,
    _update_job,
    _upload_file,
    fake_ingestion_runner,
)


def _select_eligible_aggregate_quantity_item(quantity_items: list[QuantityItem]) -> QuantityItem:
    """Return one deterministic aggregate quantity item eligible for estimate mapping."""
    eligible_items = sorted(
        (
            item
            for item in quantity_items
            if item.item_kind == "aggregate"
            and item.value is not None
            and item.quantity_gate in {"allowed", "allowed_provisional"}
        ),
        key=lambda item: (item.quantity_type, str(item.id)),
    )
    assert eligible_items, "expected at least one eligible aggregate quantity item"
    return eligible_items[0]


def _estimate_persisted_semantic_payload(
    estimate_version: EstimateVersion,
    snapshot_entries: list[EstimateSnapshotEntry],
    line_items: list[EstimateItem],
) -> dict[str, Any]:
    """Return rerun-comparable persisted estimate semantics without row identity fields."""
    snapshot_payloads = [
        {
            "entry_type": entry.entry_type,
            "entry_key": entry.entry_key,
            "entry_label": entry.entry_label,
            "sort_order": entry.sort_order,
            "currency": entry.currency,
            "quantity_value": entry.quantity_value,
            "unit": entry.unit,
            "effective_date": entry.effective_date,
            "unit_amount": entry.unit_amount,
            "source_payload_json": deepcopy(entry.source_payload_json),
            "rounding_json": deepcopy(entry.rounding_json),
            "source_rate_id": entry.source_rate_id,
            "source_material_id": entry.source_material_id,
            "source_formula_id": entry.source_formula_id,
            "source_quantity_takeoff_id": entry.source_quantity_takeoff_id,
            "source_quantity_item_id": entry.source_quantity_item_id,
            "source_checksum_sha256": entry.source_checksum_sha256,
        }
        for entry in snapshot_entries
    ]

    line_item_payloads = [
        {
            "line_type": item.line_type,
            "line_number": item.line_number,
            "line_key": item.line_key,
            "description": item.description,
            "currency": item.currency,
            "quantity_value": item.quantity_value,
            "quantity_unit": item.quantity_unit,
            "unit_rate_amount": item.unit_rate_amount,
            "effective_date": item.effective_date,
            "subtotal_amount": item.subtotal_amount,
            "tax_amount": item.tax_amount,
            "total_amount": item.total_amount,
            "rounding_json": deepcopy(item.rounding_json),
            "quantity_snapshot_entry_id": item.quantity_snapshot_entry_id,
            "rate_snapshot_entry_id": item.rate_snapshot_entry_id,
            "material_snapshot_entry_id": item.material_snapshot_entry_id,
            "formula_snapshot_entry_id": item.formula_snapshot_entry_id,
            "assumption_snapshot_entry_id": item.assumption_snapshot_entry_id,
        }
        for item in line_items
    ]

    return {
        "version": {
            "source_job_id": estimate_version.source_job_id,
            "quantity_takeoff_id": estimate_version.quantity_takeoff_id,
            "source_file_id": estimate_version.source_file_id,
            "drawing_revision_id": estimate_version.drawing_revision_id,
            "quantity_gate": estimate_version.quantity_gate,
            "trusted_totals": estimate_version.trusted_totals,
            "currency": estimate_version.currency,
            "subtotal_amount": estimate_version.subtotal_amount,
            "tax_amount": estimate_version.tax_amount,
            "total_amount": estimate_version.total_amount,
        },
        "snapshot_entries": sorted(
            snapshot_payloads,
            key=lambda entry: (entry["sort_order"], entry["entry_key"]),
        ),
        "line_items": sorted(
            line_item_payloads,
            key=lambda item: (item["line_number"], item["line_key"]),
        ),
    }


async def _get_quantity_takeoffs_for_job(job_id: uuid.UUID) -> list[QuantityTakeoff]:
    """Load persisted quantity takeoffs for a source job."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        takeoffs = (
            (
                await session.execute(
                    select(QuantityTakeoff)
                    .where(QuantityTakeoff.source_job_id == job_id)
                    .order_by(QuantityTakeoff.created_at.asc(), QuantityTakeoff.id.asc())
                )
            )
            .scalars()
            .all()
        )

    return list(takeoffs)


async def _get_quantity_items_for_takeoff(quantity_takeoff_id: uuid.UUID) -> list[QuantityItem]:
    """Load persisted quantity items for a takeoff."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        items = (
            (
                await session.execute(
                    select(QuantityItem)
                    .where(QuantityItem.quantity_takeoff_id == quantity_takeoff_id)
                    .order_by(QuantityItem.created_at.asc(), QuantityItem.id.asc())
                )
            )
            .scalars()
            .all()
        )

    return list(items)


async def _get_estimate_versions_for_job(job_id: uuid.UUID) -> list[EstimateVersion]:
    """Load persisted estimate versions for an estimate source job."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        versions = (
            (
                await session.execute(
                    select(EstimateVersion)
                    .where(EstimateVersion.source_job_id == job_id)
                    .order_by(EstimateVersion.created_at.asc(), EstimateVersion.id.asc())
                )
            )
            .scalars()
            .all()
        )

    return list(versions)


async def _get_estimate_snapshot_entries_for_version(
    estimate_version_id: uuid.UUID,
) -> list[EstimateSnapshotEntry]:
    """Load persisted estimate snapshot entries for one version."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        entries = (
            (
                await session.execute(
                    select(EstimateSnapshotEntry)
                    .where(EstimateSnapshotEntry.estimate_version_id == estimate_version_id)
                    .order_by(
                        EstimateSnapshotEntry.created_at.asc(),
                        EstimateSnapshotEntry.id.asc(),
                    )
                )
            )
            .scalars()
            .all()
        )

    return list(entries)


async def _get_estimate_items_for_version(estimate_version_id: uuid.UUID) -> list[EstimateItem]:
    """Load persisted estimate line items for one version."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        items = (
            (
                await session.execute(
                    select(EstimateItem)
                    .where(EstimateItem.estimate_version_id == estimate_version_id)
                    .order_by(EstimateItem.created_at.asc(), EstimateItem.id.asc())
                )
            )
            .scalars()
            .all()
        )

    return list(items)


async def _persist_estimate_job_input(
    *,
    estimate_job: Job,
    quantity_takeoff: QuantityTakeoff,
    assumptions_json: dict[str, Any],
    catalog_refs: list[dict[str, Any]],
) -> None:
    """Persist one estimate input row and its explicit catalog refs."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    estimate_input = EstimateJobInput(
        estimate_job_id=estimate_job.id,
        project_id=estimate_job.project_id,
        source_file_id=estimate_job.file_id,
        drawing_revision_id=quantity_takeoff.drawing_revision_id,
        quantity_takeoff_id=quantity_takeoff.id,
        source_job_type="estimate",
        quantity_gate=quantity_takeoff.quantity_gate,
        trusted_totals=quantity_takeoff.trusted_totals,
        currency="GBP",
        pricing_effective_date=date(2026, 1, 2),
        pricing_mode="explicit",
        assumptions_json=deepcopy(assumptions_json),
    )

    catalog_rows: list[EstimationRate | EstimationMaterial | EstimationFormula] = []
    for ref_payload in catalog_refs:
        ref_type = ref_payload["ref_type"]
        selection_key = ref_payload["selection_key"]
        checksum = ref_payload["catalog_checksum_sha256"]

        if ref_type == "rate":
            rate_id = ref_payload["rate_catalog_entry_id"]
            catalog_rows.append(
                EstimationRate(
                    id=rate_id,
                    scope_type=CatalogScopeType.PROJECT,
                    project_id=estimate_job.project_id,
                    rate_key=selection_key,
                    source="tests.test_jobs_worker_estimate",
                    metadata_json={},
                    name=selection_key,
                    item_type="labor",
                    per_unit="meter",
                    currency="GBP",
                    amount=Decimal("1.00"),
                    effective_from=estimate_input.pricing_effective_date,
                    effective_to=None,
                    checksum_sha256=checksum,
                )
            )
            continue

        if ref_type == "material":
            material_id = ref_payload["material_catalog_entry_id"]
            catalog_rows.append(
                EstimationMaterial(
                    id=material_id,
                    scope_type=CatalogScopeType.PROJECT,
                    project_id=estimate_job.project_id,
                    material_key=selection_key,
                    source="tests.test_jobs_worker_estimate",
                    metadata_json={},
                    name=selection_key,
                    unit="meter",
                    currency="GBP",
                    unit_cost=Decimal("1.00"),
                    effective_from=estimate_input.pricing_effective_date,
                    effective_to=None,
                    checksum_sha256=checksum,
                )
            )
            continue

        if ref_type == "formula":
            formula_id = ref_payload["formula_definition_id"]
            catalog_rows.append(
                EstimationFormula(
                    id=formula_id,
                    scope_type=CatalogScopeType.PROJECT,
                    project_id=estimate_job.project_id,
                    formula_id=selection_key,
                    version=1,
                    name=selection_key,
                    dsl_version="1.0",
                    output_key=f"formula:{selection_key}",
                    output_contract_json={"kind": "money", "currency": "GBP"},
                    declared_inputs_json=[],
                    expression_json={"kind": "constant", "value": "1"},
                    rounding_json={},
                    checksum_sha256=checksum,
                )
            )

    ref_rows = [
        EstimateJobInputCatalogRef(
            estimate_job_id=estimate_job.id,
            ref_type=ref_payload["ref_type"],
            selection_key=ref_payload["selection_key"],
            ref_order=ref_payload["ref_order"],
            rate_catalog_entry_id=ref_payload.get("rate_catalog_entry_id"),
            material_catalog_entry_id=ref_payload.get("material_catalog_entry_id"),
            formula_definition_id=ref_payload.get("formula_definition_id"),
            catalog_checksum_sha256=ref_payload["catalog_checksum_sha256"],
            selection_context_json=deepcopy(ref_payload["selection_context_json"]),
        )
        for ref_payload in catalog_refs
    ]

    async with session_maker() as session:
        session.add(estimate_input)
        session.add_all(catalog_rows)
        await session.flush()
        session.add_all(ref_rows)
        await session.commit()


async def _create_quantity_takeoff_job(
    *,
    project_id: uuid.UUID,
    file_id: uuid.UUID,
    base_revision_id: uuid.UUID,
    parent_job_id: uuid.UUID,
    status: str,
    job_type: str = JobType.QUANTITY_TAKEOFF.value,
    attempts: int = 0,
    max_attempts: int = 3,
) -> Job:
    """Persist a quantity_takeoff job linked to an ingest lineage chain."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    quantity_job = Job(
        id=uuid.uuid4(),
        project_id=project_id,
        file_id=file_id,
        extraction_profile_id=None,
        base_revision_id=base_revision_id,
        parent_job_id=parent_job_id,
        job_type=job_type,
        status=status,
        attempts=attempts,
        max_attempts=max_attempts,
        cancel_requested=False,
    )

    async with session_maker() as session:
        session.add(quantity_job)
        await session.commit()

    return await _get_job(quantity_job.id)


async def _create_ready_quantity_takeoff_job(
    async_client: httpx.AsyncClient,
) -> tuple[dict[str, Any], dict[str, Any], Job, DrawingRevision, Job]:
    """Create a project/file/revision and pending quantity job for worker tests."""
    project = await _create_project(async_client)
    uploaded = await _upload_file(async_client, project["id"])
    ingest_job = await _get_job_for_file(str(uploaded["id"]))
    await worker_module.process_ingest_job(ingest_job.id)
    base_revision = await _get_latest_revision_for_file(uuid.UUID(uploaded["id"]))
    assert base_revision is not None

    quantity_job = await _create_quantity_takeoff_job(
        project_id=uuid.UUID(project["id"]),
        file_id=uuid.UUID(uploaded["id"]),
        base_revision_id=base_revision.id,
        parent_job_id=ingest_job.id,
        status="pending",
    )

    return project, uploaded, ingest_job, base_revision, quantity_job


async def _create_ready_estimate_execution_job(
    async_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    Job,
    DrawingRevision,
    Job,
    QuantityTakeoff,
    list[QuantityItem],
    Job,
]:
    """Create a pending estimate job with a finalized trusted allowed quantity takeoff."""

    async def _run_quantity_ready_ingestion(
        request: IngestionRunRequest,
    ) -> IngestFinalizationPayload:
        payload = _build_fake_ingest_payload(request)
        confidence_json = {
            **payload.confidence_json,
            "review_state": "approved",
            "effective_confidence": payload.effective_confidence,
        }
        report_json = deepcopy(payload.report_json)
        summary = report_json.get("summary")
        if isinstance(summary, dict):
            report_json["summary"] = {
                **summary,
                "validation_status": "valid",
                "review_state": "approved",
                "quantity_gate": "allowed",
                "effective_confidence": payload.effective_confidence,
            }
        report_json["validation_status"] = "valid"
        report_json["review_state"] = "approved"
        report_json["quantity_gate"] = "allowed"
        report_json["effective_confidence"] = payload.effective_confidence

        return replace(
            payload,
            confidence_json=confidence_json,
            validation_status="valid",
            review_state="approved",
            quantity_gate="allowed",
            report_json=report_json,
        )

    monkeypatch.setattr(worker_module, "run_ingestion", _run_quantity_ready_ingestion)
    (
        project,
        uploaded,
        ingest_job,
        base_revision,
        quantity_job,
    ) = await _create_ready_quantity_takeoff_job(async_client)
    await worker_module.process_quantity_takeoff_job(quantity_job.id)
    takeoffs = await _get_quantity_takeoffs_for_job(quantity_job.id)
    assert len(takeoffs) == 1
    quantity_takeoff = takeoffs[0]
    quantity_items = await _get_quantity_items_for_takeoff(quantity_takeoff.id)

    estimate_job = await _create_quantity_takeoff_job(
        project_id=uuid.UUID(project["id"]),
        file_id=uuid.UUID(uploaded["id"]),
        base_revision_id=base_revision.id,
        parent_job_id=quantity_job.id,
        job_type=JobType.ESTIMATE.value,
        status="pending",
    )

    return (
        project,
        uploaded,
        ingest_job,
        base_revision,
        quantity_job,
        quantity_takeoff,
        quantity_items,
        estimate_job,
    )


def _estimate_catalog_ref_stub(
    *,
    ref_type: str,
    selection_key: str,
    line_key: str,
    ref_order: int,
    worker_mapping_version: str | None = worker_module._ESTIMATE_WORKER_MAPPING_VERSION,
    description: str | None = None,
    quantity_entry_key: str | None = None,
    catalog_entry_key: str | None = None,
    quantity_item_id: uuid.UUID | None = None,
    formula_inputs: dict[str, Any] | None = None,
    mapped_line_type: str | None = None,
    rate_catalog_entry_id: uuid.UUID | None = None,
    material_catalog_entry_id: uuid.UUID | None = None,
    formula_definition_id: uuid.UUID | None = None,
    catalog_checksum_sha256: str = "a" * 64,
) -> types.SimpleNamespace:
    """Build a lightweight estimate catalog-ref stub for worker mapping tests."""
    selection_context_json: dict[str, Any] = {
        "line_key": line_key,
        "line_type": mapped_line_type or ref_type,
        "description": description or f"{ref_type} {selection_key}",
    }
    if worker_mapping_version is not None:
        selection_context_json["worker_mapping_version"] = worker_mapping_version
    if quantity_entry_key is not None:
        selection_context_json["quantity_entry_key"] = quantity_entry_key
    if catalog_entry_key is not None:
        selection_context_json["catalog_entry_key"] = catalog_entry_key
    if quantity_item_id is not None:
        selection_context_json["quantity_item_id"] = str(quantity_item_id)
    if formula_inputs is not None:
        selection_context_json["formula_inputs"] = deepcopy(formula_inputs)

    return types.SimpleNamespace(
        ref_type=ref_type,
        selection_key=selection_key,
        ref_order=ref_order,
        rate_catalog_entry_id=(
            rate_catalog_entry_id or (uuid.uuid4() if ref_type == "rate" else None)
        ),
        material_catalog_entry_id=(
            material_catalog_entry_id or (uuid.uuid4() if ref_type == "material" else None)
        ),
        formula_definition_id=(
            formula_definition_id or (uuid.uuid4() if ref_type == "formula" else None)
        ),
        catalog_checksum_sha256=catalog_checksum_sha256,
        selection_context_json=selection_context_json,
    )


def _assert_estimate_mapping_invalid(
    exc_info: pytest.ExceptionInfo[worker_module._EstimateJobInputError],
    *,
    reason: str,
) -> None:
    """Assert deterministic sanitized estimate mapping failures."""
    error = exc_info.value
    assert error.error_code == ErrorCode.INPUT_INVALID
    assert error.message == worker_module._ESTIMATE_JOB_INPUT_INVALID_ERROR_MESSAGE
    assert error.details is not None
    assert error.details["reason"] == reason


async def _get_latest_revision_for_file(file_id: uuid.UUID) -> DrawingRevision | None:
    """Load the latest finalized drawing revision for a file."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        return (
            await session.execute(
                select(DrawingRevision)
                .where(DrawingRevision.source_file_id == file_id)
                .order_by(
                    DrawingRevision.revision_sequence.desc(),
                    DrawingRevision.id.desc(),
                )
                .limit(1)
            )
        ).scalar_one_or_none()


async def _create_estimate_job(
    *,
    project_id: uuid.UUID,
    file_id: uuid.UUID,
    base_revision_id: uuid.UUID,
    parent_job_id: uuid.UUID,
    status: str,
    attempts: int = 0,
    max_attempts: int = 3,
) -> Job:
    """Persist an estimate job linked to an ingest lineage chain."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    estimate_job = Job(
        id=uuid.uuid4(),
        project_id=project_id,
        file_id=file_id,
        extraction_profile_id=None,
        base_revision_id=base_revision_id,
        parent_job_id=parent_job_id,
        job_type=JobType.ESTIMATE.value,
        status=status,
        attempts=attempts,
        max_attempts=max_attempts,
        cancel_requested=False,
    )

    async with session_maker() as session:
        session.add(estimate_job)
        await session.commit()

    return await _get_job(estimate_job.id)


async def _create_ready_estimate_job(
    async_client: httpx.AsyncClient,
) -> tuple[dict[str, Any], dict[str, Any], Job, DrawingRevision, Job]:
    """Create a project/file/revision and pending estimate job for worker tests."""
    project = await _create_project(async_client)
    uploaded = await _upload_file(async_client, project["id"])
    ingest_job = await _get_job_for_file(str(uploaded["id"]))
    await worker_module.process_ingest_job(ingest_job.id)
    base_revision = await _get_latest_revision_for_file(uuid.UUID(uploaded["id"]))
    assert base_revision is not None

    estimate_job = await _create_estimate_job(
        project_id=uuid.UUID(project["id"]),
        file_id=uuid.UUID(uploaded["id"]),
        base_revision_id=base_revision.id,
        parent_job_id=ingest_job.id,
        status="pending",
    )

    return project, uploaded, ingest_job, base_revision, estimate_job


@pytest.mark.usefixtures(fake_ingestion_runner.__name__)
@requires_database
class TestJobsWorkerEstimate:
    # Lifecycle resume/cancel tests are colocated in this module.
    """Estimate worker mapping and dispatch tests."""

    async def test_begin_or_resume_estimate_job_skips_duplicate_running_attempt(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        fake_ingestion_runner: list[IngestionRunRequest],
    ) -> None:
        """Estimate duplicate delivery should leave a fresh running attempt unchanged."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        _ = fake_ingestion_runner

        _, _, _, _, estimate_job = await _create_ready_estimate_job(async_client)
        attempt_token = uuid.uuid4()
        lease_expires_at = datetime.now(UTC) + worker_module._RUNNING_JOB_STALE_AFTER

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            persisted_job = await session.get(Job, estimate_job.id)
            assert persisted_job is not None
            persisted_job.status = "running"
            persisted_job.attempts = 1
            persisted_job.started_at = datetime.now(UTC)
            persisted_job.attempt_token = attempt_token
            persisted_job.attempt_lease_expires_at = lease_expires_at
            await session.commit()

        lease = await worker_module._begin_or_resume_estimate_job(estimate_job.id)

        assert lease is None
        unchanged = await _get_job(estimate_job.id)
        assert unchanged.status == "running"
        assert unchanged.attempts == 1
        assert unchanged.started_at is not None
        assert unchanged.finished_at is None
        assert unchanged.attempt_token == attempt_token
        assert unchanged.attempt_lease_expires_at == lease_expires_at

    async def test_begin_or_resume_estimate_job_reclaims_stale_running_attempt_with_new_token(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        fake_ingestion_runner: list[IngestionRunRequest],
    ) -> None:
        """Estimate stale running attempts should be reclaimed with a fresh ownership token."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        _ = fake_ingestion_runner

        _, _, _, _, estimate_job = await _create_ready_estimate_job(async_client)
        old_attempt_token = uuid.uuid4()

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            persisted_job = await session.get(Job, estimate_job.id)
            assert persisted_job is not None
            persisted_job.status = "running"
            persisted_job.attempts = 1
            persisted_job.started_at = (
                datetime.now(UTC) - worker_module._RUNNING_JOB_STALE_AFTER - timedelta(seconds=1)
            )
            persisted_job.attempt_token = old_attempt_token
            persisted_job.attempt_lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            await session.commit()

        lease = await worker_module._begin_or_resume_estimate_job(estimate_job.id)

        assert lease is not None
        assert lease.token != old_attempt_token
        reclaimed_job = await _get_job(estimate_job.id)
        assert reclaimed_job.status == "running"
        assert reclaimed_job.attempts == 2
        assert reclaimed_job.attempt_token == lease.token
        assert reclaimed_job.attempt_lease_expires_at == lease.lease_expires_at
        assert reclaimed_job.attempt_lease_expires_at is not None
        assert reclaimed_job.attempt_lease_expires_at > datetime.now(UTC)

    async def test_begin_or_resume_estimate_job_cancels_inactive_source(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        fake_ingestion_runner: list[IngestionRunRequest],
    ) -> None:
        """Estimate begin/resume should cancel when the source project or file is inactive."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        _ = fake_ingestion_runner

        project, uploaded, _, _, estimate_job = await _create_ready_estimate_job(async_client)
        await _mark_source_deleted(
            uuid.UUID(project["id"]),
            uuid.UUID(uploaded["id"]),
            delete_project=False,
            delete_file=True,
        )

        lease = await worker_module._begin_or_resume_estimate_job(estimate_job.id)

        assert lease is None
        cancelled = await _get_job(estimate_job.id)
        assert cancelled.status == "cancelled"
        assert cancelled.cancel_requested is True
        assert cancelled.error_code == ErrorCode.JOB_CANCELLED.value
        assert cancelled.finished_at is not None

    async def test_begin_or_resume_estimate_job_finalizes_cancel_requested_job_as_cancelled(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        fake_ingestion_runner: list[IngestionRunRequest],
    ) -> None:
        """Estimate begin/resume should finalize cancel-requested jobs to cancelled."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        _ = fake_ingestion_runner

        _, _, _, _, estimate_job = await _create_ready_estimate_job(async_client)
        await _update_job(estimate_job.id, status="pending", cancel_requested=True)

        lease = await worker_module._begin_or_resume_estimate_job(estimate_job.id)

        assert lease is None
        updated = await _get_job(estimate_job.id)
        assert updated.status == "cancelled"
        assert updated.cancel_requested is True
        assert updated.error_code == ErrorCode.JOB_CANCELLED.value
        assert updated.finished_at is not None

    async def test_begin_or_resume_estimate_job_skips_terminal_status(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        fake_ingestion_runner: list[IngestionRunRequest],
    ) -> None:
        """Estimate begin/resume should ignore already terminal jobs."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        _ = fake_ingestion_runner

        _, _, _, _, estimate_job = await _create_ready_estimate_job(async_client)
        await _update_job(estimate_job.id, status="failed", attempts=1, error_message="done")
        before = await _get_job(estimate_job.id)
        before_finished_at = before.finished_at

        lease = await worker_module._begin_or_resume_estimate_job(estimate_job.id)

        assert lease is None
        unchanged = await _get_job(estimate_job.id)
        assert unchanged.status == "failed"
        assert unchanged.attempts == 1
        assert unchanged.error_message == "done"
        assert unchanged.finished_at == before_finished_at

    @pytest.mark.parametrize("status", ["pending", "running"])
    async def test_recover_incomplete_ingest_jobs_requeues_estimate_jobs(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
        status: str,
    ) -> None:
        """Startup recovery should route estimate jobs through the estimate publisher."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        estimate_recovered_job_ids: list[str] = []
        ingest_recovered_job_ids: list[str] = []

        def _fake_estimate_recovery_enqueue(job_id: uuid.UUID) -> None:
            estimate_recovered_job_ids.append(str(job_id))

        def _fake_ingest_recovery_enqueue(job_id: uuid.UUID) -> None:
            ingest_recovered_job_ids.append(str(job_id))

        monkeypatch.setattr(
            worker_module,
            "enqueue_estimate_job",
            _fake_estimate_recovery_enqueue,
        )
        monkeypatch.setattr(worker_module, "enqueue_ingest_job", _fake_ingest_recovery_enqueue)

        _, _, _, _, estimate_job = await _create_ready_estimate_job(async_client)
        await _update_job(
            estimate_job.id,
            status=status,
            attempts=1 if status == "running" else 0,
        )

        if status == "running":
            await _update_job(
                estimate_job.id,
                enqueue_status="published",
                enqueue_attempts=1,
            )
            session_maker = session_module.AsyncSessionLocal
            assert session_maker is not None
            async with session_maker() as session:
                persisted_job = await session.get(Job, estimate_job.id)
                assert persisted_job is not None
                persisted_job.started_at = (
                    datetime.now(UTC)
                    - worker_module._RUNNING_JOB_STALE_AFTER
                    - timedelta(seconds=1)
                )
                persisted_job.attempt_token = uuid.uuid4()
                persisted_job.attempt_lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
                await session.commit()
        else:
            await _update_job(
                estimate_job.id,
                enqueue_status="pending",
                enqueue_attempts=0,
            )

        original = await _get_job(estimate_job.id)

        requeued = await worker_module.recover_incomplete_ingest_jobs()

        assert requeued == [estimate_job.id]
        assert estimate_recovered_job_ids == [str(estimate_job.id)]
        assert ingest_recovered_job_ids == []
        recovered = await _get_job(estimate_job.id)
        assert recovered.status == "pending"
        assert recovered.attempts == original.attempts
        assert recovered.started_at is None
        assert recovered.attempt_token is None
        assert recovered.attempt_lease_expires_at is None
        assert recovered.enqueue_status == "published"
        assert recovered.enqueue_attempts == 1
        assert recovered.project_id == original.project_id
        assert recovered.file_id == original.file_id
        assert recovered.job_type == original.job_type
        assert recovered.extraction_profile_id == original.extraction_profile_id
        assert recovered.extraction_profile_id is None
        assert recovered.base_revision_id == original.base_revision_id
        assert recovered.parent_job_id == original.parent_job_id

    async def test_process_estimate_job_builds_engine_input_without_publishing_outputs(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Estimate worker should compose engine input before persistence lands."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        (
            _,
            _,
            _,
            _,
            _,
            quantity_takeoff,
            quantity_items,
            estimate_job,
        ) = await _create_ready_estimate_execution_job(async_client, monkeypatch)
        quantity_item = _select_eligible_aggregate_quantity_item(quantity_items)

        await _persist_estimate_job_input(
            estimate_job=estimate_job,
            quantity_takeoff=quantity_takeoff,
            assumptions_json={"tax_rate": "0.20"},
            catalog_refs=[
                {
                    "ref_type": "rate",
                    "selection_key": "paint-labor",
                    "ref_order": 20,
                    "rate_catalog_entry_id": uuid.UUID("00000000-0000-0000-0000-000000000201"),
                    "catalog_checksum_sha256": "1" * 64,
                    "selection_context_json": {
                        "worker_mapping_version": worker_module._ESTIMATE_WORKER_MAPPING_VERSION,
                        "line_key": "line-rate",
                        "line_type": "rate",
                        "description": "Paint labor",
                        "quantity_item_id": str(quantity_item.id),
                    },
                },
                {
                    "ref_type": "formula",
                    "selection_key": "waste-10",
                    "ref_order": 30,
                    "formula_definition_id": uuid.UUID("00000000-0000-0000-0000-000000000202"),
                    "catalog_checksum_sha256": "2" * 64,
                    "selection_context_json": {
                        "worker_mapping_version": worker_module._ESTIMATE_WORKER_MAPPING_VERSION,
                        "line_key": "line-formula",
                        "line_type": "formula",
                        "description": "Waste allowance",
                        "formula_inputs": {"operand_line_keys": ["line-rate"]},
                    },
                },
                {
                    "ref_type": "material",
                    "selection_key": "paint-gallon",
                    "ref_order": 10,
                    "material_catalog_entry_id": uuid.UUID("00000000-0000-0000-0000-000000000203"),
                    "catalog_checksum_sha256": "3" * 64,
                    "selection_context_json": {
                        "worker_mapping_version": worker_module._ESTIMATE_WORKER_MAPPING_VERSION,
                        "line_key": "line-material",
                        "line_type": "material",
                        "description": "Paint gallon",
                        "quantity_item_id": str(quantity_item.id),
                    },
                },
            ],
        )

        fake_formula_definition = types.SimpleNamespace(
            declared_inputs=(
                types.SimpleNamespace(
                    name="rate_input",
                    contract=types.SimpleNamespace(kind="rate"),
                ),
            ),
        )
        captured_engine_inputs: list[Any] = []
        finalized_outputs: list[Any] = []

        async def _resolve_rate(*args: Any, **kwargs: Any) -> Any:
            _ = (args, kwargs)
            return types.SimpleNamespace(
                id=uuid.UUID("00000000-0000-0000-0000-000000000201"),
                rate_key="paint-labor",
                item_type="labor",
                unit=quantity_item.unit,
                currency="GBP",
                value=Decimal("12.50"),
                effective_start=date(2026, 1, 2),
                checksum_sha256="1" * 64,
                metadata={"crew": "A"},
            )

        async def _resolve_material(*args: Any, **kwargs: Any) -> Any:
            _ = (args, kwargs)
            return types.SimpleNamespace(
                id=uuid.UUID("00000000-0000-0000-0000-000000000203"),
                material_key="paint-gallon",
                unit=quantity_item.unit,
                currency="GBP",
                value=Decimal("7.25"),
                effective_start=date(2026, 1, 2),
                checksum_sha256="3" * 64,
                metadata={"sku": "PAINT"},
            )

        async def _resolve_formula(*args: Any, **kwargs: Any) -> Any:
            _ = (args, kwargs)
            return types.SimpleNamespace(
                definition_id=uuid.UUID("00000000-0000-0000-0000-000000000202"),
                checksum_sha256="2" * 64,
                formula_id="waste-10",
                version=1,
            )

        def _compose_estimate(engine_input: Any) -> Any:
            captured_engine_inputs.append(engine_input)
            return types.SimpleNamespace()

        async def _skip_finalize(*args: Any, **kwargs: Any) -> bool:
            finalized_outputs.append(kwargs["output"])
            return False

        monkeypatch.setattr(worker_module, "resolve_rate", _resolve_rate)
        monkeypatch.setattr(worker_module, "resolve_material", _resolve_material)
        monkeypatch.setattr(worker_module, "resolve_formula", _resolve_formula)
        monkeypatch.setattr(
            worker_module,
            "formula_definition_from_selected_formula",
            lambda _: fake_formula_definition,
        )
        monkeypatch.setattr(worker_module, "compose_estimate", _compose_estimate)
        monkeypatch.setattr(worker_module, "_finalize_estimate_job", _skip_finalize)

        await worker_module.process_estimate_job(estimate_job.id)

        updated_job = await _get_job(estimate_job.id)
        estimate_versions = await _get_estimate_versions_for_job(estimate_job.id)
        assert updated_job.status == "running"
        assert updated_job.attempts == 1
        assert updated_job.error_code is None
        assert updated_job.error_message is None
        assert updated_job.finished_at is None
        assert estimate_versions == []
        assert len(captured_engine_inputs) == 1
        assert len(finalized_outputs) == 1

        engine_input = captured_engine_inputs[0]
        assert engine_input.tax_rate == Decimal("0.20")
        assert tuple(entry.entry_key for entry in engine_input.quantity_entries) == (
            f"quantity:{quantity_item.id}",
        )
        assert tuple(entry.source_quantity_item_id for entry in engine_input.quantity_entries) == (
            quantity_item.id,
        )
        assert tuple(entry.entry_key for entry in engine_input.rate_entries) == (
            "rate:paint-labor",
        )
        assert tuple(entry.entry_key for entry in engine_input.material_entries) == (
            "material:paint-gallon",
        )
        assert tuple(entry.entry_key for entry in engine_input.formula_entries) == (
            "formula:waste-10",
        )
        assert tuple(line.line_key for line in engine_input.line_inputs) == (
            "line-material",
            "line-rate",
            "line-formula",
        )
        assert engine_input.line_inputs[2].formula_inputs == {"rate_input": "rate:paint-labor"}

    async def test_process_estimate_job_persists_expected_output_atomically(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Estimate worker should atomically persist one finalized version and terminal success."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        (
            _,
            _,
            _,
            _,
            _,
            quantity_takeoff,
            quantity_items,
            estimate_job,
        ) = await _create_ready_estimate_execution_job(async_client, monkeypatch)
        quantity_item = _select_eligible_aggregate_quantity_item(quantity_items)

        await _persist_estimate_job_input(
            estimate_job=estimate_job,
            quantity_takeoff=quantity_takeoff,
            assumptions_json={"tax_rate": "0.20"},
            catalog_refs=[
                {
                    "ref_type": "rate",
                    "selection_key": "paint-labor",
                    "ref_order": 20,
                    "rate_catalog_entry_id": uuid.UUID("00000000-0000-0000-0000-000000000231"),
                    "catalog_checksum_sha256": "7" * 64,
                    "selection_context_json": {
                        "worker_mapping_version": worker_module._ESTIMATE_WORKER_MAPPING_VERSION,
                        "line_key": "line-rate",
                        "line_type": "rate",
                        "description": "Paint labor",
                        "quantity_item_id": str(quantity_item.id),
                    },
                }
            ],
        )

        async def _resolve_rate(*args: Any, **kwargs: Any) -> Any:
            _ = (args, kwargs)
            return types.SimpleNamespace(
                id=uuid.UUID("00000000-0000-0000-0000-000000000231"),
                rate_key="paint-labor",
                item_type="labor",
                unit=quantity_item.unit,
                currency="GBP",
                value=Decimal("12.50"),
                effective_start=date(2026, 1, 2),
                checksum_sha256="7" * 64,
                metadata={"crew": "A"},
            )

        monkeypatch.setattr(worker_module, "resolve_rate", _resolve_rate)

        await worker_module.process_estimate_job(estimate_job.id)

        updated_job = await _get_job(estimate_job.id)
        estimate_versions = await _get_estimate_versions_for_job(estimate_job.id)
        assert updated_job.status == "succeeded"
        assert updated_job.attempts == 1
        assert updated_job.error_code is None
        assert updated_job.error_message is None
        assert updated_job.finished_at is not None
        assert len(estimate_versions) == 1
        estimate_version = estimate_versions[0]
        snapshot_entries = await _get_estimate_snapshot_entries_for_version(estimate_version.id)
        line_items = await _get_estimate_items_for_version(estimate_version.id)
        assert snapshot_entries != []
        assert line_items != []
        assert sorted(entry.sort_order for entry in snapshot_entries) == [1, 2]

        response = await async_client.get(f"/v1/jobs/{estimate_job.id}/events")
        assert response.status_code == 200
        event_payload = response.json()["items"][-1]["data_json"]
        assert event_payload == {
            "status": "succeeded",
            "attempts": 1,
            "estimate_version_id": str(estimate_version.id),
            "snapshot_entry_count": len(snapshot_entries),
            "line_item_count": len(line_items),
        }

    async def test_process_estimate_job_fails_revision_drift_without_output(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Estimate finalization should fail closed when the output revision drifts."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        (
            _,
            _,
            _,
            _,
            _,
            quantity_takeoff,
            quantity_items,
            estimate_job,
        ) = await _create_ready_estimate_execution_job(async_client, monkeypatch)
        quantity_item = _select_eligible_aggregate_quantity_item(quantity_items)

        await _persist_estimate_job_input(
            estimate_job=estimate_job,
            quantity_takeoff=quantity_takeoff,
            assumptions_json={},
            catalog_refs=[
                {
                    "ref_type": "rate",
                    "selection_key": "paint-labor",
                    "ref_order": 20,
                    "rate_catalog_entry_id": uuid.UUID("00000000-0000-0000-0000-000000000236"),
                    "catalog_checksum_sha256": "c" * 64,
                    "selection_context_json": {
                        "worker_mapping_version": worker_module._ESTIMATE_WORKER_MAPPING_VERSION,
                        "line_key": "line-rate",
                        "line_type": "rate",
                        "description": "Paint labor",
                        "quantity_item_id": str(quantity_item.id),
                    },
                }
            ],
        )

        async def _resolve_rate(*args: Any, **kwargs: Any) -> Any:
            _ = (args, kwargs)
            return types.SimpleNamespace(
                id=uuid.UUID("00000000-0000-0000-0000-000000000236"),
                rate_key="paint-labor",
                item_type="labor",
                unit=quantity_item.unit,
                currency="GBP",
                value=Decimal("12.50"),
                effective_start=date(2026, 1, 2),
                checksum_sha256="c" * 64,
                metadata={},
            )

        mismatched_revision_id = uuid.uuid4()

        class _MismatchedRevisionOutput:
            def __init__(self, output: Any) -> None:
                self._output = output

            def estimate_version_model_kwargs(self) -> dict[str, Any]:
                kwargs = dict(self._output.estimate_version_model_kwargs())
                kwargs["drawing_revision_id"] = mismatched_revision_id
                return kwargs

            def snapshot_entry_model_kwargs(self) -> Any:
                return self._output.snapshot_entry_model_kwargs()

            def line_item_model_kwargs(self) -> Any:
                return self._output.line_item_model_kwargs()

        def _compose_mismatched_revision(engine_input: Any) -> Any:
            return _MismatchedRevisionOutput(real_compose_estimate(engine_input))

        monkeypatch.setattr(worker_module, "resolve_rate", _resolve_rate)
        monkeypatch.setattr(worker_module, "compose_estimate", _compose_mismatched_revision)

        await worker_module.process_estimate_job(estimate_job.id)

        updated_job = await _get_job(estimate_job.id)
        estimate_versions = await _get_estimate_versions_for_job(estimate_job.id)
        assert updated_job.status == "failed"
        assert updated_job.error_code == ErrorCode.REVISION_CONFLICT.value
        assert estimate_versions == []

    async def test_process_estimate_job_rollback_clears_staged_outputs_when_item_insert_fails(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Estimate finalization rollback should remove staged rows and success events."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        (
            _,
            _,
            _,
            _,
            _,
            quantity_takeoff,
            quantity_items,
            estimate_job,
        ) = await _create_ready_estimate_execution_job(async_client, monkeypatch)
        quantity_item = _select_eligible_aggregate_quantity_item(quantity_items)

        await _persist_estimate_job_input(
            estimate_job=estimate_job,
            quantity_takeoff=quantity_takeoff,
            assumptions_json={},
            catalog_refs=[
                {
                    "ref_type": "rate",
                    "selection_key": "paint-labor",
                    "ref_order": 20,
                    "rate_catalog_entry_id": uuid.UUID("00000000-0000-0000-0000-000000000234"),
                    "catalog_checksum_sha256": "a" * 64,
                    "selection_context_json": {
                        "worker_mapping_version": worker_module._ESTIMATE_WORKER_MAPPING_VERSION,
                        "line_key": "line-rate",
                        "line_type": "rate",
                        "description": "Paint labor",
                        "quantity_item_id": str(quantity_item.id),
                    },
                }
            ],
        )

        async def _resolve_rate(*args: Any, **kwargs: Any) -> Any:
            _ = (args, kwargs)
            return types.SimpleNamespace(
                id=uuid.UUID("00000000-0000-0000-0000-000000000234"),
                rate_key="paint-labor",
                item_type="labor",
                unit=quantity_item.unit,
                currency="GBP",
                value=Decimal("12.50"),
                effective_start=date(2026, 1, 2),
                checksum_sha256="a" * 64,
                metadata={},
            )

        class _InvalidLineItemOutput:
            def __init__(self, output: Any) -> None:
                self._output = output

            def estimate_version_model_kwargs(self) -> dict[str, Any]:
                return cast(dict[str, Any], self._output.estimate_version_model_kwargs())

            def snapshot_entry_model_kwargs(self) -> Any:
                return self._output.snapshot_entry_model_kwargs()

            def line_item_model_kwargs(self) -> Any:
                line_item_kwargs = [
                    dict(kwargs) for kwargs in self._output.line_item_model_kwargs()
                ]
                assert line_item_kwargs != []
                assert "estimate_version_id" in line_item_kwargs[0]
                line_item_kwargs[0]["estimate_version_id"] = uuid.uuid4()
                return line_item_kwargs

        def _compose_invalid_line_item(engine_input: Any) -> Any:
            return _InvalidLineItemOutput(real_compose_estimate(engine_input))

        monkeypatch.setattr(worker_module, "resolve_rate", _resolve_rate)
        monkeypatch.setattr(worker_module, "compose_estimate", _compose_invalid_line_item)

        with pytest.raises(IntegrityError):
            await worker_module.process_estimate_job(estimate_job.id)

        updated_job = await _get_job(estimate_job.id)
        assert updated_job.status == "failed"
        assert updated_job.attempts == 1
        assert updated_job.attempts < updated_job.max_attempts
        assert updated_job.error_code == ErrorCode.INTERNAL_ERROR.value
        assert updated_job.error_message == worker_module._FINALIZE_ESTIMATE_JOB_ERROR_MESSAGE

        response = await async_client.get(f"/v1/jobs/{estimate_job.id}/events")
        assert response.status_code == 200
        event_items = response.json()["items"]
        event_payload = event_items[-1]["data_json"]
        assert event_payload["status"] == "failed"
        assert event_payload["error_code"] == ErrorCode.INTERNAL_ERROR.value
        assert event_payload["error_message"] == worker_module._FINALIZE_ESTIMATE_JOB_ERROR_MESSAGE
        assert all(
            item["data_json"].get("status") != "succeeded"
            for item in event_items
            if isinstance(item.get("data_json"), dict)
        )

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            estimate_versions = (
                (
                    await session.execute(
                        select(EstimateVersion)
                        .where(EstimateVersion.source_job_id == estimate_job.id)
                        .order_by(EstimateVersion.created_at.asc(), EstimateVersion.id.asc())
                    )
                )
                .scalars()
                .all()
            )
            snapshot_entries = (
                (
                    await session.execute(
                        select(EstimateSnapshotEntry).where(
                            EstimateSnapshotEntry.estimate_version_id.in_(
                                select(EstimateVersion.id).where(
                                    EstimateVersion.source_job_id == estimate_job.id
                                )
                            )
                        )
                    )
                )
                .scalars()
                .all()
            )
            line_items = (
                (
                    await session.execute(
                        select(EstimateItem).where(
                            EstimateItem.estimate_version_id.in_(
                                select(EstimateVersion.id).where(
                                    EstimateVersion.source_job_id == estimate_job.id
                                )
                            )
                        )
                    )
                )
                .scalars()
                .all()
            )

        assert list(estimate_versions) == []
        assert list(snapshot_entries) == []
        assert list(line_items) == []

    async def test_process_estimate_job_fails_invalid_mapping_without_output_rows(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Estimate mapping failures should close the job with INPUT_INVALID before persistence."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        (
            _,
            _,
            _,
            _,
            _,
            quantity_takeoff,
            quantity_items,
            estimate_job,
        ) = await _create_ready_estimate_execution_job(async_client, monkeypatch)
        quantity_item = _select_eligible_aggregate_quantity_item(quantity_items)

        await _persist_estimate_job_input(
            estimate_job=estimate_job,
            quantity_takeoff=quantity_takeoff,
            assumptions_json={},
            catalog_refs=[
                {
                    "ref_type": "rate",
                    "selection_key": "paint-labor",
                    "ref_order": 20,
                    "rate_catalog_entry_id": uuid.UUID("00000000-0000-0000-0000-000000000211"),
                    "catalog_checksum_sha256": "4" * 64,
                    "selection_context_json": {
                        "line_key": "line-rate",
                        "line_type": "rate",
                        "description": "Paint labor",
                        "quantity_item_id": str(quantity_item.id),
                    },
                }
            ],
        )

        await worker_module.process_estimate_job(estimate_job.id)

        updated_job = await _get_job(estimate_job.id)
        estimate_versions = await _get_estimate_versions_for_job(estimate_job.id)
        assert updated_job.status == "failed"
        assert updated_job.error_code == ErrorCode.INPUT_INVALID.value
        assert updated_job.error_message == worker_module._ESTIMATE_JOB_INPUT_INVALID_ERROR_MESSAGE
        assert estimate_versions == []

        response = await async_client.get(f"/v1/jobs/{estimate_job.id}/events")
        assert response.status_code == 200
        event_payload = response.json()["items"][-1]["data_json"]
        assert event_payload["status"] == "failed"
        assert event_payload["error_code"] == ErrorCode.INPUT_INVALID.value
        assert event_payload["details"]["reason"] == "missing_worker_mapping_version"

    async def test_process_estimate_job_fails_invalid_quantity_or_engine_input_without_outputs(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Estimate worker should fail closed on invalid quantity selection and engine rejection."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        (
            _,
            _,
            _,
            _,
            _,
            quantity_takeoff,
            quantity_items,
            estimate_job,
        ) = await _create_ready_estimate_execution_job(async_client, monkeypatch)
        exclusion_item = next(item for item in quantity_items if item.item_kind == "exclusion")

        await _persist_estimate_job_input(
            estimate_job=estimate_job,
            quantity_takeoff=quantity_takeoff,
            assumptions_json={},
            catalog_refs=[
                {
                    "ref_type": "rate",
                    "selection_key": "paint-labor",
                    "ref_order": 20,
                    "rate_catalog_entry_id": uuid.UUID("00000000-0000-0000-0000-000000000221"),
                    "catalog_checksum_sha256": "5" * 64,
                    "selection_context_json": {
                        "worker_mapping_version": worker_module._ESTIMATE_WORKER_MAPPING_VERSION,
                        "line_key": "line-rate",
                        "line_type": "rate",
                        "description": "Paint labor",
                        "quantity_item_id": str(exclusion_item.id),
                    },
                }
            ],
        )

        await worker_module.process_estimate_job(estimate_job.id)

        invalid_quantity_job = await _get_job(estimate_job.id)
        assert invalid_quantity_job.status == "failed"
        assert invalid_quantity_job.error_code == ErrorCode.INPUT_INVALID.value
        assert await _get_estimate_versions_for_job(estimate_job.id) == []

        (
            _,
            _,
            _,
            _,
            _,
            quantity_takeoff,
            quantity_items,
            engine_invalid_job,
        ) = await _create_ready_estimate_execution_job(async_client, monkeypatch)
        quantity_item = _select_eligible_aggregate_quantity_item(quantity_items)

        await _persist_estimate_job_input(
            estimate_job=engine_invalid_job,
            quantity_takeoff=quantity_takeoff,
            assumptions_json={},
            catalog_refs=[
                {
                    "ref_type": "rate",
                    "selection_key": "paint-labor",
                    "ref_order": 20,
                    "rate_catalog_entry_id": uuid.UUID("00000000-0000-0000-0000-000000000222"),
                    "catalog_checksum_sha256": "6" * 64,
                    "selection_context_json": {
                        "worker_mapping_version": worker_module._ESTIMATE_WORKER_MAPPING_VERSION,
                        "line_key": "line-rate",
                        "line_type": "rate",
                        "description": "Paint labor",
                        "quantity_item_id": str(quantity_item.id),
                    },
                }
            ],
        )

        async def _resolve_rate(*args: Any, **kwargs: Any) -> Any:
            _ = (args, kwargs)
            return types.SimpleNamespace(
                id=uuid.UUID("00000000-0000-0000-0000-000000000222"),
                rate_key="paint-labor",
                item_type="labor",
                unit=quantity_item.unit,
                currency="GBP",
                value=Decimal("12.50"),
                effective_start=date(2026, 1, 2),
                checksum_sha256="6" * 64,
                metadata={},
            )

        def _raise_engine_input_invalid(_: Any) -> Any:
            raise EstimateEngineError(
                cast(Any, "INPUT_INVALID"),
                "engine_input_invalid",
                "engine rejected input",
            )

        monkeypatch.setattr(worker_module, "resolve_rate", _resolve_rate)
        monkeypatch.setattr(worker_module, "compose_estimate", _raise_engine_input_invalid)

        await worker_module.process_estimate_job(engine_invalid_job.id)

        updated_job = await _get_job(engine_invalid_job.id)
        estimate_versions = await _get_estimate_versions_for_job(engine_invalid_job.id)
        assert updated_job.status == "failed"
        assert updated_job.error_code == ErrorCode.INPUT_INVALID.value
        assert updated_job.error_message == worker_module._ESTIMATE_JOB_INPUT_INVALID_ERROR_MESSAGE
        assert estimate_versions == []

        response = await async_client.get(f"/v1/jobs/{engine_invalid_job.id}/events")
        assert response.status_code == 200
        event_payload = response.json()["items"][-1]["data_json"]
        assert event_payload["status"] == "failed"
        assert event_payload["details"] == {"reason": "engine_input_invalid"}

    async def test_process_estimate_job_rejects_colliding_catalog_entry_keys(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Estimate input build should fail when explicit catalog keys alias different sources."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        (
            _,
            _,
            _,
            _,
            _,
            quantity_takeoff,
            quantity_items,
            estimate_job,
        ) = await _create_ready_estimate_execution_job(async_client, monkeypatch)
        quantity_item = _select_eligible_aggregate_quantity_item(quantity_items)

        await _persist_estimate_job_input(
            estimate_job=estimate_job,
            quantity_takeoff=quantity_takeoff,
            assumptions_json={},
            catalog_refs=[
                {
                    "ref_type": "rate",
                    "selection_key": "paint-labor-a",
                    "ref_order": 20,
                    "rate_catalog_entry_id": uuid.UUID("00000000-0000-0000-0000-000000000241"),
                    "catalog_checksum_sha256": "d" * 64,
                    "selection_context_json": {
                        "worker_mapping_version": worker_module._ESTIMATE_WORKER_MAPPING_VERSION,
                        "line_key": "line-rate-a",
                        "line_type": "rate",
                        "description": "Paint labor A",
                        "quantity_item_id": str(quantity_item.id),
                        "catalog_entry_key": "shared:paint-labor",
                    },
                },
                {
                    "ref_type": "rate",
                    "selection_key": "paint-labor-b",
                    "ref_order": 30,
                    "rate_catalog_entry_id": uuid.UUID("00000000-0000-0000-0000-000000000242"),
                    "catalog_checksum_sha256": "e" * 64,
                    "selection_context_json": {
                        "worker_mapping_version": worker_module._ESTIMATE_WORKER_MAPPING_VERSION,
                        "line_key": "line-rate-b",
                        "line_type": "rate",
                        "description": "Paint labor B",
                        "quantity_item_id": str(quantity_item.id),
                        "catalog_entry_key": "shared:paint-labor",
                    },
                },
            ],
        )

        async def _resolve_rate(*args: Any, **kwargs: Any) -> Any:
            _ = args
            catalog_ref = kwargs["ref"]
            if catalog_ref.id == uuid.UUID("00000000-0000-0000-0000-000000000241"):
                return types.SimpleNamespace(
                    id=catalog_ref.id,
                    rate_key="paint-labor-a",
                    item_type="labor",
                    unit=quantity_item.unit,
                    currency="GBP",
                    value=Decimal("12.50"),
                    effective_start=date(2026, 1, 2),
                    checksum_sha256="d" * 64,
                    metadata={},
                )
            if catalog_ref.id == uuid.UUID("00000000-0000-0000-0000-000000000242"):
                return types.SimpleNamespace(
                    id=catalog_ref.id,
                    rate_key="paint-labor-b",
                    item_type="labor",
                    unit=quantity_item.unit,
                    currency="GBP",
                    value=Decimal("13.50"),
                    effective_start=date(2026, 1, 2),
                    checksum_sha256="e" * 64,
                    metadata={},
                )
            raise AssertionError(f"Unexpected rate ref: {catalog_ref.id}")

        monkeypatch.setattr(worker_module, "resolve_rate", _resolve_rate)

        await worker_module.process_estimate_job(estimate_job.id)

        updated_job = await _get_job(estimate_job.id)
        estimate_versions = await _get_estimate_versions_for_job(estimate_job.id)
        assert updated_job.status == "failed"
        assert updated_job.error_code == ErrorCode.INPUT_INVALID.value
        assert updated_job.error_message == worker_module._ESTIMATE_JOB_INPUT_INVALID_ERROR_MESSAGE
        assert estimate_versions == []

        response = await async_client.get(f"/v1/jobs/{estimate_job.id}/events")
        assert response.status_code == 200
        event_payload = response.json()["items"][-1]["data_json"]
        assert event_payload["status"] == "failed"
        assert event_payload["details"]["reason"] == "colliding_catalog_entry_key"

    async def test_process_estimate_job_honors_cancel_before_finalization(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Estimate finalization should re-check cancellation before publishing output."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        (
            _,
            _,
            _,
            _,
            _,
            quantity_takeoff,
            quantity_items,
            estimate_job,
        ) = await _create_ready_estimate_execution_job(async_client, monkeypatch)
        quantity_item = _select_eligible_aggregate_quantity_item(quantity_items)

        await _persist_estimate_job_input(
            estimate_job=estimate_job,
            quantity_takeoff=quantity_takeoff,
            assumptions_json={},
            catalog_refs=[
                {
                    "ref_type": "rate",
                    "selection_key": "paint-labor",
                    "ref_order": 20,
                    "rate_catalog_entry_id": uuid.UUID("00000000-0000-0000-0000-000000000232"),
                    "catalog_checksum_sha256": "8" * 64,
                    "selection_context_json": {
                        "worker_mapping_version": worker_module._ESTIMATE_WORKER_MAPPING_VERSION,
                        "line_key": "line-rate",
                        "line_type": "rate",
                        "description": "Paint labor",
                        "quantity_item_id": str(quantity_item.id),
                    },
                }
            ],
        )

        async def _resolve_rate(*args: Any, **kwargs: Any) -> Any:
            _ = (args, kwargs)
            return types.SimpleNamespace(
                id=uuid.UUID("00000000-0000-0000-0000-000000000232"),
                rate_key="paint-labor",
                item_type="labor",
                unit=quantity_item.unit,
                currency="GBP",
                value=Decimal("12.50"),
                effective_start=date(2026, 1, 2),
                checksum_sha256="8" * 64,
                metadata={},
            )

        original_build_execution_input = worker_module._build_estimate_engine_input

        async def _cancel_after_input_load(
            job_id: uuid.UUID,
            *,
            attempt_token: uuid.UUID,
        ) -> Any:
            engine_input = await original_build_execution_input(job_id, attempt_token=attempt_token)
            await _update_job(job_id, cancel_requested=True)
            return engine_input

        monkeypatch.setattr(worker_module, "resolve_rate", _resolve_rate)
        monkeypatch.setattr(
            worker_module,
            "_build_estimate_engine_input",
            _cancel_after_input_load,
        )

        await worker_module.process_estimate_job(estimate_job.id)

        updated_job = await _get_job(estimate_job.id)
        estimate_versions = await _get_estimate_versions_for_job(estimate_job.id)
        assert updated_job.status == "cancelled"
        assert updated_job.attempts == 1
        assert estimate_versions == []

        response = await async_client.get(f"/v1/jobs/{estimate_job.id}/events")
        assert response.status_code == 200
        assert response.json()["items"][-1]["data_json"] == {"status": "cancelled"}

    @pytest.mark.parametrize("failure_mode", ["mapping", "engine"])
    async def test_process_estimate_job_prefers_cancelled_for_deterministic_failures(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
        failure_mode: str,
    ) -> None:
        """Cancel requests should win over deterministic estimate mapping and engine failures."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        (
            _,
            _,
            _,
            _,
            _,
            quantity_takeoff,
            quantity_items,
            estimate_job,
        ) = await _create_ready_estimate_execution_job(async_client, monkeypatch)
        quantity_item = _select_eligible_aggregate_quantity_item(quantity_items)

        await _persist_estimate_job_input(
            estimate_job=estimate_job,
            quantity_takeoff=quantity_takeoff,
            assumptions_json={},
            catalog_refs=[
                {
                    "ref_type": "rate",
                    "selection_key": "paint-labor",
                    "ref_order": 20,
                    "rate_catalog_entry_id": uuid.UUID("00000000-0000-0000-0000-000000000234"),
                    "catalog_checksum_sha256": "a" * 64,
                    "selection_context_json": {
                        "worker_mapping_version": worker_module._ESTIMATE_WORKER_MAPPING_VERSION,
                        "line_key": "line-rate",
                        "line_type": "rate",
                        "description": "Paint labor",
                        "quantity_item_id": str(quantity_item.id),
                    },
                }
            ],
        )

        async def _resolve_rate(*args: Any, **kwargs: Any) -> Any:
            _ = (args, kwargs)
            return types.SimpleNamespace(
                id=uuid.UUID("00000000-0000-0000-0000-000000000234"),
                rate_key="paint-labor",
                item_type="labor",
                unit=quantity_item.unit,
                currency="GBP",
                value=Decimal("12.50"),
                effective_start=date(2026, 1, 2),
                checksum_sha256="a" * 64,
                metadata={},
            )

        original_build_execution_input = worker_module._build_estimate_engine_input

        async def _cancel_with_failure(
            job_id: uuid.UUID,
            *,
            attempt_token: uuid.UUID,
        ) -> Any:
            engine_input = await original_build_execution_input(job_id, attempt_token=attempt_token)
            await _update_job(job_id, cancel_requested=True)
            if failure_mode == "mapping":
                raise worker_module._build_estimate_job_input_error(
                    "missing_worker_mapping_version"
                )
            return engine_input

        def _raise_engine_input_invalid(_: Any) -> Any:
            raise EstimateEngineError(
                cast(Any, "INPUT_INVALID"),
                "engine_input_invalid",
                "engine rejected input",
            )

        monkeypatch.setattr(worker_module, "resolve_rate", _resolve_rate)
        monkeypatch.setattr(
            worker_module,
            "_build_estimate_engine_input",
            _cancel_with_failure,
        )
        if failure_mode == "engine":
            monkeypatch.setattr(worker_module, "compose_estimate", _raise_engine_input_invalid)

        await worker_module.process_estimate_job(estimate_job.id)

        updated_job = await _get_job(estimate_job.id)
        estimate_versions = await _get_estimate_versions_for_job(estimate_job.id)
        assert updated_job.status == "cancelled"
        assert updated_job.attempts == 1
        assert updated_job.cancel_requested is True
        assert updated_job.error_code == ErrorCode.JOB_CANCELLED.value
        assert updated_job.error_message is None
        assert estimate_versions == []

        response = await async_client.get(f"/v1/jobs/{estimate_job.id}/events")
        assert response.status_code == 200
        assert response.json()["items"][-1]["data_json"] == {"status": "cancelled"}

    async def test_finalize_estimate_job_skips_existing_output_without_duplicates(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Estimate finalization should not publish duplicates when output already exists."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        (
            _,
            _,
            _,
            _,
            _,
            quantity_takeoff,
            quantity_items,
            estimate_job,
        ) = await _create_ready_estimate_execution_job(async_client, monkeypatch)
        quantity_item = _select_eligible_aggregate_quantity_item(quantity_items)

        await _persist_estimate_job_input(
            estimate_job=estimate_job,
            quantity_takeoff=quantity_takeoff,
            assumptions_json={},
            catalog_refs=[
                {
                    "ref_type": "rate",
                    "selection_key": "paint-labor",
                    "ref_order": 20,
                    "rate_catalog_entry_id": uuid.UUID("00000000-0000-0000-0000-000000000233"),
                    "catalog_checksum_sha256": "9" * 64,
                    "selection_context_json": {
                        "worker_mapping_version": worker_module._ESTIMATE_WORKER_MAPPING_VERSION,
                        "line_key": "line-rate",
                        "line_type": "rate",
                        "description": "Paint labor",
                        "quantity_item_id": str(quantity_item.id),
                    },
                }
            ],
        )

        async def _resolve_rate(*args: Any, **kwargs: Any) -> Any:
            _ = (args, kwargs)
            return types.SimpleNamespace(
                id=uuid.UUID("00000000-0000-0000-0000-000000000233"),
                rate_key="paint-labor",
                item_type="labor",
                unit=quantity_item.unit,
                currency="GBP",
                value=Decimal("12.50"),
                effective_start=date(2026, 1, 2),
                checksum_sha256="9" * 64,
                metadata={},
            )

        monkeypatch.setattr(worker_module, "resolve_rate", _resolve_rate)

        await worker_module.process_estimate_job(estimate_job.id)
        estimate_versions = await _get_estimate_versions_for_job(estimate_job.id)
        assert len(estimate_versions) == 1
        estimate_version = estimate_versions[0]
        line_item_ids_before = [
            item.id for item in await _get_estimate_items_for_version(estimate_version.id)
        ]
        response_before = await async_client.get(f"/v1/jobs/{estimate_job.id}/events")
        assert response_before.status_code == 200

        duplicate_attempt_token = uuid.uuid4()
        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            persisted_job = await session.get(Job, estimate_job.id)
            assert persisted_job is not None
            persisted_job.status = "running"
            persisted_job.finished_at = None
            persisted_job.attempt_token = duplicate_attempt_token
            persisted_job.attempt_lease_expires_at = datetime.now(UTC) + timedelta(minutes=5)
            await session.commit()

        engine_input = await worker_module._build_estimate_engine_input(
            estimate_job.id,
            attempt_token=duplicate_attempt_token,
        )
        finalized = await worker_module._finalize_estimate_job(
            estimate_job.id,
            attempt_token=duplicate_attempt_token,
            output=real_compose_estimate(engine_input),
        )
        finalized_repeat = await worker_module._finalize_estimate_job(
            estimate_job.id,
            attempt_token=duplicate_attempt_token,
            output=real_compose_estimate(engine_input),
        )

        assert finalized is False
        assert finalized_repeat is False
        estimate_versions_after = await _get_estimate_versions_for_job(estimate_job.id)
        assert len(estimate_versions_after) == 1
        assert estimate_versions_after[0].id == estimate_version.id
        assert [
            item.id for item in await _get_estimate_items_for_version(estimate_version.id)
        ] == line_item_ids_before
        response_after = await async_client.get(f"/v1/jobs/{estimate_job.id}/events")
        assert response_after.status_code == 200
        assert response_after.json()["items"] == response_before.json()["items"]
        updated_job = await _get_job(estimate_job.id)
        assert updated_job.status == "running"
        assert updated_job.attempts == 1
        assert updated_job.attempt_token == duplicate_attempt_token

    async def test_finalize_estimate_job_skips_stale_attempt_without_output(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Estimate finalization should no-op when a stale attempt loses ownership."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        (
            _,
            _,
            _,
            _,
            _,
            quantity_takeoff,
            quantity_items,
            estimate_job,
        ) = await _create_ready_estimate_execution_job(async_client, monkeypatch)
        quantity_item = _select_eligible_aggregate_quantity_item(quantity_items)

        await _persist_estimate_job_input(
            estimate_job=estimate_job,
            quantity_takeoff=quantity_takeoff,
            assumptions_json={},
            catalog_refs=[
                {
                    "ref_type": "rate",
                    "selection_key": "paint-labor",
                    "ref_order": 20,
                    "rate_catalog_entry_id": uuid.UUID("00000000-0000-0000-0000-000000000235"),
                    "catalog_checksum_sha256": "b" * 64,
                    "selection_context_json": {
                        "worker_mapping_version": worker_module._ESTIMATE_WORKER_MAPPING_VERSION,
                        "line_key": "line-rate",
                        "line_type": "rate",
                        "description": "Paint labor",
                        "quantity_item_id": str(quantity_item.id),
                    },
                }
            ],
        )

        async def _resolve_rate(*args: Any, **kwargs: Any) -> Any:
            _ = (args, kwargs)
            return types.SimpleNamespace(
                id=uuid.UUID("00000000-0000-0000-0000-000000000235"),
                rate_key="paint-labor",
                item_type="labor",
                unit=quantity_item.unit,
                currency="GBP",
                value=Decimal("12.50"),
                effective_start=date(2026, 1, 2),
                checksum_sha256="b" * 64,
                metadata={},
            )

        monkeypatch.setattr(worker_module, "resolve_rate", _resolve_rate)

        stale_lease = await worker_module._begin_or_resume_estimate_job(estimate_job.id)
        assert stale_lease is not None
        engine_input = await worker_module._build_estimate_engine_input(
            estimate_job.id,
            attempt_token=stale_lease.token,
        )
        estimate_output = real_compose_estimate(engine_input)

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            persisted_job = await session.get(Job, estimate_job.id)
            assert persisted_job is not None
            persisted_job.status = "running"
            persisted_job.started_at = (
                datetime.now(UTC) - worker_module._RUNNING_JOB_STALE_AFTER - timedelta(seconds=1)
            )
            persisted_job.attempt_token = stale_lease.token
            persisted_job.attempt_lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            await session.commit()

        current_lease = await worker_module._begin_or_resume_estimate_job(estimate_job.id)
        assert current_lease is not None
        assert current_lease.token != stale_lease.token

        finalized = await worker_module._finalize_estimate_job(
            estimate_job.id,
            attempt_token=stale_lease.token,
            output=estimate_output,
        )

        assert finalized is False
        assert await _get_estimate_versions_for_job(estimate_job.id) == []
        updated_job = await _get_job(estimate_job.id)
        assert updated_job.status == "running"
        assert updated_job.attempts == 2
        assert updated_job.attempt_token == current_lease.token
        assert updated_job.finished_at is None

    async def test_process_estimate_job_terminal_redelivery_does_not_duplicate_output(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A duplicate delivery after success should not create another estimate output."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        (
            _,
            _,
            _,
            _,
            _,
            quantity_takeoff,
            quantity_items,
            estimate_job,
        ) = await _create_ready_estimate_execution_job(async_client, monkeypatch)
        quantity_item = _select_eligible_aggregate_quantity_item(quantity_items)

        await _persist_estimate_job_input(
            estimate_job=estimate_job,
            quantity_takeoff=quantity_takeoff,
            assumptions_json={"tax_rate": "0.20"},
            catalog_refs=[
                {
                    "ref_type": "rate",
                    "selection_key": "paint-labor",
                    "ref_order": 20,
                    "rate_catalog_entry_id": uuid.UUID("00000000-0000-0000-0000-000000000239"),
                    "catalog_checksum_sha256": "f" * 64,
                    "selection_context_json": {
                        "worker_mapping_version": worker_module._ESTIMATE_WORKER_MAPPING_VERSION,
                        "line_key": "line-rate",
                        "line_type": "rate",
                        "description": "Paint labor",
                        "quantity_item_id": str(quantity_item.id),
                    },
                }
            ],
        )

        async def _resolve_rate(*args: Any, **kwargs: Any) -> Any:
            _ = (args, kwargs)
            return types.SimpleNamespace(
                id=uuid.UUID("00000000-0000-0000-0000-000000000239"),
                rate_key="paint-labor",
                item_type="labor",
                unit=quantity_item.unit,
                currency="GBP",
                value=Decimal("12.50"),
                effective_start=date(2026, 1, 2),
                checksum_sha256="f" * 64,
                metadata={"crew": "A"},
            )

        monkeypatch.setattr(worker_module, "resolve_rate", _resolve_rate)

        await worker_module.process_estimate_job(estimate_job.id)
        response_before = await async_client.get(f"/v1/jobs/{estimate_job.id}/events")
        assert response_before.status_code == 200
        versions_before = await _get_estimate_versions_for_job(estimate_job.id)
        assert len(versions_before) == 1
        version_before = versions_before[0]
        item_ids_before = [
            item.id for item in await _get_estimate_items_for_version(version_before.id)
        ]

        await worker_module.process_estimate_job(estimate_job.id)

        updated_job = await _get_job(estimate_job.id)
        versions_after = await _get_estimate_versions_for_job(estimate_job.id)
        assert len(versions_after) == 1
        version_after = versions_after[0]
        item_ids_after = [
            item.id for item in await _get_estimate_items_for_version(version_after.id)
        ]
        response_after = await async_client.get(f"/v1/jobs/{estimate_job.id}/events")
        assert response_after.status_code == 200
        assert updated_job.status == "succeeded"
        assert updated_job.attempts == 1
        assert version_after.id == version_before.id
        assert item_ids_after == item_ids_before
        assert response_after.json()["items"] == response_before.json()["items"]

    async def test_estimate_worker_persists_replayable_historical_output_after_catalog_evolution(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Estimate outputs should stay replayable after append-only catalog evolution."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        (
            _,
            _,
            _,
            _,
            _,
            quantity_takeoff,
            quantity_items,
            estimate_job,
        ) = await _create_ready_estimate_execution_job(async_client, monkeypatch)
        quantity_item = _select_eligible_aggregate_quantity_item(quantity_items)
        assert quantity_item.value is not None
        assert quantity_item.value > 0

        rate_key = "historical-paint-labor"
        original_rate_id = uuid.UUID("00000000-0000-0000-0000-000000000291")
        evolved_rate_id = uuid.UUID("00000000-0000-0000-0000-000000000292")
        original_checksum = "1" * 64
        evolved_checksum = "2" * 64

        await _persist_estimate_job_input(
            estimate_job=estimate_job,
            quantity_takeoff=quantity_takeoff,
            assumptions_json={"tax_rate": "0.20"},
            catalog_refs=[
                {
                    "ref_type": "rate",
                    "selection_key": rate_key,
                    "ref_order": 20,
                    "rate_catalog_entry_id": original_rate_id,
                    "catalog_checksum_sha256": original_checksum,
                    "selection_context_json": {
                        "worker_mapping_version": worker_module._ESTIMATE_WORKER_MAPPING_VERSION,
                        "line_key": "historical-rate-line",
                        "line_type": "rate",
                        "description": "Historical paint labor",
                        "quantity_item_id": str(quantity_item.id),
                    },
                }
            ],
        )

        async def _resolve_latest_rate(*args: Any, **kwargs: Any) -> Any:
            _ = (args, kwargs)
            session_maker = session_module.AsyncSessionLocal
            assert session_maker is not None

            async with session_maker() as session:
                rate_row = (
                    (
                        await session.execute(
                            select(EstimationRate)
                            .where(
                                EstimationRate.project_id == estimate_job.project_id,
                                EstimationRate.rate_key == rate_key,
                            )
                            .order_by(
                                EstimationRate.effective_from.desc(),
                                EstimationRate.created_at.desc(),
                                EstimationRate.id.desc(),
                            )
                            .limit(1)
                        )
                    )
                    .scalars()
                    .one()
                )

            return types.SimpleNamespace(
                id=rate_row.id,
                rate_key=rate_row.rate_key,
                item_type=rate_row.item_type,
                unit=quantity_item.unit,
                currency=rate_row.currency,
                value=rate_row.amount,
                effective_start=rate_row.effective_from,
                checksum_sha256=rate_row.checksum_sha256,
                metadata=deepcopy(rate_row.metadata_json),
            )

        monkeypatch.setattr(worker_module, "resolve_rate", _resolve_latest_rate)

        await worker_module.process_estimate_job(estimate_job.id)

        updated_job = await _get_job(estimate_job.id)
        estimate_versions = await _get_estimate_versions_for_job(estimate_job.id)
        assert updated_job.status == "succeeded"
        assert len(estimate_versions) == 1
        estimate_version = estimate_versions[0]
        snapshot_entries = await _get_estimate_snapshot_entries_for_version(estimate_version.id)
        line_items = await _get_estimate_items_for_version(estimate_version.id)
        assert snapshot_entries != []
        assert line_items != []

        original_payload = _estimate_persisted_semantic_payload(
            estimate_version,
            snapshot_entries,
            line_items,
        )
        original_rate_snapshot = next(
            entry for entry in original_payload["snapshot_entries"] if entry["entry_type"] == "rate"
        )
        assert original_rate_snapshot["source_checksum_sha256"] == original_checksum

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            session.add(
                EstimationRate(
                    id=evolved_rate_id,
                    scope_type=CatalogScopeType.PROJECT,
                    project_id=estimate_job.project_id,
                    rate_key=rate_key,
                    source="tests.test_jobs",
                    metadata_json={"catalog_generation": "evolved"},
                    name=rate_key,
                    item_type="labor",
                    per_unit="square_meter",
                    currency="GBP",
                    amount=Decimal("99.00"),
                    effective_from=date(2026, 1, 3),
                    effective_to=None,
                    checksum_sha256=evolved_checksum,
                )
            )
            replay_attempt_token = uuid.uuid4()
            persisted_job = await session.get(Job, estimate_job.id)
            assert persisted_job is not None
            persisted_job.status = "running"
            persisted_job.finished_at = None
            persisted_job.attempt_token = replay_attempt_token
            persisted_job.attempt_lease_expires_at = datetime.now(UTC) + timedelta(minutes=5)
            await session.commit()

        replay_input = await worker_module._build_estimate_engine_input(
            estimate_job.id,
            attempt_token=replay_attempt_token,
        )
        replay_output = real_compose_estimate(replay_input)
        replay_version_kwargs = replay_output.estimate_version_model_kwargs()
        replay_snapshot_kwargs = replay_output.snapshot_entry_model_kwargs()
        replay_rate_snapshot = next(
            entry for entry in replay_snapshot_kwargs if entry["entry_type"] == "rate"
        )
        assert replay_rate_snapshot["source_checksum_sha256"] == evolved_checksum
        assert replay_rate_snapshot["unit_amount"] != original_rate_snapshot["unit_amount"]
        assert replay_version_kwargs["total_amount"] != original_payload["version"]["total_amount"]

        finalized_again = await worker_module._finalize_estimate_job(
            estimate_job.id,
            attempt_token=replay_attempt_token,
            output=replay_output,
        )
        assert finalized_again is False

        estimate_versions_after = await _get_estimate_versions_for_job(estimate_job.id)
        assert len(estimate_versions_after) == 1
        snapshot_entries_after = await _get_estimate_snapshot_entries_for_version(
            estimate_versions_after[0].id
        )
        line_items_after = await _get_estimate_items_for_version(estimate_versions_after[0].id)
        evolved_payload = _estimate_persisted_semantic_payload(
            estimate_versions_after[0],
            snapshot_entries_after,
            line_items_after,
        )
        assert evolved_payload == original_payload

    def test_build_estimate_worker_mapping_v1_maps_rate_material_and_formula_refs(
        self,
    ) -> None:
        """Estimate mapping helper should derive defaults, sort lines, and dedup entries."""
        _ = self
        shared_quantity_item_id = uuid.uuid4()
        catalog_refs = [
            _estimate_catalog_ref_stub(
                ref_type="rate",
                selection_key="paint-labor",
                line_key="line-rate",
                ref_order=20,
                description="Paint labor",
                quantity_item_id=shared_quantity_item_id,
            ),
            _estimate_catalog_ref_stub(
                ref_type="formula",
                selection_key="waste-10",
                line_key="line-formula",
                ref_order=30,
                description="Waste allowance",
                formula_inputs={
                    "operand_line_keys": ["line-rate", "line-material"],
                    "expression": "subtotal * 1.10",
                },
            ),
            _estimate_catalog_ref_stub(
                ref_type="material",
                selection_key="paint-gallon",
                line_key="line-material",
                ref_order=20,
                description="Paint gallon",
                quantity_item_id=shared_quantity_item_id,
            ),
        ]

        assembly = worker_module._build_estimate_worker_mapping_v1(catalog_refs)

        assert [line.line_key for line in assembly.lines] == [
            "line-material",
            "line-rate",
            "line-formula",
        ]
        assert [line.ref_type for line in assembly.lines] == ["material", "rate", "formula"]
        assert [line.line_type for line in assembly.lines] == ["material", "rate", "formula"]
        assert [line.description for line in assembly.lines] == [
            "Paint gallon",
            "Paint labor",
            "Waste allowance",
        ]
        assert [line.ref_order for line in assembly.lines] == [20, 20, 30]
        assert [line.catalog_entry_key for line in assembly.lines] == [
            "material:paint-gallon",
            "rate:paint-labor",
            "formula:waste-10",
        ]
        assert len(assembly.lines) == 3
        assert assembly.lines[0].quantity_item_id == shared_quantity_item_id
        assert assembly.lines[1].quantity_item_id == shared_quantity_item_id
        assert assembly.lines[2].quantity_item_id is None
        assert assembly.lines[0].quantity_entry_key == f"quantity:{shared_quantity_item_id}"
        assert assembly.lines[1].quantity_entry_key == f"quantity:{shared_quantity_item_id}"
        assert assembly.lines[2].formula_inputs == {
            "operand_line_keys": ["line-rate", "line-material"],
            "expression": "subtotal * 1.10",
        }
        assert [entry.entry_key for entry in assembly.quantity_entries] == [
            f"quantity:{shared_quantity_item_id}"
        ]
        assert assembly.quantity_entries[0].quantity_item_id == shared_quantity_item_id

    @pytest.mark.parametrize(
        ("catalog_refs", "reason"),
        [
            pytest.param(
                [
                    _estimate_catalog_ref_stub(
                        ref_type="rate",
                        selection_key="missing-version",
                        line_key="line-rate",
                        ref_order=10,
                        worker_mapping_version=None,
                        quantity_item_id=uuid.UUID("00000000-0000-0000-0000-000000000101"),
                    )
                ],
                "missing_worker_mapping_version",
                id="missing-version",
            ),
            pytest.param(
                [
                    _estimate_catalog_ref_stub(
                        ref_type="rate",
                        selection_key="first",
                        line_key="line-duplicate",
                        ref_order=10,
                        quantity_item_id=uuid.UUID("00000000-0000-0000-0000-000000000102"),
                    ),
                    _estimate_catalog_ref_stub(
                        ref_type="material",
                        selection_key="duplicate",
                        line_key="line-duplicate",
                        ref_order=20,
                        quantity_item_id=uuid.UUID("00000000-0000-0000-0000-000000000102"),
                    ),
                ],
                "duplicate_line_key",
                id="duplicate-line-key",
            ),
            pytest.param(
                [
                    _estimate_catalog_ref_stub(
                        ref_type="material",
                        selection_key="mismatched",
                        line_key="line-mismatched",
                        ref_order=10,
                        mapped_line_type="rate",
                        quantity_item_id=uuid.UUID("00000000-0000-0000-0000-000000000103"),
                    )
                ],
                "mismatched_line_ref_type",
                id="mismatched-ref-type",
            ),
        ],
    )
    def test_build_estimate_worker_mapping_v1_rejects_invalid_refs(
        self,
        catalog_refs: list[types.SimpleNamespace],
        reason: str,
    ) -> None:
        """Estimate mapping helper should reject invalid contract inputs before output assembly."""
        _ = self

        with pytest.raises(worker_module._EstimateJobInputError) as exc_info:
            worker_module._build_estimate_worker_mapping_v1(catalog_refs)

        _assert_estimate_mapping_invalid(exc_info, reason=reason)

    def test_enqueue_estimate_job_publishes_persisted_job_to_celery(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Estimate enqueue should publish the persisted job identifier to Celery."""
        _ = self

        published: list[dict[str, Any]] = []

        def _fake_apply_async(*, args: tuple[str, ...], task_id: str, retry: bool) -> None:
            published.append({"args": args, "task_id": task_id, "retry": retry})

        job_id = uuid.uuid4()
        monkeypatch.setattr(worker_module.run_estimate_job, "apply_async", _fake_apply_async)

        worker_module.enqueue_estimate_job(job_id)

        assert published == [{"args": (str(job_id),), "task_id": str(job_id), "retry": False}]

    def test_run_estimate_job_dispatches_persisted_processor(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Estimate Celery wrapper should dispatch to the persisted processor."""
        _ = self

        processed_job_ids: list[uuid.UUID] = []

        async def _fake_process(job_id: uuid.UUID) -> None:
            processed_job_ids.append(job_id)

        job_id = uuid.uuid4()
        monkeypatch.setattr(worker_module, "process_estimate_job", _fake_process)

        worker_module.run_estimate_job(str(job_id))

        assert processed_job_ids == [job_id]
