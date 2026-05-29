"""Quantity worker integration tests."""

import json
import types
import uuid
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
import yaml  # type: ignore[import-untyped]
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

import app.db.session as session_module
import app.jobs.worker as worker_module
from app.core.errors import ErrorCode
from app.ingestion.finalization import IngestFinalizationPayload, compute_adapter_result_checksum
from app.ingestion.runner import IngestionRunRequest
from app.ingestion.runner import run_ingestion as real_run_ingestion
from app.models.drawing_revision import DrawingRevision
from app.models.job import Job, JobType
from app.models.quantity_takeoff import QuantityItem, QuantityTakeoff
from tests.conftest import requires_database
from tests.jobs_test_helpers import (
    _create_project,
    _get_job,
    _get_job_for_file,
    _mark_source_deleted,
    _update_job,
    _upload_file,
    fake_ingestion_runner,
)

_FAKE_RUNNER_ADAPTER_KEY = "tests.fake_ingestion_runner"
_FAKE_RUNNER_ADAPTER_VERSION = "1.0"
_FAKE_RUNNER_CANONICAL_SCHEMA_VERSION = "0.1"
_FAKE_RUNNER_VALIDATION_REPORT_SCHEMA_VERSION = "0.1"
_FAKE_RUNNER_CONFIDENCE_SCORE = 0.75
_FAKE_RUNNER_REVIEW_STATE = "provisional"
_FAKE_RUNNER_VALIDATION_STATUS = "valid"
_FAKE_RUNNER_QUANTITY_GATE = "allowed_provisional"
_FAKE_RUNNER_VALIDATOR_NAME = "tests.fake_ingestion_runner"
_FAKE_RUNNER_VALIDATOR_VERSION = "1.0"
_FIXTURE_MANIFEST_PATH = Path(__file__).with_name("fixtures") / "manifest.yaml"


def _assert_json_object(value: Any, label: str) -> dict[str, Any]:
    """Return a JSON object from fixture metadata or fail the test with context."""
    assert isinstance(value, dict), f"{label} must be an object"
    return cast(dict[str, Any], value)


@lru_cache(maxsize=1)
def _fixture_manifest_by_filename() -> dict[str, dict[str, Any]]:
    """Load committed fixture manifest entries keyed by fixture filename."""
    with _FIXTURE_MANIFEST_PATH.open("r", encoding="utf-8") as manifest_file:
        manifest = yaml.safe_load(manifest_file)

    root = _assert_json_object(manifest, "fixture manifest")
    fixtures = root.get("fixtures")
    assert isinstance(fixtures, list), "fixture manifest must contain a fixtures list"

    by_filename: dict[str, dict[str, Any]] = {}
    for raw_fixture in fixtures:
        fixture = _assert_json_object(raw_fixture, "fixture entry")
        filename = fixture.get("filename")
        assert isinstance(filename, str), "fixture entry must contain a filename"
        by_filename[filename] = fixture
    return by_filename


def _manifest_fixture(filename: str) -> dict[str, Any]:
    """Return one manifest fixture entry by filename."""
    fixture = _fixture_manifest_by_filename().get(filename)
    assert fixture is not None, f"fixture {filename!r} is missing from manifest"
    return fixture


def _manifest_quantity_check(filename: str, quantity_name: str) -> dict[str, Any]:
    """Return one manifest acceptance quantity check."""
    fixture = _manifest_fixture(filename)
    acceptance_checks = _assert_json_object(
        fixture.get("acceptance_checks"),
        f"{filename} acceptance_checks",
    )
    quantities = _assert_json_object(
        acceptance_checks.get("quantities"),
        f"{filename} quantity acceptance checks",
    )
    return _assert_json_object(
        quantities.get(quantity_name),
        f"{filename} quantity check {quantity_name}",
    )


def _manifest_expected_number(filename: str, quantity_name: str) -> float:
    """Return the numeric expected value from a manifest quantity check."""
    expected = _manifest_quantity_check(filename, quantity_name).get("expected")
    assert isinstance(expected, int | float) and not isinstance(expected, bool)
    return float(expected)


def _manifest_expected_text(filename: str, field_name: str) -> str:
    """Return a string field from a manifest fixture entry."""
    value = _manifest_fixture(filename).get(field_name)
    assert isinstance(value, str), f"{filename} {field_name} must be a string"
    return value


def _fixture_bytes(filename: str) -> bytes:
    """Read committed fixture bytes for API upload tests."""
    path = _FIXTURE_MANIFEST_PATH.parent / filename
    assert path.is_file(), f"fixture file {filename!r} does not exist"
    return path.read_bytes()


def _manifest_dxf_line_expectations(filename: str = "dxf/simple-line.dxf") -> tuple[int, float]:
    """Return manifest-driven DXF smoke line count and total length expectations."""
    raw_line_count = _manifest_expected_number(filename, "line_count")
    assert raw_line_count.is_integer(), "manifest line_count must be integral"
    return int(raw_line_count), _manifest_expected_number(filename, "total_length")


def _manifest_line_geometry(length: float) -> dict[str, Any]:
    """Build a line geometry whose measured length comes from the fixture manifest."""
    return {
        "type": "line",
        "start": [0.0, 0.0, 0.0],
        "end": [length, 0.0, 0.0],
        "units": {"normalized": "meter"},
    }


def _quantity_item_values_by_type(items: list[QuantityItem], *, item_kind: str) -> dict[str, float]:
    """Return persisted quantity item values keyed by quantity type."""
    values: dict[str, float] = {}
    for item in items:
        if item.item_kind != item_kind:
            continue
        assert item.value is not None
        values[item.quantity_type] = item.value
    return values


def _quantity_length_total(values_by_type: dict[str, float]) -> float:
    """Return the single length total regardless of future context suffixes."""
    length_values = [
        value
        for quantity_type, value in values_by_type.items()
        if quantity_type.startswith("length")
    ]
    assert len(length_values) == 1
    return length_values[0]


def _quantity_takeoff_semantic_payload(
    takeoff: QuantityTakeoff,
    items: list[QuantityItem],
) -> dict[str, Any]:
    """Return rerun-comparable quantity semantics, excluding volatile ids/timestamps."""
    item_payloads = [
        {
            "item_kind": item.item_kind,
            "quantity_type": item.quantity_type,
            "value": item.value,
            "unit": item.unit,
            "review_state": item.review_state,
            "validation_status": item.validation_status,
            "quantity_gate": item.quantity_gate,
            "source_entity_id": item.source_entity_id,
            "excluded_source_entity_ids_json": item.excluded_source_entity_ids_json,
        }
        for item in items
    ]
    return {
        "review_state": takeoff.review_state,
        "validation_status": takeoff.validation_status,
        "quantity_gate": takeoff.quantity_gate,
        "trusted_totals": takeoff.trusted_totals,
        "items": sorted(
            item_payloads,
            key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")),
        ),
    }


def _resolve_fake_revision_kind(request: IngestionRunRequest) -> str:
    """Match ingest vs reprocess semantics for fake runner payloads."""
    if request.initial_job_id == request.job_id:
        return "ingest"

    return "reprocess"


def _resolve_fake_input_family(request: IngestionRunRequest) -> str:
    """Return a stable runner-like input family for tests."""
    if request.detected_format == "pdf" and request.media_type == "application/pdf":
        return "pdf_vector"

    if request.detected_format is not None:
        return request.detected_format

    return "unknown"


def _build_fake_ingest_payload(
    request: IngestionRunRequest,
    *,
    generated_at: datetime | None = None,
) -> IngestFinalizationPayload:
    """Build a deterministic fake finalization payload for worker tests."""
    normalized_generated_at = generated_at or datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    revision_kind = _resolve_fake_revision_kind(request)
    input_family = _resolve_fake_input_family(request)
    entity_counts = {
        "layouts": 1,
        "layers": 1,
        "blocks": 0,
        "entities": 1,
    }
    canonical_json = {
        "canonical_entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
        "schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
        "layouts": [{"name": "Model"}],
        "layers": [{"name": "A-WALL"}],
        "blocks": [],
        "entities": [{"kind": "line", "layer": "A-WALL"}],
        "entity_counts": entity_counts,
    }
    provenance_json = {
        "schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
        "bridge": "tests.fake_ingestion_runner",
        "adapter": {
            "key": _FAKE_RUNNER_ADAPTER_KEY,
            "version": _FAKE_RUNNER_ADAPTER_VERSION,
        },
        "source": {
            "file_id": str(request.file_id),
            "job_id": str(request.job_id),
            "extraction_profile_id": (
                str(request.extraction_profile_id)
                if request.extraction_profile_id is not None
                else None
            ),
            "input_family": input_family,
            "revision_kind": revision_kind,
            "original_name": request.original_name,
        },
        "generated_at": normalized_generated_at.isoformat(),
    }
    confidence_json = {
        "score": _FAKE_RUNNER_CONFIDENCE_SCORE,
        "effective_confidence": _FAKE_RUNNER_CONFIDENCE_SCORE,
        "review_state": _FAKE_RUNNER_REVIEW_STATE,
    }
    warnings_json: list[Any] = []
    diagnostics_json = {
        "runner": "tests.fake_ingestion_runner",
        "detected_format": request.detected_format,
    }
    report_json = {
        "validation_report_schema_version": _FAKE_RUNNER_VALIDATION_REPORT_SCHEMA_VERSION,
        "canonical_entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
        "validator": {
            "name": _FAKE_RUNNER_VALIDATOR_NAME,
            "version": _FAKE_RUNNER_VALIDATOR_VERSION,
        },
        "summary": {
            "validation_status": _FAKE_RUNNER_VALIDATION_STATUS,
            "review_state": _FAKE_RUNNER_REVIEW_STATE,
            "quantity_gate": _FAKE_RUNNER_QUANTITY_GATE,
            "effective_confidence": _FAKE_RUNNER_CONFIDENCE_SCORE,
            "entity_counts": entity_counts,
        },
        "checks": [],
        "findings": [],
        "adapter_warnings": warnings_json,
        "provenance": provenance_json,
    }
    result_envelope = {
        "adapter_key": _FAKE_RUNNER_ADAPTER_KEY,
        "adapter_version": _FAKE_RUNNER_ADAPTER_VERSION,
        "input_family": input_family,
        "canonical_entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
        "canonical_json": canonical_json,
        "provenance_json": provenance_json,
        "confidence_json": confidence_json,
        "confidence_score": _FAKE_RUNNER_CONFIDENCE_SCORE,
        "warnings_json": warnings_json,
        "diagnostics_json": diagnostics_json,
    }

    return IngestFinalizationPayload(
        revision_kind=revision_kind,
        adapter_key=_FAKE_RUNNER_ADAPTER_KEY,
        adapter_version=_FAKE_RUNNER_ADAPTER_VERSION,
        input_family=input_family,
        canonical_entity_schema_version=_FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
        canonical_json=canonical_json,
        provenance_json=provenance_json,
        confidence_json=confidence_json,
        confidence_score=_FAKE_RUNNER_CONFIDENCE_SCORE,
        warnings_json=warnings_json,
        diagnostics_json=diagnostics_json,
        result_checksum_sha256=compute_adapter_result_checksum(result_envelope),
        validation_report_schema_version=_FAKE_RUNNER_VALIDATION_REPORT_SCHEMA_VERSION,
        validation_status=_FAKE_RUNNER_VALIDATION_STATUS,
        review_state=_FAKE_RUNNER_REVIEW_STATE,
        quantity_gate=_FAKE_RUNNER_QUANTITY_GATE,
        effective_confidence=_FAKE_RUNNER_CONFIDENCE_SCORE,
        validator_name=_FAKE_RUNNER_VALIDATOR_NAME,
        validator_version=_FAKE_RUNNER_VALIDATOR_VERSION,
        report_json=report_json,
        generated_at=normalized_generated_at,
    )


def _build_fake_contract_entity(
    *,
    entity_id: str,
    entity_type: str,
    layer_ref: str,
    source_id: str,
    source_hash: str | None = None,
    geometry_json: dict[str, Any] | None = None,
    properties_json: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a contract-shaped canonical entity payload for quantity tests."""
    return {
        "entity_id": entity_id,
        "entity_type": entity_type,
        "entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
        "layout_ref": "Model",
        "layer_ref": layer_ref,
        "confidence_score": _FAKE_RUNNER_CONFIDENCE_SCORE,
        "confidence_json": {"score": _FAKE_RUNNER_CONFIDENCE_SCORE, "basis": "adapter"},
        "geometry_json": deepcopy(geometry_json) if geometry_json is not None else {},
        "properties_json": {"layer": layer_ref, **(properties_json or {})},
        "provenance_json": {
            "origin": "adapter_normalized",
            "adapter": {"key": _FAKE_RUNNER_ADAPTER_KEY},
            "source_ref": None,
            "source_identity": source_id,
            "source_hash": source_hash,
            "extraction_path": [],
            "notes": [],
        },
    }


def _replace_fake_canonical_payload(
    payload: IngestFinalizationPayload,
    *,
    entities: list[dict[str, Any]],
) -> IngestFinalizationPayload:
    """Replace canonical fake payload entities and refresh derived counts/checksum."""
    canonical_json = {
        **payload.canonical_json,
        "canonical_entity_schema_version": payload.canonical_entity_schema_version,
        "schema_version": payload.canonical_entity_schema_version,
        "layouts": [{"layout_ref": "Model", "name": "Model"}],
        "layers": [{"layer_ref": "A-WALL", "name": "A-WALL"}],
        "blocks": [],
        "entities": deepcopy(entities),
        "entity_counts": {
            "layouts": 1,
            "layers": 1,
            "blocks": 0,
            "entities": len(entities),
        },
    }
    report_json = deepcopy(payload.report_json)
    summary = report_json.get("summary")
    if isinstance(summary, dict):
        report_json["summary"] = {
            **summary,
            "entity_counts": canonical_json["entity_counts"],
        }
    result_envelope = {
        "adapter_key": payload.adapter_key,
        "adapter_version": payload.adapter_version,
        "input_family": payload.input_family,
        "canonical_entity_schema_version": payload.canonical_entity_schema_version,
        "canonical_json": canonical_json,
        "provenance_json": payload.provenance_json,
        "confidence_json": payload.confidence_json,
        "confidence_score": payload.confidence_score,
        "warnings_json": payload.warnings_json,
        "diagnostics_json": payload.diagnostics_json,
    }
    return replace(
        payload,
        canonical_json=canonical_json,
        report_json=report_json,
        result_checksum_sha256=compute_adapter_result_checksum(result_envelope),
    )


def _replace_fake_validation_outcome(
    payload: IngestFinalizationPayload,
    *,
    review_state: str,
    validation_status: str,
    quantity_gate: str,
) -> IngestFinalizationPayload:
    """Replace fake validation gate metadata while preserving deterministic payload shape."""
    confidence_json = {
        **payload.confidence_json,
        "review_state": review_state,
        "effective_confidence": payload.effective_confidence,
    }
    report_json = deepcopy(payload.report_json)
    summary = report_json.get("summary")
    if isinstance(summary, dict):
        report_json["summary"] = {
            **summary,
            "validation_status": validation_status,
            "review_state": review_state,
            "quantity_gate": quantity_gate,
            "effective_confidence": payload.effective_confidence,
        }
    report_json["validation_status"] = validation_status
    report_json["review_state"] = review_state
    report_json["quantity_gate"] = quantity_gate
    report_json["effective_confidence"] = payload.effective_confidence
    return replace(
        payload,
        confidence_json=confidence_json,
        validation_status=validation_status,
        review_state=review_state,
        quantity_gate=quantity_gate,
        report_json=report_json,
    )


def _build_fake_quantity_ingest_payload(
    request: IngestionRunRequest,
    *,
    review_state: str,
    validation_status: str,
    quantity_gate: str,
    entities: list[dict[str, Any]] | None = None,
) -> IngestFinalizationPayload:
    """Build a quantity-friendly fake ingest payload with configurable gate semantics."""
    payload = _replace_fake_canonical_payload(
        _build_fake_ingest_payload(request),
        entities=entities
        or [
            _build_fake_contract_entity(
                entity_id="entity-quantity-001",
                entity_type="line",
                layer_ref="A-WALL",
                source_id="entity-source-quantity-001",
            )
        ],
    )
    return _replace_fake_validation_outcome(
        payload,
        review_state=review_state,
        validation_status=validation_status,
        quantity_gate=quantity_gate,
    )


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


async def _create_quantity_takeoff_job(
    *,
    project_id: uuid.UUID,
    file_id: uuid.UUID,
    base_revision_id: uuid.UUID,
    parent_job_id: uuid.UUID,
    status: str,
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
        job_type=JobType.QUANTITY_TAKEOFF.value,
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


async def _create_real_dxf_quantity_takeoff_job(
    async_client: httpx.AsyncClient,
    fixture_filename: str,
) -> tuple[dict[str, Any], dict[str, Any], Job, DrawingRevision, Job]:
    """Create a pending quantity job from the committed DXF fixture via real ingestion."""
    project = await _create_project(async_client)
    uploaded = await _upload_file(
        async_client,
        project["id"],
        filename=Path(fixture_filename).name,
        content=_fixture_bytes(fixture_filename),
        media_type="application/dxf",
    )
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


@pytest.mark.usefixtures(fake_ingestion_runner.__name__)
@requires_database
class TestJobsWorkerQuantity:
    """Quantity worker tests split from the monolithic jobs module."""

    async def test_process_ingest_job_skips_quantity_takeoff_jobs_without_mutation(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Direct ingest-worker delivery should no-op for quantity jobs."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        async def _fake_run_ingestion(request: IngestionRunRequest) -> IngestFinalizationPayload:
            return _build_fake_ingest_payload(request)

        monkeypatch.setattr(worker_module, "run_ingestion", _fake_run_ingestion)

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
            attempts=1,
        )
        await _update_job(
            quantity_job.id,
            error_code=ErrorCode.INTERNAL_ERROR.value,
            error_message="quantity worker unavailable",
            enqueue_status="pending",
            enqueue_attempts=2,
            enqueue_owner_token=uuid.uuid4(),
            enqueue_lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
            enqueue_last_attempted_at=datetime.now(UTC) - timedelta(minutes=1),
            enqueue_published_at=datetime.now(UTC) - timedelta(minutes=2),
        )
        original = await _get_job(quantity_job.id)
        assert original.extraction_profile_id is None
        assert original.attempt_token is None
        assert original.attempt_lease_expires_at is None

        await worker_module.process_ingest_job(quantity_job.id)

        unchanged = await _get_job(quantity_job.id)
        assert unchanged.status == original.status
        assert unchanged.attempts == original.attempts
        assert unchanged.started_at == original.started_at
        assert unchanged.finished_at == original.finished_at
        assert unchanged.attempt_token == original.attempt_token
        assert unchanged.attempt_lease_expires_at == original.attempt_lease_expires_at
        assert unchanged.cancel_requested == original.cancel_requested
        assert unchanged.error_code == original.error_code
        assert unchanged.error_message == original.error_message
        assert unchanged.enqueue_status == original.enqueue_status
        assert unchanged.enqueue_attempts == original.enqueue_attempts
        assert unchanged.enqueue_owner_token == original.enqueue_owner_token
        assert unchanged.enqueue_lease_expires_at == original.enqueue_lease_expires_at
        assert unchanged.enqueue_last_attempted_at == original.enqueue_last_attempted_at
        assert unchanged.enqueue_published_at == original.enqueue_published_at
        assert unchanged.project_id == original.project_id
        assert unchanged.file_id == original.file_id
        assert unchanged.job_type == original.job_type
        assert unchanged.extraction_profile_id == original.extraction_profile_id
        assert unchanged.extraction_profile_id is None
        assert unchanged.base_revision_id == original.base_revision_id
        assert unchanged.parent_job_id == original.parent_job_id

    async def test_begin_or_resume_quantity_takeoff_job_skips_duplicate_running_attempt(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Quantity duplicate delivery should leave a fresh running attempt unchanged."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        _, _, _, _, quantity_job = await _create_ready_quantity_takeoff_job(async_client)
        attempt_token = uuid.uuid4()
        lease_expires_at = datetime.now(UTC) + worker_module._RUNNING_JOB_STALE_AFTER

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            persisted_job = await session.get(Job, quantity_job.id)
            assert persisted_job is not None
            persisted_job.status = "running"
            persisted_job.attempts = 1
            persisted_job.started_at = datetime.now(UTC)
            persisted_job.attempt_token = attempt_token
            persisted_job.attempt_lease_expires_at = lease_expires_at
            await session.commit()

        lease = await worker_module._begin_or_resume_quantity_takeoff_job(quantity_job.id)

        assert lease is None
        unchanged = await _get_job(quantity_job.id)
        assert unchanged.status == "running"
        assert unchanged.attempts == 1
        assert unchanged.started_at is not None
        assert unchanged.finished_at is None
        assert unchanged.attempt_token == attempt_token
        assert unchanged.attempt_lease_expires_at == lease_expires_at

        response = await async_client.get(f"/v1/jobs/{quantity_job.id}/events")
        assert response.status_code == 200
        assert response.json()["items"] == []

    async def test_resume_quantity_takeoff_job_reclaims_stale_attempt_with_new_token(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Quantity stale running attempts should be reclaimed with a fresh ownership token."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        _, _, _, _, quantity_job = await _create_ready_quantity_takeoff_job(async_client)
        old_attempt_token = uuid.uuid4()

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            persisted_job = await session.get(Job, quantity_job.id)
            assert persisted_job is not None
            persisted_job.status = "running"
            persisted_job.attempts = 1
            persisted_job.started_at = (
                datetime.now(UTC) - worker_module._RUNNING_JOB_STALE_AFTER - timedelta(seconds=1)
            )
            persisted_job.attempt_token = old_attempt_token
            persisted_job.attempt_lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            await session.commit()

        lease = await worker_module._begin_or_resume_quantity_takeoff_job(quantity_job.id)

        assert lease is not None
        assert lease.token != old_attempt_token
        reclaimed_job = await _get_job(quantity_job.id)
        assert reclaimed_job.status == "running"
        assert reclaimed_job.attempts == 2
        assert reclaimed_job.attempt_token == lease.token
        assert reclaimed_job.attempt_lease_expires_at == lease.lease_expires_at
        assert reclaimed_job.attempt_lease_expires_at is not None
        assert reclaimed_job.attempt_lease_expires_at > datetime.now(UTC)

        response = await async_client.get(f"/v1/jobs/{quantity_job.id}/events")
        assert response.status_code == 200
        items = response.json()["items"]
        assert [event["message"] for event in items] == ["Job started"]
        assert items[0]["data_json"] == {"status": "running", "attempts": 2, "reclaimed": True}

    async def test_begin_or_resume_quantity_takeoff_job_cancels_inactive_source(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Quantity begin/resume should cancel when the source project or file is inactive."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        (
            project,
            uploaded,
            _,
            _,
            quantity_job,
        ) = await _create_ready_quantity_takeoff_job(async_client)
        await _mark_source_deleted(
            uuid.UUID(project["id"]),
            uuid.UUID(uploaded["id"]),
            delete_project=False,
            delete_file=True,
        )

        lease = await worker_module._begin_or_resume_quantity_takeoff_job(quantity_job.id)

        assert lease is None
        cancelled = await _get_job(quantity_job.id)
        assert cancelled.status == "cancelled"
        assert cancelled.attempts == 0
        assert cancelled.cancel_requested is True
        assert cancelled.attempt_token is None
        assert cancelled.attempt_lease_expires_at is None
        assert cancelled.error_code == ErrorCode.JOB_CANCELLED.value
        assert cancelled.finished_at is not None

        response = await async_client.get(f"/v1/jobs/{quantity_job.id}/events")
        assert response.status_code == 200
        items = response.json()["items"]
        assert [event["message"] for event in items] == ["Job cancelled"]
        assert items[0]["data_json"] == {"status": "cancelled", "reason": "source_deleted"}

    async def test_begin_or_resume_quantity_takeoff_job_finalizes_cancel_requested_job_as_cancelled(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Quantity begin/resume should finalize cancel-requested jobs to cancelled."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        _, _, _, _, quantity_job = await _create_ready_quantity_takeoff_job(async_client)
        await _update_job(quantity_job.id, status="pending", cancel_requested=True)

        lease = await worker_module._begin_or_resume_quantity_takeoff_job(quantity_job.id)

        assert lease is None
        updated = await _get_job(quantity_job.id)
        assert updated.status == "cancelled"
        assert updated.attempts == 0
        assert updated.cancel_requested is True
        assert updated.attempt_token is None
        assert updated.attempt_lease_expires_at is None
        assert updated.error_code == ErrorCode.JOB_CANCELLED.value
        assert updated.finished_at is not None

        response = await async_client.get(f"/v1/jobs/{quantity_job.id}/events")
        assert response.status_code == 200
        items = response.json()["items"]
        assert [event["message"] for event in items] == ["Job cancelled"]
        assert items[0]["data_json"] == {"status": "cancelled"}

    async def test_begin_or_resume_quantity_takeoff_job_skips_terminal_status(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Quantity begin/resume should ignore already terminal jobs."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        _, _, _, _, quantity_job = await _create_ready_quantity_takeoff_job(async_client)
        await _update_job(
            quantity_job.id,
            status="failed",
            attempts=1,
            error_code=ErrorCode.INTERNAL_ERROR.value,
            error_message="done",
        )
        before = await _get_job(quantity_job.id)
        before_finished_at = before.finished_at

        lease = await worker_module._begin_or_resume_quantity_takeoff_job(quantity_job.id)

        assert lease is None
        unchanged = await _get_job(quantity_job.id)
        assert unchanged.status == "failed"
        assert unchanged.attempts == 1
        assert unchanged.attempt_token is None
        assert unchanged.attempt_lease_expires_at is None
        assert unchanged.error_code == ErrorCode.INTERNAL_ERROR.value
        assert unchanged.error_message == "done"
        assert unchanged.finished_at == before_finished_at

        response = await async_client.get(f"/v1/jobs/{quantity_job.id}/events")
        assert response.status_code == 200
        assert response.json()["items"] == []

    @pytest.mark.parametrize(
        ("review_state", "validation_status", "quantity_gate", "trusted_totals"),
        [
            ("approved", "valid", "allowed", True),
            ("provisional", "valid", "allowed_provisional", False),
        ],
    )
    async def test_process_quantity_takeoff_job_persists_expected_takeoff(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
        review_state: str,
        validation_status: str,
        quantity_gate: str,
        trusted_totals: bool,
    ) -> None:
        """Quantity worker should persist trusted outputs for allowed gates."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        async def _run_quantity_ready_ingestion(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            return _build_fake_quantity_ingest_payload(
                request,
                review_state=review_state,
                validation_status=validation_status,
                quantity_gate=quantity_gate,
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run_quantity_ready_ingestion)

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

        await worker_module.process_quantity_takeoff_job(quantity_job.id)

        updated_job = await _get_job(quantity_job.id)
        takeoffs = await _get_quantity_takeoffs_for_job(quantity_job.id)
        assert updated_job.status == "succeeded"
        assert updated_job.attempts == 1
        assert updated_job.error_code is None
        assert updated_job.error_message is None
        assert updated_job.finished_at is not None
        assert len(takeoffs) == 1

        takeoff = takeoffs[0]
        assert takeoff.project_id == uuid.UUID(project["id"])
        assert takeoff.source_file_id == uuid.UUID(uploaded["id"])
        assert takeoff.drawing_revision_id == base_revision.id
        assert takeoff.source_job_id == quantity_job.id
        assert takeoff.review_state == review_state
        assert takeoff.validation_status == validation_status
        assert takeoff.quantity_gate == quantity_gate
        assert takeoff.trusted_totals is trusted_totals

        items = await _get_quantity_items_for_takeoff(takeoff.id)
        assert len(items) == 5
        assert {item.item_kind for item in items} == {
            "aggregate",
            "contributor",
            "exclusion",
        }
        assert all(item.review_state == review_state for item in items)
        assert all(item.validation_status == validation_status for item in items)
        assert all(item.quantity_gate == quantity_gate for item in items)
        assert sum(item.item_kind == "aggregate" for item in items) == 2
        assert sum(item.item_kind == "contributor" for item in items) == 2
        assert sum(item.item_kind == "exclusion" for item in items) == 1
        contributor_quantity_types = {
            item.quantity_type for item in items if item.item_kind == "contributor"
        }
        aggregate_quantity_types = {
            item.quantity_type for item in items if item.item_kind == "aggregate"
        }
        assert len(contributor_quantity_types) == 2
        assert aggregate_quantity_types == contributor_quantity_types
        assert all(":" in quantity_type for quantity_type in contributor_quantity_types)
        assert all(item.source_entity_id is None for item in items if item.item_kind == "aggregate")
        assert all(
            item.source_entity_id is not None
            for item in items
            if item.item_kind in {"contributor", "exclusion"}
        )
        assert all(item.value is None for item in items if item.item_kind == "exclusion")

    async def test_process_quantity_takeoff_job_persists_manifest_backed_dxf_takeoff(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Approved DXF smoke fixture expectations should drive persisted takeoff values."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        fixture_filename = "dxf/simple-line.dxf"
        expected_line_count, expected_total_length = _manifest_dxf_line_expectations(
            fixture_filename
        )
        review_state = _manifest_expected_text(fixture_filename, "expected_review_state")
        validation_status = _manifest_expected_text(
            fixture_filename,
            "expected_validation_status",
        )
        monkeypatch.setattr(worker_module, "run_ingestion", real_run_ingestion)
        (
            project,
            uploaded,
            _,
            base_revision,
            quantity_job,
        ) = await _create_real_dxf_quantity_takeoff_job(async_client, fixture_filename)

        await worker_module.process_quantity_takeoff_job(quantity_job.id)

        updated_job = await _get_job(quantity_job.id)
        takeoffs = await _get_quantity_takeoffs_for_job(quantity_job.id)
        assert updated_job.status == "succeeded"
        assert updated_job.error_code is None
        assert len(takeoffs) == 1

        takeoff = takeoffs[0]
        assert takeoff.project_id == uuid.UUID(project["id"])
        assert takeoff.source_file_id == uuid.UUID(uploaded["id"])
        assert takeoff.drawing_revision_id == base_revision.id
        assert takeoff.review_state == review_state
        assert takeoff.validation_status == validation_status
        assert takeoff.quantity_gate == "allowed"
        assert takeoff.trusted_totals is True

        items = await _get_quantity_items_for_takeoff(takeoff.id)
        assert all(item.review_state == review_state for item in items)
        assert all(item.validation_status == validation_status for item in items)
        assert all(item.quantity_gate == "allowed" for item in items)
        assert all(item.item_kind != "exclusion" for item in items)

        aggregate_values = _quantity_item_values_by_type(items, item_kind="aggregate")
        assert aggregate_values["count:entity_count"] == pytest.approx(float(expected_line_count))
        assert aggregate_values["count:line_count"] == pytest.approx(float(expected_line_count))
        assert _quantity_length_total(aggregate_values) == pytest.approx(expected_total_length)

        length_contributors = [
            item
            for item in items
            if item.item_kind == "contributor" and item.quantity_type.startswith("length")
        ]
        assert len(length_contributors) == expected_line_count
        assert sum(item.value or 0.0 for item in length_contributors) == pytest.approx(
            expected_total_length
        )
        assert all(item.source_entity_id is not None for item in length_contributors)

    @pytest.mark.parametrize(
        ("fixture_filename", "quantity_gate"),
        [
            ("ifc/smoke-minimal.ifc", "review_gated"),
            ("pdf/vector-smoke.pdf", "review_gated"),
            ("pdf/raster-smoke.pdf", "blocked"),
            ("dwg/libredwg-wrapper-smoke.txt", "blocked"),
        ],
    )
    async def test_process_quantity_takeoff_job_manifest_gate_fixtures_publish_no_trusted_totals(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
        fixture_filename: str,
        quantity_gate: str,
    ) -> None:
        """Non-DXF fixture manifest entries should deterministically keep totals untrusted."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        review_state = _manifest_expected_text(fixture_filename, "expected_review_state")
        validation_status = _manifest_expected_text(
            fixture_filename,
            "expected_validation_status",
        )
        quantities = _assert_json_object(
            _assert_json_object(
                _manifest_fixture(fixture_filename).get("acceptance_checks"),
                f"{fixture_filename} acceptance_checks",
            ).get("quantities"),
            f"{fixture_filename} quantities",
        )
        assert all(
            _assert_json_object(check, f"{fixture_filename} quantity check").get("comparison")
            == "review_gated"
            for check in quantities.values()
        )

        async def _run_manifest_gated_ingestion(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            return _build_fake_quantity_ingest_payload(
                request,
                review_state=review_state,
                validation_status=validation_status,
                quantity_gate=quantity_gate,
                entities=[],
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run_manifest_gated_ingestion)
        _, _, _, _, quantity_job = await _create_ready_quantity_takeoff_job(async_client)

        await worker_module.process_quantity_takeoff_job(quantity_job.id)

        updated_job = await _get_job(quantity_job.id)
        takeoffs = await _get_quantity_takeoffs_for_job(quantity_job.id)
        assert updated_job.status == "failed"
        assert updated_job.error_code == ErrorCode.INPUT_INVALID.value
        assert takeoffs == []

        response = await async_client.get(f"/v1/jobs/{quantity_job.id}/events")
        assert response.status_code == 200
        event_payload = response.json()["items"][-1]["data_json"]
        assert event_payload["status"] == "failed"
        assert event_payload["details"]["quantity_gate"] == quantity_gate
        assert "trusted_totals" not in event_payload

    async def test_process_quantity_takeoff_job_manifest_dxf_rerun_semantics_are_stable(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Rerunning an approved DXF revision should preserve semantic takeoff payloads."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        fixture_filename = "dxf/simple-line.dxf"
        monkeypatch.setattr(worker_module, "run_ingestion", real_run_ingestion)
        (
            project,
            uploaded,
            ingest_job,
            base_revision,
            first_job,
        ) = await _create_real_dxf_quantity_takeoff_job(async_client, fixture_filename)
        second_job = await _create_quantity_takeoff_job(
            project_id=uuid.UUID(project["id"]),
            file_id=uuid.UUID(uploaded["id"]),
            base_revision_id=base_revision.id,
            parent_job_id=ingest_job.id,
            status="pending",
        )

        await worker_module.process_quantity_takeoff_job(first_job.id)
        await worker_module.process_quantity_takeoff_job(second_job.id)

        first_takeoffs = await _get_quantity_takeoffs_for_job(first_job.id)
        second_takeoffs = await _get_quantity_takeoffs_for_job(second_job.id)
        assert len(first_takeoffs) == 1
        assert len(second_takeoffs) == 1
        assert first_takeoffs[0].id != second_takeoffs[0].id
        assert first_takeoffs[0].source_job_id != second_takeoffs[0].source_job_id

        first_items = await _get_quantity_items_for_takeoff(first_takeoffs[0].id)
        second_items = await _get_quantity_items_for_takeoff(second_takeoffs[0].id)
        assert _quantity_takeoff_semantic_payload(
            first_takeoffs[0],
            first_items,
        ) == _quantity_takeoff_semantic_payload(second_takeoffs[0], second_items)

    async def test_process_quantity_takeoff_job_manifest_dxf_dedups_duplicate_contributors(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Identical source contributors should not inflate manifest-backed DXF totals."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        fixture_filename = "dxf/simple-line.dxf"
        expected_line_count, expected_total_length = _manifest_dxf_line_expectations(
            fixture_filename
        )
        review_state = _manifest_expected_text(fixture_filename, "expected_review_state")
        validation_status = _manifest_expected_text(
            fixture_filename,
            "expected_validation_status",
        )
        duplicate_hash = "b" * 64
        duplicate_entity = _build_fake_contract_entity(
            entity_id="manifest-dxf-duplicate-line",
            entity_type="line",
            layer_ref="0",
            source_id="entities.LINE:0",
            source_hash=duplicate_hash,
            geometry_json=_manifest_line_geometry(expected_total_length),
        )

        async def _run_duplicate_manifest_ingestion(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            return _build_fake_quantity_ingest_payload(
                request,
                review_state=review_state,
                validation_status=validation_status,
                quantity_gate="allowed",
                entities=[deepcopy(duplicate_entity), deepcopy(duplicate_entity)],
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run_duplicate_manifest_ingestion)
        _, _, _, _, quantity_job = await _create_ready_quantity_takeoff_job(async_client)

        await worker_module.process_quantity_takeoff_job(quantity_job.id)

        updated_job = await _get_job(quantity_job.id)
        takeoffs = await _get_quantity_takeoffs_for_job(quantity_job.id)
        assert updated_job.status == "succeeded"
        assert len(takeoffs) == 1

        items = await _get_quantity_items_for_takeoff(takeoffs[0].id)
        aggregate_values = _quantity_item_values_by_type(items, item_kind="aggregate")
        assert aggregate_values["count:entity_count"] == pytest.approx(float(expected_line_count))
        assert aggregate_values["count:line_count"] == pytest.approx(float(expected_line_count))
        assert _quantity_length_total(aggregate_values) == pytest.approx(expected_total_length)

        length_contributors = [
            item
            for item in items
            if item.item_kind == "contributor" and item.quantity_type.startswith("length")
        ]
        assert len(length_contributors) == 1
        assert length_contributors[0].excluded_source_entity_ids_json != []

    async def test_process_quantity_takeoff_job_honors_cancel_before_finalization(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Quantity finalization should re-check cancellation before publishing output."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        async def _run_quantity_ready_ingestion(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            return _build_fake_quantity_ingest_payload(
                request,
                review_state="approved",
                validation_status="valid",
                quantity_gate="allowed",
            )

        original_build_execution_input = worker_module._build_quantity_takeoff_execution_input

        async def _cancel_after_input_load(
            job_id: uuid.UUID,
            *,
            attempt_token: uuid.UUID,
        ) -> Any:
            execution = await original_build_execution_input(job_id, attempt_token=attempt_token)
            await _update_job(job_id, cancel_requested=True)
            return execution

        monkeypatch.setattr(worker_module, "run_ingestion", _run_quantity_ready_ingestion)
        _, _, _, _, quantity_job = await _create_ready_quantity_takeoff_job(async_client)
        monkeypatch.setattr(
            worker_module,
            "_build_quantity_takeoff_execution_input",
            _cancel_after_input_load,
        )

        await worker_module.process_quantity_takeoff_job(quantity_job.id)

        updated_job = await _get_job(quantity_job.id)
        takeoffs = await _get_quantity_takeoffs_for_job(quantity_job.id)
        assert updated_job.status == "cancelled"
        assert updated_job.attempts == 1
        assert takeoffs == []

        response = await async_client.get(f"/v1/jobs/{quantity_job.id}/events")
        assert response.status_code == 200
        assert response.json()["items"][-1]["data_json"] == {"status": "cancelled"}

    @pytest.mark.parametrize("failure_mode", ["gate", "conflict"])
    async def test_process_quantity_takeoff_job_prefers_cancelled_for_deterministic_failures(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
        failure_mode: str,
    ) -> None:
        """Cancel requests should win over deterministic gate and conflict failures."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        if failure_mode == "gate":

            async def _run_test_ingestion(
                request: IngestionRunRequest,
            ) -> IngestFinalizationPayload:
                return _build_fake_quantity_ingest_payload(
                    request,
                    review_state="rejected",
                    validation_status="invalid",
                    quantity_gate="blocked",
                )

        else:

            async def _run_test_ingestion(
                request: IngestionRunRequest,
            ) -> IngestFinalizationPayload:
                duplicate_hash = "b" * 64
                return _build_fake_quantity_ingest_payload(
                    request,
                    review_state="approved",
                    validation_status="valid",
                    quantity_gate="allowed",
                    entities=[
                        _build_fake_contract_entity(
                            entity_id="entity-conflict-cancel-001",
                            entity_type="line",
                            layer_ref="A-WALL",
                            source_id="entity-source-conflict-cancel-001",
                            source_hash=duplicate_hash,
                        ),
                        _build_fake_contract_entity(
                            entity_id="entity-conflict-cancel-002",
                            entity_type="line",
                            layer_ref="A-WALL",
                            source_id="entity-source-conflict-cancel-002",
                            source_hash=duplicate_hash,
                        ),
                    ],
                )

        original_build_execution_input = worker_module._build_quantity_takeoff_execution_input

        async def _cancel_after_input_load(
            job_id: uuid.UUID,
            *,
            attempt_token: uuid.UUID,
        ) -> Any:
            execution = await original_build_execution_input(job_id, attempt_token=attempt_token)
            await _update_job(job_id, cancel_requested=True)
            return execution

        monkeypatch.setattr(worker_module, "run_ingestion", _run_test_ingestion)
        _, _, _, _, quantity_job = await _create_ready_quantity_takeoff_job(async_client)
        monkeypatch.setattr(
            worker_module,
            "_build_quantity_takeoff_execution_input",
            _cancel_after_input_load,
        )

        await worker_module.process_quantity_takeoff_job(quantity_job.id)

        updated_job = await _get_job(quantity_job.id)
        takeoffs = await _get_quantity_takeoffs_for_job(quantity_job.id)
        assert updated_job.status == "cancelled"
        assert updated_job.attempts == 1
        assert updated_job.cancel_requested is True
        assert updated_job.error_code == ErrorCode.JOB_CANCELLED.value
        assert updated_job.error_message is None
        assert takeoffs == []

        response = await async_client.get(f"/v1/jobs/{quantity_job.id}/events")
        assert response.status_code == 200
        assert response.json()["items"][-1]["data_json"] == {"status": "cancelled"}

    async def test_process_quantity_takeoff_job_terminal_redelivery_does_not_duplicate_takeoff(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A duplicate delivery after success should not create another takeoff."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        async def _run_quantity_ready_ingestion(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            return _build_fake_quantity_ingest_payload(
                request,
                review_state="approved",
                validation_status="valid",
                quantity_gate="allowed",
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run_quantity_ready_ingestion)
        _, _, _, _, quantity_job = await _create_ready_quantity_takeoff_job(async_client)

        await worker_module.process_quantity_takeoff_job(quantity_job.id)
        response_before = await async_client.get(f"/v1/jobs/{quantity_job.id}/events")
        assert response_before.status_code == 200
        await worker_module.process_quantity_takeoff_job(quantity_job.id)

        updated_job = await _get_job(quantity_job.id)
        takeoffs = await _get_quantity_takeoffs_for_job(quantity_job.id)
        response_after = await async_client.get(f"/v1/jobs/{quantity_job.id}/events")
        assert response_after.status_code == 200
        assert updated_job.status == "succeeded"
        assert updated_job.attempts == 1
        assert len(takeoffs) == 1
        assert response_after.json()["items"] == response_before.json()["items"]

    @pytest.mark.parametrize("status", ["pending", "running"])
    async def test_recover_incomplete_ingest_jobs_requeues_quantity_takeoff_jobs(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
        status: str,
    ) -> None:
        """Startup recovery should route quantity jobs through the quantity publisher."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        quantity_recovered_job_ids: list[str] = []
        ingest_recovered_job_ids: list[str] = []

        def _fake_quantity_recovery_enqueue(job_id: uuid.UUID) -> None:
            quantity_recovered_job_ids.append(str(job_id))

        def _fake_ingest_recovery_enqueue(job_id: uuid.UUID) -> None:
            ingest_recovered_job_ids.append(str(job_id))

        monkeypatch.setattr(
            worker_module,
            "enqueue_quantity_takeoff_job",
            _fake_quantity_recovery_enqueue,
        )
        monkeypatch.setattr(worker_module, "enqueue_ingest_job", _fake_ingest_recovery_enqueue)

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
            status=status,
            attempts=1 if status == "running" else 0,
        )

        if status == "running":
            await _update_job(
                quantity_job.id,
                enqueue_status="published",
                enqueue_attempts=1,
            )
            session_maker = session_module.AsyncSessionLocal
            assert session_maker is not None
            async with session_maker() as session:
                persisted_job = await session.get(Job, quantity_job.id)
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
                quantity_job.id,
                enqueue_status="pending",
                enqueue_attempts=0,
            )

        original = await _get_job(quantity_job.id)

        requeued = await worker_module.recover_incomplete_ingest_jobs()

        assert requeued == [quantity_job.id]
        assert quantity_recovered_job_ids == [str(quantity_job.id)]
        assert ingest_recovered_job_ids == []
        recovered = await _get_job(quantity_job.id)
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

    async def test_quantity_takeoff_source_job_uniqueness_remains_enforced(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The DB should still reject duplicate takeoffs for one source job."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        async def _run_quantity_ready_ingestion(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            return _build_fake_quantity_ingest_payload(
                request,
                review_state="approved",
                validation_status="valid",
                quantity_gate="allowed",
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run_quantity_ready_ingestion)
        _, uploaded, _, base_revision, quantity_job = await _create_ready_quantity_takeoff_job(
            async_client
        )
        await worker_module.process_quantity_takeoff_job(quantity_job.id)

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            session.add(
                QuantityTakeoff(
                    id=uuid.uuid4(),
                    project_id=quantity_job.project_id,
                    source_file_id=uuid.UUID(uploaded["id"]),
                    drawing_revision_id=base_revision.id,
                    source_job_id=quantity_job.id,
                    source_job_type=JobType.QUANTITY_TAKEOFF.value,
                    review_state="approved",
                    validation_status="valid",
                    quantity_gate="allowed",
                    trusted_totals=True,
                )
            )
            with pytest.raises(IntegrityError):
                await session.commit()

        takeoffs = await _get_quantity_takeoffs_for_job(quantity_job.id)
        assert len(takeoffs) == 1

    async def test_process_quantity_takeoff_job_rolls_back_partial_rows_on_item_failure(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Quantity takeoff and items should commit atomically with no partial rows."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        async def _run_quantity_ready_ingestion(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            return _build_fake_quantity_ingest_payload(
                request,
                review_state="approved",
                validation_status="valid",
                quantity_gate="allowed",
            )

        def _invalid_quantity_items(**kwargs: Any) -> list[QuantityItem]:
            return [
                QuantityItem(
                    id=uuid.uuid4(),
                    quantity_takeoff_id=kwargs["quantity_takeoff_id"],
                    project_id=kwargs["project_id"],
                    drawing_revision_id=kwargs["drawing_revision_id"],
                    item_kind="contributor",
                    quantity_type="length",
                    value=-1.0,
                    unit="m",
                    review_state=kwargs["review_state"],
                    validation_status=kwargs["validation_status"],
                    quantity_gate=kwargs["quantity_gate"],
                    source_entity_id="missing-entity",
                    excluded_source_entity_ids_json=[],
                )
            ]

        monkeypatch.setattr(worker_module, "run_ingestion", _run_quantity_ready_ingestion)
        _, _, _, _, quantity_job = await _create_ready_quantity_takeoff_job(async_client)
        monkeypatch.setattr(worker_module, "_build_quantity_items", _invalid_quantity_items)

        with pytest.raises(IntegrityError):
            await worker_module.process_quantity_takeoff_job(quantity_job.id)

        updated_job = await _get_job(quantity_job.id)
        takeoffs = await _get_quantity_takeoffs_for_job(quantity_job.id)
        assert updated_job.status == "failed"
        assert updated_job.error_code == ErrorCode.INTERNAL_ERROR.value
        assert takeoffs == []

    @pytest.mark.parametrize(
        ("review_state", "validation_status", "quantity_gate"),
        [
            ("review_required", "needs_review", "review_gated"),
            ("rejected", "invalid", "blocked"),
        ],
    )
    async def test_process_quantity_takeoff_job_short_circuits_gate_before_materialization(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
        review_state: str,
        validation_status: str,
        quantity_gate: str,
    ) -> None:
        """Gate failures should not require normalized entity materialization lookups."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        async def _run_review_gated_ingestion(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            return _build_fake_quantity_ingest_payload(
                request,
                review_state=review_state,
                validation_status=validation_status,
                quantity_gate=quantity_gate,
            )

        async def _unexpected_manifest_lookup(*_: Any, **__: Any) -> None:
            raise AssertionError("Quantity gate failures must short-circuit before materialization")

        monkeypatch.setattr(worker_module, "run_ingestion", _run_review_gated_ingestion)
        _, _, _, _, quantity_job = await _create_ready_quantity_takeoff_job(async_client)
        monkeypatch.setattr(
            worker_module,
            "_get_revision_entity_manifest_for_revision",
            _unexpected_manifest_lookup,
        )

        await worker_module.process_quantity_takeoff_job(quantity_job.id)

        updated_job = await _get_job(quantity_job.id)
        takeoffs = await _get_quantity_takeoffs_for_job(quantity_job.id)
        assert updated_job.status == "failed"
        assert updated_job.error_code == ErrorCode.INPUT_INVALID.value
        assert (
            updated_job.error_message == worker_module._QUANTITY_TAKEOFF_GATE_BLOCKED_ERROR_MESSAGE
        )
        assert takeoffs == []

    @pytest.mark.parametrize(
        ("review_state", "validation_status", "quantity_gate"),
        [
            ("review_required", "needs_review", "review_gated"),
            ("rejected", "invalid", "blocked"),
        ],
    )
    async def test_process_quantity_takeoff_job_fails_review_only_gate_without_rows(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
        review_state: str,
        validation_status: str,
        quantity_gate: str,
    ) -> None:
        """Quantity worker should fail terminally when validation gate blocks execution."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        async def _run_review_gated_ingestion(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            return _build_fake_quantity_ingest_payload(
                request,
                review_state=review_state,
                validation_status=validation_status,
                quantity_gate=quantity_gate,
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run_review_gated_ingestion)

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

        await worker_module.process_quantity_takeoff_job(quantity_job.id)

        updated_job = await _get_job(quantity_job.id)
        takeoffs = await _get_quantity_takeoffs_for_job(quantity_job.id)
        assert updated_job.status == "failed"
        assert updated_job.attempts == 1
        assert updated_job.error_code == ErrorCode.INPUT_INVALID.value
        assert (
            updated_job.error_message == worker_module._QUANTITY_TAKEOFF_GATE_BLOCKED_ERROR_MESSAGE
        )
        assert takeoffs == []

        response = await async_client.get(f"/v1/jobs/{quantity_job.id}/events")
        assert response.status_code == 200
        event_payload = response.json()["items"][-1]["data_json"]
        assert event_payload["status"] == "failed"
        assert event_payload["error_code"] == ErrorCode.INPUT_INVALID.value
        assert (
            event_payload["error_message"]
            == worker_module._QUANTITY_TAKEOFF_GATE_BLOCKED_ERROR_MESSAGE
        )
        assert event_payload["details"]["quantity_gate"] == quantity_gate

    async def test_process_quantity_takeoff_job_fails_conflicts_without_rows(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Quantity worker should fail allowed jobs when dedup conflicts remain."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        async def _run_conflicting_ingestion(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            duplicate_hash = "a" * 64
            return _build_fake_quantity_ingest_payload(
                request,
                review_state="approved",
                validation_status="valid",
                quantity_gate="allowed",
                entities=[
                    _build_fake_contract_entity(
                        entity_id="entity-conflict-001",
                        entity_type="line",
                        layer_ref="A-WALL",
                        source_id="entity-source-conflict-001",
                        source_hash=duplicate_hash,
                        properties_json={"label": "first"},
                    ),
                    _build_fake_contract_entity(
                        entity_id="entity-conflict-002",
                        entity_type="line",
                        layer_ref="A-WALL",
                        source_id="entity-source-conflict-002",
                        source_hash=duplicate_hash,
                        properties_json={"label": "second"},
                    ),
                ],
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run_conflicting_ingestion)

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

        await worker_module.process_quantity_takeoff_job(quantity_job.id)

        updated_job = await _get_job(quantity_job.id)
        takeoffs = await _get_quantity_takeoffs_for_job(quantity_job.id)
        assert updated_job.status == "failed"
        assert updated_job.attempts == 1
        assert updated_job.error_code == ErrorCode.INPUT_INVALID.value
        assert updated_job.error_message == worker_module._QUANTITY_TAKEOFF_CONFLICT_ERROR_MESSAGE
        assert takeoffs == []

        response = await async_client.get(f"/v1/jobs/{quantity_job.id}/events")
        assert response.status_code == 200
        event_payload = response.json()["items"][-1]["data_json"]
        assert event_payload["status"] == "failed"
        assert event_payload["error_code"] == ErrorCode.INPUT_INVALID.value
        assert (
            event_payload["error_message"] == worker_module._QUANTITY_TAKEOFF_CONFLICT_ERROR_MESSAGE
        )
        assert event_payload["details"]["conflict_count"] == 2
        conflict_summaries = event_payload["details"]["conflicts"]
        assert len(conflict_summaries) == 2
        assert all(
            set(summary) == {"dedup_key", "entity_ids", "reason", "details"}
            for summary in conflict_summaries
        )
        assert any(summary["dedup_key"] is not None for summary in conflict_summaries)
        assert any(
            {"entity-conflict-001", "entity-conflict-002"}.issubset(set(summary["entity_ids"]))
            for summary in conflict_summaries
        )
        assert all(summary["reason"] is not None for summary in conflict_summaries)
        assert all(summary["details"] is not None for summary in conflict_summaries)

    async def test_process_quantity_takeoff_job_fails_missing_validation_report(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Quantity worker should surface missing validation reports as NOT_FOUND."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        async def _run_quantity_ready_ingestion(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            return _build_fake_quantity_ingest_payload(
                request,
                review_state="approved",
                validation_status="valid",
                quantity_gate="allowed",
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run_quantity_ready_ingestion)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        ingest_job = await _get_job_for_file(str(uploaded["id"]))
        await worker_module.process_ingest_job(ingest_job.id)
        base_revision = await _get_latest_revision_for_file(uuid.UUID(uploaded["id"]))
        assert base_revision is not None

        async def _missing_validation_report(*_: Any, **__: Any) -> None:
            return None

        monkeypatch.setattr(
            worker_module,
            "_get_validation_report_for_revision",
            _missing_validation_report,
        )

        quantity_job = await _create_quantity_takeoff_job(
            project_id=uuid.UUID(project["id"]),
            file_id=uuid.UUID(uploaded["id"]),
            base_revision_id=base_revision.id,
            parent_job_id=ingest_job.id,
            status="pending",
        )

        await worker_module.process_quantity_takeoff_job(quantity_job.id)

        updated_job = await _get_job(quantity_job.id)
        takeoffs = await _get_quantity_takeoffs_for_job(quantity_job.id)
        assert updated_job.status == "failed"
        assert updated_job.attempts == 1
        assert updated_job.error_code == ErrorCode.NOT_FOUND.value
        assert (
            updated_job.error_message
            == worker_module._QUANTITY_TAKEOFF_VALIDATION_REPORT_MISSING_ERROR_MESSAGE
        )
        assert takeoffs == []

        response = await async_client.get(f"/v1/jobs/{quantity_job.id}/events")
        assert response.status_code == 200
        event_payload = response.json()["items"][-1]["data_json"]
        assert event_payload["status"] == "failed"
        assert event_payload["error_code"] == ErrorCode.NOT_FOUND.value
        assert (
            event_payload["error_message"]
            == worker_module._QUANTITY_TAKEOFF_VALIDATION_REPORT_MISSING_ERROR_MESSAGE
        )
        assert event_payload["details"]["drawing_revision_id"] == str(base_revision.id)

    async def test_process_quantity_takeoff_job_fails_missing_materialization_without_rows(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Quantity worker should fail safely when revision materialization is missing."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        async def _run_quantity_ready_ingestion(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            return _build_fake_quantity_ingest_payload(
                request,
                review_state="approved",
                validation_status="valid",
                quantity_gate="allowed",
            )

        async def _missing_manifest(*_: Any, **__: Any) -> None:
            return None

        monkeypatch.setattr(worker_module, "run_ingestion", _run_quantity_ready_ingestion)
        _, _, _, base_revision, quantity_job = await _create_ready_quantity_takeoff_job(
            async_client
        )
        monkeypatch.setattr(
            worker_module,
            "_get_revision_entity_manifest_for_revision",
            _missing_manifest,
        )

        await worker_module.process_quantity_takeoff_job(quantity_job.id)

        updated_job = await _get_job(quantity_job.id)
        takeoffs = await _get_quantity_takeoffs_for_job(quantity_job.id)
        assert updated_job.status == "failed"
        assert updated_job.error_code == ErrorCode.NORMALIZED_ENTITIES_NOT_MATERIALIZED.value
        assert (
            updated_job.error_message
            == worker_module._QUANTITY_TAKEOFF_MATERIALIZATION_MISSING_ERROR_MESSAGE
        )
        assert takeoffs == []

        response = await async_client.get(f"/v1/jobs/{quantity_job.id}/events")
        assert response.status_code == 200
        event_payload = response.json()["items"][-1]["data_json"]
        assert event_payload["status"] == "failed"
        assert event_payload["error_code"] == ErrorCode.NORMALIZED_ENTITIES_NOT_MATERIALIZED.value
        assert event_payload["details"]["drawing_revision_id"] == str(base_revision.id)

    async def test_process_quantity_takeoff_job_fails_materialization_mismatch_without_rows(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Quantity worker should fail when manifest and entity rows disagree."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        async def _run_quantity_ready_ingestion(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            return _build_fake_quantity_ingest_payload(
                request,
                review_state="approved",
                validation_status="valid",
                quantity_gate="allowed",
            )

        async def _no_entities(*_: Any, **__: Any) -> list[Any]:
            return []

        monkeypatch.setattr(worker_module, "run_ingestion", _run_quantity_ready_ingestion)
        _, _, _, base_revision, quantity_job = await _create_ready_quantity_takeoff_job(
            async_client
        )
        monkeypatch.setattr(worker_module, "_get_revision_entities_for_revision", _no_entities)

        await worker_module.process_quantity_takeoff_job(quantity_job.id)

        updated_job = await _get_job(quantity_job.id)
        takeoffs = await _get_quantity_takeoffs_for_job(quantity_job.id)
        assert updated_job.status == "failed"
        assert updated_job.error_code == ErrorCode.NORMALIZED_ENTITIES_NOT_MATERIALIZED.value
        assert takeoffs == []

        response = await async_client.get(f"/v1/jobs/{quantity_job.id}/events")
        assert response.status_code == 200
        event_payload = response.json()["items"][-1]["data_json"]
        assert event_payload["error_code"] == ErrorCode.NORMALIZED_ENTITIES_NOT_MATERIALIZED.value
        assert event_payload["details"] == {
            "drawing_revision_id": str(base_revision.id),
            "expected_entities": 1,
            "loaded_entities": 0,
        }

    async def test_process_quantity_takeoff_job_uses_late_bound_registered_runner_callables(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Quantity wrapper should late-bind registered execution/finalization callables."""
        _ = self

        job_id = uuid.uuid4()
        attempt_token = uuid.uuid4()
        calls: list[tuple[Any, ...]] = []

        monkeypatch.setattr(worker_module, "_ensure_worker_database_configured", lambda: None)

        async def _fake_begin_or_resume_registered_job(
            job_id_value: uuid.UUID,
            *,
            process_name: str,
        ) -> Any:
            calls.append(("begin", job_id_value, process_name))
            return types.SimpleNamespace(token=attempt_token)

        async def _fake_execute_quantity_takeoff_job_attempt(
            job_id_value: uuid.UUID,
            *,
            attempt_token: uuid.UUID,
        ) -> Any:
            calls.append(("execute", job_id_value, attempt_token))
            return worker_module._RegisteredJobAttemptResult(
                finalize_kwargs={"execution": "execution", "result": "result"}
            )

        async def _fake_finalize_quantity_takeoff_job(
            job_id_value: uuid.UUID,
            *,
            attempt_token: uuid.UUID,
            execution: str,
            result: str,
        ) -> bool:
            calls.append(("finalize", job_id_value, attempt_token, execution, result))
            return True

        monkeypatch.setattr(
            worker_module,
            "_begin_or_resume_registered_job",
            _fake_begin_or_resume_registered_job,
        )
        monkeypatch.setattr(
            worker_module,
            "_execute_quantity_takeoff_job_attempt",
            _fake_execute_quantity_takeoff_job_attempt,
        )
        monkeypatch.setattr(
            worker_module,
            "_finalize_quantity_takeoff_job",
            _fake_finalize_quantity_takeoff_job,
        )

        await worker_module.process_quantity_takeoff_job(job_id)

        assert calls == [
            ("begin", job_id, "process_quantity_takeoff_job"),
            ("execute", job_id, attempt_token),
            ("finalize", job_id, attempt_token, "execution", "result"),
        ]
