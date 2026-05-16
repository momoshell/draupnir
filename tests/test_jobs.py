"""Integration tests for persisted job status and worker transitions."""

import asyncio
import hashlib
import json
import types
import uuid
from collections.abc import Callable
from contextlib import suppress
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

import app.api.v1.files as files_api
import app.api.v1.jobs as jobs_api
import app.db.session as session_module
import app.jobs.worker as worker_module
from app.core.errors import ErrorCode
from app.ingestion.contracts import (
    AdapterFailureKind,
    CancellationHandle,
    ProgressCallback,
    ProgressUpdate,
)
from app.ingestion.finalization import IngestFinalizationPayload, compute_adapter_result_checksum
from app.ingestion.runner import IngestionRunnerError, IngestionRunRequest
from app.ingestion.runner import (
    run_ingestion as real_run_ingestion,
)
from app.models.drawing_revision import DrawingRevision
from app.models.file import File
from app.models.generated_artifact import GeneratedArtifact
from app.models.job import Job, JobType
from app.models.job_event import JobEvent
from app.models.project import Project
from app.models.quantity_takeoff import QuantityItem, QuantityTakeoff
from tests.conftest import requires_database

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
_TEST_UPLOAD_BODY = b"%PDF-1.7\njob-test\n"
_FIXTURE_MANIFEST_PATH = Path(__file__).with_name("fixtures") / "manifest.yaml"
_UNSET = object()


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


class _AdapterModule(types.ModuleType):
    create_adapter: Callable[[], object]

    def __init__(self, name: str, create_adapter: Callable[[], object]) -> None:
        super().__init__(name)
        self.create_adapter = create_adapter


async def _create_project(async_client: httpx.AsyncClient) -> dict[str, Any]:
    """Create a project and return its payload."""
    response = await async_client.post(
        "/v1/projects",
        json={
            "name": "Jobs Test Project",
            "description": "A project for job tests",
        },
    )
    assert response.status_code == 201
    return cast(dict[str, Any], response.json())


async def _upload_file(
    async_client: httpx.AsyncClient,
    project_id: str,
    *,
    filename: str = "plan.pdf",
    content: bytes = _TEST_UPLOAD_BODY,
    media_type: str = "application/pdf",
) -> dict[str, Any]:
    """Upload a supported file and return its payload."""
    response = await async_client.post(
        f"/v1/projects/{project_id}/files",
        files={"file": (filename, content, media_type)},
    )
    assert response.status_code == 201
    return cast(dict[str, Any], response.json())


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


async def _get_job_for_file(file_id: str) -> Job:
    """Load the ingest job associated with a file id."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        result = await session.execute(select(Job).where(Job.file_id == uuid.UUID(file_id)))
        job = result.scalar_one_or_none()

    assert job is not None
    return job


async def _get_job(job_id: uuid.UUID) -> Job:
    """Load a job by id."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        job = await session.get(Job, job_id)

    assert job is not None
    return job


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
            await session.execute(
                select(QuantityTakeoff)
                .where(QuantityTakeoff.source_job_id == job_id)
                .order_by(QuantityTakeoff.created_at.asc(), QuantityTakeoff.id.asc())
            )
        ).scalars().all()

    return list(takeoffs)


async def _get_quantity_items_for_takeoff(quantity_takeoff_id: uuid.UUID) -> list[QuantityItem]:
    """Load persisted quantity items for a takeoff."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        items = (
            await session.execute(
                select(QuantityItem)
                .where(QuantityItem.quantity_takeoff_id == quantity_takeoff_id)
                .order_by(QuantityItem.created_at.asc(), QuantityItem.id.asc())
            )
        ).scalars().all()

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


async def _update_job(
    job_id: uuid.UUID,
    *,
    status: str | None = None,
    attempts: int | None = None,
    max_attempts: int | None = None,
    cancel_requested: bool | None = None,
    error_code: str | None = None,
    error_message: str | None = None,
    enqueue_status: str | None = None,
    enqueue_attempts: int | None = None,
    enqueue_owner_token: uuid.UUID | object = _UNSET,
    enqueue_lease_expires_at: datetime | object = _UNSET,
    enqueue_last_attempted_at: datetime | object = _UNSET,
    enqueue_published_at: datetime | object = _UNSET,
) -> Job:
    """Update and return a persisted job for test setup."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        job = await session.get(Job, job_id)
        assert job is not None

        if status is not None:
            job.status = status
        if attempts is not None:
            job.attempts = attempts
        if max_attempts is not None:
            job.max_attempts = max_attempts
        if cancel_requested is not None:
            job.cancel_requested = cancel_requested
        if error_code is not None:
            job.error_code = error_code

        if error_message is not None:
            job.error_message = error_message
        if enqueue_status is not None:
            job.enqueue_status = enqueue_status
        if enqueue_attempts is not None:
            job.enqueue_attempts = enqueue_attempts
        if enqueue_owner_token is not _UNSET:
            job.enqueue_owner_token = cast(uuid.UUID | None, enqueue_owner_token)
        if enqueue_lease_expires_at is not _UNSET:
            job.enqueue_lease_expires_at = cast(datetime | None, enqueue_lease_expires_at)
        if enqueue_last_attempted_at is not _UNSET:
            job.enqueue_last_attempted_at = cast(datetime | None, enqueue_last_attempted_at)
        if enqueue_published_at is not _UNSET:
            job.enqueue_published_at = cast(datetime | None, enqueue_published_at)

        await session.commit()

    return await _get_job(job_id)


async def _remove_source_file_bytes(file_id: uuid.UUID) -> str:
    """Delete stored source bytes without mutating append-only file metadata."""

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        source_file = await session.get(File, file_id)
        assert source_file is not None
        storage_uri = source_file.storage_uri

    assert storage_uri.startswith("file://")
    storage_path = Path(storage_uri.removeprefix("file://"))
    assert storage_path.exists()
    storage_path.unlink()
    return storage_uri


async def _mark_source_deleted(
    project_id: uuid.UUID,
    file_id: uuid.UUID,
    *,
    delete_project: bool,
    delete_file: bool,
) -> None:
    """Soft-delete the source project and/or file for worker visibility tests."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        if delete_project:
            project = await session.get(Project, project_id)
            assert project is not None
            project.deleted_at = datetime.now(UTC)
        if delete_file:
            source_file = await session.get(File, file_id)
            assert source_file is not None
            source_file.deleted_at = datetime.now(UTC)
        await session.commit()


async def _create_job_event(
    job_id: uuid.UUID,
    *,
    level: str,
    message: str,
    data_json: dict[str, Any] | None = None,
    created_at: datetime | None = None,
) -> JobEvent:
    """Persist and return a job event for test setup."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        event = JobEvent(
            job_id=job_id,
            level=level,
            message=message,
            data_json=data_json,
        )
        if created_at is not None:
            event.created_at = created_at
        session.add(event)
        await session.commit()
        await session.refresh(event)

    return event


async def _get_generated_artifacts_for_job(job_id: uuid.UUID) -> list[GeneratedArtifact]:
    """Load generated artifacts for a job id."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        artifacts = (
            await session.execute(
                select(GeneratedArtifact)
                .where(GeneratedArtifact.job_id == job_id)
                .order_by(GeneratedArtifact.created_at.asc(), GeneratedArtifact.id.asc())
            )
        ).scalars().all()

    return list(artifacts)


@pytest.fixture(autouse=True)
def fake_ingestion_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> list[IngestionRunRequest]:
    """Patch worker ingestion with a deterministic fake runner payload."""
    recorded_requests: list[IngestionRunRequest] = []

    async def _fake_run_ingestion(request: IngestionRunRequest) -> IngestFinalizationPayload:
        recorded_requests.append(request)
        return _build_fake_ingest_payload(request)

    monkeypatch.setattr(worker_module, "run_ingestion", _fake_run_ingestion)
    return recorded_requests


async def test_mark_recovery_enqueue_failed_logs_only_safe_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recovery enqueue failure logging should exclude exception text and traceback."""
    job_id = uuid.uuid4()
    logger_error_calls: list[tuple[str, dict[str, Any]]] = []
    marked_failed_job_ids: list[uuid.UUID] = []

    async def _fake_mark_job_failed_if_recovery_safe(
        failed_job_id: uuid.UUID,
        *,
        error_message: str,
        error_code: ErrorCode = ErrorCode.INTERNAL_ERROR,
    ) -> bool:
        marked_failed_job_ids.append(failed_job_id)
        assert error_message == "Failed to enqueue ingest job"
        assert error_code == ErrorCode.INTERNAL_ERROR
        return True

    def _capture_logger_error(event: str, **kwargs: Any) -> None:
        logger_error_calls.append((event, kwargs))

    monkeypatch.setattr(
        worker_module,
        "_mark_job_failed_if_recovery_safe",
        _fake_mark_job_failed_if_recovery_safe,
    )
    monkeypatch.setattr(worker_module.logger, "error", _capture_logger_error)

    marked_failed = await worker_module._mark_recovery_enqueue_failed(job_id)

    assert marked_failed is True
    assert marked_failed_job_ids == [job_id]
    assert logger_error_calls == [
        (
            "ingest_job_recovery_enqueue_failed",
            {
                "job_id": str(job_id),
                "error_code": ErrorCode.INTERNAL_ERROR.value,
                "recovery_action": "mark_failed",
            },
        )
    ]


@requires_database
class TestJobs:
    """Tests for job status retrieval and worker state transitions."""

    async def test_upload_file_enqueues_ingest_job(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Uploading a file should enqueue the persisted ingest job."""
        _ = self
        _ = cleanup_projects

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        assert enqueued_job_ids == [str(job.id)]
        assert job.status == "pending"
        assert job.attempts == 0
        assert job.enqueue_status == "published"

    async def test_upload_file_succeeds_when_publish_is_deferred(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Uploading should succeed once durable enqueue intent commits."""
        _ = self
        _ = cleanup_projects

        async def _skip_publish(
            job_id: uuid.UUID,
            *,
            publisher: Any | None = None,
            suppress_exceptions: bool = False,
            **kwargs: Any,
        ) -> bool:
            _ = (job_id, publisher, suppress_exceptions, kwargs)
            return False

        monkeypatch.setattr(files_api, "publish_job_enqueue_intent", _skip_publish)

        project = await _create_project(async_client)
        response = await async_client.post(
            f"/v1/projects/{project['id']}/files",
            files={"file": ("plan.pdf", _TEST_UPLOAD_BODY, "application/pdf")},
        )

        assert response.status_code == 201
        payload = response.json()
        job = await _get_job_for_file(payload["id"])

        assert job.status == "pending"
        assert job.attempts == 0
        assert job.error_code is None
        assert job.error_message is None
        assert job.started_at is None
        assert job.finished_at is None
        assert job.enqueue_status == "pending"
        assert job.enqueue_attempts == 0

    async def test_upload_file_replays_success_when_publish_is_deferred_after_commit(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Upload idempotency should snapshot success before best-effort publish."""
        _ = self
        _ = cleanup_projects

        async def _skip_publish(
            job_id: uuid.UUID,
            *,
            publisher: Any | None = None,
            suppress_exceptions: bool = False,
            **kwargs: Any,
        ) -> bool:
            _ = (job_id, publisher, suppress_exceptions, kwargs)
            return False

        monkeypatch.setattr(files_api, "publish_job_enqueue_intent", _skip_publish)

        project = await _create_project(async_client)
        headers = {"Idempotency-Key": "upload-outbox-replay"}

        first = await async_client.post(
            f"/v1/projects/{project['id']}/files",
            files={"file": ("plan.pdf", _TEST_UPLOAD_BODY, "application/pdf")},
            headers=headers,
        )
        second = await async_client.post(
            f"/v1/projects/{project['id']}/files",
            files={"file": ("plan.pdf", _TEST_UPLOAD_BODY, "application/pdf")},
            headers=headers,
        )

        assert first.status_code == 201
        assert second.status_code == 201
        assert second.json() == first.json()

        job = await _get_job_for_file(first.json()["id"])
        assert job.status == "pending"
        assert job.enqueue_status == "pending"

    async def test_upload_file_succeeds_when_enqueue_claim_raises_after_commit(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Upload should not fail if post-commit enqueue bookkeeping raises before claim."""
        _ = self
        _ = cleanup_projects

        async def _fail_claim(_: uuid.UUID) -> worker_module._EnqueueIntentLease | None:
            raise RuntimeError("transient enqueue claim failure")

        monkeypatch.setattr(worker_module, "_claim_job_enqueue_intent", _fail_claim)

        project = await _create_project(async_client)
        response = await async_client.post(
            f"/v1/projects/{project['id']}/files",
            files={"file": ("plan.pdf", _TEST_UPLOAD_BODY, "application/pdf")},
        )

        assert response.status_code == 201
        job = await _get_job_for_file(response.json()["id"])
        assert job.status == "pending"
        assert job.enqueue_status == "pending"

    async def test_process_ingest_job_marks_internal_error_code_on_failure(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Worker failures should persist INTERNAL_ERROR on the job row."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        async def _fail_run_ingestion(_: IngestionRunRequest) -> IngestFinalizationPayload:
            raise RuntimeError("adapter exploded")

        monkeypatch.setattr(worker_module, "run_ingestion", _fail_run_ingestion)

        with pytest.raises(RuntimeError, match="adapter exploded"):
            await worker_module.process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert updated_job.status == "failed"
        assert updated_job.error_code == ErrorCode.INTERNAL_ERROR.value
        assert updated_job.error_message == "Ingest job failed unexpectedly."
        assert "adapter exploded" not in updated_job.error_message
        assert updated_job.finished_at is not None

        response = await async_client.get(f"/v1/jobs/{job.id}/events")
        assert response.status_code == 200
        data = response.json()
        assert data["items"][-1]["data_json"] == {
            "status": "failed",
            "error_code": ErrorCode.INTERNAL_ERROR.value,
            "error_message": "Ingest job failed unexpectedly.",
        }
        assert "adapter exploded" not in str(data["items"][-1]["data_json"])

    async def test_process_ingest_job_marks_expected_runner_failure_with_sanitized_code(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Expected runner failures should persist their sanitized code and message."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        logger_error_calls: list[tuple[str, dict[str, Any]]] = []

        def _capture_logger_error(event: str, **kwargs: Any) -> None:
            logger_error_calls.append((event, kwargs))

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        async def _fail_run_ingestion(_: IngestionRunRequest) -> IngestFinalizationPayload:
            raise IngestionRunnerError(
                error_code=ErrorCode.ADAPTER_UNAVAILABLE,
                failure_kind=AdapterFailureKind.UNAVAILABLE,
                message="Adapter could not be loaded.",
                details={"stderr": "super-secret adapter detail"},
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _fail_run_ingestion)
        monkeypatch.setattr(worker_module.logger, "error", _capture_logger_error)

        with pytest.raises(IngestionRunnerError, match="Adapter could not be loaded"):
            await worker_module.process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert updated_job.status == "failed"
        assert updated_job.error_code == ErrorCode.ADAPTER_UNAVAILABLE.value
        assert updated_job.error_message == "Adapter could not be loaded."
        assert "super-secret adapter detail" not in updated_job.error_message
        assert updated_job.finished_at is not None
        assert logger_error_calls == [
            (
                "ingest_job_failed",
                {
                    "job_id": str(job.id),
                    "error_code": ErrorCode.ADAPTER_UNAVAILABLE.value,
                    "failure_kind": AdapterFailureKind.UNAVAILABLE.value,
                    "error_message": "Adapter could not be loaded.",
                },
            )
        ]

    async def test_process_ingest_job_persists_sanitized_real_runner_storage_failure(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Real runner storage failures should persist and log only sanitized fields."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        logger_error_calls: list[tuple[str, dict[str, Any]]] = []

        def _capture_logger_error(event: str, **kwargs: Any) -> None:
            logger_error_calls.append((event, kwargs))

        module = _AdapterModule("available_vector_adapter_module", lambda: object())

        def fake_import_module(module_name: str) -> types.ModuleType:
            if module_name == "app.ingestion.adapters.pymupdf":
                return module
            raise AssertionError(f"Unexpected module import: {module_name}")

        monkeypatch.setattr(worker_module, "run_ingestion", real_run_ingestion)
        monkeypatch.setattr("app.ingestion.loader.importlib.import_module", fake_import_module)
        monkeypatch.setattr(worker_module.logger, "error", _capture_logger_error)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        secret_storage_uri = await _remove_source_file_bytes(uuid.UUID(uploaded["id"]))

        with pytest.raises(
            IngestionRunnerError,
            match="Failed to read original source from storage",
        ):
            await worker_module.process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert updated_job.status == "failed"
        assert updated_job.error_code == ErrorCode.STORAGE_FAILED.value
        assert updated_job.error_message == "Failed to read original source from storage."
        assert secret_storage_uri not in updated_job.error_message

        response = await async_client.get(f"/v1/jobs/{job.id}/events")
        assert response.status_code == 200
        data = response.json()
        assert data["items"][-1]["data_json"] == {
            "status": "failed",
            "error_code": ErrorCode.STORAGE_FAILED.value,
            "error_message": "Failed to read original source from storage.",
        }
        assert secret_storage_uri not in str(data["items"][-1]["data_json"])
        assert secret_storage_uri not in str(logger_error_calls)
        assert logger_error_calls == [
            (
                "ingest_job_failed",
                {
                    "job_id": str(job.id),
                    "error_code": ErrorCode.STORAGE_FAILED.value,
                    "failure_kind": AdapterFailureKind.FAILED.value,
                    "error_message": "Failed to read original source from storage.",
                    "adapter_key": "pymupdf",
                    "input_family": "pdf_vector",
                    "reason": "not_found",
                },
            )
        ]

    async def test_process_ingest_job_persists_sanitized_real_runner_staging_failure(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Real runner staging failures should not leak temp paths or exception text."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        secret_temp_marker = "temp-path-leak"
        logger_error_calls: list[tuple[str, dict[str, Any]]] = []

        def _capture_logger_error(event: str, **kwargs: Any) -> None:
            logger_error_calls.append((event, kwargs))

        async def _fail_copy_to_path(*args: Any, **kwargs: Any) -> Any:
            raise OSError(f"{secret_temp_marker}: staging")

        module = _AdapterModule("available_stage_vector_adapter_module", lambda: object())

        def fake_import_module(module_name: str) -> types.ModuleType:
            if module_name == "app.ingestion.adapters.pymupdf":
                return module
            raise AssertionError(f"Unexpected module import: {module_name}")

        monkeypatch.setattr(worker_module, "run_ingestion", real_run_ingestion)
        monkeypatch.setattr("app.ingestion.loader.importlib.import_module", fake_import_module)
        monkeypatch.setattr(worker_module.logger, "error", _capture_logger_error)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        monkeypatch.setattr("app.storage.memory.MemoryStorage.copy_to_path", _fail_copy_to_path)
        monkeypatch.setattr(
            "app.storage.local.LocalFilesystemStorage.copy_to_path",
            _fail_copy_to_path,
        )

        with pytest.raises(IngestionRunnerError, match="Failed to stage original source"):
            await worker_module.process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert updated_job.status == "failed"
        assert updated_job.error_code == ErrorCode.STORAGE_FAILED.value
        assert updated_job.error_message == "Failed to stage original source."
        assert secret_temp_marker not in updated_job.error_message

        response = await async_client.get(f"/v1/jobs/{job.id}/events")
        assert response.status_code == 200
        data = response.json()
        assert data["items"][-1]["data_json"] == {
            "status": "failed",
            "error_code": ErrorCode.STORAGE_FAILED.value,
            "error_message": "Failed to stage original source.",
        }
        assert secret_temp_marker not in str(data["items"][-1]["data_json"])
        assert secret_temp_marker not in str(logger_error_calls)
        assert logger_error_calls == [
            (
                "ingest_job_failed",
                {
                    "job_id": str(job.id),
                    "error_code": ErrorCode.STORAGE_FAILED.value,
                    "failure_kind": AdapterFailureKind.FAILED.value,
                    "error_message": "Failed to stage original source.",
                    "adapter_key": "pymupdf",
                    "input_family": "pdf_vector",
                    "reason": "stage_failed",
                },
            )
        ]

    async def test_get_job_returns_persisted_state(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """GET should return the persisted job state for a known job."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        response = await async_client.get(f"/v1/jobs/{job.id}")
        assert response.status_code == 200

        data = response.json()
        assert data["id"] == str(job.id)
        assert data["project_id"] == project["id"]
        assert data["file_id"] == uploaded["id"]
        assert data["extraction_profile_id"] == str(job.extraction_profile_id)
        assert data["job_type"] == "ingest"
        assert data["status"] == "pending"
        assert data["attempts"] == 0
        assert data["max_attempts"] == 3
        assert data["cancel_requested"] is False
        assert data["error_code"] is None
        assert data["error_message"] is None
        assert data["started_at"] is None
        assert data["finished_at"] is None
        assert data["created_at"] is not None

    async def test_get_job_not_found(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """GET should return the standard Job 404 envelope for unknown ids."""
        _ = self
        _ = cleanup_projects

        missing_job_id = uuid.uuid4()
        response = await async_client.get(f"/v1/jobs/{missing_job_id}")
        assert response.status_code == 404
        assert response.json() == {
            "error": {
                "code": "NOT_FOUND",
                "message": f"Job with identifier '{missing_job_id}' not found",
                "details": None,
            }
        }

    async def test_list_job_events_returns_404_for_unknown_job(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """GET events should return the standard Job 404 envelope for unknown ids."""
        _ = self
        _ = cleanup_projects

        missing_job_id = uuid.uuid4()
        response = await async_client.get(f"/v1/jobs/{missing_job_id}/events")

        assert response.status_code == 404
        assert response.json() == {
            "error": {
                "code": "NOT_FOUND",
                "message": f"Job with identifier '{missing_job_id}' not found",
                "details": None,
            }
        }

    async def test_list_job_events_returns_chronological_order(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """GET events should return ascending created-at ordered results."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        base = datetime.now(UTC).replace(microsecond=0)
        await _create_job_event(
            job.id,
            level="info",
            message="third",
            created_at=base + timedelta(seconds=2),
        )
        await _create_job_event(
            job.id,
            level="info",
            message="first",
            created_at=base,
        )
        await _create_job_event(
            job.id,
            level="info",
            message="second",
            created_at=base + timedelta(seconds=1),
        )

        response = await async_client.get(f"/v1/jobs/{job.id}/events")
        assert response.status_code == 200

        data = response.json()
        assert [event["message"] for event in data["items"]] == [
            "first",
            "second",
            "third",
        ]
        assert data["next_cursor"] is None

    async def test_list_job_events_supports_cursor_pagination(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """GET events should support opaque cursor pagination."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        base = datetime.now(UTC).replace(microsecond=0)
        await _create_job_event(job.id, level="info", message="first", created_at=base)
        await _create_job_event(
            job.id,
            level="info",
            message="second",
            created_at=base + timedelta(seconds=1),
        )
        await _create_job_event(
            job.id,
            level="info",
            message="third",
            created_at=base + timedelta(seconds=2),
        )

        first_response = await async_client.get(f"/v1/jobs/{job.id}/events?limit=2")
        assert first_response.status_code == 200
        first_data = first_response.json()
        assert [event["message"] for event in first_data["items"]] == ["first", "second"]
        assert first_data["next_cursor"] is not None

        second_response = await async_client.get(
            f"/v1/jobs/{job.id}/events?limit=2&cursor={first_data['next_cursor']}"
        )
        assert second_response.status_code == 200
        second_data = second_response.json()
        assert [event["message"] for event in second_data["items"]] == ["third"]
        assert second_data["next_cursor"] is None

    async def test_list_job_events_preserves_same_transaction_emission_order_on_ties(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """GET events should preserve insertion order for same-timestamp events."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        tied_created_at = datetime.now(UTC).replace(microsecond=0)
        first_id = uuid.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
        second_id = uuid.UUID("00000000-0000-0000-0000-000000000001")

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            session.add(
                JobEvent(
                    id=first_id,
                    job_id=job.id,
                    level="info",
                    message="first emitted",
                    created_at=tied_created_at,
                )
            )
            session.add(
                JobEvent(
                    id=second_id,
                    job_id=job.id,
                    level="info",
                    message="second emitted",
                    created_at=tied_created_at,
                )
            )
            await session.commit()

        response = await async_client.get(f"/v1/jobs/{job.id}/events")
        assert response.status_code == 200

        data = response.json()
        assert [event["message"] for event in data["items"]] == [
            "first emitted",
            "second emitted",
        ]
        assert data["next_cursor"] is None

    async def test_list_job_events_invalid_cursor_returns_error_envelope(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """GET events should return standard envelope for invalid cursor values."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        response = await async_client.get(f"/v1/jobs/{job.id}/events?cursor=not-base64")

        assert response.status_code == 400
        assert response.json() == {
            "error": {
                "code": "INVALID_CURSOR",
                "message": "Invalid cursor format",
                "details": None,
            }
        }

    async def test_list_job_events_includes_worker_lifecycle_events(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """GET events should expose worker-emitted lifecycle events."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        await worker_module.process_ingest_job(job.id)

        response = await async_client.get(f"/v1/jobs/{job.id}/events")
        assert response.status_code == 200
        data = response.json()

        assert [event["message"] for event in data["items"]] == [
            "Job started",
            "Job succeeded",
        ]
        assert [event["data_json"]["status"] for event in data["items"]] == [
            "running",
            "succeeded",
        ]
        assert data["next_cursor"] is None

    async def test_process_ingest_job_flushes_progress_events_before_success(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Worker should persist queued progress updates before terminal success."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        async def _run_with_progress(
            request: IngestionRunRequest,
            *,
            on_progress: ProgressCallback | None = None,
            **_: Any,
        ) -> IngestFinalizationPayload:
            assert on_progress is not None
            on_progress(
                ProgressUpdate(
                    stage="source",
                    message="Staged original",
                    completed=1,
                    total=3,
                    percent=1 / 3,
                )
            )
            on_progress(
                ProgressUpdate(
                    stage="extract",
                    message="Extracted entities",
                    completed=2,
                    total=3,
                    percent=2 / 3,
                )
            )
            return _build_fake_ingest_payload(request)

        monkeypatch.setattr(worker_module, "run_ingestion", _run_with_progress)

        await worker_module.process_ingest_job(job.id)

        response = await async_client.get(f"/v1/jobs/{job.id}/events")
        assert response.status_code == 200
        data = response.json()
        assert [event["message"] for event in data["items"]] == [
            "Job started",
            "Staged original",
            "Extracted entities",
            "Job succeeded",
        ]
        assert [event["data_json"]["status"] for event in data["items"]] == [
            "running",
            "running",
            "running",
            "succeeded",
        ]
        assert data["items"][1]["data_json"] == {
            "status": "running",
            "event": "progress",
            "stage": "source",
            "detail": "Staged original",
            "completed": 1,
            "total": 3,
            "percent": 1 / 3,
        }
        assert data["items"][2]["data_json"] == {
            "status": "running",
            "event": "progress",
            "stage": "extract",
            "detail": "Extracted entities",
            "completed": 2,
            "total": 3,
            "percent": 2 / 3,
        }
        assert data["next_cursor"] is None

    async def test_process_ingest_job_transitions_to_succeeded(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        fake_ingestion_runner: list[IngestionRunRequest],
    ) -> None:
        """Worker processing should persist running and succeeded state transitions."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        await worker_module.process_ingest_job(job.id)

        updated_job = await _get_job_for_file(str(uploaded["id"]))
        assert len(fake_ingestion_runner) == 1
        request = fake_ingestion_runner[0]
        assert updated_job.status == "succeeded"
        assert updated_job.attempts == 1
        assert updated_job.started_at is not None
        assert updated_job.finished_at is not None
        assert updated_job.finished_at >= updated_job.started_at
        assert updated_job.error_code is None
        assert updated_job.error_message is None
        assert request.job_id == job.id
        assert request.file_id == job.file_id
        assert request.checksum_sha256 == hashlib.sha256(_TEST_UPLOAD_BODY).hexdigest()
        assert request.detected_format == "pdf"
        assert request.media_type == "application/pdf"
        assert request.original_name == "plan.pdf"
        assert request.extraction_profile_id == job.extraction_profile_id
        assert request.initial_job_id == job.id

    async def test_process_ingest_job_flushes_progress_events_before_failure(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Worker should persist queued progress updates before terminal failure."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        async def _run_with_progress_then_fail(
            _: IngestionRunRequest,
            *,
            on_progress: ProgressCallback | None = None,
            **__: Any,
        ) -> IngestFinalizationPayload:
            assert on_progress is not None
            on_progress(
                ProgressUpdate(
                    stage="source",
                    message="Staged original",
                    completed=1,
                    total=3,
                    percent=1 / 3,
                )
            )
            on_progress(
                ProgressUpdate(
                    stage="extract",
                    message="Extracted entities",
                    completed=2,
                    total=3,
                    percent=2 / 3,
                )
            )
            raise IngestionRunnerError(
                error_code=ErrorCode.ADAPTER_FAILED,
                failure_kind=AdapterFailureKind.FAILED,
                message="Adapter execution failed.",
                details={"stderr": "super-secret adapter detail"},
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run_with_progress_then_fail)

        with pytest.raises(IngestionRunnerError, match="Adapter execution failed"):
            await worker_module.process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert updated_job.status == "failed"
        assert updated_job.error_code == ErrorCode.ADAPTER_FAILED.value
        assert updated_job.error_message == "Adapter execution failed."
        assert "super-secret adapter detail" not in updated_job.error_message

        response = await async_client.get(f"/v1/jobs/{job.id}/events")
        assert response.status_code == 200
        data = response.json()
        assert [event["message"] for event in data["items"]] == [
            "Job started",
            "Staged original",
            "Extracted entities",
            "Job failed",
        ]
        assert [event["data_json"]["status"] for event in data["items"]] == [
            "running",
            "running",
            "running",
            "failed",
        ]
        assert data["items"][1]["data_json"] == {
            "status": "running",
            "event": "progress",
            "stage": "source",
            "detail": "Staged original",
            "completed": 1,
            "total": 3,
            "percent": 1 / 3,
        }
        assert data["items"][2]["data_json"] == {
            "status": "running",
            "event": "progress",
            "stage": "extract",
            "detail": "Extracted entities",
            "completed": 2,
            "total": 3,
            "percent": 2 / 3,
        }
        assert data["items"][3]["data_json"] == {
            "status": "failed",
            "error_code": ErrorCode.ADAPTER_FAILED.value,
            "error_message": "Adapter execution failed.",
        }
        assert "super-secret adapter detail" not in str(data["items"][3]["data_json"])
        assert data["next_cursor"] is None

    async def test_process_ingest_job_marks_runner_cancellation_as_cancelled(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Runner cancellation should flush progress and persist a cancelled terminal state."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        async def _cancel_run(
            _: IngestionRunRequest,
            *,
            cancellation: CancellationHandle | None = None,
            on_progress: ProgressCallback | None = None,
            **__: Any,
        ) -> IngestFinalizationPayload:
            assert cancellation is not None
            assert on_progress is not None
            on_progress(ProgressUpdate(stage="source", message="Staged original"))
            await _update_job(job.id, cancel_requested=True)
            cancellation_deadline = asyncio.get_running_loop().time() + 1
            while not cancellation.is_cancelled():
                if asyncio.get_running_loop().time() >= cancellation_deadline:
                    raise AssertionError("Expected cancellation flag within 1 second")
                await asyncio.sleep(0.01)

            await asyncio.sleep(1)
            raise AssertionError("Worker cancellation should interrupt the runner task")

        monkeypatch.setattr(worker_module, "run_ingestion", _cancel_run)

        with pytest.raises(asyncio.CancelledError):
            await worker_module.process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert updated_job.status == "cancelled"
        assert updated_job.cancel_requested is True
        assert updated_job.error_code == ErrorCode.JOB_CANCELLED.value
        assert updated_job.error_message is None
        assert updated_job.finished_at is not None

        response = await async_client.get(f"/v1/jobs/{job.id}/events")
        assert response.status_code == 200
        data = response.json()
        assert [event["message"] for event in data["items"]] == [
            "Job started",
            "Staged original",
            "Job cancelled",
        ]
        assert [event["data_json"]["status"] for event in data["items"]] == [
            "running",
            "running",
            "cancelled",
        ]

    async def test_process_ingest_job_cancels_mid_run_when_project_is_deleted(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Project deletion during execution should cancel the active runner."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        runner_started = asyncio.Event()

        async def _wait_for_project_delete_cancel(
            _: IngestionRunRequest,
            *,
            cancellation: CancellationHandle | None = None,
            **__: Any,
        ) -> IngestFinalizationPayload:
            assert cancellation is not None
            runner_started.set()
            cancellation_deadline = asyncio.get_running_loop().time() + 1
            while not cancellation.is_cancelled():
                if asyncio.get_running_loop().time() >= cancellation_deadline:
                    raise AssertionError("Expected project delete to cancel runner within 1 second")
                await asyncio.sleep(0.01)

            await asyncio.sleep(1)
            raise AssertionError("Worker cancellation should interrupt the runner task")

        monkeypatch.setattr(worker_module, "run_ingestion", _wait_for_project_delete_cancel)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        process_task = asyncio.create_task(worker_module.process_ingest_job(job.id))
        await asyncio.wait_for(runner_started.wait(), timeout=2)

        delete_response = await async_client.delete(f"/v1/projects/{project['id']}")

        assert delete_response.status_code == 204
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(process_task, timeout=2)

        updated_job = await _get_job(job.id)
        assert updated_job.status == "cancelled"
        assert updated_job.cancel_requested is True
        assert updated_job.error_code == ErrorCode.JOB_CANCELLED.value
        assert updated_job.finished_at is not None

    async def test_process_ingest_job_skips_fresh_redelivered_running_job(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Fresh redelivery should not execute or mutate an already-running attempt."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        runner_calls: list[IngestionRunRequest] = []

        async def _unexpected_run_ingestion(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            runner_calls.append(request)
            return _build_fake_ingest_payload(request)

        monkeypatch.setattr(worker_module, "run_ingestion", _unexpected_run_ingestion)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        attempt_token = uuid.uuid4()
        lease_expires_at = datetime.now(UTC) + worker_module._RUNNING_JOB_STALE_AFTER

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            persisted_job = await session.get(Job, job.id)
            assert persisted_job is not None
            persisted_job.status = "running"
            persisted_job.attempts = 1
            persisted_job.started_at = datetime.now(UTC)
            persisted_job.attempt_token = attempt_token
            persisted_job.attempt_lease_expires_at = lease_expires_at
            await session.commit()

        await worker_module.process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert runner_calls == []
        assert updated_job.status == "running"
        assert updated_job.attempts == 1
        assert updated_job.started_at is not None
        assert updated_job.finished_at is None
        assert updated_job.attempt_token == attempt_token
        assert updated_job.attempt_lease_expires_at == lease_expires_at

    async def test_begin_or_resume_ingest_job_reclaims_stale_running_attempt_with_new_token(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Stale running attempts should be reclaimed with a fresh ownership token."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        old_attempt_token = uuid.uuid4()

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            persisted_job = await session.get(Job, job.id)
            assert persisted_job is not None
            persisted_job.status = "running"
            persisted_job.attempts = 1
            persisted_job.started_at = (
                datetime.now(UTC)
                - worker_module._RUNNING_JOB_STALE_AFTER
                - timedelta(seconds=1)
            )
            persisted_job.attempt_token = old_attempt_token
            persisted_job.attempt_lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            await session.commit()

        lease = await worker_module._begin_or_resume_ingest_job(job.id)

        assert lease is not None
        assert lease.token != old_attempt_token
        reclaimed_job = await _get_job(job.id)
        assert reclaimed_job.status == "running"
        assert reclaimed_job.attempts == 2
        assert reclaimed_job.attempt_token == lease.token
        assert reclaimed_job.attempt_lease_expires_at == lease.lease_expires_at
        assert reclaimed_job.attempt_lease_expires_at is not None
        assert reclaimed_job.attempt_lease_expires_at > datetime.now(UTC)

    async def test_stale_attempt_writes_noop_after_reclaim(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Stale attempt events and terminal writes should no-op after reclaim."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        old_attempt_token = uuid.uuid4()

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            persisted_job = await session.get(Job, job.id)
            assert persisted_job is not None
            persisted_job.status = "running"
            persisted_job.attempts = 1
            persisted_job.started_at = (
                datetime.now(UTC)
                - worker_module._RUNNING_JOB_STALE_AFTER
                - timedelta(seconds=1)
            )
            persisted_job.attempt_token = old_attempt_token
            persisted_job.attempt_lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            await session.commit()

        new_lease = await worker_module._begin_or_resume_ingest_job(job.id)

        assert new_lease is not None
        event_written = await worker_module.emit_job_event(
            job.id,
            level="info",
            message="Stale progress",
            data_json={"status": "running", "event": "progress", "stage": "stale"},
            attempt_token=old_attempt_token,
        )
        failed = await worker_module._mark_job_failed(
            job.id,
            error_message="stale failure",
            attempt_token=old_attempt_token,
        )
        cancelled = await worker_module._mark_job_cancelled(
            job.id,
            attempt_token=old_attempt_token,
        )
        async with session_maker() as session:
            persisted_job = await session.get(Job, job.id)
            assert persisted_job is not None
            inactive_source_cancelled = await worker_module._cancel_job_for_inactive_source(
                session,
                persisted_job,
                reason="source_deleted",
                attempt_token=old_attempt_token,
            )

        finalize_request = IngestionRunRequest(
            job_id=job.id,
            file_id=job.file_id,
            checksum_sha256=hashlib.sha256(_TEST_UPLOAD_BODY).hexdigest(),
            detected_format="pdf",
            media_type="application/pdf",
            original_name="plan.pdf",
            extraction_profile_id=job.extraction_profile_id,
            initial_job_id=job.id,
        )
        finalized = await worker_module._finalize_ingest_job(
            job.id,
            attempt_token=old_attempt_token,
            payload=_build_fake_ingest_payload(finalize_request),
        )

        assert event_written is False
        assert failed is False
        assert cancelled is False
        assert inactive_source_cancelled is False
        assert finalized is False

        updated_job = await _get_job(job.id)
        assert updated_job.status == "running"
        assert updated_job.attempts == 2
        assert updated_job.attempt_token == new_lease.token
        assert updated_job.finished_at is None

        response = await async_client.get(f"/v1/jobs/{job.id}/events")
        assert response.status_code == 200
        data = response.json()
        assert [event["message"] for event in data["items"]] == ["Job started"]

    async def test_finalize_ingest_job_clears_owned_attempt_lease_on_success(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Owned terminal success should clear persisted attempt lease state."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        lease = await worker_module._begin_or_resume_ingest_job(job.id)

        assert lease is not None
        finalize_request = IngestionRunRequest(
            job_id=job.id,
            file_id=job.file_id,
            checksum_sha256=hashlib.sha256(_TEST_UPLOAD_BODY).hexdigest(),
            detected_format="pdf",
            media_type="application/pdf",
            original_name="plan.pdf",
            extraction_profile_id=job.extraction_profile_id,
            initial_job_id=job.id,
        )

        finalized = await worker_module._finalize_ingest_job(
            job.id,
            attempt_token=lease.token,
            payload=_build_fake_ingest_payload(finalize_request),
        )

        assert finalized is True
        updated_job = await _get_job(job.id)
        assert updated_job.status == "succeeded"
        assert updated_job.attempt_token is None
        assert updated_job.attempt_lease_expires_at is None

    async def test_recover_incomplete_ingest_jobs_requeues_stranded_pending_jobs(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Worker startup recovery should requeue pending ingest jobs."""
        _ = self
        _ = cleanup_projects

        recovered_job_ids: list[str] = []

        def _fake_recovery_enqueue(job_id: uuid.UUID) -> None:
            recovered_job_ids.append(str(job_id))

        async def _skip_publish(
            job_id: uuid.UUID,
            *,
            publisher: Any | None = None,
            suppress_exceptions: bool = False,
            **kwargs: Any,
        ) -> bool:
            _ = (job_id, publisher, suppress_exceptions, kwargs)
            return False

        monkeypatch.setattr(files_api, "publish_job_enqueue_intent", _skip_publish)
        monkeypatch.setattr(worker_module, "enqueue_ingest_job", _fake_recovery_enqueue)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        enqueued_job_ids.clear()

        requeued = await worker_module.recover_incomplete_ingest_jobs()

        assert recovered_job_ids == [str(job.id)]
        assert requeued == [job.id]
        updated_job = await _get_job(job.id)
        assert updated_job.status == "pending"
        assert updated_job.attempts == 0
        assert updated_job.enqueue_status == "published"
        assert updated_job.enqueue_attempts == 1

    async def test_recover_incomplete_ingest_jobs_does_not_duplicate_requeue_after_publish(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Recovery should not requeue the same pending job twice once publish state is durable."""
        _ = self
        _ = cleanup_projects

        recovered_job_ids: list[str] = []

        def _fake_recovery_enqueue(job_id: uuid.UUID) -> None:
            recovered_job_ids.append(str(job_id))

        async def _skip_publish(
            job_id: uuid.UUID,
            *,
            publisher: Any | None = None,
            suppress_exceptions: bool = False,
            **kwargs: Any,
        ) -> bool:
            _ = (job_id, publisher, suppress_exceptions, kwargs)
            return False

        monkeypatch.setattr(files_api, "publish_job_enqueue_intent", _skip_publish)
        monkeypatch.setattr(worker_module, "enqueue_ingest_job", _fake_recovery_enqueue)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        enqueued_job_ids.clear()

        first_requeued = await worker_module.recover_incomplete_ingest_jobs()
        second_requeued = await worker_module.recover_incomplete_ingest_jobs()

        assert recovered_job_ids == [str(job.id)]
        assert first_requeued == [job.id]
        assert second_requeued == []

        updated_job = await _get_job(job.id)
        assert updated_job.status == "pending"
        assert updated_job.enqueue_status == "published"
        assert updated_job.enqueue_attempts == 1

    async def test_recover_incomplete_ingest_jobs_requeues_stranded_pending_reprocess_jobs(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Worker startup recovery should requeue pending reprocess jobs."""
        _ = self
        _ = cleanup_projects

        recovered_job_ids: list[str] = []

        def _fake_recovery_enqueue(job_id: uuid.UUID) -> None:
            recovered_job_ids.append(str(job_id))

        monkeypatch.setattr(worker_module, "enqueue_ingest_job", _fake_recovery_enqueue)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        initial_job = await _get_job_for_file(str(uploaded["id"]))
        await worker_module.process_ingest_job(initial_job.id)

        async def _skip_publish(
            job_id: uuid.UUID,
            *,
            publisher: Any | None = None,
            suppress_exceptions: bool = False,
            **kwargs: Any,
        ) -> bool:
            _ = (job_id, publisher, suppress_exceptions, kwargs)
            return False

        monkeypatch.setattr(files_api, "publish_job_enqueue_intent", _skip_publish)
        reprocess_response = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={"extraction_profile_id": str(initial_job.extraction_profile_id)},
        )
        assert reprocess_response.status_code == 202
        job = await _get_job(uuid.UUID(reprocess_response.json()["id"]))
        enqueued_job_ids.clear()

        requeued = await worker_module.recover_incomplete_ingest_jobs()

        assert recovered_job_ids == [str(job.id)]
        assert requeued == [job.id]

        await worker_module.process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert updated_job.job_type == "reprocess"
        assert updated_job.status == "succeeded"
        assert updated_job.attempts == 1

    async def test_reprocess_job_persists_base_revision_snapshot(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Reprocess creation should persist job_type and the latest finalized base revision."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        initial_job = await _get_job_for_file(str(uploaded["id"]))
        await worker_module.process_ingest_job(initial_job.id)

        response = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={"extraction_profile_id": str(initial_job.extraction_profile_id)},
        )

        assert response.status_code == 202
        assert response.json()["job_type"] == "reprocess"
        assert response.json()["base_revision_id"] is not None

        reprocess_job = await _get_job(uuid.UUID(response.json()["id"]))
        assert reprocess_job.job_type == "reprocess"
        assert reprocess_job.base_revision_id == uuid.UUID(response.json()["base_revision_id"])

    async def test_reprocess_job_replays_success_when_publish_is_deferred_after_commit(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Reprocess idempotency should snapshot success before best-effort publish."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        initial_job = await _get_job_for_file(str(uploaded["id"]))
        await worker_module.process_ingest_job(initial_job.id)

        async def _skip_publish(
            job_id: uuid.UUID,
            *,
            publisher: Any | None = None,
            suppress_exceptions: bool = False,
            **kwargs: Any,
        ) -> bool:
            _ = (job_id, publisher, suppress_exceptions, kwargs)
            return False

        monkeypatch.setattr(files_api, "publish_job_enqueue_intent", _skip_publish)

        headers = {"Idempotency-Key": "reprocess-outbox-replay"}
        first = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={"extraction_profile_id": str(initial_job.extraction_profile_id)},
            headers=headers,
        )
        second = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={"extraction_profile_id": str(initial_job.extraction_profile_id)},
            headers=headers,
        )

        assert first.status_code == 202
        assert second.status_code == 202
        assert second.json() == first.json()

        reprocess_job = await _get_job(uuid.UUID(first.json()["id"]))
        assert reprocess_job.status == "pending"
        assert reprocess_job.enqueue_status == "pending"

    async def test_reprocess_job_succeeds_when_enqueue_finalize_raises_after_commit(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Reprocess should not fail if post-commit enqueue bookkeeping raises after publish."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        initial_job = await _get_job_for_file(str(uploaded["id"]))
        await worker_module.process_ingest_job(initial_job.id)

        async def _fail_finalize(_: uuid.UUID, *, lease_token: uuid.UUID) -> bool:
            _ = lease_token
            raise RuntimeError("transient enqueue finalize failure")

        monkeypatch.setattr(worker_module, "_mark_job_enqueue_published", _fail_finalize)

        response = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={"extraction_profile_id": str(initial_job.extraction_profile_id)},
        )

        assert response.status_code == 202
        job = await _get_job(uuid.UUID(response.json()["id"]))
        assert job.status == "pending"
        assert job.enqueue_status == "publishing"

    async def test_process_reprocess_job_rejects_payload_revision_kind_mismatch(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Worker finalization should fail reprocess jobs whose payload kind drifts."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        initial_job = await _get_job_for_file(str(uploaded["id"]))
        await worker_module.process_ingest_job(initial_job.id)

        response = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={"extraction_profile_id": str(initial_job.extraction_profile_id)},
        )
        assert response.status_code == 202
        reprocess_job = await _get_job(uuid.UUID(response.json()["id"]))

        async def _run_ingestion_with_wrong_kind(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            return replace(_build_fake_ingest_payload(request), revision_kind="ingest")

        monkeypatch.setattr(worker_module, "run_ingestion", _run_ingestion_with_wrong_kind)

        with pytest.raises(
            worker_module._RevisionConflictError,
            match=r"Ingest job revision kind changed before finalization\.",
        ):
            await worker_module.process_ingest_job(reprocess_job.id)

        failed_job = await _get_job(reprocess_job.id)
        assert failed_job.status == "failed"
        assert failed_job.error_code == ErrorCode.REVISION_CONFLICT.value
        assert failed_job.error_message == "Ingest job revision kind changed before finalization."

    async def test_recover_incomplete_ingest_jobs_requeues_orphaned_running_jobs(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Worker startup recovery should requeue stale running ingest jobs."""
        _ = self
        _ = cleanup_projects

        recovered_job_ids: list[str] = []

        def _fake_recovery_enqueue(job_id: uuid.UUID) -> None:
            recovered_job_ids.append(str(job_id))

        monkeypatch.setattr(worker_module, "enqueue_ingest_job", _fake_recovery_enqueue)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        old_attempt_token = uuid.uuid4()
        enqueued_job_ids.clear()

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            persisted_job = await session.get(Job, job.id)
            assert persisted_job is not None
            persisted_job.status = "running"
            persisted_job.attempts = 1
            persisted_job.started_at = (
                datetime.now(UTC)
                - worker_module._RUNNING_JOB_STALE_AFTER
                - timedelta(seconds=1)
            )
            persisted_job.attempt_token = old_attempt_token
            persisted_job.attempt_lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            await session.commit()

        requeued = await worker_module.recover_incomplete_ingest_jobs()

        assert recovered_job_ids == [str(job.id)]
        assert requeued == [job.id]

        recovered_job = await _get_job(job.id)
        assert recovered_job.status == "pending"
        assert recovered_job.attempts == 1
        assert recovered_job.started_at is None
        assert recovered_job.finished_at is None
        assert recovered_job.attempt_token is None
        assert recovered_job.attempt_lease_expires_at is None

        await worker_module.process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert updated_job.status == "succeeded"
        assert updated_job.attempts == 2

    async def test_recover_incomplete_ingest_jobs_skips_locked_reclaimed_jobs(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Worker startup recovery should not clobber a concurrently reclaimed live attempt."""
        _ = self
        _ = cleanup_projects

        recovered_job_ids: list[str] = []

        def _fake_recovery_enqueue(job_id: uuid.UUID) -> None:
            recovered_job_ids.append(str(job_id))

        monkeypatch.setattr(worker_module, "enqueue_ingest_job", _fake_recovery_enqueue)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        stale_attempt_token = uuid.uuid4()
        fresh_attempt_token = uuid.uuid4()
        enqueued_job_ids.clear()

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            persisted_job = await session.get(Job, job.id)
            assert persisted_job is not None
            persisted_job.status = "running"
            persisted_job.attempts = 1
            persisted_job.started_at = (
                datetime.now(UTC)
                - worker_module._RUNNING_JOB_STALE_AFTER
                - timedelta(seconds=1)
            )
            persisted_job.attempt_token = stale_attempt_token
            persisted_job.attempt_lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            await session.commit()

        fresh_lease_expires_at = datetime.now(UTC) + worker_module._RUNNING_JOB_STALE_AFTER

        async with session_maker() as reclaim_session:
            reclaimed_job = await worker_module._get_job_for_update(reclaim_session, job.id)
            assert reclaimed_job is not None
            reclaimed_job.status = "running"
            reclaimed_job.attempts = 2
            reclaimed_job.started_at = datetime.now(UTC)
            reclaimed_job.finished_at = None
            reclaimed_job.error_code = None
            reclaimed_job.error_message = None
            reclaimed_job.attempt_token = fresh_attempt_token
            reclaimed_job.attempt_lease_expires_at = fresh_lease_expires_at
            await reclaim_session.flush()

            requeued = await asyncio.wait_for(
                worker_module.recover_incomplete_ingest_jobs(),
                timeout=2,
            )

            assert requeued == []
            assert recovered_job_ids == []

            await reclaim_session.commit()

        updated_job = await _get_job(job.id)
        assert updated_job.status == "running"
        assert updated_job.attempts == 2
        assert updated_job.attempt_token == fresh_attempt_token
        assert updated_job.attempt_lease_expires_at == fresh_lease_expires_at

    async def test_recover_incomplete_ingest_jobs_skips_fresh_running_jobs(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Worker startup recovery should not requeue fresh running ingest jobs."""
        _ = self
        _ = cleanup_projects

        recovered_job_ids: list[str] = []

        def _fake_recovery_enqueue(job_id: uuid.UUID) -> None:
            recovered_job_ids.append(str(job_id))

        monkeypatch.setattr(worker_module, "enqueue_ingest_job", _fake_recovery_enqueue)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        enqueued_job_ids.clear()

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            persisted_job = await session.get(Job, job.id)
            assert persisted_job is not None
            persisted_job.status = "running"
            persisted_job.attempts = 1
            persisted_job.started_at = datetime.now(UTC)
            await session.commit()

        requeued = await worker_module.recover_incomplete_ingest_jobs()

        assert recovered_job_ids == []
        assert requeued == []

        unchanged_job = await _get_job(job.id)
        assert unchanged_job.status == "running"
        assert unchanged_job.attempts == 1
        assert unchanged_job.started_at is not None

    async def test_recover_incomplete_ingest_jobs_sanitizes_enqueue_failure_details(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Worker startup recovery should not persist raw enqueue exception text."""
        _ = self
        _ = cleanup_projects

        secret_broker_text = "amqp://user:super-secret-password@broker/vhost timed out"
        logger_error_calls: list[tuple[str, dict[str, Any]]] = []

        def _fail_recovery_enqueue(_: uuid.UUID) -> None:
            raise RuntimeError(secret_broker_text)

        def _capture_logger_error(event: str, **kwargs: Any) -> None:
            logger_error_calls.append((event, kwargs))

        async def _skip_publish(
            job_id: uuid.UUID,
            *,
            publisher: Any | None = None,
            suppress_exceptions: bool = False,
            **kwargs: Any,
        ) -> bool:
            _ = (job_id, publisher, suppress_exceptions, kwargs)
            return False

        monkeypatch.setattr(files_api, "publish_job_enqueue_intent", _skip_publish)
        monkeypatch.setattr(worker_module, "enqueue_ingest_job", _fail_recovery_enqueue)
        monkeypatch.setattr(worker_module.logger, "error", _capture_logger_error)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        enqueued_job_ids.clear()

        requeued = await worker_module.recover_incomplete_ingest_jobs()

        assert requeued == []

        failed_job = await _get_job(job.id)
        assert failed_job.status == "failed"
        assert failed_job.error_code == ErrorCode.INTERNAL_ERROR.value
        assert failed_job.error_message == "Failed to enqueue ingest job"
        assert secret_broker_text not in failed_job.error_message
        assert len(failed_job.error_message) <= 255

        response = await async_client.get(f"/v1/jobs/{job.id}/events")
        assert response.status_code == 200
        data = response.json()
        assert [event["message"] for event in data["items"]] == ["Job failed"]
        assert data["items"][0]["data_json"] == {
            "status": "failed",
            "error_code": ErrorCode.INTERNAL_ERROR.value,
            "error_message": "Failed to enqueue ingest job",
        }
        assert secret_broker_text not in str(data["items"][0]["data_json"])
        assert data["next_cursor"] is None

        assert logger_error_calls == [
            (
                "ingest_job_recovery_enqueue_failed",
                {
                    "job_id": str(job.id),
                    "error_code": ErrorCode.INTERNAL_ERROR.value,
                    "recovery_action": "mark_failed",
                },
            )
        ]

    async def test_mark_recovery_enqueue_failed_skips_owned_running_jobs(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Recovery enqueue failure should not fail a job reclaimed by a live attempt."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        attempt_token = uuid.uuid4()
        lease_expires_at = datetime.now(UTC) + worker_module._RUNNING_JOB_STALE_AFTER

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            persisted_job = await session.get(Job, job.id)
            assert persisted_job is not None
            persisted_job.status = "running"
            persisted_job.attempts = 1
            persisted_job.started_at = datetime.now(UTC)
            persisted_job.finished_at = None
            persisted_job.error_code = None
            persisted_job.error_message = None
            persisted_job.attempt_token = attempt_token
            persisted_job.attempt_lease_expires_at = lease_expires_at
            await session.commit()

        marked_failed = await worker_module._mark_recovery_enqueue_failed(job.id)

        assert marked_failed is False
        updated_job = await _get_job(job.id)
        assert updated_job.status == "running"
        assert updated_job.error_code is None
        assert updated_job.error_message is None
        assert updated_job.finished_at is None
        assert updated_job.attempt_token == attempt_token
        assert updated_job.attempt_lease_expires_at == lease_expires_at

    async def test_mark_recovery_enqueue_failed_skips_terminal_jobs(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Recovery enqueue failure should not rewrite a terminalized job."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            persisted_job = await session.get(Job, job.id)
            assert persisted_job is not None
            persisted_job.status = "succeeded"
            persisted_job.finished_at = datetime.now(UTC)
            await session.commit()

        marked_failed = await worker_module._mark_recovery_enqueue_failed(job.id)

        assert marked_failed is False
        updated_job = await _get_job(job.id)
        assert updated_job.status == "succeeded"
        assert updated_job.error_code is None
        assert updated_job.error_message is None

    async def test_process_ingest_job_ignores_duplicate_delivery_after_success(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Duplicate delivery should not mutate terminal succeeded jobs."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        await worker_module.process_ingest_job(job.id)
        first_completion = await _get_job(job.id)
        assert first_completion.status == "succeeded"
        assert first_completion.attempts == 1

        await worker_module.process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert updated_job.status == "succeeded"
        assert updated_job.attempts == 1

    async def test_cancel_job_marks_pending_job_cancel_requested(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Cancel should mark a pending job for cancellation."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        response = await async_client.post(f"/v1/jobs/{job.id}/cancel")

        assert response.status_code == 202
        updated_job = await _get_job(job.id)
        assert updated_job.cancel_requested is True
        assert updated_job.status in {"pending", "cancelled"}

    async def test_cancel_job_preserves_quantity_takeoff_lineage_fields(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Cancel should not rewrite quantity job lineage metadata."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

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
        original = await _get_job(quantity_job.id)
        assert original.extraction_profile_id is None

        response = await async_client.post(f"/v1/jobs/{quantity_job.id}/cancel")

        assert response.status_code == 202
        updated = await _get_job(quantity_job.id)
        assert updated.cancel_requested is True
        assert updated.project_id == original.project_id
        assert updated.file_id == original.file_id
        assert updated.job_type == original.job_type
        assert updated.extraction_profile_id == original.extraction_profile_id
        assert updated.extraction_profile_id is None
        assert updated.base_revision_id == original.base_revision_id
        assert updated.parent_job_id == original.parent_job_id

    async def test_cancel_job_returns_404_for_unknown_job(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """Cancel should return 404 for unknown jobs."""
        _ = self
        _ = cleanup_projects

        missing_job_id = uuid.uuid4()

        response = await async_client.post(f"/v1/jobs/{missing_job_id}/cancel")

        assert response.status_code == 404
        assert response.json() == {
            "error": {
                "code": "NOT_FOUND",
                "message": f"Job with identifier '{missing_job_id}' not found",
                "details": None,
            }
        }

    async def test_cancel_job_is_terminal_no_op_for_succeeded_job(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Cancel should not mutate terminal succeeded jobs."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        await worker_module.process_ingest_job(job.id)
        completed = await _get_job(job.id)
        assert completed.status == "succeeded"
        assert completed.cancel_requested is False

        response = await async_client.post(f"/v1/jobs/{job.id}/cancel")

        assert response.status_code == 202
        unchanged = await _get_job(job.id)
        assert unchanged.status == "succeeded"
        assert unchanged.cancel_requested is False
        assert unchanged.attempts == 1

    async def test_retry_job_requeues_failed_job_below_max_attempts(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Retry should requeue failed jobs that still have capacity."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        retried_job_ids: list[str] = []

        async def _fake_retry_publish(
            job_id: uuid.UUID,
            *,
            publisher: Any | None = None,
            suppress_exceptions: bool = False,
            **kwargs: Any,
        ) -> bool:
            _ = (publisher, suppress_exceptions, kwargs)
            retried_job_ids.append(str(job_id))
            return True

        monkeypatch.setattr(
            jobs_api,
            "publish_job_enqueue_intent",
            _fake_retry_publish,
            raising=False,
        )

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        await _update_job(
            job.id,
            status="failed",
            attempts=1,
            max_attempts=3,
            error_message="previous failure",
        )

        response = await async_client.post(f"/v1/jobs/{job.id}/retry")

        assert response.status_code == 202
        assert retried_job_ids == [str(job.id)]
        updated = await _get_job(job.id)
        assert updated.status == "pending"
        assert updated.error_code is None
        assert updated.error_message is None

    async def test_retry_job_succeeds_when_publish_is_deferred(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Retry should succeed once durable enqueue intent commits."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        async def _skip_retry_publish(
            job_id: uuid.UUID,
            *,
            publisher: Any | None = None,
            suppress_exceptions: bool = False,
            **kwargs: Any,
        ) -> bool:
            _ = (job_id, publisher, suppress_exceptions, kwargs)
            return False

        monkeypatch.setattr(
            jobs_api,
            "publish_job_enqueue_intent",
            _skip_retry_publish,
            raising=False,
        )

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        await _update_job(
            job.id,
            status="failed",
            attempts=1,
            max_attempts=3,
            error_message="previous failure",
        )

        response = await async_client.post(f"/v1/jobs/{job.id}/retry")

        assert response.status_code == 202

        updated = await _get_job(job.id)
        assert updated.status == "pending"
        assert updated.error_code is None
        assert updated.error_message is None
        assert updated.finished_at is None
        assert updated.enqueue_status == "pending"

    async def test_retry_job_replays_success_when_publish_is_deferred_after_commit(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Retry idempotency should snapshot success before best-effort publish."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        async def _skip_retry_publish(
            job_id: uuid.UUID,
            *,
            publisher: Any | None = None,
            suppress_exceptions: bool = False,
            **kwargs: Any,
        ) -> bool:
            _ = (job_id, publisher, suppress_exceptions, kwargs)
            return False

        monkeypatch.setattr(
            jobs_api,
            "publish_job_enqueue_intent",
            _skip_retry_publish,
            raising=False,
        )

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        await _update_job(
            job.id,
            status="failed",
            attempts=1,
            max_attempts=3,
            error_message="previous failure",
        )

        headers = {"Idempotency-Key": "retry-outbox-replay"}
        first = await async_client.post(f"/v1/jobs/{job.id}/retry", headers=headers)
        second = await async_client.post(f"/v1/jobs/{job.id}/retry", headers=headers)

        assert first.status_code == 202
        assert second.status_code == 202
        assert second.json() == first.json()

        updated = await _get_job(job.id)
        assert updated.status == "pending"
        assert updated.enqueue_status == "pending"

    async def test_publish_job_enqueue_intent_routes_quantity_takeoff_jobs(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Quantity jobs should claim the durable outbox and use the quantity publisher."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        quantity_enqueued_job_ids: list[str] = []
        ingest_enqueued_job_ids: list[str] = []

        def _fake_quantity_enqueue(job_id: uuid.UUID) -> None:
            quantity_enqueued_job_ids.append(str(job_id))

        def _fake_ingest_enqueue(job_id: uuid.UUID) -> None:
            ingest_enqueued_job_ids.append(str(job_id))

        monkeypatch.setattr(worker_module, "enqueue_quantity_takeoff_job", _fake_quantity_enqueue)
        monkeypatch.setattr(worker_module, "enqueue_ingest_job", _fake_ingest_enqueue)

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
        last_attempted_at = datetime.now(UTC) - timedelta(minutes=5)
        unchanged_token = uuid.uuid4()
        await _update_job(
            quantity_job.id,
            enqueue_status="pending",
            enqueue_attempts=7,
            enqueue_owner_token=unchanged_token,
            enqueue_lease_expires_at=datetime.now(UTC) - timedelta(minutes=1),
            enqueue_last_attempted_at=last_attempted_at,
        )

        published = await worker_module.publish_job_enqueue_intent(quantity_job.id)

        assert published is True
        assert quantity_enqueued_job_ids == [str(quantity_job.id)]
        assert ingest_enqueued_job_ids == []
        updated = await _get_job(quantity_job.id)
        assert updated.status == "pending"
        assert updated.enqueue_status == "published"
        assert updated.extraction_profile_id is None
        assert updated.enqueue_attempts == 8
        assert updated.enqueue_owner_token is None
        assert updated.enqueue_lease_expires_at is None
        assert updated.enqueue_last_attempted_at is not None
        assert updated.enqueue_last_attempted_at >= last_attempted_at
        assert updated.enqueue_published_at is not None

    async def test_retry_job_requeues_failed_quantity_takeoff_job(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Retry should route failed quantity jobs through durable queue publication."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        retried_job_ids: list[str] = []

        async def _fake_retry_publish(
            job_id: uuid.UUID,
            *,
            publisher: Any | None = None,
            suppress_exceptions: bool = False,
            **kwargs: Any,
        ) -> bool:
            assert publisher is None
            _ = (suppress_exceptions, kwargs)
            retried_job_ids.append(str(job_id))
            return True

        monkeypatch.setattr(
            jobs_api,
            "publish_job_enqueue_intent",
            _fake_retry_publish,
            raising=False,
        )

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
            status="failed",
            attempts=1,
            max_attempts=3,
        )
        await _update_job(
            quantity_job.id,
            error_code=ErrorCode.INTERNAL_ERROR.value,
            error_message="quantity worker unavailable",
            enqueue_status="pending",
            enqueue_attempts=2,
        )
        original = await _get_job(quantity_job.id)
        assert original.extraction_profile_id is None

        response = await async_client.post(f"/v1/jobs/{quantity_job.id}/retry")

        assert response.status_code == 202
        assert retried_job_ids == [str(quantity_job.id)]
        retried = await _get_job(quantity_job.id)
        assert retried.status == "pending"
        assert retried.attempts == original.attempts
        assert retried.error_code is None
        assert retried.error_message is None
        assert retried.enqueue_status == "pending"
        assert retried.enqueue_attempts == 0
        assert retried.project_id == original.project_id
        assert retried.file_id == original.file_id
        assert retried.job_type == original.job_type
        assert retried.extraction_profile_id == original.extraction_profile_id
        assert retried.extraction_profile_id is None
        assert retried.base_revision_id == original.base_revision_id
        assert retried.parent_job_id == original.parent_job_id

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

    async def test_process_ingest_job_skips_quantity_takeoff_jobs_without_mutation(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Direct ingest-worker delivery should no-op for quantity jobs."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

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
        project, uploaded, _, base_revision, quantity_job = (
            await _create_real_dxf_quantity_takeoff_job(async_client, fixture_filename)
        )

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
        assert aggregate_values["count:entity_count"] == pytest.approx(
            float(expected_line_count)
        )
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
        project, uploaded, ingest_job, base_revision, first_job = (
            await _create_real_dxf_quantity_takeoff_job(async_client, fixture_filename)
        )
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
        assert aggregate_values["count:entity_count"] == pytest.approx(
            float(expected_line_count)
        )
        assert aggregate_values["count:line_count"] == pytest.approx(float(expected_line_count))
        assert _quantity_length_total(aggregate_values) == pytest.approx(expected_total_length)

        length_contributors = [
            item
            for item in items
            if item.item_kind == "contributor" and item.quantity_type.startswith("length")
        ]
        assert len(length_contributors) == 1
        assert length_contributors[0].excluded_source_entity_ids_json != []

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
            updated_job.error_message
            == worker_module._QUANTITY_TAKEOFF_GATE_BLOCKED_ERROR_MESSAGE
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
            updated_job.error_message
            == worker_module._QUANTITY_TAKEOFF_GATE_BLOCKED_ERROR_MESSAGE
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
            event_payload["error_message"]
            == worker_module._QUANTITY_TAKEOFF_CONFLICT_ERROR_MESSAGE
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
        assert (
            updated_job.error_code
            == ErrorCode.NORMALIZED_ENTITIES_NOT_MATERIALIZED.value
        )
        assert (
            updated_job.error_message
            == worker_module._QUANTITY_TAKEOFF_MATERIALIZATION_MISSING_ERROR_MESSAGE
        )
        assert takeoffs == []

        response = await async_client.get(f"/v1/jobs/{quantity_job.id}/events")
        assert response.status_code == 200
        event_payload = response.json()["items"][-1]["data_json"]
        assert event_payload["status"] == "failed"
        assert (
            event_payload["error_code"]
            == ErrorCode.NORMALIZED_ENTITIES_NOT_MATERIALIZED.value
        )
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
        assert (
            updated_job.error_code
            == ErrorCode.NORMALIZED_ENTITIES_NOT_MATERIALIZED.value
        )
        assert takeoffs == []

        response = await async_client.get(f"/v1/jobs/{quantity_job.id}/events")
        assert response.status_code == 200
        event_payload = response.json()["items"][-1]["data_json"]
        assert (
            event_payload["error_code"]
            == ErrorCode.NORMALIZED_ENTITIES_NOT_MATERIALIZED.value
        )
        assert event_payload["details"] == {
            "drawing_revision_id": str(base_revision.id),
            "expected_entities": 1,
            "loaded_entities": 0,
        }

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

    async def test_retry_job_succeeds_when_enqueue_claim_raises_after_commit(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Retry should not fail if post-commit enqueue bookkeeping raises before claim."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        async def _fail_claim(_: uuid.UUID) -> worker_module._EnqueueIntentLease | None:
            raise RuntimeError("transient enqueue claim failure")

        monkeypatch.setattr(worker_module, "_claim_job_enqueue_intent", _fail_claim)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        await _update_job(
            job.id,
            status="failed",
            attempts=1,
            max_attempts=3,
            error_message="previous failure",
        )

        response = await async_client.post(f"/v1/jobs/{job.id}/retry")

        assert response.status_code == 202
        updated = await _get_job(job.id)
        assert updated.status == "pending"
        assert updated.enqueue_status == "pending"

    async def test_retry_job_noops_when_attempt_limit_reached(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Retry should no-op when attempts already reached max_attempts."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        retried_job_ids: list[str] = []

        async def _fake_retry_publish(
            job_id: uuid.UUID,
            *,
            publisher: Any | None = None,
            suppress_exceptions: bool = False,
            **kwargs: Any,
        ) -> bool:
            _ = (publisher, suppress_exceptions, kwargs)
            retried_job_ids.append(str(job_id))
            return True

        monkeypatch.setattr(
            jobs_api,
            "publish_job_enqueue_intent",
            _fake_retry_publish,
            raising=False,
        )

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        await _update_job(
            job.id,
            status="failed",
            attempts=3,
            max_attempts=3,
            error_message="maxed out",
        )

        response = await async_client.post(f"/v1/jobs/{job.id}/retry")

        assert response.status_code == 202
        assert retried_job_ids == []
        unchanged = await _get_job(job.id)
        assert unchanged.status == "failed"
        assert unchanged.attempts == 3
        assert unchanged.max_attempts == 3

    async def test_retry_job_noops_for_revision_conflict_failure(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Retry should not requeue jobs that already failed with REVISION_CONFLICT."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        retried_job_ids: list[str] = []

        async def _fake_retry_publish(
            job_id: uuid.UUID,
            *,
            publisher: Any | None = None,
            suppress_exceptions: bool = False,
            **kwargs: Any,
        ) -> bool:
            _ = (publisher, suppress_exceptions, kwargs)
            retried_job_ids.append(str(job_id))
            return True

        monkeypatch.setattr(
            jobs_api,
            "publish_job_enqueue_intent",
            _fake_retry_publish,
            raising=False,
        )

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        await _update_job(
            job.id,
            status="failed",
            attempts=1,
            max_attempts=3,
            error_code=ErrorCode.REVISION_CONFLICT.value,
            error_message="Reprocess base revision became stale before finalization.",
        )

        response = await async_client.post(f"/v1/jobs/{job.id}/retry")

        assert response.status_code == 202
        assert retried_job_ids == []
        unchanged = await _get_job(job.id)
        assert unchanged.status == "failed"
        assert unchanged.error_code == ErrorCode.REVISION_CONFLICT.value
        assert (
            unchanged.error_message
            == "Reprocess base revision became stale before finalization."
        )

    async def test_retry_job_is_terminal_no_op_for_cancelled_job(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Retry should no-op for terminal cancelled jobs."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        retried_job_ids: list[str] = []

        async def _fake_retry_publish(
            job_id: uuid.UUID,
            *,
            publisher: Any | None = None,
            suppress_exceptions: bool = False,
            **kwargs: Any,
        ) -> bool:
            _ = (publisher, suppress_exceptions, kwargs)
            retried_job_ids.append(str(job_id))
            return True

        monkeypatch.setattr(
            jobs_api,
            "publish_job_enqueue_intent",
            _fake_retry_publish,
            raising=False,
        )

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        await _update_job(job.id, status="cancelled", attempts=1, max_attempts=3)

        response = await async_client.post(f"/v1/jobs/{job.id}/retry")

        assert response.status_code == 202
        assert retried_job_ids == []
        unchanged = await _get_job(job.id)
        assert unchanged.status == "cancelled"
        assert unchanged.attempts == 1

    async def test_retry_job_returns_404_for_unknown_job(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """Retry should return 404 for unknown jobs."""
        _ = self
        _ = cleanup_projects

        missing_job_id = uuid.uuid4()

        response = await async_client.post(f"/v1/jobs/{missing_job_id}/retry")

        assert response.status_code == 404
        assert response.json() == {
            "error": {
                "code": "NOT_FOUND",
                "message": f"Job with identifier '{missing_job_id}' not found",
                "details": None,
            }
        }

    @pytest.mark.parametrize(
        ("delete_project", "delete_file"),
        [(True, False), (False, True)],
    )
    async def test_retry_job_returns_404_for_soft_deleted_source(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
        delete_project: bool,
        delete_file: bool,
    ) -> None:
        """Retry should hide jobs whose backing project or file is soft-deleted."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        retried_job_ids: list[str] = []

        async def _fake_retry_publish(
            job_id: uuid.UUID,
            *,
            publisher: Any | None = None,
            suppress_exceptions: bool = False,
            **kwargs: Any,
        ) -> bool:
            _ = (publisher, suppress_exceptions, kwargs)
            retried_job_ids.append(str(job_id))
            return True

        monkeypatch.setattr(
            jobs_api,
            "publish_job_enqueue_intent",
            _fake_retry_publish,
            raising=False,
        )

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        await _update_job(
            job.id,
            status="failed",
            attempts=1,
            max_attempts=3,
            error_message="previous failure",
        )
        await _mark_source_deleted(
            uuid.UUID(project["id"]),
            uuid.UUID(uploaded["id"]),
            delete_project=delete_project,
            delete_file=delete_file,
        )

        response = await async_client.post(f"/v1/jobs/{job.id}/retry")

        assert response.status_code == 404
        assert response.json() == {
            "error": {
                "code": "NOT_FOUND",
                "message": f"Job with identifier '{job.id}' not found",
                "details": None,
            }
        }
        assert retried_job_ids == []
        unchanged = await _get_job(job.id)
        assert unchanged.status == "failed"

    async def test_retry_job_delete_race_delete_wins_returns_404_without_requeue(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Delete vs retry lock contention should settle without deadlock when delete wins."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        retried_job_ids: list[str] = []

        async def _fake_retry_publish(
            job_id: uuid.UUID,
            *,
            publisher: Any | None = None,
            suppress_exceptions: bool = False,
            **kwargs: Any,
        ) -> bool:
            _ = (publisher, suppress_exceptions, kwargs)
            retried_job_ids.append(str(job_id))
            return True

        monkeypatch.setattr(
            jobs_api,
            "publish_job_enqueue_intent",
            _fake_retry_publish,
            raising=False,
        )

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        await _update_job(
            job.id,
            status="failed",
            attempts=1,
            max_attempts=3,
            error_message="previous failure",
        )

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            await session.execute(
                select(Project)
                .where(Project.id == uuid.UUID(project["id"]))
                .with_for_update(of=Project)
            )

            delete_task = asyncio.create_task(
                async_client.delete(f"/v1/projects/{project['id']}")
            )
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(delete_task), timeout=0.2)

            retry_task = asyncio.create_task(async_client.post(f"/v1/jobs/{job.id}/retry"))
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(retry_task), timeout=0.2)

        delete_response = await asyncio.wait_for(delete_task, timeout=2)
        retry_response = await asyncio.wait_for(retry_task, timeout=2)

        assert delete_response.status_code == 204
        assert retry_response.status_code == 404
        assert retry_response.json() == {
            "error": {
                "code": "NOT_FOUND",
                "message": f"Job with identifier '{job.id}' not found",
                "details": None,
            }
        }
        assert retried_job_ids == []

        unchanged = await _get_job(job.id)
        assert unchanged.status == "failed"

    async def test_process_ingest_job_finalizes_cancel_requested_job_as_cancelled(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Worker should finalize cancel-requested jobs to cancelled."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        await _update_job(job.id, status="pending", cancel_requested=True)

        await worker_module.process_ingest_job(job.id)

        updated = await _get_job(job.id)
        assert updated.status == "cancelled"
        assert updated.cancel_requested is True
        assert updated.error_code == ErrorCode.JOB_CANCELLED.value
        assert updated.finished_at is not None
        assert updated.error_message is None

    async def test_process_ingest_job_cancels_soft_deleted_source_before_runner(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Worker should not start ingestion when the source project is already soft-deleted."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        runner_calls: list[IngestionRunRequest] = []

        async def _unexpected_run_ingestion(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            runner_calls.append(request)
            return _build_fake_ingest_payload(request)

        monkeypatch.setattr(worker_module, "run_ingestion", _unexpected_run_ingestion)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        await _mark_source_deleted(
            uuid.UUID(project["id"]),
            uuid.UUID(uploaded["id"]),
            delete_project=True,
            delete_file=False,
        )

        await worker_module.process_ingest_job(job.id)

        updated = await _get_job(job.id)
        assert runner_calls == []
        assert updated.status == "cancelled"
        assert updated.cancel_requested is True
        assert updated.error_code == ErrorCode.JOB_CANCELLED.value
        assert updated.finished_at is not None

    async def test_process_ingest_job_skips_finalization_storage_for_soft_deleted_source(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Worker should cancel before storage writes if the source is deleted mid-run."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        class _UnexpectedStorage:
            async def put(self, *_: Any, **__: Any) -> Any:
                raise AssertionError("storage.put should not be called for deleted sources")

        async def _delete_source_during_run(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            await _mark_source_deleted(
                uuid.UUID(project["id"]),
                uuid.UUID(uploaded["id"]),
                delete_project=False,
                delete_file=True,
            )
            return _build_fake_ingest_payload(request)

        monkeypatch.setattr(worker_module, "get_storage", lambda: _UnexpectedStorage())
        monkeypatch.setattr(worker_module, "run_ingestion", _delete_source_during_run)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        await worker_module.process_ingest_job(job.id)

        updated = await _get_job(job.id)
        assert updated.status == "cancelled"
        assert updated.cancel_requested is True
        assert updated.error_code == ErrorCode.JOB_CANCELLED.value
        assert updated.finished_at is not None

    async def test_process_ingest_job_begin_race_delete_wins_cancels_before_runner(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Delete vs worker begin lock contention should cancel without deadlock."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        runner_calls: list[IngestionRunRequest] = []

        async def _unexpected_run_ingestion(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            runner_calls.append(request)
            return _build_fake_ingest_payload(request)

        monkeypatch.setattr(worker_module, "run_ingestion", _unexpected_run_ingestion)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            await session.execute(
                select(Project)
                .where(Project.id == uuid.UUID(project["id"]))
                .with_for_update(of=Project)
            )

            delete_task = asyncio.create_task(
                async_client.delete(f"/v1/projects/{project['id']}")
            )
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(delete_task), timeout=0.2)

            process_task = asyncio.create_task(worker_module.process_ingest_job(job.id))
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(process_task), timeout=0.2)

        delete_response = await asyncio.wait_for(delete_task, timeout=2)
        await asyncio.wait_for(process_task, timeout=2)

        assert delete_response.status_code == 204
        updated = await _get_job(job.id)
        assert runner_calls == []
        assert updated.status == "cancelled"
        assert updated.cancel_requested is True
        assert updated.error_code == ErrorCode.JOB_CANCELLED.value
        assert updated.finished_at is not None

        artifacts = await _get_generated_artifacts_for_job(job.id)
        assert artifacts == []

    async def test_mark_job_failed_delete_race_delete_wins_without_failed_overwrite(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Delete should win over a concurrent worker failure terminal write."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        lease = await worker_module._begin_or_resume_ingest_job(job.id)

        assert lease is not None
        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            await session.execute(
                select(Project)
                .where(Project.id == uuid.UUID(project["id"]))
                .with_for_update(of=Project)
            )

            delete_task = asyncio.create_task(
                async_client.delete(f"/v1/projects/{project['id']}")
            )
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(delete_task), timeout=0.2)

            fail_task = asyncio.create_task(
                worker_module._mark_job_failed(
                    job.id,
                    error_message="Ingest job failed unexpectedly.",
                    attempt_token=lease.token,
                )
            )
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(fail_task), timeout=0.2)

        delete_response = await asyncio.wait_for(delete_task, timeout=2)
        failed = await asyncio.wait_for(fail_task, timeout=2)

        assert delete_response.status_code == 204
        assert failed is False

        updated_job = await _get_job(job.id)
        assert updated_job.status == "cancelled"
        assert updated_job.cancel_requested is True
        assert updated_job.error_code == ErrorCode.JOB_CANCELLED.value
        assert updated_job.error_message is None
        assert updated_job.finished_at is not None

    async def test_finalize_ingest_job_delete_race_finalize_wins_soft_deletes_artifact(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Finalize before delete should keep job terminal while delete soft-deletes outputs."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        storage_put_started = asyncio.Event()
        allow_storage_put = asyncio.Event()

        class _BlockingStorage:
            async def put(self, key: str, payload: bytes, *, immutable: bool = True) -> Any:
                _ = immutable
                storage_put_started.set()
                await allow_storage_put.wait()
                return types.SimpleNamespace(
                    key=key,
                    storage_uri=f"file://tests/{key}",
                    size_bytes=len(payload),
                    checksum_sha256=hashlib.sha256(payload).hexdigest(),
                )

        monkeypatch.setattr(worker_module, "get_storage", lambda: _BlockingStorage())

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        lease = await worker_module._begin_or_resume_ingest_job(job.id)
        assert lease is not None

        finalize_request = IngestionRunRequest(
            job_id=job.id,
            file_id=job.file_id,
            checksum_sha256=hashlib.sha256(_TEST_UPLOAD_BODY).hexdigest(),
            detected_format="pdf",
            media_type="application/pdf",
            original_name="plan.pdf",
            extraction_profile_id=job.extraction_profile_id,
            initial_job_id=job.id,
        )
        payload = _build_fake_ingest_payload(finalize_request)

        finalize_task = asyncio.create_task(
            worker_module._finalize_ingest_job(
                job.id,
                attempt_token=lease.token,
                payload=payload,
            )
        )

        await asyncio.wait_for(storage_put_started.wait(), timeout=2)

        delete_task = asyncio.create_task(async_client.delete(f"/v1/projects/{project['id']}"))
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(delete_task), timeout=0.2)

        allow_storage_put.set()

        finalized = await asyncio.wait_for(finalize_task, timeout=3)
        delete_response = await asyncio.wait_for(delete_task, timeout=3)

        assert finalized is True
        assert delete_response.status_code == 204

        updated = await _get_job(job.id)
        assert updated.status == "succeeded"
        assert updated.cancel_requested is False
        assert updated.finished_at is not None

        artifacts = await _get_generated_artifacts_for_job(job.id)
        assert len(artifacts) == 1
        assert artifacts[0].deleted_at is not None

    async def test_process_ingest_job_finalizes_cancelled_when_requested_during_completion_race(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Worker should persist cancelled if cancellation commits before completion."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        async def _cancel_during_work(request: IngestionRunRequest) -> IngestFinalizationPayload:
            await _update_job(job.id, cancel_requested=True)
            return _build_fake_ingest_payload(request)

        monkeypatch.setattr(worker_module, "run_ingestion", _cancel_during_work)

        with suppress(asyncio.CancelledError):
            await worker_module.process_ingest_job(job.id)

        updated = await _get_job(job.id)
        assert updated.status == "cancelled"
        assert updated.attempts == 1
        assert updated.cancel_requested is True
        assert updated.finished_at is not None
        assert updated.error_code == ErrorCode.JOB_CANCELLED.value

    async def test_process_ingest_job_ignores_duplicate_delivery_after_cancelled(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Duplicate delivery should not mutate terminal cancelled jobs."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        await _update_job(job.id, status="cancelled", attempts=1, cancel_requested=True)

        before = await _get_job(job.id)
        before_finished_at = before.finished_at

        await worker_module.process_ingest_job(job.id)

        after = await _get_job(job.id)
        assert after.status == "cancelled"
        assert after.attempts == 1
        assert after.cancel_requested is True
        assert after.finished_at == before_finished_at

    async def test_job_constraints_accept_valid_type_status_error_writes(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Valid constrained job writes should still commit."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        initial_job = await _get_job_for_file(str(uploaded["id"]))
        await worker_module.process_ingest_job(initial_job.id)
        reprocess_response = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={"extraction_profile_id": str(initial_job.extraction_profile_id)},
        )
        assert reprocess_response.status_code == 202
        job = await _get_job(uuid.UUID(reprocess_response.json()["id"]))

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            persisted_job = await session.get(Job, job.id)
            assert persisted_job is not None

            persisted_job.job_type = "reprocess"
            persisted_job.status = "failed"
            persisted_job.error_code = ErrorCode.ADAPTER_FAILED.value
            persisted_job.error_message = "Adapter execution failed."
            persisted_job.enqueue_status = "published"

            await session.commit()

        updated = await _get_job(job.id)
        assert updated.job_type == "reprocess"
        assert updated.status == "failed"
        assert updated.error_code == ErrorCode.ADAPTER_FAILED.value

    @pytest.mark.parametrize(
        ("field_name", "invalid_value"),
        [
            ("job_type", "not-a-job-type"),
            ("status", "queued"),
            ("error_code", "NOT_A_REAL_ERROR_CODE"),
            ("enqueue_status", "queued"),
        ],
    )
    async def test_job_constraints_reject_invalid_string_writes(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        field_name: str,
        invalid_value: str,
    ) -> None:
        """Invalid constrained string writes should fail at the database."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            persisted_job = await session.get(Job, job.id)
            assert persisted_job is not None

            setattr(persisted_job, field_name, invalid_value)

            with pytest.raises(IntegrityError):
                await session.commit()

            await session.rollback()

    @pytest.mark.parametrize("job_type", ["ingest", "reprocess"])
    async def test_job_constraints_reject_profile_required_job_without_extraction_profile(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        job_type: str,
    ) -> None:
        """Persisted ingest/reprocess jobs must retain an extraction profile identifier."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            persisted_job = await session.get(Job, job.id)
            assert persisted_job is not None

            persisted_job.job_type = job_type
            persisted_job.extraction_profile_id = None

            with pytest.raises(IntegrityError):
                await session.commit()

            await session.rollback()
