"""Integration tests for persisted job status and worker transitions."""

import asyncio
import json
import types
import uuid
from collections.abc import Callable
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
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
from app.ingestion.finalization import IngestFinalizationPayload, compute_adapter_result_checksum
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
from app.models.file import File
from app.models.generated_artifact import GeneratedArtifact
from app.models.job import Job, JobType
from app.models.project import Project
from app.models.quantity_takeoff import QuantityItem, QuantityTakeoff
from tests.conftest import requires_database
from tests.jobs_test_helpers import (
    _create_project,
    _get_job,
    _get_job_for_file,
    _update_job,
    _upload_file,
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
_TEST_UPLOAD_BODY = b"%PDF-1.7\njob-test\n"
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


def test_worker_loop_reuses_same_running_loop_for_sequential_sync_entrypoints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sync worker entrypoints should reuse one running loop across sequential calls."""
    observed_calls: list[tuple[str, uuid.UUID, int]] = []
    recovered_loop_ids: list[int] = []
    ingest_job_id = uuid.uuid4()
    quantity_job_id = uuid.uuid4()
    estimate_job_id = uuid.uuid4()

    async def _fake_recover() -> list[uuid.UUID]:
        recovered_loop_ids.append(id(asyncio.get_running_loop()))
        return []

    async def _fake_process_ingest(job_id: uuid.UUID) -> None:
        observed_calls.append(("ingest", job_id, id(asyncio.get_running_loop())))

    async def _fake_process_quantity(job_id: uuid.UUID) -> None:
        observed_calls.append(("quantity", job_id, id(asyncio.get_running_loop())))

    async def _fake_process_estimate(job_id: uuid.UUID) -> None:
        observed_calls.append(("estimate", job_id, id(asyncio.get_running_loop())))

    worker_module._close_worker_loop_runner()
    monkeypatch.setattr(worker_module, "recover_incomplete_ingest_jobs", _fake_recover)
    monkeypatch.setattr(worker_module, "process_ingest_job", _fake_process_ingest)
    monkeypatch.setattr(worker_module, "process_quantity_takeoff_job", _fake_process_quantity)
    monkeypatch.setattr(worker_module, "process_estimate_job", _fake_process_estimate)

    try:
        worker_module.recover_incomplete_ingest_jobs_on_worker_start()
        worker_module.run_ingest_job(str(ingest_job_id))
        worker_module.run_quantity_takeoff_job(str(quantity_job_id))
        worker_module.run_estimate_job(str(estimate_job_id))
    finally:
        worker_module._close_worker_loop_runner()

    assert [call[:2] for call in observed_calls] == [
        ("ingest", ingest_job_id),
        ("quantity", quantity_job_id),
        ("estimate", estimate_job_id),
    ]
    assert len(recovered_loop_ids) == 1
    loop_ids = recovered_loop_ids + [loop_id for _, _, loop_id in observed_calls]
    assert len(set(loop_ids)) == 1


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


class _AdapterModule(types.ModuleType):
    create_adapter: Callable[[], object]

    def __init__(self, name: str, create_adapter: Callable[[], object]) -> None:
        super().__init__(name)
        self.create_adapter = create_adapter


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
    coverage_json = {
        "schema_version": "0.1",
        "entities": {
            "total": 1,
            "mapped": 1,
            "unmapped": 0,
            "mapped_ratio": 1.0,
            "by_type": {"line": 1},
        },
        "unmapped_by_reason": {},
        "layers": {
            "count": 1,
            "entities_with_layer_ref": 0,
            "source": None,
        },
        "blocks": {
            "count": 0,
            "child_geometry_count": 0,
        },
        "review_flagged_entities": 0,
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
            "entity_counts": entity_counts,
        },
        "coverage": coverage_json,
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
        warnings_json=warnings_json,
        diagnostics_json=diagnostics_json,
        result_checksum_sha256=compute_adapter_result_checksum(result_envelope),
        validation_report_schema_version=_FAKE_RUNNER_VALIDATION_REPORT_SCHEMA_VERSION,
        validation_status=_FAKE_RUNNER_VALIDATION_STATUS,
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
    """Replace the fake validation_status while preserving deterministic payload shape.

    Path B 5b: review_state / quantity_gate are no longer derived or persisted, so they
    flow as NULL regardless of these (now-vestigial) params; only validation_status is set.
    """
    _ = (review_state, quantity_gate)
    report_json = deepcopy(payload.report_json)
    summary = report_json.get("summary")
    if isinstance(summary, dict):
        report_json["summary"] = {**summary, "validation_status": validation_status}
    report_json["validation_status"] = validation_status
    return replace(
        payload,
        validation_status=validation_status,
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
                        EstimateSnapshotEntry.created_at.asc(), EstimateSnapshotEntry.id.asc()
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
        ref_type = cast(str, ref_payload["ref_type"])
        selection_key = cast(str, ref_payload["selection_key"])
        checksum = cast(str, ref_payload["catalog_checksum_sha256"])

        if ref_type == "rate":
            rate_id = cast(uuid.UUID, ref_payload["rate_catalog_entry_id"])
            catalog_rows.append(
                EstimationRate(
                    id=rate_id,
                    scope_type=CatalogScopeType.PROJECT,
                    project_id=estimate_job.project_id,
                    rate_key=selection_key,
                    source="tests.test_jobs",
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
            material_id = cast(uuid.UUID, ref_payload["material_catalog_entry_id"])
            catalog_rows.append(
                EstimationMaterial(
                    id=material_id,
                    scope_type=CatalogScopeType.PROJECT,
                    project_id=estimate_job.project_id,
                    material_key=selection_key,
                    source="tests.test_jobs",
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
            formula_id = cast(uuid.UUID, ref_payload["formula_definition_id"])
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
            ref_type=cast(str, ref_payload["ref_type"]),
            selection_key=cast(str, ref_payload["selection_key"]),
            ref_order=cast(int, ref_payload["ref_order"]),
            rate_catalog_entry_id=cast(uuid.UUID | None, ref_payload.get("rate_catalog_entry_id")),
            material_catalog_entry_id=cast(
                uuid.UUID | None,
                ref_payload.get("material_catalog_entry_id"),
            ),
            formula_definition_id=cast(
                uuid.UUID | None,
                ref_payload.get("formula_definition_id"),
            ),
            catalog_checksum_sha256=cast(str, ref_payload["catalog_checksum_sha256"]),
            selection_context_json=deepcopy(
                cast(dict[str, Any], ref_payload["selection_context_json"])
            ),
        )
        for ref_payload in catalog_refs
    ]

    async with session_maker() as session:
        session.add(estimate_input)
        session.add_all(catalog_rows)
        await session.flush()
        session.add_all(ref_rows)
        await session.commit()


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
        return _build_fake_quantity_ingest_payload(
            request,
            review_state="approved",
            validation_status="valid",
            quantity_gate="allowed",
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

    estimate_job = await _create_quantity_takeoff_job(
        project_id=uuid.UUID(project["id"]),
        file_id=uuid.UUID(uploaded["id"]),
        base_revision_id=base_revision.id,
        parent_job_id=ingest_job.id,
        job_type=JobType.ESTIMATE.value,
        status="pending",
    )

    return project, uploaded, ingest_job, base_revision, estimate_job


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


async def _get_generated_artifacts_for_job(job_id: uuid.UUID) -> list[GeneratedArtifact]:
    """Load generated artifacts for a job id."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        artifacts = (
            (
                await session.execute(
                    select(GeneratedArtifact)
                    .where(GeneratedArtifact.job_id == job_id)
                    .order_by(GeneratedArtifact.created_at.asc(), GeneratedArtifact.id.asc())
                )
            )
            .scalars()
            .all()
        )

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

    marked_failed = await worker_module._mark_recovery_enqueue_failed(
        job_id,
        job_type=JobType.INGEST.value,
    )

    assert marked_failed is True
    assert marked_failed_job_ids == [job_id]
    assert logger_error_calls == [
        (
            "job_recovery_enqueue_failed",
            {
                "job_id": str(job_id),
                "job_type": JobType.INGEST.value,
                "error_code": ErrorCode.INTERNAL_ERROR.value,
                "recovery_action": "mark_failed",
            },
        )
    ]


def test_claim_job_attempt_lease_adapter_forwards_stale_after_keyword(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Worker adapter should accept and forward lifecycle stale-after overrides."""
    job = cast(Job, object())
    now = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    stale_after = timedelta(seconds=17)
    sentinel_lease = cast(Any, object())
    captured: dict[str, Any] = {}

    def _fake_claim_job_attempt_lease(
        forwarded_job: Job,
        *,
        now: datetime,
        increment_attempt: bool,
        stale_after: timedelta,
    ) -> Any:
        captured.update(
            {
                "job": forwarded_job,
                "now": now,
                "increment_attempt": increment_attempt,
                "stale_after": stale_after,
            }
        )
        return sentinel_lease

    monkeypatch.setattr(
        "app.jobs.worker.job_lifecycle._claim_job_attempt_lease",
        _fake_claim_job_attempt_lease,
    )

    lease = worker_module._claim_job_attempt_lease(
        job,
        now=now,
        increment_attempt=True,
        stale_after=stale_after,
    )

    assert lease is sentinel_lease
    assert captured == {
        "job": job,
        "now": now,
        "increment_attempt": True,
        "stale_after": stale_after,
    }


def test_is_stale_running_job_adapter_forwards_stale_after_keyword(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Worker adapter should accept and forward lifecycle stale-after overrides."""
    job = cast(Job, object())
    now = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    stale_after = timedelta(seconds=29)
    captured: dict[str, Any] = {}

    def _fake_is_stale_running_job(
        forwarded_job: Job,
        *,
        now: datetime,
        stale_after: timedelta,
    ) -> bool:
        captured.update(
            {
                "job": forwarded_job,
                "now": now,
                "stale_after": stale_after,
            }
        )
        return True

    monkeypatch.setattr(
        "app.jobs.worker.job_lifecycle._is_stale_running_job",
        _fake_is_stale_running_job,
    )

    is_stale = worker_module._is_stale_running_job(
        job,
        now=now,
        stale_after=stale_after,
    )

    assert is_stale is True
    assert captured == {
        "job": job,
        "now": now,
        "stale_after": stale_after,
    }


@requires_database
class TestJobs:
    """Tests for job status retrieval and worker state transitions."""

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

    async def test_cancel_job_replays_terminal_no_op_with_idempotency_key(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Cancel should snapshot and replay terminal no-op responses."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        await worker_module.process_ingest_job(job.id)

        headers = {"Idempotency-Key": "cancel-terminal-replay"}
        first = await async_client.post(f"/v1/jobs/{job.id}/cancel", headers=headers)
        second = await async_client.post(f"/v1/jobs/{job.id}/cancel", headers=headers)

        assert first.status_code == 202
        assert second.status_code == 202
        assert second.json() == first.json()
        unchanged = await _get_job(job.id)
        assert unchanged.status == "succeeded"
        assert unchanged.cancel_requested is False

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

    async def test_retry_job_publishes_only_after_commit(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Retry publish should observe committed state only after mutation commit."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        observed_publish_states: list[tuple[str, str]] = []

        async def _observe_retry_publish(
            job_id: uuid.UUID,
            *,
            publisher: Any | None = None,
            suppress_exceptions: bool = False,
            **kwargs: Any,
        ) -> bool:
            _ = (publisher, suppress_exceptions, kwargs)
            persisted = await _get_job(job_id)
            observed_publish_states.append((persisted.status, persisted.enqueue_status))
            assert persisted.cancel_requested is False
            assert persisted.error_code is None
            assert persisted.error_message is None
            return True

        monkeypatch.setattr(
            jobs_api,
            "publish_job_enqueue_intent",
            _observe_retry_publish,
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
            cancel_requested=True,
            error_message="previous failure",
        )

        response = await async_client.post(f"/v1/jobs/{job.id}/retry")

        assert response.status_code == 202
        assert observed_publish_states == [("pending", "pending")]

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

    async def test_retry_job_requeues_failed_estimate_job(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Retry should route failed estimate jobs back through durable enqueue recovery."""
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
        ingest_job = await _get_job_for_file(str(uploaded["id"]))
        await worker_module.process_ingest_job(ingest_job.id)
        base_revision = await _get_latest_revision_for_file(uuid.UUID(uploaded["id"]))
        assert base_revision is not None

        estimate_job = await _create_quantity_takeoff_job(
            project_id=uuid.UUID(project["id"]),
            file_id=uuid.UUID(uploaded["id"]),
            base_revision_id=base_revision.id,
            parent_job_id=ingest_job.id,
            job_type=JobType.ESTIMATE.value,
            status="failed",
            attempts=1,
            max_attempts=3,
        )
        original = await _update_job(
            estimate_job.id,
            cancel_requested=True,
            error_code=ErrorCode.INTERNAL_ERROR.value,
            error_message="estimate worker unavailable",
            enqueue_status="pending",
            enqueue_attempts=2,
        )

        response = await async_client.post(f"/v1/jobs/{estimate_job.id}/retry")

        assert response.status_code == 202
        assert retried_job_ids == [str(estimate_job.id)]
        retried = await _get_job(estimate_job.id)
        assert retried.status == "pending"
        assert retried.attempts == original.attempts
        assert retried.cancel_requested is False
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
            unchanged.error_message == "Reprocess base revision became stale before finalization."
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

    async def test_retry_job_replays_attempt_limit_no_op_with_idempotency_key(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Retry should snapshot and replay attempt-limit no-op responses."""
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

        headers = {"Idempotency-Key": "retry-attempt-limit-replay"}
        first = await async_client.post(f"/v1/jobs/{job.id}/retry", headers=headers)
        second = await async_client.post(f"/v1/jobs/{job.id}/retry", headers=headers)

        assert first.status_code == 202
        assert second.status_code == 202
        assert second.json() == first.json()
        assert retried_job_ids == []
        unchanged = await _get_job(job.id)
        assert unchanged.status == "failed"
        assert unchanged.attempts == 3
        assert unchanged.max_attempts == 3

    @pytest.mark.parametrize(
        ("status", "attempts", "max_attempts", "error_code", "error_message"),
        [
            pytest.param(
                "failed",
                3,
                3,
                None,
                "maxed out",
                id="attempt-limit",
            ),
            pytest.param(
                "failed",
                1,
                3,
                ErrorCode.REVISION_CONFLICT.value,
                "Estimate base revision changed before finalization.",
                id="revision-conflict",
            ),
            pytest.param(
                "cancelled",
                1,
                3,
                ErrorCode.JOB_CANCELLED.value,
                None,
                id="cancelled",
            ),
        ],
    )
    async def test_retry_job_noops_for_non_retryable_estimate_states(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
        status: str,
        attempts: int,
        max_attempts: int,
        error_code: str | None,
        error_message: str | None,
    ) -> None:
        """Estimate retry should preserve lineage and skip non-retryable terminal states."""
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

        _, uploaded, ingest_job, base_revision, estimate_job = await _create_ready_estimate_job(
            async_client
        )
        original = await _update_job(
            estimate_job.id,
            status=status,
            attempts=attempts,
            max_attempts=max_attempts,
            error_code=error_code,
            error_message=error_message,
        )

        response = await async_client.post(f"/v1/jobs/{estimate_job.id}/retry")

        assert response.status_code == 202
        assert retried_job_ids == []
        unchanged = await _get_job(estimate_job.id)
        assert unchanged.status == original.status
        assert unchanged.attempts == original.attempts
        assert unchanged.max_attempts == original.max_attempts
        assert unchanged.error_code == original.error_code
        assert unchanged.error_message == original.error_message
        assert unchanged.project_id == original.project_id
        assert unchanged.file_id == uuid.UUID(uploaded["id"])
        assert unchanged.job_type == JobType.ESTIMATE.value
        assert unchanged.base_revision_id == base_revision.id
        assert unchanged.parent_job_id == ingest_job.id

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

            delete_task = asyncio.create_task(async_client.delete(f"/v1/projects/{project['id']}"))
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
