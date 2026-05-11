"""Reusable contract harness for ingestion adapter tests."""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from uuid import UUID, uuid4

from app.ingestion.contracts import (
    AdapterAvailability,
    AdapterDescriptor,
    AdapterDiagnostic,
    AdapterExecutionOptions,
    AdapterResult,
    AdapterSource,
    AdapterStatus,
    AdapterTimeout,
    AdapterWarning,
    ConfidenceSummary,
    IngestionAdapter,
    InputFamily,
    JSONValue,
    ProvenanceRecord,
    UploadFormat,
)
from app.ingestion.finalization import (
    IngestFinalizationContext,
    IngestFinalizationPayload,
    build_ingest_finalization_payload,
)

_DEFAULT_CONTRACT_TIMEOUT = AdapterTimeout(seconds=0.5)
_DEFAULT_FAILURE_TIMEOUT = AdapterTimeout(seconds=0.01)
_CONTRACT_ENTITY_SCHEMA_VERSION = "0.1"
_NULLABLE_LINKAGE_FIELDS = (
    "drawing_revision_id",
    "source_file_id",
    "layout_ref",
    "layer_ref",
    "block_ref",
    "parent_entity_ref",
)


@dataclass(frozen=True, slots=True)
class ContractFinalizationExpectation:
    """Expected finalization posture produced from an adapter run."""

    validation_status: str
    review_state: str
    quantity_gate: str
    warning_codes: tuple[str, ...] = ()
    diagnostic_codes: tuple[str, ...] = ()


class AdapterContractFailureKind(StrEnum):
    """Normalized failure classes for adapter contract tests."""

    UNSUPPORTED = "unsupported"
    DEPENDENCY_MISSING = "dependency_missing"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class AdapterContractFailure:
    """Captured failure from adapter execution in harness tests."""

    kind: AdapterContractFailureKind
    error: BaseException


def build_contract_source(
    *,
    file_path: Path,
    upload_format: UploadFormat = UploadFormat.DXF,
    input_family: InputFamily = InputFamily.DXF,
    media_type: str | None = None,
    original_name: str = "fixture.dat",
) -> AdapterSource:
    """Build a typed adapter source for harness execution."""

    return AdapterSource(
        file_path=file_path,
        upload_format=upload_format,
        input_family=input_family,
        media_type=media_type,
        original_name=original_name,
    )


def build_complete_canonical(*, include_pdf_scale: bool = False) -> dict[str, JSONValue]:
    """Return a canonical payload that satisfies baseline validator checks."""

    canonical: dict[str, JSONValue] = {
        "schema_version": _CONTRACT_ENTITY_SCHEMA_VERSION,
        "canonical_entity_schema_version": _CONTRACT_ENTITY_SCHEMA_VERSION,
        "units": {"normalized": "meter"},
        "coordinate_system": {"name": "local"},
        "layouts": ({"name": "Model"},),
        "layers": ({"name": "A-WALL"},),
        "blocks": (),
        "entities": (
            {
                "entity_id": "contract-entity-1",
                "entity_type": "line",
                "entity_schema_version": _CONTRACT_ENTITY_SCHEMA_VERSION,
                "id": "contract-entity-1",
                "kind": "line",
                "layout": "Model",
                "layer": "A-WALL",
                "start": {"x": 0.0, "y": 0.0},
                "end": {"x": 10.0, "y": 0.0},
                "geometry": {
                    "start": {"x": 0.0, "y": 0.0},
                    "end": {"x": 10.0, "y": 0.0},
                    "bbox": {"x_min": 0.0, "y_min": 0.0, "x_max": 10.0, "y_max": 0.0},
                    "units": {"normalized": "meter"},
                    "geometry_summary": {
                        "kind": "line_segment",
                        "length": 10.0,
                        "vertex_count": 2,
                    },
                },
                "properties": {
                    "source_type": "LINE",
                    "source_handle": "ABC",
                    "quantity_hints": {"length": 10.0, "count": 1.0},
                    "adapter_native": {"contract": {"layer": "A-WALL"}},
                },
                "provenance": {
                    "source_entity_ref": "entities.contract-entity-1",
                    "normalized_source_hash": "0" * 64,
                },
                "confidence": 0.99,
                "drawing_revision_id": None,
                "source_file_id": None,
                "layout_ref": "Model",
                "layer_ref": "A-WALL",
                "block_ref": None,
                "parent_entity_ref": None,
            },
        ),
        "xrefs": (),
    }
    if include_pdf_scale:
        canonical["pdf_scale"] = {"ratio": "1:100", "status": "confirmed"}

    return canonical


def build_result(
    *,
    adapter_key: str,
    score: float,
    canonical: dict[str, JSONValue],
    review_required: bool = False,
    warnings: tuple[AdapterWarning, ...] = (),
    diagnostics: tuple[AdapterDiagnostic, ...] = (),
) -> AdapterResult:
    """Build a well-typed adapter result for contract tests."""

    return AdapterResult(
        canonical=canonical,
        provenance=(
            ProvenanceRecord(
                stage="extract",
                adapter_key=adapter_key,
                source_ref="originals/source.dat",
            ),
        ),
        confidence=ConfidenceSummary(
            score=score,
            review_required=review_required,
            basis="contract",
        ),
        warnings=warnings,
        diagnostics=diagnostics,
    )


def assert_adapter_result_contract(
    result: AdapterResult,
    *,
    expected_adapter_key: str,
    expected_warning_codes: tuple[str, ...],
    expected_diagnostic_codes: tuple[str, ...],
) -> None:
    """Assert canonical adapter result payload shape and diagnostic metadata."""

    if "entities" not in result.canonical:
        raise AssertionError("Adapter canonical payload must include an entities collection.")
    schema_version = result.canonical.get("canonical_entity_schema_version")
    if not isinstance(schema_version, str) or not schema_version.strip():
        raise AssertionError(
            "Adapter canonical payload must include canonical_entity_schema_version."
        )
    if not result.provenance:
        raise AssertionError("Adapter result must include provenance records.")
    if result.confidence is None:
        raise AssertionError("Adapter result must include confidence metadata.")

    entities = result.canonical["entities"]
    if not isinstance(entities, tuple):
        raise AssertionError("Adapter canonical entities must be emitted as a tuple.")
    if not entities:
        _assert_empty_entity_collection_reason(result.canonical)
    else:
        _assert_entity_envelopes(entities, expected_schema_version=schema_version)

    for record in result.provenance:
        if not record.stage or not record.source_ref:
            raise AssertionError("Provenance records must include stage and source reference.")
        if record.adapter_key != expected_adapter_key:
            raise AssertionError("Provenance adapter key must match the descriptor key.")

    warning_codes = tuple(warning.code for warning in result.warnings)
    diagnostic_codes = tuple(diagnostic.code for diagnostic in result.diagnostics)
    if Counter(warning_codes) != Counter(expected_warning_codes):
        raise AssertionError("Adapter warning codes did not match expected contract output.")
    if Counter(diagnostic_codes) != Counter(expected_diagnostic_codes):
        raise AssertionError("Adapter diagnostic codes did not match expected contract output.")


def _assert_empty_entity_collection_reason(canonical: Mapping[str, JSONValue]) -> None:
    metadata = canonical.get("metadata")
    if not isinstance(metadata, dict):
        raise AssertionError(
            "Canonical payloads with empty entities must include metadata with a reason."
        )
    reason = metadata.get("empty_entities_reason")
    if not isinstance(reason, str) or not reason.strip():
        raise AssertionError(
            "Canonical payloads with empty entities must include metadata.empty_entities_reason."
        )
    if _requires_placeholder_semantics(metadata, reason=reason):
        _assert_placeholder_semantics(metadata, reason=reason)


def _requires_placeholder_semantics(metadata: dict[str, JSONValue], *, reason: str) -> bool:
    adapter_mode = metadata.get("adapter_mode")
    if adapter_mode in {"placeholder", "sparse_placeholder"}:
        return True

    return reason in {
        "placeholder_canonical_no_entity_mapping",
        "raster_vectorization_deferred",
    }


def _assert_placeholder_semantics(metadata: dict[str, JSONValue], *, reason: str) -> None:
    placeholder_semantics = metadata.get("placeholder_semantics")
    if not isinstance(placeholder_semantics, dict):
        raise AssertionError(
            "Placeholder or sparse canonical payloads must include metadata.placeholder_semantics."
        )

    status = placeholder_semantics.get("status")
    if not isinstance(status, str) or not status.strip():
        raise AssertionError(
            "Placeholder semantics must include a non-empty placeholder status."
        )
    if placeholder_semantics.get("review_required") is not True:
        raise AssertionError("Placeholder semantics must explicitly require review.")
    if placeholder_semantics.get("quantity_gate") != "review_gated":
        raise AssertionError(
            "Placeholder semantics must explicitly publish quantity_gate='review_gated'."
        )
    if placeholder_semantics.get("reason") != reason:
        raise AssertionError(
            "Placeholder semantics reason must match metadata.empty_entities_reason."
        )

    coverage = placeholder_semantics.get("coverage")
    if not isinstance(coverage, dict) or not coverage:
        raise AssertionError(
            "Placeholder semantics must describe extraction coverage limitations."
        )


def _assert_entity_envelopes(
    entities: tuple[JSONValue, ...],
    *,
    expected_schema_version: str,
) -> None:
    seen_entity_ids: set[str] = set()
    for entity_payload in entities:
        if not isinstance(entity_payload, dict):
            raise AssertionError("Canonical entities must be JSON object payloads.")

        entity_id = entity_payload.get("entity_id")
        if not isinstance(entity_id, str) or not entity_id.strip():
            raise AssertionError("Canonical entities must include a stable entity_id.")
        if entity_id in seen_entity_ids:
            raise AssertionError("Canonical entity_ids must be unique within a payload.")
        seen_entity_ids.add(entity_id)

        entity_type = entity_payload.get("entity_type")
        if not isinstance(entity_type, str) or not entity_type.strip():
            raise AssertionError("Canonical entities must include a stable entity_type.")

        entity_schema_version = entity_payload.get("entity_schema_version")
        if entity_schema_version != expected_schema_version:
            raise AssertionError(
                "Canonical entity schema versions must match canonical_entity_schema_version."
            )

        provenance = entity_payload.get("provenance")
        if not isinstance(provenance, dict) or not provenance:
            raise AssertionError("Canonical entities must include provenance metadata.")
        if not _has_stable_entity_provenance(provenance):
            raise AssertionError(
                "Canonical entity provenance must include a stable source locator."
            )

        _assert_required_nullable_linkage_fields(entity_payload)
        _assert_entity_confidence(entity_payload.get("confidence"))
        _assert_geometry_reason_if_required(entity_payload)


def _has_stable_entity_provenance(provenance: dict[str, JSONValue]) -> bool:
    stable_keys = (
        "source_entity_ref",
        "normalized_source_hash",
        "ifc_step_id",
        "ifc_global_id",
        "page_number",
        "drawing_index",
        "source",
    )
    for key in stable_keys:
        value = provenance.get(key)
        if isinstance(value, str) and value.strip():
            return True
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return True
    return False


def _assert_entity_confidence(confidence: JSONValue) -> None:
    if isinstance(confidence, (int, float)) and not isinstance(confidence, bool):
        score = float(confidence)
    elif isinstance(confidence, dict):
        score_value = confidence.get("score")
        if not isinstance(score_value, (int, float)) or isinstance(score_value, bool):
            raise AssertionError(
                "Canonical entity confidence objects must include a numeric score."
            )
        score = float(score_value)
    else:
        raise AssertionError("Canonical entities must include confidence metadata.")

    if not 0.0 <= score <= 1.0:
        raise AssertionError("Canonical entity confidence scores must be between 0.0 and 1.0.")


def _assert_required_nullable_linkage_fields(entity_payload: dict[str, JSONValue]) -> None:
    for field_name in _NULLABLE_LINKAGE_FIELDS:
        if field_name not in entity_payload:
            raise AssertionError(
                f"Canonical entities must include nullable linkage field '{field_name}'."
            )


def _assert_geometry_reason_if_required(entity_payload: dict[str, JSONValue]) -> None:
    geometry = entity_payload.get("geometry")
    if not isinstance(geometry, dict):
        reason = entity_payload.get("geometry_reason")
        if not isinstance(reason, str) or not reason.strip():
            raise AssertionError(
                "Geometry-less canonical entities must include an explicit geometry reason."
            )
        return

    if not _geometry_reason_required(geometry):
        return

    geometry_summary = geometry.get("geometry_summary")
    reason_candidates = (
        geometry.get("reason"),
        geometry_summary.get("reason") if isinstance(geometry_summary, dict) else None,
        entity_payload.get("geometry_reason"),
    )
    if not any(isinstance(reason, str) and reason.strip() for reason in reason_candidates):
        raise AssertionError(
            "Geometry-less canonical entities must include an explicit geometry reason."
        )


def _geometry_reason_required(geometry: dict[str, JSONValue]) -> bool:
    status = geometry.get("status")
    if status in {"absent", "missing", "placeholder"}:
        return True

    geometry_summary = geometry.get("geometry_summary")
    if isinstance(geometry_summary, dict):
        geometry_kind = geometry_summary.get("kind")
        if geometry_kind in {"none", "placeholder", "unknown"}:
            return True

    return geometry.get("bbox") is None and not _has_concrete_geometry_payload(geometry)


def _has_concrete_geometry_payload(geometry: dict[str, JSONValue]) -> bool:
    metadata_keys = {
        "bbox",
        "units",
        "status",
        "reason",
        "geometry_summary",
        "summary",
        "kind",
        "coordinate_space",
        "unit",
    }
    for key, value in geometry.items():
        if key in metadata_keys:
            continue
        if value is None:
            continue
        if isinstance(value, (tuple, list, dict)) and not value:
            continue
        return True
    return False


async def exercise_adapter_contract(
    adapter: IngestionAdapter,
    *,
    source: AdapterSource,
    input_family: InputFamily,
    expectation: ContractFinalizationExpectation,
    adapter_key: str,
    adapter_version: str = "contract-test-1.0",
    timeout: AdapterTimeout = _DEFAULT_CONTRACT_TIMEOUT,
    generated_at: datetime | None = None,
    job_id: UUID | None = None,
    file_id: UUID | None = None,
) -> IngestFinalizationPayload:
    """Execute an adapter and assert the shared ingestion/finalization contract."""

    availability = adapter.probe()
    if availability.status is AdapterStatus.UNAVAILABLE:
        raise AssertionError("Contract harness requires a probeable adapter.")

    options = AdapterExecutionOptions(timeout=timeout)
    result = await asyncio.wait_for(adapter.ingest(source, options), timeout=timeout.seconds)
    assert_adapter_result_contract(
        result,
        expected_adapter_key=adapter_key,
        expected_warning_codes=expectation.warning_codes,
        expected_diagnostic_codes=expectation.diagnostic_codes,
    )

    payload = build_ingest_finalization_payload(
        IngestFinalizationContext(
            job_id=job_id or uuid4(),
            file_id=file_id or uuid4(),
            extraction_profile_id=None,
            initial_job_id=None,
            input_family=input_family,
            adapter_key=adapter_key,
            adapter_version=adapter_version,
        ),
        result=result,
        generated_at=generated_at or datetime.now(UTC),
    )

    if payload.validation_status != expectation.validation_status:
        raise AssertionError("Validation status did not match expected contract output.")
    if payload.review_state != expectation.review_state:
        raise AssertionError("Review state did not match expected contract output.")
    if payload.quantity_gate != expectation.quantity_gate:
        raise AssertionError("Quantity gate did not match expected contract output.")

    return payload


async def exercise_adapter_failure(
    adapter: IngestionAdapter,
    *,
    source: AdapterSource,
    timeout: AdapterTimeout = _DEFAULT_FAILURE_TIMEOUT,
    options: AdapterExecutionOptions | None = None,
) -> AdapterContractFailure:
    """Execute a failing adapter and classify the failure kind."""

    execution_options = options or AdapterExecutionOptions(timeout=timeout)
    timeout_seconds = (
        execution_options.timeout.seconds
        if execution_options.timeout is not None
        else timeout.seconds
    )
    try:
        await asyncio.wait_for(
            adapter.ingest(source, execution_options),
            timeout=timeout_seconds,
        )
    except TimeoutError as error:
        return AdapterContractFailure(kind=AdapterContractFailureKind.TIMEOUT, error=error)
    except asyncio.CancelledError as error:
        return AdapterContractFailure(kind=AdapterContractFailureKind.CANCELLED, error=error)
    except ModuleNotFoundError as error:
        return AdapterContractFailure(
            kind=AdapterContractFailureKind.DEPENDENCY_MISSING,
            error=error,
        )
    except NotImplementedError as error:
        return AdapterContractFailure(kind=AdapterContractFailureKind.UNSUPPORTED, error=error)
    except Exception as error:
        return AdapterContractFailure(kind=AdapterContractFailureKind.FAILED, error=error)

    raise AssertionError("Expected adapter execution to fail in contract failure harness.")


def build_descriptor(*, key: str, family: InputFamily) -> AdapterDescriptor:
    """Create a minimal descriptor for fake contract adapters."""

    upload_format = {
        InputFamily.DWG: UploadFormat.DWG,
        InputFamily.DXF: UploadFormat.DXF,
        InputFamily.IFC: UploadFormat.IFC,
        InputFamily.PDF_VECTOR: UploadFormat.PDF,
        InputFamily.PDF_RASTER: UploadFormat.PDF,
    }[family]
    return AdapterDescriptor(
        key=key,
        family=family,
        upload_formats=(upload_format,),
        display_name=f"{key} adapter",
        module="tests.fake.adapter",
        license_name="MIT",
        adapter_version="contract-test-1.0",
    )


def available_probe() -> AdapterAvailability:
    """Return a standard available probe result for fake adapters."""

    return AdapterAvailability(status=AdapterStatus.AVAILABLE)
