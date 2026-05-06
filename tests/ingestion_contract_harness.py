"""Reusable contract harness for ingestion adapter tests."""

from __future__ import annotations

import asyncio
from collections import Counter
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
        "units": {"normalized": "meter"},
        "coordinate_system": {"name": "local"},
        "layouts": ({"name": "Model"},),
        "layers": ({"name": "A-WALL"},),
        "blocks": (),
        "entities": (
            {
                "kind": "line",
                "layer": "A-WALL",
                "start": {"x": 0.0, "y": 0.0},
                "end": {"x": 10.0, "y": 0.0},
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
    if not result.provenance:
        raise AssertionError("Adapter result must include provenance records.")
    if result.confidence is None:
        raise AssertionError("Adapter result must include confidence metadata.")

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
