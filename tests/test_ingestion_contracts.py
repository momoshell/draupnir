"""Tests for ingestion adapter contracts and registry metadata."""

from __future__ import annotations

from dataclasses import fields
from pathlib import Path
from typing import Any, cast

from app.core.errors import ErrorCode
from app.ingestion.contracts import (
    AdapterAvailability,
    AdapterCapabilities,
    AdapterDescriptor,
    AdapterDiagnostic,
    AdapterFailureKind,
    AdapterResult,
    AdapterStatus,
    AdapterTimeout,
    AvailabilityReason,
    ConfidenceSummary,
    InputFamily,
    LicenseState,
    ProbeKind,
    ProbeObservation,
    ProbeRequirement,
    ProbeStatus,
    ProgressUpdate,
    ProvenanceRecord,
    UploadFormat,
    error_code_for_failure,
    input_families_for_upload_format,
)
from app.ingestion.registry import (
    descriptors_for_upload_format,
    evaluate_availability,
    get_registry,
    list_descriptors,
)


def test_upload_formats_cover_all_input_families() -> None:
    assert input_families_for_upload_format(UploadFormat.DWG) == (InputFamily.DWG,)
    assert input_families_for_upload_format(UploadFormat.DXF) == (InputFamily.DXF,)
    assert input_families_for_upload_format(UploadFormat.IFC) == (InputFamily.IFC,)
    assert input_families_for_upload_format(UploadFormat.PDF) == (
        InputFamily.PDF_VECTOR,
        InputFamily.PDF_RASTER,
    )


def test_adapter_result_exposes_required_trd_fields() -> None:
    result = AdapterResult(
        canonical={"entities": ({"kind": "line"},)},
        provenance=(
            ProvenanceRecord(
                stage="extract",
                adapter_key="ezdxf",
                source_ref="originals/file.dxf",
            ),
        ),
        confidence=ConfidenceSummary(score=0.97, review_required=False, basis="vector"),
    )

    assert "entities" in result.canonical
    assert result.provenance[0].adapter_key == "ezdxf"
    assert result.confidence is not None
    assert result.warnings == ()
    assert result.diagnostics == ()


def test_registry_is_static_and_covers_every_family() -> None:
    descriptors = list_descriptors()
    registry = get_registry()

    assert descriptors is list_descriptors()
    assert set(registry) == {
        InputFamily.DWG,
        InputFamily.DXF,
        InputFamily.IFC,
        InputFamily.PDF_VECTOR,
        InputFamily.PDF_RASTER,
    }
    assert registry[InputFamily.PDF_VECTOR].module == "app.ingestion.adapters.pymupdf"
    assert all(descriptor.adapter_key == descriptor.key for descriptor in descriptors)
    assert all(descriptor.input_formats == descriptor.upload_formats for descriptor in descriptors)
    assert all(descriptor.output_formats == ("canonical_json",) for descriptor in descriptors)
    assert all(descriptor.bounded_probe_ms > 0 for descriptor in descriptors)
    assert all(descriptor.confidence_range is not None for descriptor in descriptors)
    assert registry[InputFamily.PDF_RASTER].experimental is True
    assert registry[InputFamily.DWG].capabilities.can_read is True
    assert registry[InputFamily.DWG].capabilities.can_write is False
    assert registry[InputFamily.DWG].capabilities.extracts_geometry is True
    assert registry[InputFamily.DWG].capabilities.supports_quantity_hints is True
    assert registry[InputFamily.DWG].capabilities.supports_layout_selection is True
    assert registry[InputFamily.DWG].capabilities.supports_xref_resolution is True
    assert registry[InputFamily.IFC].capabilities.extracts_materials is True

    dwg_license_probe = next(
        probe for probe in registry[InputFamily.DWG].probes if probe.kind is ProbeKind.LICENSE
    )
    pdf_vector_license_probe = next(
        probe
        for probe in registry[InputFamily.PDF_VECTOR].probes
        if probe.kind is ProbeKind.LICENSE
    )
    pdf_raster_binary_probe = next(
        probe for probe in registry[InputFamily.PDF_RASTER].probes if probe.name == "tesseract"
    )

    assert dwg_license_probe.failure_status is AdapterStatus.UNAVAILABLE
    assert pdf_vector_license_probe.failure_status is AdapterStatus.UNAVAILABLE
    assert pdf_raster_binary_probe.failure_status is AdapterStatus.DEGRADED


def test_registry_rejects_mutation_and_preserves_cached_metadata() -> None:
    registry = get_registry()
    mutable_registry = cast(Any, registry)

    try:
        mutable_registry[InputFamily.DWG] = registry[InputFamily.DXF]
    except TypeError:
        pass
    else:
        raise AssertionError("Expected registry mutation to fail.")

    assert get_registry()[InputFamily.DWG].module == "app.ingestion.adapters.libredwg"


def test_adapter_capabilities_use_trd_aligned_field_names() -> None:
    capability_fields = {field.name for field in fields(AdapterCapabilities)}

    assert capability_fields == {
        "can_read",
        "can_write",
        "extracts_canonical",
        "extracts_provenance",
        "extracts_confidence",
        "extracts_warnings",
        "extracts_diagnostics",
        "extracts_geometry",
        "extracts_materials",
        "extracts_layers",
        "extracts_blocks",
        "extracts_text",
        "supports_exports",
        "supports_quantity_hints",
        "supports_layout_selection",
        "supports_xref_resolution",
    }


def test_availability_contract_uses_trd_status_and_reason_fields() -> None:
    availability_fields = {field.name for field in fields(AdapterAvailability)}

    assert {status.value for status in AdapterStatus} == {
        "available",
        "degraded",
        "unavailable",
    }
    assert {reason.value for reason in AvailabilityReason} == {
        "missing_binary",
        "missing_license",
        "probe_failed",
        "disabled_by_config",
        "unsupported_platform",
    }
    assert "availability_reason" in availability_fields
    assert "reason" not in availability_fields


def test_pdf_upload_format_returns_vector_and_raster_candidates() -> None:
    families = tuple(
        descriptor.family for descriptor in descriptors_for_upload_format(UploadFormat.PDF)
    )

    assert families == (InputFamily.PDF_VECTOR, InputFamily.PDF_RASTER)


def test_degraded_status_keeps_optional_binary_issue_visible() -> None:
    descriptor = AdapterDescriptor(
        key="test-adapter",
        family=InputFamily.PDF_RASTER,
        upload_formats=(UploadFormat.PDF,),
        display_name="Test Adapter",
        module="tests.fake",
        license_name="Proprietary",
        capabilities=AdapterCapabilities(),
        confidence_range=(0.3, 0.9),
        probes=(
            ProbeRequirement(
                kind=ProbeKind.BINARY,
                name="vectorizer",
                failure_status=AdapterStatus.DEGRADED,
                detail="Vectorizer binary is optional but recommended.",
            ),
        ),
    )

    availability = evaluate_availability(
        descriptor,
        observations=(
            ProbeObservation(
                kind=ProbeKind.BINARY,
                name="vectorizer",
                status=ProbeStatus.MISSING,
            ),
        ),
    )

    assert availability.status is AdapterStatus.DEGRADED
    assert availability.availability_reason is AvailabilityReason.MISSING_BINARY
    assert availability.license_state is LicenseState.NOT_REQUIRED
    assert availability.last_checked_at is not None
    assert availability.details == {
        "required_probe_count": 1,
        "observed_probe_count": 1,
        "missing_probe_count": 0,
        "issue_count": 1,
    }
    assert {(issue.kind, issue.name) for issue in availability.issues} == {
        (ProbeKind.BINARY, "vectorizer"),
    }


def test_missing_required_probe_observations_block_availability() -> None:
    descriptor = AdapterDescriptor(
        key="missing-probes",
        family=InputFamily.DWG,
        upload_formats=(UploadFormat.DWG,),
        display_name="Missing Probes",
        module="tests.fake",
        license_name="Restricted",
        capabilities=AdapterCapabilities(),
        confidence_range=(0.95, 1.0),
        probes=(
            ProbeRequirement(
                kind=ProbeKind.BINARY,
                name="dwgread",
                failure_status=AdapterStatus.UNAVAILABLE,
                detail="Binary is required.",
            ),
            ProbeRequirement(
                kind=ProbeKind.LICENSE,
                name="deployment-review",
                failure_status=AdapterStatus.UNAVAILABLE,
                detail="License review is required.",
            ),
        ),
    )

    availability = evaluate_availability(descriptor, observations=())

    assert availability.status is AdapterStatus.UNAVAILABLE
    assert availability.availability_reason is AvailabilityReason.MISSING_BINARY
    assert availability.license_state is LicenseState.UNKNOWN
    assert availability.details == {
        "required_probe_count": 2,
        "observed_probe_count": 0,
        "missing_probe_count": 2,
        "issue_count": 2,
    }
    assert [(issue.kind, issue.observed_status) for issue in availability.issues] == [
        (ProbeKind.BINARY, ProbeStatus.UNKNOWN),
        (ProbeKind.LICENSE, ProbeStatus.UNKNOWN),
    ]


def test_missing_required_license_probe_is_unavailable() -> None:
    descriptor = AdapterDescriptor(
        key="required-license",
        family=InputFamily.PDF_VECTOR,
        upload_formats=(UploadFormat.PDF,),
        display_name="Required License",
        module="tests.fake",
        license_name="Restricted",
        capabilities=AdapterCapabilities(),
        confidence_range=(0.6, 0.95),
        probes=(
            ProbeRequirement(
                kind=ProbeKind.LICENSE,
                name="deployment-review",
                failure_status=AdapterStatus.UNAVAILABLE,
                detail="License review is required before use.",
            ),
        ),
    )

    availability = evaluate_availability(
        descriptor,
        observations=(
            ProbeObservation(
                kind=ProbeKind.LICENSE,
                name="deployment-review",
                status=ProbeStatus.MISSING,
            ),
        ),
    )

    assert availability.status is AdapterStatus.UNAVAILABLE
    assert availability.availability_reason is AvailabilityReason.MISSING_LICENSE
    assert availability.license_state is LicenseState.MISSING
    assert availability.issues[0].detail == "License review is required before use."


def test_availability_defaults_license_state_when_no_license_probe_exists() -> None:
    descriptor = AdapterDescriptor(
        key="no-license-probe",
        family=InputFamily.DXF,
        upload_formats=(UploadFormat.DXF,),
        display_name="No License Probe",
        module="tests.fake",
        license_name="MIT",
        capabilities=AdapterCapabilities(extracts_geometry=True),
        confidence_range=(0.95, 1.0),
        probes=(
            ProbeRequirement(
                kind=ProbeKind.PYTHON_PACKAGE,
                name="ezdxf",
                failure_status=AdapterStatus.UNAVAILABLE,
                detail="Package is required.",
            ),
        ),
    )

    availability = evaluate_availability(
        descriptor,
        observations=(
            ProbeObservation(
                kind=ProbeKind.PYTHON_PACKAGE,
                name="ezdxf",
                status=ProbeStatus.AVAILABLE,
            ),
        ),
    )

    assert availability.status is AdapterStatus.AVAILABLE
    assert availability.availability_reason is None
    assert availability.license_state is LicenseState.NOT_REQUIRED


def test_contract_validation_types_exist() -> None:
    timeout = AdapterTimeout(seconds=5)
    progress = ProgressUpdate(stage="extract", completed=1, total=4, percent=0.25)
    diagnostic = AdapterDiagnostic(code="probe", message="timed", elapsed_ms=4.2)

    assert timeout.seconds == 5
    assert progress.stage == "extract"
    assert diagnostic.elapsed_ms == 4.2


def test_error_mapping_uses_shared_error_code_enum() -> None:
    expected = {
        AdapterFailureKind.UNSUPPORTED_FORMAT: ErrorCode.INPUT_UNSUPPORTED_FORMAT,
        AdapterFailureKind.UNAVAILABLE: ErrorCode.ADAPTER_UNAVAILABLE,
        AdapterFailureKind.TIMEOUT: ErrorCode.ADAPTER_TIMEOUT,
        AdapterFailureKind.FAILED: ErrorCode.ADAPTER_FAILED,
        AdapterFailureKind.CANCELLED: ErrorCode.JOB_CANCELLED,
        AdapterFailureKind.INTERNAL: ErrorCode.INTERNAL_ERROR,
    }

    assert {kind: error_code_for_failure(kind) for kind in AdapterFailureKind} == expected


def test_adapter_source_rejects_invalid_upload_family_pairing() -> None:
    from app.ingestion.contracts import AdapterSource

    try:
        AdapterSource(
            file_path=Path("drawing.dwg"),
            upload_format=UploadFormat.DWG,
            input_family=InputFamily.PDF_VECTOR,
        )
    except ValueError as exc:
        assert "is not valid" in str(exc)
    else:
        raise AssertionError("Expected invalid upload/family pairing to fail.")
