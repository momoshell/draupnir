"""Static adapter registry metadata for ingestion families."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from functools import lru_cache
from types import MappingProxyType

from .contracts import (
    AdapterAvailability,
    AdapterCapabilities,
    AdapterDescriptor,
    AdapterStatus,
    AvailabilityReason,
    InputFamily,
    JSONValue,
    LicenseState,
    ProbeIssue,
    ProbeKind,
    ProbeObservation,
    ProbeRequirement,
    ProbeStatus,
    UploadFormat,
    input_families_for_upload_format,
)

_STATUS_SEVERITY: Mapping[AdapterStatus, int] = {
    AdapterStatus.AVAILABLE: 0,
    AdapterStatus.DEGRADED: 1,
    AdapterStatus.UNAVAILABLE: 2,
}

_LICENSE_STATE_SEVERITY: Mapping[LicenseState, int] = {
    LicenseState.NOT_REQUIRED: 0,
    LicenseState.PRESENT: 1,
    LicenseState.UNKNOWN: 2,
    LicenseState.MISSING: 3,
}

_ADAPTER_DESCRIPTORS: tuple[AdapterDescriptor, ...] = (
    AdapterDescriptor(
        key="libredwg",
        family=InputFamily.DWG,
        upload_formats=(UploadFormat.DWG,),
        display_name="LibreDWG",
        module="app.ingestion.adapters.libredwg",
        license_name="GPL-3.0-or-later",
        capabilities=AdapterCapabilities(
        ),
        confidence_range=(0.2, 0.72),
        probes=(
            ProbeRequirement(
                kind=ProbeKind.BINARY,
                name="dwgread",
                failure_status=AdapterStatus.UNAVAILABLE,
                detail="LibreDWG binary is required to read DWG sources.",
            ),
            ProbeRequirement(
                kind=ProbeKind.LICENSE,
                name="libredwg-distribution-review",
                failure_status=AdapterStatus.UNAVAILABLE,
                detail="Distribution or on-prem bundling requires GPL review.",
            ),
        ),
        notes=(
            "Primary DWG adapter is isolated behind the ingestion contract.",
            "Current Phase 2 output is placeholder-only and does not expose "
            "real DWG extraction coverage yet.",
        ),
    ),
    AdapterDescriptor(
        key="ezdxf",
        family=InputFamily.DXF,
        upload_formats=(UploadFormat.DXF,),
        display_name="ezdxf",
        module="app.ingestion.adapters.ezdxf",
        license_name="MIT",
        capabilities=AdapterCapabilities(
            extracts_geometry=True,
            extracts_layers=True,
            extracts_blocks=True,
            extracts_text=True,
            supports_quantity_hints=True,
            supports_layout_selection=True,
            supports_xref_resolution=True,
        ),
        confidence_range=(0.95, 1.0),
        probes=(
            ProbeRequirement(
                kind=ProbeKind.PYTHON_PACKAGE,
                name="ezdxf",
                failure_status=AdapterStatus.UNAVAILABLE,
                detail="The ezdxf package is required to process DXF sources.",
            ),
        ),
    ),
    AdapterDescriptor(
        key="ifcopenshell",
        family=InputFamily.IFC,
        upload_formats=(UploadFormat.IFC,),
        display_name="IfcOpenShell semantic IFC adapter",
        module="app.ingestion.adapters.ifcopenshell",
        license_name="LGPL-3.0-or-later",
        capabilities=AdapterCapabilities(
            extracts_materials=True,
            extracts_layers=True,
            supports_quantity_hints=True,
        ),
        confidence_range=(0.2, 0.55),
        probes=(
            ProbeRequirement(
                kind=ProbeKind.PYTHON_PACKAGE,
                name="ifcopenshell",
                failure_status=AdapterStatus.UNAVAILABLE,
                detail="The IfcOpenShell package is required to process IFC sources.",
            ),
        ),
        notes=("Semantic-only IFC extraction; tessellation and shape creation are disabled.",),
    ),
    AdapterDescriptor(
        key="pymupdf",
        family=InputFamily.PDF_VECTOR,
        upload_formats=(UploadFormat.PDF,),
        display_name="PyMuPDF",
        module="app.ingestion.adapters.pymupdf",
        license_name="AGPL-3.0-or-later OR commercial",
        capabilities=AdapterCapabilities(
            extracts_geometry=True,
            extracts_text=True,
            supports_quantity_hints=True,
            supports_layout_selection=True,
        ),
        confidence_range=(0.6, 0.95),
        probes=(
            ProbeRequirement(
                kind=ProbeKind.PYTHON_PACKAGE,
                name="fitz",
                failure_status=AdapterStatus.UNAVAILABLE,
                detail="PyMuPDF is required for vector PDF extraction.",
            ),
            ProbeRequirement(
                kind=ProbeKind.LICENSE,
                name="pymupdf-deployment-review",
                failure_status=AdapterStatus.UNAVAILABLE,
                detail="Commercial or AGPL compliance review is required for deployment.",
            ),
        ),
    ),
    AdapterDescriptor(
        key="vtracer_tesseract",
        family=InputFamily.PDF_RASTER,
        upload_formats=(UploadFormat.PDF,),
        display_name="VTracer + Tesseract",
        module="app.ingestion.adapters.vtracer_tesseract",
        license_name="MIT + Apache-2.0",
        capabilities=AdapterCapabilities(),
        experimental=True,
        confidence_range=(0.3, 0.6),
        probes=(
            ProbeRequirement(
                kind=ProbeKind.PYTHON_PACKAGE,
                name="vtracer",
                failure_status=AdapterStatus.UNAVAILABLE,
                detail="VTracer is required for raster vectorization.",
            ),
            ProbeRequirement(
                kind=ProbeKind.BINARY,
                name="tesseract",
                failure_status=AdapterStatus.DEGRADED,
                detail="Tesseract enables OCR and confidence scoring for raster PDFs.",
            ),
        ),
        notes=(
            "Experimental raster scaffold only; vectorization, OCR, and "
            "quantity hints remain deferred.",
        ),
    ),
)


def _merge_status(left: AdapterStatus, right: AdapterStatus) -> AdapterStatus:
    if _STATUS_SEVERITY[right] > _STATUS_SEVERITY[left]:
        return right
    return left


def _merge_license_state(left: LicenseState, right: LicenseState) -> LicenseState:
    if _LICENSE_STATE_SEVERITY[right] > _LICENSE_STATE_SEVERITY[left]:
        return right
    return left


def _license_state_for_observation(observation: ProbeObservation) -> LicenseState:
    if observation.status is ProbeStatus.AVAILABLE:
        return LicenseState.PRESENT
    if observation.status is ProbeStatus.MISSING:
        return LicenseState.MISSING
    return LicenseState.UNKNOWN


def _availability_reason_for_issue(issue: ProbeIssue) -> AvailabilityReason:
    if issue.kind is ProbeKind.BINARY:
        return AvailabilityReason.MISSING_BINARY
    if issue.kind is ProbeKind.LICENSE:
        return AvailabilityReason.MISSING_LICENSE
    return AvailabilityReason.PROBE_FAILED


def _primary_availability_reason(issues: list[ProbeIssue]) -> AvailabilityReason | None:
    primary_issue: ProbeIssue | None = None
    for issue in issues:
        if primary_issue is None:
            primary_issue = issue
            continue
        if _STATUS_SEVERITY[issue.adapter_status] > _STATUS_SEVERITY[primary_issue.adapter_status]:
            primary_issue = issue
    if primary_issue is None:
        return None
    return _availability_reason_for_issue(primary_issue)


@lru_cache(maxsize=1)
def list_descriptors() -> tuple[AdapterDescriptor, ...]:
    """Return the static registry descriptors without importing adapters."""

    return _ADAPTER_DESCRIPTORS


@lru_cache(maxsize=1)
def get_registry() -> Mapping[InputFamily, AdapterDescriptor]:
    """Return registry descriptors keyed by normalized input family."""

    return MappingProxyType(
        {descriptor.family: descriptor for descriptor in list_descriptors()}
    )


def get_descriptor(family: InputFamily) -> AdapterDescriptor:
    """Return the descriptor registered for a normalized input family."""

    return get_registry()[family]


def descriptors_for_upload_format(upload_format: UploadFormat) -> tuple[AdapterDescriptor, ...]:
    """Return candidate descriptors for a top-level upload format."""

    return tuple(
        get_descriptor(family)
        for family in input_families_for_upload_format(upload_format)
    )


def evaluate_availability(
    descriptor: AdapterDescriptor,
    observations: tuple[ProbeObservation, ...],
    *,
    last_checked_at: datetime | None = None,
    details: Mapping[str, JSONValue] | None = None,
    probe_elapsed_ms: float | None = None,
) -> AdapterAvailability:
    """Resolve adapter availability from probe observations."""

    observed_by_key = {(item.kind, item.name): item for item in observations}
    status = AdapterStatus.AVAILABLE
    license_state = LicenseState.NOT_REQUIRED
    issues: list[ProbeIssue] = []
    missing_probe_count = 0

    for requirement in descriptor.probes:
        observation = observed_by_key.get((requirement.kind, requirement.name))
        if observation is None:
            missing_probe_count += 1
            issues.append(
                ProbeIssue(
                    kind=requirement.kind,
                    name=requirement.name,
                    observed_status=ProbeStatus.UNKNOWN,
                    adapter_status=requirement.failure_status,
                    detail=(
                        "Required probe observation missing for "
                        f"{requirement.kind.value} '{requirement.name}'. "
                        f"{requirement.detail}"
                    ),
                )
            )
            if requirement.kind is ProbeKind.LICENSE:
                license_state = _merge_license_state(
                    license_state,
                    LicenseState.UNKNOWN,
                )
            status = _merge_status(status, requirement.failure_status)
            continue

        if requirement.kind is ProbeKind.LICENSE:
            license_state = _merge_license_state(
                license_state,
                _license_state_for_observation(observation),
            )
        if observation.status is ProbeStatus.AVAILABLE:
            continue

        issues.append(
            ProbeIssue(
                kind=requirement.kind,
                name=requirement.name,
                observed_status=observation.status,
                adapter_status=requirement.failure_status,
                detail=observation.detail or requirement.detail,
            )
        )
        status = _merge_status(status, requirement.failure_status)

    resolved_details = details or {
        "required_probe_count": len(descriptor.probes),
        "observed_probe_count": len(observations),
        "missing_probe_count": missing_probe_count,
        "issue_count": len(issues),
    }
    availability_reason = _primary_availability_reason(issues)

    return AdapterAvailability(
        status=status,
        availability_reason=availability_reason,
        license_state=license_state,
        issues=tuple(issues),
        observed=observations,
        last_checked_at=last_checked_at or datetime.now(UTC),
        details=resolved_details,
        probe_elapsed_ms=probe_elapsed_ms,
    )


__all__ = [
    "descriptors_for_upload_format",
    "evaluate_availability",
    "get_descriptor",
    "get_registry",
    "list_descriptors",
]
