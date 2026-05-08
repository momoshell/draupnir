"""Contracts for ingestion adapters and capability registry metadata."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Protocol

from app.core.errors import ErrorCode

type JSONScalar = str | int | float | bool | None
type JSONValue = JSONScalar | Mapping[str, JSONValue] | tuple[JSONValue, ...]


class UploadFormat(StrEnum):
    """Top-level upload formats accepted by the ingestion surface."""

    DWG = "dwg"
    DXF = "dxf"
    IFC = "ifc"
    PDF = "pdf"


class InputFamily(StrEnum):
    """Normalized input families used to select a specific adapter path."""

    DWG = "dwg"
    DXF = "dxf"
    IFC = "ifc"
    PDF_VECTOR = "pdf_vector"
    PDF_RASTER = "pdf_raster"


INPUT_FAMILIES_BY_UPLOAD_FORMAT: Mapping[UploadFormat, tuple[InputFamily, ...]] = {
    UploadFormat.DWG: (InputFamily.DWG,),
    UploadFormat.DXF: (InputFamily.DXF,),
    UploadFormat.IFC: (InputFamily.IFC,),
    UploadFormat.PDF: (InputFamily.PDF_VECTOR, InputFamily.PDF_RASTER),
}


class AdapterStatus(StrEnum):
    """Runtime availability for an adapter descriptor or probe result."""

    AVAILABLE = "available"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"


class AvailabilityReason(StrEnum):
    """Primary availability reason exposed by the ingestion contract."""

    MISSING_BINARY = "missing_binary"
    MISSING_LICENSE = "missing_license"
    PROBE_FAILED = "probe_failed"
    DISABLED_BY_CONFIG = "disabled_by_config"
    UNSUPPORTED_PLATFORM = "unsupported_platform"


class ProbeKind(StrEnum):
    """Kinds of external requirements checked by adapter probes."""

    BINARY = "binary"
    LICENSE = "license"
    PYTHON_PACKAGE = "python_package"
    SERVICE = "service"


class ProbeStatus(StrEnum):
    """Observed status for an individual probe."""

    AVAILABLE = "available"
    MISSING = "missing"
    UNKNOWN = "unknown"


class LicenseState(StrEnum):
    """Resolved license posture surfaced with adapter availability."""

    PRESENT = "present"
    MISSING = "missing"
    NOT_REQUIRED = "not_required"
    UNKNOWN = "unknown"


class AdapterFailureKind(StrEnum):
    """Failure categories that map onto the API error contract."""

    UNSUPPORTED_FORMAT = "unsupported_format"
    UNAVAILABLE = "unavailable"
    TIMEOUT = "timeout"
    FAILED = "failed"
    CANCELLED = "cancelled"
    INTERNAL = "internal"


class AdapterUnavailableError(Exception):
    """Sanitized availability failure adapters can raise during preflight/execute."""

    def __init__(
        self,
        availability_reason: AvailabilityReason,
        *,
        detail: str | None = None,
    ) -> None:
        super().__init__("Adapter reported unavailable.")
        self.availability_reason = availability_reason
        self.detail = detail


def input_families_for_upload_format(upload_format: UploadFormat) -> tuple[InputFamily, ...]:
    """Return the normalized families that can satisfy an upload format."""

    return INPUT_FAMILIES_BY_UPLOAD_FORMAT[upload_format]


def error_code_for_failure(failure_kind: AdapterFailureKind) -> ErrorCode:
    """Map adapter contract failures onto the shared API error contract."""

    if failure_kind is AdapterFailureKind.UNSUPPORTED_FORMAT:
        return ErrorCode.INPUT_UNSUPPORTED_FORMAT
    if failure_kind is AdapterFailureKind.UNAVAILABLE:
        return ErrorCode.ADAPTER_UNAVAILABLE
    if failure_kind is AdapterFailureKind.TIMEOUT:
        return ErrorCode.ADAPTER_TIMEOUT
    if failure_kind is AdapterFailureKind.FAILED:
        return ErrorCode.ADAPTER_FAILED
    if failure_kind is AdapterFailureKind.CANCELLED:
        return ErrorCode.JOB_CANCELLED
    return ErrorCode.INTERNAL_ERROR


@dataclass(frozen=True, slots=True)
class AdapterTimeout:
    """Deadline contract passed into adapter execution."""

    seconds: float

    def __post_init__(self) -> None:
        if self.seconds <= 0:
            raise ValueError("Adapter timeout must be greater than zero.")


@dataclass(frozen=True, slots=True)
class ProgressUpdate:
    """Progress payload emitted by long-running adapters."""

    stage: str
    message: str | None = None
    completed: int | None = None
    total: int | None = None
    percent: float | None = None

    def __post_init__(self) -> None:
        if self.completed is not None and self.completed < 0:
            raise ValueError("Progress completed count cannot be negative.")
        if self.total is not None and self.total < 0:
            raise ValueError("Progress total count cannot be negative.")
        if self.completed is not None and self.total is not None and self.completed > self.total:
            raise ValueError("Progress completed count cannot exceed total.")
        if self.percent is not None and not 0 <= self.percent <= 1:
            raise ValueError("Progress percent must be normalized to the 0..1 range.")


class CancellationHandle(Protocol):
    """Cancellation token contract shared with jobs/workers."""

    def is_cancelled(self) -> bool:
        """Return whether the caller requested cancellation."""


type ProgressCallback = Callable[[ProgressUpdate], None]


@dataclass(frozen=True, slots=True)
class AdapterExecutionOptions:
    """Execution-time controls for an adapter run."""

    timeout: AdapterTimeout | None = None
    cancellation: CancellationHandle | None = None
    on_progress: ProgressCallback | None = None


@dataclass(frozen=True, slots=True)
class AdapterWarning:
    """Non-fatal warning surfaced by an adapter run."""

    code: str
    message: str
    details: JSONValue | None = None


@dataclass(frozen=True, slots=True)
class AdapterDiagnostic:
    """Structured diagnostic emitted for troubleshooting or review."""

    code: str
    message: str
    details: JSONValue | None = None
    elapsed_ms: float | None = None

    def __post_init__(self) -> None:
        if self.elapsed_ms is not None and self.elapsed_ms < 0:
            raise ValueError("Diagnostic elapsed time cannot be negative.")


@dataclass(frozen=True, slots=True)
class ConfidenceSummary:
    """Confidence signal emitted by an adapter result."""

    score: float | None
    review_required: bool
    basis: str | None = None

    def __post_init__(self) -> None:
        if self.score is not None and not 0 <= self.score <= 1:
            raise ValueError("Confidence score must be normalized to the 0..1 range.")


@dataclass(frozen=True, slots=True)
class ProvenanceRecord:
    """Lineage emitted by adapter stages while building canonical output."""

    stage: str
    adapter_key: str
    source_ref: str
    details: JSONValue | None = None


@dataclass(frozen=True, slots=True)
class AdapterResult:
    """Canonical adapter output required by the TRD ingestion contract."""

    canonical: Mapping[str, JSONValue]
    provenance: tuple[ProvenanceRecord, ...]
    confidence: ConfidenceSummary | None
    warnings: tuple[AdapterWarning, ...] = ()
    diagnostics: tuple[AdapterDiagnostic, ...] = ()


@dataclass(frozen=True, slots=True)
class AdapterSource:
    """Immutable source reference handed to an adapter implementation."""

    file_path: Path
    upload_format: UploadFormat
    input_family: InputFamily
    media_type: str | None = None
    original_name: str | None = None

    def __post_init__(self) -> None:
        if self.input_family not in input_families_for_upload_format(self.upload_format):
            raise ValueError(
                f"Input family {self.input_family!s} is not valid for {self.upload_format!s}."
            )


@dataclass(frozen=True, slots=True)
class AdapterCapabilities:
    """Static capabilities exposed by a registry descriptor."""

    can_read: bool = True
    can_write: bool = False
    extracts_canonical: bool = True
    extracts_provenance: bool = True
    extracts_confidence: bool = True
    extracts_warnings: bool = True
    extracts_diagnostics: bool = True
    extracts_geometry: bool = False
    extracts_materials: bool = False
    extracts_layers: bool = False
    extracts_blocks: bool = False
    extracts_text: bool = False
    supports_exports: bool = False
    supports_quantity_hints: bool = False
    supports_layout_selection: bool = False
    supports_xref_resolution: bool = False

    @property
    def canonical(self) -> bool:
        return self.extracts_canonical

    @property
    def provenance(self) -> bool:
        return self.extracts_provenance

    @property
    def confidence(self) -> bool:
        return self.extracts_confidence

    @property
    def warnings(self) -> bool:
        return self.extracts_warnings

    @property
    def diagnostics(self) -> bool:
        return self.extracts_diagnostics

    @property
    def geometry(self) -> bool:
        return self.extracts_geometry

    @property
    def materials(self) -> bool:
        return self.extracts_materials

    @property
    def layers(self) -> bool:
        return self.extracts_layers

    @property
    def blocks(self) -> bool:
        return self.extracts_blocks

    @property
    def text(self) -> bool:
        return self.extracts_text

    @property
    def exports(self) -> bool:
        return self.supports_exports

    @property
    def quantity_hints(self) -> bool:
        return self.supports_quantity_hints

    @property
    def layout_selection(self) -> bool:
        return self.supports_layout_selection

    @property
    def xref_resolution(self) -> bool:
        return self.supports_xref_resolution


@dataclass(frozen=True, slots=True)
class ProbeRequirement:
    """Static probe metadata used to evaluate adapter availability."""

    kind: ProbeKind
    name: str
    failure_status: AdapterStatus
    detail: str


@dataclass(frozen=True, slots=True)
class ProbeObservation:
    """Observed result for a probe check."""

    kind: ProbeKind
    name: str
    status: ProbeStatus
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class ProbeIssue:
    """Availability issue derived from a failed probe."""

    kind: ProbeKind
    name: str
    observed_status: ProbeStatus
    adapter_status: AdapterStatus
    detail: str


@dataclass(frozen=True, slots=True)
class AdapterAvailability:
    """Availability state resolved from an adapter descriptor and probe results."""

    status: AdapterStatus
    availability_reason: AvailabilityReason | None = None
    license_state: LicenseState = LicenseState.NOT_REQUIRED
    issues: tuple[ProbeIssue, ...] = ()
    observed: tuple[ProbeObservation, ...] = ()
    last_checked_at: datetime | None = None
    details: Mapping[str, JSONValue] | None = None
    probe_elapsed_ms: float | None = None

    def __post_init__(self) -> None:
        if self.probe_elapsed_ms is not None and self.probe_elapsed_ms < 0:
            raise ValueError("Probe elapsed time cannot be negative.")


@dataclass(frozen=True, slots=True)
class AdapterDescriptor:
    """Lazy/static adapter registry descriptor."""

    key: str
    family: InputFamily
    upload_formats: tuple[UploadFormat, ...]
    display_name: str
    module: str
    license_name: str
    capabilities: AdapterCapabilities = field(default_factory=AdapterCapabilities)
    probes: tuple[ProbeRequirement, ...] = ()
    notes: tuple[str, ...] = ()
    output_formats: tuple[str, ...] = ("canonical_json",)
    experimental: bool = False
    confidence_range: tuple[float, float] | None = None
    bounded_probe_ms: int = 500
    adapter_version: str | None = None

    def __post_init__(self) -> None:
        if not self.key:
            raise ValueError("Adapter key cannot be empty.")
        if any(not (char.islower() or char.isdigit() or char in {"-", "_"}) for char in self.key):
            raise ValueError("Adapter key must use stable lowercase slug semantics.")
        if not self.output_formats:
            raise ValueError("Adapter descriptor must declare at least one output format.")
        if self.bounded_probe_ms <= 0:
            raise ValueError("Bounded probe time must be greater than zero.")
        if self.confidence_range is not None:
            low, high = self.confidence_range
            if not 0 <= low <= high <= 1:
                raise ValueError("Confidence range must be normalized to the 0..1 range.")
        for upload_format in self.upload_formats:
            if self.family not in input_families_for_upload_format(upload_format):
                raise ValueError(
                    f"Input family {self.family!s} is not valid for {upload_format!s}."
                )

    @property
    def adapter_key(self) -> str:
        return self.key

    @property
    def input_formats(self) -> tuple[UploadFormat, ...]:
        return self.upload_formats


class IngestionAdapter(Protocol):
    """Protocol that concrete ingestion adapters must implement."""

    descriptor: AdapterDescriptor

    def probe(self) -> AdapterAvailability:
        """Return current availability without importing optional peers."""

    async def ingest(
        self,
        source: AdapterSource,
        options: AdapterExecutionOptions,
    ) -> AdapterResult:
        """Transform a source file into canonical output."""
