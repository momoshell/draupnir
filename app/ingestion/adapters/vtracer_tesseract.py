"""Experimental raster PDF adapter scaffold."""

from __future__ import annotations

import asyncio
import importlib
import re
import shutil
from pathlib import Path
from typing import Final, cast

from app.ingestion.contracts import (
    AdapterAvailability,
    AdapterDiagnostic,
    AdapterExecutionOptions,
    AdapterResult,
    AdapterSource,
    AdapterUnavailableError,
    AdapterWarning,
    AvailabilityReason,
    ConfidenceSummary,
    IngestionAdapter,
    InputFamily,
    JSONValue,
    ProbeKind,
    ProbeObservation,
    ProbeStatus,
    ProvenanceRecord,
    UploadFormat,
)
from app.ingestion.registry import evaluate_availability, get_registry

_SCHEMA_VERSION: Final[str] = "0.1"
_DEFAULT_LAYER: Final[str] = "default"
_PDF_HEADER: Final[bytes] = b"%PDF-"
_PAGE_PATTERN: Final[re.Pattern[bytes]] = re.compile(rb"/Type\s*/Page\b")
_PAGE_SCAN_CHUNK_SIZE: Final[int] = 64 * 1024
_PAGE_SCAN_OVERLAP: Final[int] = 64
_MAX_PAGES: Final[int] = 1024


class VTracerTesseractAdapter(IngestionAdapter):
    """Safe, lazy raster PDF scaffold with deferred vectorization and OCR."""

    def __init__(self) -> None:
        self.descriptor = get_registry()[InputFamily.PDF_RASTER]

    def probe(self) -> AdapterAvailability:
        return evaluate_availability(self.descriptor, observations=self._probe_observations())

    async def ingest(
        self,
        source: AdapterSource,
        options: AdapterExecutionOptions,
    ) -> AdapterResult:
        self._validate_source(source)
        _raise_if_cancelled(options.cancellation)
        self._runtime_for_ingest()
        _raise_if_cancelled(options.cancellation)

        page_count = _detect_page_count(
            source.file_path,
            page_cap=_MAX_PAGES,
            cancellation=options.cancellation,
        )
        canonical = cast(
            dict[str, JSONValue],
            {
            "schema_version": _SCHEMA_VERSION,
            "canonical_entity_schema_version": _SCHEMA_VERSION,
            "units": {"normalized": "unknown"},
            "coordinate_system": {
                "name": "pdf_page_space_unrotated",
                "origin": "top_left",
                "x_axis": "right",
                "y_axis": "down",
            },
            "layouts": tuple(
                {"name": f"page-{page_number}", "page_number": page_number}
                for page_number in range(1, page_count + 1)
            ),
            "layers": ({"name": _DEFAULT_LAYER},),
            "blocks": (),
            "entities": (),
            "xrefs": (),
            "metadata": {
                "source_format": UploadFormat.PDF.value,
                "geometry_mode": "raster",
                "page_count": page_count,
                "default_layer": _DEFAULT_LAYER,
                "text_blocks": [],
                "pdf_scale": {
                    "status": "unconfirmed",
                    "coordinate_space": "pdf_page_space_unrotated",
                    "unit": "point",
                    "real_world_units": False,
                    "calibration": {
                        "provided": False,
                        "source": "not_supported_in_adapter_options",
                        "requires_extraction_profile_pass_through": True,
                    },
                },
            },
            },
        )

        return AdapterResult(
            canonical=canonical,
            provenance=(
                ProvenanceRecord(
                    stage="extract",
                    adapter_key=self.descriptor.key,
                    source_ref=_durable_source_ref(source),
                    details={
                        "geometry_mode": "raster",
                        "page_count": page_count,
                        "scaffold_only": True,
                    },
                ),
            ),
            confidence=ConfidenceSummary(
                score=0.3,
                review_required=True,
                basis="raster_scaffold",
            ),
            warnings=(
                AdapterWarning(
                    code="RASTER_SCAFFOLD_ONLY",
                    message="Raster PDF ingestion currently returns scaffold metadata only.",
                ),
                AdapterWarning(
                    code="RASTER_SCALE_UNCONFIRMED",
                    message="Raster PDF scale is unconfirmed and requires downstream review.",
                ),
                AdapterWarning(
                    code="RASTER_GEOMETRY_REVIEW_REQUIRED",
                    message=(
                        "Raster PDF geometry remains review-required until "
                        "vectorization ships."
                    ),
                ),
                AdapterWarning(
                    code="RASTER_OCR_DEFERRED",
                    message="Raster PDF OCR extraction is deferred in this scaffold adapter.",
                ),
            ),
            diagnostics=(
                AdapterDiagnostic(
                    code="raster_scaffold_created",
                    message="Created raster scaffold canonical payload.",
                    details={"page_count": page_count},
                ),
                AdapterDiagnostic(
                    code="raster_dependency_probe",
                    message="Captured raster dependency availability for scaffold execution.",
                    details={
                        "vtracer_available": _vtracer_available(),
                        "tesseract_available": _tesseract_binary_path() is not None,
                    },
                ),
                AdapterDiagnostic(
                    code="raster_vectorization_deferred",
                    message="Raster vectorization is deferred in the experimental scaffold.",
                ),
                AdapterDiagnostic(
                    code="raster_ocr_deferred",
                    message="Raster OCR extraction is deferred in the experimental scaffold.",
                ),
                AdapterDiagnostic(
                    code="raster_scale_unconfirmed",
                    message="Raster PDF scale remains unconfirmed in scaffold output.",
                ),
            ),
        )

    def _probe_observations(self) -> tuple[ProbeObservation, ...]:
        return (
            ProbeObservation(
                kind=ProbeKind.PYTHON_PACKAGE,
                name="vtracer",
                status=ProbeStatus.AVAILABLE if _vtracer_available() else ProbeStatus.MISSING,
            ),
            ProbeObservation(
                kind=ProbeKind.BINARY,
                name="tesseract",
                status=(
                    ProbeStatus.AVAILABLE
                    if _tesseract_binary_path() is not None
                    else ProbeStatus.MISSING
                ),
            ),
        )

    def _runtime_for_ingest(self) -> object:
        try:
            runtime = _load_vtracer_runtime()
        except Exception as exc:
            raise AdapterUnavailableError(
                AvailabilityReason.PROBE_FAILED,
                detail="Required Python package 'vtracer' could not be loaded.",
            ) from exc
        if runtime is None:
            raise AdapterUnavailableError(
                AvailabilityReason.PROBE_FAILED,
                detail="Required Python package 'vtracer' is not installed.",
            )
        return runtime

    def _validate_source(self, source: AdapterSource) -> None:
        if source.upload_format is not UploadFormat.PDF:
            raise ValueError("Raster PDF adapter only accepts PDF uploads.")
        if source.input_family is not InputFamily.PDF_RASTER:
            raise ValueError("Raster PDF adapter requires InputFamily.PDF_RASTER.")


def create_adapter() -> VTracerTesseractAdapter:
    """Build the experimental raster PDF adapter."""

    return VTracerTesseractAdapter()


def _load_vtracer_runtime() -> object | None:
    try:
        return importlib.import_module("vtracer")
    except ModuleNotFoundError as exc:
        if exc.name == "vtracer":
            return None
        raise


def _vtracer_available() -> bool:
    try:
        return _load_vtracer_runtime() is not None
    except Exception:
        return False


def _tesseract_binary_path() -> str | None:
    return shutil.which("tesseract")


def _raise_if_cancelled(cancellation: object | None) -> None:
    if cancellation is None:
        return

    is_cancelled = getattr(cancellation, "is_cancelled", None)
    if callable(is_cancelled) and is_cancelled():
        raise asyncio.CancelledError


def _detect_page_count(
    path: Path,
    *,
    page_cap: int,
    cancellation: object | None,
) -> int:
    count = 0
    carry = b""
    saw_header = False
    with path.open("rb") as source_file:
        _raise_if_cancelled(cancellation)
        while chunk := source_file.read(_PAGE_SCAN_CHUNK_SIZE):
            _raise_if_cancelled(cancellation)
            if not saw_header:
                saw_header = True
                if not chunk.startswith(_PDF_HEADER):
                    raise ValueError("Raster PDF scaffold requires a valid PDF file.")

            data = carry + chunk
            if len(data) <= _PAGE_SCAN_OVERLAP:
                carry = data
                continue

            safe_boundary = len(data) - _PAGE_SCAN_OVERLAP
            count += sum(
                1 for match in _PAGE_PATTERN.finditer(data) if match.start() < safe_boundary
            )
            if count > page_cap:
                raise RuntimeError("Raster PDF scaffold exceeded page limit.")
            carry = data[safe_boundary:]

    if not saw_header:
        raise ValueError("Raster PDF scaffold requires a valid PDF file.")

    _raise_if_cancelled(cancellation)
    count += sum(1 for _match in _PAGE_PATTERN.finditer(carry))
    if count > page_cap:
        raise RuntimeError("Raster PDF scaffold exceeded page limit.")
    return max(count, 1)


def _durable_source_ref(source: AdapterSource) -> str:
    if source.original_name is None:
        return "originals/source.pdf"

    sanitized_name = source.original_name.replace("\\", "/").split("/")[-1]
    if not sanitized_name:
        return "originals/source.pdf"
    return f"originals/{sanitized_name}"
