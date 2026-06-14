"""Conservative PyMuPDF vector PDF adapter."""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import importlib.metadata
import json
import math
import multiprocessing
import os
import re
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, replace
from pathlib import Path, PureWindowsPath
from time import perf_counter
from types import ModuleType
from typing import Any, cast

from app.ingestion.canonical import build_entity_provenance
from app.ingestion.contracts import (
    AdapterAvailability,
    AdapterDiagnostic,
    AdapterExecutionOptions,
    AdapterResult,
    AdapterSource,
    AdapterStatus,
    AdapterTimeout,
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
from app.ingestion.registry import evaluate_availability, get_descriptor

_DESCRIPTOR = get_descriptor(InputFamily.PDF_VECTOR)
_DISTRIBUTION_NAME = "PyMuPDF"
_LICENSE_PROBE_NAME = "pymupdf-deployment-review"
_APPROVED_LICENSE_PROBES_ENV_VAR = "DRAUPNIR_APPROVED_LICENSE_PROBES"
_RUNTIME_MODULE = "fitz"
_SCHEMA_VERSION = "0.1"
_DEFAULT_LAYER_NAME = "default"
_VECTOR_CONFIDENCE_SCORE = 0.75
# Confidence for a partial extraction that hit a configurable complexity cap and
# was truncated; lowered (and still review-gated) to flag the missing geometry.
_DEGRADED_CONFIDENCE_SCORE = 0.4
_MAX_PAGES = 256
_MAX_PATH_ITEMS_PER_DRAWING = 10_000
_MAX_POINTS_PER_ENTITY = 5_000
_MAX_TEXT_BLOCKS = 10_000
_MAX_TEXT_BYTES = 1_000_000
_PROCESS_POLL_INTERVAL_SECONDS = 0.01
_PROCESS_TERMINATE_GRACE_SECONDS = 0.5
_PROCESS_KILL_GRACE_SECONDS = 0.5


class PyMuPDFAvailabilityError(Exception):
    """Sanitized adapter preflight unavailability."""

    def __init__(self, *, availability_reason: AvailabilityReason, message: str) -> None:
        super().__init__(message)
        self.availability_reason = availability_reason


class PyMuPDFLicenseError(PermissionError):
    """Sanitized missing deployment-review acknowledgement error."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.availability_reason = AvailabilityReason.MISSING_LICENSE


class PyMuPDFExtractionLimitError(RuntimeError):
    """Sanitized extraction budget/cap failure."""

    #: Coarse, content-free token surfaced to the runner/job for diagnosis.
    failure_reason = "extraction_limit"


class _EntityBudgetError(PyMuPDFExtractionLimitError):
    """Internal signal that the configurable entity cap was reached.

    Caught inside page extraction to degrade to a partial result; unlike the
    per-element guards (path-item/point limits), it never escapes as a failure.
    """


class PyMuPDFNonFiniteValueError(ValueError):
    """Raised when parser output contains a non-finite number."""


@dataclass(frozen=True, slots=True)
class _ExtractionLimits:
    """Configurable complexity caps; exceeding one degrades, not fails."""

    max_drawings_per_page: int
    max_total_drawings: int
    max_entities: int

    @classmethod
    def from_settings(cls) -> _ExtractionLimits:
        from app.core.config import settings

        return cls(
            max_drawings_per_page=settings.pymupdf_max_drawings_per_page,
            max_total_drawings=settings.pymupdf_max_total_drawings,
            max_entities=settings.pymupdf_max_entities,
        )


@dataclass(frozen=True)
class _ExtractionBudget:
    started_at: float
    timeout_seconds: float | None

    def checkpoint(self, options: AdapterExecutionOptions) -> None:
        _raise_if_cancelled(options)
        if self.timeout_seconds is None:
            return
        if self.timeout_seconds <= 0:
            raise TimeoutError("PyMuPDF extraction timed out.")
        elapsed_seconds = perf_counter() - self.started_at
        if elapsed_seconds >= self.timeout_seconds:
            raise TimeoutError("PyMuPDF extraction timed out.")


@dataclass(frozen=True, slots=True)
class _ProcessExtractionRequest:
    file_path: str
    upload_format: str
    timeout_seconds: float | None
    limits: _ExtractionLimits


@dataclass(slots=True)
class _ProcessExtractionHandle:
    process: Any
    receiver: Any

    def poll(self) -> bool:
        return bool(self.receiver.poll())

    def recv(self) -> dict[str, Any]:
        return cast(dict[str, Any], self.receiver.recv())

    def is_alive(self) -> bool:
        return bool(self.process.is_alive())

    def join(self, timeout: float | None = None) -> None:
        self.process.join(timeout)

    def terminate(self) -> None:
        self.process.terminate()

    def kill(self) -> None:
        kill = getattr(self.process, "kill", None)
        if callable(kill):
            kill()

    def close(self) -> None:
        self.receiver.close()
        close_process = getattr(self.process, "close", None)
        if callable(close_process):
            try:
                close_process()
            except ValueError:
                return


def create_adapter(
    *,
    license_acknowledged: Callable[[], bool] | None = None,
) -> IngestionAdapter:
    """Create the PyMuPDF adapter without importing fitz at module import time."""

    return PyMuPDFAdapter(
        license_acknowledged=license_acknowledged or _license_acknowledged_from_env,
    )


class PyMuPDFAdapter(IngestionAdapter):
    """Conservative vector PDF adapter backed by PyMuPDF."""

    def __init__(self, *, license_acknowledged: Callable[[], bool]) -> None:
        self._license_acknowledged = license_acknowledged
        self.descriptor = replace(_DESCRIPTOR, adapter_version=_package_version())

    def probe(self) -> AdapterAvailability:
        """Report PyMuPDF runtime and deployment-review availability."""

        started_at = perf_counter()
        observations: list[ProbeObservation] = []
        details: dict[str, JSONValue] = {"package": _RUNTIME_MODULE}

        runtime, package_observation = _runtime_probe_observation()
        observations.append(package_observation)

        version = _runtime_version(runtime) if runtime is not None else _package_version()
        if version is not None:
            details["package_version"] = version

        license_observation = _license_probe_observation(
            self._license_acknowledged,
            details=details,
        )
        observations.append(license_observation)

        availability = evaluate_availability(
            self.descriptor,
            observations=tuple(observations),
            details=details,
            probe_elapsed_ms=(perf_counter() - started_at) * 1000.0,
        )
        if license_observation.status is ProbeStatus.UNKNOWN:
            return replace(
                availability,
                status=AdapterStatus.UNAVAILABLE,
                availability_reason=AvailabilityReason.PROBE_FAILED,
            )
        return availability

    async def ingest(
        self,
        source: AdapterSource,
        options: AdapterExecutionOptions,
    ) -> AdapterResult:
        """Extract conservative vector linework and metadata from a PDF."""

        started_at = perf_counter()
        _raise_if_cancelled(options)
        self._runtime_for_ingest()
        _raise_if_cancelled(options)
        canonical, warnings = await _extract_with_process(source, options)

        elapsed_ms = (perf_counter() - started_at) * 1000.0
        entity_count = len(cast(tuple[object, ...], canonical["entities"]))
        metadata = cast(dict[str, JSONValue], canonical["metadata"])
        page_count = int(cast(int, metadata["page_count"]))
        text_block_count = len(cast(tuple[object, ...], metadata["text_blocks"]))
        truncated = "complexity_truncation" in metadata

        return AdapterResult(
            canonical=canonical,
            provenance=(
                ProvenanceRecord(
                    stage="extract",
                    adapter_key=self.descriptor.key,
                    source_ref=_durable_source_ref(source),
                    details={
                        "page_count": page_count,
                        "entity_count": entity_count,
                        "text_block_count": text_block_count,
                    },
                ),
            ),
            confidence=ConfidenceSummary(
                score=_DEGRADED_CONFIDENCE_SCORE if truncated else _VECTOR_CONFIDENCE_SCORE,
                review_required=True,
                basis=(
                    "vector_pdf_complexity_truncated"
                    if truncated
                    else "vector_pdf_unconfirmed_scale"
                ),
            ),
            warnings=tuple(warnings),
            diagnostics=(
                AdapterDiagnostic(
                    code="pymupdf.extract",
                    message="Extracted conservative vector PDF geometry in unrotated page space.",
                    details={
                        "page_count": page_count,
                        "entity_count": entity_count,
                        "text_block_count": text_block_count,
                    },
                    elapsed_ms=elapsed_ms,
                ),
            ),
        )

    def _runtime_for_ingest(self) -> None:
        availability = self.probe()
        if availability.status is not AdapterStatus.AVAILABLE:
            raise _availability_error(availability)


def _runtime_probe_observation() -> tuple[ModuleType | None, ProbeObservation]:
    try:
        runtime = _load_runtime_module()
    except ModuleNotFoundError:
        return None, ProbeObservation(
            kind=ProbeKind.PYTHON_PACKAGE,
            name=_RUNTIME_MODULE,
            status=ProbeStatus.MISSING,
            detail="Optional runtime package is not installed.",
        )
    except Exception:
        return None, ProbeObservation(
            kind=ProbeKind.PYTHON_PACKAGE,
            name=_RUNTIME_MODULE,
            status=ProbeStatus.UNKNOWN,
            detail="PyMuPDF runtime import failed.",
        )

    version = _runtime_version(runtime)
    detail = (
        f"PyMuPDF {version} is installed."
        if version is not None
        else "PyMuPDF runtime is importable."
    )
    return runtime, ProbeObservation(
        kind=ProbeKind.PYTHON_PACKAGE,
        name=_RUNTIME_MODULE,
        status=ProbeStatus.AVAILABLE,
        detail=detail,
    )


def _license_probe_observation(
    provider: Callable[[], bool],
    *,
    details: dict[str, JSONValue],
) -> ProbeObservation:
    try:
        acknowledged = bool(provider())
    except Exception:
        details["license_acknowledged"] = "unknown"
        details["license_probe_status"] = "failed"
        return ProbeObservation(
            kind=ProbeKind.LICENSE,
            name=_LICENSE_PROBE_NAME,
            status=ProbeStatus.UNKNOWN,
            detail="Deployment review probe failed.",
        )

    details["license_acknowledged"] = acknowledged
    if acknowledged:
        return ProbeObservation(
            kind=ProbeKind.LICENSE,
            name=_LICENSE_PROBE_NAME,
            status=ProbeStatus.AVAILABLE,
            detail="Deployment review acknowledgement is present.",
        )

    return ProbeObservation(
        kind=ProbeKind.LICENSE,
        name=_LICENSE_PROBE_NAME,
        status=ProbeStatus.MISSING,
        detail="Deployment review acknowledgement is required before use.",
    )


def _extract_document_canonical(
    document: Any,
    *,
    source: AdapterSource,
    options: AdapterExecutionOptions,
    budget: _ExtractionBudget,
    limits: _ExtractionLimits,
) -> tuple[dict[str, JSONValue], list[AdapterWarning]]:
    entities: list[dict[str, JSONValue]] = []
    layouts: list[dict[str, JSONValue]] = []
    paper_sizes: list[dict[str, JSONValue]] = []
    text_blocks: list[dict[str, JSONValue]] = []
    warnings: list[AdapterWarning] = []
    layer_names: list[str] = [_DEFAULT_LAYER_NAME]
    text_bytes = 0
    total_drawings = 0
    dropped_drawings = 0
    entities_truncated = False

    page_count = int(getattr(document, "page_count", 0))
    if page_count > _MAX_PAGES:
        raise PyMuPDFExtractionLimitError("PyMuPDF extraction exceeded page limit.")

    for page_index in range(page_count):
        budget.checkpoint(options)
        page = document.load_page(page_index)
        budget.checkpoint(options)
        page_number = page_index + 1
        layout_name = f"page-{page_number}"
        page_rect = getattr(page, "mediabox", page.rect)
        layouts.append(
            {
                "name": layout_name,
                "page_number": page_number,
                "width": _round_float(float(page_rect.width)),
                "height": _round_float(float(page_rect.height)),
                "rotation": int(getattr(page, "rotation", 0)),
                "bbox": _bbox_from_rect(page_rect),
            }
        )
        paper_sizes.append(
            _paper_size(
                width_pt=float(page_rect.width),
                height_pt=float(page_rect.height),
                page_number=page_number,
            )
        )

        budget.checkpoint(options)
        drawings = cast(list[dict[str, Any]], page.get_drawings())
        budget.checkpoint(options)
        # Degrade instead of failing: truncate to the configurable per-page and
        # cumulative drawing caps, recording how much geometry was dropped.
        kept = min(len(drawings), limits.max_drawings_per_page)
        kept = min(kept, max(0, limits.max_total_drawings - total_drawings))
        dropped_drawings += len(drawings) - kept
        drawings = drawings[:kept]
        total_drawings += kept
        page_entities, page_warnings, page_layers, page_entities_truncated = _extract_page_entities(
            drawings,
            page_number=page_number,
            layout_name=layout_name,
            options=options,
            budget=budget,
            entity_limit=limits.max_entities - len(entities),
        )
        entities.extend(page_entities)
        warnings.extend(page_warnings)
        entities_truncated = entities_truncated or page_entities_truncated
        for layer_name in page_layers:
            if layer_name not in layer_names:
                layer_names.append(layer_name)

        budget.checkpoint(options)
        page_text_blocks, page_text_warnings, page_text_bytes = _extract_text_blocks(
            page,
            page_number=page_number,
            layout_name=layout_name,
            options=options,
            budget=budget,
            text_block_limit=_MAX_TEXT_BLOCKS - len(text_blocks),
            text_byte_limit=_MAX_TEXT_BYTES - text_bytes,
        )
        budget.checkpoint(options)
        text_blocks.extend(page_text_blocks)
        warnings.extend(page_text_warnings)
        text_bytes += page_text_bytes

    metadata: dict[str, JSONValue] = {
        "source_format": source.upload_format.value,
        "geometry_mode": "vector",
        "page_count": page_count,
        "default_layer": _DEFAULT_LAYER_NAME,
        "pdf_scale": _derive_pdf_scale(text_blocks),
        "paper_sizes": tuple(paper_sizes),
        "text_blocks": tuple(text_blocks),
    }
    if not entities:
        metadata["empty_entities_reason"] = "no_vector_entities_detected"

    if dropped_drawings > 0 or entities_truncated:
        truncation: dict[str, JSONValue] = {
            "dropped_drawings": dropped_drawings,
            "entities_truncated": entities_truncated,
            "max_drawings_per_page": limits.max_drawings_per_page,
            "max_total_drawings": limits.max_total_drawings,
            "max_entities": limits.max_entities,
        }
        metadata["complexity_truncation"] = truncation
        warnings.append(
            AdapterWarning(
                code="pymupdf.extraction_truncated",
                message=(
                    "PyMuPDF extraction exceeded a complexity cap; the result is "
                    "partial and gated for review."
                ),
                details=truncation,
            )
        )

    return {
        "schema_version": _SCHEMA_VERSION,
        "canonical_entity_schema_version": _SCHEMA_VERSION,
        "units": {"normalized": "unknown"},
        "coordinate_system": {
            "name": "pdf_page_space_unrotated",
            "origin": "top_left",
            "x_axis": "right",
            "y_axis": "down",
        },
        "layouts": tuple(layouts),
        "layers": tuple({"name": layer_name} for layer_name in layer_names),
        # Layers are derived from each path's pen signature (stroke colour + width); PDFs have
        # no native layer table. OCG-sourced layers (#414) will set this to "ocg" when present.
        "layer_source": "pen_signature",
        "blocks": (),
        "entities": tuple(entities),
        "xrefs": (),
        "metadata": metadata,
    }, warnings


def _extract_page_entities(
    drawings: list[dict[str, Any]],
    *,
    page_number: int,
    layout_name: str,
    options: AdapterExecutionOptions,
    budget: _ExtractionBudget,
    entity_limit: int,
) -> tuple[list[dict[str, JSONValue]], list[AdapterWarning], list[str], bool]:
    """Extract a page's entities, degrading to a partial result at the entity cap.

    Returns the accumulated entities/warnings/layers plus a flag indicating the
    configurable entity cap was reached and emission stopped early.
    """
    entities: list[dict[str, JSONValue]] = []
    warnings: list[AdapterWarning] = []
    layer_names: list[str] = []
    truncated = False
    try:
        _populate_page_entities(
            drawings,
            entities=entities,
            warnings=warnings,
            layer_names=layer_names,
            page_number=page_number,
            layout_name=layout_name,
            options=options,
            budget=budget,
            entity_limit=entity_limit,
        )
    except _EntityBudgetError:
        truncated = True
    return entities, warnings, layer_names, truncated


def _populate_page_entities(
    drawings: list[dict[str, Any]],
    *,
    entities: list[dict[str, JSONValue]],
    warnings: list[AdapterWarning],
    layer_names: list[str],
    page_number: int,
    layout_name: str,
    options: AdapterExecutionOptions,
    budget: _ExtractionBudget,
    entity_limit: int,
) -> None:
    for drawing_index, drawing in enumerate(drawings):
        budget.checkpoint(options)
        layer_name = _resolve_drawing_layer(drawing)
        closed = bool(drawing.get("closePath"))
        items = drawing.get("items", ())
        _enforce_path_item_limit(items)
        if layer_name not in layer_names:
            layer_names.append(layer_name)

        current_points: list[tuple[float, float]] = []
        current_item_indices: list[int] = []
        emitted_count = 0

        for item_index, item in enumerate(cast(tuple[Any, ...], items)):
            budget.checkpoint(options)
            if item_index >= _MAX_PATH_ITEMS_PER_DRAWING:
                raise PyMuPDFExtractionLimitError(
                    "PyMuPDF extraction exceeded drawing path item limit."
                )
            if not isinstance(item, tuple) or not item:
                warnings.append(
                    AdapterWarning(
                        code="pymupdf_path_item_invalid",
                        message="Skipping invalid PyMuPDF path item.",
                        details={
                            "page_number": page_number,
                            "drawing_index": drawing_index,
                            "item_index": item_index,
                        },
                    )
                )
                continue

            operator = item[0]
            if operator == "l" and len(item) >= 3:
                try:
                    start = _point_tuple(item[1])
                    end = _point_tuple(item[2])
                except PyMuPDFNonFiniteValueError:
                    emitted_count, warning = _flush_pending_entity(
                        entities=entities,
                        points=current_points,
                        page_number=page_number,
                        layout_name=layout_name,
                        layer_name=layer_name,
                        drawing_index=drawing_index,
                        entity_index=emitted_count,
                        item_indices=current_item_indices,
                        drawing=drawing,
                        closed=closed,
                        entity_limit=entity_limit,
                    )
                    if warning is not None:
                        warnings.append(warning)
                    warnings.append(
                        AdapterWarning(
                            code="pymupdf_path_item_non_finite",
                            message="Skipping PyMuPDF path item with non-finite numeric values.",
                            details={
                                "page_number": page_number,
                                "drawing_index": drawing_index,
                                "item_index": item_index,
                            },
                        )
                    )
                    continue
                if not current_points:
                    _enforce_pending_point_limit(current_points, additional_points=2)
                    current_points.extend((start, end))
                    current_item_indices.append(item_index)
                    continue
                if _same_point(current_points[-1], start):
                    _enforce_pending_point_limit(current_points, additional_points=1)
                    current_points.append(end)
                    current_item_indices.append(item_index)
                    continue
                emitted_count, warning = _flush_pending_entity(
                    entities=entities,
                    points=current_points,
                    page_number=page_number,
                    layout_name=layout_name,
                    layer_name=layer_name,
                    drawing_index=drawing_index,
                    entity_index=emitted_count,
                    item_indices=current_item_indices,
                    drawing=drawing,
                    closed=closed,
                    entity_limit=entity_limit,
                )
                if warning is not None:
                    warnings.append(warning)
                _enforce_pending_point_limit(current_points, additional_points=2)
                current_points.extend((start, end))
                current_item_indices.append(item_index)
                continue

            if operator == "re" and len(item) >= 2:
                emitted_count, warning = _flush_pending_entity(
                    entities=entities,
                    points=current_points,
                    page_number=page_number,
                    layout_name=layout_name,
                    layer_name=layer_name,
                    drawing_index=drawing_index,
                    entity_index=emitted_count,
                    item_indices=current_item_indices,
                    drawing=drawing,
                    closed=closed,
                    entity_limit=entity_limit,
                )
                if warning is not None:
                    warnings.append(warning)
                try:
                    _append_entity(
                        entities,
                        _build_rect_entity(
                            rect=item[1],
                            page_number=page_number,
                            layout_name=layout_name,
                            layer_name=layer_name,
                            drawing_index=drawing_index,
                            entity_index=emitted_count,
                            item_index=item_index,
                            drawing=drawing,
                        ),
                        entity_limit=entity_limit,
                    )
                except PyMuPDFNonFiniteValueError:
                    warnings.append(
                        AdapterWarning(
                            code="pymupdf_entity_non_finite",
                            message="Skipping PyMuPDF entity with non-finite numeric values.",
                            details={
                                "page_number": page_number,
                                "drawing_index": drawing_index,
                                "item_index": item_index,
                            },
                        )
                    )
                    continue
                emitted_count += 1
                continue

            emitted_count, warning = _flush_pending_entity(
                entities=entities,
                points=current_points,
                page_number=page_number,
                layout_name=layout_name,
                layer_name=layer_name,
                drawing_index=drawing_index,
                entity_index=emitted_count,
                item_indices=current_item_indices,
                drawing=drawing,
                closed=closed,
                entity_limit=entity_limit,
            )
            if warning is not None:
                warnings.append(warning)
            try:
                _append_entity(
                    entities,
                    _build_unknown_entity(
                        item=item,
                        page_number=page_number,
                        layout_name=layout_name,
                        layer_name=layer_name,
                        drawing_index=drawing_index,
                        entity_index=emitted_count,
                        item_index=item_index,
                        drawing=drawing,
                    ),
                    entity_limit=entity_limit,
                )
            except PyMuPDFNonFiniteValueError:
                warnings.append(
                    AdapterWarning(
                        code="pymupdf_entity_non_finite",
                        message="Skipping PyMuPDF entity with non-finite numeric values.",
                        details={
                            "page_number": page_number,
                            "drawing_index": drawing_index,
                            "item_index": item_index,
                            "operator": str(operator),
                        },
                    )
                )
                warnings.append(
                    AdapterWarning(
                        code="pymupdf_path_operator_unsupported",
                        message="Skipping unsupported PyMuPDF path operator.",
                        details={
                            "page_number": page_number,
                            "drawing_index": drawing_index,
                            "item_index": item_index,
                            "operator": str(operator),
                        },
                    )
                )
                continue
            emitted_count += 1
            warnings.append(
                AdapterWarning(
                    code="pymupdf_path_operator_unsupported",
                    message="Retained unsupported PyMuPDF path operator as unknown entity.",
                    details={
                        "page_number": page_number,
                        "drawing_index": drawing_index,
                        "item_index": item_index,
                        "operator": str(operator),
                    },
                )
            )
            continue

        _, warning = _flush_pending_entity(
            entities=entities,
            points=current_points,
            page_number=page_number,
            layout_name=layout_name,
            layer_name=layer_name,
            drawing_index=drawing_index,
            entity_index=emitted_count,
            item_indices=current_item_indices,
            drawing=drawing,
            closed=closed,
            entity_limit=entity_limit,
        )
        if warning is not None:
            warnings.append(warning)


def _extract_text_blocks(
    page: Any,
    *,
    page_number: int,
    layout_name: str,
    options: AdapterExecutionOptions,
    budget: _ExtractionBudget,
    text_block_limit: int,
    text_byte_limit: int,
) -> tuple[list[dict[str, JSONValue]], list[AdapterWarning], int]:
    extracted: list[dict[str, JSONValue]] = []
    warnings: list[AdapterWarning] = []
    text_bytes = 0

    if text_block_limit < 0:
        raise PyMuPDFExtractionLimitError("PyMuPDF extraction exceeded text block limit.")
    if text_byte_limit < 0:
        raise PyMuPDFExtractionLimitError("PyMuPDF extraction exceeded text content limit.")

    budget.checkpoint(options)
    text_payload = cast(dict[str, Any], page.get_text("dict"))
    budget.checkpoint(options)
    for block in cast(list[dict[str, Any]], text_payload.get("blocks", [])):
        budget.checkpoint(options)
        if block.get("type") != 0:
            continue
        if len(extracted) >= text_block_limit:
            raise PyMuPDFExtractionLimitError("PyMuPDF extraction exceeded text block limit.")

        lines = cast(list[dict[str, Any]], block.get("lines", []))
        line_text: list[str] = []
        for line in lines:
            budget.checkpoint(options)
            spans = cast(list[dict[str, Any]], line.get("spans", []))
            line_text.append("".join(str(span.get("text", "")) for span in spans))

        text = "\n".join(part for part in line_text if part)
        block_text_bytes = len(text.encode("utf-8"))
        if text_bytes + block_text_bytes > text_byte_limit:
            raise PyMuPDFExtractionLimitError("PyMuPDF extraction exceeded text content limit.")

        try:
            extracted.append(
                {
                    "page_number": page_number,
                    "layout": layout_name,
                    "block_number": int(block.get("number", 0)),
                    "bbox": _bbox_from_tuple(_rect_tuple(block.get("bbox"))),
                    "text": text,
                }
            )
        except PyMuPDFNonFiniteValueError:
            warnings.append(
                AdapterWarning(
                    code="pymupdf_text_block_non_finite",
                    message="Skipping PyMuPDF text block with non-finite numeric values.",
                    details={
                        "page_number": page_number,
                        "block_number": int(block.get("number", 0)),
                    },
                )
            )
            continue
        text_bytes += block_text_bytes
    return extracted, warnings, text_bytes


def _build_lineish_entity(
    *,
    points: list[tuple[float, float]],
    page_number: int,
    layout_name: str,
    layer_name: str,
    drawing_index: int,
    entity_index: int,
    item_indices: tuple[int, ...],
    drawing: dict[str, Any],
    closed: bool,
    rect_like: bool,
) -> dict[str, JSONValue] | None:
    normalized_points = _normalize_points(points, closed=closed)
    if len(normalized_points) < 2:
        return None
    if len(normalized_points) > _MAX_POINTS_PER_ENTITY:
        raise PyMuPDFExtractionLimitError("PyMuPDF extraction exceeded entity point limit.")

    entity_id = _entity_id(
        page_number=page_number,
        drawing_index=drawing_index,
        entity_index=entity_index,
    )
    provenance = _entity_provenance(
        page_number=page_number,
        drawing_index=drawing_index,
        drawing=drawing,
        item_indices=item_indices,
        operator="re" if rect_like else "l",
    )
    bbox = _bbox_from_points(normalized_points)
    entity_type = "line" if len(normalized_points) == 2 and not closed else "polyline"
    entity: dict[str, JSONValue] = {
        "entity_id": entity_id,
        "entity_type": entity_type,
        "entity_schema_version": _SCHEMA_VERSION,
        "id": entity_id,
        "layout": layout_name,
        "layer": layer_name,
        "bbox": bbox,
        "properties": _entity_properties(drawing, closed=closed, rect_like=rect_like),
        **provenance,
        "provenance": provenance,
        "confidence": {
            "score": _VECTOR_CONFIDENCE_SCORE,
            "basis": "vector_path_segment",
        },
        "drawing_revision_id": None,
        "source_file_id": None,
        "layout_ref": layout_name,
        "layer_ref": layer_name,
        "block_ref": None,
        "parent_entity_ref": None,
    }
    if entity_type == "line":
        start, end = normalized_points
        start_json = _point_json(start)
        end_json = _point_json(end)
        entity.update(
            {
                "kind": entity_type,
                "start": start_json,
                "end": end_json,
                "geometry": _line_geometry(
                    start=start_json,
                    end=end_json,
                    bbox=bbox,
                    point_count=len(normalized_points),
                    closed=closed,
                ),
            }
        )
        return entity

    points_json = tuple(_point_json(point) for point in normalized_points)
    entity.update(
        {
            "kind": entity_type,
            "points": points_json,
            "geometry": _polyline_geometry(
                points=points_json,
                bbox=bbox,
                point_count=len(normalized_points),
                closed=closed,
            ),
        }
    )
    return entity


def _build_rect_entity(
    *,
    rect: Any,
    page_number: int,
    layout_name: str,
    layer_name: str,
    drawing_index: int,
    entity_index: int,
    item_index: int,
    drawing: dict[str, Any],
) -> dict[str, JSONValue]:
    points = [
        (_round_float(float(rect.x0)), _round_float(float(rect.y0))),
        (_round_float(float(rect.x1)), _round_float(float(rect.y0))),
        (_round_float(float(rect.x1)), _round_float(float(rect.y1))),
        (_round_float(float(rect.x0)), _round_float(float(rect.y1))),
        (_round_float(float(rect.x0)), _round_float(float(rect.y0))),
    ]
    entity = _build_lineish_entity(
        points=points,
        page_number=page_number,
        layout_name=layout_name,
        layer_name=layer_name,
        drawing_index=drawing_index,
        entity_index=entity_index,
        item_indices=(item_index,),
        drawing=drawing,
        closed=True,
        rect_like=True,
    )
    if entity is None:
        raise AssertionError("Rect-like PyMuPDF entity unexpectedly failed to build.")
    return entity


def _build_unknown_entity(
    *,
    item: tuple[Any, ...],
    page_number: int,
    layout_name: str,
    layer_name: str,
    drawing_index: int,
    entity_index: int,
    item_index: int,
    drawing: dict[str, Any],
) -> dict[str, JSONValue]:
    operator = str(item[0])
    entity_id = _entity_id(
        page_number=page_number,
        drawing_index=drawing_index,
        entity_index=entity_index,
    )
    bbox = _bbox_from_unsupported_item(item)
    provenance: dict[str, JSONValue] = {
        **_entity_provenance(
            page_number=page_number,
            drawing_index=drawing_index,
            drawing=drawing,
            item_index=item_index,
            operator=operator,
        ),
    }

    return {
        "entity_id": entity_id,
        "entity_type": "unknown",
        "entity_schema_version": _SCHEMA_VERSION,
        "id": entity_id,
        "kind": "unknown",
        "layout": layout_name,
        "layer": layer_name,
        "bbox": bbox,
        "properties": {
            **_entity_properties(drawing, closed=False, rect_like=False),
            "unsupported_operator": operator,
        },
        **provenance,
        "provenance": provenance,
        "confidence": {
            "score": _VECTOR_CONFIDENCE_SCORE,
            "review_required": True,
            "basis": "vector_pdf_unconfirmed_scale",
        },
        "geometry": {
            "kind": "unknown",
            "coordinate_space": "pdf_page_space_unrotated",
            "units": "unknown",
            "bbox": bbox,
            "status": "unsupported",
            "reason": "unsupported_path_operator",
        },
        "drawing_revision_id": None,
        "source_file_id": None,
        "layout_ref": layout_name,
        "layer_ref": layer_name,
        "block_ref": None,
        "parent_entity_ref": None,
    }


def _flush_pending_entity(
    *,
    entities: list[dict[str, JSONValue]],
    points: list[tuple[float, float]],
    page_number: int,
    layout_name: str,
    layer_name: str,
    drawing_index: int,
    entity_index: int,
    item_indices: list[int],
    drawing: dict[str, Any],
    closed: bool,
    entity_limit: int,
) -> tuple[int, AdapterWarning | None]:
    if not points:
        return entity_index, None

    try:
        entity = _build_lineish_entity(
            points=points,
            page_number=page_number,
            layout_name=layout_name,
            layer_name=layer_name,
            drawing_index=drawing_index,
            entity_index=entity_index,
            item_indices=tuple(item_indices),
            drawing=drawing,
            closed=closed,
            rect_like=False,
        )
    except PyMuPDFNonFiniteValueError:
        warning = AdapterWarning(
            code="pymupdf_entity_non_finite",
            message="Skipping PyMuPDF entity with non-finite numeric values.",
            details={
                "page_number": page_number,
                "drawing_index": drawing_index,
                "item_indices": tuple(item_indices),
            },
        )
        points.clear()
        item_indices.clear()
        return entity_index, warning

    points.clear()
    item_indices.clear()
    if entity is None:
        return entity_index, None

    _append_entity(entities, entity, entity_limit=entity_limit)
    return entity_index + 1, None


def _entity_properties(
    drawing: dict[str, Any],
    *,
    closed: bool,
    rect_like: bool,
) -> dict[str, JSONValue]:
    # PyMuPDF emits some path attributes as present-but-None (e.g. ``width`` for fill-only
    # paths, ``lineCap``/``seqno`` on certain drawings). ``dict.get(key, default)`` returns the
    # stored None rather than the default in that case, so coerce None to the default explicitly.
    return {
        "path_type": str(drawing.get("type") or "unknown"),
        "stroke_width": _round_float(float(drawing.get("width") or 0.0)),
        "stroke_color_rgb": _color_tuple(drawing.get("color")),
        "stroke_opacity": _optional_float(drawing.get("stroke_opacity")),
        "fill_color_rgb": _color_tuple(drawing.get("fill")),
        "fill_opacity": _optional_float(drawing.get("fill_opacity")),
        "line_cap": tuple(
            int(value) for value in cast(tuple[int, ...], drawing.get("lineCap") or ())
        ),
        "line_join": _optional_float(drawing.get("lineJoin")),
        "dashes": str(drawing.get("dashes") or ""),
        "closed": closed,
        "rect_like": rect_like,
        "sequence_number": int(drawing.get("seqno") or 0),
    }


def _entity_provenance(
    *,
    page_number: int,
    drawing_index: int,
    drawing: Mapping[str, Any],
    operator: str,
    item_indices: tuple[int, ...] | None = None,
    item_index: int | None = None,
) -> dict[str, JSONValue]:
    extraction_path = (
        "get_drawings",
        f"page-{page_number}",
        f"drawing-{drawing_index}",
        operator,
    )
    notes = ("vector_pdf_unconfirmed_scale",)
    location_payload = _entity_location_payload(
        page_number=page_number,
        drawing_index=drawing_index,
        operator=operator,
        item_indices=item_indices,
        item_index=item_index,
    )
    source_ref = _entity_source_ref(location_payload)
    source_hash = _entity_source_hash(
        _entity_source_payload(
            page_number=page_number,
            operator=operator,
            drawing=drawing,
            item_indices=item_indices,
            item_index=item_index,
        )
    )
    provenance = cast(
        dict[str, JSONValue],
        build_entity_provenance(
            origin="adapter_normalized",
            adapter={"key": _DESCRIPTOR.key},
            source_ref=source_ref,
            source_identity=_entity_source_identity(location_payload),
            source_hash=source_hash,
            extraction_path=list(extraction_path),
            notes=list(notes),
            extra={
                "native": {
                    "pymupdf": {
                        "page_number": page_number,
                        "drawing_index": drawing_index,
                        "operator": operator,
                    }
                },
                "legacy_aliases": {
                    "adapter_key": _DESCRIPTOR.key,
                    "source": location_payload["source"],
                    "source_entity_ref": source_ref,
                    "normalized_source_hash": source_hash,
                },
            },
        ),
    )
    provenance["extraction_path"] = extraction_path
    provenance["notes"] = notes
    provenance["adapter_key"] = _DESCRIPTOR.key
    provenance["source"] = location_payload["source"]
    provenance["source_entity_ref"] = source_ref
    provenance["page_number"] = page_number
    provenance["drawing_index"] = drawing_index
    provenance["operator"] = operator
    provenance["normalized_source_hash"] = source_hash
    extra = cast(dict[str, JSONValue], provenance["extra"])
    native = cast(dict[str, JSONValue], cast(dict[str, JSONValue], extra["native"])["pymupdf"])
    if item_indices is not None:
        native["item_indices"] = item_indices
        provenance["item_indices"] = item_indices
    if item_index is not None:
        native["item_index"] = item_index
        provenance["item_index"] = item_index
    return provenance


def _entity_location_payload(
    *,
    page_number: int,
    drawing_index: int,
    operator: str,
    item_indices: tuple[int, ...] | None,
    item_index: int | None,
) -> dict[str, JSONValue]:
    payload: dict[str, JSONValue] = {
        "page_number": page_number,
        "drawing_index": drawing_index,
        "operator": operator,
        "source": "pymupdf.get_drawings",
    }
    if item_indices is not None:
        payload["item_indices"] = item_indices
    if item_index is not None:
        payload["item_index"] = item_index
    return payload


def _entity_source_payload(
    *,
    page_number: int,
    operator: str,
    drawing: Mapping[str, Any],
    item_indices: tuple[int, ...] | None,
    item_index: int | None,
) -> dict[str, Any]:
    return {
        "page_number": page_number,
        "operator": operator,
        "source": "pymupdf.get_drawings",
        "drawing": _entity_source_drawing_payload(drawing),
        "items": _entity_source_items(
            drawing,
            item_indices=item_indices,
            item_index=item_index,
        ),
    }


def _entity_source_ref(payload: Mapping[str, JSONValue]) -> str:
    page_number = int(cast(int, payload["page_number"]))
    drawing_index = int(cast(int, payload["drawing_index"]))
    operator = str(payload["operator"])
    if "item_indices" in payload:
        indices = ",".join(str(index) for index in cast(tuple[int, ...], payload["item_indices"]))
        return f"pdf://page-{page_number}/drawing-{drawing_index}/{operator}/items:{indices}"
    item_index = int(cast(int, payload["item_index"]))
    return f"pdf://page-{page_number}/drawing-{drawing_index}/{operator}/item:{item_index}"


def _entity_source_identity(payload: Mapping[str, JSONValue]) -> str:
    page_number = int(cast(int, payload["page_number"]))
    drawing_index = int(cast(int, payload["drawing_index"]))
    if "item_indices" in payload:
        indices = ",".join(str(index) for index in cast(tuple[int, ...], payload["item_indices"]))
        return f"page-{page_number}:drawing-{drawing_index}:items-{indices}"
    return (
        f"page-{page_number}:drawing-{drawing_index}:item-{int(cast(int, payload['item_index']))}"
    )


def _entity_source_hash(payload: Mapping[str, Any]) -> str:
    source_hash = _normalized_source_hash(payload)
    if source_hash is None:
        raise TypeError("PyMuPDF source payload is not JSON-normalizable.")
    return source_hash


def _entity_source_drawing_payload(drawing: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in drawing.items() if key != "items"}


def _entity_source_items(
    drawing: Mapping[str, Any],
    *,
    item_indices: tuple[int, ...] | None,
    item_index: int | None,
) -> tuple[Any, ...]:
    raw_items = drawing.get("items")
    if not isinstance(raw_items, (tuple, list)):
        return ()
    items = tuple(raw_items)
    if item_indices is not None:
        return tuple(items[index] for index in item_indices if 0 <= index < len(items))
    if item_index is not None and 0 <= item_index < len(items):
        return (items[item_index],)
    return ()


def _normalized_source_hash(value: Any) -> str | None:
    normalized, representable = _normalize_source_value(value)
    if not representable:
        return None
    payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _normalize_source_value(value: Any) -> tuple[JSONValue, bool]:
    if value is None or isinstance(value, (str, bool)):
        return value, True
    if isinstance(value, int):
        return value, True
    if isinstance(value, float):
        return _round_float(value), True
    if _looks_like_point(value):
        return {
            "x": _round_float(float(value.x)),
            "y": _round_float(float(value.y)),
        }, True
    if _looks_like_rect(value):
        return {
            "x0": _round_float(float(value.x0)),
            "y0": _round_float(float(value.y0)),
            "x1": _round_float(float(value.x1)),
            "y1": _round_float(float(value.y1)),
        }, True
    if _looks_like_quad(value):
        corners = _quad_corner_points(value)
        labels = ("ul", "ur", "lr", "ll")
        return {
            label: {"x": point[0], "y": point[1]}
            for label, point in zip(labels, corners, strict=True)
        }, True
    if isinstance(value, dict):
        normalized: dict[str, JSONValue] = {}
        for key in sorted(value, key=str):
            child, representable = _normalize_source_value(value[key])
            if not representable:
                return None, False
            normalized[str(key)] = child
        return normalized, True
    if isinstance(value, (list, tuple)):
        normalized_items: list[JSONValue] = []
        for child_value in value:
            child, representable = _normalize_source_value(child_value)
            if not representable:
                return None, False
            normalized_items.append(child)
        return tuple(normalized_items), True
    return None, False


def _bbox_from_unsupported_item(item: tuple[Any, ...]) -> dict[str, JSONValue] | None:
    points = _bbox_points_from_value(item[1:])
    if not points:
        return None
    return _bbox_from_points(tuple(points))


def _bbox_points_from_value(value: Any) -> list[tuple[float, float]]:
    if _looks_like_point(value):
        return [_point_tuple(value)]
    if _looks_like_rect(value):
        return [
            (_round_float(float(value.x0)), _round_float(float(value.y0))),
            (_round_float(float(value.x1)), _round_float(float(value.y1))),
        ]
    if _looks_like_quad(value):
        return _quad_corner_points(value)
    if (
        isinstance(value, tuple)
        and len(value) == 2
        and all(isinstance(component, int | float) for component in value)
    ):
        x, y = value
        return [(_round_float(float(x)), _round_float(float(y)))]
    if (
        isinstance(value, tuple)
        and len(value) == 4
        and all(isinstance(component, int | float) for component in value)
    ):
        x0, y0, x1, y1 = value
        return [
            (_round_float(float(x0)), _round_float(float(y0))),
            (_round_float(float(x1)), _round_float(float(y1))),
        ]
    if isinstance(value, dict):
        points: list[tuple[float, float]] = []
        for child in value.values():
            points.extend(_bbox_points_from_value(child))
        return points
    if isinstance(value, (list, tuple)):
        nested_points: list[tuple[float, float]] = []
        for child in value:
            nested_points.extend(_bbox_points_from_value(child))
        return nested_points
    return []


def _looks_like_point(value: Any) -> bool:
    return hasattr(value, "x") and hasattr(value, "y")


def _looks_like_rect(value: Any) -> bool:
    return all(hasattr(value, attr) for attr in ("x0", "y0", "x1", "y1"))


def _looks_like_quad(value: Any) -> bool:
    # PyMuPDF Quad: four corner Points (ul/ur/ll/lr); emitted for "qu" draw items.
    return not _looks_like_rect(value) and all(
        hasattr(value, attr) for attr in ("ul", "ur", "ll", "lr")
    )


def _quad_corner_points(value: Any) -> list[tuple[float, float]]:
    return [
        (_round_float(float(corner.x)), _round_float(float(corner.y)))
        for corner in (value.ul, value.ur, value.lr, value.ll)
    ]


def _line_geometry(
    *,
    start: dict[str, JSONValue],
    end: dict[str, JSONValue],
    bbox: dict[str, JSONValue],
    point_count: int,
    closed: bool,
) -> dict[str, JSONValue]:
    return {
        "kind": "line",
        "coordinate_space": "pdf_page_space_unrotated",
        "unit": "point",
        "bbox": bbox,
        "summary": _geometry_summary(point_count=point_count, closed=closed),
        "start": start,
        "end": end,
    }


def _polyline_geometry(
    *,
    points: tuple[dict[str, JSONValue], ...],
    bbox: dict[str, JSONValue],
    point_count: int,
    closed: bool,
) -> dict[str, JSONValue]:
    return {
        "kind": "polyline",
        "coordinate_space": "pdf_page_space_unrotated",
        "unit": "point",
        "bbox": bbox,
        "summary": _geometry_summary(point_count=point_count, closed=closed),
        "points": points,
    }


def _geometry_summary(*, point_count: int, closed: bool) -> dict[str, JSONValue]:
    return {
        "point_count": point_count,
        "segment_count": max(point_count - 1, 0),
        "closed": closed,
    }


def _availability_error(availability: AdapterAvailability) -> Exception:
    if availability.availability_reason is AvailabilityReason.MISSING_LICENSE:
        return PyMuPDFLicenseError(
            "PyMuPDF deployment review acknowledgement is required before extraction."
        )
    if availability.availability_reason is AvailabilityReason.PROBE_FAILED:
        return PyMuPDFAvailabilityError(
            availability_reason=AvailabilityReason.PROBE_FAILED,
            message="PyMuPDF adapter preflight reported unavailable.",
        )
    return RuntimeError("PyMuPDF adapter is unavailable for vector PDF extraction.")


def _raise_if_cancelled(options: AdapterExecutionOptions) -> None:
    if options.cancellation is not None and options.cancellation.is_cancelled():
        raise asyncio.CancelledError


async def _extract_with_process(
    source: AdapterSource,
    options: AdapterExecutionOptions,
) -> tuple[dict[str, JSONValue], list[AdapterWarning]]:
    request = _ProcessExtractionRequest(
        file_path=str(source.file_path),
        upload_format=source.upload_format.value,
        timeout_seconds=options.timeout.seconds if options.timeout is not None else None,
        limits=_ExtractionLimits.from_settings(),
    )
    try:
        handle = _start_extraction_process(request)
    except (AssertionError, OSError) as exc:
        # A daemonic parent (e.g. a Celery prefork pool worker) cannot spawn child
        # processes — multiprocessing raises AssertionError("daemonic processes are not
        # allowed to have children"). Subprocess isolation is unavailable here, so fall
        # back to in-process extraction (cooperative budget timeout still applies).
        return await _extract_in_current_process(request, reason=str(exc) or type(exc).__name__)
    try:
        envelope = await _wait_for_process_envelope(handle, options=options)
    finally:
        handle.close()
    return _decode_process_envelope(envelope)


async def _extract_in_current_process(
    request: _ProcessExtractionRequest,
    *,
    reason: str,
) -> tuple[dict[str, JSONValue], list[AdapterWarning]]:
    """Run extraction in-process when subprocess isolation is unavailable.

    Reuses the exact child-process code path and envelope decoding, so error and
    warning behaviour is identical to the isolated run. CPU-bound work is offloaded
    to a worker thread to keep the event loop responsive.
    """

    envelope = await asyncio.to_thread(_extract_in_child_process, request)
    canonical, warnings = _decode_process_envelope(envelope)
    warnings = [
        AdapterWarning(
            code="pymupdf_subprocess_isolation_unavailable",
            message=(
                "PyMuPDF extraction ran in-process because the worker could not spawn an "
                "isolated subprocess; crash isolation is degraded for this run."
            ),
            details={"reason": reason},
        ),
        *warnings,
    ]
    return canonical, warnings


def _start_extraction_process(request: _ProcessExtractionRequest) -> _ProcessExtractionHandle:
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(
        target=_run_process_extraction_child,
        args=(sender, request),
        daemon=True,
    )
    try:
        process.start()
    except Exception:
        receiver.close()
        sender.close()
        raise
    sender.close()
    return _ProcessExtractionHandle(process=process, receiver=receiver)


async def _wait_for_process_envelope(
    handle: _ProcessExtractionHandle,
    *,
    options: AdapterExecutionOptions,
) -> dict[str, Any]:
    deadline = perf_counter() + options.timeout.seconds if options.timeout is not None else None
    try:
        while True:
            _raise_if_cancelled(options)
            if handle.poll():
                envelope = handle.recv()
                await _join_process_handle(handle, _PROCESS_TERMINATE_GRACE_SECONDS)
                if handle.is_alive():
                    await _stop_process_handle(handle)
                return envelope
            if not handle.is_alive():
                await _join_process_handle(handle, _PROCESS_TERMINATE_GRACE_SECONDS)
                raise RuntimeError("PyMuPDF extraction failed.")
            if deadline is not None and perf_counter() >= deadline:
                raise TimeoutError("PyMuPDF extraction timed out.")
            await asyncio.sleep(_PROCESS_POLL_INTERVAL_SECONDS)
    except BaseException:
        await _stop_process_handle(handle)
        raise


async def _join_process_handle(
    handle: _ProcessExtractionHandle,
    timeout_seconds: float | None,
) -> None:
    await asyncio.to_thread(handle.join, timeout_seconds)


async def _stop_process_handle(handle: _ProcessExtractionHandle) -> None:
    if not handle.is_alive():
        await _join_process_handle(handle, _PROCESS_TERMINATE_GRACE_SECONDS)
        return
    handle.terminate()
    await _join_process_handle(handle, _PROCESS_TERMINATE_GRACE_SECONDS)
    if not handle.is_alive():
        return
    handle.kill()
    await _join_process_handle(handle, _PROCESS_KILL_GRACE_SECONDS)


def _run_process_extraction_child(sender: Any, request: _ProcessExtractionRequest) -> None:
    try:
        sender.send(_extract_in_child_process(request))
    except BaseException:
        with suppress(Exception):
            sender.send(
                {
                    "status": "error",
                    "kind": "failed",
                    "message": "PyMuPDF extraction failed.",
                }
            )
    finally:
        sender.close()


def _extract_in_child_process(request: _ProcessExtractionRequest) -> dict[str, Any]:
    timeout = (
        AdapterTimeout(seconds=request.timeout_seconds)
        if request.timeout_seconds is not None
        else None
    )
    source = AdapterSource(
        file_path=Path(request.file_path),
        upload_format=UploadFormat(request.upload_format),
        input_family=InputFamily.PDF_VECTOR,
    )
    options = AdapterExecutionOptions(timeout=timeout)
    budget = _ExtractionBudget(
        started_at=perf_counter(),
        timeout_seconds=request.timeout_seconds,
    )
    document: Any | None = None

    try:
        budget.checkpoint(options)
        runtime = _load_runtime_module()
        budget.checkpoint(options)
        document = _open_document(runtime, source.file_path)
        budget.checkpoint(options)
        canonical, warnings = _extract_document_canonical(
            document,
            source=source,
            options=options,
            budget=budget,
            limits=request.limits,
        )
    except TimeoutError:
        return {
            "status": "error",
            "kind": "timeout",
            "message": "PyMuPDF extraction timed out.",
        }
    except PyMuPDFExtractionLimitError as exc:
        return {"status": "error", "kind": "limit", "message": str(exc)}
    except ModuleNotFoundError as exc:
        if exc.name == _RUNTIME_MODULE:
            return {
                "status": "error",
                "kind": "dependency_missing",
                "dependency": _RUNTIME_MODULE,
            }
        return {
            "status": "error",
            "kind": "failed",
            "message": "PyMuPDF extraction failed.",
        }
    except Exception:
        return {
            "status": "error",
            "kind": "failed",
            "message": "PyMuPDF extraction failed.",
        }
    finally:
        if document is not None:
            _close_document(document)

    return {
        "status": "ok",
        "canonical": canonical,
        "warnings": [_warning_payload(warning) for warning in warnings],
    }


def _decode_process_envelope(
    envelope: dict[str, Any],
) -> tuple[dict[str, JSONValue], list[AdapterWarning]]:
    status = envelope.get("status")
    if status == "ok":
        canonical = cast(dict[str, JSONValue], envelope["canonical"])
        warnings = [
            AdapterWarning(
                code=str(item["code"]),
                message=str(item["message"]),
                details=cast(dict[str, JSONValue] | None, item.get("details")),
            )
            for item in cast(list[dict[str, Any]], envelope.get("warnings", []))
        ]
        return canonical, warnings

    kind = str(envelope.get("kind", "failed"))
    message = str(envelope.get("message", "PyMuPDF extraction failed."))
    if kind == "timeout":
        raise TimeoutError(message)
    if kind == "dependency_missing":
        raise ModuleNotFoundError(name=str(envelope.get("dependency", _RUNTIME_MODULE)))
    if kind == "limit":
        raise PyMuPDFExtractionLimitError(message)
    raise RuntimeError(message)


def _warning_payload(warning: AdapterWarning) -> dict[str, JSONValue]:
    payload: dict[str, JSONValue] = {
        "code": warning.code,
        "message": warning.message,
    }
    if warning.details is not None:
        payload["details"] = warning.details
    return payload


def _append_entity(
    entities: list[dict[str, JSONValue]],
    entity: dict[str, JSONValue],
    *,
    entity_limit: int,
) -> None:
    if len(entities) >= entity_limit:
        raise _EntityBudgetError("PyMuPDF extraction reached the entity cap.")
    entities.append(entity)


def _enforce_path_item_limit(items: Any) -> None:
    if hasattr(items, "__len__") and len(cast(Any, items)) > _MAX_PATH_ITEMS_PER_DRAWING:
        raise PyMuPDFExtractionLimitError("PyMuPDF extraction exceeded drawing path item limit.")


def _enforce_pending_point_limit(
    current_points: list[tuple[float, float]],
    *,
    additional_points: int,
) -> None:
    if len(current_points) + additional_points > _MAX_POINTS_PER_ENTITY:
        raise PyMuPDFExtractionLimitError("PyMuPDF extraction exceeded entity point limit.")


def _durable_source_ref(source: AdapterSource) -> str:
    candidate = _safe_source_name(source.original_name)
    if candidate is None:
        candidate = _safe_source_name(source.file_path.name)
    if candidate is None:
        candidate = "source"
    return f"originals/{candidate}"


def _safe_source_name(raw_name: str | None) -> str | None:
    if raw_name is None:
        return None
    stripped = raw_name.strip()
    if not stripped:
        return None
    candidate = PureWindowsPath(stripped).name.strip()
    if candidate in {"", ".", ".."}:
        return None
    return candidate


def _open_document(runtime: ModuleType, file_path: Path) -> Any:
    return runtime.open(file_path)


def _close_document(document: Any) -> None:
    close = getattr(document, "close", None)
    if callable(close):
        close()


def _license_acknowledged_from_env() -> bool:
    raw_value = os.getenv(_APPROVED_LICENSE_PROBES_ENV_VAR)
    if raw_value is None:
        return False

    acknowledged_probes = {
        candidate.strip() for candidate in raw_value.split(",") if candidate.strip()
    }
    return _LICENSE_PROBE_NAME in acknowledged_probes


def _package_version() -> str | None:
    try:
        return importlib.metadata.version(_DISTRIBUTION_NAME)
    except importlib.metadata.PackageNotFoundError:
        return None


def _runtime_version(runtime: ModuleType) -> str | None:
    for attr_name in ("__version__", "VersionBind", "version"):
        value = getattr(runtime, attr_name, None)
        if isinstance(value, str) and value:
            return value
    return _package_version()


def _load_runtime_module() -> ModuleType:
    return importlib.import_module(_RUNTIME_MODULE)


def _entity_id(*, page_number: int, drawing_index: int, entity_index: int) -> str:
    return f"page-{page_number}:drawing-{drawing_index}:entity-{entity_index}"


def _resolve_drawing_layer(drawing: Mapping[str, Any]) -> str:
    """Return the canonical layer name for a drawing.

    Honors an explicit ``layer`` field when present (the hook for native OCG layers, #414);
    otherwise derives a deterministic layer from the pen signature.
    """

    raw_layer = drawing.get("layer")
    if isinstance(raw_layer, str) and raw_layer.strip():
        return raw_layer.strip()
    return _pen_signature_layer_name(drawing)


def _pen_signature_layer_name(drawing: Mapping[str, Any]) -> str:
    """Derive a stable layer name from a drawing's pen signature (stroke colour + width).

    Plotted CAD PDFs encode layer identity in pen colour/weight (CTB/pen tables), so grouping
    by ``(colour, width)`` is a deterministic, faithful proxy for the original layers. The name
    is content-derived (not positional), so the same pen always maps to the same layer across
    runs and drawings. Stroke colour is preferred; fill colour is the fallback for fill-only
    paths; colourless paths fall back to the default layer name.
    """

    colour = drawing.get("color")
    if colour is None:
        colour = drawing.get("fill")
    width = _safe_pen_width(drawing.get("width"))
    hexcode = _rgb_hex(colour)
    if hexcode is None:
        return f"{_DEFAULT_LAYER_NAME}-w{width:g}"
    return f"pen-{hexcode}-w{width:g}"


def _safe_pen_width(raw_width: Any) -> float:
    # Layer naming runs before per-entity sanitization, so it must never raise on a
    # non-finite/invalid pen width; degrade to 0.0 (the entity build still skips it).
    try:
        width = float(raw_width) if raw_width is not None else 0.0
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(width):
        return 0.0
    return _round_float(width)


def _rgb_hex(colour: Any) -> str | None:
    if colour is None:
        return None
    try:
        components = [float(component) for component in colour]
    except (TypeError, ValueError):
        return None
    if len(components) < 3 or not all(math.isfinite(component) for component in components[:3]):
        return None
    channels = tuple(max(0, min(255, round(component * 255))) for component in components[:3])
    return "".join(f"{channel:02x}" for channel in channels)


def _point_tuple(raw_point: Any) -> tuple[float, float]:
    return (_round_float(float(raw_point.x)), _round_float(float(raw_point.y)))


def _normalize_points(
    points: list[tuple[float, float]],
    *,
    closed: bool,
) -> tuple[tuple[float, float], ...]:
    normalized: list[tuple[float, float]] = []
    for point in points:
        if not normalized or not _same_point(normalized[-1], point):
            normalized.append(point)
    if closed and normalized and not _same_point(normalized[0], normalized[-1]):
        normalized.append(normalized[0])
    return tuple(normalized)


def _same_point(left: tuple[float, float], right: tuple[float, float]) -> bool:
    return left[0] == right[0] and left[1] == right[1]


def _point_json(point: tuple[float, float]) -> dict[str, JSONValue]:
    return {"x": point[0], "y": point[1]}


def _bbox_from_points(points: tuple[tuple[float, float], ...]) -> dict[str, JSONValue]:
    xs = tuple(point[0] for point in points)
    ys = tuple(point[1] for point in points)
    return {
        "x_min": min(xs),
        "y_min": min(ys),
        "x_max": max(xs),
        "y_max": max(ys),
    }


def _bbox_from_rect(rect: Any) -> dict[str, JSONValue]:
    return {
        "x_min": _round_float(float(rect.x0)),
        "y_min": _round_float(float(rect.y0)),
        "x_max": _round_float(float(rect.x1)),
        "y_max": _round_float(float(rect.y1)),
    }


_POINTS_PER_INCH = 72.0
_MM_PER_INCH = 25.4

# Standard sheet sizes as (short_edge_mm, long_edge_mm); matched orientation-agnostically.
_STANDARD_SHEET_SIZES: dict[str, tuple[float, float]] = {
    "A0": (841.0, 1189.0),
    "A1": (594.0, 841.0),
    "A2": (420.0, 594.0),
    "A3": (297.0, 420.0),
    "A4": (210.0, 297.0),
    "A5": (148.0, 210.0),
    "ANSI A (Letter)": (215.9, 279.4),
    "ANSI Legal": (215.9, 355.6),
    "ANSI B": (279.4, 431.8),
    "ANSI C": (431.8, 558.8),
    "ANSI D": (558.8, 863.6),
    "ANSI E": (863.6, 1117.6),
}
# Tolerance for matching a standard sheet (mm); absorbs rounding and small print margins.
_SHEET_MATCH_TOLERANCE_MM = 3.0


def _points_to_mm(points: float) -> float:
    return _round_float(points / _POINTS_PER_INCH * _MM_PER_INCH)


def _match_sheet_size(width_mm: float, height_mm: float) -> str | None:
    short_edge, long_edge = sorted((width_mm, height_mm))
    for name, (std_short, std_long) in _STANDARD_SHEET_SIZES.items():
        if (
            abs(short_edge - std_short) <= _SHEET_MATCH_TOLERANCE_MM
            and abs(long_edge - std_long) <= _SHEET_MATCH_TOLERANCE_MM
        ):
            return name
    return None


def _paper_size(*, width_pt: float, height_pt: float, page_number: int) -> dict[str, JSONValue]:
    """Return deterministic paper-size metadata for a page from its MediaBox dimensions."""

    width_mm = _points_to_mm(width_pt)
    height_mm = _points_to_mm(height_pt)
    return {
        "page_number": page_number,
        "width_pt": _round_float(width_pt),
        "height_pt": _round_float(height_pt),
        "width_mm": width_mm,
        "height_mm": height_mm,
        "name": _match_sheet_size(width_mm, height_mm),
    }


# Scale ratio like "1:50". Time-safe: the look-arounds reject digits/colons on either side, so a
# timestamp ("17:52:20") does not match. Architectural scales (one side == 1) are required, which
# also rules out incidental "N:M" tokens.
_SCALE_RATIO_RE = re.compile(r"(?<![\d:])(\d{1,4})\s*:\s*(\d{1,4})(?![\d:])")
_SCALE_LABEL_RE = re.compile(r"(?i)\bscale\b")
_DO_NOT_SCALE_RE = re.compile(r"(?i)do\s+not\s+scale")
# Anchor the plan unit on "DIMENSIONS ... IN <unit>" so the "LEVELS ARE IN METRES" datum note is
# not mistaken for the plan unit.
_DIMENSION_UNIT_RE = re.compile(
    r"(?i)\bdimensions?\b[^.]{0,40}?\bin\b\s+(millimet\w*|centimet\w*|met\w*|mm|cm|m)\b"
)
_REAL_UNIT_PER_MM = {"millimeter": 1.0, "centimeter": 0.1, "meter": 0.001}


def _normalize_real_unit(token: str) -> str | None:
    normalized = token.strip().lower()
    if normalized.startswith("milli") or normalized == "mm":
        return "millimeter"
    if normalized.startswith("centi") or normalized == "cm":
        return "centimeter"
    if normalized.startswith("met") or normalized == "m":
        return "meter"
    return None


def _text_block_center(bbox: Any) -> tuple[float, float] | None:
    if not isinstance(bbox, Mapping):
        return None
    try:
        x = (float(bbox["x_min"]) + float(bbox["x_max"])) / 2
        y = (float(bbox["y_min"]) + float(bbox["y_max"])) / 2
    except (KeyError, TypeError, ValueError):
        return None
    return (x, y)


def _parse_scale_ratio(
    text_blocks: Sequence[Mapping[str, Any]],
) -> tuple[int, int, str, str] | None:
    """Return (numerator, denominator, source_text, confidence) for the drawing scale, if found.

    Only architectural ratios (one side == 1) are accepted. When a "Scale" label is present the
    nearest candidate is chosen and confidence is "high"; otherwise "medium".
    """

    candidates: list[tuple[int, int, Mapping[str, Any]]] = []
    label_blocks: list[Mapping[str, Any]] = []
    for block in text_blocks:
        text = str(block.get("text") or "")
        if _SCALE_LABEL_RE.search(text):
            label_blocks.append(block)
        for match in _SCALE_RATIO_RE.finditer(text):
            numerator, denominator = int(match.group(1)), int(match.group(2))
            if numerator >= 1 and denominator >= 1 and (numerator == 1 or denominator == 1):
                candidates.append((numerator, denominator, block))

    if not candidates:
        return None

    confidence = "medium"
    if label_blocks:
        confidence = "high"

        def _distance_to_label(candidate: tuple[int, int, Mapping[str, Any]]) -> float:
            center = _text_block_center(candidate[2].get("bbox"))
            if center is None:
                return math.inf
            best = math.inf
            for label in label_blocks:
                label_center = _text_block_center(label.get("bbox"))
                if label_center is not None:
                    best = min(best, math.dist(center, label_center))
            return best

        candidates.sort(key=_distance_to_label)

    numerator, denominator, block = candidates[0]
    return numerator, denominator, str(block.get("text") or ""), confidence


def _parse_real_world_unit(
    text_blocks: Sequence[Mapping[str, Any]],
) -> tuple[str, str] | None:
    for block in text_blocks:
        text = str(block.get("text") or "")
        match = _DIMENSION_UNIT_RE.search(text)
        if match is not None:
            unit = _normalize_real_unit(match.group(1))
            if unit is not None:
                return unit, text
    return None


def _derive_pdf_scale(text_blocks: Sequence[Mapping[str, Any]]) -> dict[str, JSONValue]:
    """Derive real-world scale/units from title-block/notes text.

    Geometry stays in PDF points; this records the ratio, real-world unit, and a point->real
    transform factor so consumers can convert without duplicating geometry. Degrades to
    ``status: unconfirmed`` when no confident scale is found.
    """

    scale: dict[str, JSONValue] = {
        "status": "unconfirmed",
        "coordinate_space": "pdf_page_space_unrotated",
        "unit": "point",
        "real_world_units": False,
    }

    if any(_DO_NOT_SCALE_RE.search(str(block.get("text") or "")) for block in text_blocks):
        scale["caveats"] = ("do_not_scale",)

    ratio = _parse_scale_ratio(text_blocks)
    if ratio is not None:
        numerator, denominator, ratio_source, confidence = ratio
        scale["scale_ratio"] = {
            "numerator": numerator,
            "denominator": denominator,
            "text": f"{numerator}:{denominator}",
        }
        scale["scale_ratio_source"] = ratio_source
        scale["confidence"] = confidence

    unit_result = _parse_real_world_unit(text_blocks)
    if unit_result is not None:
        scale["real_world_unit"] = unit_result[0]
        scale["real_world_unit_source"] = unit_result[1]

    if ratio is not None and unit_result is not None:
        numerator, denominator, _, _ = ratio
        unit = unit_result[0]
        points_to_mm = _MM_PER_INCH / _POINTS_PER_INCH
        points_to_real = points_to_mm * (denominator / numerator) * _REAL_UNIT_PER_MM[unit]
        scale["points_to_real"] = _round_float(points_to_real)
        scale["real_world_units"] = True
        scale["status"] = "derived_from_text"

    return scale


def _rect_tuple(value: Any) -> tuple[float, float, float, float]:
    if isinstance(value, tuple) and len(value) == 4:
        return cast(tuple[float, float, float, float], value)
    return (0.0, 0.0, 0.0, 0.0)


def _bbox_from_tuple(rect: tuple[float, float, float, float]) -> dict[str, JSONValue]:
    x0, y0, x1, y1 = rect
    return {
        "x_min": _round_float(float(x0)),
        "y_min": _round_float(float(y0)),
        "x_max": _round_float(float(x1)),
        "y_max": _round_float(float(y1)),
    }


def _color_tuple(raw_color: Any) -> tuple[float, ...] | None:
    if raw_color is None:
        return None
    return tuple(_round_float(float(component)) for component in raw_color)


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return _round_float(float(value))


def _round_float(value: float) -> float:
    if not math.isfinite(value):
        raise PyMuPDFNonFiniteValueError("PyMuPDF parser returned a non-finite number.")
    return round(value, 6)


__all__ = ["PyMuPDFAdapter", "create_adapter"]
# temp mutation for in-scope execution contract
