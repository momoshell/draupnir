"""Conservative PyMuPDF vector PDF adapter."""

from __future__ import annotations

import asyncio
import importlib
import importlib.metadata
import math
import multiprocessing
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, replace
from pathlib import Path, PureWindowsPath
from time import perf_counter
from types import ModuleType
from typing import Any, cast

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
_RUNTIME_MODULE = "fitz"
_SCHEMA_VERSION = "0.1"
_DEFAULT_LAYER_NAME = "default"
_VECTOR_CONFIDENCE_SCORE = 0.75
_MAX_PAGES = 256
_MAX_ENTITIES = 20_000
_MAX_DRAWINGS_PER_PAGE = 5_000
_MAX_TOTAL_DRAWINGS = 20_000
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


class PyMuPDFNonFiniteValueError(ValueError):
    """Raised when parser output contains a non-finite number."""


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
        license_acknowledged=license_acknowledged or _license_unacknowledged,
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
                score=_VECTOR_CONFIDENCE_SCORE,
                review_required=True,
                basis="vector_pdf_unconfirmed_scale",
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
) -> tuple[dict[str, JSONValue], list[AdapterWarning]]:
    entities: list[dict[str, JSONValue]] = []
    layouts: list[dict[str, JSONValue]] = []
    text_blocks: list[dict[str, JSONValue]] = []
    warnings: list[AdapterWarning] = []
    layer_names: list[str] = [_DEFAULT_LAYER_NAME]
    text_bytes = 0
    total_drawings = 0

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

        budget.checkpoint(options)
        drawings = cast(list[dict[str, Any]], page.get_drawings())
        budget.checkpoint(options)
        _enforce_drawings_limit(drawings, total_drawings=total_drawings)
        total_drawings += len(drawings)
        page_entities, page_warnings, page_layers = _extract_page_entities(
            drawings,
            page_number=page_number,
            layout_name=layout_name,
            options=options,
            budget=budget,
            entity_limit=_MAX_ENTITIES - len(entities),
        )
        entities.extend(page_entities)
        warnings.extend(page_warnings)
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
        "pdf_scale": {
            "status": "unconfirmed",
            "coordinate_space": "pdf_page_space_unrotated",
            "unit": "point",
            "real_world_units": False,
        },
        "text_blocks": tuple(text_blocks),
    }

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
) -> tuple[list[dict[str, JSONValue]], list[AdapterWarning], list[str]]:
    entities: list[dict[str, JSONValue]] = []
    warnings: list[AdapterWarning] = []
    layer_names: list[str] = []

    if entity_limit < 0:
        raise PyMuPDFExtractionLimitError("PyMuPDF extraction exceeded entity limit.")

    for drawing_index, drawing in enumerate(drawings):
        budget.checkpoint(options)
        layer_name = _layer_name(drawing.get("layer"))
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

    return entities, warnings, layer_names


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
        "provenance": {
            "page_number": page_number,
            "drawing_index": drawing_index,
            "item_indices": item_indices,
            "source": "pymupdf.get_drawings",
        },
        "confidence": {
            "score": _VECTOR_CONFIDENCE_SCORE,
            "basis": "vector_path_segment",
        },
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
    return {
        "path_type": str(drawing.get("type", "unknown")),
        "stroke_width": _round_float(float(drawing.get("width", 0.0))),
        "stroke_color_rgb": _color_tuple(drawing.get("color")),
        "stroke_opacity": _optional_float(drawing.get("stroke_opacity")),
        "fill_color_rgb": _color_tuple(drawing.get("fill")),
        "fill_opacity": _optional_float(drawing.get("fill_opacity")),
        "line_cap": tuple(
            int(value)
            for value in cast(tuple[int, ...], drawing.get("lineCap", ()))
        ),
        "line_join": _optional_float(drawing.get("lineJoin")),
        "dashes": str(drawing.get("dashes", "")),
        "closed": closed,
        "rect_like": rect_like,
        "sequence_number": int(drawing.get("seqno", 0)),
    }


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
    handle = _start_extraction_process(
        _ProcessExtractionRequest(
            file_path=str(source.file_path),
            upload_format=source.upload_format.value,
            timeout_seconds=options.timeout.seconds if options.timeout is not None else None,
        )
    )
    try:
        envelope = await _wait_for_process_envelope(handle, options=options)
    finally:
        handle.close()
    return _decode_process_envelope(envelope)


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
    deadline = (
        perf_counter() + options.timeout.seconds if options.timeout is not None else None
    )
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
        raise PyMuPDFExtractionLimitError("PyMuPDF extraction exceeded entity limit.")
    entities.append(entity)


def _enforce_drawings_limit(drawings: list[dict[str, Any]], *, total_drawings: int) -> None:
    drawing_count = len(drawings)
    if drawing_count > _MAX_DRAWINGS_PER_PAGE:
        raise PyMuPDFExtractionLimitError("PyMuPDF extraction exceeded page drawing limit.")
    if total_drawings + drawing_count > _MAX_TOTAL_DRAWINGS:
        raise PyMuPDFExtractionLimitError("PyMuPDF extraction exceeded total drawing limit.")


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


def _license_unacknowledged() -> bool:
    return False


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


def _layer_name(raw_layer: Any) -> str:
    if isinstance(raw_layer, str) and raw_layer.strip():
        return raw_layer.strip()
    return _DEFAULT_LAYER_NAME


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
