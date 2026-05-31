"""Validation constants for ingest validation policy."""

from __future__ import annotations

from typing import Final

VALIDATION_REPORT_SCHEMA_VERSION: Final[str] = "0.1"
RUNNER_VALIDATOR_NAME: Final[str] = "ingestion.runner"
RUNNER_VALIDATOR_VERSION: Final[str] = "0.1"

_REVIEW_THRESHOLD: Final[float] = 0.60
_APPROVED_THRESHOLD: Final[float] = 0.95
_REVIEW_CAPPED_CONFIDENCE: Final[float] = 0.59
_SUPPORTED_IFC_SCHEMAS: Final[frozenset[str]] = frozenset({"IFC2X3", "IFC4", "IFC4X3"})
_SOURCE_DOCUMENT_REF: Final[str] = "source-document"
_REVIEW_STATUS_VALUES: Final[frozenset[str]] = frozenset(
    {
        "incomplete",
        "manual_required",
        "missing",
        "pending",
        "review_required",
        "unconfirmed",
        "unknown",
        "unresolved",
    }
)
_FAIL_STATUS_VALUES: Final[frozenset[str]] = frozenset(
    {"error", "fail", "failed", "false", "invalid", "rejected", "unsupported"}
)
_PASS_STATUS_VALUES: Final[frozenset[str]] = frozenset(
    {
        "captured",
        "complete",
        "confirmed",
        "normalized",
        "ok",
        "pass",
        "passed",
        "present",
        "resolved",
        "supported",
        "true",
        "valid",
    }
)
_PDF_SCALE_UNCONFIRMED_VALUES: Final[frozenset[str]] = frozenset(
    {"manual_required", "pending", "placeholder", "tbd", "unconfirmed", "unknown"}
)
_PLACEHOLDER_ADAPTER_MODE_VALUES: Final[frozenset[str]] = frozenset(
    {"placeholder", "sparse_placeholder"}
)
_PLACEHOLDER_STATUS_VALUES: Final[frozenset[str]] = frozenset(
    {"placeholder", "scaffold", "sparse", "synthetic"}
)
_PLACEHOLDER_EMPTY_ENTITY_REASONS: Final[frozenset[str]] = frozenset(
    {"placeholder_canonical_no_entity_mapping", "raster_vectorization_deferred"}
)
_CENTER_RADIUS_ENTITY_KINDS: Final[frozenset[str]] = frozenset({"arc", "circle"})
_LINE_ENTITY_KINDS: Final[frozenset[str]] = frozenset({"line"})
_POINT_ENTITY_KINDS: Final[frozenset[str]] = frozenset({"point"})
_POLYGON_ENTITY_KINDS: Final[frozenset[str]] = frozenset(
    {"hatch", "lwpolyline", "polygon", "polyline", "solid"}
)
_BLOCK_REFERENCE_ENTITY_KINDS: Final[frozenset[str]] = frozenset({"block_reference", "insert"})
_REQUIRED_CHECK_KEYS: Final[tuple[str, ...]] = (
    "units_presence_normalization",
    "coordinate_system_capture",
    "geometry_validity",
    "closed_polygon_eligibility_for_area_quantities",
    "block_transform_validity",
    "layer_mapping_completeness",
    "xref_resolution_status",
    "entity_provenance_contract",
    "pdf_scale_presence_calibration_status",
    "ifc_schema_support",
)
