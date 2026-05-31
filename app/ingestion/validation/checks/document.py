"""Document-level validation checks."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from app.ingestion.contracts import InputFamily

from .._constants import (  # noqa: TID252
    _PDF_SCALE_UNCONFIRMED_VALUES,
    _SOURCE_DOCUMENT_REF,
    _SUPPORTED_IFC_SCHEMAS,
)
from .._utils import _is_meaningful_value, _json_compatible, _mapping_value  # noqa: TID252
from ..geometry import _normalize_status_hint  # noqa: TID252
from ._common import _check, _not_applicable_check


def _build_pdf_scale_check(
    *,
    input_family: InputFamily,
    canonical_json: Mapping[str, Any],
    add_finding: Any,
) -> tuple[dict[str, Any], bool]:
    check_key = "pdf_scale_presence_calibration_status"
    if input_family not in {InputFamily.PDF_VECTOR, InputFamily.PDF_RASTER}:
        return (
            _not_applicable_check(
                check_key,
                "PDF scale calibration is not applicable for this input family.",
            ),
            False,
        )

    scale_status, scale_value = _extract_pdf_scale(canonical_json)
    if scale_status == "present" and scale_value is not None:
        return (
            _check(
                check_key=check_key,
                status="pass",
                summary_message="PDF scale metadata is present.",
                details={"applicable": True, "scale_present": True, "scale": scale_value},
            ),
            False,
        )

    if scale_status == "invalid":
        finding_ref = add_finding(
            check_key=check_key,
            severity="error",
            message="PDF scale metadata is invalid and blocks deterministic quantities.",
            target_type="revision",
            target_ref=_SOURCE_DOCUMENT_REF,
            quantity_effect="blocks_quantity",
            source="validator",
            details={"input_family": input_family.value, "scale_present": True},
        )
        return (
            _check(
                check_key=check_key,
                status="fail",
                summary_message="PDF scale metadata is invalid.",
                finding_refs=[finding_ref],
                details={
                    "applicable": True,
                    "scale_present": True,
                    "calibration_status": "invalid",
                },
            ),
            False,
        )

    if scale_status == "unconfirmed":
        finding_ref = add_finding(
            check_key=check_key,
            severity="warning",
            message="PDF scale metadata is unconfirmed and requires review before quantities run.",
            target_type="revision",
            target_ref=_SOURCE_DOCUMENT_REF,
            quantity_effect="blocks_quantity",
            source="validator",
            details={"input_family": input_family.value, "scale_present": True},
        )
        return (
            _check(
                check_key=check_key,
                status="review_required",
                summary_message="PDF scale metadata is unconfirmed.",
                finding_refs=[finding_ref],
                details={
                    "applicable": True,
                    "scale_present": True,
                    "calibration_status": "unconfirmed",
                },
            ),
            True,
        )

    finding_ref = add_finding(
        check_key=check_key,
        severity="warning",
        message="PDF scale metadata is missing and requires review before quantities run.",
        target_type="revision",
        target_ref=_SOURCE_DOCUMENT_REF,
        quantity_effect="blocks_quantity",
        source="validator",
        details={"input_family": input_family.value, "scale_present": False},
    )
    return (
        _check(
            check_key=check_key,
            status="review_required",
            summary_message="PDF scale metadata is missing.",
            finding_refs=[finding_ref],
            details={"applicable": True, "scale_present": False, "calibration_status": "missing"},
        ),
        True,
    )


def _build_ifc_schema_check(
    *,
    input_family: InputFamily,
    canonical_json: Mapping[str, Any],
    add_finding: Any,
) -> tuple[dict[str, Any], bool]:
    check_key = "ifc_schema_support"
    if input_family != InputFamily.IFC:
        return (
            _not_applicable_check(
                check_key,
                "IFC schema support is not applicable for this input family.",
            ),
            False,
        )

    schema = _extract_ifc_schema(canonical_json)
    if schema is None:
        finding_ref = add_finding(
            check_key=check_key,
            severity="error",
            message="IFC schema metadata is missing.",
            target_type="revision",
            target_ref=_SOURCE_DOCUMENT_REF,
            quantity_effect="blocks_quantity",
            source="validator",
            details={"schema_present": False, "supported_schemas": sorted(_SUPPORTED_IFC_SCHEMAS)},
        )
        return (
            {
                "check_key": check_key,
                "status": "fail",
                "summary_message": "IFC schema metadata is missing.",
                "finding_refs": [finding_ref],
                "details": {
                    "applicable": True,
                    "schema_present": False,
                    "supported": False,
                },
            },
            True,
        )

    normalized_schema = schema.upper()
    if normalized_schema not in _SUPPORTED_IFC_SCHEMAS:
        finding_ref = add_finding(
            check_key=check_key,
            severity="error",
            message="IFC schema is not supported for deterministic quantity generation.",
            target_type="revision",
            target_ref=_SOURCE_DOCUMENT_REF,
            quantity_effect="blocks_quantity",
            source="validator",
            details={
                "schema_present": True,
                "schema": schema,
                "supported_schemas": sorted(_SUPPORTED_IFC_SCHEMAS),
            },
        )
        return (
            {
                "check_key": check_key,
                "status": "fail",
                "summary_message": "IFC schema is not supported.",
                "finding_refs": [finding_ref],
                "details": {
                    "applicable": True,
                    "schema_present": True,
                    "schema": schema,
                    "supported": False,
                },
            },
            True,
        )

    return (
        {
            "check_key": check_key,
            "status": "pass",
            "summary_message": "IFC schema is supported.",
            "finding_refs": [],
            "details": {
                "applicable": True,
                "schema_present": True,
                "schema": schema,
                "supported": True,
            },
        },
        False,
    )


def _extract_pdf_scale(canonical_json: Mapping[str, Any]) -> tuple[str, Any | None]:
    candidates: tuple[Any | None, ...] = (
        canonical_json.get("pdf_scale"),
        canonical_json.get("document_scale"),
        canonical_json.get("scale"),
        _mapping_value(canonical_json.get("metadata"), "pdf_scale"),
        _mapping_value(canonical_json.get("metadata"), "document_scale"),
        _mapping_value(canonical_json.get("metadata"), "scale"),
    )
    for candidate in candidates:
        if candidate is None:
            continue
        explicit_status = _explicit_pdf_scale_status(candidate)
        if explicit_status is not None:
            return explicit_status, None
        if _is_valid_pdf_scale(candidate):
            return "present", _json_compatible(candidate)
        if _is_unconfirmed_pdf_scale(candidate):
            return "unconfirmed", None
        return "invalid", None

    return "missing", None


def _extract_ifc_schema(canonical_json: Mapping[str, Any]) -> str | None:
    candidates = (
        canonical_json.get("ifc_schema"),
        canonical_json.get("ifc_schema_version"),
        _mapping_value(canonical_json.get("metadata"), "ifc_schema"),
        _mapping_value(canonical_json.get("metadata"), "ifc_schema_version"),
    )
    for candidate in candidates:
        if candidate is not None:
            return str(candidate)

    return None


def _is_valid_pdf_scale(candidate: Any) -> bool:
    if isinstance(candidate, bool):
        return False
    if isinstance(candidate, (int, float)):
        return candidate > 0
    if isinstance(candidate, str):
        return (
            bool(candidate.strip())
            and candidate.strip().lower() not in _PDF_SCALE_UNCONFIRMED_VALUES
        )
    if isinstance(candidate, Mapping):
        if not candidate:
            return False
        if _normalize_status_hint(candidate.get("confirmed"), candidate.get("calibrated")) is False:
            return False
        return any(
            _is_meaningful_value(candidate.get(key))
            for key in ("scale", "ratio", "value", "factor", "numerator", "denominator")
        )
    if isinstance(candidate, (list, tuple)):
        return bool(candidate)

    return False


def _explicit_pdf_scale_status(candidate: Any) -> str | None:
    if not isinstance(candidate, Mapping):
        return None

    explicit_status = _normalize_status_hint(
        candidate.get("status"),
        candidate.get("calibration_status"),
    )
    if explicit_status is False:
        return "invalid"
    for value in (candidate.get("status"), candidate.get("calibration_status")):
        if isinstance(value, str) and value.strip().lower() in _PDF_SCALE_UNCONFIRMED_VALUES:
            return "unconfirmed"
    if _normalize_status_hint(candidate.get("confirmed"), candidate.get("calibrated")) is False:
        return "unconfirmed"

    return None


def _is_unconfirmed_pdf_scale(candidate: Any) -> bool:
    if isinstance(candidate, str):
        return candidate.strip().lower() in _PDF_SCALE_UNCONFIRMED_VALUES
    if isinstance(candidate, Mapping):
        status = candidate.get("status") or candidate.get("calibration_status")
        if isinstance(status, str) and status.strip().lower() in _PDF_SCALE_UNCONFIRMED_VALUES:
            return True
        return (
            _normalize_status_hint(candidate.get("confirmed"), candidate.get("calibrated")) is False
        )

    return False
