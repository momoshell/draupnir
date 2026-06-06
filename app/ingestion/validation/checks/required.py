"""Required validation checks for ingest finalization payloads."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from app.ingestion.canonical.entity_provenance import (
    EntityProvenanceError,
    validate_entity_provenance,
)

from .._constants import _SOURCE_DOCUMENT_REF  # noqa: TID252
from .._utils import (  # noqa: TID252
    _extract_meaningful_metadata,
    _metadata_candidate,
)
from ..geometry import (  # noqa: TID252
    _entity_has_valid_geometry,
    _entity_mappings,
    _entity_ref,
    _has_block_transform_content,
    _normalize_status_hint,
    _polygon_closed_state,
    _polygon_entities,
    _sequence_mappings,
    _xref_ref,
)
from ._common import _check, _not_applicable_check


def _build_required_checks(
    *,
    canonical_json: Mapping[str, Any],
    add_finding: Any,
) -> tuple[list[dict[str, Any]], bool, bool]:
    checks: list[dict[str, Any]] = []
    requires_review = False
    invalid = False

    for builder in (
        _build_units_check,
        _build_coordinate_system_check,
        _build_geometry_validity_check,
        _build_closed_polygon_check,
        _build_block_transform_check,
        _build_layer_mapping_check,
        _build_xref_check,
        _build_entity_provenance_check,
    ):
        check, check_requires_review, check_invalid = builder(
            canonical_json=canonical_json,
            add_finding=add_finding,
        )
        checks.append(check)
        requires_review = requires_review or check_requires_review
        invalid = invalid or check_invalid

    return checks, requires_review, invalid


def _build_units_check(
    *, canonical_json: Mapping[str, Any], add_finding: Any
) -> tuple[dict[str, Any], bool, bool]:
    check_key = "units_presence_normalization"
    units = _extract_meaningful_metadata(
        canonical_json,
        "units",
        "normalized_units",
        "unit",
        "unit_system",
        "measurement_unit",
    )
    if units is not None:
        return (
            _check(
                check_key=check_key,
                status="pass",
                summary_message="Units metadata is present and normalized.",
                details={"applicable": True, "units": units},
            ),
            False,
            False,
        )

    finding_ref = add_finding(
        check_key=check_key,
        severity="warning",
        message=(
            "Units metadata is missing or unnormalized and requires review before quantities run."
        ),
        target_type="revision",
        target_ref=_SOURCE_DOCUMENT_REF,
        quantity_effect="blocks_quantity",
        source="validator",
        details={"units_present": False},
    )
    return (
        _check(
            check_key=check_key,
            status="review_required",
            summary_message="Units metadata is missing or unnormalized.",
            finding_refs=[finding_ref],
            details={"applicable": True, "units_present": False},
        ),
        True,
        False,
    )


def _build_coordinate_system_check(
    *, canonical_json: Mapping[str, Any], add_finding: Any
) -> tuple[dict[str, Any], bool, bool]:
    check_key = "coordinate_system_capture"
    coordinate_system = _extract_meaningful_metadata(
        canonical_json,
        "coordinate_system",
        "coordinate_reference_system",
        "crs",
        "spatial_reference",
    )
    if coordinate_system is not None:
        return (
            _check(
                check_key=check_key,
                status="pass",
                summary_message="Coordinate system metadata is present.",
                details={"applicable": True, "coordinate_system": coordinate_system},
            ),
            False,
            False,
        )

    finding_ref = add_finding(
        check_key=check_key,
        severity="warning",
        message="Coordinate system metadata is missing and requires review before quantities run.",
        target_type="revision",
        target_ref=_SOURCE_DOCUMENT_REF,
        quantity_effect="blocks_quantity",
        source="validator",
        details={"coordinate_system_present": False},
    )
    return (
        _check(
            check_key=check_key,
            status="review_required",
            summary_message="Coordinate system metadata is missing.",
            finding_refs=[finding_ref],
            details={"applicable": True, "coordinate_system_present": False},
        ),
        True,
        False,
    )


def _build_geometry_validity_check(
    *, canonical_json: Mapping[str, Any], add_finding: Any
) -> tuple[dict[str, Any], bool, bool]:
    check_key = "geometry_validity"
    entities = _entity_mappings(canonical_json)
    geometry_hint = _normalize_status_hint(
        _metadata_candidate(canonical_json, "geometry_validity"),
        _metadata_candidate(canonical_json, "geometry_validation"),
        _metadata_candidate(canonical_json, "geometry_status"),
        _metadata_candidate(canonical_json, "geometry_valid"),
    )
    if geometry_hint is False:
        finding_ref = add_finding(
            check_key=check_key,
            severity="error",
            message="Geometry validity check reported invalid geometry.",
            target_type="revision",
            target_ref=_SOURCE_DOCUMENT_REF,
            quantity_effect="blocks_quantity",
            source="validator",
            details={"entity_count": len(entities)},
        )
        return (
            _check(
                check_key=check_key,
                status="fail",
                summary_message="Geometry validity check failed.",
                finding_refs=[finding_ref],
                details={
                    "applicable": True,
                    "entity_count": len(entities),
                    "geometry_valid": False,
                },
            ),
            False,
            True,
        )

    geometry_states = [_entity_has_valid_geometry(entity) for entity in entities]
    validated_entity_count = sum(state is True for state in geometry_states)

    if geometry_hint is True or (
        geometry_states and validated_entity_count == len(geometry_states)
    ):
        return (
            _check(
                check_key=check_key,
                status="pass",
                summary_message="Geometry validity reported no blocking issues.",
                details={
                    "applicable": True,
                    "entity_count": len(entities),
                    "validated_entity_count": validated_entity_count,
                    "geometry_valid": True,
                },
            ),
            False,
            False,
        )

    finding_ref = add_finding(
        check_key=check_key,
        severity="warning",
        message=(
            "Geometry validity could not be confirmed and requires review before quantities run."
        ),
        target_type="revision",
        target_ref=_SOURCE_DOCUMENT_REF,
        quantity_effect="blocks_quantity",
        source="validator",
        details={
            "entity_count": len(entities),
            "validated_entity_count": validated_entity_count,
        },
    )
    return (
        _check(
            check_key=check_key,
            status="review_required",
            summary_message="Geometry validity could not be confirmed.",
            finding_refs=[finding_ref],
            details={
                "applicable": True,
                "entity_count": len(entities),
                "validated_entity_count": validated_entity_count,
                "geometry_valid": None,
            },
        ),
        True,
        False,
    )


def _build_closed_polygon_check(
    *, canonical_json: Mapping[str, Any], add_finding: Any
) -> tuple[dict[str, Any], bool, bool]:
    check_key = "closed_polygon_eligibility_for_area_quantities"
    polygon_entities = _polygon_entities(canonical_json)
    polygon_hint = _normalize_status_hint(
        _metadata_candidate(canonical_json, "closed_polygon_eligibility_for_area_quantities"),
        _metadata_candidate(canonical_json, "polygon_closure"),
        _metadata_candidate(canonical_json, "area_quantity_eligibility"),
    )
    if not polygon_entities and polygon_hint is None:
        return (
            _not_applicable_check(
                check_key,
                "Closed polygon eligibility is not applicable because no area-bearing "
                "polygons were reported.",
            ),
            False,
            False,
        )

    if polygon_hint is False or any(
        _polygon_closed_state(entity) is False for entity in polygon_entities
    ):
        finding_ref = add_finding(
            check_key=check_key,
            severity="error",
            message=(
                "Area-bearing polygons are not closed and cannot drive deterministic quantities."
            ),
            target_type="revision",
            target_ref=_SOURCE_DOCUMENT_REF,
            quantity_effect="blocks_quantity",
            source="validator",
            details={"polygon_count": len(polygon_entities)},
        )
        return (
            _check(
                check_key=check_key,
                status="fail",
                summary_message="Closed polygon eligibility failed.",
                finding_refs=[finding_ref],
                details={
                    "applicable": True,
                    "polygon_count": len(polygon_entities),
                    "eligible": False,
                },
            ),
            False,
            True,
        )

    polygon_states = [_polygon_closed_state(entity) for entity in polygon_entities]
    if polygon_hint is True or (
        polygon_entities and all(state is True for state in polygon_states)
    ):
        return (
            _check(
                check_key=check_key,
                status="pass",
                summary_message="Closed polygon eligibility reported no blocking issues.",
                details={
                    "applicable": True,
                    "polygon_count": len(polygon_entities),
                    "eligible": True,
                },
            ),
            False,
            False,
        )

    finding_ref = add_finding(
        check_key=check_key,
        severity="warning",
        message=(
            "Polygon closure eligibility is incomplete and requires review before area "
            "quantities run."
        ),
        target_type="revision",
        target_ref=_SOURCE_DOCUMENT_REF,
        quantity_effect="blocks_quantity",
        source="validator",
        details={"polygon_count": len(polygon_entities)},
    )
    return (
        _check(
            check_key=check_key,
            status="review_required",
            summary_message="Polygon closure eligibility could not be confirmed.",
            finding_refs=[finding_ref],
            details={"applicable": True, "polygon_count": len(polygon_entities), "eligible": None},
        ),
        True,
        False,
    )


def _build_block_transform_check(
    *, canonical_json: Mapping[str, Any], add_finding: Any
) -> tuple[dict[str, Any], bool, bool]:
    check_key = "block_transform_validity"
    has_block_transforms = _has_block_transform_content(canonical_json)
    transform_hint = _normalize_status_hint(
        _metadata_candidate(canonical_json, "block_transform_validity"),
        _metadata_candidate(canonical_json, "transform_validity"),
        _metadata_candidate(canonical_json, "transforms_valid"),
    )
    if not has_block_transforms and transform_hint is None:
        return (
            _not_applicable_check(
                check_key,
                "Block transform validity is not applicable because no block references "
                "were reported.",
            ),
            False,
            False,
        )

    if transform_hint is False:
        finding_ref = add_finding(
            check_key=check_key,
            severity="error",
            message="Block transform validation reported invalid transforms.",
            target_type="revision",
            target_ref=_SOURCE_DOCUMENT_REF,
            quantity_effect="blocks_quantity",
            source="validator",
            details={"block_references_present": has_block_transforms},
        )
        return (
            _check(
                check_key=check_key,
                status="fail",
                summary_message="Block transform validity failed.",
                finding_refs=[finding_ref],
                details={
                    "applicable": True,
                    "block_references_present": has_block_transforms,
                    "transforms_valid": False,
                },
            ),
            False,
            True,
        )

    if transform_hint is True:
        return (
            _check(
                check_key=check_key,
                status="pass",
                summary_message="Block transform validity reported no blocking issues.",
                details={
                    "applicable": True,
                    "block_references_present": has_block_transforms,
                    "transforms_valid": True,
                },
            ),
            False,
            False,
        )

    finding_ref = add_finding(
        check_key=check_key,
        severity="warning",
        message=(
            "Block transform validity could not be confirmed and requires review before "
            "quantities run."
        ),
        target_type="revision",
        target_ref=_SOURCE_DOCUMENT_REF,
        quantity_effect="blocks_quantity",
        source="validator",
        details={"block_references_present": has_block_transforms},
    )
    return (
        _check(
            check_key=check_key,
            status="review_required",
            summary_message="Block transform validity could not be confirmed.",
            finding_refs=[finding_ref],
            details={
                "applicable": True,
                "block_references_present": has_block_transforms,
                "transforms_valid": None,
            },
        ),
        True,
        False,
    )


def _build_layer_mapping_check(
    *, canonical_json: Mapping[str, Any], add_finding: Any
) -> tuple[dict[str, Any], bool, bool]:
    check_key = "layer_mapping_completeness"
    entities = _entity_mappings(canonical_json)
    layer_mapping_hint = _normalize_status_hint(
        _metadata_candidate(canonical_json, "layer_mapping_completeness"),
        _metadata_candidate(canonical_json, "layer_mapping"),
        _metadata_candidate(canonical_json, "layer_map"),
    )
    used_layer_names: set[str] = set()
    used_layer_refs: set[str] = set()
    for entity in entities:
        for layer_name in (entity.get("layer"), entity.get("layer_name")):
            if isinstance(layer_name, str):
                normalized_name = layer_name.strip()
                if normalized_name:
                    used_layer_names.add(normalized_name)
        layer_ref = entity.get("layer_ref")
        if isinstance(layer_ref, str):
            normalized_ref = layer_ref.strip()
            if normalized_ref:
                used_layer_refs.add(normalized_ref)

    declared_layer_names: set[str] = set()
    declared_layer_refs: set[str] = set()
    declared_layer_ref_fallbacks: set[str] = set()
    for layer in _sequence_mappings(canonical_json.get("layers")):
        layer_names: set[str] = set()
        layer_refs: set[str] = set()
        for layer_name in (layer.get("name"), layer.get("layer_name")):
            if isinstance(layer_name, str):
                normalized_name = layer_name.strip()
                if normalized_name:
                    layer_names.add(normalized_name)
        for layer_ref in (layer.get("ref"), layer.get("layer_ref")):
            if isinstance(layer_ref, str):
                normalized_ref = layer_ref.strip()
                if normalized_ref:
                    layer_refs.add(normalized_ref)

        declared_layer_names.update(layer_names)
        declared_layer_refs.update(layer_refs)
        if not layer_refs:
            declared_layer_ref_fallbacks.update(layer_names)

    used_layers = used_layer_names | used_layer_refs
    declared_layers = declared_layer_names | declared_layer_refs
    missing_layers = (
        sorted(
            (used_layer_names - declared_layer_names)
            | (used_layer_refs - (declared_layer_refs | declared_layer_ref_fallbacks))
        )
        if declared_layers
        else sorted(used_layers)
    )

    if layer_mapping_hint is True:
        return (
            _check(
                check_key=check_key,
                status="pass",
                summary_message="Layer mapping completeness reported no blocking issues.",
                details={
                    "applicable": True,
                    "used_layers": sorted(used_layers),
                    "declared_layers": sorted(declared_layers),
                },
            ),
            False,
            False,
        )

    if layer_mapping_hint is False:
        finding_ref = add_finding(
            check_key=check_key,
            severity="warning",
            message=(
                "Layer mapping completeness reported incomplete mappings and requires "
                "review before quantities run."
            ),
            target_type="revision",
            target_ref=_SOURCE_DOCUMENT_REF,
            quantity_effect="blocks_quantity",
            source="validator",
            details={
                "used_layers": sorted(used_layers),
                "declared_layers": sorted(declared_layers),
            },
        )
        return (
            _check(
                check_key=check_key,
                status="review_required",
                summary_message="Layer mapping completeness reported incomplete mappings.",
                finding_refs=[finding_ref],
                details={
                    "applicable": True,
                    "used_layers": sorted(used_layers),
                    "declared_layers": sorted(declared_layers),
                },
            ),
            True,
            False,
        )

    if entities and declared_layers and used_layers and not missing_layers:
        return (
            _check(
                check_key=check_key,
                status="pass",
                summary_message="Layer mapping completeness reported no blocking issues.",
                details={
                    "applicable": True,
                    "used_layers": sorted(used_layers),
                    "declared_layers": sorted(declared_layers),
                },
            ),
            False,
            False,
        )

    finding_ref = add_finding(
        check_key=check_key,
        severity="warning",
        message=(
            "Layer mapping completeness is missing or incomplete and requires review "
            "before quantities run."
        ),
        target_type="revision",
        target_ref=_SOURCE_DOCUMENT_REF,
        quantity_effect="blocks_quantity",
        source="validator",
        details={
            "used_layers": sorted(used_layers),
            "declared_layers": sorted(declared_layers),
            "missing_layers": missing_layers,
        },
    )
    return (
        _check(
            check_key=check_key,
            status="review_required",
            summary_message="Layer mapping completeness is missing or incomplete.",
            finding_refs=[finding_ref],
            details={
                "applicable": True,
                "used_layers": sorted(used_layers),
                "declared_layers": sorted(declared_layers),
                "missing_layers": missing_layers,
            },
        ),
        True,
        False,
    )


def _build_xref_check(
    *, canonical_json: Mapping[str, Any], add_finding: Any
) -> tuple[dict[str, Any], bool, bool]:
    check_key = "xref_resolution_status"
    xref_value = _metadata_candidate(
        canonical_json,
        "xref_resolution_status",
        "xrefs",
        "external_references",
    )
    xref_hint = _normalize_status_hint(xref_value)
    xrefs = _sequence_mappings(xref_value)
    if xref_hint is True:
        return (
            _check(
                check_key=check_key,
                status="pass",
                summary_message="Xref resolution reported no blocking issues.",
                details={"applicable": True, "xref_count": len(xrefs)},
            ),
            False,
            False,
        )

    if isinstance(xref_value, (list, tuple)) and not xrefs:
        return (
            _not_applicable_check(
                check_key,
                "Xref resolution is not applicable because no xrefs were reported.",
            ),
            False,
            False,
        )

    if xrefs:
        unresolved_refs = [
            _xref_ref(xref, index=index)
            for index, xref in enumerate(xrefs, start=1)
            if _normalize_status_hint(
                xref.get("resolved"),
                xref.get("status"),
                xref.get("resolution_status"),
            )
            is not True
        ]
        if not unresolved_refs:
            return (
                _check(
                    check_key=check_key,
                    status="pass",
                    summary_message="Xref resolution reported no blocking issues.",
                    details={"applicable": True, "xref_count": len(xrefs), "unresolved_refs": []},
                ),
                False,
                False,
            )

        finding_ref = add_finding(
            check_key=check_key,
            severity="warning",
            message="One or more xrefs are unresolved and require review before quantities run.",
            target_type="revision",
            target_ref=_SOURCE_DOCUMENT_REF,
            quantity_effect="blocks_quantity",
            source="validator",
            details={"xref_count": len(xrefs), "unresolved_refs": unresolved_refs},
        )
        return (
            _check(
                check_key=check_key,
                status="review_required",
                summary_message="Xref resolution is incomplete.",
                finding_refs=[finding_ref],
                details={
                    "applicable": True,
                    "xref_count": len(xrefs),
                    "unresolved_refs": unresolved_refs,
                },
            ),
            True,
            False,
        )

    finding_ref = add_finding(
        check_key=check_key,
        severity="warning",
        message="Xref resolution status is missing and requires review before quantities run.",
        target_type="revision",
        target_ref=_SOURCE_DOCUMENT_REF,
        quantity_effect="blocks_quantity",
        source="validator",
        details={"xref_present": False},
    )
    return (
        _check(
            check_key=check_key,
            status="review_required",
            summary_message="Xref resolution status is missing.",
            finding_refs=[finding_ref],
            details={"applicable": True, "xref_present": False},
        ),
        True,
        False,
    )


def _build_entity_provenance_check(
    *, canonical_json: Mapping[str, Any], add_finding: Any
) -> tuple[dict[str, Any], bool, bool]:
    check_key = "entity_provenance_contract"
    entities = _entity_mappings(canonical_json)
    if not entities:
        return (
            _not_applicable_check(
                check_key,
                "Entity provenance contract is not applicable because no entities were reported.",
            ),
            False,
            False,
        )

    invalid_entities: list[dict[str, str]] = []
    for index, entity in enumerate(entities, start=1):
        try:
            provenance = entity.get("provenance")
            if not isinstance(provenance, Mapping):
                raise EntityProvenanceError("Entity provenance must be a mapping.")
            validate_entity_provenance(provenance)
        except EntityProvenanceError as exc:
            invalid_entities.append(
                {
                    "entity_ref": _entity_ref(entity, index=index),
                    "error": str(exc),
                }
            )

    if not invalid_entities:
        return (
            _check(
                check_key=check_key,
                status="pass",
                summary_message="Entity provenance contract reported no blocking issues.",
                details={
                    "applicable": True,
                    "entity_count": len(entities),
                    "validated_entity_count": len(entities),
                },
            ),
            False,
            False,
        )

    finding_ref = add_finding(
        check_key=check_key,
        severity="error",
        message="One or more canonical entities failed the provenance contract.",
        target_type="revision",
        target_ref=_SOURCE_DOCUMENT_REF,
        quantity_effect="blocks_quantity",
        source="validator",
        details={
            "entity_count": len(entities),
            "invalid_entity_count": len(invalid_entities),
            "invalid_entities": invalid_entities,
        },
    )
    return (
        _check(
            check_key=check_key,
            status="fail",
            summary_message="Entity provenance contract failed.",
            finding_refs=[finding_ref],
            details={
                "applicable": True,
                "entity_count": len(entities),
                "invalid_entity_count": len(invalid_entities),
                "invalid_entities": invalid_entities,
            },
        ),
        False,
        True,
    )
