"""Regression contract for the revision API route table."""

from annotated_types import Ge, Le
from fastapi.routing import APIRoute
from pydantic_core import PydanticUndefined

from app.api.v1.revisions import revisions_router

type RouteParameterContract = tuple[
    str,
    str | None,
    bool | None,
    object | None,
    object | None,
    object | None,
    str | None,
]


def _parameter_contract(route: APIRoute) -> tuple[RouteParameterContract, ...]:
    """Return path/query/dependency metadata for a route."""

    parameter_contract: list[RouteParameterContract] = []

    for parameter in route.dependant.path_params:
        ge: object | None = None
        le: object | None = None
        for metadata in parameter.field_info.metadata:
            if isinstance(metadata, Ge):
                ge = metadata.ge
            elif isinstance(metadata, Le):
                le = metadata.le

        required = parameter.default is PydanticUndefined
        default = None if required else parameter.default
        parameter_contract.append(("path", parameter.name, required, default, ge, le, None))

    for parameter in route.dependant.query_params:
        ge = None
        le = None
        for metadata in parameter.field_info.metadata:
            if isinstance(metadata, Ge):
                ge = metadata.ge
            elif isinstance(metadata, Le):
                le = metadata.le

        required = parameter.default is PydanticUndefined
        default = None if required else parameter.default
        parameter_contract.append(("query", parameter.name, required, default, ge, le, None))

    for dependency in route.dependant.dependencies:
        dependency_name = dependency.call.__name__ if dependency.call else None
        parameter_contract.append(
            (
                "dependency",
                dependency.name,
                None,
                None,
                None,
                None,
                dependency_name,
            )
        )

    return tuple(parameter_contract)


def _route_contract() -> list[
    tuple[
        str,
        str,
        str,
        str,
        str | None,
        int | None,
        tuple[RouteParameterContract, ...],
    ]
]:
    """Return ordered revision route contract tuples."""

    contract: list[
        tuple[
            str,
            str,
            str,
            str,
            str | None,
            int | None,
            tuple[RouteParameterContract, ...],
        ]
    ] = []
    for route in revisions_router.routes:
        if not isinstance(route, APIRoute):
            continue

        method = next(iter(route.methods))
        response_model_name = (
            route.response_model.__name__ if route.response_model is not None else None
        )
        contract.append(
            (
                method,
                route.path,
                route.name,
                route.endpoint.__name__,
                response_model_name,
                route.status_code,
                _parameter_contract(route),
            )
        )

    return contract


def test_revision_router_routes_match_baseline_contract() -> None:
    # Arrange
    expected_contract: list[
        tuple[
            str,
            str,
            str,
            str,
            str | None,
            int | None,
            tuple[RouteParameterContract, ...],
        ]
    ] = [
        (
            "GET",
            "/files/{file_id}/revisions",
            "list_file_revisions",
            "list_file_revisions",
            "DrawingRevisionListResponse",
            None,
            (
                ("path", "file_id", True, None, None, None, None),
                ("query", "limit", False, 50, 1, 200, None),
                ("query", "cursor", False, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/scale",
            "get_revision_scale",
            "get_revision_scale",
            "RevisionScaleRead",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/summary",
            "get_revision_summary",
            "get_revision_summary",
            "RevisionSummaryRead",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/interpretation",
            "get_revision_interpretation",
            "get_revision_interpretation",
            "RevisionInterpretationRead",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/census",
            "get_revision_census",
            "get_revision_census",
            "RevisionCensusRead",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/diff",
            "get_revision_diff",
            "get_revision_diff",
            "RevisionDiffRead",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("query", "against", True, None, None, None, None),
                ("query", "fields", False, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/adapter-output",
            "get_revision_adapter_output",
            "get_revision_adapter_output",
            "AdapterRunOutputRead",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "GET",
            "/adapter-outputs/{adapter_output_id}",
            "get_adapter_output",
            "get_adapter_output",
            "AdapterRunOutputRead",
            None,
            (
                ("path", "adapter_output_id", True, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "POST",
            "/revisions/{revision_id}/changesets",
            "create_revision_changeset",
            "create_revision_changeset",
            "CadChangeSetRead",
            201,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
                (
                    "dependency",
                    "idempotency_key",
                    None,
                    None,
                    None,
                    None,
                    "get_idempotency_key",
                ),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/changesets",
            "list_revision_changesets",
            "list_revision_changesets",
            "CadChangeSetListResponse",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("query", "limit", False, 50, 1, 200, None),
                ("query", "cursor", False, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/changesets/{change_set_id}",
            "get_revision_changeset",
            "get_revision_changeset",
            "CadChangeSetRead",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("path", "change_set_id", True, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "POST",
            "/revisions/{revision_id}/changesets/{change_set_id}/validate",
            "validate_revision_changeset",
            "validate_revision_changeset",
            "CadChangeSetValidationActionRead",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("path", "change_set_id", True, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
                (
                    "dependency",
                    "idempotency_key",
                    None,
                    None,
                    None,
                    None,
                    "get_idempotency_key",
                ),
            ),
        ),
        (
            "POST",
            "/revisions/{revision_id}/changesets/{change_set_id}/apply",
            "apply_revision_changeset",
            "apply_revision_changeset",
            "JobRead",
            202,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("path", "change_set_id", True, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
                (
                    "dependency",
                    "idempotency_key",
                    None,
                    None,
                    None,
                    None,
                    "get_idempotency_key",
                ),
            ),
        ),
        (
            "GET",
            "/files/{file_id}/generated-artifacts",
            "list_file_generated_artifacts",
            "list_file_generated_artifacts",
            "GeneratedArtifactListResponse",
            None,
            (
                ("path", "file_id", True, None, None, None, None),
                ("query", "limit", False, 50, 1, 200, None),
                ("query", "cursor", False, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "GET",
            "/generated-artifacts/{artifact_id}/download",
            "download_generated_artifact",
            "download_generated_artifact",
            None,
            None,
            (
                ("path", "artifact_id", True, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
                ("dependency", "storage", None, None, None, None, "get_storage"),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/generated-artifacts",
            "list_revision_generated_artifacts",
            "list_revision_generated_artifacts",
            "GeneratedArtifactListResponse",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("query", "limit", False, 50, 1, 200, None),
                ("query", "cursor", False, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/entities/{entity_id:path}/source",
            "get_revision_entity_source",
            "get_revision_entity_source",
            "RevisionEntitySourceRead",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("path", "entity_id", True, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
                ("dependency", "storage", None, None, None, None, "get_storage"),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/layouts",
            "list_revision_layouts",
            "list_revision_layouts",
            "RevisionLayoutListResponse",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("query", "limit", False, 50, 1, 200, None),
                ("query", "cursor", False, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/layers",
            "list_revision_layers",
            "list_revision_layers",
            "RevisionLayerListResponse",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("query", "limit", False, 50, 1, 200, None),
                ("query", "cursor", False, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/blocks",
            "list_revision_blocks",
            "list_revision_blocks",
            "RevisionBlockListResponse",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("query", "limit", False, 50, 1, 200, None),
                ("query", "cursor", False, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/entities",
            "list_revision_entities",
            "list_revision_entities",
            "RevisionEntityListResponse",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("query", "limit", False, 50, 1, 200, None),
                ("query", "cursor", False, None, None, None, None),
                ("query", "entity_id", False, None, None, None, None),
                ("query", "entity_type", False, None, None, None, None),
                ("query", "layout_ref", False, None, None, None, None),
                ("query", "layer_ref", False, None, None, None, None),
                ("query", "block_ref", False, None, None, None, None),
                ("query", "parent_entity_ref", False, None, None, None, None),
                ("query", "source_identity", False, None, None, None, None),
                ("query", "source_hash", False, None, None, None, None),
                ("query", "on_sheet", False, None, None, None, None),
                ("query", "min_x", False, None, None, None, None),
                ("query", "min_y", False, None, None, None, None),
                ("query", "max_x", False, None, None, None, None),
                ("query", "max_y", False, None, None, None, None),
                ("query", "near_x", False, None, None, None, None),
                ("query", "near_y", False, None, None, None, None),
                ("query", "radius", False, None, None, None, None),
                ("query", "fields", False, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/entities/{entity_id:path}",
            "get_revision_entity",
            "get_revision_entity",
            "RevisionEntityRead",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("path", "entity_id", True, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/devices",
            "list_revision_devices",
            "list_revision_devices",
            "RevisionDeviceListResponse",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("query", "limit", False, 50, 1, 200, None),
                ("query", "cursor", False, None, None, None, None),
                ("query", "device_layer", False, None, None, None, None),
                ("query", "tag_layer", False, None, None, None, None),
                ("query", "max_tag_distance", False, None, None, None, None),
                ("query", "max_depth", False, 8, 0, 8, None),
                ("query", "kind", False, "all", None, None, None),
                ("query", "scope", False, "sheet", None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/legend-devices",
            "list_revision_legend_devices",
            "list_revision_legend_devices",
            "RevisionLegendDeviceListResponse",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("query", "limit", False, 50, 1, 200, None),
                ("query", "cursor", False, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/layer-roles",
            "list_revision_layer_roles",
            "list_revision_layer_roles",
            "RevisionLayerRoleListResponse",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/rooms",
            "list_revision_rooms",
            "list_revision_rooms",
            "RevisionRoomListResponse",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("query", "strategy", False, "auto", None, None, None),
                ("query", "device_layer", False, None, None, None, None),
                ("query", "tag_layer", False, None, None, None, None),
                ("query", "snap_tolerance", False, 0.0, 0.0, None, None),
                ("query", "min_area", False, 0.0, 0.0, None, None),
                ("query", "max_depth", False, 8, 0, 8, None),
                ("query", "scope", False, "sheet", None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/rooms/{room_id}/entities",
            "list_revision_room_entities",
            "list_revision_room_entities",
            "RevisionRoomEntityListResponse",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("path", "room_id", True, None, None, None, None),
                ("query", "limit", False, 50, 1, 200, None),
                ("query", "cursor", False, None, None, None, None),
                ("query", "entity_type", False, None, None, None, None),
                ("query", "layer_ref", False, None, None, None, None),
                ("query", "strategy", False, "auto", None, None, None),
                ("query", "device_layer", False, None, None, None, None),
                ("query", "tag_layer", False, None, None, None, None),
                ("query", "snap_tolerance", False, 0.0, 0.0, None, None),
                ("query", "min_area", False, 0.0, 0.0, None, None),
                ("query", "max_depth", False, 8, 0, 8, None),
                ("query", "scope", False, "sheet", None, None, None),
                ("query", "fields", False, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/service-takeoff",
            "get_revision_service_takeoff",
            "get_revision_service_takeoff",
            "ServiceTakeoffResponse",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("query", "layer_refs", False, None, None, None, None),
                ("query", "tag_layers", False, None, None, None, None),
                ("query", "legend_layers", False, None, None, None, None),
                ("query", "scope", False, "sheet", None, None, None),
                ("query", "snap_tolerance", False, 0.0, 0.0, None, None),
                ("query", "min_area", False, 0.0, 0.0, None, None),
                ("query", "radius", False, 5.0, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/cable-estimate",
            "get_revision_cable_estimate",
            "get_revision_cable_estimate",
            "CableEstimateResponse",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("query", "containment_revision_id", False, None, None, None, None),
                ("query", "scope", False, "sheet", None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "GET",
            "/floors/takeoff",
            "get_floor_takeoff",
            "get_floor_takeoff",
            "FloorTakeoffResponse",
            None,
            (
                ("query", "reference_revision_id", True, None, None, None, None),
                ("query", "member", False, [], None, None, None),
                ("query", "containment_revision_id", False, None, None, None, None),
                ("query", "scope", False, "sheet", None, None, None),
                ("query", "strategy", False, "auto", None, None, None),
                ("query", "snap_tolerance", False, 0.0, 0, None, None),
                ("query", "min_area", False, 0.0, 0, None, None),
                ("query", "voronoi_fallback", False, True, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/quantity-takeoffs",
            "list_revision_quantity_takeoffs",
            "list_revision_quantity_takeoffs",
            "QuantityTakeoffListResponse",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("query", "limit", False, 50, 1, 200, None),
                ("query", "cursor", False, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/quantity-takeoffs/{takeoff_id}",
            "get_revision_quantity_takeoff",
            "get_revision_quantity_takeoff",
            "QuantityTakeoffRead",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("path", "takeoff_id", True, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/quantity-takeoffs/{takeoff_id}/items",
            "list_revision_quantity_takeoff_items",
            "list_revision_quantity_takeoff_items",
            "QuantityItemListResponse",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("path", "takeoff_id", True, None, None, None, None),
                ("query", "limit", False, 50, 1, 200, None),
                ("query", "cursor", False, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/estimates",
            "list_revision_estimates",
            "list_revision_estimates",
            "EstimateVersionListResponse",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("query", "limit", False, 50, 1, 200, None),
                ("query", "cursor", False, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "POST",
            "/revisions/{revision_id}/quantity-takeoffs/{takeoff_id}/estimate-versions",
            "create_revision_estimate_version",
            "create_revision_estimate_version",
            "JobRead",
            202,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("path", "takeoff_id", True, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
                (
                    "dependency",
                    "idempotency_key",
                    None,
                    None,
                    None,
                    None,
                    "get_idempotency_key",
                ),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/estimates/{estimate_version_id}",
            "get_revision_estimate",
            "get_revision_estimate",
            "EstimateVersionRead",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("path", "estimate_version_id", True, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/estimates/{estimate_version_id}/items",
            "list_revision_estimate_items",
            "list_revision_estimate_items",
            "EstimateItemListResponse",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("path", "estimate_version_id", True, None, None, None, None),
                ("query", "limit", False, 50, 1, 200, None),
                ("query", "cursor", False, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/estimates/{estimate_version_id}/snapshot-entries",
            "list_revision_estimate_snapshot_entries",
            "list_revision_estimate_snapshot_entries",
            "EstimateSnapshotEntryListResponse",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("path", "estimate_version_id", True, None, None, None, None),
                ("query", "limit", False, 50, 1, 200, None),
                ("query", "cursor", False, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
        (
            "POST",
            "/revisions/{revision_id}/exports/revision-json",
            "create_revision_json_export",
            "create_revision_json_export",
            "JobRead",
            202,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
                (
                    "dependency",
                    "idempotency_key",
                    None,
                    None,
                    None,
                    None,
                    "get_idempotency_key",
                ),
            ),
        ),
        (
            "POST",
            "/revisions/{revision_id}/exports/dxf",
            "create_dxf_export",
            "create_dxf_export",
            "JobRead",
            202,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
                (
                    "dependency",
                    "idempotency_key",
                    None,
                    None,
                    None,
                    None,
                    "get_idempotency_key",
                ),
            ),
        ),
        (
            "POST",
            "/revisions/{revision_id}/exports/revised-dxf",
            "create_revised_dxf_export",
            "create_revised_dxf_export",
            "JobRead",
            202,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
                (
                    "dependency",
                    "idempotency_key",
                    None,
                    None,
                    None,
                    None,
                    "get_idempotency_key",
                ),
            ),
        ),
        (
            "POST",
            "/revisions/{revision_id}/quantity-takeoffs/{takeoff_id}/exports/quantity-csv",
            "create_revision_quantity_csv_export",
            "create_revision_quantity_csv_export",
            "JobRead",
            202,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("path", "takeoff_id", True, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
                (
                    "dependency",
                    "idempotency_key",
                    None,
                    None,
                    None,
                    None,
                    "get_idempotency_key",
                ),
            ),
        ),
        (
            "POST",
            "/revisions/{revision_id}/quantity-takeoffs/{takeoff_id}/estimates/{estimate_version_id}/exports",
            "create_revision_estimate_export",
            "create_revision_estimate_export",
            "JobRead",
            202,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("path", "takeoff_id", True, None, None, None, None),
                ("path", "estimate_version_id", True, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
                (
                    "dependency",
                    "idempotency_key",
                    None,
                    None,
                    None,
                    None,
                    "get_idempotency_key",
                ),
            ),
        ),
        (
            "POST",
            "/revisions/{revision_id}/quantity-takeoffs",
            "create_revision_quantity_takeoff",
            "create_revision_quantity_takeoff",
            "JobRead",
            202,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
                (
                    "dependency",
                    "idempotency_key",
                    None,
                    None,
                    None,
                    None,
                    "get_idempotency_key",
                ),
            ),
        ),
        (
            "GET",
            "/revisions/{revision_id}/validation-report",
            "get_validation_report",
            "get_validation_report",
            "ValidationReportResponse",
            None,
            (
                ("path", "revision_id", True, None, None, None, None),
                ("dependency", "db", None, None, None, None, "get_db"),
            ),
        ),
    ]

    # Act
    actual_contract = _route_contract()

    # Assert
    assert actual_contract == expected_contract


def test_floor_takeoff_route_is_mounted_in_revisions_router() -> None:
    """Mounting-regression guard: /floors/takeoff must be in revisions_router.routes."""
    floor_route = next(
        (
            r
            for r in revisions_router.routes
            if isinstance(r, APIRoute) and r.path == "/floors/takeoff"
        ),
        None,
    )
    assert floor_route is not None, (
        "/floors/takeoff is not mounted in revisions_router — "
        "include floor_takeoff_router in app/api/v1/revisions.py"
    )


def test_revision_router_excludes_legacy_entity_and_estimate_post_routes() -> None:
    # Arrange
    forbidden_contract_entries = {
        (
            "GET",
            "/revisions/{revision_id}/entities/{entity_id}",
            "get_revision_entity",
            "get_revision_entity",
            "RevisionEntityRead",
            None,
            (),
        ),
        (
            "POST",
            "/revisions/{revision_id}/estimate-versions",
            "create_revision_estimate_version",
            "create_revision_estimate_version",
            "JobRead",
            202,
            (),
        ),
    }

    # Act
    actual_contract = {
        (
            method,
            path,
            name,
            endpoint_name,
            response_model_name,
            status_code,
            (),
        )
        for (
            method,
            path,
            name,
            endpoint_name,
            response_model_name,
            status_code,
            _,
        ) in _route_contract()
    }

    # Assert
    assert actual_contract.isdisjoint(forbidden_contract_entries)
