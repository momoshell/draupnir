"""OpenAPI surface contract — keeps the spec clean for SDK / MCP generation.

Pins that every operation has a clean, unique operationId (derived from the
route-handler name, not FastAPI's mangled default), is grouped under exactly one
documented tag, and that core app metadata is present. New routes must keep the
surface clean (add their operationId here + tag the router).
"""

from __future__ import annotations

from typing import Any

from app.main import create_app

_HTTP_METHODS = {"get", "post", "put", "patch", "delete"}


def _v1_operations() -> dict[str, dict[str, Any]]:
    spec = create_app().openapi()
    ops: dict[str, dict[str, Any]] = {}
    for path, methods in spec["paths"].items():
        if "/v1" not in path:
            continue
        for method, operation in methods.items():
            if method in _HTTP_METHODS:
                ops[f"{method.upper()} {path}"] = operation
    return ops


def test_app_metadata_is_populated() -> None:
    spec = create_app().openapi()
    info = spec["info"]
    assert info["title"]
    assert info["version"]
    assert info["description"]
    assert info["contact"]["name"]
    assert info["license"]["name"]


def test_every_operation_has_one_documented_tag() -> None:
    spec = create_app().openapi()
    documented = {tag["name"] for tag in spec["tags"]}
    assert documented, "openapi_tags must declare the tag groups"
    for label, operation in _v1_operations().items():
        tags = operation.get("tags") or []
        assert len(tags) == 1, f"{label} must have exactly one tag, got {tags}"
        assert tags[0] in documented, f"{label} uses undocumented tag {tags[0]!r}"


def test_operation_ids_are_clean_and_unique() -> None:
    operation_ids = [op["operationId"] for op in _v1_operations().values()]
    # Clean = no FastAPI path-mangling suffix (the default appends the route path).
    for operation_id in operation_ids:
        assert "__" not in operation_id, f"mangled operationId: {operation_id}"
        assert "_v1_" not in operation_id, f"mangled operationId: {operation_id}"
    assert len(operation_ids) == len(set(operation_ids)), "operationIds must be unique"


def test_data_endpoints_document_the_error_envelope() -> None:
    """Representative data endpoints advertise the standard 400/404/409 error envelope."""
    spec = create_app().openapi()
    assert "APIErrorResponse" in spec["components"]["schemas"], (
        "the error envelope schema must be a documented component"
    )
    ops = _v1_operations()
    by_id = {op["operationId"]: op for op in ops.values()}
    for operation_id in (
        "get_project",
        "list_revision_entities",
        "create_revision_changeset",
        "get_job",
        "create_rate",
    ):
        responses = by_id[operation_id]["responses"]
        for code in ("400", "404", "409"):
            assert code in responses, f"{operation_id} should document a {code} response"
            schema = responses["400"]["content"]["application/json"]["schema"]
            assert schema["$ref"].endswith("/APIErrorResponse"), (
                f"{operation_id} {code} should use the APIErrorResponse envelope"
            )


def test_operation_id_inventory_is_pinned() -> None:
    """Pin the operationId set so new/renamed routes consciously keep the surface clean."""
    actual = {op["operationId"] for op in _v1_operations().values()}
    expected = {
        "apply_revision_changeset",
        "cancel_job",
        "create_dxf_export",
        "create_formula",
        "create_material",
        "create_project",
        "create_rate",
        "create_revised_dxf_export",
        "create_revision_changeset",
        "create_revision_estimate_export",
        "create_revision_estimate_version",
        "create_revision_json_export",
        "create_revision_quantity_csv_export",
        "create_revision_quantity_takeoff",
        "delete_project",
        "download_generated_artifact",
        "get_adapter_output",
        "get_formula",
        "get_health",
        "get_job",
        "get_material",
        "get_project",
        "get_project_file",
        "get_rate",
        "get_revision_adapter_output",
        "get_revision_census",
        "get_revision_changeset",
        "get_revision_entity",
        "get_revision_diff",
        "get_revision_entity_source",
        "get_revision_estimate",
        "get_revision_interpretation",
        "get_revision_quantity_takeoff",
        "get_revision_scale",
        "get_revision_service_takeoff",
        "get_revision_summary",
        "get_system_capabilities",
        "get_system_health",
        "get_validation_report",
        "list_file_generated_artifacts",
        "list_file_revisions",
        "list_formulas",
        "list_job_events",
        "list_materials",
        "list_project_files",
        "list_projects",
        "list_rates",
        "list_revision_blocks",
        "list_revision_changesets",
        "list_revision_devices",
        "list_revision_entities",
        "list_revision_estimate_items",
        "list_revision_estimate_snapshot_entries",
        "list_revision_estimates",
        "list_revision_generated_artifacts",
        "list_revision_layer_roles",
        "list_revision_layers",
        "list_revision_layouts",
        "list_revision_legend_devices",
        "list_revision_quantity_takeoff_items",
        "list_revision_quantity_takeoffs",
        "list_revision_room_entities",
        "list_revision_rooms",
        "reprocess_project_file",
        "retry_job",
        "update_project",
        "upload_project_file",
        "validate_revision_changeset",
    }
    assert actual == expected, (
        f"operationId surface changed.\nmissing: {sorted(expected - actual)}\n"
        f"unexpected: {sorted(actual - expected)}"
    )
