"""Tests for the generated MCP read surface (#526).

Builds the MCP server from the real API OpenAPI spec and pins the curated
tool/resource-template inventory, so a route that changes the surface has to
consciously update this contract. Also round-trips one query tool against a
mocked API. The runtime server fetches the spec over HTTP and imports nothing
from ``app``; this test imports the app only to obtain the spec for assertions.
"""

import httpx
from fastmcp import FastMCP

from app.main import create_app
from mcp_server.config import MCPSettings
from mcp_server.server import build_server

# Single-object reads addressed by id (path ends in a path param) → browse resource templates.
_EXPECTED_RESOURCE_TEMPLATES = {
    "get_adapter_output",
    "get_formula",
    "get_job",
    "get_material",
    "get_project",
    "get_project_file",
    "get_rate",
    "get_revision_changeset",
    "get_revision_entity",
    "get_revision_estimate",
    "get_revision_quantity_takeoff",
}

# Lists + parameterized queries + computed views → query tools; mutations → action tools;
# plus the bespoke server_info / upload_project_file / wait_for_job.
_EXPECTED_TOOLS = {
    # bespoke
    "server_info",
    "upload_project_file",
    "wait_for_job",
    "verify_revision",
    "explain_finding",
    # mutations / actions (generated)
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
    "reprocess_project_file",
    "retry_job",
    "update_project",
    "validate_revision_changeset",
    # reads (generated)
    "get_revision_adapter_output",
    "get_floor_takeoff",
    "get_revision_cable_estimate",
    "get_revision_census",
    "get_revision_diff",
    "get_revision_entity_source",
    "get_revision_interpretation",
    "get_revision_scale",
    "get_revision_service_takeoff",
    "get_revision_summary",
    "get_system_capabilities",
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
}

# Irreversible delete + binary download + liveness are never exposed. (The binary
# upload_project_file operation is excluded from generation but re-added as a
# bespoke path-based tool, so it is intentionally absent from this set.)
_EXPECTED_EXCLUDED = {
    "delete_project",
    "download_generated_artifact",
    "get_health",
    "get_system_health",
}


def _spec() -> dict[str, object]:
    return create_app().openapi()


def _server(handler: httpx.MockTransport) -> tuple[FastMCP, httpx.AsyncClient]:
    client = httpx.AsyncClient(transport=handler, base_url="http://api.test")
    server = build_server(_spec(), MCPSettings(api_base_url="http://api.test"), client)
    return server, client


async def test_generated_inventory_matches_curated_surface() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={})

    server, client = _server(httpx.MockTransport(handler))
    async with client:
        tool_names = {tool.name for tool in await server.list_tools()}
        template_names = {tmpl.name for tmpl in await server.list_resource_templates()}

    assert tool_names == _EXPECTED_TOOLS
    assert template_names == _EXPECTED_RESOURCE_TEMPLATES
    # No mutation/binary/liveness operation leaked into either surface.
    assert _EXPECTED_EXCLUDED.isdisjoint(tool_names | template_names)


async def test_query_tool_round_trips_against_api() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["method"] = request.method
        seen["path"] = request.url.path
        return httpx.Response(
            200, json={"items": [{"id": "p1", "name": "Proj"}], "next_cursor": None}
        )

    server, client = _server(httpx.MockTransport(handler))
    async with client:
        result = await server.call_tool("list_projects", {})

    assert seen == {"method": "GET", "path": "/v1/projects"}
    payload = result.structured_content
    assert payload is not None
    assert payload["items"][0]["id"] == "p1"
