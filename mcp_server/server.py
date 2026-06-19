"""Draupnir MCP server construction.

Builds the FastMCP server by **generating** the tool/resource surface from the
Draupnir API's OpenAPI spec (fetched at startup from the running API), plus the
foundational ``server_info`` tool. Action tools (mutations) are layered on in a
later change (#527); here the surface is read-only.
"""

from typing import Any

import httpx
from fastmcp import FastMCP
from fastmcp.server.providers.openapi import MCPType, RouteMap

from mcp_server.config import MCPSettings
from mcp_server.health import build_server_info, probe_api_health

SERVER_NAME = "draupnir-mcp"
OPENAPI_SPEC_PATH = "/openapi.json"

# Ordered route maps (first match wins) that curate the generated surface:
#   1. Mutations are excluded — actions land in #527 (this layer is read-only).
#   2. The binary artifact download is not a useful MCP result.
#   3. Liveness endpoints are covered by ``server_info``.
#   4. GETs addressing a single object by id (path ends in a path param) become
#      browseable resource templates; every other GET is a query tool.
ROUTE_MAPS: list[RouteMap] = [
    RouteMap(methods=["POST", "PUT", "PATCH", "DELETE"], mcp_type=MCPType.EXCLUDE),
    RouteMap(methods=["GET"], pattern=r"/download$", mcp_type=MCPType.EXCLUDE),
    RouteMap(methods=["GET"], pattern=r"/health$", mcp_type=MCPType.EXCLUDE),
    RouteMap(methods=["GET"], pattern=r"\}$", mcp_type=MCPType.RESOURCE_TEMPLATE),
    RouteMap(methods=["GET"], mcp_type=MCPType.TOOL),
]


def build_api_client(settings: MCPSettings) -> httpx.AsyncClient:
    """Build the shared HTTP client pointed at the Draupnir API."""

    return httpx.AsyncClient(
        base_url=settings.api_base_url,
        timeout=settings.request_timeout_seconds,
    )


async def fetch_openapi_spec(client: httpx.AsyncClient) -> dict[str, Any]:
    """Fetch the Draupnir API's OpenAPI spec (the source the surface is generated from)."""

    try:
        response = await client.get(OPENAPI_SPEC_PATH)
        response.raise_for_status()
        spec: dict[str, Any] = response.json()
    except httpx.HTTPError as exc:
        raise RuntimeError(
            f"Could not fetch the Draupnir OpenAPI spec from {client.base_url}{OPENAPI_SPEC_PATH}: "
            f"{exc}. Is the API running and DRAUPNIR_API_BASE_URL correct?"
        ) from exc
    return spec


def _register_server_info(mcp: FastMCP, settings: MCPSettings, client: httpx.AsyncClient) -> None:
    """Register the bespoke identity/health tool on the server."""

    @mcp.tool
    async def server_info() -> dict[str, Any]:
        """Return the MCP server identity/version and a live probe of the Draupnir API.

        Use this first to confirm the server is wired to a reachable, healthy API
        before issuing other tools.
        """

        api_health = await probe_api_health(client, settings.api_prefix)
        return build_server_info(settings, api_health)


def build_server(spec: dict[str, Any], settings: MCPSettings, client: httpx.AsyncClient) -> FastMCP:
    """Build the MCP server from an OpenAPI spec + a client (pure; injectable for tests)."""

    mcp: FastMCP = FastMCP.from_openapi(
        spec,
        client=client,
        name=SERVER_NAME,
        route_maps=ROUTE_MAPS,
    )
    _register_server_info(mcp, settings, client)
    return mcp


async def create_server(
    settings: MCPSettings | None = None,
    *,
    client: httpx.AsyncClient | None = None,
) -> FastMCP:
    """Create the Draupnir MCP server by generating its surface from the live API spec.

    ``settings`` and ``client`` are injectable for tests; in normal use both are
    derived from the environment and the spec is fetched from the running API.
    """

    settings = settings or MCPSettings()
    api_client = client or build_api_client(settings)
    spec = await fetch_openapi_spec(api_client)
    return build_server(spec, settings, api_client)
