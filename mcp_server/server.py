"""Draupnir MCP server construction.

Builds the FastMCP instance, its shared HTTP client to the Draupnir API, and the
foundational ``server_info`` tool. Generated query/resource/action tools are
layered on top in later changes (#526 / #527).
"""

from typing import Any

import httpx
from fastmcp import FastMCP

from mcp_server.config import MCPSettings
from mcp_server.health import build_server_info, probe_api_health


def build_api_client(settings: MCPSettings) -> httpx.AsyncClient:
    """Build the shared HTTP client pointed at the Draupnir API."""

    return httpx.AsyncClient(
        base_url=settings.api_base_url,
        timeout=settings.request_timeout_seconds,
    )


def create_server(
    settings: MCPSettings | None = None,
    *,
    client: httpx.AsyncClient | None = None,
) -> FastMCP:
    """Create the Draupnir MCP server.

    ``settings`` and ``client`` are injectable for tests; in normal use both are
    derived from the environment.
    """

    settings = settings or MCPSettings()
    api_client = client or build_api_client(settings)

    mcp: FastMCP = FastMCP(name="draupnir-mcp")

    @mcp.tool
    async def server_info() -> dict[str, Any]:
        """Return the MCP server identity/version and a live probe of the Draupnir API.

        Use this first to confirm the server is wired to a reachable, healthy API
        before issuing other tools.
        """

        api_health = await probe_api_health(api_client, settings.api_prefix)
        return build_server_info(settings, api_health)

    return mcp
