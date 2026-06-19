"""Health/info logic for the MCP server (pure, client-injected for testability)."""

from typing import Any

import httpx

from mcp_server import __version__
from mcp_server.config import MCPSettings

SERVER_NAME = "draupnir-mcp"


async def probe_api_health(client: httpx.AsyncClient, api_prefix: str) -> dict[str, Any]:
    """Probe the Draupnir API's system-health endpoint.

    Returns a small status dict. ``reachable`` is ``True`` whenever the API
    answered (including a 503 "degraded" — that is still a reachable API);
    ``False`` only when the request could not complete (connection/timeout).
    """

    try:
        response = await client.get(f"{api_prefix}/system/health")
    except httpx.HTTPError as exc:
        return {
            "reachable": False,
            "http_status": None,
            "api_status": None,
            "error": str(exc),
        }

    try:
        body = response.json()
    except ValueError:
        body = None
    api_status = body.get("status") if isinstance(body, dict) else None

    return {
        "reachable": True,
        "http_status": response.status_code,
        "api_status": api_status,
        "error": None,
    }


def build_server_info(settings: MCPSettings, api_health: dict[str, Any]) -> dict[str, Any]:
    """Assemble the ``server_info`` payload: MCP identity + a live API probe."""

    return {
        "name": SERVER_NAME,
        "version": __version__,
        "api_base_url": settings.api_base_url,
        "api": api_health,
    }
