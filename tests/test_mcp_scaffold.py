"""Tests for the MCP server scaffold (#525) — config, health probe, server_info tool."""

import httpx
import pytest

from mcp_server import __version__
from mcp_server.config import MCPSettings
from mcp_server.health import build_server_info, probe_api_health
from mcp_server.server import build_server

_MINIMAL_SPEC = {
    "openapi": "3.1.0",
    "info": {"title": "Draupnir", "version": "0.1.0"},
    "paths": {},
}


def _client(handler: httpx.MockTransport) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=handler, base_url="http://api.test")


def test_settings_defaults() -> None:
    settings = MCPSettings()
    assert settings.api_base_url == "http://localhost:8000"
    assert settings.api_prefix == "/v1"
    assert settings.request_timeout_seconds > 0


def test_settings_read_prefixed_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DRAUPNIR_API_BASE_URL", "http://api.internal:9000")
    monkeypatch.setenv("DRAUPNIR_API_PREFIX", "/v2")
    settings = MCPSettings()
    assert settings.api_base_url == "http://api.internal:9000"
    assert settings.api_prefix == "/v2"


async def test_probe_api_health_ok() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        return httpx.Response(200, json={"status": "ok"})

    async with _client(httpx.MockTransport(handler)) as client:
        result = await probe_api_health(client, "/v1")

    assert seen["url"] == "http://api.test/v1/system/health"
    assert result == {"reachable": True, "http_status": 200, "api_status": "ok", "error": None}


async def test_probe_api_health_degraded_is_still_reachable() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"status": "degraded"})

    async with _client(httpx.MockTransport(handler)) as client:
        result = await probe_api_health(client, "/v1")

    assert result["reachable"] is True
    assert result["http_status"] == 503
    assert result["api_status"] == "degraded"


async def test_probe_api_health_unreachable() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    async with _client(httpx.MockTransport(handler)) as client:
        result = await probe_api_health(client, "/v1")

    assert result["reachable"] is False
    assert result["http_status"] is None
    assert result["api_status"] is None
    assert "connection refused" in result["error"]


def test_build_server_info_shape() -> None:
    settings = MCPSettings(api_base_url="http://api.test")
    health = {"reachable": True, "http_status": 200, "api_status": "ok", "error": None}
    info = build_server_info(settings, health)
    assert info == {
        "name": "draupnir-mcp",
        "version": __version__,
        "api_base_url": "http://api.test",
        "api": health,
    }


async def test_server_exposes_server_info_tool() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "ok"})

    settings = MCPSettings(api_base_url="http://api.test")
    async with _client(httpx.MockTransport(handler)) as client:
        server = build_server(_MINIMAL_SPEC, settings, client)

        tools = await server.list_tools()
        names = {tool.name for tool in tools}
        assert "server_info" in names
        info_tool = next(tool for tool in tools if tool.name == "server_info")
        assert info_tool.description  # documented for agents

        result = await server.call_tool("server_info", {})
        payload = result.structured_content
        assert payload is not None
        assert payload["name"] == "draupnir-mcp"
        assert payload["version"] == __version__
        assert payload["api"] == {
            "reachable": True,
            "http_status": 200,
            "api_status": "ok",
            "error": None,
        }
