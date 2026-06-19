"""Tests for the MCP action tools (#527) — idempotency, upload, job polling."""

from pathlib import Path

import httpx
from fastmcp import FastMCP

from mcp_server.config import MCPSettings
from mcp_server.server import (
    build_api_client,
    build_server,
    inject_idempotency_key,
)

_MINIMAL_SPEC = {
    "openapi": "3.1.0",
    "info": {"title": "Draupnir", "version": "0.1.0"},
    "paths": {},
}


def _server_and_client(handler: httpx.MockTransport) -> tuple[FastMCP, httpx.AsyncClient]:
    client = httpx.AsyncClient(
        transport=handler,
        base_url="http://api.test",
        event_hooks={"request": [inject_idempotency_key]},
    )
    server = build_server(_MINIMAL_SPEC, MCPSettings(api_base_url="http://api.test"), client)
    return server, client


async def test_inject_idempotency_key_on_mutation_when_absent() -> None:
    request = httpx.Request("POST", "http://api.test/v1/projects")
    await inject_idempotency_key(request)
    assert request.headers["Idempotency-Key"]


async def test_inject_idempotency_key_preserves_caller_key() -> None:
    request = httpx.Request(
        "POST", "http://api.test/v1/projects", headers={"Idempotency-Key": "caller-key"}
    )
    await inject_idempotency_key(request)
    assert request.headers["Idempotency-Key"] == "caller-key"


async def test_inject_idempotency_key_skips_get() -> None:
    request = httpx.Request("GET", "http://api.test/v1/projects")
    await inject_idempotency_key(request)
    assert "Idempotency-Key" not in request.headers


def test_build_api_client_attaches_idempotency_hook() -> None:
    client = build_api_client(MCPSettings(api_base_url="http://api.test"))
    assert inject_idempotency_key in client.event_hooks["request"]


async def test_upload_project_file_posts_multipart(tmp_path: Path) -> None:
    drawing = tmp_path / "plan.dxf"
    drawing.write_bytes(b"DXF-BYTES")
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["method"] = request.method
        seen["path"] = request.url.path
        seen["ctype"] = request.headers.get("content-type", "")
        seen["idem"] = request.headers.get("Idempotency-Key", "")
        seen["body_has_file"] = b"DXF-BYTES" in request.content
        return httpx.Response(201, json={"id": "file-1", "original_filename": "plan.dxf"})

    server, client = _server_and_client(httpx.MockTransport(handler))
    async with client:
        result = await server.call_tool(
            "upload_project_file", {"project_id": "p1", "file_path": str(drawing)}
        )

    assert seen["method"] == "POST"
    assert seen["path"] == "/v1/projects/p1/files"
    assert "multipart/form-data" in str(seen["ctype"])
    assert seen["body_has_file"] is True
    assert seen["idem"]  # auto-injected
    assert result.structured_content == {"id": "file-1", "original_filename": "plan.dxf"}


async def test_upload_project_file_missing_path_returns_error() -> None:
    server, client = _server_and_client(httpx.MockTransport(lambda _request: httpx.Response(201)))
    async with client:
        result = await server.call_tool(
            "upload_project_file", {"project_id": "p1", "file_path": "/no/such/file.dxf"}
        )
    payload = result.structured_content
    assert payload is not None
    assert "not found" in payload["error"].lower()


async def test_wait_for_job_polls_until_terminal() -> None:
    statuses = iter(["pending", "running", "succeeded"])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"id": "job-1", "status": next(statuses)})

    server, client = _server_and_client(httpx.MockTransport(handler))
    async with client:
        result = await server.call_tool(
            "wait_for_job",
            {"job_id": "job-1", "timeout_seconds": 5, "poll_interval_seconds": 0.001},
        )
    payload = result.structured_content
    assert payload is not None
    assert payload["status"] == "succeeded"
    assert payload["timed_out"] is False


async def test_wait_for_job_times_out_while_running() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"id": "job-1", "status": "running"})

    server, client = _server_and_client(httpx.MockTransport(handler))
    async with client:
        result = await server.call_tool(
            "wait_for_job",
            {"job_id": "job-1", "timeout_seconds": 0.0, "poll_interval_seconds": 0.001},
        )
    payload = result.structured_content
    assert payload is not None
    assert payload["timed_out"] is True
    assert payload["status"] == "running"


async def test_wait_for_job_not_found() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={})

    server, client = _server_and_client(httpx.MockTransport(handler))
    async with client:
        result = await server.call_tool("wait_for_job", {"job_id": "missing"})
    payload = result.structured_content
    assert payload is not None
    assert "not found" in payload["error"].lower()
