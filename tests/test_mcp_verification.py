"""Tests for the MCP verify/explain affordances (#521)."""

from typing import Any

import httpx
from fastmcp import FastMCP

from mcp_server.config import MCPSettings
from mcp_server.server import build_server
from mcp_server.verification import explain_finding_from_report, summarize_verification

_MINIMAL_SPEC = {
    "openapi": "3.1.0",
    "info": {"title": "Draupnir", "version": "0.1.0"},
    "paths": {},
}

_REPORT: dict[str, Any] = {
    "validation_status": "valid_with_warnings",
    "checks": [
        {
            "check_key": "units",
            "status": "pass",
            "summary_message": "Units resolved.",
            "finding_refs": [],
        },
        {
            "check_key": "layer_mapping",
            "status": "warning",
            "summary_message": "Some layers unmapped.",
            "finding_refs": ["finding-001"],
        },
        {
            "check_key": "geometry_validity",
            "status": "fail",
            "summary_message": "Bad geom.",
            "finding_refs": [],
        },
    ],
    "findings": [
        {
            "finding_id": "finding-001",
            "check_key": "layer_mapping",
            "severity": "warning",
            "message": "Layer A-7 has no role.",
            "target_type": "layer",
            "target_ref": "A-7",
            "quantity_effect": "none",
            "source": "validator",
        }
    ],
    "coverage": {
        "entities": {"total": 100, "mapped": 92, "unmapped": 8, "mapped_ratio": 0.92},
        "unmapped_by_reason": {"no_layer": 5, "bad_geom": 2, "other": 1},
        "review_flagged_entities": 3,
    },
}


def _server(handler: httpx.MockTransport) -> tuple[FastMCP, httpx.AsyncClient]:
    client = httpx.AsyncClient(transport=handler, base_url="http://api.test")
    server = build_server(_MINIMAL_SPEC, MCPSettings(api_base_url="http://api.test"), client)
    return server, client


def test_summarize_verification_shape() -> None:
    verdict = summarize_verification("rev-1", _REPORT)
    assert verdict["validation_status"] == "valid_with_warnings"
    assert verdict["usable"] is True
    assert verdict["checks"]["failed"] == ["geometry_validity"]
    assert verdict["checks"]["warnings"] == ["layer_mapping"]
    assert verdict["coverage"]["mapped_ratio"] == 0.92
    assert verdict["coverage"]["review_flagged_entities"] == 3
    # Top unmapped reasons are ranked by count.
    assert list(verdict["coverage"]["top_unmapped_reasons"]) == ["no_layer", "bad_geom", "other"]
    assert "92%" in verdict["rationale"]


def test_summarize_invalid_is_not_usable() -> None:
    verdict = summarize_verification(
        "rev-1", {"validation_status": "invalid", "checks": [], "findings": []}
    )
    assert verdict["usable"] is False


def test_explain_finding_found() -> None:
    out = explain_finding_from_report(_REPORT, "finding-001")
    assert out["finding"]["message"] == "Layer A-7 has no role."
    assert out["check"]["check_key"] == "layer_mapping"
    assert out["target"] == {"type": "layer", "ref": "A-7"}
    assert out["severity"] == "warning"


def test_explain_finding_missing_lists_available() -> None:
    out = explain_finding_from_report(_REPORT, "finding-999")
    assert "not found" in out["error"].lower()
    assert out["available_finding_ids"] == ["finding-001"]


async def test_verify_revision_tool_round_trips() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        return httpx.Response(200, json=_REPORT)

    server, client = _server(httpx.MockTransport(handler))
    async with client:
        result = await server.call_tool("verify_revision", {"revision_id": "rev-1"})

    assert seen["path"] == "/v1/revisions/rev-1/validation-report"
    payload = result.structured_content
    assert payload is not None
    assert payload["validation_status"] == "valid_with_warnings"
    assert payload["checks"]["failed"] == ["geometry_validity"]


async def test_explain_finding_tool_round_trips() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_REPORT)

    server, client = _server(httpx.MockTransport(handler))
    async with client:
        result = await server.call_tool(
            "explain_finding", {"revision_id": "rev-1", "finding_id": "finding-001"}
        )
    payload = result.structured_content
    assert payload is not None
    assert payload["check"]["check_key"] == "layer_mapping"


async def test_verify_revision_handles_missing_report() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={})

    server, client = _server(httpx.MockTransport(handler))
    async with client:
        result = await server.call_tool("verify_revision", {"revision_id": "rev-x"})
    payload = result.structured_content
    assert payload is not None
    assert "error" in payload
