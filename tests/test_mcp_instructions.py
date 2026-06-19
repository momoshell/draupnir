"""Tests for the MCP guidance layer (#528) — server instructions + skill doc."""

from pathlib import Path

import httpx
from fastmcp import FastMCP

from mcp_server.config import MCPSettings
from mcp_server.instructions import INSTRUCTIONS
from mcp_server.server import build_server

_MINIMAL_SPEC = {
    "openapi": "3.1.0",
    "info": {"title": "Draupnir", "version": "0.1.0"},
    "paths": {},
}

# The guidance must name the tools/concepts that anchor each phase of the workflow.
_REQUIRED_TOPICS = [
    "server_info",
    "get_revision_summary",
    "verify_revision",
    "explain_finding",
    "list_revision_entities",
    "list_revision_room_entities",
    "upload_project_file",
    "wait_for_job",
    "idempotency",
    "Kitchen",  # the worked end-to-end example
]


def _server() -> tuple[FastMCP, httpx.AsyncClient]:
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _r: httpx.Response(200, json={})),
        base_url="http://api.test",
    )
    server = build_server(_MINIMAL_SPEC, MCPSettings(api_base_url="http://api.test"), client)
    return server, client


def test_instructions_cover_the_workflow() -> None:
    lowered = INSTRUCTIONS.lower()
    for topic in _REQUIRED_TOPICS:
        assert topic.lower() in lowered, f"guidance is missing {topic!r}"


def test_build_server_sets_instructions() -> None:
    server, _client = _server()
    assert server.instructions == INSTRUCTIONS


def test_skill_doc_is_present_and_described() -> None:
    skill = Path(__file__).resolve().parents[1] / "mcp_server" / "SKILL.md"
    text = skill.read_text(encoding="utf-8")
    assert text.startswith("---")  # frontmatter
    assert "name: draupnir" in text
    assert "description:" in text
    # The skill points agents at the verify-before-trust discipline + room query.
    assert "verify_revision" in text
    assert "list_revision_room_entities" in text
