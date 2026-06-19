"""Draupnir MCP server construction.

Generates the MCP tool/resource surface from the Draupnir API's OpenAPI spec
(fetched from the running API at startup) via FastMCP.from_openapi, curated with
ordered route maps, plus a few bespoke tools (``server_info``, a path-based
``upload_project_file``, and a ``wait_for_job`` poller). Idempotency keys are
injected automatically on mutating requests.
"""

import asyncio
import time
import uuid
from pathlib import Path
from typing import Any

import httpx
from fastmcp import FastMCP
from fastmcp.server.providers.openapi import MCPType, RouteMap

from mcp_server.config import MCPSettings
from mcp_server.health import build_server_info, probe_api_health
from mcp_server.instructions import INSTRUCTIONS
from mcp_server.verification import explain_finding_from_report, summarize_verification

SERVER_NAME = "draupnir-mcp"
OPENAPI_SPEC_PATH = "/openapi.json"
_MUTATING_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
TERMINAL_JOB_STATES = frozenset({"succeeded", "failed", "cancelled"})

# Ordered route maps (first match wins) that curate the generated surface:
#   - Mutations become tools, EXCEPT the irreversible project delete and the binary
#     file upload (replaced by a path-based bespoke tool).
#   - The binary artifact download is excluded; liveness is covered by server_info.
#   - GETs addressing a single object by id (path ends in a path param) become
#     browseable resource templates; every other GET is a query tool.
ROUTE_MAPS: list[RouteMap] = [
    RouteMap(methods=["DELETE"], mcp_type=MCPType.EXCLUDE),
    RouteMap(methods=["POST"], pattern=r"/projects/\{[^/]+\}/files$", mcp_type=MCPType.EXCLUDE),
    RouteMap(methods=["GET"], pattern=r"/download$", mcp_type=MCPType.EXCLUDE),
    RouteMap(methods=["GET"], pattern=r"/health$", mcp_type=MCPType.EXCLUDE),
    RouteMap(methods=["GET"], pattern=r"\}$", mcp_type=MCPType.RESOURCE_TEMPLATE),
    RouteMap(methods=["GET"], mcp_type=MCPType.TOOL),
    RouteMap(methods=["POST", "PUT", "PATCH"], mcp_type=MCPType.TOOL),
]


async def inject_idempotency_key(request: httpx.Request) -> None:
    """httpx request hook: stamp a fresh Idempotency-Key on mutations that lack one.

    Each tool call is one logical operation, so a fresh key per mutating request is
    the correct default; a caller that supplies its own key is left untouched.
    """

    if request.method in _MUTATING_METHODS and "Idempotency-Key" not in request.headers:
        request.headers["Idempotency-Key"] = str(uuid.uuid4())


def build_api_client(settings: MCPSettings) -> httpx.AsyncClient:
    """Build the shared HTTP client pointed at the Draupnir API (with idempotency hook)."""

    return httpx.AsyncClient(
        base_url=settings.api_base_url,
        timeout=settings.request_timeout_seconds,
        event_hooks={"request": [inject_idempotency_key]},
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


def _register_bespoke_tools(mcp: FastMCP, settings: MCPSettings, client: httpx.AsyncClient) -> None:
    """Register tools that aren't a 1:1 mapping of an API operation."""

    @mcp.tool
    async def server_info() -> dict[str, Any]:
        """Return the MCP server identity/version and a live probe of the Draupnir API.

        Use this first to confirm the server is wired to a reachable, healthy API
        before issuing other tools.
        """

        api_health = await probe_api_health(client, settings.api_prefix)
        return build_server_info(settings, api_health)

    @mcp.tool
    async def upload_project_file(
        project_id: str,
        file_path: str,
        extraction_profile: str | None = None,
    ) -> dict[str, Any]:
        """Upload a local drawing file to a project, which starts ingestion.

        ``file_path`` is a path on the machine running this MCP server. Returns the
        created file record (including its id); poll the resulting ingest job with
        ``wait_for_job``.
        """

        path = Path(file_path)
        if not path.is_file():
            return {"error": f"File not found: {file_path}"}
        data = {"extraction_profile": extraction_profile} if extraction_profile else None
        with path.open("rb") as handle:
            response = await client.post(
                f"{settings.api_prefix}/projects/{project_id}/files",
                files={"file": (path.name, handle.read(), "application/octet-stream")},
                data=data,
            )
        response.raise_for_status()
        result: dict[str, Any] = response.json()
        return result

    @mcp.tool
    async def wait_for_job(
        job_id: str,
        timeout_seconds: float = 120.0,
        poll_interval_seconds: float = 2.0,
    ) -> dict[str, Any]:
        """Poll a job until it reaches a terminal state (succeeded/failed/cancelled).

        Returns the final job record with a ``timed_out`` flag. Use after any action
        that returns a job (upload/ingest, reprocess, changeset apply, exports, …).
        """

        deadline = time.monotonic() + timeout_seconds
        while True:
            response = await client.get(f"{settings.api_prefix}/jobs/{job_id}")
            if response.status_code == httpx.codes.NOT_FOUND:
                return {"error": "Job not found", "job_id": job_id, "timed_out": False}
            response.raise_for_status()
            job: dict[str, Any] = response.json()
            if job.get("status") in TERMINAL_JOB_STATES:
                return {"timed_out": False, **job}
            if time.monotonic() >= deadline:
                return {"timed_out": True, **job}
            await asyncio.sleep(poll_interval_seconds)

    @mcp.tool
    async def verify_revision(revision_id: str) -> dict[str, Any]:
        """Return a compact, actionable verdict on whether a revision is usable.

        Aggregates the revision's validation report: ``validation_status``, the
        failed/warning check keys, an extraction-coverage headline (mapped ratio,
        top unmapped reasons, review-flagged count), and a one-line rationale. Use
        ``explain_finding`` to drill into a specific issue.
        """

        report = await _get_validation_report(client, settings.api_prefix, revision_id)
        if "error" in report:
            return {"revision_id": revision_id, **report}
        return summarize_verification(revision_id, report)

    @mcp.tool
    async def explain_finding(revision_id: str, finding_id: str) -> dict[str, Any]:
        """Explain a single validation finding: its check, severity, target, and effect.

        Use after ``verify_revision`` to understand why a revision is flagged and
        where to investigate (the affected entity/layer/document ref).
        """

        report = await _get_validation_report(client, settings.api_prefix, revision_id)
        if "error" in report:
            return {"revision_id": revision_id, **report}
        return explain_finding_from_report(report, finding_id)


async def _get_validation_report(
    client: httpx.AsyncClient, api_prefix: str, revision_id: str
) -> dict[str, Any]:
    """Fetch a revision's validation report, or a ``{"error": ...}`` dict on 404."""

    response = await client.get(f"{api_prefix}/revisions/{revision_id}/validation-report")
    if response.status_code == httpx.codes.NOT_FOUND:
        return {"error": "No validation report for this revision."}
    response.raise_for_status()
    report: dict[str, Any] = response.json()
    return report


def build_server(spec: dict[str, Any], settings: MCPSettings, client: httpx.AsyncClient) -> FastMCP:
    """Build the MCP server from an OpenAPI spec + a client (pure; injectable for tests)."""

    mcp: FastMCP = FastMCP.from_openapi(
        spec,
        client=client,
        name=SERVER_NAME,
        route_maps=ROUTE_MAPS,
        instructions=INSTRUCTIONS,
    )
    _register_bespoke_tools(mcp, settings, client)
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
