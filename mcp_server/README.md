# Draupnir MCP server

A standalone [MCP](https://modelcontextprotocol.io) server that exposes the
Draupnir platform to AI agents as typed tools and resources. It is a **thin
adapter over the running HTTP API** — it talks to the API over HTTP and imports
nothing from `app` at runtime. The tool surface is generated from the API's
OpenAPI spec to avoid drift.

## Configuration

Environment variables (all prefixed `DRAUPNIR_`):

| Variable | Default | Meaning |
| --- | --- | --- |
| `DRAUPNIR_API_BASE_URL` | `http://localhost:8000` | Base URL of the running Draupnir API. |
| `DRAUPNIR_API_PREFIX` | `/v1` | API version path prefix. |
| `DRAUPNIR_REQUEST_TIMEOUT_SECONDS` | `30` | Per-request timeout for API calls. |

## Run

```bash
uv sync --extra mcp
DRAUPNIR_API_BASE_URL=http://localhost:8000 uv run python -m mcp_server
```

The server speaks the **stdio** transport (v1); a streamable-HTTP transport is a
later follow-up.

## Register with Claude Code

```bash
claude mcp add draupnir \
  --env DRAUPNIR_API_BASE_URL=http://localhost:8000 \
  -- uv run python -m mcp_server
```

## Surface

The tool/resource surface is **generated from the API's OpenAPI spec** (fetched
from the running API at startup) and curated with route maps:

- **Query tools** — every read/list/query endpoint (entities with filters +
  projection + spatial/in-room, devices, rooms, layers, scale, summary, coverage,
  quantities, estimates, …) plus `get_system_capabilities`.
- **Resource templates** — single objects addressed by id (project, file, job,
  entity, changeset, estimate, takeoff, catalog material/rate/formula, adapter
  output) for browsing.
- **`server_info`** (bespoke) — MCP version + a live API health probe; call it
  first.

Not exposed in this layer: mutations (added in #527), the binary artifact
download, and liveness endpoints (covered by `server_info`).
