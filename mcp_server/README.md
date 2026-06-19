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

## Tools

- `server_info` — MCP server version + a live probe of the Draupnir API's system
  health. Use it first to confirm the server is wired to a reachable, healthy API.

(Query/resource/action tools are added in later changes — see epic #430.)
