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
- **Action tools** — the mutating flows (create project, reprocess, changeset
  create/validate/apply, quantity takeoff, estimate version, exports, job
  cancel/retry, …). A fresh `Idempotency-Key` is injected automatically per call.
- **`server_info`** (bespoke) — MCP version + a live API health probe; call first.
- **`upload_project_file`** (bespoke) — upload a local file by path (multipart),
  which starts ingestion. Returns the created file record.
- **`wait_for_job`** (bespoke) — poll a job until it reaches a terminal state
  (`succeeded`/`failed`/`cancelled`); use after any action that returns a job.
- **`verify_revision`** (bespoke) — compact verdict on whether a revision is
  usable: `validation_status`, failed/warning checks, coverage headline, rationale.
- **`explain_finding`** (bespoke) — drill into one validation finding: its check,
  severity, affected target, and quantity effect.

Not exposed: the irreversible project delete, the binary artifact download, and
liveness endpoints (covered by `server_info`).
