# draupnir

[![CI](https://github.com/momoshell/draupnir/actions/workflows/ci.yml/badge.svg)](https://github.com/momoshell/draupnir/actions/workflows/ci.yml)

Draupnir is a backend-first CAD/BIM ingestion and estimation system. The MVP
focuses on accepting common real-world drawing inputs, extracting structured
geometry and semantic data, producing deterministic quantities and estimates,
and exposing everything through a UI-agnostic API.

The MVP product surface is API-only. Future clients may include a web UI, TUI,
CLI, or other services, but this repository does not imply that any product
client exists today.

## Current Status

The current repository implementation is API-only. It supports project and file
creation/upload, queued ingestion and reprocess jobs, revision reads with
materialization and validation data, quantity takeoff workflows, estimation
catalog/formula management and estimate job workflows, export job creation plus
generated artifact metadata, changeset validate/apply flows, and the
revised-DXF export path for eligible changeset-origin revisions.

Docker Compose development infrastructure is available for local API and worker
setup; see "Local Development" below.

## Local Development

Local Docker Compose development and GitHub Actions CI both run PostgreSQL 18.

### Prerequisites

- Docker and Docker Compose (v2)
- uv

### Quick Start with Docker

> [!WARNING]
> Docker Compose now runs PostgreSQL 18. Existing local Docker volumes created
> for PostgreSQL 17 will not boot as-is under PostgreSQL 18.
>
> - If the data matters, dump it from PostgreSQL 17 first and restore it into a
>   fresh PostgreSQL 18 volume.
> - If the data does not matter, reset the local stack destructively with:
>   ```bash
>   docker compose down -v
>   ```
> - Rollback note: after you migrate or reset to PostgreSQL 18, you cannot
>   reuse that data directory with PostgreSQL 17; rolling back requires
>   restoring a PostgreSQL 17 backup into a fresh PostgreSQL 17 volume.

1. **Copy environment file**:
   ```bash
   cp .env.example .env
   ```

2. **Start all services**:
   ```bash
   make up
   # or: docker compose up -d
   ```

3. **Run database migrations**:
   ```bash
   make migrate
   # or: uv run alembic upgrade head
   ```

4. **Verify containers are running**:
   ```bash
   make ps
   # or: docker compose ps
   ```

5. **Verify the API responds**:
   ```bash
   curl http://localhost:8000/v1/health
   ```

6. **Run the Compose smoke workflow**:
   ```bash
   make compose-smoke
   ```

7. **Confirm background services are available**:
   - RabbitMQ management UI: http://localhost:15672
   - Login: `guest` / `guest`
   - Flower dashboard: http://localhost:5555 (optional; start it with
     `docker compose --profile debug up -d flower` when needed)

   AMQP is also exposed on `localhost:5672` for host-side clients only.

8. **View logs**:
   ```bash
   make logs
   # or: docker compose logs -f
   ```

9. **Stop all services**:
   ```bash
   make down
   # or: docker compose down
   ```

### Docker Compose Services

| Service   | Port(s)              | Description                    |
|-----------|----------------------|--------------------------------|
| api       | 8000                 | FastAPI application            |
| worker    | —                    | Celery background worker       |
| postgres  | localhost:5432       | PostgreSQL 18 database         |
| rabbitmq  | localhost:5672, localhost:15672 | RabbitMQ message broker |
| flower    | localhost:5555       | Celery dashboard (`debug` profile only) |

Postgres and RabbitMQ host ports bind to `127.0.0.1` only. Flower is optional
and intended for debug/profile runs. Containers still reach RabbitMQ over the
internal Docker network at `rabbitmq:5672`.

### Useful Commands

```bash
make shell-api      # Open shell in API container
make shell-worker   # Open shell in worker container
make migrate        # Run database migrations
docker compose down -v  # Stop and remove volumes (destructive)
```

### Pytest Lanes

The repository keeps three top-level pytest lanes with non-overlapping marker expressions:

- `make smoke` runs the fast smoke lane: `smoke and not integration and not compose_smoke`
- `make integration` runs the database-backed integration lane: `integration and not compose_smoke`
- `make compose-smoke` runs the opt-in Docker Compose lane: `compose_smoke`

The database-backed integration lane is also split into five non-overlapping DB sub-lanes for targeted local runs and CI policy:

- `make integration-db-api` runs `integration and db_api and not compose_smoke`
- `make integration-db-worker` runs `integration and db_worker and not compose_smoke`
- `make integration-db-estimation-export` runs `integration and db_estimation_export and not compose_smoke`
- `make integration-db-lineage` runs `integration and db_lineage and not compose_smoke`
- `make integration-db-migration` runs `integration and db_migration and not compose_smoke`

CI now uses two DB coverage modes:

- Pull requests run only the fast DB API gate: `Test integration (db_api)`
- Pushes to `main`, the weekly scheduled run, and manual `workflow_dispatch` run the full five-lane DB matrix

For local CI-equivalent runs, use:

- `make ci-db-pr-fast` to mirror the pull-request DB fast gate via the profiling runner
- `make ci-db-extended` to mirror the extended DB matrix via the profiling runner

> [!IMPORTANT]
> If GitHub branch protection still requires `Test integration (db_worker)`, `Test integration (db_estimation_export)`, `Test integration (db_lineage)`, or `Test integration (db_migration)` on pull requests, update the required checks. Pull requests now publish only `Test integration (db_api)` for DB-backed CI.

#### Smoke

Purpose: fast host-side verification without Docker Compose.

Prerequisites:

- `uv sync --locked --extra db --extra jobs --extra ingestion --extra dev --extra test`

Command:

```bash
make smoke
# or: uv run pytest -m "smoke and not integration and not compose_smoke"
```

#### Integration

Purpose: host-side tests that require PostgreSQL and a migrated schema.

Prerequisites:

- PostgreSQL 18 reachable via `DATABASE_URL`
- Alembic migrations applied to that database

Command:

```bash
export DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/draupnir_test
make integration
# or: uv run alembic upgrade head && uv run pytest -m "integration and not compose_smoke"
```

CI keeps this lane separate from smoke and preserves the PostgreSQL service. CI-parity DB targets route through `scripts/profile_integration_lane.py`, which owns the migration timing pass plus pytest execution.

For CI-parity runs, prefer `make ci-db-pr-fast` for the pull-request fast gate and `make ci-db-extended` for the extended non-PR coverage, because those targets route through `scripts/profile_integration_lane.py` just like GitHub Actions.

#### Compose smoke

Purpose: opt-in/manual checks against a running Docker Compose stack.

Prerequisites:

- `make up` (or equivalent `docker compose up -d`) already running
- `COMPOSE_SMOKE=1` enabled (the `make compose-smoke` target sets this for you)
- `SMOKE_BASE_URL` set to the API base URL under test (defaults to `http://localhost:8000` in `make compose-smoke`)

Command:

```bash
make compose-smoke
# override base URL: make compose-smoke SMOKE_BASE_URL=http://localhost:8001
# or: COMPOSE_SMOKE=1 SMOKE_BASE_URL=http://localhost:8000 uv run pytest -m "compose_smoke"
```

This lane stays manual and is not started automatically in CI.

### Local Development (without Docker)

Prerequisite: use PostgreSQL 18 for host-side database development, or point
your host tools at the Docker Compose PostgreSQL 18 instance on
`postgresql://postgres:postgres@localhost:5432/draupnir`.

Check your local PostgreSQL client/server major version before using a host-side
database:

```bash
psql --version
```

The reported version should be PostgreSQL 18.x.

1. **Install dependencies**:
   ```bash
   uv sync --locked --extra db --extra jobs --extra ingestion --extra dev --extra test
   ```

2. **Set up environment**:
   ```bash
   cp .env.example .env
   # Adjust DATABASE_URL and BROKER_URL for local services
   ```

3. **Run migrations**:
   ```bash
   uv run alembic upgrade head
   ```

4. **Run the API**:
   ```bash
   uv run uvicorn app.main:app --reload
   ```

5. **Run the worker**:
   ```bash
   uv run celery -A app.jobs.worker worker --loglevel=info
   ```

### Schema

Alembic migrations are the canonical source of truth for the database schema. Migration files live in `alembic/versions/`.

Use these commands to inspect and apply schema state:

```bash
# Apply all pending migrations
uv run alembic upgrade head

# Check current migration state
uv run alembic current

# View migration history
uv run alembic history
```

If you want a visual snapshot, generate an ER diagram on demand with an optional tool such as:

```bash
uvx eralchemy2 -i "$DATABASE_URL" -o schema.png
```

Point it at a migrated database. If the tool cannot use your async driver URL, use the equivalent synchronous SQLAlchemy/Postgres URL for the same database.

## Live API

The API surface is auto-documented from the running FastAPI application:

- **OpenAPI JSON schema**: `GET /openapi.json`
- **Swagger UI**: `GET /docs`
- **ReDoc**: `GET /redoc`

The interactive docs are unversioned FastAPI endpoints. The application API
itself mounts under `/v1`.

The static `docs/API.md` API plan was retired once Phase 1 shipped. The live
OpenAPI schema is the canonical source of truth for endpoints and request/response
shapes.

## API Consumer Quick Guide

Use the live OpenAPI docs for exact request and response shapes. The sections
below give an operator-friendly local verification path and a curl-first API
smoke workflow.

### Local verification checklist

After starting the stack, verify the basics in this order:

1. `curl http://localhost:8000/v1/health`
2. `curl http://localhost:8000/v1/system/health`
3. `make compose-smoke`
4. Open `http://localhost:8000/docs` for the live schema

If `GET /v1/system/health` reports degraded adapter availability, expect only
the adapters available in your environment to work.

### End-to-end API smoke workflow

This example walks the common API path: project -> upload -> job -> revision ->
validation -> entities -> quantities -> exports -> artifacts.

Set a reusable base URL:

```bash
export BASE_URL=http://localhost:8000
```

1. **Create a project**

```bash
curl -sS -X POST "$BASE_URL/v1/projects" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: readme-project-1' \
  -d '{"name":"README smoke project"}'
```

Save the returned project ID as `PROJECT_ID`.

2. **Upload a file and create the ingestion job**

```bash
export PROJECT_ID=<project-id>

curl -sS -X POST "$BASE_URL/v1/projects/$PROJECT_ID/files" \
  -H 'Idempotency-Key: readme-upload-1' \
  -F 'file=@tests/fixtures/dxf/simple-line.dxf'
```

Save the returned file ID as `FILE_ID` and the returned job ID as `JOB_ID`.

3. **Poll the job to a terminal state**

```bash
export JOB_ID=<job-id>

curl -sS "$BASE_URL/v1/jobs/$JOB_ID"
```

Repeat until the job reaches a terminal status such as `succeeded`, `failed`,
or `cancelled`.

4. **List revisions for the uploaded file**

```bash
export FILE_ID=<file-id>

curl -sS "$BASE_URL/v1/files/$FILE_ID/revisions"
```

Save the active revision ID as `REVISION_ID`.

5. **Inspect the validation report**

```bash
export REVISION_ID=<revision-id>

curl -sS "$BASE_URL/v1/revisions/$REVISION_ID/validation-report"
```

Check the returned validation outcome before starting downstream workflows.

6. **Read materialized entities**

```bash
curl -sS "$BASE_URL/v1/revisions/$REVISION_ID/entities"
```

7. **Create a quantity takeoff job**

```bash
curl -sS -X POST "$BASE_URL/v1/revisions/$REVISION_ID/quantity-takeoffs" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: readme-quantity-1' \
  -d '{}'
```

Save the returned quantity job ID, poll it via `GET /v1/jobs/{job_id}`, then
list takeoffs:

```bash
curl -sS "$BASE_URL/v1/revisions/$REVISION_ID/quantity-takeoffs"
```

Save the takeoff ID as `TAKEOFF_ID`. Use the returned gate/trust fields before
treating quantities as billing or estimating inputs.

8. **Create an export job**

Create a revision JSON export:

```bash
curl -sS -X POST "$BASE_URL/v1/revisions/$REVISION_ID/exports/revision-json" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: readme-export-revision-json-1' \
  -d '{"options":{}}'
```

Or create a quantity CSV export:

```bash
export TAKEOFF_ID=<takeoff-id>

curl -sS -X POST "$BASE_URL/v1/revisions/$REVISION_ID/quantity-takeoffs/$TAKEOFF_ID/exports/quantity-csv" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: readme-export-quantity-csv-1' \
  -d '{"options":{}}'
```

Save the returned export job ID and poll `GET /v1/jobs/{job_id}` to completion.

9. **List generated artifacts metadata**

```bash
curl -sS "$BASE_URL/v1/revisions/$REVISION_ID/generated-artifacts"
```

Use artifact endpoints for metadata and lineage. They do not expose raw bytes or
download URLs.

### Health and capabilities

- `GET /v1/health` is shallow liveness only.
- `GET /v1/system/health` checks dependencies and adapter availability and can
  return `503`.

### Core workflow

1. Create a project.
2. Upload a source file to that project.
3. Track the background job until terminal state.
4. Read the resulting revision, validation, quantities, estimates, or exports.

### Projects and files

- Start with project and file endpoints under `/v1`.
- File upload is multipart and expects the file field name `file`.
- Upload requests must include `Content-Length`.
- The API accepts PDF, DWG, DXF, and IFC inputs when the relevant adapters are
  available.

### Jobs

- Long-running ingestion, quantity, estimate, export, and changeset-apply work
  is job-backed.
- Use the jobs API to fetch job status, read job events, cancel work, and retry
  eligible failures.

### Revisions, materialization, and validation

- Revisions are the durable read model produced by ingestion or changeset apply.
- Read revision details and materialized entities through revision-scoped `/v1`
  endpoints.
- Validation reports are part of the revision workflow and determine whether
  downstream quantity and export flows are allowed, provisional, or blocked.

### Quantities

- Quantity takeoffs are revision-scoped.
- Review-gated or blocked validation outcomes are not equivalent to trusted
  totals; consumers should check returned gate and trust fields before using
  quantities as a billing or estimating input.

### Estimates

- Estimates are created from eligible quantity takeoffs, not directly from raw
  files.
- Estimate reads are also revision-scoped and preserve frozen pricing inputs for
  reproducibility.

### Exports and artifacts

- Export kinds currently include `revision_json`, `quantity_csv`,
  `estimate_csv`, `estimate_pdf`, and `revised_dxf`.
- Generated artifact routes expose metadata and lineage, not raw artifact bytes
  or presigned download URLs.
- Consumers should treat artifact metadata as the API contract available today.

### Changesets

- Changesets are revision-scoped proposals for CAD-visible or metadata-only
  edits.
- The API supports create, list, validate, and apply workflows under revision
  routes.
- Revised DXF export is intended for changeset-origin revisions.

### Adapter caveats

- Adapter availability is environment-dependent; use system health/capabilities
  checks instead of assuming every format is enabled everywhere.
- Current registry covers DWG, DXF, IFC, vector PDF, raster PDF, and a
  `revised_dxf` writer path.

### Idempotency

- Use `Idempotency-Key` on mutation requests that may be retried by clients.
- This is especially important for uploads and job-creating endpoints.

## MVP Direction

- API-only MVP product surface.
- Backend API that future web UIs, TUIs, CLIs, or services can consume.
- Primary starting inputs: DWG, vector PDF, and raster PDF.
- Direct normalized/open inputs: DXF and IFC where available.
- Outputs: JSON, CSV, PDF estimate/report, and editable CAD revisions.
- DWG support must be adapter-based because reliable commercial DWG read/write
  requires licensed tooling.
- RAG is out of scope for MVP.
- AI/agent behavior is out of the core path for MVP and should only suggest,
  classify, or explain after deterministic services exist.

## Planned Stack

- Python 3.12
- FastAPI
- Pydantic v2
- PostgreSQL 18
- SQLAlchemy 2.x
- Alembic
- Celery
- RabbitMQ
- Local filesystem storage first, S3-compatible storage later
- ezdxf for DXF handling
- IfcOpenShell for IFC handling
- DWG/PDF adapters through licensed or explicitly approved tools

## Where Things Live

GitHub is the single source of truth for what is being built and when:

- Tasks: [project board](https://github.com/users/momoshell/projects/1)
- Schedule: [milestones](https://github.com/momoshell/draupnir/milestones)
- Open work: [issues](https://github.com/momoshell/draupnir/issues)

The repo holds normative docs that travel with code through PR review:

- [MVP scope](docs/MVP.md)
- [Technical requirements](docs/TRD.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Architecture decisions](docs/decisions/)
- [Agent instructions](AGENTS.md)
