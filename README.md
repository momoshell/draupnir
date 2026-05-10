# draupnir

[![CI](https://github.com/momoshell/draupnir/actions/workflows/ci.yml/badge.svg)](https://github.com/momoshell/draupnir/actions/workflows/ci.yml)

Draupnir is a backend-first CAD/BIM ingestion and estimation system. The MVP
focuses on accepting common real-world drawing inputs, extracting structured
geometry and semantic data, producing deterministic quantities and estimates,
and exposing everything through a UI-agnostic API.

The MVP product surface is API-only. Future clients may include a web UI, TUI,
CLI, or other services, but this repository does not imply that a product CLI
exists today.

## Current Status

Docker Compose development stack is available. See "Local Development" below for setup instructions.

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
>   make down -v
>   # or: docker compose down -v
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

3. **Verify services are running**:
   ```bash
   make ps
   # or: docker compose ps
   ```

4. **Check API health**:
   ```bash
   curl http://localhost:8000/v1/health
   ```

5. **View RabbitMQ management UI**:
    - Open http://localhost:15672 in your browser
    - Login: guest / guest
    - AMQP is also exposed on localhost:5672 for host-side clients only

6. **View logs**:
   ```bash
   make logs
   # or: docker compose logs -f
   ```

7. **Stop all services**:
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
| flower    | localhost:5555       | Celery dashboard (optional)    |

Postgres, RabbitMQ, and Flower host ports bind to `127.0.0.1` only. Containers
still reach RabbitMQ over the internal Docker network at `rabbitmq:5672`.

### Useful Commands

```bash
make shell-api      # Open shell in API container
make shell-worker   # Open shell in worker container
make migrate        # Run database migrations
make down -v        # Stop and remove volumes (destructive)
```

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
   uv sync --locked --extra db --extra jobs --extra dev --extra test
   ```

2. **Set up environment**:
   ```bash
   cp .env.example .env
   # Adjust DATABASE_URL and BROKER_URL for local services
   ```

3. **Run the API**:
   ```bash
   uv run uvicorn app.main:app --reload
   ```

4. **Run the worker**:
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

- **OpenAPI JSON schema**: `GET /v1/openapi.json`
- **Swagger UI**: `GET /v1/docs`
- **ReDoc**: `GET /v1/redoc`

The static `docs/API.md` API plan was retired once Phase 1 shipped. The live
OpenAPI schema is the canonical source of truth for endpoints and request/response
shapes.

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
