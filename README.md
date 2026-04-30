# draupnir

[![CI](https://github.com/momoshell/draupnir/actions/workflows/ci.yml/badge.svg)](https://github.com/momoshell/draupnir/actions/workflows/ci.yml)

Draupnir is a backend-first CAD/BIM ingestion and estimation system. The MVP
focuses on accepting common real-world drawing inputs, extracting structured
geometry and semantic data, producing deterministic quantities and estimates,
and exposing everything through a UI-agnostic API.

## Current Status

Docker Compose development stack is available. See "Local Development" below for setup instructions.

## Local Development

### Prerequisites

- Docker and Docker Compose (v2)
- Python 3.12 (for local development without Docker)

### Quick Start with Docker

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
| postgres  | 5432                 | PostgreSQL database            |
| rabbitmq  | 5672, 15672          | RabbitMQ message broker        |
| flower    | 5555                 | Celery dashboard (optional)    |

### Useful Commands

```bash
make shell-api      # Open shell in API container
make shell-worker   # Open shell in worker container
make migrate        # Run database migrations
make down -v        # Stop and remove volumes (destructive)
```

### Local Development (without Docker)

1. **Install dependencies**:
   ```bash
   pip install -e ".[db,jobs,dev,test]"
   ```

2. **Set up environment**:
   ```bash
   cp .env.example .env
   # Adjust DATABASE_URL and BROKER_URL for local services
   ```

3. **Run the API**:
   ```bash
   uvicorn app.main:app --reload
   ```

4. **Run the worker**:
   ```bash
   celery -A app.jobs.worker worker --loglevel=info
   ```

## MVP Direction

- Backend API that any web UI, TUI, CLI, or service can consume.
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

Two transitional docs will be retired in favor of live equivalents:

- [API plan](docs/API.md) - replaced by `/v1/openapi.json` once Phase 1 ships.
- [Data model](docs/DATA_MODEL.md) - replaced by Alembic migrations once Phase
  2 ships.
