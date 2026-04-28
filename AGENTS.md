# AGENTS.md

This file is the operating guide for coding agents working in this repository.
Do not rely on chat history for project requirements. Use the docs in this repo.

## Project Goal

Build a backend-first CAD/BIM ingestion, estimation, and revision system. The
backend must expose APIs that can be consumed by a web UI, TUI, CLI, or another
service.

## Current MVP Scope

- Accept DWG, vector PDF, and raster PDF inputs.
- Treat DXF and IFC as first-class normalized/open inputs where available.
- Preserve original uploads immutably.
- Normalize drawings into a canonical internal representation.
- Store full extracted entities and provenance in Postgres.
- Compute quantities deterministically.
- Generate JSON, CSV, PDF, and editable CAD revision exports.
- Model edits as versioned changesets before writing revised CAD files.
- Make jobs cancellable and retryable.
- Keep auth disabled for local MVP, but design for API keys later.

## Explicitly Out Of Scope For MVP

- RAG or Graph RAG.
- LLM-driven source-of-truth geometry, pricing, or structural validation.
- Direct LLM writes to DWG/DXF/IFC.
- Full production multi-tenant auth.
- Unlicensed or legally unclear DWG/PDF conversion dependencies.

## Planned Stack

- Python 3.12
- FastAPI
- Pydantic v2
- PostgreSQL 18
- SQLAlchemy 2.x
- Alembic
- Celery
- RabbitMQ
- Local filesystem storage first
- ezdxf
- IfcOpenShell
- FreeCAD/OCCT later for geometry validation
- LiteLLM + Pydantic AI later, only for optional advisory workflows

## Repository Layout

Planned layout:

```text
app/
  api/
  core/
  db/
  models/
  schemas/
  jobs/
  ingestion/
  cad/
  estimating/
  exports/
  storage/
tests/
docs/
```

## Development Commands

Application code is not scaffolded yet. When scaffolding is added, update this
section with exact commands for:

- dependency installation
- local development server
- database migrations
- worker startup
- tests
- linting and formatting

## Engineering Rules

- Prefer small, typed, testable modules.
- Keep original CAD/PDF uploads immutable.
- Do not commit proprietary drawing samples unless they are explicitly approved
  as fixtures.
- Store derived revisions separately from original inputs.
- All schema changes require Alembic migrations.
- Store job state in Postgres, not only in memory.
- Long-running ingestion and export work belongs in workers, not API handlers.
- Do not add RAG or agent frameworks unless a scoped task explicitly asks for it.
- Do not introduce GPL/AGPL/commercially risky dependencies without documenting
  the decision in `docs/decisions/`.
- Use adapters for DWG/PDF conversion so tooling can be swapped later.

## Handoff Protocol

GitHub is the single source of truth for status and outstanding work. Before
ending a work session:

- move the relevant issue on the [project board](https://github.com/users/momoshell/projects/1)
  to its new column (Backlog / Ready / In progress / In review / Done)
- update or comment on the issue with completed work, blockers, and next steps
- open new issues for follow-ups instead of leaving TODO comments in code
- record any setup or test failures on the issue
- keep generated artifacts out of git unless they are explicit fixtures
- do not leave database model changes without a migration
