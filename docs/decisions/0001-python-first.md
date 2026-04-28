# ADR 0001 - Python First

## Status

Accepted

## Context

The project depends heavily on CAD/BIM libraries that are strongest in Python:

- ezdxf
- IfcOpenShell
- FreeCAD Python APIs
- Pydantic/FastAPI ecosystem

Rust remains attractive for a future control plane, but it would slow the MVP and
add Python embedding or service boundary complexity too early.

## Decision

Start with a Python backend.

Use FastAPI, Pydantic, Celery, SQLAlchemy, and Alembic.

## Consequences

- Faster CAD/BIM ingestion development.
- Easier integration with existing libraries.
- Rust can still be introduced later for specific performance or orchestration
  needs.

