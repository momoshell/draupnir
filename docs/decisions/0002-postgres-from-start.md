# ADR 0002 - PostgreSQL From The Start

## Status

Accepted

## Context

The system must persist projects, files, jobs, extracted entities, quantities,
estimates, changesets, and artifacts. Job state and auditability matter even for
local MVP work.

## Decision

Use PostgreSQL from the start.

Use SQLAlchemy 2.x and Alembic for database access and migrations.

## Consequences

- More setup than SQLite.
- Better alignment with future SaaS requirements.
- Supports durable jobs, queryable entities, and estimate audit trails.

