# ADR 0003 - No RAG For MVP

## Status

Accepted

## Context

The MVP is primarily a deterministic ingestion, quantity, estimation, and export
backend. RAG is useful later for historical project lookup, document Q&A, and
similarity search, but it is not needed to parse drawings or calculate estimates.

## Decision

Do not add RAG or Graph RAG to the MVP.

## Consequences

- Lower architectural complexity.
- More focus on reliable CAD/BIM extraction.
- Future graph or vector retrieval can be added after project data exists.

