# ADR 0004 - DWG And PDF First Ingestion

## Status

Accepted

## Context

The expected starting files are DWG, vector PDF, and raster PDF. DXF and IFC are
still important, but they are not enough for the practical first user workflow.

DWG is proprietary and PDF extraction can lose CAD semantics. Raster PDFs are
especially uncertain because they require image processing, tracing, and often
human review.

## Decision

Treat DWG, vector PDF, and raster PDF as starting input targets.

Implement them through adapters so the project can swap tooling as licensing,
quality, and deployment constraints become clear.

DXF and IFC remain first-class normalized/open inputs.

## Consequences

- The MVP targets real user files sooner.
- Adapter choice becomes an early critical decision.
- Raster PDF support may initially be experimental and confidence-scored.

