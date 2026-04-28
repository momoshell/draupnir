# ADR 0005 - Agent Is Advisory

## Status

Accepted

## Context

The system may later use an LLM or agent to classify ambiguous entities, explain
estimates, or propose optimizations. However, geometry, quantity extraction,
pricing, and structural validation must be deterministic and auditable.

## Decision

AI agents may propose structured changesets, classifications, assumptions, and
explanations. They must not directly mutate CAD files or act as the source of
truth for quantities or estimates.

## Consequences

- Safer revision workflow.
- Easier auditability.
- Agent output can be reviewed, validated, and rejected before export.

