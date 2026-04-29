# ADR 0006 - LibreDWG As First DWG Adapter

## Status

Accepted

## Context

The MVP must ingest DWG files, which are proprietary and require specialized
tooling. Four candidate adapters were evaluated:

- ODA Drawings SDK — Commercial ($3,000–$37,500/yr). Full coverage but high
  cost barrier for MVP.
- Autodesk RealDWG — Proprietary, restricted access, not viable for independent
  MVP.
- QCAD Professional — One-time license (~€42). Wraps ODA (Teigha) engine,
  headless mode, outputs DXF directly.
- LibreDWG — GPL-3.0+ C library with SWIG Python bindings. ~99% read coverage
  through r2018, active maintenance (0.13.4, March 2026).

## Decision

Select **LibreDWG** as the first DWG adapter.

Rationale:

- Free and open-source; no licensing friction for MVP development.
- Native C library with Python bindings; no CLI wrapping required.
- ~99% read coverage for DWG r1.2 through r2018.
- Can convert DWG to DXF/JSON for our canonical representation pipeline.
- Aligns with the project's preference for open normalized inputs.
- Easy to swap for ODA SDK later without changing the adapter contract.

## Alternatives Considered

1. **ODA Drawings SDK** — Best long-term option with full Python API and 100%
   coverage. Deferred to Phase 4 due to $3,000–$37,500 annual subscription cost.

2. **Autodesk RealDWG** — Proprietary SDK with restricted partner access.
   Rejected due to unavailable licensing for independent projects.

3. **QCAD Professional** — Low-cost CLI wrapper around ODA/Teigha. Rejected
   because LibreDWG provides a native library with Python bindings, avoiding
   CLI process management and per-seat/server licensing complexity.

## Consequences

- **Positive**: Zero licensing cost, native Python integration, strong read
  coverage, active GNU project maintenance.
- **Negative**: GPL-3.0+ copyleft requires compliance awareness if the adapter
  is distributed (e.g., on-prem packaging, Docker images bundling LibreDWG).
  Write support for r2004+ DWG is incomplete.
- **Mitigation**: Use LibreDWG as an external process or clearly isolated
  optional component. Do not vendor or modify without recording GPL compliance
  obligations. Adapter contract is abstracted; swapping to ODA SDK later
  requires only adapter implementation change.
- **Risk**: If future distribution requires bundling LibreDWG, revisit GPL
  compliance or add QCAD/ODA as an alternative adapter.
- **Follow-up**: Phase 4 should scaffold the LibreDWG adapter implementation
  and evaluate ODA Drawings SDK as a second adapter.
