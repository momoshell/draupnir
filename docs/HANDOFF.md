# Open Decisions & Handoff Notes

## Closed

- [ADR-0001](./decisions/0001-python-first.md) — Python-first backend
- [ADR-0002](./decisions/0002-postgres-from-start.md) — Postgres from start
- [ADR-0003](./decisions/0003-no-rag-for-mvp.md) — No RAG for MVP
- [ADR-0004](./decisions/0004-dwg-pdf-first-ingestion.md) — DWG/PDF first ingestion
- [ADR-0005](./decisions/0005-agent-is-advisory.md) — Agent is advisory only
- [ADR-0006](./decisions/0006-dwg-adapter.md) — LibreDWG as first DWG adapter
- [ADR-0007](./decisions/0007-vector-pdf-adapter.md) — PyMuPDF as first vector PDF adapter
- [ADR-0008](./decisions/0008-raster-pdf-strategy.md) — Raster PDF candidate CAD pipeline with VTracer, centerline extraction, and Tesseract

## Open
- Final PDF report generator (WeasyPrint vs ReportLab)
- Geometry validation engine (FreeCAD vs OCCT)
- Webhook/SSE vs polling-only for MVP events
- Materials: shared catalog vs per-project
- Generated artifact quantity foreign key remains deferred even though docs now
  describe append-only quantity takeoff persistence lineage.

## External Review Follow-Ups

- External architecture review follow-ups are tracked in issues #54-#69.
- Keep these docs aligned with accepted ADRs; do not expand full specs for those
  follow-up issues here.
