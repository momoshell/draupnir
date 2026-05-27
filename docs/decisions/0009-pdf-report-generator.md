# ADR 0009 - ReportLab As First Estimate PDF Report Generator

## Status

Accepted

## Context

The MVP Phase 8 export plan includes `estimate_pdf` as a generated artifact for a
finalized estimate derived from a trusted quantity takeoff. Issue #249 is limited
to selecting the first PDF generator; implementation of pure PDF generation
remains in #255 and worker finalization remains in #256.

The first generator must fit the MVP export contract:

- deterministic output for pinned lineage, generator version, and options
- backend-first runtime with no browser dependency
- acceptable layout control for estimate report tables, totals, headers, and
  pagination
- licensing that is safe to document and operate in MVP
- a clean fallback path if formatting requirements outgrow the first choice

Three options were evaluated:

- **ReportLab** — Python PDF generation toolkit with direct drawing primitives,
  table/layout helpers, and no browser runtime requirement. BSD-style license.

- **WeasyPrint** — HTML/CSS to PDF engine for Python. Good paged-media layout and
  print styling, but adds a larger rendering stack and shifts report generation
  toward HTML templating.

- **Playwright/Chromium print-to-PDF** — browser-based rendering fallback with
  strong CSS fidelity, but heavyweight runtime and operational surface for MVP.

## Decision

Select **ReportLab** as the first estimate PDF report generator.

Rationale:

- More direct fit for backend-generated, structured estimate reports.
- Better operational simplicity for MVP because it avoids bundling a browser or
  HTML/CSS rendering stack.
- Supports explicit programmatic layout for tables, totals, page headers,
  footers, and page breaks.
- Easier to keep export behavior deterministic because document structure is
  produced from Python code rather than browser-style rendering behavior.
- Matches the project preference for small, swappable backend components behind a
  clear export contract.

## Licensing Considerations

- **ReportLab** uses a permissive BSD-style open-source license suitable for MVP
  distribution and hosted operation.
- **WeasyPrint** also uses a permissive BSD-style license, so licensing is not
  the primary differentiator between the two candidates.
- **Playwright** and **Chromium** are not selected for MVP; they remain a
  deferred fallback because their heavier runtime and packaging footprint are the
  main concern, not an immediate licensing blocker.
- This ADR does not add a legal exception or require special runtime license
  acknowledgement.

## Alternatives Considered

1. **WeasyPrint** — Main alternative. Advantages:
   - Strong HTML/CSS paged-media model for branded documents and print styling.
   - Potentially faster iteration if future reports need complex visual layouts
     that map naturally to templates.

   Deferred as the first generator because:
   - it introduces a larger rendering/runtime dependency surface than ReportLab
   - it pushes MVP report generation toward HTML/CSS template maintenance
   - deterministic pagination and layout can become more sensitive to renderer
     behavior, fonts, and environment differences than a direct PDF writer

   **Designated fallback**: if estimate PDF requirements become more document-
   design-heavy than data-report-heavy, WeasyPrint is the first alternative to
   evaluate behind the same export contract.

2. **Playwright/Chromium print-to-PDF** — Deferred heavyweight fallback. Useful
   as a reference option when exact browser print rendering becomes necessary,
   but rejected for MVP-first selection because it adds a browser runtime,
   operational complexity, and larger dependency footprint.

## Consequences

- **Positive**: smaller backend runtime surface; explicit layout control;
  deterministic, code-driven PDF generation; good fit for estimate tables and
  totals.

- **Negative**: visual composition is more programmatic and less designer-friendly
  than HTML/CSS templating. Complex branded layouts may take more manual work.

- **Mitigation**: keep PDF generation behind a swappable export generator
  boundary so #255 can implement ReportLab first without locking out a later
  WeasyPrint-based generator.

- **Fallback**: if ReportLab proves too rigid for future document styling,
  evaluate WeasyPrint first; use Playwright/Chromium print-to-PDF only if a
  browser-rendered path becomes necessary after that.
