# ADR 0007 - PyMuPDF As First Vector PDF Adapter

## Status

Accepted

## Context

The MVP must ingest vector PDF files and extract geometric entities (paths, lines,
curves, shapes) for normalization into the canonical drawing representation. Vector
PDF extraction is distinct from raster PDF processing and requires specialized
libraries capable of parsing PDF content streams and graphics operators.

Three candidate adapters were evaluated:

- **PyMuPDF (fitz)** — Python bindings for MuPDF. Supports `Page.get_drawings()`
  for vector path extraction including lines, curves, rectangles, and complex paths.
  AGPL-3.0 or commercial licensing. Active development with comprehensive PDF
  feature support.

- **pdfplumber** — MIT-licensed Python library built on pdfminer.six. Focused on
  text and table extraction with some geometric primitive support (chars, lines,
  rects, curves, edges). More text/table-oriented than pure vector graphics.

- **pdfminer.six** — MIT-licensed pure Python PDF parser. Low-level access to PDF
  content streams but requires significant custom code to extract vector geometry.

## Decision

Select **PyMuPDF** as the first vector PDF adapter.

Rationale:

- Superior vector graphics extraction via `Page.get_drawings()` API, providing
  direct access to PDF path construction operators (lines, curves, rectangles).
- Mature, actively maintained library with comprehensive PDF feature coverage.
- Clean Python API that fits naturally behind the existing adapter contract.
- Can extract both vector geometry and text in a single pass when needed.
- Performance is acceptable for MVP-scale document processing.

**Critical Constraint**: PyMuPDF is used ONLY as a replaceable extraction tool
behind the existing ingestion adapter contract. The core backend must never
import or depend on PyMuPDF objects directly.

## Adapter Contract Compliance

The vector PDF adapter must implement the standard adapter contract:

```text
adapt(source_file, options) ->
  AdapterResult {
    canonical: { layers, blocks, entities, units, ... },
    provenance: { adapter, source_ref, extraction_path, notes },
    confidence: float in [0, 1],
    warnings: [...],
    diagnostics: { adapter_name, adapter_version, started_at, finished_at },
  }
```

Required behavior:

- All PyMuPDF imports are isolated within the adapter implementation module.
- Adapter outputs canonical drawing entities, not PyMuPDF objects.
- Confidence scoring reflects semantic loss during PDF-to-entity conversion
  (typical range: 0.7-0.9 for vector PDF per TRD reference points).
- Warnings capture unsupported PDF features, font substitution issues, or
  pathological vector constructions.
- Diagnostics record PyMuPDF version and extraction timing for debugging.

## Licensing Considerations

PyMuPDF is a Python binding for MuPDF, which is licensed under AGPL-3.0
(or commercial license available from Artifex Software).

- **AGPL Surface**: Using PyMuPDF behind the adapter contract bounds the AGPL
  obligations to the adapter/service boundary, but does not eliminate them.
- **Distribution Impact**: If the system is distributed (on-prem deployment,
  Docker images bundling PyMuPDF, closed commercial packaging), AGPL obligations
  may apply to the adapter component.
- **Mitigation Options**:
  1. Publish/source the adapter/service code as needed for AGPL compliance.
  2. Purchase commercial MuPDF license from Artifex Software for proprietary
     distribution scenarios.
  3. Deploy PyMuPDF as an external service/microservice to further isolate
     the AGPL boundary.

## Alternatives Considered

1. **pdfplumber** — MIT-licensed alternative with good text/table extraction.
   Deferred as primary adapter because its vector graphics support is secondary
   to its text extraction focus. **Selected as the designated fallback option**:
   if licensing issues, quality problems, or runtime failures emerge with
   PyMuPDF, the adapter can be swapped to pdfplumber without changing the
   adapter contract.

2. **pdfminer.six** — Low-level PDF parser requiring significant custom
   geometry extraction code. Rejected due to implementation complexity and
   maintenance burden for MVP timeline.

3. **Commercial PDF SDKs** (e.g., Adobe PDF Library, Foxit SDK) — Offer
   comprehensive PDF support but introduce licensing costs and vendor lock-in.
   Deferred to post-MVP evaluation if PyMuPDF proves insufficient.

## Consequences

- **Positive**: Best-in-class vector path extraction for Python; clean adapter
  contract integration; single-pass geometry + text extraction capability.

- **Negative**: AGPL-3.0 licensing requires compliance awareness for
  distribution scenarios; commercial license may be needed for closed-source
  on-prem or SaaS offerings depending on legal interpretation.

- **Mitigation**: Adapter contract isolation allows clean swap to pdfplumber
  (MIT) if licensing or technical issues arise. Core backend remains
  license-agnostic through the adapter abstraction.

- **Risk**: AGPL obligations in distributed scenarios may require code
  publication or commercial licensing. Monitor usage context and legal
  requirements as deployment scenarios evolve.

- **Follow-up**: Implement adapter contract tests for both PyMuPDF and
  pdfplumber to ensure swap compatibility. Document swap procedure in
  adapter README.
