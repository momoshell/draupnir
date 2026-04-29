# ADR 0008 - Raster PDF Candidate CAD Pipeline With VTracer, Centerline Extraction, And Tesseract

## Status

Accepted

## Context

The MVP must ingest raster PDF files (scanned blueprints, photographed drawings) and extract geometric entities and text for normalization into the canonical drawing representation. Raster PDF processing requires two distinct capabilities: vectorization (tracing) to convert pixel-based geometry into lines and curves, and OCR to extract dimension text and annotations.

Four candidate strategies were evaluated:

- **VTracer** — MIT-licensed Rust-based tracer with Python bindings. Designed for high-resolution blueprint scans, outputs SVG/vector paths with tunable parameters for clustering, curve fitting, and speckle filtering.

- **Potrace** — GPL-2.0 bitmap tracing program. B&W only, last release 2019. Limited color support and maintenance.

- **Tesseract** — Apache 2.0 OCR engine maintained by Google. Lightweight (~10MB), CPU-only, fast on clean printed text. Decades of production use.

- **PaddleOCR** — Apache 2.0 OCR with higher accuracy but ~500MB install and PaddlePaddle dependency. Heavier runtime.

## Decision

Select a **multi-stage raster-to-CAD pipeline** as the first raster PDF strategy. The pipeline produces an editable candidate DXF/CAD representation, not a faithful reconstruction. The output requires human review before trusted quantities can be derived.

### Pipeline Stages

1. **Raster Image Preprocessing**: Deskew, threshold/binarize, denoise/despeckle, and contrast cleanup to prepare scanned blueprints for extraction.

2. **VTracer for Outline/Shape/Symbol Candidates**: Use VTracer to trace high-resolution raster regions into SVG paths suitable for shapes, symbols, and non-linework geometry. VTracer excels at boundary detection but produces outline-style paths, not engineering centerlines.

3. **Centerline/Skeletonization Path for Engineering Linework**: When VTracer outlines are too thick or outline-oriented for engineering linework, apply OpenCV/scikit-image centerline extraction and skeletonization to derive single-pixel-width centerlines from thick raster lines.

4. **CAD Entity Reconstruction**: Convert traced paths and centerlines into canonical CAD entities—lines, polylines, arcs/circles where detectable—plus text annotations. Reconstruct drawing structure (layers, blocks) with units, scale, provenance, and confidence metadata.

5. **Tesseract OCR for Dimension Text and Annotations**: Extract dimension text, labels, and annotations using Tesseract on preprocessed raster regions.

6. **Manual Scale Calibration for MVP**: User provides a known measurement (e.g., "this line = 5 meters") to compute pixel-to-unit ratio. Auto-detection of dimension lines is deferred to post-MVP.

7. **Human Review Gate**: All raster PDF outputs are **review candidates, not trusted quantities**. Confidence is reported in the range 0.3-0.6 per the TRD. All traced entities and OCR-extracted text must be flagged for human review before quantities are accepted for estimation.

### Rationale

- **VTracer**: MIT licensed, native Python bindings, designed for high-resolution blueprint scans, tunable parameters for clustering and curve fitting. Best suited for shapes and symbols, not centerline extraction.
- **OpenCV/scikit-image**: Permissive BSD-style licenses, proven computer vision libraries for morphological operations and skeletonization. Provides the centerline path missing from VTracer's outline-oriented output.
- **Tesseract**: Apache 2.0 licensed, lightweight (~10MB), fast on clean printed text, decades of production use. Adequate for dimension text when preprocessed.
- **Pipeline Approach**: No single tool converts raster drawings to CAD faithfully. Combining preprocessing, outline tracing, centerline extraction, and CAD reconstruction produces an editable candidate that users can review and correct.

## Adapter Contract Compliance

The raster PDF adapter must implement the standard adapter contract:

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

- All VTracer, OpenCV/scikit-image, and Tesseract imports are isolated within the adapter implementation module.
- Adapter outputs canonical drawing entities, not VTracer SVG paths or Tesseract text boxes.
- Confidence scoring reflects semantic loss during raster-to-vector conversion and OCR (typical range: 0.3-0.6 for raster PDF per TRD reference points).
- Warnings capture tracing artifacts, OCR confidence below threshold, unsupported image formats, or pathological raster constructions.
- Diagnostics record VTracer version, Tesseract version, and extraction timing for debugging.

## Licensing Considerations

All pipeline components use permissive open-source licenses with no copyleft obligations:

- **MIT (VTracer)**: Permits commercial use, modification, distribution, and sublicensing. No requirement to disclose source code or use the same license for derivative works.
- **BSD-3-Clause (OpenCV, scikit-image)**: Permissive licenses allowing commercial use and redistribution with attribution. No copyleft obligations.
- **Apache 2.0 (Tesseract)**: Similar permissions to MIT with explicit patent grant protection. No copyleft obligations.
- **Distribution Impact**: None of these licenses impose obligations on the containing application. The raster PDF adapter can be distributed, bundled in Docker images, or used in SaaS offerings without license contamination.
- **Mitigation**: Zero licensing risk for MVP. Adapter contract isolation ensures that future swaps to alternatives (including copyleft-licensed tools) are bounded to the adapter boundary.

## Alternatives Considered

1. **Potrace** — GPL-2.0 bitmap tracing program. B&W only, last release 2019. Rejected due to copyleft licensing and limited color support/maintenance.

2. **AutoTrace** — GPL-2.0 tracing tool with less maintenance and documentation gaps. Rejected due to licensing and maturity concerns.

3. **PaddleOCR** — Apache 2.0 OCR with higher accuracy on complex documents. **Selected as the designated fallback**: if Tesseract accuracy on degraded or complex engineering drawings proves insufficient, the adapter can be swapped to PaddleOCR without changing the adapter contract. Deferred due to ~500MB install size and PaddlePaddle dependency.

4. **Commercial tracing SDKs** (e.g., Adobe PDF SDK, Foxit SDK) — Offer comprehensive PDF support but introduce licensing costs and vendor lock-in. Deferred to post-MVP evaluation if VTracer proves insufficient.

## Consequences

- **Positive**: MIT + BSD-style + Apache 2.0 stack means zero copyleft risk; VTracer handles high-res blueprint scans well; OpenCV/scikit-image provide centerline extraction; Tesseract is lightweight and fast; adapter contract allows clean swap to alternatives.

- **Negative**: Traced geometry from raster is inherently approximate; OCR on degraded drawings will have errors; manual scale calibration is a UX friction point.

- **Mitigation**: Low confidence scores (0.3-0.6), mandatory human review gate, adapter contract isolation for future swaps.

- **Risk**: VTracer outputs SVG paths that need conversion to canonical CAD entities (lines, arcs, polylines) — this conversion layer is non-trivial. VTracer produces vector paths/outline-style SVG, not CAD semantics; the output represents traced boundaries rather than engineering linework centerlines.

- **Fallback**: If VTracer output proves too outline-oriented for engineering linework, evaluate a Python-orchestrated OpenCV/scikit-image centerline/skeletonization pipeline before replacing the tracing strategy. This provides native-backed adapter tooling that may better extract centerlines from thick raster lines.

- **Follow-up**: Scaffold the raster PDF adapter; evaluate PaddleOCR if Tesseract accuracy on dimension text is insufficient; explore auto-dimension-line detection for scale calibration post-MVP.
