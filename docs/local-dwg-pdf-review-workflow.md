# Local DWG/PDF Review Workflow

Use this workflow when you need to re-run a proprietary DWG or PDF extraction review on your own machine without committing the source file, manifest entries, or generated artifacts. For PDFs, choose the mode deliberately with the upload `extraction_profile.pdf_input_mode` payload. Future containerized PDF service-boundary work is out of scope for #358 and is not documented here as available behavior.

## Hard rules

- Keep proprietary samples local-only.
- Do not add proprietary samples to `tests/fixtures/manifest.yaml`.
- Do not commit generated artifacts, debug overlays, or local review notes that expose sensitive details.
- Use neutral placeholders in screenshots, shell history, notes, and issue updates.

If you need a committed fixture or a manifest entry for a proprietary sample, stop and get explicit clearance first.

## What this workflow is for

- Re-running local ingestion for proprietary DWG, vector PDF, or raster PDF samples
- Inspecting validation and materialized entities
- Generating local-only debug overlays for visual QA
- Checking whether quantity results are allowed, provisional, review-gated, or blocked

## Before you start

1. Copy the sample into a local-only location outside git, or into an ignored scratch location you will not commit.
2. If you need local vector PDF support, prefer the narrower `pdf-vector` extra before reaching for the full `ingestion` bundle, and do it before any Compose rebuild/start rather than assuming the default Compose bundle includes it:
   - Host install: sync `pdf-vector` for vector-only work, or `ingestion` only when you also need raster dependencies.
   - Compose rebuild/start: set both `DRAUPNIR_UV_EXTRAS` and the needed license probe values before `docker compose build` or `make up`.
   - Licensing: PyMuPDF is covered by the ADR-0007 AGPL/commercial caveat, so set the approval probe only after your deployment review.
3. If you need local raster PDF support, install the full `ingestion` extra so the raster pipeline dependencies are present. Do not expect `pdf-vector` alone to cover raster mode. `vtracer` is required; `tesseract` is optional/degraded and should be interpreted through capability reporting rather than assumed.
4. If you are testing DWG extraction, make sure the required local license probe value is also present in your session.
5. If you need both DWG and vector PDF review in the same session, use a comma-separated `DRAUPNIR_APPROVED_LICENSE_PROBES` value so one probe does not overwrite the other:
   ```bash
   export DRAUPNIR_APPROVED_LICENSE_PROBES=pymupdf-deployment-review,libredwg-distribution-review
   ```
   If you only need one workflow, export only the relevant probe value.
6. Confirm your local stack is running:
   - Default local stack:
     ```bash
     make up
     make migrate
     ```
   - Vector-PDF Compose stack: set the env vars first, then rebuild/start and migrate:
     ```bash
     export DRAUPNIR_UV_EXTRAS="--extra db --extra jobs --extra dxf --extra pdf-vector"
     export DRAUPNIR_APPROVED_LICENSE_PROBES=pymupdf-deployment-review
     docker compose build api worker
     docker compose up -d
     make migrate
     ```
      If you also need DWG review in the same Compose session, use the comma-separated probe value instead.
   - Raster-PDF host setup: install the full ingestion extra so raster dependencies are available:
      ```bash
      uv sync --locked --extra db --extra jobs --extra ingestion
      ```
   - Raster-PDF Compose stack: rebuild with the full ingestion extra when you want raster review in containers:
      ```bash
      export DRAUPNIR_UV_EXTRAS="--extra db --extra jobs --extra ingestion"
      docker compose build api worker
      docker compose up -d
      make migrate
      ```
7. If needed, raise the LibreDWG JSON output cap for larger local DWG review runs:
   ```bash
   export LIBREDWG_MAX_OUTPUT_MB=64
   ```

### Host and Compose enablement notes

- Host-side vector PDF review should normally use the narrower path: `uv sync --locked --extra db --extra jobs --extra pdf-vector`. Use `uv sync --locked --extra db --extra jobs --extra ingestion` instead when the same local environment also needs raster PDF dependencies.
- Host-side raster PDF review requires the full ingestion extra: `uv sync --locked --extra db --extra jobs --extra ingestion`. The raster path requires `vtracer`; `tesseract` is optional/degraded and may surface through capability reporting when absent.
- Default Compose stays DXF-only. For vector-only PDF review, rebuild with `pdf-vector` rather than the full ingestion bundle. Export the env vars before build/start:
  ```bash
  export DRAUPNIR_UV_EXTRAS="--extra db --extra jobs --extra dxf --extra pdf-vector"
  export DRAUPNIR_APPROVED_LICENSE_PROBES=pymupdf-deployment-review
  docker compose build api worker
  docker compose up -d
  ```
- If the same Compose session needs both vector PDF and DWG review, keep the `pdf-vector` extras and use:
  ```bash
  export DRAUPNIR_APPROVED_LICENSE_PROBES=pymupdf-deployment-review,libredwg-distribution-review
  ```
- Default Compose still does not include PDF support unless you rebuild with the needed extras intentionally. For raster PDF review in Compose, switch from the narrow `pdf-vector` extras to the full `ingestion` extras.

### If PDF ingestion is still unavailable

If a PDF upload fails with `ADAPTER_UNAVAILABLE`, distinguish the common local causes:

- Missing package/runtime: the worker or API image was built without `pdf-vector`/`ingestion`, so the vector adapter is not installed.
- Missing raster runtime: the worker or API environment lacks the full `ingestion` extra or a required raster binary such as `vtracer`.
- Missing license approval: PyMuPDF is installed, but `DRAUPNIR_APPROVED_LICENSE_PROBES=pymupdf-deployment-review` was not set for the relevant process.
- Degraded OCR capability: raster ingestion can still report reduced capability when `tesseract` is missing.

Use `GET /v1/system/health` plus the job failure/event diagnostics to tell those cases apart before retrying.

## Extraction-readiness smoke procedure

Use this named procedure when you have representative local DWG/PDF inputs and want a repeatable readiness check before trusting follow-on comparison or quantity work.

### Expected inputs

- One representative DWG kept local-only.
- One representative vector or raster PDF for the same or equivalent drawing content, also kept local-only.
- Neutral local labels for each file so your notes do not expose sample names or paths.

### Smoke outcomes

- **Pass**: title metadata, labels, geometry, overlay alignment, units/calibration, and entity/layer meaning are good enough for comparison; validation is `valid` or `valid_with_warnings`; quantity work can proceed only if the quantity gate is `allowed`.
- **Provisional**: extraction is usable for review, but some warnings or partial recovery remain; quantity output stays provisional or otherwise not fully trusted.
- **Review-gated**: extraction is review-first; use it for human comparison only. If the quantity gate is `review_gated`, or if units, calibration, or geometry are still unresolved, keep takeoff blocked or untrusted.
- **Fail**: extraction is not reliable enough for comparison or downstream use; validation is `invalid`, geometry is materially wrong, or critical title/label/context recovery is missing.

## Run the local review

1. Create a project:
   ```bash
   export BASE_URL=http://localhost:8000

   curl -sS -X POST "$BASE_URL/v1/projects" \
     -H 'Content-Type: application/json' \
     -d '{"name":"local-review"}'
   ```
2. Upload the local sample. For PDF uploads, send the optional multipart `extraction_profile` JSON form field to choose vector or raster mode deliberately:
   ```bash
   export PROJECT_ID=<project-id>
   export LOCAL_SAMPLE=/path/to/local-sample.dwg

   curl -sS -X POST "$BASE_URL/v1/projects/$PROJECT_ID/files" \
     -H 'Idempotency-Key: local-review-upload-1' \
     -F "file=@$LOCAL_SAMPLE"
   ```
   Example PDF uploads:
   ```bash
   curl -sS -X POST "$BASE_URL/v1/projects/$PROJECT_ID/files" \
     -H 'Idempotency-Key: vector-pdf-upload-1' \
     -F 'extraction_profile={"pdf_input_mode":"vector"};type=application/json' \
     -F 'file=@tests/fixtures/pdf/vector-smoke.pdf;type=application/pdf'

   curl -sS -X POST "$BASE_URL/v1/projects/$PROJECT_ID/files" \
     -H 'Idempotency-Key: raster-pdf-upload-1' \
     -F 'extraction_profile={"pdf_input_mode":"raster"};type=application/json' \
     -F 'file=@tests/fixtures/pdf/raster-smoke.pdf;type=application/pdf'
   ```
3. Poll the returned `JOB_ID` until it reaches a terminal state:
   ```bash
   export JOB_ID=<job-id>

   curl -sS "$BASE_URL/v1/jobs/$JOB_ID"
   ```
4. Save the active `REVISION_ID`, then inspect the revision outputs:
   ```bash
   export REVISION_ID=<revision-id>

   curl -sS "$BASE_URL/v1/revisions/$REVISION_ID/validation-report"
   curl -sS "$BASE_URL/v1/revisions/$REVISION_ID/entities"
   curl -sS "$BASE_URL/v1/revisions/$REVISION_ID/generated-artifacts"
   ```
5. If the revision is eligible for a quantity run, create a takeoff job:
    ```bash
    curl -sS -X POST "$BASE_URL/v1/revisions/$REVISION_ID/quantity-takeoffs" \
      -H 'Content-Type: application/json' \
      -H 'Idempotency-Key: local-review-quantity-1' \
      -d '{}'
    ```

### Repeatable extraction-readiness smoke flow

Run these checks for the local DWG and the local PDF mode you are evaluating, then compare the review records side by side when needed.

1. Pick a representative local DWG/PDF pair and assign neutral labels.
2. Run the upload and revision inspection flow for the DWG.
3. Run the same flow for the PDF, using `{"pdf_input_mode":"vector"}` or `{"pdf_input_mode":"raster"}` as appropriate.
4. For each revision, review:
   - title metadata recovery such as drawing identity, revision markers, sheet markers, or dates
   - containment and navigation cues such as rooms, spaces, zones, or enclosure boundaries when the drawing carries them
   - equipment, fixture, callout, and room labels that a reviewer would use to confirm meaning
   - route or path linework plus debug-overlay alignment against the source drawing
   - units, PDF calibration status, and whether missing units or geometry makes quantities untrusted
   - entity/layer meaning, including whether important content fell into unknown or fallback buckets
   - validation status and any findings that still require review
   - quantity gate, noting that `review_gated` or unresolved units/geometry means takeoff is still blocked or untrusted
5. Classify each input as pass, provisional, review-gated, or fail.
6. Only treat the pair as extraction-ready when both inputs are at least good enough for human comparison and neither has unresolved issues that would hide drawing meaning or make quantity trust impossible.

### Committed PDF smoke fixtures

Use these committed smoke fixtures when you want a non-proprietary PDF ingest check:

- `tests/fixtures/pdf/vector-smoke.pdf` with `pdf_input_mode=vector`
- `tests/fixtures/pdf/raster-smoke.pdf` with `pdf_input_mode=raster`

Expected outcome for both smoke fixtures: validation remains `needs_review` and downstream quantity trust remains `review_gated`. Treat them as review-first readiness checks, not trusted quantity baselines.

## Review checklist

Record only safe, non-sensitive notes.

For a reusable comparison template, use [Local DWG/PDF extraction comparison checklist](./local-dwg-pdf-extraction-comparison-checklist.md) after you inspect the validation report, entities, and any local debug overlay.

### 1. Units and scale

- Did the extracted units match the drawing expectation?
- For PDF inputs, is the scale assumption or calibration still required?
- If units or scale are unknown, treat downstream quantities as untrusted.

### 2. Geometry and entity counts

- Do top-level geometry/entity counts look plausible for the sample?
- Are important entity classes present or missing?
- Are large unknown buckets explained by the current adapter limits?

### 3. Overlay comparison

- Generate or inspect the local debug overlay artifact.
- Compare the overlay against the source drawing visually.
- Check for missing linework, misplaced entities, bad extents, or unreadable clustering.
- Use overlay labels/confidence/review cues to spot extraction drift quickly.

### 4. Labels, text, and layers

- Are important labels or text blocks present and readable?
- Are containment, equipment, fixture, or room labels present where the source depends on them?
- Are layers/layouts attached where expected?
- Are entities landing on obviously wrong layers or missing layer names entirely?

### 5. Validation status

Interpret the validation report using the API contract values:

- `valid`: downstream quantity/export flows are eligible.
- `valid_with_warnings`: eligible, but review the warnings before trusting the result.
- `needs_review`: human review is still required.
- `invalid`: treat the revision as blocked for downstream trusted use.

### 6. Quantity gate interpretation

If you run quantities, check the returned gate/trust fields before using totals:

- `allowed`: acceptable for normal downstream use.
- `allowed_provisional`: usable only as provisional output; do not treat as fully trusted.
- `review_gated`: review-first; do not treat totals as trusted until reviewed.
- `blocked`: do not use the quantity output.

If units are missing, PDF calibration is still unresolved, or core geometry is materially wrong, keep the takeoff blocked or untrusted even if other checks look acceptable.

DWG, vector PDF, and raster PDF inputs may remain review-gated or blocked depending on extraction confidence and validation outcomes.

## After the review

- Keep the source sample local-only.
- Delete any local generated artifacts you do not need.
- Do not commit project IDs, job IDs, artifact IDs, screenshots, or sample-derived metadata if they could reveal proprietary details.
- If you need to share findings, summarize behavior in neutral terms and avoid naming the sample or client.

## Related docs

- [README](../README.md)
- [tests/fixtures/README.md](../tests/fixtures/README.md)
- [Local DWG/PDF extraction comparison checklist](./local-dwg-pdf-extraction-comparison-checklist.md)
- [Architecture](./ARCHITECTURE.md)
- [Technical requirements](./TRD.md)
