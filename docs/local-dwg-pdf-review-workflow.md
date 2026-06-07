# Local DWG/PDF Review Workflow

Use this workflow when you need to re-run a proprietary DWG or vector PDF extraction review on your own machine without committing the source file, manifest entries, or generated artifacts. Raster PDF remains deferred for local review in the standard host and Compose workflows.

## Hard rules

- Keep proprietary samples local-only.
- Do not add proprietary samples to `tests/fixtures/manifest.yaml`.
- Do not commit generated artifacts, debug overlays, or local review notes that expose sensitive details.
- Use neutral placeholders in screenshots, shell history, notes, and issue updates.

If you need a committed fixture or a manifest entry for a proprietary sample, stop and get explicit clearance first.

## What this workflow is for

- Re-running local ingestion for proprietary DWG or vector PDF samples
- Inspecting validation and materialized entities
- Generating local-only debug overlays for visual QA
- Checking whether quantity results are allowed, provisional, review-gated, or blocked

## Before you start

1. Copy the sample into a local-only location outside git, or into an ignored scratch location you will not commit.
2. If you need local vector PDF support, enable PyMuPDF intentionally before any Compose rebuild/start rather than assuming the default Compose bundle includes it:
   - Host install: sync either the full `ingestion` extra or the narrower `pdf-vector` extra.
   - Compose rebuild/start: set both `DRAUPNIR_UV_EXTRAS` and the needed license probe values before `docker compose build` or `make up`.
   - Licensing: PyMuPDF is covered by the ADR-0007 AGPL/commercial caveat, so set the approval probe only after your deployment review.
3. If you are testing DWG extraction, make sure the required local license probe value is also present in your session.
4. If you need both DWG and vector PDF review in the same session, use a comma-separated `DRAUPNIR_APPROVED_LICENSE_PROBES` value so one probe does not overwrite the other:
   ```bash
   export DRAUPNIR_APPROVED_LICENSE_PROBES=pymupdf-deployment-review,libredwg-distribution-review
   ```
   If you only need one workflow, export only the relevant probe value.
5. Confirm your local stack is running:
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
6. If needed, raise the LibreDWG JSON output cap for larger local DWG review runs:
   ```bash
   export LIBREDWG_MAX_OUTPUT_MB=64
   ```

### Host and Compose enablement notes

- Host-side vector PDF review requires the PyMuPDF-backed adapter to be installed via `uv sync --locked --extra db --extra jobs --extra ingestion` or the narrower `uv sync --locked --extra db --extra jobs --extra pdf-vector` path.
- Default Compose stays DXF-only. To rebuild with vector PDF support, export the env vars before build/start:
  ```bash
  export DRAUPNIR_UV_EXTRAS="--extra db --extra jobs --extra dxf --extra pdf-vector"
  export DRAUPNIR_APPROVED_LICENSE_PROBES=pymupdf-deployment-review
  docker compose build api worker
  docker compose up -d
  ```
- If the same Compose session needs both vector PDF and DWG review, use:
  ```bash
  export DRAUPNIR_APPROVED_LICENSE_PROBES=pymupdf-deployment-review,libredwg-distribution-review
  ```
- Raster PDF is still deferred locally. Do not expect this workflow or the default Compose stack to enable raster extraction.

### If vector PDF is still unavailable

If a vector PDF upload fails with `ADAPTER_UNAVAILABLE`, distinguish the common local causes:

- Missing package/runtime: the worker or API image was built without `pdf-vector`/`ingestion`, so the vector adapter is not installed.
- Missing license approval: PyMuPDF is installed, but `DRAUPNIR_APPROVED_LICENSE_PROBES=pymupdf-deployment-review` was not set for the relevant process.

Use `GET /v1/system/health` plus the job failure/event diagnostics to tell those cases apart before retrying.

## Run the local review

1. Create a project:
   ```bash
   export BASE_URL=http://localhost:8000

   curl -sS -X POST "$BASE_URL/v1/projects" \
     -H 'Content-Type: application/json' \
     -d '{"name":"local-review"}'
   ```
2. Upload the local sample:
   ```bash
   export PROJECT_ID=<project-id>
   export LOCAL_SAMPLE=/path/to/local-sample.dwg

   curl -sS -X POST "$BASE_URL/v1/projects/$PROJECT_ID/files" \
     -H 'Idempotency-Key: local-review-upload-1' \
     -F "file=@$LOCAL_SAMPLE"
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

DWG and vector PDF inputs may remain review-gated or blocked depending on extraction confidence and validation outcomes. Raster PDF remains deferred for local review in this workflow.

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
