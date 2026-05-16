# MVP Scope

## Goal

Build a local-first backend that accepts real-world drawing inputs, extracts
structured CAD/BIM data, computes quantities and estimates, and exposes results
through a stable API.

## Target Inputs

The MVP must start from the formats users are expected to provide:

- DWG
- vector PDF
- raster PDF

The system should also support these normalized/open formats as direct inputs:

- DXF
- IFC

DXF is especially important because it is the practical editable exchange format
for early revision/export work. IFC is important for BIM data when available.

## Input Risk Policy

DWG and PDF support must be adapter-based.

- DWG is proprietary. Reliable commercial read/write likely requires ODA,
  Autodesk RealDWG, QCAD Professional, or another licensed adapter.
- Vector PDF extraction is useful but can lose semantic CAD structure.
- Raster PDF extraction is important, but confidence will be lower and human
  review is required before outputs can become trusted quantity input.

MVP review/confidence workflow:

- ingestion classifies output as `approved`, `provisional`, `review_required`,
  `rejected`, or `superseded`
- default thresholds are `>= 0.95` approved, `0.60` to `< 0.95` provisional,
  and `< 0.60` review-required
- raster PDF is always review-first until human approval, even if heuristic
  confidence looks higher
- `rejected` and `superseded` are non-quantity states: they block quantity
  generation until a newer eligible revision exists
- provisional quantity output may exist, but it must stay explicitly
  provisional and must not silently flow into trusted estimation/export paths

The core system must not depend on one converter being permanently available.

## Outputs

The MVP should support:

- JSON extraction result
- CSV quantity/estimate export
- PDF estimate/report
- DXF revised drawing/export

Planned later outputs:

- DWG revised drawing/export through a licensed adapter
- IFC export/revision for BIM workflows
- STEP/IGES for 3D CAD workflows where relevant
- SVG/PNG preview exports

## Uploads

- Upload size must be configurable.
- Default local value: `200 MB`.
- Planned env var: `MAX_UPLOAD_MB`.
- Original uploads must be immutable.

## Samples And Fixtures

Existing DWG and PDF samples should be used for local development and testing.
Do not commit proprietary or client-provided samples unless they are explicitly
cleared as fixtures.

When possible, maintain a fixture manifest with:

- filename
- format
- source
- allowed git status
- known units
- expected extraction notes
- expected quantities, if known

## Units

- Support metric and imperial.
- UK testing default: metric.
- Store original units, normalized units, and conversion factor.
- Allow manual project/file unit override.

Default normalized units:

- length: m
- area: m2
- volume: m3
- mass: kg
- currency: GBP

Internal quantity math uses normalized units and full precision. Tolerance is a
comparison/assertion rule for tests and validation, not a stored-value mutation
rule. Rounding happens only when presenting/exporting values or freezing a
finalized estimate snapshot.

## Quantities

Extract and store all practical quantities available from the input:

- counts
- lengths
- areas
- volumes
- mass/weight where density/profile data is known
- perimeters
- bounding boxes
- layer counts
- block counts
- entity counts
- gross/net quantities where determinable

Every quantity must keep provenance back to source entities.

Default MVP quantity policy:

- suppress duplicate contributors by canonical entity identity plus source hash
  or equivalent immutable source fingerprint
- document one shared tolerance policy so tests and fixture assertions compare
  quantities consistently
- keep review state and validation status attached to quantity outputs
- allow provisional quantities only when the revision is quantity-eligible as
  provisional; `review_required`, `rejected`, and `superseded` revisions do not
  publish trusted quantity totals

## Estimation

- Default currency: GBP.
- No FX conversion in MVP; estimate inputs and outputs must already be in GBP.
- Trusted estimate input requires `trusted_totals = true` and
  `quantity_gate = allowed` on the referenced quantity takeoff.
- `allowed_provisional` quantities are blocked from trusted estimation by
  default; they remain explicit provisional data until a later contract says
  otherwise.
- Rates must be stored in a configurable catalog.
- Catalog scope is global by default with project-specific overrides.
- Auto-selection checks project-scoped matches first and falls back to global
  entries only when zero project-scoped entries match; multiple matches in the
  chosen scope fail.
- Store rate source, unit, effective date, currency, and whether the rate is
  manual or imported.
- `pricing_effective_date` is a date; when omitted it defaults from the job's
  UTC enqueue timestamp by taking the UTC calendar date.
- Effective-date intervals are half-open.
- Explicit catalog item id + checksum selection may bypass auto-match, but it
  must still validate scope, checksum, type, unit, currency, and lineage.
- Estimate math must be deterministic.
- Formula evaluation uses a restricted JSON AST Formula DSL v0 with required
  formula id/version, output contract, declared inputs, AST root, rounding,
  checksum, enumerated AST node categories, decimal string literals, and
  allowlisted operators only.
- Finalized estimates freeze quantity inputs, rates, materials, formulas,
  assumptions, and rounded money outputs for reproducibility.
- Monetary snapshot/output rounding defaults to GBP scale 2 with
  `ROUND_HALF_UP`.
- Non-money formula math keeps explicit higher precision until a field is frozen
  as money.
- Supersession is append-only: new estimate/catalog/formula versions replace old
  ones through new versions or immutable lineage, not in-place edits.

## Jobs

- Jobs must be persisted in Postgres.
- Jobs must be retryable.
- Jobs must be cancellable.
- Store attempts, status, timestamps, errors, and job events.

## Auth

No auth is required for local MVP.

The API should be designed so API-key auth can be added later without changing
core endpoint semantics.

## Storage

Store all extracted entities, not only summaries.

Original uploads and generated artifacts are immutable once written. If an
export is regenerated, it must produce a new artifact record/object rather than
overwrite an existing one.

Generated artifact records must keep lineage back to the producing source file,
drawing revision, changeset/takeoff/estimate context when applicable, job, and
generator metadata so the same output can be traced and reproduced later.

MVP retention/deletion is soft-delete/manual-first:

- keep original uploads and generated artifacts by default
- hide or soft-delete metadata before any physical object removal
- do not run automatic cleanup of superseded artifacts in MVP

Use this split:

- object/file storage for original files and generated artifacts
- Postgres for metadata, normalized entities, quantities, estimates, changesets,
  and artifact records
