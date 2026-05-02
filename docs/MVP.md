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
  review may be required.

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

## Estimation

- Default currency: GBP.
- Rates must be stored in a configurable catalog.
- Store rate source, unit, effective date, currency, and whether the rate is
  manual or imported.
- Estimate math must be deterministic.

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
