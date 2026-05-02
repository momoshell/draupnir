# Technical Requirements Document

## System Overview

Draupnir is a backend service for CAD/BIM ingestion, quantity extraction,
estimation, and revision generation. It exposes a stable API for multiple future
clients, including web apps, TUIs, CLIs, and other backend services.

## Architecture Principles

- Backend first.
- Deterministic computation before AI.
- Original files are immutable.
- All derived revisions are versioned.
- DWG/PDF tooling is adapter-based.
- CAD edits are expressed as changesets before export.
- The API contract matters more than the implementation language.

## MVP Tech Stack

```text
Language:       Python 3.12
API:            FastAPI
Schemas:        Pydantic v2
Database:       PostgreSQL 18
DB access:      SQLAlchemy 2.x
Migrations:     Alembic
Jobs:           Celery
Broker:         RabbitMQ
Storage:        local filesystem first, S3-compatible later
DXF:            ezdxf
IFC:            IfcOpenShell
Reports:        JSON/CSV first, PDF report after estimate model stabilizes
AI gateway:     LiteLLM later
Agent layer:    Pydantic AI later, optional
```

## Input Handling

### DWG

DWG is a required starting input, but it must be handled through an adapter.
The accepted first DWG adapter is LibreDWG behind the adapter contract. The
adapter should normalize DWG into an internal representation and, where
possible, DXF-compatible geometry.

### Vector PDF

Vector PDF is a required starting input. The system should attempt to extract
vector geometry and normalize it into drawing entities. The accepted first
vector PDF adapter is PyMuPDF behind the adapter contract. Semantic loss is
expected and must be represented through confidence/provenance fields.

### Raster PDF

Raster PDF is important for the product, but it is higher-risk than DWG/vector
PDF. It is implemented behind an experimental adapter strategy using VTracer,
centerline extraction, and Tesseract. Initial support may produce trace
candidates and review items rather than fully trusted quantities.

### DXF

DXF is a first-class normalized/open CAD input and the initial editable export
format.

### IFC

IFC is a first-class BIM input. Use IfcOpenShell for products, properties,
quantities, relationships, and geometry where available.

## Canonical Representation

The normalized representation is versioned. MVP adapters and persistence target
`canonical_entity_schema_version = "0.1"`.

Every drawing revision produced by ingestion or revision export must record the
canonical entity schema version used for its stored entities.

The normalized representation must capture:

- project
- source file
- drawing revision
- canonical entity schema version
- layouts
- layers
- blocks
- entities
- geometry summaries
- properties and attributes
- units
- quantities
- provenance
- confidence

Minimum top-level canonical revision payload requirements:

- `canonical_entity_schema_version` - revision-level canonical contract version
- `layouts` - list of extracted layout descriptors (includes modelspace entry
  when applicable)
- `layers` - list of normalized layer descriptors
- `blocks` - list of normalized block definitions/instances metadata
- `entities` - list of canonical entities linked to layout/layer/block context

`layouts`, `layers`, `blocks`, and `entities` are required keys and may be
empty lists when extraction finds none.

### Canonical Entity Schema v0.1

Canonical entity schema v0.1 is a JSON-first strict contract for normalized
drawing data now, with room for future relational persistence without changing
the meaning of stored entities.

Each entity record must include:

- `entity_id` - stable revision-scoped entity id
- `entity_type` - canonical entity type
- `entity_schema_version` - entity payload version, `0.1` for MVP
- `geometry` - normalized geometry payload for the entity type
- `properties` - normalized semantic properties and adapter-carried attributes
- `provenance` - structured origin and extraction metadata
- `confidence` - float in `[0, 1]`

Version-field rule: `canonical_entity_schema_version` is revision-scoped and
declares the canonical contract for the full revision payload. The
`entity_schema_version` field is per-entity for forward compatibility; in MVP
it must equal the revision-level `canonical_entity_schema_version`.

Required linkage fields:

- `drawing_revision_id`
- `source_file_id`
- `layout_ref` - modelspace or named layout/paper space source
- `layer_ref`
- `block_ref` when the entity comes from a block definition or insert context
- `parent_entity_ref` when the entity is nested, exploded from a parent, or
  otherwise derived from another entity in the same revision

Canonical v0.1 entity types:

- `line`
- `polyline`
- `arc`
- `circle`
- `ellipse`
- `spline`
- `point`
- `hatch`
- `text`
- `mtext`
- `dimension`
- `insert`
- `face`
- `solid`
- `mesh`
- `ifc_product`
- `image_reference`
- `viewport`
- `unknown`

Required geometry fields are entity-type specific, but every entity must carry
enough normalized geometry for deterministic quantity computation plus:

- `bbox`
- `transform` when block, layout, or source transforms apply
- `units`
- `geometry_summary` for fast indexing/query use

Required properties fields:

- `source_type`
- `source_handle` or equivalent adapter-native identifier when available
- `style_ref` when applicable
- `name` or `tag` when applicable
- `material_ref` when applicable
- `quantity_hints` when the adapter can safely provide them
- adapter-native attributes preserved under a namespaced payload

Block/layout/parent linkage must preserve source structure. The system must be
able to distinguish a direct entity from a block instance, block definition
member, exploded child, or nested derived entity. Block explosion and dedup
logic must not lose source identity.

Entity identity must preserve source-native identity when available and also
record a normalized source hash or equivalent fingerprint so dedup/re-ingestion
logic can compare semantically identical entities across adapter passes.

For raster-derived entities, the canonical payload must also record page-scoped
or view-scoped scale calibration metadata when used, because quantities are not
trustworthy without explicit scale context.

## Revision Model

Agents and users do not directly mutate CAD files. They create proposed
changesets.

Example operation types:

- annotate entity
- change layer
- add entity
- remove entity
- replace block
- replace profile/material candidate
- update property
- flag for review

Changesets must be validated before export.

## Quantity Engine

The quantity engine must compute deterministic quantities from normalized data:

- counts
- lengths
- areas
- volumes
- mass/weight when material data permits
- perimeters
- bounding boxes
- entity/layer/block summaries

Every computed quantity must include source entity references and units.

## Estimation Engine

The estimation engine must be deterministic and auditable.

Required data:

- rate catalog
- material catalog
- formulas
- assumptions
- quantity references
- estimate line items
- estimate versions

## API Requirements

- API must be UI-agnostic.
- Long-running work must run as jobs.
- Job status must be persisted.
- Job progress should be observable through polling first, events later.
- API-key auth should be easy to add later.

## Non-Functional Requirements

- Local development must work through Docker Compose.
- Upload size must be configurable.
- Ingestion adapters must have timeouts.
- Failures must be stored in job records.
- Generated artifacts must be reproducible from stored revisions and their
  recorded lineage inputs.

## Generated Artifact Contract

Generated artifacts are immutable, append-only outputs. The system must never
overwrite an existing generated artifact row/object in place, even when
regenerating the same logical export.

Minimum artifact lineage fields:

- `source_file_id`
- `drawing_revision_id`
- `changeset_id` when produced from a revision export
- `quantity_takeoff_id` when produced from quantity output
- `estimate_id` when produced from estimate output
- `job_id`
- `generator_name`
- `generator_version`
- `generator_config_snapshot` or equivalent options snapshot
- `checksum`

Rules:

- Regeneration or re-export creates a new artifact id, row, and storage object.
- Artifacts may reference a prior artifact for lineage, supersession, or UI
  history, but the prior artifact remains immutable.
- Artifact storage keys are server-derived and unique per artifact record.
- Reproducibility means the system can trace which stored source revision,
  changeset/takeoff/estimate context, job, and generator configuration produced
  the artifact.
- Original uploads and generated artifacts follow the same immutability rule but
  remain distinct storage classes and records.

## MVP Retention And Deletion Policy

For MVP, retention is conservative:

- keep original uploads and generated artifacts by default
- prefer soft-delete or hidden metadata state before physical deletion
- require manual/administrative action for physical artifact deletion
- do not auto-delete superseded artifacts in MVP

Any physical deletion flow must preserve at least the metadata needed to explain
what was produced and must not imply that a replacement artifact overwrote the
prior one.

## API Conventions

- All MVP routes are mounted under `/v1/`. Breaking changes require a new prefix.
- Errors use a single envelope: `{ "error": { "code": str, "message": str, "details": object? } }`.
- List endpoints support cursor pagination via `?cursor=` and `?limit=` and return
  `{ items, next_cursor }`. Default limit 50, max 200.
- Timestamps are ISO-8601 UTC.
- Ids are opaque ULIDs or UUIDs; clients must not parse them.
- Idempotent mutating endpoints accept `Idempotency-Key` headers and return the
  prior response on replay within a documented retention window.

## Error Taxonomy

`jobs.error_code` and the error envelope share an enumerated taxonomy. Initial
codes:

- `NOT_FOUND` - requested resource does not exist in the addressed scope
- `INVALID_CURSOR` - cursor token is malformed or cannot be decoded
- `VALIDATION_ERROR` - request body/path/query validation failed
- `INPUT_INVALID` - otherwise invalid client-supplied input
- `INPUT_UNSUPPORTED_FORMAT` - format detected but not handled
- `ADAPTER_UNAVAILABLE` - adapter binary or license missing
- `ADAPTER_TIMEOUT` - adapter exceeded configured timeout
- `ADAPTER_FAILED` - adapter returned non-zero or malformed output
- `STORAGE_FAILED` - read/write/link/checksum failure
- `DB_CONFLICT` - optimistic concurrency or unique violation
- `JOB_CANCELLED` - cancelled by user before completion
- `INTERNAL_ERROR` - unhandled exception or internal publish/worker failure

New codes require a docs change in this file.

## Worker And Job Behavior

- Default ingestion adapter timeout: 5 minutes. Override per-adapter via config.
- Default max attempts: 3. Backoff is exponential with jitter.
- Cancel is cooperative. Workers must check `cancel_requested` between adapter
  steps and on every persisted progress event.
- Retries must be safe: workers re-derive output from the source file and the
  `attempts` counter, not from prior partial state.
- Job pipelines may auto-chain (ingestion -> quantity takeoff) when configured;
  default for MVP is manual chaining via the API.
- All long-running steps must emit `job_events` rows for progress and errors.

## Adapter Contract

Every ingestion adapter must implement the same shape:

```text
adapt(source_file, options) ->
  AdapterResult {
    canonical: {
      canonical_entity_schema_version: "0.1",
      layouts,
      layers,
      blocks,
      entities,
      units,
      ...
    },
    provenance: ...,
    confidence: float in [0, 1],
    warnings: [...],
    diagnostics: { adapter_name, adapter_version, started_at, finished_at },
  }
```

Required behavior:

- honor a wall-clock timeout
- respond to a cancel signal between phases
- emit progress callbacks the worker can persist as `job_events`
- never write to the original file
- surface unsupported features as warnings, not silent drops

## Coordinate Systems, Units, Encoding

- Adapters must record source units, normalized units, and conversion factor.
- Adapters must surface paperspace vs modelspace and which layouts were
  extracted. The MVP extracts modelspace by default; layout selection is
  configurable.
- External references (xrefs) must be reported with their resolution status. If
  an xref cannot be resolved, the adapter records a warning and proceeds.
- DXF/DWG text encoding must be detected per file. If detection fails, the
  adapter records the raw bytes in provenance and uses a configured fallback.
- IFC schema version (2x3, 4, 4.3) must be recorded. Unsupported versions fail
  with `INPUT_UNSUPPORTED_FORMAT`.

## Provenance And Confidence

- `provenance_json` is a structured object, not free text. Required keys:
  `origin`, `adapter`, `source_ref`, `source_identity`, `source_hash`,
  `extraction_path`, `notes`.
- `origin` must be one of exactly: `source_direct`, `adapter_normalized`,
  `inferred`, `user_created`, `agent_proposed`, `generated_export`.
- Origin meanings:
  - `source_direct` - direct source-native entity mapped without semantic
    inference.
  - `adapter_normalized` - adapter-transformed/normalized representation of
    source data.
  - `inferred` - derived or imputed from source context, not explicitly present
    in source data.
  - `user_created` - explicitly created by a user action in a revision
    changeset.
  - `agent_proposed` - created or modified by an automated assistant proposal,
    pending/subject to review workflow.
  - `generated_export` - synthesized during export generation from approved
    revision state.
- `source_identity` records the adapter-native source identifier when available
  (for example handle, GUID, object id, page/object tuple, or IFC global id).
- `source_hash` records a stable normalized fingerprint when the adapter can
  compute one.
- `confidence` is a float in [0, 1] with documented reference points: 1.0 for
  direct DXF/IFC reads, 0.7-0.9 for vector PDF, 0.3-0.6 for raster PDF before
  human review.
- Entities and revisions must persist confidence alongside provenance so review
  workflows can distinguish trusted geometry from review-first extraction.
- Quantity dedup rules must be documented per quantity type so the same entity
  is not counted twice across length/area/volume aggregates.

## Observability

- All services emit structured JSON logs with at minimum: `ts`, `level`,
  `service`, `request_id`, `job_id?`, `event`, `msg`.
- API requests carry an `X-Request-Id` header; if absent the API generates one.
  Workers propagate it via job records.
- Errors are logged with stack traces and the active error code.
- Metrics surface (Prometheus-compatible) is planned post-MVP; the logging
  contract above must already be in place.

## Testing Strategy

- Unit tests for pure functions (quantity math, unit conversion, formula eval).
- Integration tests for API + Postgres using a real database in Docker.
- Adapter contract tests: every adapter passes the same canonical shape suite
  against committed fixtures.
- Fixture manifest in `tests/fixtures/manifest.yaml` records source, license,
  expected outputs, and allowed-in-git status.
- No mocking of Postgres in integration tests. Use disposable schemas.
- A smoke test must exercise the upload -> ingest -> query path end to end on
  every CI run.

## Security And Upload Hardening

- Upload size enforced server-side via `MAX_UPLOAD_MB` and streaming chunk
  validation.
- Content-type detection by magic bytes, not by filename or client header.
- Reject unsupported formats early with `INPUT_UNSUPPORTED_FORMAT`.
- DXF, IFC, and PDF parsers run with strict resource limits (file handles,
  memory) to mitigate decompression bombs and pathological inputs.
- Original storage paths are derived from server-controlled IDs, never from the
  client filename, to prevent path traversal.
- Filenames are stored only as metadata; they are never used as keys.
- API-key auth is deferred for MVP, but every route is written assuming it will
  be added; no route relies on the absence of identity.

## Open Technical Questions

- Whether PDF generation uses WeasyPrint or ReportLab.
- Whether geometry validation starts with FreeCAD or direct OCCT bindings.
- Webhook/SSE event delivery vs polling-only for MVP.
- Whether materials live in a shared catalog or per-project.
