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
candidates and review items rather than fully trusted quantities. Raster-derived
outputs remain review-first and must not silently become trusted quantity
inputs.

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

Each changeset must declare the `base_revision_id` it was prepared against.
Apply-time execution must re-check the current revision for the drawing. If the
current revision no longer matches the declared base, the system must reject the
apply request instead of merging implicitly.

Applying an accepted changeset is append-only. The system creates a new drawing
revision with lineage back to the base revision and approved changeset; it never
overwrites the base revision in place.

Database ownership and mutation rules:

- API services create and update request-driven workflow records such as
  projects, file metadata rows, changesets, cancel requests, and soft-delete
  markers.
- Workers create and finalize execution-derived records such as drawing
  revisions, validation reports, quantity takeoffs, estimate versions, export
  artifacts, and `job_events`.
- Approved apply/reprocess paths append a new drawing revision row; they do not
  rewrite entity payloads or lineage on an existing revision.
- A revision may transition to `superseded` or another allowed review state for
  workflow purposes, but its stored canonical payload and provenance remain
  historical and immutable.

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

Stale-base apply attempts must return `REVISION_CONFLICT`. Minimum error details
must include the requested `base_revision_id`, the current revision id at apply
time, and the conflicting changeset or apply target when available.

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

Quantity generation must enforce ingestion review state and confidence policy,
not just copy confidence metadata through to the output.

Phase 6 quantity policy defaults:

- quantity math runs in normalized internal units with full stored precision
- tolerance is used for assertions, comparisons, and deterministic test
  expectations; it must not mutate stored quantity values
- rounding happens only at presentation/export boundaries and when freezing a
  finalized estimate snapshot, never during intermediate quantity computation
- duplicate source entities must be suppressed before aggregate math by stable
  canonical identity and source-hash checks
- every quantity item must carry provenance to the source entities that produced
  it plus its review-gated/provisional status

### Quantity Deduplication, Tolerance, And Rounding

The quantity engine must prevent duplicate counting when the same source entity,
block expansion, or adapter-emitted record appears more than once in normalized
input.

- Canonical entity identity must be stable per drawing revision.
- Deduplication must key on canonical entity identity plus source hash or
  equivalent immutable source fingerprint.
- If two normalized records resolve to the same identity/fingerprint pair, they
  are one quantity contributor, not two.
- Conflicting duplicates must be surfaced for review or validation rather than
  summed optimistically.

Type-specific quantity rules:

- counts: one countable canonical entity contributes at most once to the same
  aggregate scope
- lengths/perimeters: the same linear edge/path must not be re-summed through
  duplicate entity rows, repeated block expansion, or overlapping alias records
- areas: area is measured only from eligible closed geometry; the same closed
  shape must not contribute twice through duplicate outlines or duplicate block
  references
- volumes/mass: volume-bearing solids/profiles contribute once per canonical
  source entity; mass derives from the accepted volume and material data and
  inherits the same dedup rule

Numeric policy defaults:

- internal computation uses normalized units and full precision
- stored quantity values preserve computed precision subject to database/storage
  capabilities; tolerance is not a storage-rounding mechanism
- one project-wide tolerance policy must exist so tests can encode the same
  comparison behavior for unit conversions, aggregate checks, and fixture
  assertions
- rounding policy must be explicit per output format or estimate snapshot field

Minimum quantity provenance fields:

- quantity_type
- value
- unit
- source_entity_refs
- excluded_entity_refs when validation/review policy removed contributors
- review_state
- validation_status
- quantity_gate
- provisional boolean or equivalent explicit status marker

### Canonical Validation Report v0.1

Between ingestion and quantity generation, every drawing revision must expose a
canonical validation report using `validation_report_schema_version = "0.1"`.

Validation status is separate from review state and job status. Validation
answers whether the normalized revision is technically usable; review state
answers whether the revision is trusted for downstream quantity behavior; job
status answers whether the ingestion/validation process has completed, failed,
or been cancelled.

Validation statuses:

- `valid` - no blocking findings and no warnings that require operator review
- `valid_with_warnings` - technically usable, but warnings must be recorded
- `needs_review` - technically usable only with explicit operator review or
  provisional handling
- `invalid` - not eligible for quantity generation until corrected or replaced

Derived quantity gate values:

- `allowed` - quantity generation may run normally
- `allowed_provisional` - quantity generation may run, but outputs must remain
  provisional
- `review_gated` - quantity generation is blocked pending review
- `blocked` - quantity generation is not allowed

Minimum validation report fields:

- `validation_report_schema_version` - `0.1`
- `drawing_revision_id`
- `source_job_id`
- `canonical_entity_schema_version`
- `validation_status`
- `review_state`
- `quantity_gate`
- `effective_confidence`
- `validator_name`
- `validator_version`
- `summary` - counts of checks/findings by severity/status
- `checks` - per-check results
- `findings` - normalized finding list with linkage back to source context
- `adapter_warnings` - adapter-emitted warnings carried forward into the report
- `generated_at`

Each check record must include:

- `check_key`
- `status` - `pass`, `warning`, `review_required`, or `fail`
- `summary_message`
- `finding_refs`
- `details` - structured, check-specific metadata

Required MVP checks:

- units presence/normalization
- coordinate system capture
- geometry validity
- closed polygon eligibility for area quantities
- block transform validity
- layer mapping completeness
- xref resolution status
- PDF scale presence/calibration status where applicable
- IFC schema support

Each finding must include normalized target and impact fields:

- `target_type` - one of `entity`, `layer`, `layout`, `page`, `block`, `xref`,
  `ifc_product`, `adapter_warning`, or `revision`
- `target_ref` - id/ref for the addressed target in that scope
- `quantity_effect` - one of `none`, `warning_only`, `provisional_only`,
  `excluded_quantity`, `blocks_quantity`
- `source` - where the finding came from, such as `validator`,
  `adapter_warning`, or adapter-native diagnostics

`effective_confidence` is conservative for quantity-gate decisions. It may be
capped or downgraded by adapter limits, validation findings, raster scale
status, or quantity-relevant low-confidence entities, but validation must not
rewrite the historical stored confidence captured on the revision/entities.

Finding severities are `info`, `warning`, `error`, and `critical`. Findings may
also carry a remediation hint, machine-readable check key, and structured
context payload for UI/API consumers.

#### Validation Report Endpoint

`GET /v1/revisions/{revision_id}/validation-report`

Returns the canonical validation report for the addressed revision.

Minimum response shape:

```text
{
  drawing_revision_id,
  source_job_id,
  validation_report_schema_version: "0.1",
  canonical_entity_schema_version: "0.1",
  validation_status,
  review_state,
  quantity_gate,
  effective_confidence,
  validator_name,
  validator_version,
  summary: {
    checks_total,
    findings_total,
    warnings_total,
    errors_total,
    critical_total
  },
  checks: [
    {
      check_key,
      status,
      summary_message,
      finding_refs,
      details?
    }
  ],
  findings: [
    {
      finding_id,
      check_key,
      severity,
      message,
      target_type,
      target_ref,
      quantity_effect,
      source,
      details?
    }
  ],
  adapter_warnings: [...],
  generated_at
}
```

HTTP/status semantics:

- `200` - report exists for the revision
- `404` with `NOT_FOUND` - revision or report does not exist in scope

#### Quantity Behavior From Validation And Review State

Quantity eligibility is derived from both `validation_status` and `review_state`.
Technical validity must not replace the review-state taxonomy.

- `review_state = approved` + `validation_status = valid` or
  `valid_with_warnings` -> `quantity_gate = allowed`
- `review_state = provisional` + `validation_status = valid`,
  `valid_with_warnings`, or `needs_review` -> `quantity_gate = allowed_provisional`
- `review_state = review_required` + `validation_status != invalid` ->
  `quantity_gate = review_gated`
- `review_state = rejected` or `superseded` -> `quantity_gate = blocked`
- `validation_status = invalid` -> `quantity_gate = blocked` regardless of
  review state

Additional gate rules:

- Missing/unsupported IFC schema support is `invalid`.
- Missing PDF scale for scale-dependent PDF quantities is at least
  `needs_review`; if no safe manual calibration exists yet, treat as `invalid`
  for quantity generation.
- Geometry that prevents deterministic measurement is `invalid` for affected
  quantities and may block the full run when the scope cannot be isolated.
- Open polylines or unclosed polygons may still support length/count quantities,
  but they must not silently produce area quantities.
- Unresolved xrefs, incomplete layer mapping, or adapter warnings that preserve
  usable geometry may downgrade the report to `valid_with_warnings` or
  `needs_review` depending on measurement impact.
- Quantities that do run under `allowed_provisional` must carry explicit
  provisional/review-linked metadata down to API responses, exports, and frozen
  estimate snapshots.
- `review_gated` revisions may expose why quantities were blocked and which
  entities/findings caused the block, but they must not publish trusted quantity
  totals.

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

Estimate reproducibility rules:

- Every formula used for estimation must have a stable identifier and version.
- Formula evaluation must be restricted to a deterministic expression language or
  equivalent allowlisted evaluator. Raw Python `eval`/`exec` or equivalent
  unrestricted runtime code execution is explicitly forbidden.
- The evaluator must operate only on declared inputs, fixed operators,
  deterministic functions, and explicit unit/rounding rules.
- Re-running the same estimate version against the same frozen inputs must
  produce the same estimate output.

Estimate snapshots:

- Finalized estimate versions must persist a frozen snapshot of the exact rates,
  materials, formulas, assumptions, and quantity inputs used to produce the
  estimate.
- The snapshot must also preserve the catalog item identifiers/versions or other
  lineage needed to trace each frozen value back to its source.
- Line items must reference the frozen snapshot entries they used, not only live
  catalog rows.
- Finalized estimate versions are append-only records created by estimate
  workers; recalculation creates a new version rather than mutating a finalized
  one in place.

Catalog scope and mutation rules:

- Catalog data may exist at a global scope and at a project override scope.
- Project overrides take precedence over global defaults only at estimate
  creation or recalculation time.
- Once an estimate version is finalized, its frozen snapshot becomes the source
  of truth for reproducibility.
- Later changes to global catalogs or project overrides must not change prior
  estimates, their line items, or any regenerated estimate artifact derived from
  that estimate version.

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

Database ownership and append-only policy:

- Original file rows are API-created metadata for immutable uploads; workers may
  read them, but never replace the original object or rewrite the row as a new
  source.
- Drawing revisions, approved quantity takeoffs, finalized estimate versions,
  export artifacts, and `job_events` are append-only historical records.
- Mutable workflow/state records include job status, attempt counters, progress,
  `cancel_requested`, `deleted_at`, and allowed review/supersession state
  transitions.
- Mutable state must not be used to rewrite the payload, lineage, checksum, or
  source references of an already committed append-only record.

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
- `REVISION_CONFLICT` - changeset base revision is stale at apply time
- `JOB_CANCELLED` - cancelled by user before completion
- `INTERNAL_ERROR` - unhandled exception or internal publish/worker failure

New codes require a docs change in this file.

## Worker And Job Behavior

- Default ingestion adapter timeout: 5 minutes. Override per-adapter via config.
- Default max attempts: 3. Backoff is exponential with jitter.
- Cancel is cooperative. Workers must check `cancel_requested` before starting
  work, between adapter steps, on every persisted progress event, and again
  before any terminal success commit.
- Retries must be safe and idempotent. A rerun re-derives output from the
  immutable source inputs and current attempt context; prior partial attempt
  output is never treated as committed input for the retry path.
- Workers may stage attempt-local output while a job is running, but staged
  output is not visible as the job's final result and must not be published as a
  committed artifact set.
- A job has at most one committed final output set. Promotion of staged output,
  creation of any final artifact rows/objects, and transition to a terminal
  `succeeded` state must happen atomically under the job lock so clients never
  observe committed outputs with a non-terminal job or a terminal success with
  missing outputs.
- Finalization ownership is worker-only: the worker that wins the job lock is
  the only writer allowed to convert staged attempt data into committed
  revision/takeoff/estimate/artifact rows.
- Retry and duplicate-delivery handling must fence stale workers and prevent
  duplicate finalization. Redelivered messages, overlapping attempts, or stale
  completions must not create duplicate committed outputs or move the job from a
  newer terminal state back to an older one.
- Retry is only for failed work. A retry attempt may replace staged attempt
  data, but it does not overwrite or mutate already committed artifacts from a
  different completed job path.
- Attempt-local staging is mutable until commit, but staged rows/objects are not
  authoritative product records. Only the atomic finalization step may publish
  new append-only records, and cancellation before that step must leave no new
  committed output rows visible.
- Cancellation before finalization produces `cancelled` with no new committed
  output set. A cancel request after a terminal state is a no-op.
- `job_events` must capture the operational state machine, including enqueue,
  start, progress checkpoints, cancel requested, cancel observed, retry
  scheduled, terminal failure, and terminal success so clients can reconstruct
  what happened per attempt.
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
- assign a revision review state from the confidence/review policy before
  downstream quantity work can auto-proceed

### Adapter Capability Registry

The system must maintain a capability registry for every configured ingestion
adapter so the API can report what is installed, usable, and allowed.

Minimum registry fields per adapter:

- `adapter_key` - stable identifier such as `dwg.libredwg` or `pdf.vector.pymupdf`
- `input_family` - one of `dwg`, `dxf`, `ifc`, `pdf_vector`, `pdf_raster`
- `adapter_name`
- `adapter_version`
- `input_formats` - concrete source formats the adapter can accept
- `output_formats` - concrete normalized or export formats the adapter can emit
- `status` - one of `available`, `unavailable`, `degraded`
- `availability_reason` - nullable enum: `missing_binary`, `missing_license`,
  `probe_failed`, `disabled_by_config`, `unsupported_platform`
- `license_state` - one of `present`, `missing`, `not_required`, `unknown`
- `license_name` - nullable short identifier for the runtime dependency license
- `can_read`
- `can_write`
- `extracts_geometry`
- `extracts_text`
- `extracts_layers`
- `extracts_blocks`
- `extracts_materials`
- `supports_exports`
- `supports_quantity_hints`
- `supports_layout_selection`
- `supports_xref_resolution`
- `experimental` - true for review-first or otherwise non-default capability
- `confidence_range` - nullable `{ min, max }` range for expected extraction confidence
- `bounded_probe_ms` - max probe budget for startup/runtime capability checks
- `last_checked_at`
- `details` - optional structured diagnostic metadata safe for operators

Rules:

- Missing required adapter binaries or required runtime license material map to
  `ADAPTER_UNAVAILABLE`.
- Timeout during adapter invocation or bounded capability/health probing maps to
  `ADAPTER_TIMEOUT`.
- Capability checks must be bounded, side-effect free, and must not parse full
  customer files.
- `degraded` means the adapter is callable but some optional capability is not
  working; it must not be used to hide a missing binary or missing license.

### System Capability And Health Endpoints

`/v1/health` remains a shallow liveness endpoint with the existing exact shape:

```json
{ "status": "ok", "version": "..." }
```

Dependency-specific detail belongs under `/v1/system/*`.

#### `GET /v1/system/capabilities`

Returns the current adapter capability registry.

Response shape:

```text
{
  adapters: [
    {
      adapter_key,
      input_family,
      adapter_name,
      adapter_version,
      input_formats,
      output_formats,
      status,
      availability_reason?,
      license_state,
      license_name?,
      can_read,
      can_write,
      extracts_geometry,
      extracts_text,
      extracts_layers,
      extracts_blocks,
      extracts_materials,
      supports_exports,
      supports_quantity_hints,
      supports_layout_selection,
      supports_xref_resolution,
      experimental,
      confidence_range?,
      bounded_probe_ms,
      last_checked_at?,
      details?
    }
  ]
}
```

HTTP/status semantics:

- `200` - endpoint itself succeeded, even if some adapters are unavailable or
  degraded
- `500` with `INTERNAL_ERROR` - registry cannot be produced at all

#### `GET /v1/system/health`

Returns bounded dependency and adapter health for operators and automation.

Minimum response shape:

```text
{
  status,
  checks: {
    database: { status, latency_ms? },
    storage: { status, latency_ms? },
    broker: { status, latency_ms? },
    adapters: [
      { adapter_key, status, error_code?, latency_ms?, details? }
    ]
  }
}
```

Status semantics:

- top-level `status` is one of `ok`, `degraded`, `down`
- dependency/check `status` is one of `ok`, `degraded`, `down`, `unknown`
- adapter `error_code` may be `ADAPTER_UNAVAILABLE`, `ADAPTER_TIMEOUT`, or
  `ADAPTER_FAILED`

HTTP semantics:

- `200` - overall system status `ok`
- `503` - overall system status `degraded` or `down`
- `500` with `INTERNAL_ERROR` - health response itself cannot be assembled

Probe rules:

- Health probes must be bounded by small per-check timeouts and must fail closed
  rather than hanging the request.
- Adapter health probes must verify callable readiness only; they must not run
  full ingestion against production-sized inputs.
- Expensive checks may use cached probe results when the cache age is explicitly
  bounded and reported.

## Coordinate Systems, Units, Encoding

- Ingestion settings are captured as an immutable `extraction_profile` object so
  jobs and revisions reference a stable extraction contract instead of ad hoc
  request flags.
- `extraction_profile` v0.1 must include at minimum:
  - `id` - server-generated extraction profile identifier.
  - `version` - profile schema version, initially `v0.1`.
  - `units_override` - optional explicit source-unit override when adapter
    detection is missing or known to be wrong.
  - `layout_mode` - layout selection policy, with MVP support for `modelspace`
    by default and explicit alternate layout selection later.
  - `xref_handling` - how external references and nested blocks are handled,
    including whether unresolved xrefs are skipped with warnings or hard-fail.
  - `block_handling` - whether block references stay instanced in provenance,
    are expanded for canonical extraction, or both.
  - `text_extraction` - whether text entities are extracted and normalized.
  - `dimension_extraction` - whether dimensions are extracted as semantic
    dimensions, raw geometry, or both where supported.
  - `pdf_page_range` - optional inclusive page selection for PDF inputs.
  - `raster_calibration` - required scale calibration input for raster-derived
    geometry; raster extraction without calibration remains review-only.
  - `confidence_threshold` - optional minimum entity/revision confidence the job
    should accept before marking output review-gated.
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

- Ingestion and downstream jobs must reference both `file_id` and
  `extraction_profile_id`. The file identifies the immutable source upload; the
  extraction profile identifies the normalization/extraction settings used to
  derive a revision.
- Reprocessing is not an in-place rerun. Reprocessing creates a new drawing
  revision from an existing `file_id`, records the `extraction_profile_id` used
  for the run, and records the adapter name/version that produced the result.
- The prior revision remains available for lineage, comparison, and audit even
  when the newer reprocessed revision supersedes it.
- API direction for future `POST /v1/files/{file_id}/reprocess`:
  - request body should accept either an existing `extraction_profile_id` or a
    profile payload to persist as a new extraction profile before job creation
  - the server creates a new ingestion/reprocessing job bound to `file_id` and
    the resolved `extraction_profile_id`
  - successful completion creates a new drawing revision rather than mutating
    the previous revision in place
  - the resulting revision must expose adapter version, extraction profile, and
    predecessor/superseded lineage metadata
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
- Review state is behavioral, not descriptive metadata only. Every ingested
  revision must be classified as exactly one of:
  - `approved` - trusted for downstream quantity generation without additional
    review gate.
  - `provisional` - quantity generation may run, but outputs remain explicitly
    provisional and must not be used as silent source-of-truth inputs for
    estimate/export flows that require approved quantities.
  - `review_required` - blocked from trusted quantity generation until human
    review explicitly approves, rejects, or replaces it.
  - `rejected` - human-reviewed and not eligible for quantity generation.
  - `superseded` - replaced by a newer revision and not eligible for quantity
    generation.
- Default confidence-to-state thresholds for ingestion outputs:
  - `>= 0.95` -> `approved`
  - `0.60` to `< 0.95` -> `provisional`
  - `< 0.60` -> `review_required`
- Raster PDF is a hard exception to the generic threshold rule: raster-derived
  revisions and raster-derived entities remain `review_required` until human
  review, even when individual heuristics score above the normal provisional
  band. Raster confidence may guide prioritization, but it cannot auto-approve
  trusted quantity input.
- Human review may promote a revision from `review_required` or `provisional` to
  `approved`, keep it `provisional`, mark it `rejected`, or leave it
  `review_required` pending more information. A revision replaced by a later
  revision may be marked `superseded`.
- Quantity jobs must enforce the review state:
  - `approved` ingestion output may generate normal quantities.
  - `provisional` ingestion output may generate provisional quantities only, and
    those quantities must be labeled and stored as provisional.
  - `review_required`, `rejected`, and `superseded` ingestion output must not
    generate quantities; the system returns a review-gated or ineligible
    outcome instead of silently proceeding.
- Validation/reporting between ingestion and quantity generation must include:
  - inherited review state and effective confidence at revision level
  - whether any source entities were excluded or downgraded due to review policy
  - raster scale-calibration status when raster-derived geometry is present
  - warnings explaining why quantity output is blocked, provisional, or fully
    approved
- Quantity dedup rules must be documented per quantity type so the same entity
  is not counted twice across length/area/volume aggregates.
- Review-gated quantity behavior defaults:
  - `allowed` revisions may publish normal quantity totals.
  - `allowed_provisional` revisions may publish quantities only with explicit
    provisional labeling and provenance.
  - `review_gated` revisions may publish blockage metadata, but not trusted
    quantity totals.
  - entity-level exclusions caused by review/validation findings must remain
    traceable in provenance rather than silently disappearing.

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
- Risky parsers and external adapter binaries must run in mandatory isolation
  boundaries such as dedicated subprocesses or containers, not in-process with
  the API worker.
- Isolation boundaries must enforce time budgets/timeouts, memory, file
  descriptor/file-access limits, per-job tempdir ownership and cleanup, and
  network disabled by default with explicit allowlisting only when required;
  adapter stdout/stderr and exit status remain untrusted inputs.
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
