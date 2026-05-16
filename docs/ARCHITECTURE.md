# Architecture

## High-Level Shape

```text
Future client UI / CLI / TUI
  -> FastAPI backend
      -> Postgres
      -> local/object storage
      -> Celery job queue
          -> ingestion workers
          -> quantity workers
          -> estimate workers
          -> export workers
```

## Main Components

MVP delivery is API-only. Client applications such as a web UI, CLI, or TUI are
downstream consumers of the backend contract and are not themselves MVP product
requirements in this repository.

### API Service

Owns HTTP endpoints, validation, job creation, metadata lookup, and artifact
download.

### Job Workers

Run long-lived operations:

- file ingestion
- DWG/PDF conversion
- DXF parsing
- IFC parsing
- quantity extraction
- estimate generation
- CAD export

### Storage Layer

Keeps original files and generated artifacts outside Postgres. Local filesystem
is enough for MVP, but the interface should allow S3-compatible storage later.

### Database Layer

Postgres is the source of truth for:

- projects
- files
- jobs
- drawing revisions
- extracted entities
- quantities
- estimates
- changesets
- artifacts

Write ownership is split by responsibility:

- API services write user/request workflow records such as projects, file
  metadata, changesets, cancel flags, and soft-delete markers.
- Workers write execution-derived records such as drawing revisions, validation
  outputs, quantity takeoffs, estimate versions, export artifacts, and
  `job_events`.
- Append-only records keep their payload and lineage immutable after commit;
  mutable records are limited to workflow/state transitions.

Database-level append-only enforcement protects the core lineage/history tables:

- `files`
- `extraction_profiles`
- `adapter_run_outputs`
- `drawing_revisions`
- `validation_reports`
- `revision_entity_manifests`
- `revision_layouts`
- `revision_layers`
- `revision_blocks`
- `revision_entities`
- `quantity_takeoffs`
- `quantity_items`
- `generated_artifacts`
- `job_events`

The implementation uses shared PostgreSQL PL/pgSQL trigger functions with
per-table row and truncate triggers. Row-update checks compare `to_jsonb(OLD)`
and `to_jsonb(NEW)` after removing any explicitly allowlisted mutable keys, so
the comparison is JSON-safe across nullable and structured columns.

The mutable allowlist is intentionally minimal:

- `files.deleted_at`
- `generated_artifacts.deleted_at`

Those fields are write-once and may transition only from `NULL` to non-`NULL`.
Protected tables reject `DELETE` and `TRUNCATE`, including cascaded truncate
attempts. Review-state or report-state changes on protected historical rows are
not allowed by this enforcement; any future workflow that needs them must ship
an explicit migration and allowlist update.

This trigger-based protection is an MVP integrity guardrail, not a full
privilege boundary. Postgres table owners and superusers can still disable
triggers, so stronger production role separation is a later hardening concern.

### Ingestion Adapters

Format-specific adapters turn source files into canonical drawing data.

```text
DWG adapter
PDF vector adapter
PDF raster adapter
DXF adapter
IFC adapter
```

Adapters must report confidence and provenance.

Ingestion also assigns a review state (`approved`, `provisional`,
`review_required`, `rejected`, or `superseded`) that downstream quantity work
must enforce. Confidence affects behavior, not just metadata.

Ingestion and normalization also produce a canonical validation report. This is
separate from review state and job status: validation status captures technical
usability (`valid`, `valid_with_warnings`, `needs_review`, `invalid`), review
state captures trust/approval policy, and job status captures execution state.
Quantity eligibility is exposed as a derived gate (`allowed`,
`allowed_provisional`, `review_gated`, `blocked`).

The backend also maintains an adapter capability registry that reports, per
adapter, installed availability, input/output format support, extraction and
export features, expected confidence ranges, and experimental/license status.
Operator-facing system detail lives under `/v1/system/capabilities` and
`/v1/system/health`; `/v1/health` remains a shallow liveness endpoint returning
only the current `{status, version}` shape.

Missing adapter binaries or required license material are reported as
`ADAPTER_UNAVAILABLE`. Bounded capability or health probes that exceed their
time budget are reported as `ADAPTER_TIMEOUT`.

Risky parsers and external adapter binaries are mandatory isolation candidates:
they must run in dedicated subprocesses or containers with bounded CPU/memory/
file descriptor/file-access controls, explicit time budgets/timeouts, per-job
tempdir ownership and cleanup, and network disabled by default unless an
allowlist is explicitly required, rather than in-process with the API. Adapter
stdout/stderr and exit status remain untrusted inputs.

### CAD Revision Engine

Applies approved changesets to normalized data and exports revised CAD artifacts.
Changesets are prepared against a `base_revision_id` and applied with an
optimistic current-revision check. If the addressed drawing has advanced since
that base, the apply request fails with `REVISION_CONFLICT` rather than
rewriting or auto-merging in place. Successful apply is append-only: it creates
a new drawing revision linked to the base revision and changeset. The first
editable export target is DXF. DWG export is a later adapter.

### Optional AI Layer

AI is not part of the MVP source-of-truth path. Later it may:

- classify ambiguous entities
- explain estimates
- propose changesets
- ask clarification questions
- summarize differences between revisions

AI must not directly write CAD files or compute final quantities/prices.

## Data Flow

```text
Upload
  -> file record
     (durable `initial_job_id` + `initial_extraction_profile_id` lineage)
  -> extraction profile selection/persistence
  -> ingestion job (file_id + extraction_profile_id)
  -> source adapter
  -> canonical entities + confidence/review state + validation report
  -> quantity extraction or review gate
  -> estimate generation
  -> exports
```

Extraction profiles are immutable configuration records that capture the
extraction contract for a run: unit overrides, layout mode, xref/block
handling, text/dimension extraction policy, PDF page range, raster calibration,
and any confidence threshold used to gate the output.

After ingestion finalization, workers also materialize immutable
revision-scoped query rows in `revision_entity_manifests`, `revision_layouts`,
`revision_layers`, `revision_blocks`, and `revision_entities`. These rows are a
derived query surface for `/v1/revisions/{revision_id}/layouts|layers|blocks|entities`:
they are built from adapter output and canonical revision payloads, scoped to a
single revision, and never rewrite the canonical JSON already stored on the
drawing revision.

Quantity workers must refuse to treat review-gated ingestion output as trusted
source-of-truth input. Provisional quantity runs are allowed only when the
upstream revision is marked provisional, and the resulting quantities must stay
explicitly provisional downstream.

Quantity workers evaluate both review state and validation status before
starting. A technically invalid revision is blocked even if its review state was
previously approved. Revisions that are technically usable but have warnings or
review-needed findings may still proceed only through the derived gate policy.

When quantity persistence is present, workers append a `quantity_takeoffs`
header row plus `quantity_items` detail rows for one pinned drawing revision.
Those lineage tables are append-only: a rerun persists a new takeoff/result set
instead of updating an earlier takeoff in place. Each persisted takeoff is tied
to one unique `source_job_id`, and that source job must remain constrained to
the same project, file, `base_revision_id`, and `quantity_takeoff` job type.

Revisions marked `review_required`, `rejected`, or `superseded` are not eligible
for quantity generation.

Persisted quantity items distinguish included contributors from rolled-up totals
and blocked inputs by kind: `contributor`, `aggregate`, `exclusion`, and
`conflict`. Conflict rows must stay FK-backed to `revision_entities`, may use
only `review_gated` or `blocked`, and inherit the parent takeoff gate through a
composite FK contract. Only takeoffs with `quantity_gate = allowed` should be
treated as trusted totals, so trusted takeoffs cannot persist conflict rows.

### Visual and Debug Overlay Artifacts

Adapters and downstream QA flows may emit visual/debug overlays as generated
artifacts when operators need to inspect extraction results against the source
drawing.

Overlay artifacts follow the normal artifact lifecycle and are never a mutable
sidecar attached in place to the source upload or drawing revision.

Minimum contract:

- artifact kind: `debug_overlay`
- artifact name: stable, operator-readable output such as
  `page-0001-entity-overlay`, `page-0001-quantity-overlay`, or
  `sheet-a101-review-overlay`
- MVP formats: SVG when the source/annotation pipeline stays vector-friendly,
  PNG for raster-first inspection output, and PDF overlay output when a
  page-faithful review artifact is needed for multi-page or print-oriented QA

Each overlay must preserve enough context for later fixture-based acceptance and
human review. Required visible content:

- source drawing/page or sheet reference being reviewed
- extracted entities drawn in registration with the source view when available
- entity ids or other stable labels that map back to stored extraction records
- confidence indicators or review cues for uncertain/flagged entities
- quantity regions, measurement spans, or takeoff highlights when quantities are
  part of the review target

Each overlay artifact record must also capture lineage back to:

- source file id
- drawing revision id
- job id
- adapter/generator name and version
- generator options snapshot, including page/sheet selection and any overlay
  rendering switches that affect review output

When applicable, the overlay lineage should also retain the specific extracted
entity ids, quantity/takeoff ids, or predecessor artifact id used to assemble
the rendered review view.

Storage and retention behavior are the same as all generated artifacts:

- overlays are written under the artifact storage prefix, not beside originals
- overlays are immutable once written and cannot be overwritten in place
- regeneration or rerendering creates a new artifact id and storage key
- soft deletion may hide an overlay from normal listings, but MVP retention
  keeps the stored object and lineage metadata for audit/debug use

For edits:

```text
Existing drawing revision
  -> user or agent proposed changeset, or file reprocess request
  -> apply-time current revision check for changesets
  -> validation / adapter execution
  -> new drawing revision
  -> DXF export
  -> later DWG/IFC export
```

Reprocessing a file always creates a new drawing revision from the original
immutable `file_id`; it never overwrites the previous revision. The new revision
records the adapter version used for the run and the `extraction_profile_id`
that defined the extraction settings so later jobs and audits can reproduce the
result. Each reprocess job also captures the finalized base revision in
`jobs.base_revision_id`.

The reprocess path uses the same optimistic revision guardrail as changeset
apply, but at request and finalization time instead of edit apply time:

- if `POST /v1/projects/{project_id}/files/{file_id}/reprocess` finds no
  finalized base revision, it returns `409 REVISION_CONFLICT`, creates no job,
  and enqueues nothing
- worker finalization re-checks that `jobs.base_revision_id` still matches the
  current finalized revision; if not, the job fails with `REVISION_CONFLICT`
  and commits no new revision, outputs, artifacts, or storage writes
- retrying a `REVISION_CONFLICT` reprocess job returns the unchanged failed job
  and does not enqueue a new attempt

## Original File Policy

Original uploads are never modified.

Every derived output must point back to:

- source file
- drawing revision
- changeset, if any
- takeoff, if any
- estimate, if any
- job that produced it

## Storage Interface

Storage is accessed through a single abstract interface so the local filesystem
implementation can be swapped for S3-compatible backends later.

```text
class Storage(Protocol):
    def put(self, key: str, data: BinaryIO, *, immutable: bool) -> StoredObject
    def get(self, key: str) -> BinaryIO
    def stat(self, key: str) -> StoredObjectMeta
    def exists(self, key: str) -> bool
    def delete(self, key: str) -> None  # forbidden when immutable=True
    def presign(self, key: str, *, ttl_seconds: int) -> str  # later
```

Rules:

- Originals are written with `immutable=True`. Backends must refuse overwrite or
  delete on immutable keys.
- Keys are derived from server IDs (`originals/{file_id}/{checksum}`), never
  from the client filename.
- The local filesystem backend uses `chmod 0o444` and rejects writes to existing
  paths to enforce immutability.
- Generated artifacts use a separate prefix (`artifacts/{artifact_id}/...`) and
  are immutable records/objects once written.
- Storage backends must reject any attempt to overwrite an existing generated
  artifact object, even when the payload is byte-identical.

## Retention and Soft Deletion

MVP retention is conservative: original uploads, derived revisions, and
generated artifacts are kept by default so lineage, debugging, and
reproducibility remain intact.

Soft deletion is a database visibility flag, not a storage mutation.

- `projects.deleted_at` hides the project from normal active listings but does
  not remove child file, revision, job, or artifact records from audit/history
  views.
- `files.deleted_at` marks the uploaded file record as deleted for product
  behavior, but the original immutable storage object remains in place during
  MVP.
- `artifacts.deleted_at` marks an artifact as hidden or no longer active for
  normal listings/download UX, but does not permit overwrite-in-place and does
  not remove the immutable stored object during MVP.
- Where a model supports soft deletion, `deleted_at` must be nullable, set only
  when the record transitions to a deleted/hidden state, and left unchanged for
  active records.
- Soft deletion never rewrites lineage. References from artifacts to source
  files, drawing revisions, jobs, changesets, takeoffs, or estimates remain
  intact even when one of those records is hidden from default views.
- Database-level delete behavior must match that policy: restrictive foreign
  keys should block physical deletion while lineage, audit, or history children
  still exist, rather than cascading through append-only records.

MVP does not perform automatic physical deletion. In particular:

- original upload objects are never physically removed by background retention
  jobs in MVP
- immutable generated artifact objects are never physically removed by automatic
  cleanup in MVP
- superseded or soft-deleted artifacts may be filtered from default product
  views, but they remain restorable from retained metadata/object storage during
  MVP
- physical deletes are exceptional/manual and should fail until dependent
  lineage-bearing rows are handled in a way that preserves audit history

## Job Pipeline Orchestration

Jobs form a small DAG per uploaded file, with `jobs.file_id` as the durable
origin and lock anchor for the chain:

```text
ingest(file)
  -> [optional] quantity_takeoff(drawing_revision)
       -> [optional] estimate(quantity_takeoff, rate_catalog)
            -> [optional] export(...)
```

`ingest(file)` produces a `drawing_revision` that becomes the pinned input for
later revision-scoped work. `quantity_takeoff(drawing_revision)` worker
execution runs against exactly one persisted revision rather than against the
mutable file head.

For MVP every step is triggered explicitly via API. A later iteration may add an
auto-chain configuration on the project that fires the next step on success.
Quantity worker execution exists for this DAG and persisted lineage model, while
manual/API creation and read surfaces for quantity takeoffs remain separate
deferred work.

Workers must:

- record the parent job id when chaining
- keep any optional `jobs.parent_job_id` in the same project/file lineage as the
  child job
- use `jobs.base_revision_id` to pin the exact input/base revision for
  revision-scoped work; reprocess keeps its stale-base conflict fence as the
  special current-revision check
- never silently start downstream work on failure or partial output
- surface the chain in `job_events` so clients can render a pipeline view
- treat each attempt as an isolated execution context with any temporary staged
  output scoped by job and attempt until finalization
- commit final outputs and terminal success together under the job row lock so a
  visible artifact set and a `succeeded` job state appear atomically
- be the only writers that promote staged execution data into committed drawing
  revisions, quantity takeoffs, estimate versions, artifact rows, and
  corresponding `job_events`
- reject stale completion paths from duplicate delivery, worker loss recovery,
  or overlapping retries so only one final commit path wins
- observe cancel checkpoints before expensive phases and again before final
  commit so a requested cancel can stop publication of new outputs

Operationally, this distinguishes retry from regeneration or re-export:

- Retry is recovery for the same job after failure. It may recreate staged
  attempt-local data, but it must still produce at most one visible committed
  final output set for that job.
- Attempt-local staging may be rewritten or discarded during retry, but committed
  append-only records from a prior successful finalization are never updated in
  place.
- Regeneration or re-export is a new job producing new immutable artifacts with
  new ids and storage keys per the artifact lifecycle rules below.
- `job_events` are the durable trace for queueing, execution, cancellation,
  retry scheduling, and terminal outcome transitions across attempts.

## Deployment Topology

Local development uses Docker Compose with these services:

- `api` - FastAPI/Uvicorn
- `worker` - Celery worker (one container per scaling unit)
- `postgres` - PostgreSQL 18
- `rabbitmq` - broker
- `flower` (optional) - Celery dashboard for development

GitHub Actions CI uses the same PostgreSQL 18 runtime so migrations and tests
exercise the same database major version as local development.

Production topology is deferred. The compose file should keep service names and
env vars stable so the same configuration can be lifted to a managed runtime.

## Artifact Lifecycle

Generated artifacts are append-only outputs. Once an artifact row/object is
written, it is immutable and cannot be replaced in place.

Each artifact record must keep enough lineage to explain exactly what produced
it and to reproduce it later. Minimum lineage fields:

- source file id
- drawing revision id
- changeset id, when the artifact comes from a revision export
- quantity takeoff id, when the artifact comes from quantity output
- estimate id, when the artifact comes from estimate output
- job id
- generator name
- generator version
- generator configuration or options snapshot
- output checksum

Rules:

- Regeneration or re-export always creates a new artifact row/object with a new
  artifact id and storage key.
- A newer artifact may reference an older artifact as its predecessor for
  lineage, but it must not overwrite or delete the older object as part of the
  write path.
- Clients may treat an artifact as superseded, but superseded does not mean
  mutable.
- Reproducibility is trace-based: the system must retain the lineage metadata
  required to rerun the same generator against the same stored source revision
  and changeset/takeoff/estimate inputs.
- The documented quantity lineage slot does not, by itself, make generated
  artifacts currently own a required quantity-takeoff foreign key.
- Original uploads and generated artifacts remain separate immutable classes of
  stored objects.

Append-only versus mutable record rules:

- Append-only records include original file records, drawing revisions, approved
  quantity takeoffs, finalized estimate versions, generated artifacts, and
  `job_events`.
- Mutable workflow/state fields include job status, progress, and retry/cancel
  control flags. Within protected append-only tables, only `files.deleted_at`
  and `generated_artifacts.deleted_at` are mutable, and only as write-once
  `NULL -> non-NULL` soft-delete markers.
- Mutable workflow changes must never replace the stored payload, checksum,
  provenance, or lineage of an already committed append-only record.

Retention/deletion for MVP is manual-first and soft-delete-first:

- By default, MVP keeps original uploads and generated artifacts for debugging,
  audit, and reproducibility.
- Deletion is a metadata action first. Project, file, and artifact rows may be
  soft-deleted with `deleted_at` and hidden from normal listings before any
  storage object is considered for removal.
- During MVP, physical cleanup is manual/administrative only and does not run as
  an automatic retention path.
- Any post-MVP physical cleanup flow must first verify all of the following:
  the target object is not the sole retained source or artifact required for
  lineage/reproducibility, the row is already soft-deleted or superseded per
  product rules, no active record still points at the object as its current
  visible payload, legal/audit retention requirements permit removal, and the
  cleanup action is recorded as an explicit administrative event.
- Post-MVP automation may prune eligible superseded or orphaned objects only if
  those safety checks are enforced before delete and immutable write-path rules
  remain unchanged.
