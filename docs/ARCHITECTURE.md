# Architecture

## High-Level Shape

```text
Client UI / CLI / TUI
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
The first editable export target is DXF. DWG export is a later adapter.

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

Quantity workers must refuse to treat review-gated ingestion output as trusted
source-of-truth input. Provisional quantity runs are allowed only when the
upstream revision is marked provisional, and the resulting quantities must stay
explicitly provisional downstream.

Quantity workers evaluate both review state and validation status before
starting. A technically invalid revision is blocked even if its review state was
previously approved. Revisions that are technically usable but have warnings or
review-needed findings may still proceed only through the derived gate policy.

Revisions marked `review_required`, `rejected`, or `superseded` are not eligible
for quantity generation.

For edits:

```text
Existing drawing revision
  -> user or agent proposed changeset, or file reprocess request
  -> validation / adapter execution
  -> new drawing revision
  -> DXF export
  -> later DWG/IFC export
```

Reprocessing a file always creates a new drawing revision from the original
immutable `file_id`; it never overwrites the previous revision. The new revision
records the adapter version used for the run and the `extraction_profile_id`
that defined the extraction settings so later jobs and audits can reproduce the
result.

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

MVP does not perform automatic physical deletion. In particular:

- original upload objects are never physically removed by background retention
  jobs in MVP
- immutable generated artifact objects are never physically removed by automatic
  cleanup in MVP
- superseded or soft-deleted artifacts may be filtered from default product
  views, but they remain restorable from retained metadata/object storage during
  MVP

## Job Pipeline Orchestration

Jobs form a small DAG per uploaded file:

```text
ingest(file)
  -> [optional] quantity_takeoff(drawing_revision)
       -> [optional] estimate(quantity_takeoff, rate_catalog)
            -> [optional] export(...)
```

For MVP every step is triggered explicitly via API. A later iteration may add an
auto-chain configuration on the project that fires the next step on success.

Workers must:

- record the parent job id when chaining
- never silently start downstream work on failure or partial output
- surface the chain in `job_events` so clients can render a pipeline view
- treat each attempt as an isolated execution context with any temporary staged
  output scoped by job and attempt until finalization
- commit final outputs and terminal success together under the job row lock so a
  visible artifact set and a `succeeded` job state appear atomically
- reject stale completion paths from duplicate delivery, worker loss recovery,
  or overlapping retries so only one final commit path wins
- observe cancel checkpoints before expensive phases and again before final
  commit so a requested cancel can stop publication of new outputs

Operationally, this distinguishes retry from regeneration or re-export:

- Retry is recovery for the same job after failure. It may recreate staged
  attempt-local data, but it must still produce at most one visible committed
  final output set for that job.
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
- Original uploads and generated artifacts remain separate immutable classes of
  stored objects.

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
