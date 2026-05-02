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
  -> ingestion job
  -> source adapter
  -> canonical entities
  -> quantity extraction
  -> estimate generation
  -> exports
```

For edits:

```text
Existing drawing revision
  -> user or agent proposed changeset
  -> validation
  -> new drawing revision
  -> DXF export
  -> later DWG/IFC export
```

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
- Deletion is a metadata action first. Artifact rows may be marked deleted or
  hidden from normal listings before any storage object is physically removed.
- Physical deletion, if performed during MVP, is manual/administrative and must
  never target the sole remaining copy needed for trace reproducibility.
- Automated cleanup of superseded or orphaned artifacts is post-MVP work.
