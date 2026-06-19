---
name: draupnir
description: >-
  Query and act on CAD/BIM drawings ingested by Draupnir — orient on a revision,
  verify data quality, run spatial/room/entity queries, and drive ingestion,
  changeset, and export flows. Use when a task involves drawings, floor plans,
  device schedules, room takeoffs, or quantities/estimates from CAD/PDF/IFC.
---

# Draupnir

Draupnir ingests CAD/BIM drawings (DXF/DWG/PDF/IFC) into a canonical model you can
query, validate, and turn into quantities/estimates. Access is through the
Draupnir MCP server's tools and resources. The MCP server also delivers a concise
version of this guidance in its `instructions`; this skill is the fuller reference.

## Mental model

- **Project → File → Revision.** A file is one uploaded drawing; each ingestion (or
  applied changeset) produces a **revision** — an immutable canonical snapshot.
  Almost everything is scoped to a `revision_id`.
- **Tiers.** Canonical entities/layers/blocks (materialized) → interpretations
  (devices, rooms, layer-roles — derived on demand) → quantities/estimates.
- **Trust is explicit.** Every revision has a validation report: a deterministic
  `validation_status`, structured checks/findings, and extraction **coverage**.
  Coverage tells you *how much* was mapped, not that it was mapped *correctly*.

## Workflow

### 1. Orient
1. `server_info` — confirm the API is reachable/healthy.
2. `list_projects` → `get_project` → `list_project_files` → `list_file_revisions`.
   The newest revision is usually the target.
3. `get_revision_summary(revision_id)` — one-call overview (entity/layer/device/room
   counts + coverage + scale). `get_revision_scale` for units and drawing/page scale.

### 2. Verify before trusting
- `verify_revision(revision_id)` → `validation_status`
  (`valid` / `valid_with_warnings` / `invalid`), failed/warning checks, coverage
  headline, and a `usable` flag (false only when `invalid`).
- `explain_finding(revision_id, finding_id)` → the check behind a finding, its
  severity, and the affected target.
- Be cautious with a low `mapped_ratio` or large `unmapped_by_reason` counts.

### 3. Query
- `list_revision_entities` — filter by `entity_type` / `layer_ref`; opt into heavy
  blocks with `fields=` (geometry, properties, provenance, confidence, canonical);
  spatial via `min_x/min_y/max_x/max_y` (in-bbox) or `near_x/near_y/radius`.
- **Entities in a room:** `list_revision_rooms` → match the room `name`, take its
  `id` → `list_revision_room_entities(revision_id, room_id, entity_type=…)`.
  Containment is centroid-in-polygon, smallest containing room (nested-room safe).
- Also: `list_revision_devices`, `list_revision_layers`, `list_revision_layer_roles`,
  `list_revision_rooms`, quantity/estimate lists. Single objects are browseable
  resources by id (project, file, job, entity, changeset, estimate, takeoff, …).

### 4. Act (asynchronous; idempotency is automatic)
- **Ingest:** `upload_project_file(project_id, file_path)` (local path) starts
  ingestion → `wait_for_job(job_id)` until `succeeded`/`failed`/`cancelled`.
- **Changeset:** `create_revision_changeset` → `validate_revision_changeset` →
  `apply_revision_changeset` (returns a job → `wait_for_job`).
- **Export:** `create_*_export` returns a job → `wait_for_job` → the artifact is
  listed under the revision/file.

## Worked example — "which devices are in the kitchen on the latest revision"

1. `list_file_revisions(file_id)` → newest `revision_id`.
2. `verify_revision(revision_id)` → confirm it's usable.
3. `list_revision_rooms(revision_id)` → find the room named "Kitchen"; take its `id`.
4. `list_revision_room_entities(revision_id, room_id, entity_type="insert")` → the
   device instances placed in that room.
