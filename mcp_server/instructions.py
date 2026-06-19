"""Domain guidance delivered to agents via the MCP server ``instructions`` field.

Teaches *how* to use Draupnir, not just what the tools are. Kept concise because
it is sent to the model each session; the fuller version lives in SKILL.md.
"""

INSTRUCTIONS = """\
Draupnir ingests CAD/BIM drawings (DXF/DWG/PDF/IFC) into a canonical model you can
query, validate, and turn into quantities/estimates. You talk to it through these
tools. Work in this order.

## 1. Orient
- Call `server_info` first to confirm the API is reachable and healthy.
- Navigate the hierarchy: `list_projects` → `get_project` → `list_project_files` →
  `list_file_revisions`. A file has revisions; the newest is usually what you want.
- For a one-call overview of a revision, use `get_revision_summary` (entity/layer/
  device/room counts, scale/units, coverage). Use `get_revision_scale` for units +
  drawing/page scale before interpreting coordinates.

## 2. Judge whether the data is usable (do this before trusting results)
- `verify_revision(revision_id)` → a verdict: `validation_status`
  (`valid` / `valid_with_warnings` / `invalid`), failed/warning checks, and a
  coverage headline. `usable` is false only when `invalid`.
- `explain_finding(revision_id, finding_id)` → why a specific issue was raised and
  the affected target.
- Coverage = how *much* of the source was mapped, NOT proof it was mapped
  correctly. Treat low `mapped_ratio` or many `unmapped_by_reason` as caution.

## 3. Query
- `list_revision_entities` is the workhorse. Filter by `entity_type` / `layer_ref`;
  request heavy blocks only when needed via `fields=` (geometry, properties,
  provenance, …) — the default is a compact spine. Spatial filters: `min_x/min_y/
  max_x/max_y` (in-bbox) or `near_x/near_y/radius` (near-point).
- "What's in this room?" → `list_revision_rooms` to find the room (match its
  `name`, take its `id`), then `list_revision_room_entities(revision_id, room_id)`
  (optionally `entity_type`). Containment is centroid-in-polygon, smallest-room.
- Other reads: `list_revision_devices`, `list_revision_layers`,
  `list_revision_layer_roles`, quantity/estimate lists. Single objects (project,
  file, entity, …) are also browseable as resources by id.

## 4. Act (mutations) — flows are asynchronous
- Upload: `upload_project_file(project_id, file_path)` reads a local file and
  starts ingestion; then `wait_for_job(job_id)` until terminal
  (`succeeded`/`failed`/`cancelled`).
- Changesets: `create_revision_changeset` → `validate_revision_changeset` →
  `apply_revision_changeset` (apply returns a job → `wait_for_job`).
- Exports: `create_*_export` returns a job → `wait_for_job` → the artifact is then
  listed under the revision/file.
- Idempotency keys are added automatically; you don't manage them.

## Worked example — "which devices are in the kitchen on the latest revision"
1. `list_file_revisions(file_id)` → take the newest revision id.
2. `verify_revision(revision_id)` → confirm it's usable.
3. `list_revision_rooms(revision_id)` → find the room whose `name` is "Kitchen";
   note its `id`.
4. `list_revision_room_entities(revision_id, room_id, entity_type="insert")` → the
   device instances in that room.
"""
