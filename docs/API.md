# API Plan

This is the planned HTTP API surface. Exact schemas will be generated from the
FastAPI application once implementation starts.

## Health

```text
GET /health
```

## Projects

```text
POST /projects
GET /projects
GET /projects/{project_id}
PATCH /projects/{project_id}
DELETE /projects/{project_id}
```

## Files

```text
POST /projects/{project_id}/files
GET /projects/{project_id}/files
GET /projects/{project_id}/files/{file_id}
```

Uploads should create a file record and enqueue ingestion.

## Jobs

```text
GET /jobs/{job_id}
POST /jobs/{job_id}/cancel
POST /jobs/{job_id}/retry
GET /jobs/{job_id}/events
```

## Drawing Revisions

```text
GET /projects/{project_id}/drawings
GET /projects/{project_id}/drawings/{drawing_revision_id}
GET /projects/{project_id}/drawings/{drawing_revision_id}/entities
GET /projects/{project_id}/drawings/{drawing_revision_id}/layers
GET /projects/{project_id}/drawings/{drawing_revision_id}/blocks
```

## Quantities

```text
POST /projects/{project_id}/quantity-takeoffs
GET /projects/{project_id}/quantity-takeoffs
GET /projects/{project_id}/quantity-takeoffs/{takeoff_id}
GET /projects/{project_id}/quantity-takeoffs/{takeoff_id}/items
```

## Estimates

```text
POST /projects/{project_id}/estimates
GET /projects/{project_id}/estimates
GET /projects/{project_id}/estimates/{estimate_id}
GET /projects/{project_id}/estimates/{estimate_id}/items
```

## Rate Catalog

```text
POST /rate-catalogs
GET /rate-catalogs
GET /rate-catalogs/{catalog_id}
POST /rate-catalogs/{catalog_id}/rates
GET /rate-catalogs/{catalog_id}/rates
```

## Changesets

```text
POST /projects/{project_id}/changesets
GET /projects/{project_id}/changesets
GET /projects/{project_id}/changesets/{changeset_id}
POST /projects/{project_id}/changesets/{changeset_id}/validate
POST /projects/{project_id}/changesets/{changeset_id}/apply
```

Changesets are the only supported path for modifying a drawing revision.

## Exports

```text
POST /projects/{project_id}/exports
GET /projects/{project_id}/exports
GET /projects/{project_id}/exports/{artifact_id}
```

Supported MVP export types:

- JSON
- CSV
- PDF
- DXF

Planned later export types:

- DWG
- IFC
- STEP
- IGES
- SVG
- PNG

## Future AI Endpoints

Not part of the MVP core implementation.

```text
POST /projects/{project_id}/ai/classify
POST /projects/{project_id}/ai/explain-estimate
POST /projects/{project_id}/ai/propose-changeset
```

