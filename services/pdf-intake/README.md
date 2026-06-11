# Draupnir PDF intake service

Optional, containerized service boundary for PDF ingestion. It isolates the
PDF-specific dependencies (notably the AGPL-or-commercial PyMuPDF stack and the
raster tooling) from the lean core API/worker runtime.

This is a **scaffold**: it exposes the stable boundary contract but does not yet
carry real PDF extraction. See ADR 0010 (`docs/decisions/0010-pdf-intake-service-boundary.md`)
in the core repository for the full rationale and what is deferred.

## Endpoints

| Method | Path              | Purpose                                                  |
| ------ | ----------------- | -------------------------------------------------------- |
| GET    | `/health`         | Liveness probe consumed by the core capability/health.   |
| GET    | `/capabilities`   | Advertises supported modes (`vector`, `raster`).         |
| POST   | `/v1/ingest`      | Placeholder; returns `501 Not Implemented` for now.      |

## What this service is for

Use this README when you want to work on the standalone PDF intake service
itself or exercise the service-boundary scaffold from ADR 0010.

- If you want PDF ingestion inside the main Draupnir repo/API/worker workflow,
  use the root-repo docs instead: default Compose is DXF-only, vector PDF is an
  intentional `pdf-vector` opt-in, and raster PDF requires the full
  `ingestion` extra.
- If you want the isolated PDF service boundary, use the steps below and point
  the core repo at this service with `PDF_INTAKE_SERVICE_URL`.

## Run locally

```bash
cd services/pdf-intake
uv sync --extra vector
uv run uvicorn app.main:app --host 0.0.0.0 --port 8100
```

## Run via Compose (off by default)

The service is gated behind the `pdf` profile so the default stack does not
start it:

```bash
docker compose --profile pdf up pdf-intake
```

Point the core at it by setting `PDF_INTAKE_SERVICE_URL` (e.g.
`http://pdf-intake:8100`) on the `api`/`worker` services. When unset, the core
reports the `pdf_intake_service` adapter as `disabled_by_config`.

This profile is separate from the main repo's built-in PDF extras workflow. It
does not replace the documented host/Compose setup for direct vector/raster PDF
support in the core API and worker.
