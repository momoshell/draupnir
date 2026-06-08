# ADR 0010 - Containerized PDF Intake Service Boundary

## Status

Accepted

## Context

PDF ingestion depends on tooling that the rest of the system does not:

- **Vector PDF** uses PyMuPDF (`fitz`), licensed **AGPL-3.0-or-later OR commercial**
  (ADR 0007). Bundling it into a distributed image extends AGPL obligations to
  whatever the image contains.
- **Raster PDF** uses a multi-stage pipeline — VTracer, OpenCV/scikit-image,
  Pillow, and the Tesseract system binary (ADR 0008). These are permissively
  licensed (MIT / BSD / Apache-2.0) but heavy and binary-dependent.

Today both adapters run **in-process** behind the ingestion adapter contract
(`app/ingestion/contracts.py`), isolated only at the module boundary. That keeps
the AGPL surface and the heavy raster dependencies inside the core API/worker
runtime. We want PDF ingestion to be **operationally available** via
Podman/Compose without forcing all PDF tooling — and its licensing surface —
into the lean core runtime.

ADR 0007 already anticipated this: its licensing mitigations list
"Deploy PyMuPDF as an external service/microservice to further isolate the AGPL
boundary." This ADR realizes that boundary.

This is intentionally scoped as **Tier 3 architecture work before
implementation**: it establishes the boundary, its contract, and its operational
model so the dependency and licensing choices are documented and reviewable
*before* any production or default-deployment decision.

## Decision

Introduce an **optional, additive, off-by-default** containerized PDF intake
service boundary. Nothing about the existing in-process PDF paths changes.

1. **Separate service.** A new `services/pdf-intake/` FastAPI service owns the
   PDF-specific dependencies (its own `pyproject.toml` with `vector` / `raster`
   optional-dependency extras and its own `Containerfile`). The lean core image
   does not gain PyMuPDF or the raster stack.

2. **HTTP transport.** The core reaches the service over HTTP using `httpx`
   (already a core dependency), not the Celery broker. HTTP gives the service a
   first-class, synchronously probeable health/capabilities contract and matches
   the reserved `ProbeKind.SERVICE` capability-probe value.

3. **Stable health/capabilities contract.** The service exposes:
   - `GET /health` → liveness;
   - `GET /capabilities` → `{ service, version, modes: ["vector", "raster"], extraction_implemented }`;
   - `POST /v1/ingest` → placeholder returning `501 Not Implemented` for now.

4. **Capability surfacing, not routing.** A new non-routable registry descriptor
   `pdf_intake_service` (family `pdf_vector`, `can_read=False`) advertises the
   boundary through `GET /v1/system/capabilities`. Because `can_read=False` it is
   excluded from the family-keyed read registry, adapter selection, the runner,
   and the export registry — so the in-process PyMuPDF / VTracer paths remain the
   active read paths and generic PDF fallback behavior is unchanged. It is also
   excluded from `/v1/system/health` aggregation so that an intentionally
   unconfigured optional service never degrades overall system health.

5. **Bounded `SERVICE` probe.** `/v1/system/capabilities` resolves the boundary's
   status from `settings.pdf_intake_service_url`:
   - unset → no network call; reported as `disabled_by_config` (the steady,
     off-by-default state);
   - configured → bounded TCP handshake (≤ `pdf_intake_service_timeout_seconds`),
     reachable ⇒ available, otherwise reported unreachable.

6. **Service adapter stub.** `app/ingestion/adapters/pdf_service.py` implements
   the `IngestionAdapter` contract as the implementation target for the
   descriptor and defines how remote transport failures map onto the adapter
   contract (timeout → `TIMEOUT`, connect error → `UNAVAILABLE`, non-2xx →
   `FAILED`, `501` → `UNAVAILABLE`/not-implemented). It is not wired into runner
   selection.

7. **Operational model.** The service is wired into `docker-compose.yml` under a
   `pdf` profile (mirroring `flower`'s `debug` profile), so the default
   `compose up` does not start it. The core's `PDF_INTAKE_SERVICE_URL` defaults
   empty.

## Licensing & Dependency Isolation

- **PyMuPDF (AGPL-or-commercial)** is confined to `services/pdf-intake/`. The core
  image stays license-light; the AGPL surface is bounded to the separately built
  service image, the strongest isolation among ADR 0007's mitigation options.
- **Raster stack** (VTracer MIT, OpenCV/scikit-image BSD, Tesseract Apache-2.0,
  Pillow) likewise lives only in the service image and its `raster` extra.
- The core declares **no new heavyweight dependencies**; the boundary uses the
  already-present `httpx`.

## Alternatives Considered

1. **Keep PDF tooling in-process (status quo).** Rejected as the default end
   state: it keeps the AGPL surface and heavy binaries in the core runtime.
   Retained as the active path until extraction is migrated.
2. **Broker/Celery transport instead of HTTP.** Rejected: couples the boundary to
   the broker and provides no clean synchronous health/capabilities contract.
3. **New `InputFamily` for the service.** Rejected: the registry indexes read
   descriptors by family and forbids duplicates; a new family (or a routable
   duplicate) would entangle selection/runner logic. Reusing `pdf_vector` with
   `can_read=False` keeps the descriptor purely informational.

## Consequences

- **Positive:** PDF tooling and its licensing surface are isolated from the lean
  core; the boundary is observable via the existing capability/health contract;
  the change is fully additive and off by default, so no existing behavior moves.
- **Negative:** A second deployable unit and image to build/operate; the boundary
  does not yet carry real extraction, so it is observability + contract only.
- **Risk:** When real extraction migrates, confidence/provenance parity with the
  in-process adapters must be preserved through the adapter contract.

## Deferred / Follow-up

- Implement real remote extraction in the service (`POST /v1/ingest`) and result
  handling in the service adapter.
- Decide whether/when to route PDF ingestion through the service by default
  (would make the descriptor routable); until then the in-process adapters remain
  authoritative.
- Production deployment, scaling, and licensing sign-off for distributing the
  service image.
