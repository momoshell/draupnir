"""Revision API routes."""

from fastapi import APIRouter

from app.api.v1.revision_routes.adapter_outputs import (
    adapter_outputs_router,
)
from app.api.v1.revision_routes.canonical import (
    canonical_router,
)
from app.api.v1.revision_routes.changesets import (
    changesets_router,
)
from app.api.v1.revision_routes.devices import (
    devices_router,
)
from app.api.v1.revision_routes.diff import (
    diff_router,
)
from app.api.v1.revision_routes.estimates import (
    estimates_router,
)
from app.api.v1.revision_routes.exports import (
    exports_router,
)
from app.api.v1.revision_routes.file_revisions import (
    file_revisions_router,
)
from app.api.v1.revision_routes.generated_artifacts import (
    generated_artifacts_router,
)
from app.api.v1.revision_routes.materialization import (
    materialization_router,
)
from app.api.v1.revision_routes.quantity_takeoffs import (
    quantity_takeoff_create_router,
    quantity_takeoffs_router,
)
from app.api.v1.revision_routes.rooms import (
    rooms_router,
)
from app.api.v1.revision_routes.scale import (
    scale_router,
)
from app.api.v1.revision_routes.service_takeoff import (
    service_takeoff_router,
)
from app.api.v1.revision_routes.source import (
    source_router,
)
from app.api.v1.revision_routes.summary import (
    summary_router,
)
from app.api.v1.revision_routes.validation_reports import (
    validation_reports_router,
)

revisions_router = APIRouter()


revisions_router.include_router(file_revisions_router, tags=["Revisions"])
revisions_router.include_router(scale_router, tags=["Revisions"])
revisions_router.include_router(summary_router, tags=["Revisions"])
revisions_router.include_router(canonical_router, tags=["Revisions"])
revisions_router.include_router(diff_router, tags=["Revisions"])
revisions_router.include_router(adapter_outputs_router, tags=["Adapter Outputs"])
revisions_router.include_router(changesets_router, tags=["Changesets"])
revisions_router.include_router(generated_artifacts_router, tags=["Artifacts"])
# Registered before materialization so /entities/{id}/source wins over the
# /entities/{entity_id:path} catch-all detail route.
revisions_router.include_router(source_router, tags=["Entities"])
revisions_router.include_router(materialization_router, tags=["Entities"])
revisions_router.include_router(devices_router, tags=["Devices"])
revisions_router.include_router(rooms_router, tags=["Rooms"])
revisions_router.include_router(service_takeoff_router, tags=["Service Takeoff"])
revisions_router.include_router(quantity_takeoffs_router, tags=["Quantities"])
revisions_router.include_router(estimates_router, tags=["Estimates"])
revisions_router.include_router(exports_router, tags=["Exports"])
revisions_router.include_router(quantity_takeoff_create_router, tags=["Quantities"])


revisions_router.include_router(validation_reports_router, tags=["Validation"])
