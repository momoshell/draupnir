"""Revision API routes."""

from fastapi import APIRouter

from app.api.v1.revision_routes.adapter_outputs import (
    adapter_outputs_router,
)
from app.api.v1.revision_routes.changesets import (
    changesets_router,
)
from app.api.v1.revision_routes.devices import (
    devices_router,
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
from app.api.v1.revision_routes.validation_reports import (
    validation_reports_router,
)

revisions_router = APIRouter()


revisions_router.include_router(file_revisions_router)
revisions_router.include_router(adapter_outputs_router)
revisions_router.include_router(changesets_router)
revisions_router.include_router(generated_artifacts_router)
revisions_router.include_router(materialization_router)
revisions_router.include_router(devices_router)
revisions_router.include_router(quantity_takeoffs_router)
revisions_router.include_router(estimates_router)
revisions_router.include_router(exports_router)
revisions_router.include_router(quantity_takeoff_create_router)


revisions_router.include_router(validation_reports_router)
