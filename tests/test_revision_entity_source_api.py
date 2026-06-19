"""Integration test for the source-anchored entity drill-down API (#522).

Uses REAL DXF ingestion (no fake runner) so the original file is stored and entities
carry real DXF handles, then drills from a canonical entity back to its raw source.
"""

import uuid
from pathlib import Path
from typing import Any

import httpx

from app.jobs.worker import process_ingest_job
from tests.conftest import requires_database
from tests.jobs_test_helpers import _create_project, _get_job_for_file, _upload_file
from tests.test_ingest_output_persistence import (
    _load_project_materialization,
    _load_project_outputs,
)

_DXF_FIXTURE = Path(__file__).with_name("fixtures") / "dxf" / "simple-line.dxf"


@requires_database
class TestRevisionEntitySourceApi:
    """Tests for ``GET /revisions/{revision_id}/entities/{entity_id}/source``."""

    async def _ingest_dxf(self, async_client: httpx.AsyncClient) -> tuple[Any, Any]:
        project = await _create_project(async_client)
        uploaded = await _upload_file(
            async_client,
            project["id"],
            filename="simple-line.dxf",
            content=_DXF_FIXTURE.read_bytes(),
            media_type="application/dxf",
        )
        job = await _get_job_for_file(str(uploaded["id"]))
        await process_ingest_job(job.id)

        _outputs, revisions, _reports, _artifacts = await _load_project_outputs(project["id"])
        _manifests, _layouts, _layers, _blocks, entities = await _load_project_materialization(
            project["id"]
        )
        return revisions[0], entities

    async def test_drills_from_entity_to_raw_dxf_fragment(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """A DXF entity resolves to its raw native object via the stored source."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        revision, entities = await self._ingest_dxf(async_client)
        entity = next(e for e in entities if e.source_identity is not None)

        response = await async_client.get(
            f"/v1/revisions/{revision.id}/entities/{entity.entity_id}/source"
        )

        assert response.status_code == 200
        body = response.json()
        assert body["entity_id"] == entity.entity_id
        assert body["upload_format"] == "dxf"
        assert body["available"] is True
        assert body["fragment"]["format"] == "dxf"
        assert body["fragment"]["native_type"]  # e.g. LINE
        assert body["fragment"]["handle"] == entity.source_identity
        assert body["reason"] is None

    async def test_unknown_entity_returns_404(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """An unknown entity id on a materialized revision returns 404."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        revision, _entities = await self._ingest_dxf(async_client)
        response = await async_client.get(
            f"/v1/revisions/{revision.id}/entities/{uuid.uuid4()}/source"
        )
        assert response.status_code == 404
