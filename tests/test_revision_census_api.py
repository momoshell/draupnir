"""Integration tests for the revision interpretation + source-census read API (#566)."""

import uuid
from collections.abc import Awaitable, Callable
from dataclasses import replace
from typing import Any

import httpx
import pytest

import app.jobs.worker as worker_module
from app.core.errors import ErrorCode
from app.ingestion.finalization import IngestFinalizationPayload
from app.ingestion.runner import IngestionRunRequest
from app.jobs.worker import process_ingest_job
from tests.conftest import requires_database
from tests.jobs_test_helpers import _create_project, _get_job_for_file, _upload_file
from tests.test_ingest_output_persistence import _load_project_outputs
from tests.test_jobs import _build_fake_ingest_payload

_INTERPRETATION = {
    "schema_version": "0.1",
    "length": {"source": "INSUNITS", "normalized": "millimeter", "confirmed": True},
    "angle": {
        "source": "dwgread",
        "stored_unit": "radians",
        "canonical_unit": "degrees",
        "confirmed": True,
    },
    "orientation": {
        "source": "HEADER",
        "angbase_degrees": 0.0,
        "angdir": 0,
        "north_degrees": None,
        "rotated": False,
        "confirmed": True,
    },
}
_CENSUS = {
    "schema_version": "0.1",
    "source": "dwgread",
    "raw_object_total": 1200,
    "raw_objects": {"LINE": 800, "CIRCLE": 200, "INSERT": 200},
    "drawable_candidates": 1100,
    "materialized": 1090,
    "dropped": {
        "total": 10,
        "unsupported_drawables": 4,
        "malformed_drawables": 3,
        "unsupported_hatches": 1,
        "malformed_hatches": 1,
        "malformed_inserts": 1,
        "unsupported_types": ["SOLID", "VIEWPORT"],
    },
    "unsupported_classes": [
        {"dxfname": "ACAD_PROXY", "cppname": None, "is_zombie": False, "is_proxy": True},
    ],
}


def _fake_runner_with_canonical(
    *,
    interpretation: dict[str, Any] | None,
    census: dict[str, Any] | None,
) -> Callable[[IngestionRunRequest], Awaitable[IngestFinalizationPayload]]:
    """Build a fake ``run_ingestion`` that injects interpretation/census into the canonical."""

    async def _run(request: IngestionRunRequest) -> IngestFinalizationPayload:
        payload = _build_fake_ingest_payload(request)
        canonical = dict(payload.canonical_json)
        if interpretation is not None:
            canonical["interpretation"] = interpretation
        if census is not None:
            canonical["census"] = census
        return replace(payload, canonical_json=canonical)

    return _run


@requires_database
class TestRevisionInterpretationCensusApi:
    """Tests for ``GET /revisions/{id}/interpretation`` and ``.../census``."""

    async def _ingest_revision(
        self,
        async_client: httpx.AsyncClient,
        *,
        filename: str,
        media_type: str,
    ) -> Any:
        project = await _create_project(async_client)
        uploaded = await _upload_file(
            async_client,
            project["id"],
            filename=filename,
            media_type=media_type,
        )
        job = await _get_job_for_file(str(uploaded["id"]))
        await process_ingest_job(job.id)
        adapter_outputs, drawing_revisions, _reports, _artifacts = await _load_project_outputs(
            project["id"]
        )
        return drawing_revisions[0], adapter_outputs[0]

    async def test_interpretation_block_surfaced(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A revision whose canonical carries an interpretation block exposes it verbatim."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        monkeypatch.setattr(
            worker_module,
            "run_ingestion",
            _fake_runner_with_canonical(interpretation=_INTERPRETATION, census=None),
        )

        revision, adapter_output = await self._ingest_revision(
            async_client, filename="plan.dxf", media_type="application/dxf"
        )

        response = await async_client.get(f"/v1/revisions/{revision.id}/interpretation")

        assert response.status_code == 200
        body = response.json()
        assert body["available"] is True
        assert body["schema_version"] == "0.1"
        assert body["length"] == _INTERPRETATION["length"]
        assert body["angle"] == _INTERPRETATION["angle"]
        assert body["orientation"] == _INTERPRETATION["orientation"]
        assert body["source_input_family"] == adapter_output.input_family

    async def test_census_block_surfaced(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A revision whose canonical carries a census block exposes histogram + dispositions."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        monkeypatch.setattr(
            worker_module,
            "run_ingestion",
            _fake_runner_with_canonical(interpretation=None, census=_CENSUS),
        )

        revision, adapter_output = await self._ingest_revision(
            async_client, filename="plan.dxf", media_type="application/dxf"
        )

        response = await async_client.get(f"/v1/revisions/{revision.id}/census")

        assert response.status_code == 200
        body = response.json()
        assert body["available"] is True
        assert body["schema_version"] == "0.1"
        assert body["source"] == "dwgread"
        assert body["raw_object_total"] == 1200
        assert body["raw_objects"] == {"LINE": 800, "CIRCLE": 200, "INSERT": 200}
        assert body["drawable_candidates"] == 1100
        assert body["materialized"] == 1090
        assert body["dropped"] == _CENSUS["dropped"]
        assert body["unsupported_classes"] == _CENSUS["unsupported_classes"]
        assert body["source_input_family"] == adapter_output.input_family

    async def test_interpretation_absent_reports_unavailable(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A revision whose adapter emitted no interpretation block reports available=False."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        monkeypatch.setattr(
            worker_module,
            "run_ingestion",
            _fake_runner_with_canonical(interpretation=None, census=None),
        )

        revision, adapter_output = await self._ingest_revision(
            async_client, filename="plan.pdf", media_type="application/pdf"
        )

        body = (await async_client.get(f"/v1/revisions/{revision.id}/interpretation")).json()
        assert body["available"] is False
        assert body["length"] is None
        assert body["angle"] is None
        assert body["orientation"] is None
        # The adapter ran (PDF), so the input family is still reported honestly.
        assert body["source_input_family"] == adapter_output.input_family

    async def test_census_absent_reports_unavailable(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A revision whose adapter emitted no census block reports available=False, empties."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        monkeypatch.setattr(
            worker_module,
            "run_ingestion",
            _fake_runner_with_canonical(interpretation=None, census=None),
        )

        revision, adapter_output = await self._ingest_revision(
            async_client, filename="plan.pdf", media_type="application/pdf"
        )

        body = (await async_client.get(f"/v1/revisions/{revision.id}/census")).json()
        assert body["available"] is False
        assert body["raw_object_total"] is None
        assert body["raw_objects"] == {}
        assert body["dropped"] is None
        assert body["unsupported_classes"] == []
        assert body["source_input_family"] == adapter_output.input_family

    async def test_missing_revision_interpretation_returns_not_found(
        self,
        async_client: httpx.AsyncClient,
    ) -> None:
        """An unknown revision id returns the standard not-found error envelope."""
        _ = self

        response = await async_client.get(f"/v1/revisions/{uuid.uuid4()}/interpretation")

        assert response.status_code == 404
        assert response.json()["error"]["code"] == ErrorCode.NOT_FOUND.value

    async def test_missing_revision_census_returns_not_found(
        self,
        async_client: httpx.AsyncClient,
    ) -> None:
        """An unknown revision id returns the standard not-found error envelope."""
        _ = self

        response = await async_client.get(f"/v1/revisions/{uuid.uuid4()}/census")

        assert response.status_code == 404
        assert response.json()["error"]["code"] == ErrorCode.NOT_FOUND.value
