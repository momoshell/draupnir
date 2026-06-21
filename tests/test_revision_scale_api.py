"""Integration tests for the revision drawing-scale / units read API."""

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

_DXF_UNITS = {
    "normalized": "millimeter",
    "source": "$INSUNITS",
    "source_value": 4,
    "conversion_target": "meter",
    "conversion_factor": 0.001,
}
_PDF_SCALE = {
    "status": "detected",
    "coordinate_space": "page_points",
    "unit": "point",
    "real_world_units": "meter",
    "scale_ratio": "1:100",
    "points_to_real": 0.0352778,
    "confidence": "medium",
}


def _fake_runner_with_canonical(
    *,
    units: dict[str, Any] | None,
    pdf_scale: dict[str, Any] | None,
) -> Callable[[IngestionRunRequest], Awaitable[IngestFinalizationPayload]]:
    """Build a fake ``run_ingestion`` that injects scale/units into the canonical payload."""

    async def _run(request: IngestionRunRequest) -> IngestFinalizationPayload:
        payload = _build_fake_ingest_payload(request)
        canonical = dict(payload.canonical_json)
        if units is not None:
            canonical["units"] = units
        if pdf_scale is not None:
            canonical["pdf_scale"] = pdf_scale
        return replace(payload, canonical_json=canonical)

    return _run


@requires_database
class TestRevisionScaleApi:
    """Tests for ``GET /revisions/{revision_id}/scale``."""

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

    async def test_dxf_revision_exposes_units_and_input_family(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A vector CAD revision reports its normalized units + conversion factor."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        monkeypatch.setattr(
            worker_module,
            "run_ingestion",
            _fake_runner_with_canonical(units=_DXF_UNITS, pdf_scale=None),
        )

        revision, adapter_output = await self._ingest_revision(
            async_client, filename="plan.dxf", media_type="application/dxf"
        )

        response = await async_client.get(f"/v1/revisions/{revision.id}/scale")

        assert response.status_code == 200
        body = response.json()
        assert body["units"] == _DXF_UNITS
        assert body["units"]["normalized"] == "millimeter"
        assert body["units"]["conversion_factor"] == 0.001
        assert body["pdf_scale"] is None
        assert body["source_input_family"] == adapter_output.input_family
        # #557: a resolved $INSUNITS unit is confirmed → real-world dimensions available.
        assert body["units_confidence"] == "confirmed"
        assert body["real_world_dimensions_available"] is True

    async def test_pdf_revision_exposes_pdf_scale(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A PDF revision reports the page-space scale payload + unknown drawing units."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        monkeypatch.setattr(
            worker_module,
            "run_ingestion",
            _fake_runner_with_canonical(units={"normalized": "unknown"}, pdf_scale=_PDF_SCALE),
        )

        revision, adapter_output = await self._ingest_revision(
            async_client, filename="plan.pdf", media_type="application/pdf"
        )

        response = await async_client.get(f"/v1/revisions/{revision.id}/scale")

        assert response.status_code == 200
        body = response.json()
        assert body["units"] == {"normalized": "unknown"}
        assert body["pdf_scale"] == _PDF_SCALE
        assert body["source_input_family"] == adapter_output.input_family
        assert adapter_output.input_family == "pdf_vector"
        # #557: drawing units are unknown, but a detected PDF point->real factor makes
        # real-world dimensions available; unit confidence still reflects the unknown unit.
        assert body["units_confidence"] == "unknown"
        assert body["real_world_dimensions_available"] is True

    async def test_revision_without_units_payload_reports_unknown(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A revision whose adapter payload omits units honestly reports unknown."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        monkeypatch.setattr(
            worker_module,
            "run_ingestion",
            _fake_runner_with_canonical(units=None, pdf_scale=None),
        )

        revision, _adapter_output = await self._ingest_revision(
            async_client, filename="plan.dxf", media_type="application/dxf"
        )

        response = await async_client.get(f"/v1/revisions/{revision.id}/scale")

        assert response.status_code == 200
        body = response.json()
        assert body["units"] == {"normalized": "unknown"}
        assert body["pdf_scale"] is None
        # #557: no unit and no PDF factor → honest unknown, no real-world dimensions.
        assert body["units_confidence"] == "unknown"
        assert body["real_world_dimensions_available"] is False

    async def test_unconfirmed_insunits_dwg_reports_unknown_unavailable(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A DWG whose units couldn't be confirmed ($INSUNITS=0/missing) resolves to unknown →
        no silent assumption: confidence unknown, real-world dimensions unavailable (#557)."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        monkeypatch.setattr(
            worker_module,
            "run_ingestion",
            _fake_runner_with_canonical(units={"normalized": "unknown"}, pdf_scale=None),
        )

        revision, _adapter_output = await self._ingest_revision(
            async_client, filename="plan.dxf", media_type="application/dxf"
        )

        body = (await async_client.get(f"/v1/revisions/{revision.id}/scale")).json()
        assert body["units_confidence"] == "unknown"
        assert body["real_world_dimensions_available"] is False

    async def test_adapter_declared_unit_confidence_is_passed_through(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Forward-compat (#558): when the adapter records its own ``confidence`` on the units
        block (e.g. an inferred unit), the route surfaces it verbatim rather than re-deriving."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        inferred_units = {
            "normalized": "millimeter",
            "source": "inferred",
            "confidence": "inferred",
            "conversion_target": "meter",
            "conversion_factor": 0.001,
        }
        monkeypatch.setattr(
            worker_module,
            "run_ingestion",
            _fake_runner_with_canonical(units=inferred_units, pdf_scale=None),
        )

        revision, _adapter_output = await self._ingest_revision(
            async_client, filename="plan.dxf", media_type="application/dxf"
        )

        body = (await async_client.get(f"/v1/revisions/{revision.id}/scale")).json()
        assert body["units_confidence"] == "inferred"
        assert body["real_world_dimensions_available"] is True

    async def test_missing_revision_returns_not_found(
        self,
        async_client: httpx.AsyncClient,
    ) -> None:
        """An unknown revision id returns the standard not-found error envelope."""
        _ = self

        response = await async_client.get(f"/v1/revisions/{uuid.uuid4()}/scale")

        assert response.status_code == 404
        assert response.json()["error"]["code"] == ErrorCode.NOT_FOUND.value
