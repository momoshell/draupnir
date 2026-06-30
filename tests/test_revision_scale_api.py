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
    "real_world_units": True,
    "real_world_unit": "meter",
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

    async def test_contradicted_units_voids_units_signal(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A units block with a truthy contradiction (#558) → real_world_dimensions_available
        False and units_contradicted True, even when confidence and conversion_factor are valid."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        contradicted_units = {
            "normalized": "meter",
            "confidence": "confirmed",
            "conversion_target": "meter",
            "conversion_factor": 1.0,
            "contradiction": {
                "declared_normalized": "meter",
                "inferred_normalized": "millimeter",
            },
        }
        monkeypatch.setattr(
            worker_module,
            "run_ingestion",
            _fake_runner_with_canonical(units=contradicted_units, pdf_scale=None),
        )

        revision, _adapter_output = await self._ingest_revision(
            async_client, filename="plan.dxf", media_type="application/dxf"
        )

        body = (await async_client.get(f"/v1/revisions/{revision.id}/scale")).json()
        assert body["real_world_dimensions_available"] is False
        assert body["units_contradicted"] is True

    async def test_non_contradicted_units_remain_available(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The same units block WITHOUT contradiction → real_world_dimensions_available True,
        units_contradicted False. No regression against #557 confirmed behaviour."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        clean_units = {
            "normalized": "meter",
            "confidence": "confirmed",
            "conversion_target": "meter",
            "conversion_factor": 1.0,
        }
        monkeypatch.setattr(
            worker_module,
            "run_ingestion",
            _fake_runner_with_canonical(units=clean_units, pdf_scale=None),
        )

        revision, _adapter_output = await self._ingest_revision(
            async_client, filename="plan.dxf", media_type="application/dxf"
        )

        body = (await async_client.get(f"/v1/revisions/{revision.id}/scale")).json()
        assert body["real_world_dimensions_available"] is True
        assert body["units_contradicted"] is False

    async def test_pdf_signal_independent_of_units_contradiction(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A contradicted units block does NOT suppress a genuine PDF points_to_real signal (#558):
        real_world_dimensions_available stays True when PDF scale is detected."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        contradicted_units = {
            "normalized": "meter",
            "confidence": "confirmed",
            "conversion_target": "meter",
            "conversion_factor": 1.0,
            "contradiction": {
                "declared_normalized": "meter",
                "inferred_normalized": "millimeter",
            },
        }
        monkeypatch.setattr(
            worker_module,
            "run_ingestion",
            _fake_runner_with_canonical(units=contradicted_units, pdf_scale=_PDF_SCALE),
        )

        revision, _adapter_output = await self._ingest_revision(
            async_client, filename="plan.pdf", media_type="application/pdf"
        )

        body = (await async_client.get(f"/v1/revisions/{revision.id}/scale")).json()
        assert body["real_world_dimensions_available"] is True
        assert body["units_contradicted"] is True

    async def test_missing_revision_returns_not_found(
        self,
        async_client: httpx.AsyncClient,
    ) -> None:
        """An unknown revision id returns the standard not-found error envelope."""
        _ = self

        response = await async_client.get(f"/v1/revisions/{uuid.uuid4()}/scale")

        assert response.status_code == 404
        assert response.json()["error"]["code"] == ErrorCode.NOT_FOUND.value

    async def test_pdf_scale_nested_in_metadata_is_found(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """pdf_scale stored under canonical[metadata][pdf_scale] (pymupdf layout) is surfaced.

        The pymupdf adapter stores pdf_scale under canonical["metadata"]["pdf_scale"], not at
        the canonical top level.  Confirm the resolver finds it and returns it in the response.
        """
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        metadata_scale = {
            "status": "derived_from_text",
            "coordinate_space": "pdf_page_space_unrotated",
            "unit": "point",
            "real_world_units": True,
            "real_world_unit": "millimeter",
            "real_world_unit_source": "ALL DIMENSIONS ARE IN MILLIMETRES U.N.O.",
            "scale_ratio": {"numerator": 1, "denominator": 50, "text": "1:50"},
            "points_to_real": 17.638889,
            "confidence": "high",
        }

        async def _run(request: IngestionRunRequest) -> IngestFinalizationPayload:
            payload = _build_fake_ingest_payload(request)
            canonical = dict(payload.canonical_json)
            # Place pdf_scale under metadata (mirrors the real pymupdf adapter output).
            canonical["metadata"] = {"pdf_scale": metadata_scale}
            # Leave canonical["pdf_scale"] absent to confirm the nested lookup fires.
            return replace(payload, canonical_json=canonical, input_family="pdf_vector")

        monkeypatch.setattr(worker_module, "run_ingestion", _run)

        revision, _adapter_output = await self._ingest_revision(
            async_client, filename="plan.pdf", media_type="application/pdf"
        )

        response = await async_client.get(f"/v1/revisions/{revision.id}/scale")

        assert response.status_code == 200
        body = response.json()
        assert body["pdf_scale"] is not None
        assert body["pdf_scale"]["status"] == "derived_from_text"
        assert body["pdf_scale"]["points_to_real"] == pytest.approx(17.638889)
        assert body["real_world_dimensions_available"] is True

    async def test_pdf_derived_from_text_enriches_units_normalized_and_confidence(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """pdf_scale status=derived_from_text on a pdf_vector revision surfaces units.normalized
        and units_confidence=confirmed.

        With a high-confidence 1:50 mm derived scale, the resolver must:
        - set units.normalized = "millimeter" (the real_world_unit from the scale)
        - set units_confidence = "confirmed"
        - keep real_world_dimensions_available = True
        The DWG units block is left untouched (separately tested).
        """
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        derived_scale = {
            "status": "derived_from_text",
            "real_world_unit": "millimeter",
            "points_to_real": 17.638889,
            "confidence": "high",
        }

        monkeypatch.setattr(
            worker_module,
            "run_ingestion",
            _fake_runner_with_canonical(units={"normalized": "unknown"}, pdf_scale=derived_scale),
        )

        revision, _adapter_output = await self._ingest_revision(
            async_client, filename="plan.pdf", media_type="application/pdf"
        )

        response = await async_client.get(f"/v1/revisions/{revision.id}/scale")

        assert response.status_code == 200
        body = response.json()
        assert body["units"]["normalized"] == "millimeter"
        assert body["units_confidence"] == "confirmed"
        assert body["real_world_dimensions_available"] is True
        assert body["pdf_scale"]["points_to_real"] == pytest.approx(17.638889)
        assert body["source_input_family"] == "pdf_vector"

    async def test_pdf_unconfirmed_scale_leaves_units_unknown(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """pdf_scale status=unconfirmed does NOT enrich units: stays unknown, no real dimensions.

        ADR-004 honesty: when the adapter could not derive a confident scale, the resolver
        must not claim units are known or real-world dimensions available.
        """
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        unconfirmed_scale = {
            "status": "unconfirmed",
            "coordinate_space": "pdf_page_space_unrotated",
            "unit": "point",
            "real_world_units": False,
        }

        monkeypatch.setattr(
            worker_module,
            "run_ingestion",
            _fake_runner_with_canonical(
                units={"normalized": "unknown"}, pdf_scale=unconfirmed_scale
            ),
        )

        revision, _adapter_output = await self._ingest_revision(
            async_client, filename="plan.pdf", media_type="application/pdf"
        )

        response = await async_client.get(f"/v1/revisions/{revision.id}/scale")

        assert response.status_code == 200
        body = response.json()
        assert body["units"]["normalized"] == "unknown"
        assert body["units_confidence"] == "unknown"
        assert body["real_world_dimensions_available"] is False

    async def test_pdf_ambiguous_multi_scale_leaves_units_unknown(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """pdf_scale status=ambiguous_multi_scale (multiple scales, no single ratio) stays honest.

        A multi-scale sheet cannot emit a single reliable points_to_real factor; the resolver
        must not claim units are available for such a revision.
        """
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        ambiguous_scale = {
            "status": "ambiguous_multi_scale",
            "coordinate_space": "pdf_page_space_unrotated",
            "unit": "point",
            "real_world_units": False,
            "scale_ratio_candidates": ["1:50", "1:100"],
            "confidence": "low",
        }

        monkeypatch.setattr(
            worker_module,
            "run_ingestion",
            _fake_runner_with_canonical(units={"normalized": "unknown"}, pdf_scale=ambiguous_scale),
        )

        revision, _adapter_output = await self._ingest_revision(
            async_client, filename="plan.pdf", media_type="application/pdf"
        )

        response = await async_client.get(f"/v1/revisions/{revision.id}/scale")

        assert response.status_code == 200
        body = response.json()
        assert body["units"]["normalized"] == "unknown"
        assert body["units_confidence"] == "unknown"
        assert body["real_world_dimensions_available"] is False

    async def test_dwg_units_path_unchanged_by_pdf_enrichment(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A DWG/DXF revision is not affected by the PDF enrichment gate.

        Regression guard: when input_family != pdf_vector, units.normalized and
        units_confidence come exclusively from the DWG units block (INSUNITS path).
        The presence of a pdf_scale dict in the canonical payload must not bleed into
        the DWG resolution.
        """
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        monkeypatch.setattr(
            worker_module,
            "run_ingestion",
            _fake_runner_with_canonical(units=_DXF_UNITS, pdf_scale=None),
        )

        revision, _adapter_output = await self._ingest_revision(
            async_client, filename="plan.dxf", media_type="application/dxf"
        )

        response = await async_client.get(f"/v1/revisions/{revision.id}/scale")

        assert response.status_code == 200
        body = response.json()
        assert body["units"]["normalized"] == "millimeter"
        assert body["units"]["conversion_factor"] == 0.001
        assert body["units_confidence"] == "confirmed"
        assert body["real_world_dimensions_available"] is True
        assert body["pdf_scale"] is None


class TestResolvePdfScale:
    """Unit tests for _resolve_pdf_scale — no DB required."""

    def test_returns_none_when_both_absent(self) -> None:
        from app.api.v1.revision_routes.scale import _resolve_pdf_scale

        assert _resolve_pdf_scale({}) is None
        assert _resolve_pdf_scale({"units": {"normalized": "unknown"}}) is None

    def test_top_level_wins_over_metadata_nested(self) -> None:
        from app.api.v1.revision_routes.scale import _resolve_pdf_scale

        top = {"status": "derived_from_text", "points_to_real": 1.0}
        nested = {"status": "unconfirmed"}
        canonical = {"pdf_scale": top, "metadata": {"pdf_scale": nested}}
        result = _resolve_pdf_scale(canonical)
        assert result is not None
        assert result["status"] == "derived_from_text"

    def test_metadata_nested_found_when_top_absent(self) -> None:
        from app.api.v1.revision_routes.scale import _resolve_pdf_scale

        nested = {"status": "derived_from_text", "points_to_real": 17.638889}
        canonical = {"metadata": {"pdf_scale": nested}}
        result = _resolve_pdf_scale(canonical)
        assert result is not None
        assert result["status"] == "derived_from_text"
        assert result["points_to_real"] == pytest.approx(17.638889)

    def test_non_dict_top_level_falls_through_to_metadata(self) -> None:
        from app.api.v1.revision_routes.scale import _resolve_pdf_scale

        nested = {"status": "derived_from_text"}
        canonical = {"pdf_scale": "not_a_dict", "metadata": {"pdf_scale": nested}}
        result = _resolve_pdf_scale(canonical)
        assert result is not None
        assert result["status"] == "derived_from_text"


class TestEnrichUnitsFromPdfScale:
    """Unit tests for _enrich_units_from_pdf_scale — no DB required."""

    def test_enriches_when_all_gates_pass(self) -> None:
        from app.api.v1.revision_routes.scale import _enrich_units_from_pdf_scale

        units = {"normalized": "unknown"}
        pdf_scale = {
            "status": "derived_from_text",
            "real_world_unit": "millimeter",
            "points_to_real": 17.638889,
        }
        result = _enrich_units_from_pdf_scale(units, pdf_scale, "pdf_vector")
        assert result["normalized"] == "millimeter"
        assert result["confidence"] == "confirmed"

    def test_does_not_mutate_original_units(self) -> None:
        from app.api.v1.revision_routes.scale import _enrich_units_from_pdf_scale

        units: dict[str, Any] = {"normalized": "unknown"}
        pdf_scale = {
            "status": "derived_from_text",
            "real_world_unit": "millimeter",
        }
        _enrich_units_from_pdf_scale(units, pdf_scale, "pdf_vector")
        assert units == {"normalized": "unknown"}

    def test_no_enrichment_for_non_pdf_family(self) -> None:
        from app.api.v1.revision_routes.scale import _enrich_units_from_pdf_scale

        units = {"normalized": "unknown"}
        pdf_scale = {"status": "derived_from_text", "real_world_unit": "millimeter"}
        for family in ("dwg", "dxf", "dxf_vector", None):
            result = _enrich_units_from_pdf_scale(units, pdf_scale, family)
            assert result.get("normalized") == "unknown", f"enriched for family={family}"

    def test_no_enrichment_for_unconfirmed_status(self) -> None:
        from app.api.v1.revision_routes.scale import _enrich_units_from_pdf_scale

        units = {"normalized": "unknown"}
        for status in ("unconfirmed", "ambiguous_multi_scale", "not_set", None):
            pdf_scale: dict[str, Any] = {"real_world_unit": "millimeter"}
            if status is not None:
                pdf_scale["status"] = status
            result = _enrich_units_from_pdf_scale(units, pdf_scale, "pdf_vector")
            assert result.get("normalized") == "unknown", f"enriched for status={status}"

    def test_no_enrichment_for_unknown_real_world_unit(self) -> None:
        from app.api.v1.revision_routes.scale import _enrich_units_from_pdf_scale

        units = {"normalized": "unknown"}
        pdf_scale = {"status": "derived_from_text", "real_world_unit": "inch"}
        result = _enrich_units_from_pdf_scale(units, pdf_scale, "pdf_vector")
        assert result.get("normalized") == "unknown"

    def test_no_enrichment_when_pdf_scale_none(self) -> None:
        from app.api.v1.revision_routes.scale import _enrich_units_from_pdf_scale

        units = {"normalized": "unknown"}
        result = _enrich_units_from_pdf_scale(units, None, "pdf_vector")
        assert result == units
