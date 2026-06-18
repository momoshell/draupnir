"""Integration coverage for revision -> quantity -> estimate API flow."""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal
from typing import Any
from uuid import UUID

import httpx
import pytest

import app.jobs.worker as worker_module
from app.api.v1.revision_routes import estimates as estimates_routes_module
from app.api.v1.revision_routes import quantity_takeoffs as quantity_takeoffs_routes_module
from app.db import session as session_module
from app.ingestion.finalization import IngestFinalizationPayload
from app.ingestion.runner import IngestionRunRequest
from app.models.job import JobType
from tests.conftest import requires_database, truncate_projects_cascade_for_cleanup
from tests.jobs_test_helpers import _create_project, _get_job, _get_job_for_file, _upload_file
from tests.test_jobs import (
    _build_fake_quantity_ingest_payload,
    _get_estimate_versions_for_job,
    _get_latest_revision_for_file,
)
from tests.test_quantity_takeoff_api import (
    _build_estimate_request_body,
)


async def _create_rate_catalog_entry(
    async_client: httpx.AsyncClient,
    *,
    project_id: str,
    per_unit: str,
) -> dict[str, Any]:
    response = await async_client.post(
        "/v1/estimation/catalog/rates",
        json={
            "scope_type": "project",
            "project_id": project_id,
            "rate_key": "labour:revision-flow-install",
            "source": "integration-test",
            "metadata_json": {"flow": "revision-quantity-estimate"},
            "name": "Revision flow install labour",
            "item_type": "labour",
            "per_unit": per_unit,
            "currency": "GBP",
            "amount": "12.500000",
            "effective_from": "2026-01-01",
            "effective_to": None,
        },
    )
    assert response.status_code == 201
    return dict(response.json())


def _select_eligible_aggregate_quantity_item_payload(
    items: list[dict[str, Any]],
) -> dict[str, Any]:
    eligible_items = sorted(
        (item for item in items if item["item_kind"] == "aggregate" and item["value"] is not None),
        key=lambda item: (str(item["quantity_type"]), str(item["id"])),
    )
    assert eligible_items, "expected at least one API-visible eligible aggregate quantity item"
    return eligible_items[0]


def _money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


async def _reset_database_pool() -> None:
    """Drop DB connections opened by direct helpers before ASGI API calls."""

    await session_module.close_db()
    session_module.engine, session_module.AsyncSessionLocal = session_module._init_db_resources()


async def _truncate_and_reset_database_pool() -> None:
    await truncate_projects_cascade_for_cleanup()
    await _reset_database_pool()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("review_state", "validation_status", "quantity_gate", "trusted_totals"),
    [
        ("approved", "valid", "allowed", True),
        # Path B: a review-gated, untrusted revision flows end-to-end (takeoff
        # creation #495 → estimate creation #488) to a persisted estimate version.
        ("review_required", "needs_review", "review_gated", False),
    ],
)
@requires_database
async def test_revision_quantity_takeoff_to_estimate_create_and_read_api_flow(
    async_client: httpx.AsyncClient,
    enqueued_job_ids: list[str],
    monkeypatch: pytest.MonkeyPatch,
    review_state: str,
    validation_status: str,
    quantity_gate: str,
    trusted_totals: bool,
) -> None:
    await _truncate_and_reset_database_pool()
    _ = enqueued_job_ids
    published_job_ids: list[UUID] = []

    async def _run_quantity_ready_ingestion(
        request: IngestionRunRequest,
    ) -> IngestFinalizationPayload:
        return _build_fake_quantity_ingest_payload(
            request,
            review_state=review_state,
            validation_status=validation_status,
            quantity_gate=quantity_gate,
        )

    async def _capture_publish_job_enqueue_intent(
        job_id: UUID,
        *,
        publisher: Any = None,
        suppress_exceptions: bool = False,
    ) -> None:
        _ = (publisher, suppress_exceptions)
        published_job_ids.append(job_id)

    monkeypatch.setattr(worker_module, "run_ingestion", _run_quantity_ready_ingestion)
    monkeypatch.setattr(
        quantity_takeoffs_routes_module,
        "_publish_job_enqueue_intent",
        _capture_publish_job_enqueue_intent,
    )
    monkeypatch.setattr(
        estimates_routes_module,
        "_publish_job_enqueue_intent",
        _capture_publish_job_enqueue_intent,
    )

    project = await _create_project(async_client)
    uploaded = await _upload_file(async_client, project["id"])
    ingest_job = await _get_job_for_file(str(uploaded["id"]))
    await worker_module.process_ingest_job(ingest_job.id)
    revision = await _get_latest_revision_for_file(ingest_job.file_id)
    assert revision is not None
    await _reset_database_pool()

    quantity_response = await async_client.post(f"/v1/revisions/{revision.id}/quantity-takeoffs")

    assert quantity_response.status_code == 202
    quantity_job_id = UUID(quantity_response.json()["id"])
    assert quantity_response.json()["job_type"] == JobType.QUANTITY_TAKEOFF.value
    assert quantity_response.json()["base_revision_id"] == str(revision.id)
    await worker_module.process_quantity_takeoff_job(quantity_job_id)
    await _reset_database_pool()

    quantity_list_response = await async_client.get(
        f"/v1/revisions/{revision.id}/quantity-takeoffs"
    )
    assert quantity_list_response.status_code == 200
    quantity_list_body = quantity_list_response.json()
    assert quantity_list_body["next_cursor"] is None
    assert len(quantity_list_body["items"]) == 1
    quantity_takeoff = quantity_list_body["items"][0]
    assert quantity_takeoff["project_id"] == project["id"]
    assert quantity_takeoff["source_file_id"] == uploaded["id"]
    assert quantity_takeoff["drawing_revision_id"] == str(revision.id)
    assert quantity_takeoff["source_job_id"] == str(quantity_job_id)
    assert quantity_takeoff["source_job_type"] == JobType.QUANTITY_TAKEOFF.value
    # Path B 5a: quantity_gate / trusted_totals are no longer exposed in the response.
    assert "quantity_gate" not in quantity_takeoff
    assert "trusted_totals" not in quantity_takeoff

    quantity_read_response = await async_client.get(
        f"/v1/revisions/{revision.id}/quantity-takeoffs/{quantity_takeoff['id']}"
    )
    assert quantity_read_response.status_code == 200
    assert quantity_read_response.json()["id"] == quantity_takeoff["id"]

    quantity_items_response = await async_client.get(
        f"/v1/revisions/{revision.id}/quantity-takeoffs/{quantity_takeoff['id']}/items"
    )
    assert quantity_items_response.status_code == 200
    quantity_items_body = quantity_items_response.json()
    assert quantity_items_body["next_cursor"] is None
    quantity_item = _select_eligible_aggregate_quantity_item_payload(quantity_items_body["items"])
    assert quantity_item["quantity_takeoff_id"] == quantity_takeoff["id"]
    assert quantity_item["project_id"] == project["id"]
    assert quantity_item["drawing_revision_id"] == str(revision.id)
    assert "review_state" not in quantity_item
    assert quantity_item["validation_status"] == validation_status

    rate = await _create_rate_catalog_entry(
        async_client,
        project_id=project["id"],
        per_unit=quantity_item["unit"],
    )
    quantity_item_id = UUID(quantity_item["id"])
    estimate_request = _build_estimate_request_body(quantity_item_id=quantity_item_id)
    estimate_request["pricing"]["effective_date"] = "2026-05-20"
    estimate_request["assumptions"] = {"tax_rate": "0.20"}
    estimate_request["catalog_refs"] = [
        {
            "ref_type": "rate",
            "selection_id": rate["id"],
            "selection_key": rate["rate_key"],
            "selection_checksum_sha256": rate["checksum_sha256"],
            "description": "Revision flow install labour",
            "line_key": "line-rate-revision-flow",
            "quantity_item_id": quantity_item["id"],
        }
    ]

    estimate_response = await async_client.post(
        f"/v1/revisions/{revision.id}/quantity-takeoffs/{quantity_takeoff['id']}/estimate-versions",
        json=estimate_request,
    )

    assert estimate_response.status_code == 202
    estimate_job_id = UUID(estimate_response.json()["id"])
    assert estimate_response.json()["job_type"] == JobType.ESTIMATE.value
    assert estimate_response.json()["base_revision_id"] == str(revision.id)
    await worker_module.process_estimate_job(estimate_job_id)

    estimate_job = await _get_job(estimate_job_id)
    assert estimate_job.status == "succeeded"
    estimate_versions = await _get_estimate_versions_for_job(estimate_job_id)
    assert len(estimate_versions) == 1
    estimate_version = estimate_versions[0]
    await _reset_database_pool()

    list_response = await async_client.get(f"/v1/revisions/{revision.id}/estimates")
    assert list_response.status_code == 200
    list_body = list_response.json()
    assert list_body["next_cursor"] is None
    list_items = list_body["items"]
    assert [item["id"] for item in list_items] == [str(estimate_version.id)]
    listed_estimate = list_items[0]
    assert listed_estimate["project_id"] == project["id"]
    assert listed_estimate["source_file_id"] == uploaded["id"]
    assert listed_estimate["drawing_revision_id"] == str(revision.id)
    assert listed_estimate["quantity_takeoff_id"] == quantity_takeoff["id"]
    assert listed_estimate["source_job_id"] == str(estimate_job_id)
    assert "quantity_gate" not in listed_estimate
    assert "trusted_totals" not in listed_estimate
    assert listed_estimate["currency"] == "GBP"

    quantity_value = Decimal(str(quantity_item["value"]))
    rate_amount = Decimal(rate["amount"])
    expected_subtotal = _money(quantity_value * rate_amount)
    expected_tax = _money(expected_subtotal * Decimal("0.20"))
    expected_total = expected_subtotal + expected_tax
    assert Decimal(listed_estimate["subtotal_amount"]) == expected_subtotal
    assert Decimal(listed_estimate["tax_amount"]) == expected_tax
    assert Decimal(listed_estimate["total_amount"]) == expected_total

    read_response = await async_client.get(
        f"/v1/revisions/{revision.id}/estimates/{estimate_version.id}"
    )
    assert read_response.status_code == 200
    read_estimate = read_response.json()
    assert read_estimate == listed_estimate

    items_response = await async_client.get(
        f"/v1/revisions/{revision.id}/estimates/{estimate_version.id}/items"
    )
    assert items_response.status_code == 200
    estimate_items = items_response.json()["items"]
    assert items_response.json()["next_cursor"] is None
    assert len(estimate_items) == 1
    estimate_item = estimate_items[0]
    assert estimate_item["estimate_version_id"] == str(estimate_version.id)
    assert estimate_item["project_id"] == project["id"]
    assert estimate_item["drawing_revision_id"] == str(revision.id)
    assert estimate_item["line_type"] == "rate"
    assert estimate_item["line_key"] == "line-rate-revision-flow"
    assert estimate_item["description"] == "Revision flow install labour"
    assert estimate_item["currency"] == "GBP"
    assert Decimal(estimate_item["quantity_value"]) == quantity_value
    assert estimate_item["quantity_unit"] == quantity_item["unit"]
    assert Decimal(estimate_item["unit_rate_amount"]) == rate_amount
    assert estimate_item["effective_date"] == rate["effective_from"]
    assert Decimal(estimate_item["subtotal_amount"]) == expected_subtotal
    assert Decimal(estimate_item["tax_amount"]) == expected_tax
    assert Decimal(estimate_item["total_amount"]) == expected_total

    snapshot_response = await async_client.get(
        f"/v1/revisions/{revision.id}/estimates/{estimate_version.id}/snapshot-entries"
    )
    assert snapshot_response.status_code == 200
    assert snapshot_response.json()["next_cursor"] is None
    snapshot_entries = snapshot_response.json()["items"]
    assert snapshot_entries != []
    assert {entry["entry_type"] for entry in snapshot_entries} >= {"quantity_input", "rate"}
    quantity_snapshot = next(
        entry for entry in snapshot_entries if entry["entry_type"] == "quantity_input"
    )
    assert quantity_snapshot["source_quantity_takeoff_id"] == quantity_takeoff["id"]
    assert quantity_snapshot["source_quantity_item_id"] == quantity_item["id"]
    assert Decimal(quantity_snapshot["quantity_value"]) == quantity_value
    assert quantity_snapshot["unit"] == quantity_item["unit"]
    assert "source_checksum_sha256" in quantity_snapshot

    rate_snapshot = next(entry for entry in snapshot_entries if entry["entry_type"] == "rate")
    assert rate_snapshot["source_rate_id"] == rate["id"]
    assert rate_snapshot["source_checksum_sha256"] == rate["checksum_sha256"]
    assert rate_snapshot["currency"] == "GBP"
    assert rate_snapshot["unit"] == quantity_item["unit"]
    assert Decimal(rate_snapshot["unit_amount"]) == rate_amount
    assert rate_snapshot["effective_date"] == rate["effective_from"]
    assert published_job_ids == [quantity_job_id, estimate_job_id]
