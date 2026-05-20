"""Database-backed API tests for estimation catalog and formulas endpoints."""

from __future__ import annotations

import re
from datetime import date
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import httpx
from sqlalchemy import select

import app.db.session as session_module
from app.estimating.catalog.api_checksums import (
    formula_checksum_sha256,
    material_checksum_sha256,
    rate_checksum_sha256,
)
from app.models.project import Project
from tests.conftest import requires_database


def test_rate_checksum_is_deterministic_for_json_key_order_and_changes_with_content() -> None:
    # Arrange
    project_id = str(uuid4())
    payload = _valid_rate_payload(project_id=project_id)

    # Act
    checksum_a = rate_checksum_sha256(
        scope_type=payload["scope_type"],
        project_id=UUID(payload["project_id"]),
        rate_key=payload["rate_key"],
        source=payload["source"],
        metadata_json={"a": 1, "b": 2},
        name=payload["name"],
        item_type=payload["item_type"],
        per_unit=payload["per_unit"],
        currency=payload["currency"],
        amount=Decimal(payload["amount"]),
        effective_from=date.fromisoformat(payload["effective_from"]),
        effective_to=None,
    )
    checksum_b = rate_checksum_sha256(
        scope_type=payload["scope_type"],
        project_id=UUID(payload["project_id"]),
        rate_key=payload["rate_key"],
        source=payload["source"],
        metadata_json={"b": 2, "a": 1},
        name=payload["name"],
        item_type=payload["item_type"],
        per_unit=payload["per_unit"],
        currency=payload["currency"],
        amount=Decimal(payload["amount"]),
        effective_from=date.fromisoformat(payload["effective_from"]),
        effective_to=None,
    )
    checksum_changed = rate_checksum_sha256(
        scope_type=payload["scope_type"],
        project_id=UUID(payload["project_id"]),
        rate_key=payload["rate_key"],
        source=payload["source"],
        metadata_json={"a": 1, "b": 2},
        name=payload["name"],
        item_type=payload["item_type"],
        per_unit=payload["per_unit"],
        currency=payload["currency"],
        amount=Decimal("12.600000"),
        effective_from=date.fromisoformat(payload["effective_from"]),
        effective_to=None,
    )

    # Assert
    assert checksum_a == checksum_b
    assert checksum_a != checksum_changed


def test_material_checksum_is_deterministic_for_json_key_order_and_changes_with_content() -> None:
    # Arrange
    project_id = str(uuid4())
    payload = _valid_material_payload(project_id=project_id)

    # Act
    checksum_a = material_checksum_sha256(
        scope_type=payload["scope_type"],
        project_id=UUID(payload["project_id"]),
        material_key=payload["material_key"],
        source=payload["source"],
        metadata_json={"first": "x", "second": "y"},
        name=payload["name"],
        unit=payload["unit"],
        currency=payload["currency"],
        unit_cost=Decimal(payload["unit_cost"]),
        effective_from=date.fromisoformat(payload["effective_from"]),
        effective_to=None,
    )
    checksum_b = material_checksum_sha256(
        scope_type=payload["scope_type"],
        project_id=UUID(payload["project_id"]),
        material_key=payload["material_key"],
        source=payload["source"],
        metadata_json={"second": "y", "first": "x"},
        name=payload["name"],
        unit=payload["unit"],
        currency=payload["currency"],
        unit_cost=Decimal(payload["unit_cost"]),
        effective_from=date.fromisoformat(payload["effective_from"]),
        effective_to=None,
    )
    checksum_changed = material_checksum_sha256(
        scope_type=payload["scope_type"],
        project_id=UUID(payload["project_id"]),
        material_key=payload["material_key"],
        source=payload["source"],
        metadata_json={"first": "x", "second": "y"},
        name="PVC conduit 25mm",
        unit=payload["unit"],
        currency=payload["currency"],
        unit_cost=Decimal(payload["unit_cost"]),
        effective_from=date.fromisoformat(payload["effective_from"]),
        effective_to=None,
    )

    # Assert
    assert checksum_a == checksum_b
    assert checksum_a != checksum_changed


def test_formula_checksum_is_deterministic_for_json_key_order_and_changes_with_content() -> None:
    # Arrange
    project_id = str(uuid4())
    payload = _valid_formula_payload(project_id=project_id)
    declared_inputs_a = [
        {
            "name": "rate",
            "contract": {"kind": "rate", "currency": "GBP", "per_unit": "m"},
        },
        {"name": "quantity", "contract": {"kind": "quantity", "unit": "m"}},
    ]
    declared_inputs_b = [
        {
            "contract": {"per_unit": "m", "currency": "GBP", "kind": "rate"},
            "name": "rate",
        },
        {"contract": {"unit": "m", "kind": "quantity"}, "name": "quantity"},
    ]

    # Act
    checksum_a = formula_checksum_sha256(
        scope_type=payload["scope_type"],
        project_id=UUID(payload["project_id"]),
        formula_id=payload["formula_id"],
        version=payload["version"],
        name=payload["name"],
        dsl_version=payload["dsl_version"],
        output_key=payload["output_key"],
        output_contract_json={"kind": "money", "currency": "GBP"},
        declared_inputs_json=declared_inputs_a,
        expression_json=payload["expression_json"],
        rounding_json=payload["rounding_json"],
    )
    checksum_b = formula_checksum_sha256(
        scope_type=payload["scope_type"],
        project_id=UUID(payload["project_id"]),
        formula_id=payload["formula_id"],
        version=payload["version"],
        name=payload["name"],
        dsl_version=payload["dsl_version"],
        output_key=payload["output_key"],
        output_contract_json={"currency": "GBP", "kind": "money"},
        declared_inputs_json=declared_inputs_b,
        expression_json=payload["expression_json"],
        rounding_json={"mode": "ROUND_HALF_UP", "scale": 2},
    )
    checksum_changed = formula_checksum_sha256(
        scope_type=payload["scope_type"],
        project_id=UUID(payload["project_id"]),
        formula_id=payload["formula_id"],
        version=2,
        name=payload["name"],
        dsl_version=payload["dsl_version"],
        output_key=payload["output_key"],
        output_contract_json={"kind": "money", "currency": "GBP"},
        declared_inputs_json=declared_inputs_a,
        expression_json=payload["expression_json"],
        rounding_json=payload["rounding_json"],
    )

    # Assert
    assert checksum_a == checksum_b
    assert checksum_a != checksum_changed


def _missing_uuid() -> str:
    return "00000000-0000-0000-0000-000000000000"


def _assert_checksum_sha256(value: Any) -> None:
    assert isinstance(value, str)
    assert re.fullmatch(r"[0-9a-f]{64}", value) is not None


async def _create_project(async_client: httpx.AsyncClient, name: str) -> str:
    response = await async_client.post("/v1/projects", json={"name": name})
    assert response.status_code == 201
    project = response.json()
    return str(project["id"])


async def _mark_project_deleted(project_id: str) -> None:
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        project = (
            await session.execute(select(Project).where(Project.id == UUID(project_id)))
        ).scalar_one()
        project.deleted_at = project.updated_at
        await session.commit()


def _valid_rate_payload(*, project_id: str) -> dict[str, Any]:
    return {
        "scope_type": "project",
        "project_id": project_id,
        "rate_key": "labour:installer",
        "source": "manual",
        "metadata_json": {"region": "uk-south"},
        "name": "Installer labour",
        "item_type": "labour",
        "per_unit": "m",
        "currency": "GBP",
        "amount": "12.500000",
        "effective_from": "2026-01-01",
        "effective_to": None,
    }


def _valid_material_payload(*, project_id: str) -> dict[str, Any]:
    return {
        "scope_type": "project",
        "project_id": project_id,
        "material_key": "conduit:pvc-20mm",
        "source": "manual",
        "metadata_json": {"grade": "standard"},
        "name": "PVC conduit 20mm",
        "unit": "m",
        "currency": "GBP",
        "unit_cost": "3.250000",
        "effective_from": "2026-01-01",
        "effective_to": None,
    }


def _valid_formula_payload(*, project_id: str) -> dict[str, Any]:
    return {
        "scope_type": "project",
        "project_id": project_id,
        "formula_id": "line-total",
        "version": 1,
        "name": "Line total",
        "dsl_version": "1.0",
        "output_key": "line_total",
        "output_contract_json": {"kind": "money", "currency": "GBP"},
        "declared_inputs_json": [
            {
                "name": "rate",
                "contract": {"kind": "rate", "currency": "GBP", "per_unit": "m"},
            },
            {"name": "quantity", "contract": {"kind": "quantity", "unit": "m"}},
        ],
        "expression_json": {
            "kind": "multiply",
            "args": [
                {"kind": "input", "name": "rate"},
                {"kind": "input", "name": "quantity"},
            ],
        },
        "rounding_json": {"scale": 2, "mode": "ROUND_HALF_UP"},
    }


@requires_database
class TestEstimationCatalogApiHappyPath:
    async def test_rates_create_read_list(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        _ = self
        _ = cleanup_projects

        # Arrange
        project_id = await _create_project(async_client, name="Rate Project")
        payload = _valid_rate_payload(project_id=project_id)

        # Act
        create_response = await async_client.post("/v1/estimation/catalog/rates", json=payload)

        # Assert
        assert create_response.status_code == 201
        created = create_response.json()
        assert created["scope_type"] == "project"
        assert created["project_id"] == project_id
        assert created["rate_key"] == payload["rate_key"]
        assert created["item_type"] == payload["item_type"]
        assert created["per_unit"] == payload["per_unit"]
        assert created["currency"] == payload["currency"]
        assert created["amount"] == payload["amount"]
        _assert_checksum_sha256(created["checksum_sha256"])

        rate_id = created["id"]
        read_response = await async_client.get(f"/v1/estimation/catalog/rates/{rate_id}")
        assert read_response.status_code == 200
        read_rate = read_response.json()
        assert read_rate["id"] == rate_id
        assert read_rate["checksum_sha256"] == created["checksum_sha256"]

        list_response = await async_client.get("/v1/estimation/catalog/rates")
        assert list_response.status_code == 200
        items = list_response.json()["items"]
        assert any(item["id"] == rate_id for item in items)

    async def test_materials_create_read_list(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        _ = self
        _ = cleanup_projects

        # Arrange
        project_id = await _create_project(async_client, name="Material Project")
        payload = _valid_material_payload(project_id=project_id)

        # Act
        create_response = await async_client.post("/v1/estimation/catalog/materials", json=payload)

        # Assert
        assert create_response.status_code == 201
        created = create_response.json()
        assert created["scope_type"] == "project"
        assert created["project_id"] == project_id
        assert created["material_key"] == payload["material_key"]
        assert created["unit"] == payload["unit"]
        assert created["currency"] == payload["currency"]
        assert created["unit_cost"] == payload["unit_cost"]
        _assert_checksum_sha256(created["checksum_sha256"])

        material_id = created["id"]
        read_response = await async_client.get(f"/v1/estimation/catalog/materials/{material_id}")
        assert read_response.status_code == 200
        read_material = read_response.json()
        assert read_material["id"] == material_id
        assert read_material["checksum_sha256"] == created["checksum_sha256"]

        list_response = await async_client.get("/v1/estimation/catalog/materials")
        assert list_response.status_code == 200
        items = list_response.json()["items"]
        assert any(item["id"] == material_id for item in items)

    async def test_formulas_create_read_list(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        _ = self
        _ = cleanup_projects

        # Arrange
        project_id = await _create_project(async_client, name="Formula Project")
        payload = _valid_formula_payload(project_id=project_id)

        # Act
        create_response = await async_client.post("/v1/estimation/formulas", json=payload)

        # Assert
        assert create_response.status_code == 201
        created = create_response.json()
        assert created["scope_type"] == "project"
        assert created["project_id"] == project_id
        assert created["formula_id"] == payload["formula_id"]
        assert created["version"] == payload["version"]
        assert created["dsl_version"] == payload["dsl_version"]
        assert created["output_key"] == payload["output_key"]
        _assert_checksum_sha256(created["checksum_sha256"])

        formula_definition_id = created["id"]
        read_response = await async_client.get(f"/v1/estimation/formulas/{formula_definition_id}")
        assert read_response.status_code == 200
        read_formula = read_response.json()
        assert read_formula["id"] == formula_definition_id
        assert read_formula["checksum_sha256"] == created["checksum_sha256"]

        list_response = await async_client.get("/v1/estimation/formulas")
        assert list_response.status_code == 200
        items = list_response.json()["items"]
        assert any(item["id"] == formula_definition_id for item in items)


@requires_database
class TestEstimationCatalogApiErrorPaths:
    async def test_create_rate_rejects_malformed_scope_project_combination(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        _ = self
        _ = cleanup_projects

        payload = _valid_rate_payload(project_id=str(uuid4()))
        payload["scope_type"] = "global"

        response = await async_client.post("/v1/estimation/catalog/rates", json=payload)

        assert response.status_code == 422
        assert response.json()["error"]["code"] == "VALIDATION_ERROR"

    async def test_create_rate_rejects_metadata_json_float_with_validation_envelope(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        _ = self
        _ = cleanup_projects

        project_id = await _create_project(async_client, name="Rate Float Metadata Project")
        payload = _valid_rate_payload(project_id=project_id)
        payload["metadata_json"] = {"multiplier": 1.25}

        response = await async_client.post("/v1/estimation/catalog/rates", json=payload)

        assert response.status_code == 422
        assert response.json()["error"]["code"] == "VALIDATION_ERROR"

    async def test_create_rate_returns_not_found_for_missing_project(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        _ = self
        _ = cleanup_projects

        payload = _valid_rate_payload(project_id=_missing_uuid())

        response = await async_client.post("/v1/estimation/catalog/rates", json=payload)

        assert response.status_code == 404
        assert response.json()["error"]["code"] == "NOT_FOUND"

    async def test_create_material_returns_not_found_for_soft_deleted_project(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        _ = self
        _ = cleanup_projects

        project_id = await _create_project(async_client, name="Deleted Project")
        await _mark_project_deleted(project_id)
        payload = _valid_material_payload(project_id=project_id)

        response = await async_client.post("/v1/estimation/catalog/materials", json=payload)

        assert response.status_code == 404
        assert response.json()["error"]["code"] == "NOT_FOUND"

    async def test_create_material_rejects_metadata_json_float_with_validation_envelope(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        _ = self
        _ = cleanup_projects

        project_id = await _create_project(async_client, name="Material Float Metadata Project")
        payload = _valid_material_payload(project_id=project_id)
        payload["metadata_json"] = {"density": 2.5}

        response = await async_client.post("/v1/estimation/catalog/materials", json=payload)

        assert response.status_code == 422
        assert response.json()["error"]["code"] == "VALIDATION_ERROR"

    async def test_get_rate_returns_not_found_for_missing_resource(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        _ = self
        _ = cleanup_projects

        response = await async_client.get(f"/v1/estimation/catalog/rates/{_missing_uuid()}")

        assert response.status_code == 404
        assert response.json()["error"]["code"] == "NOT_FOUND"

    async def test_get_material_returns_not_found_for_missing_resource(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        _ = self
        _ = cleanup_projects

        response = await async_client.get(f"/v1/estimation/catalog/materials/{_missing_uuid()}")

        assert response.status_code == 404
        assert response.json()["error"]["code"] == "NOT_FOUND"

    async def test_get_formula_returns_not_found_for_missing_resource(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        _ = self
        _ = cleanup_projects

        response = await async_client.get(f"/v1/estimation/formulas/{_missing_uuid()}")

        assert response.status_code == 404
        assert response.json()["error"]["code"] == "NOT_FOUND"

    async def test_create_formula_rejects_legacy_invalid_dsl_before_persistence(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        _ = self
        _ = cleanup_projects

        project_id = await _create_project(async_client, name="Formula Validation Project")
        payload = _valid_formula_payload(project_id=project_id)
        payload["declared_inputs_json"] = [
            {
                "key": "rate",
                "contract": {"kind": "rate", "currency": "GBP", "per_unit": "m"},
            },
            {"name": "quantity", "contract": {"kind": "quantity", "unit": "m"}},
        ]
        payload["expression_json"] = {
            "op": "multiply",
            "args": [
                {"kind": "input", "name": "rate"},
                {"kind": "input", "name": "quantity"},
            ],
        }

        create_response = await async_client.post("/v1/estimation/formulas", json=payload)

        assert create_response.status_code == 422
        assert create_response.json()["error"]["code"] == "VALIDATION_ERROR"

        list_response = await async_client.get("/v1/estimation/formulas")
        assert list_response.status_code == 200
        assert list_response.json()["items"] == []


@requires_database
class TestEstimationCatalogApiRateIdempotency:
    async def test_create_rate_replays_same_response_for_same_idempotency_key_and_payload(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        _ = self
        _ = cleanup_projects

        project_id = await _create_project(async_client, name="Idempotency Replay Project")
        payload = _valid_rate_payload(project_id=project_id)
        headers = {"Idempotency-Key": "rate-create-replay-key"}

        first_response = await async_client.post(
            "/v1/estimation/catalog/rates",
            json=payload,
            headers=headers,
        )
        second_response = await async_client.post(
            "/v1/estimation/catalog/rates",
            json=payload,
            headers=headers,
        )

        assert first_response.status_code == 201
        assert second_response.status_code == 201
        first_body = first_response.json()
        second_body = second_response.json()
        assert first_body["id"] == second_body["id"]
        assert first_body["checksum_sha256"] == second_body["checksum_sha256"]
        assert first_body == second_body

    async def test_create_rate_returns_conflict_for_same_idempotency_key_and_different_payload(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        _ = self
        _ = cleanup_projects

        project_id = await _create_project(async_client, name="Idempotency Conflict Project")
        payload = _valid_rate_payload(project_id=project_id)
        conflicting_payload = _valid_rate_payload(project_id=project_id)
        conflicting_payload["name"] = "Installer labour changed"
        headers = {"Idempotency-Key": "rate-create-conflict-key"}

        first_response = await async_client.post(
            "/v1/estimation/catalog/rates",
            json=payload,
            headers=headers,
        )
        conflict_response = await async_client.post(
            "/v1/estimation/catalog/rates",
            json=conflicting_payload,
            headers=headers,
        )

        assert first_response.status_code == 201
        assert conflict_response.status_code == 409
        body = conflict_response.json()
        assert "error" in body
        assert isinstance(body["error"].get("code"), str)
        assert body["error"]["code"]


@requires_database
class TestEstimationCatalogApiSupersessionAndAsOf:
    async def test_rate_superseded_entry_hidden_by_default_and_included_when_requested(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        _ = self
        _ = cleanup_projects

        project_id = await _create_project(async_client, name="Rate Supersession Project")

        predecessor_payload = _valid_rate_payload(project_id=project_id)
        predecessor_payload["effective_to"] = "2026-03-01"
        predecessor_response = await async_client.post(
            "/v1/estimation/catalog/rates",
            json=predecessor_payload,
        )
        assert predecessor_response.status_code == 201
        predecessor_id = predecessor_response.json()["id"]

        successor_payload = _valid_rate_payload(project_id=project_id)
        successor_payload["name"] = "Installer labour v2"
        successor_payload["amount"] = "13.500000"
        successor_payload["effective_from"] = "2026-03-01"
        successor_payload["supersedes_rate_id"] = predecessor_id
        successor_response = await async_client.post(
            "/v1/estimation/catalog/rates",
            json=successor_payload,
        )
        assert successor_response.status_code == 201
        successor_id = successor_response.json()["id"]

        default_list_response = await async_client.get(
            "/v1/estimation/catalog/rates",
            params={"project_id": project_id, "rate_key": predecessor_payload["rate_key"]},
        )
        assert default_list_response.status_code == 200
        default_ids = {item["id"] for item in default_list_response.json()["items"]}
        assert successor_id in default_ids
        assert predecessor_id not in default_ids

        include_superseded_response = await async_client.get(
            "/v1/estimation/catalog/rates",
            params={
                "project_id": project_id,
                "rate_key": predecessor_payload["rate_key"],
                "include_superseded": "true",
            },
        )
        assert include_superseded_response.status_code == 200
        include_superseded_ids = {
            item["id"] for item in include_superseded_response.json()["items"]
        }
        assert predecessor_id in include_superseded_ids
        assert successor_id in include_superseded_ids

    async def test_rate_list_as_of_filters_by_effective_window(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        _ = self
        _ = cleanup_projects

        project_id = await _create_project(async_client, name="Rate As-Of Project")

        january_rate_payload = _valid_rate_payload(project_id=project_id)
        january_rate_payload["rate_key"] = "labour:jan"
        january_rate_payload["effective_from"] = "2026-01-01"
        january_rate_payload["effective_to"] = "2026-02-01"
        january_rate_response = await async_client.post(
            "/v1/estimation/catalog/rates",
            json=january_rate_payload,
        )
        assert january_rate_response.status_code == 201
        january_rate_id = january_rate_response.json()["id"]

        march_rate_payload = _valid_rate_payload(project_id=project_id)
        march_rate_payload["rate_key"] = "labour:mar"
        march_rate_payload["name"] = "March installer labour"
        march_rate_payload["effective_from"] = "2026-03-01"
        march_rate_payload["effective_to"] = None
        march_rate_response = await async_client.post(
            "/v1/estimation/catalog/rates",
            json=march_rate_payload,
        )
        assert march_rate_response.status_code == 201
        march_rate_id = march_rate_response.json()["id"]

        as_of_response = await async_client.get(
            "/v1/estimation/catalog/rates",
            params={"project_id": project_id, "as_of": "2026-01-15"},
        )
        assert as_of_response.status_code == 200
        as_of_ids = {item["id"] for item in as_of_response.json()["items"]}
        assert january_rate_id in as_of_ids
        assert march_rate_id not in as_of_ids

    async def test_material_list_as_of_filters_by_effective_window(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        _ = self
        _ = cleanup_projects

        project_id = await _create_project(async_client, name="Material As-Of Project")

        january_material_payload = _valid_material_payload(project_id=project_id)
        january_material_payload["material_key"] = "material:jan"
        january_material_payload["effective_from"] = "2026-01-01"
        january_material_payload["effective_to"] = "2026-02-01"
        january_material_response = await async_client.post(
            "/v1/estimation/catalog/materials",
            json=january_material_payload,
        )
        assert january_material_response.status_code == 201
        january_material_id = january_material_response.json()["id"]

        march_material_payload = _valid_material_payload(project_id=project_id)
        march_material_payload["material_key"] = "material:mar"
        march_material_payload["name"] = "March PVC conduit"
        march_material_payload["effective_from"] = "2026-03-01"
        march_material_payload["effective_to"] = None
        march_material_response = await async_client.post(
            "/v1/estimation/catalog/materials",
            json=march_material_payload,
        )
        assert march_material_response.status_code == 201
        march_material_id = march_material_response.json()["id"]

        as_of_response = await async_client.get(
            "/v1/estimation/catalog/materials",
            params={"project_id": project_id, "as_of": "2026-01-15"},
        )
        assert as_of_response.status_code == 200
        as_of_ids = {item["id"] for item in as_of_response.json()["items"]}
        assert january_material_id in as_of_ids
        assert march_material_id not in as_of_ids


@requires_database
class TestEstimationCatalogApiConflicts:
    async def test_create_formula_duplicate_payload_returns_db_conflict(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        _ = self
        _ = cleanup_projects

        project_id = await _create_project(async_client, name="Formula Conflict Project")
        payload = _valid_formula_payload(project_id=project_id)

        first_response = await async_client.post("/v1/estimation/formulas", json=payload)
        conflict_response = await async_client.post("/v1/estimation/formulas", json=payload)

        assert first_response.status_code == 201
        assert conflict_response.status_code == 409
        assert conflict_response.json()["error"]["code"] == "DB_CONFLICT"

    async def test_create_rate_duplicate_payload_returns_db_conflict(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        _ = self
        _ = cleanup_projects

        project_id = await _create_project(async_client, name="Rate Conflict Project")
        payload = _valid_rate_payload(project_id=project_id)

        first_response = await async_client.post("/v1/estimation/catalog/rates", json=payload)
        conflict_response = await async_client.post("/v1/estimation/catalog/rates", json=payload)

        assert first_response.status_code == 201
        assert conflict_response.status_code == 409
        assert conflict_response.json()["error"]["code"] == "DB_CONFLICT"
