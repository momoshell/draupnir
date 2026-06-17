"""Estimate input helpers extracted from revisions router."""

import importlib
import pkgutil
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from math import isfinite
from typing import Any
from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import ErrorCode
from app.core.exceptions import create_error_response
from app.models.drawing_revision import DrawingRevision
from app.models.job import Job, JobType
from app.models.quantity_takeoff import (
    QuantityItem,
    QuantityItemKind,
    QuantityTakeoff,
)
from app.schemas.estimate import EstimateVersionCreateCatalogRef, EstimateVersionCreateRequest

_APP_MODEL_MODULES_LOADED = False


@dataclass(slots=True)
class _NormalizedEstimateCatalogRef:
    ref_type: str
    selection_id: UUID
    selection_key: str
    selection_checksum_sha256: str
    description: str
    line_key: str
    quantity_item_id: UUID | None
    formula_inputs: dict[str, Any] | None


def _raise_estimate_input_invalid(
    message: str,
    *,
    details: dict[str, Any] | None = None,
) -> None:
    """Raise the standard estimate input validation error."""

    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=create_error_response(
            code=ErrorCode.INPUT_INVALID,
            message=message,
            details=details,
        ),
    )


def _raise_estimate_takeoff_gate_invalid(
    takeoff: QuantityTakeoff,
    *,
    raise_estimate_input_invalid: Callable[..., None] = _raise_estimate_input_invalid,
) -> None:
    """Raise the standard pre-enqueue error for non-runnable estimate inputs."""

    raise_estimate_input_invalid(
        "Estimate jobs require a trusted quantity takeoff with an allowed quantity gate.",
        details={
            "quantity_gate": takeoff.quantity_gate,
            "trusted_totals": takeoff.trusted_totals,
            "review_state": takeoff.review_state,
            "validation_status": takeoff.validation_status,
        },
    )


def _normalize_estimate_request_body(payload: EstimateVersionCreateRequest) -> dict[str, Any]:
    """Return the normalized create-request body used for idempotency fingerprints."""

    return payload.model_dump(mode="json", by_alias=False, exclude_none=False)


def _mapped_attribute_keys(model_class: type[Any]) -> set[str]:
    """Return mapped SQLAlchemy attribute keys for a model class."""

    return {attribute.key for attribute in model_class.__mapper__.attrs}


def _build_mapped_instance(
    model_class: type[Any],
    values: dict[str, Any],
    *,
    mapped_attribute_keys: Callable[[type[Any]], set[str]] = _mapped_attribute_keys,
) -> Any:
    """Instantiate a mapped object while ignoring unknown compatibility fields."""

    instance = model_class()
    valid_keys = mapped_attribute_keys(model_class)
    for key, value in values.items():
        if key in valid_keys:
            setattr(instance, key, value)
    return instance


def _first_present_attribute(instance: Any, names: tuple[str, ...]) -> Any:
    """Return the first present attribute value from a candidate name list."""

    for name in names:
        if hasattr(instance, name):
            return getattr(instance, name)
    return None


def _load_app_model_modules() -> None:
    """Import app model modules so mapped classes are registered."""

    import app.models as models_package

    global _APP_MODEL_MODULES_LOADED

    if not _APP_MODEL_MODULES_LOADED:
        for module_info in pkgutil.iter_modules(models_package.__path__):
            importlib.import_module(f"{models_package.__name__}.{module_info.name}")
        _APP_MODEL_MODULES_LOADED = True


def _resolve_estimate_job_model_classes(
    *,
    load_app_model_modules: Callable[[], None] = _load_app_model_modules,
) -> tuple[type[Any], type[Any]]:
    """Resolve mapped model classes used to persist estimate job inputs."""

    load_app_model_modules()
    class_map = {mapper.class_.__name__: mapper.class_ for mapper in Job.registry.mappers}
    input_model = class_map.get("EstimateJobInput")
    ref_model = class_map.get("EstimateJobInputCatalogRef")
    if input_model is None or ref_model is None:
        raise RuntimeError("Estimate job input models are unavailable")
    return input_model, ref_model


def _resolve_catalog_model(
    ref_type: str,
    *,
    load_app_model_modules: Callable[[], None] = _load_app_model_modules,
) -> type[Any]:
    """Resolve the mapped catalog/formula model for a request ref type."""

    load_app_model_modules()
    candidates: dict[str, list[type[Any]]] = {"rate": [], "material": [], "formula": []}
    for mapper in Job.registry.mappers:
        model_class = mapper.class_
        name = model_class.__name__.lower()
        table_name = getattr(model_class, "__tablename__", "").lower()
        if "estimatejobinput" in name or "quantity" in name:
            continue
        if "supersession" in name or "supersession" in table_name:
            continue
        if (
            "catalog" in name
            or "catalog" in table_name
            or "formula" in name
            or "formula" in table_name
        ):
            if "rate" in name or "rate" in table_name:
                candidates["rate"].append(model_class)
            if "material" in name or "material" in table_name:
                candidates["material"].append(model_class)
            if "formula" in name or "formula" in table_name:
                candidates["formula"].append(model_class)

    matches = candidates.get(ref_type, [])
    if not matches:
        raise RuntimeError(f"Catalog model for ref type '{ref_type}' is unavailable")

    def _model_score(model_class: type[Any]) -> tuple[int, int]:
        name = model_class.__name__.lower()
        table_name = getattr(model_class, "__tablename__", "").lower()
        return (
            int("catalog" in name or "catalog" in table_name),
            int(ref_type in name or ref_type in table_name),
        )

    return sorted(matches, key=_model_score, reverse=True)[0]


def _resolve_catalog_supersession_model(
    ref_type: str,
    *,
    load_app_model_modules: Callable[[], None] = _load_app_model_modules,
) -> type[Any]:
    """Resolve the mapped supersession model for a request ref type."""

    load_app_model_modules()
    for mapper in Job.registry.mappers:
        model_class = mapper.class_
        name = model_class.__name__.lower()
        table_name = getattr(model_class, "__tablename__", "").lower()
        if "supersession" not in name and "supersession" not in table_name:
            continue
        if ref_type == "rate" and "rate" in name:
            return model_class
        if ref_type == "material" and "material" in name:
            return model_class
        if ref_type == "formula" and "formula" in name:
            return model_class
    raise RuntimeError(f"Catalog supersession model for ref type '{ref_type}' is unavailable")


async def _catalog_selection_is_superseded(
    ref_type: str,
    selection_id: UUID,
    db: AsyncSession,
    *,
    resolve_catalog_supersession_model: Callable[
        [str], type[Any]
    ] = _resolve_catalog_supersession_model,
) -> bool:
    """Return whether a catalog selection has been superseded."""

    supersession_model = resolve_catalog_supersession_model(ref_type)
    predecessor_field_name = {
        "rate": "predecessor_rate_id",
        "material": "predecessor_material_id",
        "formula": "predecessor_formula_id",
    }[ref_type]
    result = await db.execute(
        select(supersession_model).where(
            getattr(supersession_model, predecessor_field_name) == selection_id
        )
    )
    return result.scalar_one_or_none() is not None


def _resolve_estimate_pricing_contract(
    request: EstimateVersionCreateRequest,
    *,
    job_created_at: datetime | None = None,
) -> tuple[str, date]:
    """Resolve the persisted pricing contract for an estimate request."""

    if request.pricing.effective_date is not None:
        return "explicit", request.pricing.effective_date
    resolved_at = job_created_at or datetime.now(UTC)
    return "derived_from_job_created_at_utc", resolved_at.date()


def _quantity_item_is_executable(quantity_item: QuantityItem) -> bool:
    """Return whether a quantity item can back a rate/material estimate ref."""

    if quantity_item.item_kind not in {
        QuantityItemKind.CONTRIBUTOR.value,
        QuantityItemKind.AGGREGATE.value,
    }:
        return False

    value = quantity_item.value
    return value is not None and isfinite(value)


async def _get_estimate_catalog_selection(
    ref_type: str,
    selection_id: UUID,
    db: AsyncSession,
    *,
    resolve_catalog_model: Callable[[str], type[Any]] = _resolve_catalog_model,
) -> Any | None:
    """Return the selected catalog/formula row for a request ref."""

    model_class = resolve_catalog_model(ref_type)
    result = await db.execute(select(model_class).where(model_class.id == selection_id))
    return result.scalar_one_or_none()


async def _resolve_estimate_catalog_refs(
    revision: DrawingRevision,
    takeoff: QuantityTakeoff,
    request_refs: list[EstimateVersionCreateCatalogRef],
    pricing_effective_date: date,
    db: AsyncSession,
    *,
    raise_estimate_input_invalid: Callable[..., None] = _raise_estimate_input_invalid,
    get_estimate_catalog_selection: Callable[
        [str, UUID, AsyncSession], Awaitable[Any | None]
    ] = _get_estimate_catalog_selection,
    catalog_selection_is_superseded: Callable[
        [str, UUID, AsyncSession], Awaitable[bool]
    ] = _catalog_selection_is_superseded,
    first_present_attribute: Callable[[Any, tuple[str, ...]], Any] = _first_present_attribute,
    quantity_item_is_executable: Callable[[QuantityItem], bool] = _quantity_item_is_executable,
) -> list[_NormalizedEstimateCatalogRef]:
    """Validate request refs and normalize worker mapping inputs."""

    if not request_refs:
        raise_estimate_input_invalid(
            "Estimate jobs require at least one catalog or formula reference.",
            details={"catalog_refs": []},
        )

    normalized_refs: list[_NormalizedEstimateCatalogRef] = []
    seen_line_keys: set[str] = set()
    for request_ref in request_refs:
        if request_ref.ref_type not in {"rate", "material", "formula"}:
            raise_estimate_input_invalid(
                "Estimate refs must use supported ref types.",
                details={
                    "ref_type": request_ref.ref_type,
                    "selection_id": str(request_ref.selection_id),
                },
            )

        selection = await get_estimate_catalog_selection(
            request_ref.ref_type,
            request_ref.selection_id,
            db,
        )
        if selection is None:
            raise_estimate_input_invalid(
                "Estimate refs must point to visible catalog selections.",
                details={
                    "ref_type": request_ref.ref_type,
                    "selection_id": str(request_ref.selection_id),
                },
            )

        selection_project_id = getattr(selection, "project_id", None)
        if selection_project_id not in {None, revision.project_id}:
            raise_estimate_input_invalid(
                "Estimate refs must belong to the same project lineage as the revision.",
                details={
                    "ref_type": request_ref.ref_type,
                    "selection_id": str(request_ref.selection_id),
                },
            )

        if await catalog_selection_is_superseded(
            request_ref.ref_type, request_ref.selection_id, db
        ):
            raise_estimate_input_invalid(
                "Estimate refs must point to current catalog selections.",
                details={
                    "ref_type": request_ref.ref_type,
                    "selection_id": str(request_ref.selection_id),
                },
            )

        if getattr(selection, "deleted_at", None) is not None:
            raise_estimate_input_invalid(
                "Estimate refs must point to visible catalog selections.",
                details={
                    "ref_type": request_ref.ref_type,
                    "selection_id": str(request_ref.selection_id),
                },
            )

        selection_key = first_present_attribute(
            selection,
            (
                "selection_key",
                "entry_key",
                "key",
                "formula_key",
                "formula_id",
                "rate_key",
                "material_key",
            ),
        )
        if selection_key is not None and str(selection_key) != request_ref.selection_key:
            raise_estimate_input_invalid(
                "Estimate refs must use the current catalog selection key.",
                details={
                    "ref_type": request_ref.ref_type,
                    "selection_id": str(request_ref.selection_id),
                    "selection_key": request_ref.selection_key,
                },
            )

        selection_checksum = first_present_attribute(
            selection,
            ("checksum_sha256", "source_checksum_sha256"),
        )
        if (
            selection_checksum is not None
            and str(selection_checksum) != request_ref.selection_checksum_sha256
        ):
            raise_estimate_input_invalid(
                "Estimate refs must use the current catalog selection checksum.",
                details={
                    "ref_type": request_ref.ref_type,
                    "selection_id": str(request_ref.selection_id),
                    "selection_checksum_sha256": request_ref.selection_checksum_sha256,
                },
            )

        selection_currency = first_present_attribute(selection, ("currency",))
        if selection_currency is not None and str(selection_currency).upper() != "GBP":
            raise_estimate_input_invalid(
                "Estimate jobs only support GBP pricing inputs.",
                details={
                    "ref_type": request_ref.ref_type,
                    "selection_id": str(request_ref.selection_id),
                    "currency": str(selection_currency),
                },
            )

        effective_from = first_present_attribute(selection, ("effective_from",))
        effective_to = first_present_attribute(selection, ("effective_to",))
        if effective_from is not None and (
            pricing_effective_date < effective_from
            or (effective_to is not None and pricing_effective_date >= effective_to)
        ):
            raise_estimate_input_invalid(
                "Estimate refs must be effective for the resolved pricing date.",
                details={
                    "ref_type": request_ref.ref_type,
                    "selection_id": str(request_ref.selection_id),
                    "pricing_effective_date": pricing_effective_date.isoformat(),
                },
            )

        if request_ref.ref_type in {"rate", "material"}:
            if request_ref.quantity_item_id is None:
                raise_estimate_input_invalid(
                    "Rate and material refs require a quantity item.",
                    details={
                        "ref_type": request_ref.ref_type,
                        "selection_id": str(request_ref.selection_id),
                    },
                )

            quantity_item_result = await db.execute(
                select(QuantityItem).where(
                    (QuantityItem.id == request_ref.quantity_item_id)
                    & (QuantityItem.quantity_takeoff_id == takeoff.id)
                    & (QuantityItem.project_id == revision.project_id)
                    & (QuantityItem.drawing_revision_id == revision.id)
                )
            )
            quantity_item = quantity_item_result.scalar_one_or_none()
            if quantity_item is None or not quantity_item_is_executable(quantity_item):
                raise_estimate_input_invalid(
                    "Rate and material refs must target quantity items on the requested takeoff.",
                    details={
                        "ref_type": request_ref.ref_type,
                        "selection_id": str(request_ref.selection_id),
                        "quantity_item_id": str(request_ref.quantity_item_id),
                    },
                )

        if request_ref.ref_type == "formula" and request_ref.formula_inputs is None:
            raise_estimate_input_invalid(
                "Formula refs require formula_inputs.",
                details={
                    "ref_type": request_ref.ref_type,
                    "selection_id": str(request_ref.selection_id),
                },
            )

        line_key = request_ref.line_key or f"{request_ref.ref_type}:{request_ref.selection_key}"
        if line_key in seen_line_keys:
            raise_estimate_input_invalid(
                "Estimate refs must use unique line keys.",
                details={"line_key": line_key},
            )
        seen_line_keys.add(line_key)

        normalized_refs.append(
            _NormalizedEstimateCatalogRef(
                ref_type=request_ref.ref_type,
                selection_id=request_ref.selection_id,
                selection_key=request_ref.selection_key,
                selection_checksum_sha256=request_ref.selection_checksum_sha256,
                description=request_ref.description,
                line_key=line_key,
                quantity_item_id=request_ref.quantity_item_id,
                formula_inputs=request_ref.formula_inputs,
            )
        )

    return normalized_refs


def _build_estimate_job_input_payload(
    estimate_job: Job,
    revision: DrawingRevision,
    takeoff: QuantityTakeoff,
    request: EstimateVersionCreateRequest,
    normalized_refs: list[_NormalizedEstimateCatalogRef],
    *,
    pricing_mode: str,
    pricing_effective_date: date,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Build persistence payloads for estimate job input rows."""

    input_payload = {
        "estimate_job_id": estimate_job.id,
        "project_id": revision.project_id,
        "source_file_id": revision.source_file_id,
        "drawing_revision_id": revision.id,
        "quantity_takeoff_id": takeoff.id,
        "source_job_type": JobType.ESTIMATE.value,
        "currency": request.pricing.currency,
        "quantity_gate": takeoff.quantity_gate,
        "trusted_totals": takeoff.trusted_totals,
        "pricing_mode": pricing_mode,
        "pricing_effective_date": pricing_effective_date,
        "assumptions_json": request.assumptions,
    }

    ref_payloads: list[dict[str, Any]] = []
    for ref_order, ref in enumerate(normalized_refs, start=1):
        selection_context_json = {
            "worker_mapping_version": "estimate-line-v1",
            "line_number": ref_order,
            "line_key": ref.line_key,
            "line_type": ref.ref_type,
            "ref_type": ref.ref_type,
            "description": ref.description,
            "catalog_entry_key": f"{ref.ref_type}:{ref.selection_key}",
            "formula_inputs": ref.formula_inputs,
        }
        if ref.quantity_item_id is not None:
            selection_context_json["quantity_item_id"] = str(ref.quantity_item_id)

        ref_payloads.append(
            {
                "estimate_job_id": estimate_job.id,
                "ref_order": ref_order,
                "ref_type": ref.ref_type,
                "selection_key": ref.selection_key,
                "catalog_checksum_sha256": ref.selection_checksum_sha256,
                "selection_context_json": selection_context_json,
                "rate_catalog_entry_id": ref.selection_id if ref.ref_type == "rate" else None,
                "material_catalog_entry_id": (
                    ref.selection_id if ref.ref_type == "material" else None
                ),
                "formula_definition_id": ref.selection_id if ref.ref_type == "formula" else None,
            }
        )

    return input_payload, ref_payloads


__all__ = [
    "_NormalizedEstimateCatalogRef",
    "_build_estimate_job_input_payload",
    "_build_mapped_instance",
    "_catalog_selection_is_superseded",
    "_first_present_attribute",
    "_get_estimate_catalog_selection",
    "_load_app_model_modules",
    "_mapped_attribute_keys",
    "_normalize_estimate_request_body",
    "_quantity_item_is_executable",
    "_raise_estimate_input_invalid",
    "_raise_estimate_takeoff_gate_invalid",
    "_resolve_catalog_model",
    "_resolve_catalog_supersession_model",
    "_resolve_estimate_catalog_refs",
    "_resolve_estimate_job_model_classes",
    "_resolve_estimate_pricing_contract",
]
