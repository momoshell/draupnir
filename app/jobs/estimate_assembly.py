"""Deterministic estimate-engine input assembly for the estimate worker job.

Extracted from ``app.jobs.worker`` (issue #387). The assembly loads the claimed
estimate job's persisted inputs, resolves catalog references through the injected
``WorkerDeps`` collaborators, and composes a validated ``EstimateEngineInput``.
It performs no writes; finalization persists the engine output separately.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Callable
from copy import deepcopy
from decimal import Decimal, InvalidOperation
from typing import Any, cast
from uuid import UUID

from sqlalchemy import select

from app.db.session import get_session_maker
from app.estimating.catalog import CatalogFormulaRef, CatalogMaterialRef, CatalogRateRef
from app.estimating.catalog.selection import CatalogSelectionError
from app.estimating.engine import formula_definition_from_selected_formula
from app.estimating.engine.contracts import (
    EstimateAssumptionEntryInput,
    EstimateEngineInput,
    EstimateFormulaEntryInput,
    EstimateLineInputSpec,
    EstimateMaterialEntryInput,
    EstimateQuantityEntryInput,
    EstimateRateEntryInput,
)
from app.jobs.estimate_execution_input import (
    validate_estimate_input,
    validate_estimate_job,
    validate_quantity_takeoff,
)
from app.jobs.estimate_mapping import (
    _build_estimate_job_input_error,
    _build_estimate_worker_mapping_v1,
    _estimate_formula_binding_tokens,
    _EstimateWorkerLineInput,
    _resolve_formula_binding_snapshot_key,
)
from app.jobs.worker_deps import WorkerDeps
from app.models.estimate_job_input import EstimateJobInput, EstimateJobInputCatalogRef
from app.models.job import Job
from app.models.quantity_takeoff import QuantityItem, QuantityItemKind, QuantityTakeoff


def _estimate_input_checksum(payload: dict[str, Any]) -> str:
    """Build a deterministic checksum for worker-synthesized estimate inputs."""
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _estimate_decimal(
    value: Any,
    *,
    reason: str,
    extra_details: dict[str, Any] | None = None,
) -> Decimal:
    """Parse one persisted estimate numeric input as a finite Decimal."""
    if isinstance(value, bool):
        raise _build_estimate_job_input_error(reason, extra_details=extra_details)
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise _build_estimate_job_input_error(reason, extra_details=extra_details) from exc
    if not decimal_value.is_finite():
        raise _build_estimate_job_input_error(reason, extra_details=extra_details)
    return decimal_value


def _estimate_tax_rate(assumptions_json: dict[str, Any]) -> Decimal:
    """Load the persisted estimate tax rate with a default of zero."""
    raw_tax_rate = assumptions_json.get("tax_rate", 0)
    tax_rate = _estimate_decimal(
        raw_tax_rate,
        reason="invalid_tax_rate",
        extra_details={"field": "tax_rate"},
    )
    if tax_rate < 0:
        raise _build_estimate_job_input_error(
            "invalid_tax_rate",
            extra_details={"field": "tax_rate"},
        )
    return tax_rate


def _build_estimate_assumption_entries(
    assumptions_json: dict[str, Any],
    *,
    next_sort_order: Callable[[], int],
) -> list[EstimateAssumptionEntryInput]:
    """Build deterministic assumption snapshot entries from ``assumptions_json["inputs"]``.

    Named assumption values are the source for formula ``scalar``/``money`` declared
    inputs; each becomes a snapshot entry keyed ``assumption:<name>`` that the engine
    resolves to a scalar/money ``FormulaValue``. Sorted by name for determinism.
    """
    raw_inputs = assumptions_json.get("inputs", {})
    if not isinstance(raw_inputs, dict):
        raise _build_estimate_job_input_error(
            "invalid_assumption_inputs",
            extra_details={"field": "assumptions.inputs"},
        )

    entries: list[EstimateAssumptionEntryInput] = []
    for name in sorted(raw_inputs):
        spec = raw_inputs[name]
        if not isinstance(spec, dict):
            raise _build_estimate_job_input_error(
                "invalid_assumption_input",
                extra_details={"assumption": name},
            )
        kind = spec.get("kind")
        if kind not in ("scalar", "money"):
            raise _build_estimate_job_input_error(
                "invalid_assumption_input_kind",
                extra_details={"assumption": name, "kind": kind},
            )
        amount = _estimate_decimal(
            spec.get("amount"),
            reason="invalid_assumption_amount",
            extra_details={"assumption": name},
        )
        if amount < 0:
            raise _build_estimate_job_input_error(
                "invalid_assumption_amount",
                extra_details={"assumption": name},
            )
        if kind == "money" and spec.get("currency", "GBP") != "GBP":
            raise _build_estimate_job_input_error(
                "unsupported_assumption_currency",
                extra_details={"assumption": name, "currency": spec.get("currency")},
            )
        payload: dict[str, Any] = {"kind": kind, "amount": str(amount)}
        if kind == "money":
            payload["currency"] = "GBP"
        entries.append(
            EstimateAssumptionEntryInput(
                entry_key=f"assumption:{name}",
                entry_label=name,
                sort_order=next_sort_order(),
                source_checksum_sha256=_estimate_input_checksum(payload),
                amount=amount,
                source_payload=payload,
                kind=cast(Any, kind),
            )
        )
    return entries


async def _build_estimate_engine_input(
    job_id: UUID,
    *,
    attempt_token: UUID,
    deps: WorkerDeps,
) -> EstimateEngineInput:
    """Load deterministic engine inputs for a claimed persisted estimate job."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        job = validate_estimate_job(
            await session.get(Job, job_id),
            attempt_token=attempt_token,
            job_id=job_id,
        )
        estimate_input = validate_estimate_input(
            await session.get(EstimateJobInput, job.id),
            job=job,
        )
        quantity_takeoff = validate_quantity_takeoff(
            await session.get(QuantityTakeoff, estimate_input.quantity_takeoff_id),
            estimate_input=estimate_input,
            job=job,
        )

        catalog_refs_result = await session.execute(
            select(EstimateJobInputCatalogRef)
            .where(EstimateJobInputCatalogRef.estimate_job_id == job.id)
            .order_by(
                EstimateJobInputCatalogRef.ref_order.asc(),
                EstimateJobInputCatalogRef.ref_type.asc(),
                EstimateJobInputCatalogRef.selection_key.asc(),
            )
        )
        catalog_refs = list(catalog_refs_result.scalars().all())
        assembly = _build_estimate_worker_mapping_v1(catalog_refs)

        quantity_items_result = await session.execute(
            select(QuantityItem)
            .where(QuantityItem.quantity_takeoff_id == quantity_takeoff.id)
            .order_by(QuantityItem.created_at.asc(), QuantityItem.id.asc())
        )
        quantity_items = list(quantity_items_result.scalars().all())
        quantity_items_by_id = {item.id: item for item in quantity_items}

        next_snapshot_sort_order = 1

        def _next_snapshot_sort_order() -> int:
            nonlocal next_snapshot_sort_order
            sort_order = next_snapshot_sort_order
            next_snapshot_sort_order += 1
            return sort_order

        quantity_entries: list[EstimateQuantityEntryInput] = []
        for quantity_dependency in assembly.quantity_entries:
            quantity_item = quantity_items_by_id.get(quantity_dependency.quantity_item_id)
            if quantity_item is None:
                raise _build_estimate_job_input_error(
                    "missing_quantity_item",
                    extra_details={
                        "quantity_item_id": str(quantity_dependency.quantity_item_id),
                        "quantity_entry_key": quantity_dependency.entry_key,
                    },
                )
            if quantity_item.item_kind in {
                QuantityItemKind.EXCLUSION.value,
                QuantityItemKind.CONFLICT.value,
            }:
                raise _build_estimate_job_input_error(
                    "invalid_quantity_item_kind",
                    extra_details={
                        "quantity_item_id": str(quantity_item.id),
                        "item_kind": quantity_item.item_kind,
                    },
                )
            if quantity_item.value is None or not math.isfinite(quantity_item.value):
                raise _build_estimate_job_input_error(
                    "invalid_quantity_value",
                    extra_details={"quantity_item_id": str(quantity_item.id)},
                )

            quantity_payload = {
                "quantity_takeoff_id": str(quantity_takeoff.id),
                "quantity_item_id": str(quantity_item.id),
                "item_kind": quantity_item.item_kind,
                "quantity_type": quantity_item.quantity_type,
                "value": quantity_item.value,
                "unit": quantity_item.unit,
                "review_state": quantity_item.review_state,
                "validation_status": quantity_item.validation_status,
                "quantity_gate": quantity_item.quantity_gate,
                "source_entity_id": quantity_item.source_entity_id,
                "excluded_source_entity_ids_json": deepcopy(
                    quantity_item.excluded_source_entity_ids_json
                ),
            }
            quantity_entries.append(
                EstimateQuantityEntryInput(
                    entry_key=quantity_dependency.entry_key,
                    entry_label=quantity_item.quantity_type,
                    sort_order=_next_snapshot_sort_order(),
                    source_quantity_item_id=quantity_item.id,
                    source_checksum_sha256=_estimate_input_checksum(quantity_payload),
                    quantity_value=Decimal(str(quantity_item.value)),
                    unit=quantity_item.unit,
                    source_quantity_takeoff_id=quantity_takeoff.id,
                    source_payload=quantity_payload,
                )
            )

        lines_by_key = {line.line_key: line for line in assembly.lines}
        quantity_entry_keys = {entry.entry_key for entry in quantity_entries}
        assumption_entries = _build_estimate_assumption_entries(
            estimate_input.assumptions_json,
            next_sort_order=_next_snapshot_sort_order,
        )
        assumption_entry_keys = {entry.entry_key for entry in assumption_entries}
        rate_entry_keys = {
            line.catalog_entry_key
            for line in assembly.lines
            if line.rate_catalog_entry_id is not None
        }
        material_entry_keys = {
            line.catalog_entry_key
            for line in assembly.lines
            if line.material_catalog_entry_id is not None
        }
        rate_entries: list[EstimateRateEntryInput] = []
        material_entries: list[EstimateMaterialEntryInput] = []
        formula_entries: list[EstimateFormulaEntryInput] = []
        line_inputs: list[EstimateLineInputSpec] = []
        emitted_catalog_entry_sources: dict[str, tuple[str, UUID, str]] = {}

        def _register_catalog_entry_source(
            *,
            line: _EstimateWorkerLineInput,
            source_id: UUID,
            source_checksum_sha256: str,
        ) -> bool:
            candidate = (line.ref_type, source_id, source_checksum_sha256)
            existing = emitted_catalog_entry_sources.get(line.catalog_entry_key)
            if existing is None:
                emitted_catalog_entry_sources[line.catalog_entry_key] = candidate
                return True
            if existing != candidate:
                existing_ref_type, existing_source_id, existing_checksum = existing
                raise _build_estimate_job_input_error(
                    "colliding_catalog_entry_key",
                    ref_type=line.ref_type,
                    selection_key=line.selection_key,
                    line_key=line.line_key,
                    extra_details={
                        "catalog_entry_key": line.catalog_entry_key,
                        "existing_ref_type": existing_ref_type,
                        "existing_source_id": str(existing_source_id),
                        "existing_checksum_sha256": existing_checksum,
                    },
                )
            return False

        for line in assembly.lines:
            if line.ref_type == "rate":
                assert line.rate_catalog_entry_id is not None
                try:
                    matched_rate = await deps.resolve_rate(
                        session,
                        ref=CatalogRateRef(
                            id=line.rate_catalog_entry_id,
                            checksum_sha256=line.catalog_checksum_sha256,
                        ),
                    )
                except CatalogSelectionError as exc:
                    raise _build_estimate_job_input_error(
                        "catalog_ref_unresolved",
                        ref_type=line.ref_type,
                        selection_key=line.selection_key,
                        line_key=line.line_key,
                        extra_details={
                            "conflict_count": len(exc.conflicting_candidate_ids),
                        },
                    ) from exc

                if _register_catalog_entry_source(
                    line=line,
                    source_id=matched_rate.id,
                    source_checksum_sha256=matched_rate.checksum_sha256,
                ):
                    rate_entries.append(
                        EstimateRateEntryInput(
                            entry_key=line.catalog_entry_key,
                            entry_label=line.description,
                            sort_order=_next_snapshot_sort_order(),
                            source_rate_id=matched_rate.id,
                            source_checksum_sha256=matched_rate.checksum_sha256,
                            unit=matched_rate.unit,
                            effective_date=matched_rate.effective_start,
                            unit_amount=matched_rate.value,
                            source_payload={
                                "selection_key": line.selection_key,
                                "rate_key": matched_rate.rate_key,
                                "item_type": matched_rate.item_type,
                                "metadata": deepcopy(matched_rate.metadata or {}),
                            },
                            currency=cast(Any, matched_rate.currency),
                        )
                    )

                line_inputs.append(
                    EstimateLineInputSpec(
                        line_key=line.line_key,
                        line_type=cast(Any, line.line_type),
                        description=line.description,
                        quantity_entry_key=line.quantity_entry_key,
                        rate_entry_key=line.catalog_entry_key,
                    )
                )
                continue

            if line.ref_type == "material":
                assert line.material_catalog_entry_id is not None
                try:
                    matched_material = await deps.resolve_material(
                        session,
                        ref=CatalogMaterialRef(
                            id=line.material_catalog_entry_id,
                            checksum_sha256=line.catalog_checksum_sha256,
                        ),
                    )
                except CatalogSelectionError as exc:
                    raise _build_estimate_job_input_error(
                        "catalog_ref_unresolved",
                        ref_type=line.ref_type,
                        selection_key=line.selection_key,
                        line_key=line.line_key,
                        extra_details={
                            "conflict_count": len(exc.conflicting_candidate_ids),
                        },
                    ) from exc

                if _register_catalog_entry_source(
                    line=line,
                    source_id=matched_material.id,
                    source_checksum_sha256=matched_material.checksum_sha256,
                ):
                    material_entries.append(
                        EstimateMaterialEntryInput(
                            entry_key=line.catalog_entry_key,
                            entry_label=line.description,
                            sort_order=_next_snapshot_sort_order(),
                            source_material_id=matched_material.id,
                            source_checksum_sha256=matched_material.checksum_sha256,
                            unit=matched_material.unit,
                            effective_date=matched_material.effective_start,
                            unit_amount=matched_material.value,
                            source_payload={
                                "selection_key": line.selection_key,
                                "material_key": matched_material.material_key,
                                "metadata": deepcopy(matched_material.metadata or {}),
                            },
                            currency=cast(Any, matched_material.currency),
                        )
                    )

                line_inputs.append(
                    EstimateLineInputSpec(
                        line_key=line.line_key,
                        line_type=cast(Any, line.line_type),
                        description=line.description,
                        quantity_entry_key=line.quantity_entry_key,
                        material_entry_key=line.catalog_entry_key,
                    )
                )
                continue

            assert line.formula_definition_id is not None
            try:
                selected_formula = await deps.resolve_formula(
                    session,
                    ref=CatalogFormulaRef(
                        id=line.formula_definition_id,
                        checksum_sha256=line.catalog_checksum_sha256,
                    ),
                )
            except CatalogSelectionError as exc:
                raise _build_estimate_job_input_error(
                    "catalog_ref_unresolved",
                    ref_type=line.ref_type,
                    selection_key=line.selection_key,
                    line_key=line.line_key,
                    extra_details={
                        "conflict_count": len(exc.conflicting_candidate_ids),
                    },
                ) from exc

            try:
                formula_definition = formula_definition_from_selected_formula(selected_formula)
            except Exception as exc:
                raise _build_estimate_job_input_error(
                    "invalid_formula_definition",
                    ref_type=line.ref_type,
                    selection_key=line.selection_key,
                    line_key=line.line_key,
                ) from exc
            if _register_catalog_entry_source(
                line=line,
                source_id=selected_formula.definition_id,
                source_checksum_sha256=selected_formula.checksum_sha256,
            ):
                formula_entries.append(
                    EstimateFormulaEntryInput(
                        entry_key=line.catalog_entry_key,
                        entry_label=line.description,
                        sort_order=_next_snapshot_sort_order(),
                        source_formula_id=selected_formula.definition_id,
                        source_checksum_sha256=selected_formula.checksum_sha256,
                        definition=formula_definition,
                        source_payload={
                            "selection_key": line.selection_key,
                            "formula_id": selected_formula.formula_id,
                            "formula_version": selected_formula.version,
                        },
                    )
                )

            raw_formula_inputs = line.formula_inputs or {}
            declared_input_names = tuple(
                declared_input.name for declared_input in formula_definition.declared_inputs
            )
            binding_tokens = _estimate_formula_binding_tokens(
                raw_formula_inputs,
                line=line,
                declared_input_names=declared_input_names,
            )
            for declared_input_name in declared_input_names:
                if declared_input_name not in binding_tokens:
                    raise _build_estimate_job_input_error(
                        "missing_formula_input_binding",
                        ref_type=line.ref_type,
                        selection_key=line.selection_key,
                        line_key=line.line_key,
                        extra_details={"input": declared_input_name},
                    )
            resolved_formula_inputs = {
                declared_input.name: _resolve_formula_binding_snapshot_key(
                    binding_tokens[declared_input.name],
                    line=line,
                    contract_kind=declared_input.contract.kind,
                    lines_by_key=lines_by_key,
                    quantity_entry_keys=quantity_entry_keys,
                    rate_entry_keys=rate_entry_keys,
                    material_entry_keys=material_entry_keys,
                    assumption_entry_keys=assumption_entry_keys,
                )
                for declared_input in formula_definition.declared_inputs
            }
            line_inputs.append(
                EstimateLineInputSpec(
                    line_key=line.line_key,
                    line_type=cast(Any, line.line_type),
                    description=line.description,
                    formula_entry_key=line.catalog_entry_key,
                    formula_inputs=resolved_formula_inputs,
                )
            )

        return EstimateEngineInput(
            estimate_job_id=job.id,
            project_id=job.project_id,
            file_id=job.file_id,
            source_job_id=job.id,
            drawing_revision_id=estimate_input.drawing_revision_id,
            quantity_takeoff_id=quantity_takeoff.id,
            currency=cast(Any, estimate_input.currency),
            quantity_gate=cast(Any, estimate_input.quantity_gate),
            trusted_totals=estimate_input.trusted_totals,
            tax_rate=_estimate_tax_rate(estimate_input.assumptions_json),
            quantity_entries=tuple(quantity_entries),
            rate_entries=tuple(rate_entries),
            material_entries=tuple(material_entries),
            formula_entries=tuple(formula_entries),
            assumption_entries=tuple(assumption_entries),
            line_inputs=tuple(line_inputs),
        )
