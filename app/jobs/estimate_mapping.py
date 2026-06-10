"""Estimate worker-mapping inputs: assembly builders, value types, and error.

Extracted from ``worker.py``: turns resolved catalog refs into deterministic
worker-ready estimate line/quantity inputs, plus the estimate-input error type and
its builder. Pure transformations over their arguments (validation raises the
estimate-input error); no DB access or worker-module state, no monkeypatch seams.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, cast
from uuid import UUID

from app.core.errors import ErrorCode
from app.jobs.revision_materialization import (
    _first_string_ref,
    _hash_ref,
    _json_object,
    _string_ref,
)

_ESTIMATE_JOB_INPUT_INVALID_ERROR_MESSAGE = "Estimate job input mapping is invalid."
_ESTIMATE_WORKER_MAPPING_VERSION = "estimate-line-v1"


@dataclass(frozen=True, slots=True)
class _EstimateJobInputError(Exception):
    """Raised for deterministic estimate input mapping failures."""

    error_code: ErrorCode
    message: str
    details: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.message


@dataclass(frozen=True, slots=True)
class _EstimateWorkerQuantityEntry:
    """Deduped quantity entry dependency for estimate worker assembly."""

    entry_key: str
    quantity_item_id: UUID


@dataclass(frozen=True, slots=True)
class _EstimateWorkerLineInput:
    """Normalized worker line assembled from one explicit catalog ref."""

    line_key: str
    line_type: str
    description: str
    ref_type: str
    selection_key: str
    catalog_entry_key: str
    ref_order: int
    catalog_checksum_sha256: str
    rate_catalog_entry_id: UUID | None
    material_catalog_entry_id: UUID | None
    formula_definition_id: UUID | None
    quantity_entry_key: str | None
    quantity_item_id: UUID | None
    formula_inputs: dict[str, Any] | None


@dataclass(frozen=True, slots=True)
class _EstimateWorkerAssemblyInput:
    """Estimate worker-ready line and quantity-entry inputs."""

    lines: list[_EstimateWorkerLineInput]
    quantity_entries: list[_EstimateWorkerQuantityEntry]


def _build_estimate_job_input_error(
    reason: str,
    *,
    ref_index: int | None = None,
    ref_type: str | None = None,
    selection_key: str | None = None,
    line_key: str | None = None,
    extra_details: dict[str, Any] | None = None,
) -> _EstimateJobInputError:
    """Build a sanitized deterministic estimate input mapping error."""
    details: dict[str, Any] = {"reason": reason}
    if ref_index is not None:
        details["ref_index"] = ref_index
    if ref_type is not None:
        details["ref_type"] = ref_type
    if selection_key is not None:
        details["selection_key"] = selection_key
    if line_key is not None:
        details["line_key"] = line_key
    if extra_details:
        details.update(extra_details)
    return _EstimateJobInputError(
        error_code=ErrorCode.INPUT_INVALID,
        message=_ESTIMATE_JOB_INPUT_INVALID_ERROR_MESSAGE,
        details=details,
    )


def _estimate_mapping_uuid(
    value: Any,
    *,
    reason: str,
    ref_index: int,
    ref_type: str,
    selection_key: str,
    line_key: str,
    field_name: str,
) -> UUID:
    """Parse one required UUID field from estimate mapping context."""
    normalized = _string_ref(value)
    if normalized is None:
        raise _build_estimate_job_input_error(
            reason,
            ref_index=ref_index,
            ref_type=ref_type,
            selection_key=selection_key,
            line_key=line_key,
            extra_details={"field": field_name},
        )
    try:
        return UUID(normalized)
    except ValueError as exc:
        raise _build_estimate_job_input_error(
            reason,
            ref_index=ref_index,
            ref_type=ref_type,
            selection_key=selection_key,
            line_key=line_key,
            extra_details={"field": field_name},
        ) from exc


def _build_estimate_worker_mapping_v1(
    catalog_refs: Sequence[Any],
) -> _EstimateWorkerAssemblyInput:
    """Assemble deterministic estimate worker mapping inputs from catalog refs."""
    pending_lines: list[_EstimateWorkerLineInput] = []
    quantity_entries_by_key: dict[str, _EstimateWorkerQuantityEntry] = {}
    seen_line_keys: set[str] = set()

    for ref_index, catalog_ref in enumerate(catalog_refs):
        ref_type = _string_ref(getattr(catalog_ref, "ref_type", None))
        selection_key = _string_ref(getattr(catalog_ref, "selection_key", None))
        context = _json_object(getattr(catalog_ref, "selection_context_json", None))

        if context.get("worker_mapping_version") != _ESTIMATE_WORKER_MAPPING_VERSION:
            raise _build_estimate_job_input_error(
                "missing_worker_mapping_version",
                ref_index=ref_index,
                ref_type=ref_type,
                selection_key=selection_key,
                extra_details={
                    "expected_worker_mapping_version": _ESTIMATE_WORKER_MAPPING_VERSION,
                },
            )

        line_key = _string_ref(context.get("line_key"))
        line_type = _string_ref(context.get("line_type"))
        description = _string_ref(context.get("description"))
        if (
            line_key is None
            or line_type is None
            or description is None
            or ref_type is None
            or selection_key is None
        ):
            raise _build_estimate_job_input_error(
                "missing_required_mapping_field",
                ref_index=ref_index,
                ref_type=ref_type,
                selection_key=selection_key,
                line_key=line_key,
            )

        if line_type != ref_type:
            raise _build_estimate_job_input_error(
                "mismatched_line_ref_type",
                ref_index=ref_index,
                ref_type=ref_type,
                selection_key=selection_key,
                line_key=line_key,
                extra_details={"line_type": line_type},
            )

        raw_ref_order = getattr(catalog_ref, "ref_order", None)
        if isinstance(raw_ref_order, bool) or not isinstance(raw_ref_order, int):
            raise _build_estimate_job_input_error(
                "invalid_ref_order",
                ref_index=ref_index,
                ref_type=ref_type,
                selection_key=selection_key,
                line_key=line_key,
            )

        if line_key in seen_line_keys:
            raise _build_estimate_job_input_error(
                "duplicate_line_key",
                ref_index=ref_index,
                ref_type=ref_type,
                selection_key=selection_key,
                line_key=line_key,
            )
        seen_line_keys.add(line_key)

        quantity_entry_key = _string_ref(context.get("quantity_entry_key"))
        catalog_checksum_sha256 = _hash_ref(getattr(catalog_ref, "catalog_checksum_sha256", None))
        if catalog_checksum_sha256 is None:
            raise _build_estimate_job_input_error(
                "invalid_catalog_checksum",
                ref_index=ref_index,
                ref_type=ref_type,
                selection_key=selection_key,
                line_key=line_key,
            )
        catalog_entry_key = _first_string_ref(
            context.get("catalog_entry_key"),
            f"{ref_type}:{selection_key}",
        )
        assert catalog_entry_key is not None
        rate_catalog_entry_id: UUID | None = None
        material_catalog_entry_id: UUID | None = None
        formula_definition_id: UUID | None = None
        quantity_item_id: UUID | None = None
        formula_inputs: dict[str, Any] | None = None

        if ref_type in {"rate", "material"}:
            quantity_item_id = _estimate_mapping_uuid(
                context.get("quantity_item_id"),
                reason="missing_quantity_item_id",
                ref_index=ref_index,
                ref_type=ref_type,
                selection_key=selection_key,
                line_key=line_key,
                field_name="quantity_item_id",
            )
            if quantity_entry_key is None:
                quantity_entry_key = f"quantity:{quantity_item_id}"
            existing_quantity_entry = quantity_entries_by_key.get(quantity_entry_key)
            if existing_quantity_entry is None:
                quantity_entries_by_key[quantity_entry_key] = _EstimateWorkerQuantityEntry(
                    entry_key=quantity_entry_key,
                    quantity_item_id=quantity_item_id,
                )
            elif existing_quantity_entry.quantity_item_id != quantity_item_id:
                raise _build_estimate_job_input_error(
                    "mismatched_quantity_entry",
                    ref_index=ref_index,
                    ref_type=ref_type,
                    selection_key=selection_key,
                    line_key=line_key,
                    extra_details={"quantity_entry_key": quantity_entry_key},
                )
            if ref_type == "rate":
                rate_catalog_entry_id = _estimate_mapping_uuid(
                    getattr(catalog_ref, "rate_catalog_entry_id", None),
                    reason="missing_catalog_entry_id",
                    ref_index=ref_index,
                    ref_type=ref_type,
                    selection_key=selection_key,
                    line_key=line_key,
                    field_name="rate_catalog_entry_id",
                )
            else:
                material_catalog_entry_id = _estimate_mapping_uuid(
                    getattr(catalog_ref, "material_catalog_entry_id", None),
                    reason="missing_catalog_entry_id",
                    ref_index=ref_index,
                    ref_type=ref_type,
                    selection_key=selection_key,
                    line_key=line_key,
                    field_name="material_catalog_entry_id",
                )
        elif ref_type == "formula":
            if not isinstance(context.get("formula_inputs"), dict):
                raise _build_estimate_job_input_error(
                    "missing_formula_inputs",
                    ref_index=ref_index,
                    ref_type=ref_type,
                    selection_key=selection_key,
                    line_key=line_key,
                )
            formula_inputs = _json_object(context.get("formula_inputs"))
            formula_definition_id = _estimate_mapping_uuid(
                getattr(catalog_ref, "formula_definition_id", None),
                reason="missing_catalog_entry_id",
                ref_index=ref_index,
                ref_type=ref_type,
                selection_key=selection_key,
                line_key=line_key,
                field_name="formula_definition_id",
            )
        else:
            raise _build_estimate_job_input_error(
                "unsupported_ref_type",
                ref_index=ref_index,
                ref_type=ref_type,
                selection_key=selection_key,
                line_key=line_key,
            )

        pending_lines.append(
            _EstimateWorkerLineInput(
                line_key=line_key,
                line_type=line_type,
                description=description,
                ref_type=ref_type,
                selection_key=selection_key,
                catalog_entry_key=catalog_entry_key,
                ref_order=raw_ref_order,
                catalog_checksum_sha256=catalog_checksum_sha256,
                rate_catalog_entry_id=rate_catalog_entry_id,
                material_catalog_entry_id=material_catalog_entry_id,
                formula_definition_id=formula_definition_id,
                quantity_entry_key=quantity_entry_key,
                quantity_item_id=quantity_item_id,
                formula_inputs=formula_inputs,
            )
        )

    lines = sorted(
        pending_lines,
        key=lambda line: (line.ref_order, line.ref_type, line.selection_key),
    )
    quantity_entries: list[_EstimateWorkerQuantityEntry] = []
    emitted_quantity_entry_keys: set[str] = set()
    for line in lines:
        if (
            line.quantity_entry_key is None
            or line.quantity_entry_key in emitted_quantity_entry_keys
        ):
            continue
        quantity_entries.append(quantity_entries_by_key[line.quantity_entry_key])
        emitted_quantity_entry_keys.add(line.quantity_entry_key)

    return _EstimateWorkerAssemblyInput(lines=lines, quantity_entries=quantity_entries)


def _estimate_formula_binding_tokens(
    raw_bindings: dict[str, Any],
    *,
    line: _EstimateWorkerLineInput,
    declared_input_names: tuple[str, ...],
) -> dict[str, str]:
    """Normalize persisted formula-binding payloads into declared-input tokens."""
    bindings_payload = raw_bindings.get("bindings")
    if isinstance(bindings_payload, dict):
        if not all(
            isinstance(key, str) and key and isinstance(value, str) and value
            for key, value in bindings_payload.items()
        ):
            raise _build_estimate_job_input_error(
                "invalid_formula_input_binding",
                ref_type=line.ref_type,
                selection_key=line.selection_key,
                line_key=line.line_key,
            )
        return cast(dict[str, str], dict(bindings_payload))

    if raw_bindings and all(
        isinstance(key, str) and key and isinstance(value, str) and value
        for key, value in raw_bindings.items()
    ):
        return cast(dict[str, str], dict(raw_bindings))

    operand_line_keys = raw_bindings.get("operand_line_keys")
    if isinstance(operand_line_keys, list) and len(operand_line_keys) == len(declared_input_names):
        if not all(isinstance(value, str) and value for value in operand_line_keys):
            raise _build_estimate_job_input_error(
                "invalid_formula_input_binding",
                ref_type=line.ref_type,
                selection_key=line.selection_key,
                line_key=line.line_key,
            )
        return dict(zip(declared_input_names, operand_line_keys, strict=True))

    raise _build_estimate_job_input_error(
        "invalid_formula_input_binding",
        ref_type=line.ref_type,
        selection_key=line.selection_key,
        line_key=line.line_key,
    )


def _resolve_formula_binding_snapshot_key(
    token: str,
    *,
    line: _EstimateWorkerLineInput,
    contract_kind: str,
    lines_by_key: dict[str, _EstimateWorkerLineInput],
    quantity_entry_keys: set[str],
    rate_entry_keys: set[str],
    material_entry_keys: set[str],
) -> str:
    """Resolve one persisted formula-binding token into an engine snapshot entry key."""
    if contract_kind == "quantity" and token in quantity_entry_keys:
        return token
    if contract_kind == "rate" and token in rate_entry_keys | material_entry_keys:
        return token

    bound_line = lines_by_key.get(token)
    if bound_line is None:
        raise _build_estimate_job_input_error(
            "invalid_formula_input_binding",
            ref_type=line.ref_type,
            selection_key=line.selection_key,
            line_key=line.line_key,
            extra_details={"binding": token, "contract_kind": contract_kind},
        )

    if contract_kind == "quantity" and bound_line.quantity_entry_key is not None:
        return bound_line.quantity_entry_key
    if contract_kind == "rate":
        if bound_line.rate_catalog_entry_id is not None:
            return bound_line.catalog_entry_key
        if bound_line.material_catalog_entry_id is not None:
            return bound_line.catalog_entry_key

    raise _build_estimate_job_input_error(
        "invalid_formula_input_binding",
        ref_type=line.ref_type,
        selection_key=line.selection_key,
        line_key=line.line_key,
        extra_details={"binding": token, "contract_kind": contract_kind},
    )
