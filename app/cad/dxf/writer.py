"""Pure canonical-revision to deterministic ASCII DXF writer."""

from __future__ import annotations

import io
import json
import math
import re
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from app.cad.dxf.units import UnitSpec, resolve_unit

_ezdxf = __import__("ezdxf")
new_dxf_document = _ezdxf.new


@dataclass(frozen=True, slots=True)
class DxfWriteOptions:
    """Writer options for deterministic DXF output."""

    unit: str | None = None


@dataclass(frozen=True, slots=True)
class DxfWriteResult:
    """Result payload returned from the DXF writer."""

    content: bytes
    warnings: tuple[str, ...]
    diagnostics: tuple[dict[str, Any], ...]


class DxfWriteError(RuntimeError):
    """Deterministic conversion error for canonical DXF writes."""

    code: str
    message: str
    details: Mapping[str, Any]

    def __init__(
        self, *, code: str, message: str, details: Mapping[str, Any] | None = None
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}


_SUPPORTED_LAYOUT_NAMES = {
    "model",
    "model space",
    "modelspace",
    "model-space",
    "model_space",
}

_LAYOUT_NAME_RE = re.compile(r"^Model Space$", re.IGNORECASE)
_LAYOUT_FALLBACK_NAME = "Model"

_LINE_ENTITY_TYPES = {"line", "lines"}
_POLYLINE_ENTITY_TYPES = {
    "polyline",
    "lwpolyline",
    "polyline2d",
    "polyline2dp",
    "lw-polyline",
}
_ARC_ENTITY_TYPES = {"arc", "arcs"}
_CIRCLE_ENTITY_TYPES = {"circle", "circles"}
_TEXT_ENTITY_TYPES = {"text", "mtext"}
_INSERT_ENTITY_TYPES = {"insert", "block_reference", "blockreference"}
# Geometry-less placeholders the adapter emits for records it could not map (e.g. unsupported
# HATCH/LWPOLYLINE). They carry no geometry, so they are skipped on export and reported as a
# diagnostic rather than aborting the whole render.
_SKIP_ENTITY_TYPES = {"unknown"}
# Fallback TEXT height (meters) when the canonical payload carries no height; placement and
# content are what matter for the round-trip, exact height is a fidelity follow-up.
_DEFAULT_TEXT_HEIGHT = 2.5

_LAYER_NAME_RE = re.compile(r'^[^\\/:*?"<>;|,`~\[\]]{1,255}$')


@dataclass(frozen=True, slots=True)
class _LayoutRecord:
    layout_id: str | None
    name: str
    source: str


@dataclass(frozen=True, slots=True)
class _LayerRecord:
    layer_id: str | None
    name: str
    source: str


@dataclass(frozen=True, slots=True)
class _ResolvedEntity:
    entity_type: str
    geometry: Any
    properties: dict[str, Any]
    layout_ref: Any
    layer_ref: Any
    path: str
    block_ref: Any = None
    transform: Any = None


@dataclass(frozen=True, slots=True)
class _BlockRecord:
    name: str
    base_point: Any
    entities: list[Any]


def write_canonical_dxf(
    canonical: Mapping[str, Any],
    options: DxfWriteOptions | Mapping[str, Any] | None = None,
) -> DxfWriteResult:
    """Render canonical revision payload into parseable ASCII DXF bytes."""

    payload = _coerce_mapping(canonical, field_path="canonical")
    write_options = _coerce_write_options(options)
    _reject_unsupported_top_level_sections(payload)

    layouts = _extract_layout_records(payload)
    layout_by_id, layout_by_name = _index_layout_records(layouts)

    layers = _extract_layer_records(payload)

    entities = _extract_entities(payload)
    blocks = _extract_block_records(payload)
    # Resolve block child entities up front so their layers can be declared alongside the
    # top-level layer table (block children are not surfaced in the canonical layer list).
    resolved_blocks = [
        (
            block,
            [
                _resolve_entity_payload(
                    entity=child, entity_path=f"blocks[{block.name}].entities[{child_index}]"
                )
                for child_index, child in enumerate(block.entities, start=1)
            ],
        )
        for block in blocks
    ]
    layers = _augment_layers_with_referenced(layers, resolved_blocks)
    layer_by_id, layer_by_name = _index_layer_records(layers)

    unit_spec = _resolve_unit(payload, write_options, entities=entities)
    default_layout = _resolve_default_model_layout(layouts)

    with _fixed_ezdxf_metadata():
        doc = new_dxf_document(dxfversion="R2010")
        doc.header["$INSUNITS"] = unit_spec.insunits
        _declare_layers(doc=doc, layers=layers)
        msp = doc.modelspace()

        skipped = 0

        # Create all BLOCK definitions first (so INSERT references resolve), then populate them.
        block_layouts = {
            block.name: doc.blocks.new(name=block.name) for block, _ in resolved_blocks
        }
        for block, children in resolved_blocks:
            block_layout = block_layouts[block.name]
            for child in children:
                if child.entity_type.lower().strip() in _SKIP_ENTITY_TYPES:
                    skipped += 1
                    continue
                child_layer = _resolve_entity_layer(
                    resolved=child, layer_by_id=layer_by_id, layer_by_name=layer_by_name
                )
                _write_entity(
                    target=block_layout,
                    resolved=child,
                    unit_spec=unit_spec,
                    layer_name=child_layer,
                )

        for index, raw_entity in enumerate(entities, start=1):
            entity_path = f"entities[{index}]"
            resolved = _resolve_entity_payload(entity=raw_entity, entity_path=entity_path)

            if resolved.entity_type.lower().strip() in _SKIP_ENTITY_TYPES:
                skipped += 1
                continue

            _resolve_entity_layout(
                layout_ref=resolved.layout_ref,
                path=entity_path,
                layout_by_id=layout_by_id,
                layout_by_name=layout_by_name,
                default_layout=default_layout,
            )

            layer_name = _resolve_entity_layer(
                resolved=resolved,
                layer_by_id=layer_by_id,
                layer_by_name=layer_by_name,
            )

            _write_entity(
                target=msp,
                resolved=resolved,
                unit_spec=unit_spec,
                layer_name=layer_name,
            )

        _normalize_required_class_order(doc)
        buffer = io.StringIO()
        doc.write(buffer)
    diagnostics: tuple[dict[str, Any], ...] = (
        ({"code": "skipped_unsupported_entities", "count": skipped},) if skipped else ()
    )
    return DxfWriteResult(
        content=buffer.getvalue().encode("utf-8"),
        warnings=(),
        diagnostics=diagnostics,
    )


@contextmanager
def _fixed_ezdxf_metadata() -> Any:
    prior_value = _ezdxf.options.write_fixed_meta_data_for_testing
    _ezdxf.options.write_fixed_meta_data_for_testing = True
    try:
        yield
    finally:
        _ezdxf.options.write_fixed_meta_data_for_testing = prior_value


def _normalize_required_class_order(doc: Any) -> None:
    doc.classes.add_required_classes(doc.dxfversion)

    classes = doc.classes.classes
    layout_key = ("LAYOUT", "AcDbLayout")
    placeholder_key = ("ACDBPLACEHOLDER", "AcDbPlaceHolder")
    if layout_key not in classes or placeholder_key not in classes:
        return

    layout_record = classes.pop(layout_key)
    placeholder_record = classes.pop(placeholder_key)
    classes[layout_key] = layout_record
    classes[placeholder_key] = placeholder_record


def _coerce_write_options(
    options: DxfWriteOptions | Mapping[str, Any] | None,
) -> DxfWriteOptions:
    if options is None:
        return DxfWriteOptions()

    if isinstance(options, DxfWriteOptions):
        return options

    if not isinstance(options, Mapping):
        raise DxfWriteError(
            code="UNSUPPORTED_UNITS",
            message="DXF writer options must be omitted or a mapping",
            details={"path": "options", "type": type(options).__name__},
        )

    return DxfWriteOptions(unit=options.get("unit"))


def _resolve_unit(
    payload: Mapping[str, Any],
    options: DxfWriteOptions,
    *,
    entities: Sequence[Any],
) -> UnitSpec:
    raw_unit: str | None
    unit_path = "canonical.unit"
    if options.unit is not None:
        raw_unit = options.unit
        unit_path = "options.unit"
    elif "unit" in payload:
        raw_unit = _coerce_unit_text(payload.get("unit"), field_path="canonical.unit")
    elif "units" in payload:
        raw_unit = _coerce_unit_text(payload.get("units"), field_path="canonical.units")
        unit_path = "canonical.units"
    else:
        raw_unit = None

    if raw_unit is None:
        return _resolve_unit_from_entity_geometry_units(entities)

    try:
        return resolve_unit(raw_unit)
    except ValueError as exc:
        raise DxfWriteError(
            code="UNSUPPORTED_UNITS",
            message="Unsupported drawing unit",
            details={"path": unit_path, "value": raw_unit},
        ) from exc


def _resolve_unit_from_entity_geometry_units(entities: Sequence[Any]) -> UnitSpec:
    if not entities:
        raise DxfWriteError(
            code="UNSUPPORTED_UNITS",
            message="Drawing units are required",
            details={"path": "canonical.unit"},
        )

    for index, raw_entity in enumerate(entities, start=1):
        entity_path = f"entities[{index}]"
        resolved = _resolve_entity_payload(entity=raw_entity, entity_path=entity_path)
        geometry = _coerce_mapping(resolved.geometry, field_path=f"{entity_path}.geometry")
        units = _coerce_mapping(geometry.get("units"), field_path=f"{entity_path}.geometry.units")
        normalized = _coerce_required_text(
            units.get("normalized"),
            field_path=f"{entity_path}.geometry.units.normalized",
            error_code="UNSUPPORTED_UNITS",
        )

        if normalized.lower() == "unitless":
            raise DxfWriteError(
                code="UNSUPPORTED_UNITS",
                message="DXF writer requires meter geometry units",
                details={
                    "path": f"{entity_path}.geometry.units.normalized",
                    "value": normalized,
                },
            )

        try:
            unit_spec = resolve_unit(normalized)
        except ValueError as exc:
            raise DxfWriteError(
                code="UNSUPPORTED_UNITS",
                message="DXF writer requires meter geometry units",
                details={
                    "path": f"{entity_path}.geometry.units.normalized",
                    "value": normalized,
                },
            ) from exc

        if unit_spec.insunits != 6 or not math.isclose(unit_spec.meter_scale, 1.0):
            raise DxfWriteError(
                code="UNSUPPORTED_UNITS",
                message="DXF writer requires meter geometry units",
                details={
                    "path": f"{entity_path}.geometry.units.normalized",
                    "value": normalized,
                },
            )

    return resolve_unit("m")


def _reject_unsupported_top_level_sections(payload: Mapping[str, Any]) -> None:
    # Blocks are now emitted as a BLOCK table with INSERT references; only xrefs remain
    # unsupported (external references need resolution beyond a single drawing).
    for field_name in ("xrefs",):
        if not _has_non_empty_value(payload.get(field_name)):
            continue

        raise DxfWriteError(
            code="UNSUPPORTED_ENTITY_TYPE",
            message=f"Top-level {field_name} are not supported",
            details={"path": field_name},
        )


def _extract_layout_records(payload: Mapping[str, Any]) -> list[_LayoutRecord]:
    raw_layouts = payload.get("layouts")
    if raw_layouts is None:
        return []

    if not isinstance(raw_layouts, list):
        raise DxfWriteError(
            code="UNSUPPORTED_LAYOUT",
            message="layouts must be a list",
            details={"path": "layouts", "type": type(raw_layouts).__name__},
        )

    layouts: list[_LayoutRecord] = []
    for index, raw_layout in enumerate(raw_layouts, start=1):
        row_path = f"layouts[{index}]"
        row = _coerce_layout_or_layer_row(raw_layout, row_path=row_path)

        name = _coerce_required_text(
            row.get("name"),
            field_path=f"{row_path}.name",
            error_code="UNSUPPORTED_LAYOUT",
        )

        if not _is_supported_layout_name(name):
            raise DxfWriteError(
                code="UNSUPPORTED_LAYOUT",
                message=f"Unsupported layout '{name}'",
                details={"path": row_path, "name": name},
            )

        layout_id = _coerce_optional_identifier(
            row.get("id") or row.get("layout_id") or row.get("layout_ref"),
            field_path=f"{row_path}.id",
            error_code="UNSUPPORTED_LAYOUT",
        )
        layouts.append(_LayoutRecord(layout_id=layout_id, name=name, source=row_path))

    return layouts


def _extract_layer_records(payload: Mapping[str, Any]) -> list[_LayerRecord]:
    raw_layers = payload.get("layers")
    if raw_layers is None:
        return []

    if not isinstance(raw_layers, list):
        raise DxfWriteError(
            code="UNKNOWN_LAYER",
            message="layers must be a list",
            details={"path": "layers", "type": type(raw_layers).__name__},
        )

    layers: list[_LayerRecord] = []
    for index, raw_layer in enumerate(raw_layers, start=1):
        row_path = f"layers[{index}]"
        row = _coerce_layout_or_layer_row(raw_layer, row_path=row_path)

        name = _coerce_required_text(
            row.get("name"),
            field_path=f"{row_path}.name",
            error_code="UNKNOWN_LAYER",
        )
        if not _LAYER_NAME_RE.match(name):
            raise DxfWriteError(
                code="UNKNOWN_LAYER",
                message=f"Unsupported layer '{name}'",
                details={"path": row_path},
            )

        layer_id = _coerce_optional_identifier(
            row.get("id") or row.get("layer_id") or row.get("layer_ref"),
            field_path=f"{row_path}.id",
            error_code="UNKNOWN_LAYER",
        )
        layers.append(_LayerRecord(layer_id=layer_id, name=name, source=row_path))

    return layers


def _extract_entities(payload: Mapping[str, Any]) -> list[Any]:
    entities = payload.get("entities")
    if entities is None:
        return []

    if not isinstance(entities, list):
        raise DxfWriteError(
            code="UNSUPPORTED_ENTITY_TYPE",
            message="entities must be a list",
            details={"path": "entities", "type": type(entities).__name__},
        )

    return list(entities)


def _extract_block_records(payload: Mapping[str, Any]) -> list[_BlockRecord]:
    raw_blocks = payload.get("blocks")
    if raw_blocks is None:
        return []
    if not isinstance(raw_blocks, (list, tuple)):
        raise DxfWriteError(
            code="UNSUPPORTED_ENTITY_TYPE",
            message="blocks must be a list",
            details={"path": "blocks", "type": type(raw_blocks).__name__},
        )

    records: list[_BlockRecord] = []
    for index, raw_block in enumerate(raw_blocks, start=1):
        block = _coerce_mapping(raw_block, field_path=f"blocks[{index}]")
        name = _coerce_required_text(
            block.get("name") or block.get("block_ref"),
            field_path=f"blocks[{index}].name",
            error_code="UNSUPPORTED_ENTITY_TYPE",
        )
        raw_entities = block.get("entities")
        entities = list(raw_entities) if isinstance(raw_entities, (list, tuple)) else []
        records.append(
            _BlockRecord(name=name, base_point=block.get("base_point"), entities=entities)
        )
    return records


def _layer_ref_text(layer_ref: Any) -> str | None:
    if isinstance(layer_ref, str):
        return layer_ref.strip() or None
    if isinstance(layer_ref, Mapping):
        for key in ("name", "layer", "id", "layer_id"):
            value = layer_ref.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def _augment_layers_with_referenced(
    layers: list[_LayerRecord], resolved_blocks: list[tuple[_BlockRecord, list[_ResolvedEntity]]]
) -> list[_LayerRecord]:
    """Declare any layer referenced by a block child that isn't already in the layer table.

    The canonical layer list is built from top-level entities only, so block-owned geometry can
    reference layers absent from it; those must still be declared for the DXF to be valid.
    """

    by_id, by_name = _index_layer_records(layers)
    declared = {layer.name.lower() for layer in layers}
    augmented = list(layers)
    for _block, children in resolved_blocks:
        for child in children:
            ref = _layer_ref_text(child.layer_ref)
            if ref is None or ref == "0":
                continue
            # Skip refs that already resolve by id or name; only a genuinely undeclared layer
            # (a name absent from the top-level table) needs a new record.
            if ref in by_id or ref.lower() in by_name or ref.lower() in declared:
                continue
            augmented.append(_LayerRecord(layer_id=None, name=ref, source="block_child"))
            declared.add(ref.lower())
    return augmented


def _coerce_layout_or_layer_row(row: Any, *, row_path: str) -> dict[str, Any]:
    mapping = _coerce_mapping(row, field_path=row_path)

    merged: dict[str, Any] = {}
    payload = mapping.get("payload")
    if payload is not None:
        merged.update(
            _coerce_object_mapping(
                payload,
                field_path=f"{row_path}.payload",
                error_code="UNSUPPORTED_LAYOUT"
                if row_path.startswith("layouts[")
                else "UNKNOWN_LAYER",
            )
        )

    payload_json = mapping.get("payload_json")
    if payload_json is not None:
        merged.update(
            _coerce_object_mapping(
                payload_json,
                field_path=f"{row_path}.payload_json",
                error_code="UNSUPPORTED_LAYOUT"
                if row_path.startswith("layouts[")
                else "UNKNOWN_LAYER",
            )
        )
    merged.update(
        {key: value for key, value in mapping.items() if key not in {"payload", "payload_json"}}
    )
    return merged


def _resolve_entity_payload(*, entity: Any, entity_path: str) -> _ResolvedEntity:
    mapping = _coerce_mapping(entity, field_path=entity_path)

    merged: dict[str, Any] = {}

    payload = mapping.get("payload")
    if payload is not None:
        _merge_entity_mapping(merged, payload, field_path=f"{entity_path}.payload")

    payload_json = mapping.get("payload_json")
    if payload_json is not None:
        _merge_entity_mapping(
            merged,
            payload_json,
            field_path=f"{entity_path}.payload_json",
        )

    canonical_entity_json = mapping.get("canonical_entity_json")
    if canonical_entity_json is not None:
        merged.update(
            _coerce_object_mapping(
                canonical_entity_json,
                field_path=f"{entity_path}.canonical_entity_json",
                error_code="UNSUPPORTED_ENTITY_TYPE",
            )
        )

    canonical_entity = mapping.get("canonical_entity")
    if canonical_entity is not None:
        merged.update(
            _coerce_object_mapping(
                canonical_entity,
                field_path=f"{entity_path}.canonical_entity",
                error_code="UNSUPPORTED_ENTITY_TYPE",
            )
        )

    merged.update(
        {
            key: value
            for key, value in mapping.items()
            if key
            not in {
                "payload",
                "payload_json",
                "canonical_entity",
                "canonical_entity_json",
            }
        }
    )

    geometry_json = mapping.get("geometry_json")
    if geometry_json is None:
        geometry_json = merged.get("geometry_json")
    if geometry_json is not None:
        merged["geometry"] = _coerce_json_mapping(
            geometry_json,
            field_path=f"{entity_path}.geometry_json",
        )

    properties_json = mapping.get("properties_json")
    if properties_json is None:
        properties_json = merged.get("properties_json")
    if properties_json is not None:
        merged["properties"] = _coerce_json_mapping(
            properties_json,
            field_path=f"{entity_path}.properties_json",
        )

    entity_type = _coerce_required_text(
        merged.get("entity_type") or merged.get("type") or merged.get("kind"),
        field_path=f"{entity_path}.entity_type",
        error_code="UNSUPPORTED_ENTITY_TYPE",
    )

    layout_ref = (
        merged.get("layout_id")
        or merged.get("layout")
        or merged.get("layout_ref")
        or merged.get("layoutRef")
    )

    properties = _coerce_mapping(
        merged.get("properties") if merged.get("properties") is not None else {},
        field_path=f"{entity_path}.properties",
    )

    layer_ref = (
        properties.get("layer_id")
        or properties.get("layer_ref")
        or properties.get("layerRef")
        or merged.get("layer_id")
        or merged.get("layer_ref")
        or merged.get("layerRef")
    )

    block_ref = (
        merged.get("block_ref")
        or merged.get("blockRef")
        or merged.get("block_name")
        or properties.get("block_ref")
        or properties.get("blockRef")
    )

    geometry = merged.get("geometry") if merged.get("geometry") is not None else {}
    transform = merged.get("transform")
    if transform is None and isinstance(geometry, Mapping):
        transform = geometry.get("transform")

    return _ResolvedEntity(
        entity_type=entity_type,
        geometry=geometry,
        properties=dict(properties),
        layout_ref=layout_ref,
        layer_ref=layer_ref,
        path=entity_path,
        block_ref=block_ref,
        transform=transform,
    )


def _index_layout_records(
    records: list[_LayoutRecord],
) -> tuple[dict[str, _LayoutRecord], dict[str, _LayoutRecord]]:
    by_id: dict[str, _LayoutRecord] = {}
    by_name: dict[str, _LayoutRecord] = {}

    for record in records:
        if record.layout_id is not None:
            by_id[record.layout_id] = record

        by_name[record.name.lower()] = record

    return by_id, by_name


def _index_layer_records(
    records: list[_LayerRecord],
) -> tuple[dict[str, _LayerRecord], dict[str, _LayerRecord]]:
    by_id: dict[str, _LayerRecord] = {}
    by_name: dict[str, _LayerRecord] = {}

    for record in records:
        if record.layer_id is not None:
            by_id[record.layer_id] = record

        by_name[record.name.lower()] = record

    return by_id, by_name


def _declare_layers(*, doc: Any, layers: list[_LayerRecord]) -> None:
    for layer in layers:
        if layer.name in doc.layers:
            continue
        doc.layers.add(layer.name)


def _resolve_default_model_layout(layouts: list[_LayoutRecord]) -> str | None:
    if not layouts:
        return _LAYOUT_FALLBACK_NAME

    if len(layouts) == 1:
        return layouts[0].name

    model_layouts = [layout.name for layout in layouts if _is_supported_layout_name(layout.name)]
    if not model_layouts:
        raise DxfWriteError(
            code="UNSUPPORTED_LAYOUT",
            message="No supported model layout",
            details={"path": "layouts"},
        )

    if len(model_layouts) == 1:
        return model_layouts[0]

    raise DxfWriteError(
        code="UNSUPPORTED_LAYOUT",
        message="Ambiguous layout references; multiple layout rows were provided",
        details={"path": "layouts", "count": len(layouts)},
    )


def _resolve_entity_layout(
    *,
    layout_ref: Any,
    path: str,
    layout_by_id: dict[str, _LayoutRecord],
    layout_by_name: dict[str, _LayoutRecord],
    default_layout: str | None,
) -> None:
    if layout_ref is None:
        if default_layout is not None:
            return

        raise DxfWriteError(
            code="UNSUPPORTED_LAYOUT",
            message="Entity is missing a layout reference",
            details={"path": path},
        )

    if isinstance(layout_ref, Mapping):
        layout_ref = layout_ref.get("id") or layout_ref.get("layout_id") or layout_ref.get("name")

    if layout_ref is None:
        raise DxfWriteError(
            code="UNSUPPORTED_LAYOUT",
            message="Layout reference is empty",
            details={"path": path},
        )

    layout_ref_text = _coerce_required_text(
        layout_ref,
        field_path=f"{path}.layout",
        error_code="UNSUPPORTED_LAYOUT",
    )

    if layout_ref_text in layout_by_id:
        return

    normalized = layout_ref_text.strip().lower()
    if normalized in layout_by_name:
        return

    raise DxfWriteError(
        code="UNSUPPORTED_LAYOUT",
        message="Unknown layout reference",
        details={"path": path, "layout": layout_ref_text},
    )


def _resolve_entity_layer(
    *,
    resolved: _ResolvedEntity,
    layer_by_id: dict[str, _LayerRecord],
    layer_by_name: dict[str, _LayerRecord],
) -> str:
    if resolved.layer_ref is not None:
        layer_ref = resolved.layer_ref
        if isinstance(layer_ref, Mapping):
            layer_ref = (
                layer_ref.get("id")
                or layer_ref.get("layer_id")
                or layer_ref.get("name")
                or layer_ref.get("layer")
            )

        layer_ref_text = _coerce_required_text(
            layer_ref,
            field_path=f"{resolved.path}.layer",
            error_code="UNKNOWN_LAYER",
        )

        resolved_layer = layer_by_id.get(layer_ref_text)
        if resolved_layer is None:
            resolved_layer = layer_by_name.get(layer_ref_text.lower())

        if resolved_layer is not None:
            return resolved_layer.name

        if layer_ref_text == "0":
            return "0"

        raise DxfWriteError(
            code="UNKNOWN_LAYER",
            message="Unknown layer reference",
            details={"path": resolved.path, "layer": layer_ref_text},
        )

    direct_layer = resolved.properties.get("layer")
    if direct_layer is not None:
        direct_layer_text = _coerce_optional_text(
            direct_layer,
            field_path=f"{resolved.path}.properties.layer",
            error_code="UNKNOWN_LAYER",
        )

        if direct_layer_text is None:
            raise DxfWriteError(
                code="UNKNOWN_LAYER",
                message="Layer reference is empty",
                details={"path": resolved.path},
            )

        if direct_layer_text == "0":
            return "0"

        resolved_layer = layer_by_name.get(direct_layer_text.lower())
        if resolved_layer is not None:
            return resolved_layer.name

        raise DxfWriteError(
            code="UNKNOWN_LAYER",
            message="Layer is not declared in layer collection",
            details={"path": resolved.path, "layer": direct_layer_text},
        )

    raise DxfWriteError(
        code="UNKNOWN_LAYER",
        message="Entity is missing a layer reference",
        details={"path": resolved.path},
    )


def _write_entity(
    *, target: Any, resolved: _ResolvedEntity, unit_spec: UnitSpec, layer_name: str
) -> None:
    """Emit one canonical entity into ``target`` (a modelspace or a block layout)."""

    entity_type = resolved.entity_type.lower().strip()

    if entity_type in _LINE_ENTITY_TYPES:
        start, end = _extract_line_points(
            geometry=resolved.geometry,
            entity_path=resolved.path,
            unit_spec=unit_spec,
        )
        target.add_line(start=start, end=end, dxfattribs={"layer": layer_name})
        return

    if entity_type in _POLYLINE_ENTITY_TYPES:
        points = _extract_polyline_points(
            geometry=resolved.geometry,
            entity_path=resolved.path,
            unit_spec=unit_spec,
        )
        if not points:
            raise DxfWriteError(
                code="UNSUPPORTED_GEOMETRY",
                message="Polyline has no points",
                details={"path": resolved.path},
            )

        closed = _coerce_optional_bool(
            resolved.properties.get("closed")
            or (resolved.geometry if isinstance(resolved.geometry, Mapping) else {}).get("closed"),
            field_path=f"{resolved.path}.properties.closed",
        )

        target.add_lwpolyline(points, dxfattribs={"layer": layer_name}, close=closed)
        return

    if entity_type in _CIRCLE_ENTITY_TYPES:
        center, radius = _extract_circle_geometry(
            geometry=resolved.geometry, entity_path=resolved.path, unit_spec=unit_spec
        )
        target.add_circle(center=center, radius=radius, dxfattribs={"layer": layer_name})
        return

    if entity_type in _ARC_ENTITY_TYPES:
        center, radius, start_angle, end_angle = _extract_arc_geometry(
            geometry=resolved.geometry, entity_path=resolved.path, unit_spec=unit_spec
        )
        target.add_arc(
            center=center,
            radius=radius,
            start_angle=start_angle,
            end_angle=end_angle,
            dxfattribs={"layer": layer_name},
        )
        return

    if entity_type in _TEXT_ENTITY_TYPES:
        insertion, content = _extract_text_geometry(
            geometry=resolved.geometry, entity_path=resolved.path, unit_spec=unit_spec
        )
        text_entity = target.add_text(
            content,
            dxfattribs={"layer": layer_name, "height": _DEFAULT_TEXT_HEIGHT},
        )
        text_entity.set_placement((insertion[0], insertion[1]))
        return

    if entity_type in _INSERT_ENTITY_TYPES:
        block_name, insertion, xscale, yscale, rotation = _extract_insert_placement(
            resolved=resolved, unit_spec=unit_spec
        )
        target.add_blockref(
            block_name,
            (insertion[0], insertion[1]),
            dxfattribs={
                "layer": layer_name,
                "xscale": xscale,
                "yscale": yscale,
                "rotation": rotation,
            },
        )
        return

    raise DxfWriteError(
        code="UNSUPPORTED_ENTITY_TYPE",
        message=f"Unsupported entity type '{resolved.entity_type}'",
        details={"path": resolved.path, "entity_type": resolved.entity_type},
    )


def _extract_circle_geometry(
    *, geometry: Any, entity_path: str, unit_spec: UnitSpec
) -> tuple[tuple[float, float, float], float]:
    if not isinstance(geometry, Mapping) or geometry.get("center") is None:
        raise DxfWriteError(
            code="UNSUPPORTED_GEOMETRY",
            message=f"Circle geometry requires a center at {entity_path}",
            details={"path": entity_path},
        )
    center = _coerce_point(
        geometry["center"], field_path=f"{entity_path}.geometry.center", scale=unit_spec.meter_scale
    )
    radius = _coerce_real_number(
        geometry.get("radius"),
        field_path=f"{entity_path}.geometry.radius",
        scale=unit_spec.meter_scale,
    )
    return center, radius


def _extract_arc_geometry(
    *, geometry: Any, entity_path: str, unit_spec: UnitSpec
) -> tuple[tuple[float, float, float], float, float, float]:
    if not isinstance(geometry, Mapping) or geometry.get("center") is None:
        raise DxfWriteError(
            code="UNSUPPORTED_GEOMETRY",
            message=f"Arc geometry requires a center at {entity_path}",
            details={"path": entity_path},
        )
    center = _coerce_point(
        geometry["center"], field_path=f"{entity_path}.geometry.center", scale=unit_spec.meter_scale
    )
    radius = _coerce_real_number(
        geometry.get("radius"),
        field_path=f"{entity_path}.geometry.radius",
        scale=unit_spec.meter_scale,
    )
    start_angle = _coerce_real_number(
        geometry.get("start_angle_degrees"),
        field_path=f"{entity_path}.geometry.start_angle_degrees",
        scale=1.0,
    )
    end_angle = _coerce_real_number(
        geometry.get("end_angle_degrees"),
        field_path=f"{entity_path}.geometry.end_angle_degrees",
        scale=1.0,
    )
    return center, radius, start_angle, end_angle


def _extract_text_geometry(
    *, geometry: Any, entity_path: str, unit_spec: UnitSpec
) -> tuple[tuple[float, float, float], str]:
    if not isinstance(geometry, Mapping):
        raise DxfWriteError(
            code="UNSUPPORTED_GEOMETRY",
            message=f"Text geometry must be an object at {entity_path}",
            details={"path": entity_path},
        )
    insertion_raw = geometry.get("insertion") or geometry.get("insert")
    if insertion_raw is None:
        raise DxfWriteError(
            code="UNSUPPORTED_GEOMETRY",
            message=f"Text geometry requires an insertion point at {entity_path}",
            details={"path": entity_path},
        )
    insertion = _coerce_point(
        insertion_raw, field_path=f"{entity_path}.geometry.insertion", scale=unit_spec.meter_scale
    )
    content = geometry.get("text")
    text = content if isinstance(content, str) else ""
    return insertion, text


def _extract_insert_placement(
    *, resolved: _ResolvedEntity, unit_spec: UnitSpec
) -> tuple[str, tuple[float, float, float], float, float, float]:
    block_name = _coerce_required_text(
        resolved.block_ref,
        field_path=f"{resolved.path}.block_ref",
        error_code="UNSUPPORTED_ENTITY_TYPE",
    )
    transform = resolved.transform if isinstance(resolved.transform, Mapping) else {}
    insertion_raw = transform.get("insertion_point") or transform.get("insertion") or {}
    insertion = _coerce_point(
        insertion_raw,
        field_path=f"{resolved.path}.transform.insertion_point",
        scale=unit_spec.meter_scale,
    )
    scale_raw = transform.get("scale")
    scale = scale_raw if isinstance(scale_raw, Mapping) else {}
    xscale = _coerce_optional_scale(scale.get("x"), field_path=f"{resolved.path}.transform.scale.x")
    yscale = _coerce_optional_scale(scale.get("y"), field_path=f"{resolved.path}.transform.scale.y")
    rotation_raw = transform.get("rotation_degrees")
    rotation = (
        _coerce_real_number(
            rotation_raw, field_path=f"{resolved.path}.transform.rotation", scale=1.0
        )
        if rotation_raw is not None
        else 0.0
    )
    return block_name, insertion, xscale, yscale, rotation


def _coerce_optional_scale(value: Any, *, field_path: str) -> float:
    if value is None:
        return 1.0
    return _coerce_real_number(value, field_path=field_path, scale=1.0)


def _is_supported_layout_name(name: str) -> bool:
    if _LAYOUT_NAME_RE.fullmatch(name):
        return True

    normalized = name.strip().lower()
    return normalized in _SUPPORTED_LAYOUT_NAMES


def _extract_line_points(
    *, geometry: Any, entity_path: str, unit_spec: UnitSpec
) -> tuple[tuple[float, float, float], tuple[float, float, float]]:
    if not isinstance(geometry, Mapping):
        if isinstance(geometry, Sequence) and not isinstance(geometry, (str, bytes, bytearray)):
            if len(geometry) == 2:
                start, end = geometry[0], geometry[1]
            elif len(geometry) >= 4:
                start, end = geometry[:2]
            else:
                raise DxfWriteError(
                    code="UNSUPPORTED_GEOMETRY",
                    message=f"Line geometry must be an object at {entity_path}",
                    details={"path": entity_path},
                )
        else:
            raise DxfWriteError(
                code="UNSUPPORTED_GEOMETRY",
                message=f"Line geometry must be an object at {entity_path}",
                details={"path": entity_path},
            )
    else:
        start = geometry.get("start")
        end = geometry.get("end")
        if start is None and isinstance(geometry.get("points"), Sequence):
            points = geometry["points"]
            if len(points) >= 2:
                start, end = points[0], points[1]

    if start is None or end is None:
        raise DxfWriteError(
            code="UNSUPPORTED_GEOMETRY",
            message=f"Line geometry requires start and end points at {entity_path}",
            details={"path": entity_path},
        )

    start_point = _coerce_point(
        start,
        field_path=f"{entity_path}.geometry.start",
        scale=unit_spec.meter_scale,
    )
    end_point = _coerce_point(
        end,
        field_path=f"{entity_path}.geometry.end",
        scale=unit_spec.meter_scale,
    )
    return start_point, end_point


def _extract_polyline_points(
    *, geometry: Any, entity_path: str, unit_spec: UnitSpec
) -> list[tuple[float, float]]:
    raw_points: list[Any] | None
    if isinstance(geometry, list):
        raw_points = geometry
    elif isinstance(geometry, Mapping):
        points_field = geometry.get("points")
        raw_points = points_field if isinstance(points_field, list) else geometry.get("vertices")
    else:
        raise DxfWriteError(
            code="UNSUPPORTED_GEOMETRY",
            message=f"Polyline geometry must be an object at {entity_path}",
            details={"path": entity_path},
        )

    if raw_points is None or not isinstance(raw_points, list):
        raise DxfWriteError(
            code="UNSUPPORTED_GEOMETRY",
            message=f"Polyline geometry requires points at {entity_path}",
            details={"path": entity_path},
        )

    points: list[tuple[float, float]] = []
    for index, raw_point in enumerate(raw_points, start=1):
        point = _coerce_point(
            raw_point,
            field_path=f"{entity_path}.geometry.points[{index}]",
            scale=unit_spec.meter_scale,
        )
        points.append((point[0], point[1]))
    return points


def _coerce_point(value: Any, *, field_path: str, scale: float) -> tuple[float, float, float]:
    z: Any = None
    if isinstance(value, Mapping):
        x = value.get("x")
        y = value.get("y")
        z = value.get("z")
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        if len(value) < 2:
            raise DxfWriteError(
                code="UNSUPPORTED_GEOMETRY",
                message=f"Point must provide at least x/y at {field_path}",
                details={"path": field_path},
            )
        if len(value) > 3:
            raise DxfWriteError(
                code="UNSUPPORTED_GEOMETRY",
                message=f"Point must be 2D at {field_path}",
                details={"path": field_path},
            )
        x, y = value[0], value[1]
        if len(value) == 3:
            z = value[2]
    else:
        raise DxfWriteError(
            code="UNSUPPORTED_GEOMETRY",
            message=f"Point must be an object or sequence at {field_path}",
            details={"path": field_path},
        )

    x_val = _coerce_real_number(x, field_path=f"{field_path}.x", scale=scale)
    y_val = _coerce_real_number(y, field_path=f"{field_path}.y", scale=scale)

    if z is None:
        return x_val, y_val, 0.0

    z_val = _coerce_real_number(z, field_path=f"{field_path}.z", scale=scale)
    if z_val != 0.0:
        raise DxfWriteError(
            code="UNSUPPORTED_GEOMETRY",
            message=f"Point z coordinate must be 0 at {field_path}",
            details={"path": f"{field_path}.z", "value": z},
        )

    return x_val, y_val, z_val


def _has_non_empty_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, Mapping):
        return any(_has_non_empty_value(item) for item in value.values())
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return any(_has_non_empty_value(item) for item in value)
    return True


def _coerce_real_number(value: Any, *, field_path: str, scale: float) -> float:
    if not isinstance(value, int | float):
        raise DxfWriteError(
            code="INVALID_COORDINATE",
            message=f"Coordinate must be numeric at {field_path}",
            details={"path": field_path, "value": value},
        )

    if isinstance(value, bool):
        raise DxfWriteError(
            code="INVALID_COORDINATE",
            message=f"Coordinate must be a real number at {field_path}",
            details={"path": field_path},
        )

    numeric = float(value)
    if not math.isfinite(numeric):
        raise DxfWriteError(
            code="INVALID_COORDINATE",
            message=f"Coordinate must be finite at {field_path}",
            details={"path": field_path},
        )

    return numeric * float(scale)


def _coerce_optional_bool(value: Any, *, field_path: str) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)

    raise DxfWriteError(
        code="UNSUPPORTED_GEOMETRY",
        message=f"Boolean value expected at {field_path}",
        details={"path": field_path},
    )


def _coerce_optional_text(value: Any, *, field_path: str, error_code: str) -> str | None:
    if value is None:
        return None

    if not isinstance(value, str):
        raise DxfWriteError(
            code=error_code,
            message=f"Expected text at {field_path}",
            details={"path": field_path},
        )

    text = value.strip()
    if not text:
        return None
    return text


def _coerce_required_text(value: Any, *, field_path: str, error_code: str) -> str:
    text = _coerce_optional_text(value, field_path=field_path, error_code=error_code)
    if text is None:
        raise DxfWriteError(
            code=error_code,
            message=f"Missing text at {field_path}",
            details={"path": field_path},
        )
    return text


def _coerce_unit_text(value: Any, *, field_path: str) -> str | None:
    text = _coerce_optional_text(
        value,
        field_path=field_path,
        error_code="UNSUPPORTED_UNITS",
    )
    if text is None:
        return None
    return text


def _coerce_optional_identifier(value: Any, *, field_path: str, error_code: str) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise DxfWriteError(
            code=error_code,
            message="Boolean is not a valid identifier",
            details={"path": field_path},
        )
    if not isinstance(value, (str, int)):
        raise DxfWriteError(
            code=error_code,
            message="Identifier must be string or integer",
            details={"path": field_path, "type": type(value).__name__},
        )
    text = str(value).strip()
    if not text:
        return None
    return text


def _coerce_mapping(value: Any, *, field_path: str) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if value is None:
        return {}

    raise DxfWriteError(
        code="UNSUPPORTED_GEOMETRY",
        message=f"Expected object at {field_path}",
        details={"path": field_path, "type": type(value).__name__},
    )


def _merge_entity_mapping(merged: dict[str, Any], value: Any, *, field_path: str) -> None:
    payload_mapping = _coerce_object_mapping(
        value,
        field_path=field_path,
        error_code="UNSUPPORTED_ENTITY_TYPE",
    )
    merged.update(payload_mapping)

    for nested_key in ("canonical_entity_json", "canonical_entity"):
        nested_value = payload_mapping.get(nested_key)
        if nested_value is None:
            continue
        merged.update(
            _coerce_object_mapping(
                nested_value,
                field_path=f"{field_path}.{nested_key}",
                error_code="UNSUPPORTED_ENTITY_TYPE",
            )
        )


def _coerce_object_mapping(value: Any, *, field_path: str, error_code: str) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            loaded = json.loads(value)
        except json.JSONDecodeError as exc:
            raise DxfWriteError(
                code=error_code,
                message=f"Invalid JSON at {field_path}: {exc}",
                details={"path": field_path},
            ) from exc
    else:
        loaded = value

    if not isinstance(loaded, Mapping):
        raise DxfWriteError(
            code=error_code,
            message=f"Expected JSON object at {field_path}",
            details={"path": field_path},
        )

    return dict(loaded)


def _coerce_json_mapping(value: Any, *, field_path: str) -> dict[str, Any]:
    return _coerce_object_mapping(
        value,
        field_path=field_path,
        error_code="UNSUPPORTED_GEOMETRY",
    )


__all__ = [
    "DxfWriteError",
    "DxfWriteOptions",
    "DxfWriteResult",
    "write_canonical_dxf",
]
