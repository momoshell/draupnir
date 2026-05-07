"""Semantic-first IFC ingestion adapter with optional IfcOpenShell runtime."""

from __future__ import annotations

import asyncio
import importlib
import importlib.metadata
import importlib.util
import inspect
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from types import ModuleType
from typing import cast

from app.ingestion.contracts import (
    AdapterAvailability,
    AdapterCapabilities,
    AdapterDescriptor,
    AdapterDiagnostic,
    AdapterExecutionOptions,
    AdapterResult,
    AdapterSource,
    AdapterStatus,
    AdapterWarning,
    AvailabilityReason,
    ConfidenceSummary,
    IngestionAdapter,
    InputFamily,
    JSONValue,
    LicenseState,
    ProbeIssue,
    ProbeKind,
    ProbeObservation,
    ProbeRequirement,
    ProbeStatus,
    ProgressUpdate,
    ProvenanceRecord,
    UploadFormat,
)

_PACKAGE_NAME = "ifcopenshell"
_ADAPTER_KEY = "ifcopenshell"
_SCHEMA_PATTERN = re.compile(r"FILE_SCHEMA\s*\(\s*\(\s*'([^']+)'", re.IGNORECASE)
_SUPPORTED_SCHEMAS = frozenset({"IFC2X3", "IFC4", "IFC4X3"})
_HEADER_READ_LIMIT_BYTES = 64 * 1024
_STEP_HEADER_MARKERS = ("ISO-10303-21", "HEADER;")
_SCHEMA_ALIASES = {
    "IFC2X3TC1": "IFC2X3",
    "IFC2X3CV2": "IFC2X3",
    "IFC4ADD1": "IFC4",
    "IFC4ADD2": "IFC4",
    "IFC4ADD2TC1": "IFC4",
    "IFC4X3ADD1": "IFC4X3",
    "IFC4X3ADD2": "IFC4X3",
    "IFC4X3ADD2TC1": "IFC4X3",
}

_BASE_DESCRIPTOR = AdapterDescriptor(
    key=_ADAPTER_KEY,
    family=InputFamily.IFC,
    upload_formats=(UploadFormat.IFC,),
    display_name="IfcOpenShell semantic IFC adapter",
    module="app.ingestion.adapters.ifcopenshell",
    license_name="LGPL-3.0-or-later",
    capabilities=AdapterCapabilities(
        extracts_materials=True,
        extracts_layers=True,
        supports_quantity_hints=True,
    ),
    probes=(
        ProbeRequirement(
            kind=ProbeKind.PYTHON_PACKAGE,
            name=_PACKAGE_NAME,
            failure_status=AdapterStatus.UNAVAILABLE,
            detail="Install the optional ingestion dependency 'ifcopenshell'.",
        ),
    ),
    notes=(
        (
            "Semantic-first IFC extraction only; tessellation and shape creation are "
            "intentionally disabled."
        ),
    ),
    confidence_range=(0.2, 0.55),
)


@dataclass(frozen=True, slots=True)
class _SchemaSniff:
    schema: str | None
    readable: bool
    supported: bool


@dataclass(slots=True)
class _RawEntity:
    payload: dict[str, JSONValue]
    preferred_id: str
    has_global_id: bool
    step_id_value: int | None


def create_adapter() -> IngestionAdapter:
    """Create an IFC adapter without importing IfcOpenShell at module import time."""

    return IfcOpenShellAdapter()


class IfcOpenShellAdapter(IngestionAdapter):
    """Extract semantic IFC metadata with lazy runtime loading."""

    def __init__(self) -> None:
        self.descriptor = replace(_BASE_DESCRIPTOR, adapter_version=_package_version())

    def probe(self) -> AdapterAvailability:
        """Report runtime availability without importing IfcOpenShell."""

        started_at = perf_counter()
        observed_at = datetime.now(UTC)
        spec = _find_runtime_spec()
        elapsed_ms = (perf_counter() - started_at) * 1000.0

        if spec is None:
            observation = ProbeObservation(
                kind=ProbeKind.PYTHON_PACKAGE,
                name=_PACKAGE_NAME,
                status=ProbeStatus.MISSING,
                detail="Optional runtime package is not installed.",
            )
            issue = ProbeIssue(
                kind=ProbeKind.PYTHON_PACKAGE,
                name=_PACKAGE_NAME,
                observed_status=ProbeStatus.MISSING,
                adapter_status=AdapterStatus.UNAVAILABLE,
                detail=(
                    "Install draupnir[ingestion] or the ifcopenshell wheel to "
                    "enable IFC parsing."
                ),
            )
            return AdapterAvailability(
                status=AdapterStatus.UNAVAILABLE,
                availability_reason=AvailabilityReason.PROBE_FAILED,
                license_state=LicenseState.NOT_REQUIRED,
                issues=(issue,),
                observed=(observation,),
                last_checked_at=observed_at,
                details={"package": _PACKAGE_NAME},
                probe_elapsed_ms=elapsed_ms,
            )

        details: dict[str, JSONValue] = {"package": _PACKAGE_NAME}
        version = _package_version()
        if version is not None:
            details["version"] = version
        return AdapterAvailability(
            status=AdapterStatus.AVAILABLE,
            license_state=LicenseState.NOT_REQUIRED,
            observed=(
                ProbeObservation(
                    kind=ProbeKind.PYTHON_PACKAGE,
                    name=_PACKAGE_NAME,
                    status=ProbeStatus.AVAILABLE,
                    detail=version,
                ),
            ),
            last_checked_at=observed_at,
            details=details,
            probe_elapsed_ms=elapsed_ms,
        )

    async def ingest(
        self,
        source: AdapterSource,
        options: AdapterExecutionOptions,
    ) -> AdapterResult:
        """Extract semantic IFC project/product metadata."""

        started_at = perf_counter()
        warnings: list[AdapterWarning] = []
        diagnostics: list[AdapterDiagnostic] = []
        provenance: list[ProvenanceRecord] = []
        source_ref = _build_source_ref(source)

        await _checkpoint(options, started_at)
        await _emit_progress(
            options,
            ProgressUpdate(
                stage="sniff_header",
                message="Inspecting IFC STEP header",
                percent=0.10,
            ),
        )

        sniff_started_at = perf_counter()
        sniff = _sniff_step_header(source.file_path)
        diagnostics.append(
            AdapterDiagnostic(
                code="ifc.schema_sniff",
                message="Inspected IFC STEP header for schema metadata.",
                details={
                    "ifc_schema": sniff.schema,
                    "readable": sniff.readable,
                    "supported": sniff.supported,
                },
                elapsed_ms=(perf_counter() - sniff_started_at) * 1000.0,
            )
        )
        provenance.append(
            ProvenanceRecord(
                stage="sniff_header",
                adapter_key=self.descriptor.key,
                source_ref=source_ref,
                details={
                    "ifc_schema": sniff.schema,
                    "readable": sniff.readable,
                    "supported": sniff.supported,
                },
            )
        )

        if sniff.readable and (sniff.schema is None or not sniff.supported):
            warnings.extend(_schema_warnings(sniff))
            diagnostics.append(
                AdapterDiagnostic(
                    code="ifc.semantic_extract",
                    message=(
                        "Skipped native IFC parsing because schema metadata was "
                        "missing or unsupported."
                    ),
                    details={
                        "ifc_schema": sniff.schema,
                        "native_runtime_used": False,
                    },
                )
            )
            provenance.append(
                ProvenanceRecord(
                    stage="semantic_extract",
                    adapter_key=self.descriptor.key,
                    source_ref=source_ref,
                    details={
                        "ifc_schema": sniff.schema,
                        "native_runtime_used": False,
                        "entity_count": 0,
                    },
                )
            )
            return AdapterResult(
                canonical=_build_canonical(
                    schema=sniff.schema,
                    project_metadata={},
                    entities=(),
                    layer_names=(),
                    units={"normalized": "meter", "source": "default", "assumed": True},
                ),
                provenance=tuple(provenance),
                confidence=ConfidenceSummary(
                    score=0.2,
                    review_required=True,
                    basis="semantic_ifc_metadata_only",
                ),
                warnings=tuple(warnings),
                diagnostics=tuple(diagnostics),
            )

        await _checkpoint(options, started_at)
        await _emit_progress(
            options,
            ProgressUpdate(stage="open_model", message="Opening IFC model", percent=0.35),
        )

        runtime = _load_runtime_module()
        model = runtime.open(str(source.file_path))
        runtime_schema = _normalize_schema(_maybe_call_or_value(getattr(model, "schema", None)))

        await _checkpoint(options, started_at)
        await _emit_progress(
            options,
            ProgressUpdate(
                stage="extract_semantics",
                message="Extracting semantic IFC entities",
                percent=0.65,
            ),
        )

        products = [
            product
            for product in _sequence(_model_by_type(model, "IfcProduct"))
            if not _matches_ifc_type(product, "IfcProject")
        ]
        project = _first(_sequence(_model_by_type(model, "IfcProject")))
        util_element = _resolve_element_module(runtime)

        raw_entities = [
            _extract_entity_payload(
                product=product,
                util_element=util_element,
                warnings=warnings,
            )
            for product in products
        ]
        entities = _finalize_entity_ids(raw_entities)
        layer_names = tuple(_unique_strings(entity.get("layer") for entity in entities))
        project_metadata = _extract_project_metadata(project)
        units = _extract_units(project)

        if units.get("assumed") is True:
            warnings.append(
                AdapterWarning(
                    code="ifc.units_assumed",
                    message=(
                        "IFC length units were not explicit; canonical units "
                        "defaulted to meters."
                    ),
                )
            )
        if not entities:
            warnings.append(
                AdapterWarning(
                    code="ifc.no_products",
                    message="No concrete IfcProduct entities were found in the IFC model.",
                )
            )

        diagnostics.append(
            AdapterDiagnostic(
                code="ifc.semantic_extract",
                message="Extracted semantic IFC project and product metadata.",
                details={
                    "ifc_schema": runtime_schema or sniff.schema,
                    "entity_count": len(entities),
                    "project_present": project is not None,
                    "runtime_version": self.descriptor.adapter_version,
                },
            )
        )
        provenance.append(
            ProvenanceRecord(
                stage="semantic_extract",
                adapter_key=self.descriptor.key,
                source_ref=source_ref,
                details={
                    "ifc_schema": runtime_schema or sniff.schema,
                    "entity_count": len(entities),
                    "runtime_version": self.descriptor.adapter_version,
                },
            )
        )

        await _checkpoint(options, started_at)
        await _emit_progress(
            options,
            ProgressUpdate(
                stage="finalize",
                message="Finalizing canonical IFC payload",
                percent=0.90,
            ),
        )

        return AdapterResult(
            canonical=_build_canonical(
                schema=runtime_schema or sniff.schema,
                project_metadata=project_metadata,
                entities=tuple(entities),
                layer_names=layer_names,
                units=units,
            ),
            provenance=tuple(provenance),
            confidence=ConfidenceSummary(
                score=0.4,
                review_required=True,
                basis="semantic_ifc_metadata_only",
            ),
            warnings=tuple(warnings),
            diagnostics=tuple(diagnostics),
        )


def _find_runtime_spec() -> importlib.machinery.ModuleSpec | None:
    return importlib.util.find_spec(_PACKAGE_NAME)


def _package_version() -> str | None:
    try:
        return importlib.metadata.version(_PACKAGE_NAME)
    except importlib.metadata.PackageNotFoundError:
        return None


def _load_runtime_module() -> ModuleType:
    return importlib.import_module(_PACKAGE_NAME)


async def _checkpoint(options: AdapterExecutionOptions, started_at: float) -> None:
    cancellation = options.cancellation
    if cancellation is not None and cancellation.is_cancelled():
        raise asyncio.CancelledError

    timeout = options.timeout
    if timeout is not None and perf_counter() - started_at > timeout.seconds:
        raise TimeoutError("IfcOpenShell adapter timed out.")


async def _emit_progress(options: AdapterExecutionOptions, update: ProgressUpdate) -> None:
    callback = options.on_progress
    if callback is None:
        return
    result = callback(update)
    if inspect.isawaitable(result):
        await cast("asyncio.Future[object]", result)


def _sniff_step_header(file_path: Path) -> _SchemaSniff:
    try:
        with file_path.open("rb") as stream:
            header_bytes = stream.read(_HEADER_READ_LIMIT_BYTES)
    except OSError:
        return _SchemaSniff(schema=None, readable=False, supported=False)

    header_text = header_bytes.decode("utf-8", errors="ignore")
    upper_header = header_text.upper()
    if not all(marker in upper_header for marker in _STEP_HEADER_MARKERS):
        return _SchemaSniff(schema=None, readable=False, supported=False)

    match = _SCHEMA_PATTERN.search(header_text)
    if match is None:
        return _SchemaSniff(schema=None, readable=True, supported=False)

    schema = _normalize_schema(match.group(1))
    return _SchemaSniff(
        schema=schema,
        readable=True,
        supported=schema in _SUPPORTED_SCHEMAS,
    )


def _schema_warnings(sniff: _SchemaSniff) -> list[AdapterWarning]:
    if sniff.schema is None:
        return [
            AdapterWarning(
                code="ifc.schema_missing",
                message="IFC STEP header did not declare FILE_SCHEMA metadata.",
            )
        ]
    return [
        AdapterWarning(
            code="ifc.schema_unsupported",
            message=f"IFC schema '{sniff.schema}' is not supported by the semantic IFC adapter.",
            details={"ifc_schema": sniff.schema},
        )
    ]


def _normalize_schema(schema: object) -> str | None:
    if not isinstance(schema, str):
        return None
    normalized = (
        schema.strip().upper().replace(" ", "").replace("_", "").replace("-", "")
    )
    if not normalized:
        return None
    if normalized in _SUPPORTED_SCHEMAS:
        return normalized
    alias = _SCHEMA_ALIASES.get(normalized)
    if alias is not None:
        return alias
    return normalized


def _build_source_ref(source: AdapterSource) -> str:
    original_name = source.original_name
    if isinstance(original_name, str) and original_name.strip():
        candidate = original_name.strip().replace("\\", "/")
        name = Path(candidate).name
    else:
        name = source.file_path.name
    return f"originals/{name}"


def _build_canonical(
    *,
    schema: str | None,
    project_metadata: Mapping[str, JSONValue],
    entities: Sequence[Mapping[str, JSONValue]],
    layer_names: Sequence[str],
    units: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    project_payload = dict(project_metadata)
    metadata: dict[str, JSONValue] = {"project": project_payload}
    canonical: dict[str, JSONValue] = {
        "units": dict(units),
        "coordinate_system": {"name": "local", "source": "semantic_ifc_metadata"},
        "layouts": (),
        "layers": tuple({"name": name} for name in layer_names),
        "blocks": (),
        "entities": tuple(dict(entity) for entity in entities),
        "xrefs": (),
        "metadata": metadata,
        "project": project_payload,
    }
    if schema is not None:
        canonical["ifc_schema"] = schema
        metadata["ifc_schema"] = schema
    return canonical


def _model_by_type(model: object, ifc_type: str) -> object:
    by_type = getattr(model, "by_type", None)
    if not callable(by_type):
        return ()
    return by_type(ifc_type)


def _resolve_element_module(runtime: ModuleType) -> object | None:
    util_module = getattr(runtime, "util", None)
    if util_module is not None:
        element_module = getattr(util_module, "element", None)
        if element_module is not None:
            return cast(object, element_module)
    try:
        return importlib.import_module("ifcopenshell.util.element")
    except ModuleNotFoundError:
        return None


def _extract_entity_payload(
    *,
    product: object,
    util_element: object | None,
    warnings: list[AdapterWarning],
) -> _RawEntity:
    ifc_type = _entity_type(product)
    global_id = _string_or_none(getattr(product, "GlobalId", None))
    step_id_value = _step_id_value(product)
    preferred_id = global_id or _step_id_token(step_id_value) or f"anonymous-{ifc_type}"
    step_id_token = _step_id_token(step_id_value)
    try:
        psets = _extract_property_sets(product=product, util_element=util_element, qtos_only=False)
    except Exception:
        psets = ()
        warnings.append(
            _build_helper_warning(
                code="ifc.psets_unavailable",
                message=(
                    "IfcOpenShell property-set helper failed; continuing without "
                    "property sets."
                ),
                ifc_type=ifc_type,
                step_id_token=step_id_token,
            )
        )
    try:
        qtos = _extract_property_sets(product=product, util_element=util_element, qtos_only=True)
    except Exception:
        qtos = ()
        warnings.append(
            _build_helper_warning(
                code="ifc.qtos_unavailable",
                message=(
                    "IfcOpenShell quantity helper failed; continuing without "
                    "quantity metadata."
                ),
                ifc_type=ifc_type,
                step_id_token=step_id_token,
            )
        )
    try:
        material_refs = _extract_material_refs(product=product, util_element=util_element)
    except Exception:
        material_refs = ()
        warnings.append(
            _build_helper_warning(
                code="ifc.material_refs_unavailable",
                message=(
                    "IfcOpenShell material helper failed; continuing without "
                    "material references."
                ),
                ifc_type=ifc_type,
                step_id_token=step_id_token,
            )
        )
    payload: dict[str, JSONValue] = {
        "id": preferred_id,
        "kind": "ifc_product",
        "ifc_type": ifc_type,
        "layer": ifc_type,
        "name": _string_or_none(getattr(product, "Name", None)),
        "description": _string_or_none(getattr(product, "Description", None)),
        "object_type": _string_or_none(getattr(product, "ObjectType", None)),
        "predefined_type": _string_or_none(
            _maybe_call_or_value(getattr(product, "PredefinedType", None))
        ),
        "ifc_global_id": global_id,
        "ifc_step_id": _step_id_token(step_id_value),
        "representation": _extract_representation_metadata(
            getattr(product, "Representation", None)
        ),
        "psets": psets,
        "qtos": qtos,
        "material_refs": material_refs,
    }
    return _RawEntity(
        payload={key: value for key, value in payload.items() if value is not None},
        preferred_id=preferred_id,
        has_global_id=global_id is not None,
        step_id_value=step_id_value,
    )


def _build_helper_warning(
    *,
    code: str,
    message: str,
    ifc_type: str,
    step_id_token: str | None,
) -> AdapterWarning:
    details: dict[str, JSONValue] = {"ifc_type": ifc_type}
    if step_id_token is not None:
        details["ifc_step_id"] = step_id_token
    return AdapterWarning(code=code, message=message, details=details)


def _finalize_entity_ids(raw_entities: Sequence[_RawEntity]) -> tuple[dict[str, JSONValue], ...]:
    ordered = sorted(
        raw_entities,
        key=lambda entity: (
            0 if entity.has_global_id else 1,
            entity.preferred_id,
            entity.step_id_value if entity.step_id_value is not None else 10**12,
        ),
    )
    seen: dict[str, int] = {}
    finalized: list[dict[str, JSONValue]] = []
    for entity in ordered:
        count = seen.get(entity.preferred_id, 0) + 1
        seen[entity.preferred_id] = count
        payload = dict(entity.payload)
        payload["id"] = entity.preferred_id if count == 1 else f"{entity.preferred_id}-{count}"
        finalized.append(payload)
    return tuple(finalized)


def _extract_project_metadata(project: object | None) -> dict[str, JSONValue]:
    if project is None:
        return {}
    payload: dict[str, JSONValue] = {
        "ifc_type": _entity_type(project),
        "name": _string_or_none(getattr(project, "Name", None)),
        "description": _string_or_none(getattr(project, "Description", None)),
        "long_name": _string_or_none(getattr(project, "LongName", None)),
        "phase": _string_or_none(getattr(project, "Phase", None)),
        "global_id": _string_or_none(getattr(project, "GlobalId", None)),
    }
    return {key: value for key, value in payload.items() if value is not None}


def _extract_units(project: object | None) -> dict[str, JSONValue]:
    unit_value = _string_or_none(getattr(project, "length_unit", None))
    if unit_value is None:
        units_in_context = getattr(project, "UnitsInContext", None)
        if units_in_context is not None:
            unit_value = _string_or_none(getattr(units_in_context, "length_unit", None))

    normalized = _normalize_length_unit(unit_value)
    if normalized is not None:
        return {"normalized": normalized, "source": unit_value or normalized, "assumed": False}
    return {"normalized": "meter", "source": "default", "assumed": True}


def _normalize_length_unit(unit_value: str | None) -> str | None:
    if unit_value is None:
        return None
    normalized = unit_value.strip().lower()
    if normalized in {"m", "meter", "metre", "meters", "metres"}:
        return "meter"
    if normalized in {"mm", "millimeter", "millimetre", "millimeters", "millimetres"}:
        return "millimeter"
    if normalized in {"cm", "centimeter", "centimetre", "centimeters", "centimetres"}:
        return "centimeter"
    if normalized in {"ft", "foot", "feet"}:
        return "foot"
    if normalized in {"in", "inch", "inches"}:
        return "inch"
    return None


def _extract_representation_metadata(representation: object) -> dict[str, JSONValue]:
    if representation is None:
        return {"has_representation": False, "representations": (), "representation_count": 0}

    raw_representations = getattr(representation, "Representations", None)
    representations = _sequence(raw_representations)
    payload = tuple(
        {
            "representation_identifier": _string_or_none(
                getattr(item, "RepresentationIdentifier", None)
            ),
            "representation_type": _string_or_none(getattr(item, "RepresentationType", None)),
            "item_count": len(_sequence(getattr(item, "Items", None))),
        }
        for item in representations
    )
    return {
        "has_representation": bool(representations),
        "representations": payload,
        "representation_count": len(representations),
    }


def _extract_property_sets(
    *,
    product: object,
    util_element: object | None,
    qtos_only: bool,
) -> tuple[dict[str, JSONValue], ...]:
    direct_attribute_name = "qtos" if qtos_only else "psets"
    direct_payload = getattr(product, direct_attribute_name, None)
    if isinstance(direct_payload, Mapping):
        return _mapping_to_named_records(direct_payload)

    if util_element is None:
        return ()
    get_psets = getattr(util_element, "get_psets", None)
    if not callable(get_psets):
        return ()

    try:
        raw_payload = get_psets(
            product,
            psets_only=not qtos_only,
            qtos_only=qtos_only,
            should_inherit=False,
        )
    except TypeError:
        raw_payload = get_psets(product)
    if not isinstance(raw_payload, Mapping):
        return ()
    return _mapping_to_named_records(raw_payload)


def _extract_material_refs(
    *,
    product: object,
    util_element: object | None,
) -> tuple[dict[str, JSONValue], ...]:
    direct_payload = getattr(product, "material_refs", None)
    if direct_payload is not None:
        return tuple(_material_to_record(material) for material in _sequence(direct_payload))

    if util_element is None:
        return ()
    get_materials = getattr(util_element, "get_materials", None)
    if callable(get_materials):
        raw_materials = get_materials(product)
        return tuple(_material_to_record(material) for material in _sequence(raw_materials))
    return ()


def _mapping_to_named_records(payload: Mapping[object, object]) -> tuple[dict[str, JSONValue], ...]:
    records: list[dict[str, JSONValue]] = []
    for name, value in sorted(payload.items(), key=lambda item: str(item[0])):
        record: dict[str, JSONValue] = {"name": str(name)}
        if isinstance(value, Mapping):
            record["properties"] = _json_mapping(value)
        else:
            record["value"] = _json_value(value)
        records.append(record)
    return tuple(records)


def _material_to_record(material: object) -> dict[str, JSONValue]:
    if isinstance(material, Mapping):
        return _json_mapping(material)

    payload: dict[str, JSONValue] = {
        "name": _string_or_none(getattr(material, "Name", None)) or str(material),
        "ifc_type": _entity_type(material),
        "category": _string_or_none(getattr(material, "Category", None)),
    }
    return {key: value for key, value in payload.items() if value is not None}


def _json_mapping(payload: Mapping[object, object]) -> dict[str, JSONValue]:
    return {
        str(key): _json_value(value)
        for key, value in sorted(payload.items(), key=lambda item: str(item[0]))
    }


def _json_value(value: object) -> JSONValue:
    if value is None or isinstance(value, (str, int, float, bool)):
        return cast(JSONValue, value)
    if isinstance(value, Mapping):
        return cast(JSONValue, _json_mapping(value))
    if isinstance(value, (list, tuple, set)):
        return cast(JSONValue, tuple(_json_value(item) for item in value))
    return cast(JSONValue, str(value))


def _entity_type(entity: object) -> str:
    is_a = getattr(entity, "is_a", None)
    if callable(is_a):
        result = is_a()
        if isinstance(result, str):
            return result
    return entity.__class__.__name__


def _matches_ifc_type(entity: object, type_name: str) -> bool:
    is_a = getattr(entity, "is_a", None)
    if callable(is_a):
        try:
            result = is_a(type_name)
        except TypeError:
            result = None
        if isinstance(result, bool):
            return result
    return _entity_type(entity) == type_name


def _step_id_value(entity: object) -> int | None:
    step_id = getattr(entity, "id", None)
    value = step_id() if callable(step_id) else step_id
    return value if isinstance(value, int) else None


def _step_id_token(step_id: int | None) -> str | None:
    return f"#{step_id}" if step_id is not None else None


def _string_or_none(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return str(value)


def _maybe_call_or_value(value: object) -> object:
    if callable(value):
        result: object = value()
        return result
    return value


def _sequence(value: object) -> tuple[object, ...]:
    if value is None:
        return ()
    if isinstance(value, tuple):
        return value
    if isinstance(value, list):
        return tuple(value)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(value)
    return tuple(cast(Sequence[object], value))


def _first(values: Sequence[object]) -> object | None:
    return values[0] if values else None


def _unique_strings(values: Iterable[object]) -> tuple[str, ...]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if not isinstance(value, str) or value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return tuple(ordered)
