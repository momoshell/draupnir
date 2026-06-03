"""Write-only revised DXF export adapter."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from app.cad.dxf import write_canonical_dxf
from app.ingestion.contracts import (
    AdapterAvailability,
    AdapterCapabilities,
    AdapterDescriptor,
    AdapterDiagnostic,
    AdapterExecutionOptions,
    AdapterExportRequest,
    AdapterExportResult,
    AdapterStatus,
    AdapterWarning,
    ExportAdapter,
    InputFamily,
    JSONValue,
    ProbeKind,
    ProbeRequirement,
    UploadFormat,
)

_OUTPUT_FORMAT = "revised_dxf"
_MEDIA_TYPE = "application/dxf"

DESCRIPTOR = AdapterDescriptor(
    key="ezdxf_writer",
    family=InputFamily.DXF,
    upload_formats=(UploadFormat.DXF,),
    output_formats=(_OUTPUT_FORMAT,),
    display_name="ezdxf DXF writer",
    module="app.ingestion.adapters.ezdxf_writer",
    license_name="MIT",
    capabilities=AdapterCapabilities(
        can_read=False,
        can_write=True,
        extracts_canonical=False,
        extracts_provenance=False,
        extracts_confidence=False,
        extracts_warnings=False,
        extracts_diagnostics=False,
        supports_exports=True,
    ),
    confidence_range=(1.0, 1.0),
    probes=(
        ProbeRequirement(
            kind=ProbeKind.PYTHON_PACKAGE,
            name="ezdxf",
            failure_status=AdapterStatus.UNAVAILABLE,
            detail="The ezdxf package is required to write DXF exports.",
        ),
    ),
    notes=("Write-only revised DXF export adapter backed by the pure DXF writer.",),
)


def _normalize_json_value(
    value: object,
) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _normalize_json_value(nested) for key, nested in value.items()}
    if isinstance(value, tuple):
        return [_normalize_json_value(item) for item in value]
    if isinstance(value, list):
        return [_normalize_json_value(item) for item in value]
    return value


def _normalize_canonical_payload(canonical: Mapping[str, JSONValue]) -> dict[str, Any]:
    payload = {str(key): _normalize_json_value(value) for key, value in canonical.items()}
    units = payload.get("units")
    if isinstance(units, Mapping):
        normalized_unit = units.get("normalized")
        if isinstance(normalized_unit, str) and normalized_unit:
            payload["units"] = normalized_unit
    return payload


def _build_warnings(result: Any) -> tuple[AdapterWarning, ...]:
    return tuple(
        AdapterWarning(code="dxf_write_warning", message=warning) for warning in result.warnings
    )


def _build_diagnostics(result: Any) -> tuple[AdapterDiagnostic, ...]:
    diagnostics: list[AdapterDiagnostic] = []
    for diagnostic in result.diagnostics:
        message = diagnostic.get("message")
        if not isinstance(message, str) or not message:
            message = "DXF writer diagnostic"
        diagnostics.append(
            AdapterDiagnostic(
                code="dxf_write_diagnostic",
                message=message,
                details=diagnostic,
            )
        )
    return tuple(diagnostics)


class EzdxfWriterAdapter(ExportAdapter):
    descriptor = DESCRIPTOR
    version = "1"

    def probe(self) -> AdapterAvailability:
        return AdapterAvailability(status=AdapterStatus.AVAILABLE)

    async def export(
        self,
        request: AdapterExportRequest,
        options: AdapterExecutionOptions,
    ) -> AdapterExportResult:
        del options

        if request.output_format != _OUTPUT_FORMAT:
            raise ValueError(f"Unsupported DXF export format '{request.output_format}'.")

        write_result = write_canonical_dxf(_normalize_canonical_payload(request.canonical))
        return AdapterExportResult(
            output_format=_OUTPUT_FORMAT,
            media_type=_MEDIA_TYPE,
            content=write_result.content,
            warnings=_build_warnings(write_result),
            diagnostics=_build_diagnostics(write_result),
        )


def create_export_adapter() -> ExportAdapter:
    return EzdxfWriterAdapter()
