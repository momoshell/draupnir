"""Tests for reusable ingestion adapter contract harness helpers."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, cast

import pytest
import yaml  # type: ignore[import-untyped]

from app.ingestion.contracts import (
    AdapterAvailability,
    AdapterDiagnostic,
    AdapterExecutionOptions,
    AdapterResult,
    AdapterSource,
    AdapterWarning,
    IngestionAdapter,
    InputFamily,
)
from tests.ingestion_contract_harness import (
    AdapterContractFailureKind,
    ContractFinalizationExpectation,
    available_probe,
    build_complete_canonical,
    build_contract_source,
    build_descriptor,
    build_result,
    exercise_adapter_contract,
    exercise_adapter_failure,
)


def _load_fixture_manifest() -> dict[str, Any]:
    manifest_path = Path(__file__).parent / "fixtures" / "manifest.yaml"
    payload = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise AssertionError("Fixture manifest must parse to a mapping.")
    return payload


class _ResultAdapter(IngestionAdapter):
    def __init__(self, *, result: AdapterResult, family: InputFamily, key: str) -> None:
        self._result = result
        self.descriptor = build_descriptor(key=key, family=family)

    def probe(self) -> AdapterAvailability:
        return available_probe()

    async def ingest(
        self,
        source: AdapterSource,
        options: AdapterExecutionOptions,
    ) -> AdapterResult:
        _ = (source, options)
        return self._result


class _FailingAdapter(IngestionAdapter):
    def __init__(self, *, family: InputFamily, key: str, mode: str) -> None:
        self.descriptor = build_descriptor(key=key, family=family)
        self._mode = mode

    def probe(self) -> AdapterAvailability:
        return available_probe()

    async def ingest(
        self,
        source: AdapterSource,
        options: AdapterExecutionOptions,
    ) -> AdapterResult:
        _ = source
        if self._mode == "unsupported":
            raise NotImplementedError("unsupported")
        if self._mode == "dependency":
            raise ModuleNotFoundError(name="ifcopenshell")
        if self._mode == "timeout":
            await asyncio.sleep(0.05)
            raise AssertionError("timeout guard should trigger before this line")
        if self._mode == "cancelled":
            if options.cancellation is not None and options.cancellation.is_cancelled():
                raise asyncio.CancelledError
            raise AssertionError("cancelled mode requires cancellation handle")
        raise RuntimeError("unexpected")


class _AlwaysCancelled:
    def is_cancelled(self) -> bool:
        return True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("score", "validation_status", "review_state", "quantity_gate"),
    [
        (0.97, "valid", "approved", "allowed"),
        (0.70, "valid", "provisional", "allowed_provisional"),
        (0.40, "needs_review", "review_required", "review_gated"),
    ],
)
async def test_contract_harness_applies_review_thresholds(
    tmp_path: Path,
    score: float,
    validation_status: str,
    review_state: str,
    quantity_gate: str,
) -> None:
    source_path = tmp_path / "source.dxf"
    source_path.write_text("0\nSECTION\n2\nENTITIES\n0\nENDSEC\n0\nEOF\n", encoding="utf-8")
    source = build_contract_source(file_path=source_path)
    adapter_key = "fake-dxf"
    adapter = _ResultAdapter(
        result=build_result(
            adapter_key=adapter_key,
            score=score,
            canonical=build_complete_canonical(),
        ),
        family=InputFamily.DXF,
        key=adapter_key,
    )

    payload = await exercise_adapter_contract(
        adapter,
        source=source,
        input_family=InputFamily.DXF,
        adapter_key=adapter_key,
        expectation=ContractFinalizationExpectation(
            validation_status=validation_status,
            review_state=review_state,
            quantity_gate=quantity_gate,
        ),
    )

    assert payload.input_family == InputFamily.DXF.value
    assert payload.provenance_json["records"][0]["adapter_key"] == adapter_key


@pytest.mark.asyncio
async def test_contract_harness_asserts_warnings_and_diagnostics_shape(tmp_path: Path) -> None:
    source_path = tmp_path / "source.dxf"
    source_path.write_text("0\nSECTION\n2\nENTITIES\n0\nENDSEC\n0\nEOF\n", encoding="utf-8")
    source = build_contract_source(file_path=source_path)
    adapter_key = "fake-dxf"
    warnings = (
        AdapterWarning(code="layer-map", message="Layer map incomplete"),
        AdapterWarning(code="xref", message="Xref unresolved"),
    )
    diagnostics = (
        AdapterDiagnostic(code="probe.elapsed", message="Probe completed", elapsed_ms=5.0),
    )
    adapter = _ResultAdapter(
        result=build_result(
            adapter_key=adapter_key,
            score=0.97,
            canonical=build_complete_canonical(),
            warnings=warnings,
            diagnostics=diagnostics,
        ),
        family=InputFamily.DXF,
        key=adapter_key,
    )

    payload = await exercise_adapter_contract(
        adapter,
        source=source,
        input_family=InputFamily.DXF,
        adapter_key=adapter_key,
        expectation=ContractFinalizationExpectation(
            validation_status="valid_with_warnings",
            review_state="approved",
            quantity_gate="allowed",
            warning_codes=("layer-map", "xref"),
            diagnostic_codes=("probe.elapsed",),
        ),
    )

    assert payload.report_json["adapter_warnings"][0]["code"] == "layer-map"
    assert payload.diagnostics_json["diagnostics"][0]["code"] == "probe.elapsed"


@pytest.mark.asyncio
async def test_contract_harness_rejects_missing_entities_key(tmp_path: Path) -> None:
    source_path = tmp_path / "source.dxf"
    source_path.write_text("0\nSECTION\n2\nENTITIES\n0\nENDSEC\n0\nEOF\n", encoding="utf-8")
    source = build_contract_source(file_path=source_path)
    adapter_key = "fake-dxf"
    adapter = _ResultAdapter(
        result=build_result(
            adapter_key=adapter_key,
            score=0.97,
            canonical={"units": {"normalized": "meter"}},
        ),
        family=InputFamily.DXF,
        key=adapter_key,
    )

    with pytest.raises(AssertionError):
        await exercise_adapter_contract(
            adapter,
            source=source,
            input_family=InputFamily.DXF,
            adapter_key=adapter_key,
            expectation=ContractFinalizationExpectation(
                validation_status="valid",
                review_state="approved",
                quantity_gate="allowed",
            ),
        )


@pytest.mark.asyncio
async def test_contract_harness_rejects_missing_entity_envelope_fields(tmp_path: Path) -> None:
    source_path = tmp_path / "source.dxf"
    source_path.write_text("0\nSECTION\n2\nENTITIES\n0\nENDSEC\n0\nEOF\n", encoding="utf-8")
    source = build_contract_source(file_path=source_path)
    adapter_key = "fake-dxf"
    canonical = build_complete_canonical()
    canonical["entities"] = (
        {
            "kind": "line",
            "layer": "A-WALL",
            "start": {"x": 0.0, "y": 0.0},
            "end": {"x": 10.0, "y": 0.0},
        },
    )
    adapter = _ResultAdapter(
        result=build_result(
            adapter_key=adapter_key,
            score=0.97,
            canonical=canonical,
        ),
        family=InputFamily.DXF,
        key=adapter_key,
    )

    with pytest.raises(AssertionError, match="entity_id"):
        await exercise_adapter_contract(
            adapter,
            source=source,
            input_family=InputFamily.DXF,
            adapter_key=adapter_key,
            expectation=ContractFinalizationExpectation(
                validation_status="valid",
                review_state="approved",
                quantity_gate="allowed",
            ),
        )


@pytest.mark.asyncio
async def test_contract_harness_rejects_empty_entities_without_explicit_reason(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "source.dxf"
    source_path.write_text("0\nSECTION\n2\nENTITIES\n0\nENDSEC\n0\nEOF\n", encoding="utf-8")
    source = build_contract_source(file_path=source_path)
    adapter_key = "fake-dxf"
    canonical = build_complete_canonical()
    canonical["entities"] = ()
    adapter = _ResultAdapter(
        result=build_result(
            adapter_key=adapter_key,
            score=0.97,
            canonical=canonical,
        ),
        family=InputFamily.DXF,
        key=adapter_key,
    )

    with pytest.raises(AssertionError, match="metadata with a reason"):
        await exercise_adapter_contract(
            adapter,
            source=source,
            input_family=InputFamily.DXF,
            adapter_key=adapter_key,
            expectation=ContractFinalizationExpectation(
                validation_status="valid",
                review_state="approved",
                quantity_gate="allowed",
            ),
        )


@pytest.mark.asyncio
async def test_contract_harness_rejects_absent_geometry_without_explicit_reason(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "source.dxf"
    source_path.write_text("0\nSECTION\n2\nENTITIES\n0\nENDSEC\n0\nEOF\n", encoding="utf-8")
    source = build_contract_source(file_path=source_path)
    adapter_key = "fake-dxf"
    canonical = build_complete_canonical()
    entity = cast(tuple[dict[str, Any], ...], canonical["entities"])[0]
    canonical["entities"] = (
        {
            **entity,
            "geometry": {
                "bbox": None,
                "units": {"normalized": "meter"},
                "status": "absent",
                "geometry_summary": {"kind": "line_segment"},
            },
        },
    )
    adapter = _ResultAdapter(
        result=build_result(
            adapter_key=adapter_key,
            score=0.97,
            canonical=canonical,
        ),
        family=InputFamily.DXF,
        key=adapter_key,
    )

    with pytest.raises(AssertionError, match="geometry reason"):
        await exercise_adapter_contract(
            adapter,
            source=source,
            input_family=InputFamily.DXF,
            adapter_key=adapter_key,
            expectation=ContractFinalizationExpectation(
                validation_status="valid",
                review_state="approved",
                quantity_gate="allowed",
            ),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode", "expected_kind"),
    [
        ("unsupported", AdapterContractFailureKind.UNSUPPORTED),
        ("dependency", AdapterContractFailureKind.DEPENDENCY_MISSING),
        ("timeout", AdapterContractFailureKind.TIMEOUT),
    ],
)
async def test_contract_harness_classifies_common_failure_modes(
    tmp_path: Path,
    mode: str,
    expected_kind: AdapterContractFailureKind,
) -> None:
    source_path = tmp_path / "source.dxf"
    source_path.write_text("0\nSECTION\n2\nENTITIES\n0\nENDSEC\n0\nEOF\n", encoding="utf-8")
    source = build_contract_source(file_path=source_path)
    adapter = _FailingAdapter(family=InputFamily.DXF, key="failing-dxf", mode=mode)

    failure = await exercise_adapter_failure(adapter, source=source)

    assert failure.kind is expected_kind


@pytest.mark.asyncio
async def test_contract_harness_classifies_cancellation(tmp_path: Path) -> None:
    source_path = tmp_path / "source.dxf"
    source_path.write_text("0\nSECTION\n2\nENTITIES\n0\nENDSEC\n0\nEOF\n", encoding="utf-8")
    source = build_contract_source(file_path=source_path)
    adapter = _FailingAdapter(family=InputFamily.DXF, key="failing-dxf", mode="cancelled")

    failure = await exercise_adapter_failure(
        adapter,
        source=source,
        options=AdapterExecutionOptions(cancellation=_AlwaysCancelled()),
    )

    assert failure.kind is AdapterContractFailureKind.CANCELLED


def test_fixture_manifest_has_multi_format_smoke_contract_coverage() -> None:
    manifest = _load_fixture_manifest()
    fixtures_payload = manifest.get("fixtures")
    if not isinstance(fixtures_payload, list):
        raise AssertionError("Fixture manifest fixtures list is missing.")

    fixtures_by_filename: dict[str, dict[str, Any]] = {
        fixture["filename"]: fixture
        for fixture in fixtures_payload
        if isinstance(fixture, dict) and isinstance(fixture.get("filename"), str)
    }

    expected = {
        "dxf/simple-line.dxf": ("approved", "valid", "total_length"),
        "ifc/smoke-minimal.ifc": ("review_required", "needs_review", "entity_count"),
        "pdf/vector-smoke.pdf": ("review_required", "needs_review", "linework_hint_count"),
        "pdf/raster-smoke.pdf": ("review_required", "needs_review", "page_count"),
        "dwg/libredwg-wrapper-smoke.txt": (
            "review_required",
            "needs_review",
            "entity_count",
        ),
    }

    for filename, (review_state, validation_status, quantity_key) in expected.items():
        fixture = fixtures_by_filename.get(filename)
        if fixture is None:
            raise AssertionError(f"Missing fixture manifest entry: {filename}")

        if not filename.startswith("dxf/"):
            license_value = fixture.get("license")
            if not isinstance(license_value, str) or not license_value.strip():
                raise AssertionError(f"Fixture {filename} missing non-empty license metadata.")

            allowed_in_git = fixture.get("allowed_in_git")
            if not isinstance(allowed_in_git, bool):
                raise AssertionError(f"Fixture {filename} missing boolean allowed_in_git flag.")

            if filename.endswith(".txt"):
                placeholder_intent = fixture.get("placeholder_intent")
                has_placeholder_intent = (
                    isinstance(placeholder_intent, str) and bool(placeholder_intent.strip())
                )
                has_fallback_intent = any(
                    isinstance(fixture.get(key), str) and fixture.get(key, "").strip()
                    for key in ("description", "notes", "source")
                )
                if not (has_placeholder_intent or has_fallback_intent):
                    raise AssertionError(
                        f"Placeholder fixture {filename} missing intent metadata fields."
                    )

        assert fixture.get("expected_review_state") == review_state
        assert fixture.get("expected_validation_status") == validation_status

        acceptance_checks = fixture.get("acceptance_checks")
        if not isinstance(acceptance_checks, dict):
            raise AssertionError(f"Fixture {filename} missing acceptance_checks mapping.")

        quantities = acceptance_checks.get("quantities")
        if not isinstance(quantities, dict):
            raise AssertionError(f"Fixture {filename} missing quantities acceptance checks.")

        quantity_check = quantities.get(quantity_key)
        if not isinstance(quantity_check, dict):
            raise AssertionError(
                f"Fixture {filename} missing quantity check for {quantity_key}."
            )

        assert quantity_check.get("expected_review_state") == review_state
