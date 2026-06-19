"""Unit tests for source-fragment re-derivation (#522)."""

from pathlib import Path

import ezdxf
import ifcopenshell
import pytest

from app.ingestion.source_fragment import (
    _handle_from_ref,
    _jsonable,
    fetch_source_fragment,
    unsupported_reason,
)

_DXF_FIXTURE = Path("tests/fixtures/dxf/simple-line.dxf")
_IFC_FIXTURE = Path("tests/fixtures/ifc/smoke-minimal.ifc")


def _first_dxf_handle() -> str:
    document = ezdxf.readfile(str(_DXF_FIXTURE))  # type: ignore[attr-defined]
    return str(next(iter(document.modelspace())).dxf.handle)


def test_dxf_fragment_resolves_by_handle() -> None:
    handle = _first_dxf_handle()
    result = fetch_source_fragment(
        file_path=_DXF_FIXTURE, upload_format="dxf", source_identity=handle, source_ref=None
    )
    assert result["available"] is True
    assert result["native_type"] == "LINE"
    assert result["handle"] == handle
    assert "start" in result["attributes"]


def test_dxf_handle_falls_back_to_source_ref() -> None:
    handle = _first_dxf_handle()
    result = fetch_source_fragment(
        file_path=_DXF_FIXTURE,
        upload_format="dxf",
        source_identity=None,
        source_ref=f"entities.LINE:{handle}",
    )
    assert result["available"] is True
    assert result["handle"] == handle


def test_dxf_unknown_handle_is_unavailable() -> None:
    result = fetch_source_fragment(
        file_path=_DXF_FIXTURE, upload_format="dxf", source_identity="ZZZZ", source_ref=None
    )
    assert result["available"] is False
    assert "ZZZZ" in result["reason"]


def test_ifc_fragment_resolves_by_step_id_and_guid() -> None:
    model = ifcopenshell.open(str(_IFC_FIXTURE))
    entity = next(iter(model))
    by_id = fetch_source_fragment(
        file_path=_IFC_FIXTURE,
        upload_format="ifc",
        source_identity=f"#{entity.id()}",
        source_ref=None,
    )
    assert by_id["available"] is True
    assert by_id["ifc_type"] == entity.is_a()

    guid_entities = [item for item in model if getattr(item, "GlobalId", None)]
    if guid_entities:
        guid = guid_entities[0].GlobalId
        by_guid = fetch_source_fragment(
            file_path=_IFC_FIXTURE, upload_format="ifc", source_identity=guid, source_ref=None
        )
        assert by_guid["available"] is True
        assert by_guid["global_id"] == guid


@pytest.mark.parametrize("fmt", ["dwg", "pdf"])
def test_unsupported_formats_report_a_reason(fmt: str) -> None:
    result = fetch_source_fragment(
        file_path=Path("unused"), upload_format=fmt, source_identity="x", source_ref=None
    )
    assert result["available"] is False
    assert result["reason"] == unsupported_reason(fmt)
    assert result["reason"]


def test_handle_from_ref_parses_trailing_token() -> None:
    assert _handle_from_ref("entities.LINE:1F") == "1F"
    assert _handle_from_ref("no-colon") is None
    assert _handle_from_ref(None) is None


def test_jsonable_coerces_native_values() -> None:
    assert _jsonable("x") == "x"
    assert _jsonable([1, "a"]) == [1, "a"]
    assert _jsonable(ezdxf.math.Vec3(1.0, 2.0, 3.0)) == {"x": 1.0, "y": 2.0, "z": 3.0}
    assert isinstance(_jsonable(object()), str)
