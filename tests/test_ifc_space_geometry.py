"""Unit tests for IfcSpace footprint geometry extraction (issue #462).

The mesh→footprint projection is pure and tested with synthetic meshes. The
gated adapter path is tested with a fake ifcopenshell.geom runtime, so no real
IFC file or tessellation backend is needed.
"""

from __future__ import annotations

from types import ModuleType, SimpleNamespace
from typing import Any

from app.ingestion.adapters.ifcopenshell import (
    _extract_space_geometry,
    _footprint_from_mesh,
    _resolve_entity_geometry,
)
from app.ingestion.contracts import AdapterWarning

# A unit square in the XY plane (z=0), two triangles, IfcOpenShell flat mesh layout.
_SQUARE_VERTS = [
    0.0, 0.0, 0.0,
    1.0, 0.0, 0.0,
    1.0, 1.0, 0.0,
    0.0, 1.0, 0.0,
]  # fmt: skip
_SQUARE_FACES = [0, 1, 2, 0, 2, 3]

_UNITS: dict[str, Any] = {"normalized": "meter", "assumed": False}


# --- _footprint_from_mesh (pure) ---


def test_footprint_from_square_mesh() -> None:
    ring = _footprint_from_mesh(_SQUARE_VERTS, _SQUARE_FACES)
    assert ring is not None
    # Open ring (no repeated closing point), 4 corners of the unit square.
    assert len(ring) == 4
    assert set(ring) == {(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)}


def test_footprint_projects_3d_box_to_xy() -> None:
    # A box spanning z still footprints to its XY extent (top + bottom faces project
    # onto the same square).
    verts = [
        0.0, 0.0, 0.0,  1.0, 0.0, 0.0,  1.0, 1.0, 0.0,  0.0, 1.0, 0.0,
        0.0, 0.0, 3.0,  1.0, 0.0, 3.0,  1.0, 1.0, 3.0,  0.0, 1.0, 3.0,
    ]  # fmt: skip
    faces = [0, 1, 2, 0, 2, 3, 4, 5, 6, 4, 6, 7]
    ring = _footprint_from_mesh(verts, faces)
    assert ring is not None
    assert set(ring) == {(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)}


def test_footprint_too_few_vertices_returns_none() -> None:
    assert _footprint_from_mesh([0.0, 0.0, 0.0], [0, 0, 0]) is None


def test_footprint_degenerate_triangles_return_none() -> None:
    # All points collinear -> zero-area triangles -> no footprint.
    verts = [0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 2.0, 0.0, 0.0]
    assert _footprint_from_mesh(verts, [0, 1, 2]) is None


# --- fake ifcopenshell.geom runtime ---


class _FakeGeometry:
    def __init__(self, verts: list[float], faces: list[int]) -> None:
        self.verts = verts
        self.faces = faces


class _FakeShape:
    def __init__(self, verts: list[float], faces: list[int]) -> None:
        self.geometry = _FakeGeometry(verts, faces)


class _FakeSettings:
    USE_WORLD_COORDS = "use-world-coords"

    def __init__(self) -> None:
        self.flags: dict[str, bool] = {}

    def set(self, flag: str, value: bool) -> None:
        self.flags[flag] = value


class _FakeGeomModule:
    def __init__(self, verts: list[float], faces: list[int], *, fail: bool = False) -> None:
        self._verts = verts
        self._faces = faces
        self._fail = fail

    def settings(self) -> _FakeSettings:
        return _FakeSettings()

    def create_shape(self, _settings: _FakeSettings, _product: object) -> _FakeShape:
        if self._fail:
            raise RuntimeError("no representation")
        return _FakeShape(self._verts, self._faces)


def _runtime_with_geom(geom: object | None) -> ModuleType:
    module = ModuleType("fake_ifcopenshell")
    if geom is not None:
        module.geom = geom  # type: ignore[attr-defined]
    return module


def _space(ifc_type: str = "IfcSpace") -> SimpleNamespace:
    return SimpleNamespace(is_a=lambda query=None: query == ifc_type if query else ifc_type)


# --- _extract_space_geometry ---


def test_extract_space_geometry_emits_polygon() -> None:
    runtime = _runtime_with_geom(_FakeGeomModule(_SQUARE_VERTS, _SQUARE_FACES))
    geometry = _extract_space_geometry(_space(), runtime=runtime, units=_UNITS)
    assert geometry is not None
    assert geometry["kind"] == "polygon"
    assert geometry["closed"] is True
    assert geometry["status"] == "present"
    assert geometry["bbox"] == {
        "min": {"x": 0.0, "y": 0.0, "z": 0.0},
        "max": {"x": 1.0, "y": 1.0, "z": 0.0},
    }
    assert len(geometry["vertices"]) == 4  # type: ignore[arg-type]


def test_extract_space_geometry_returns_none_without_geom_module() -> None:
    assert _extract_space_geometry(_space(), runtime=_runtime_with_geom(None), units=_UNITS) is None


def test_extract_space_geometry_returns_none_on_tessellation_failure() -> None:
    runtime = _runtime_with_geom(_FakeGeomModule(_SQUARE_VERTS, _SQUARE_FACES, fail=True))
    assert _extract_space_geometry(_space(), runtime=runtime, units=_UNITS) is None


# --- _resolve_entity_geometry (the gate) ---


def test_resolve_geometry_stub_when_disabled() -> None:
    runtime = _runtime_with_geom(_FakeGeomModule(_SQUARE_VERTS, _SQUARE_FACES))
    warnings: list[AdapterWarning] = []
    geometry = _resolve_entity_geometry(
        product=_space(),
        ifc_type="IfcSpace",
        units=_UNITS,
        runtime=runtime,
        space_geometry_enabled=False,
        step_id_token="#1",
        warnings=warnings,
    )
    assert geometry["status"] == "absent"
    assert geometry["reason"] == "semantic_metadata_only"
    assert warnings == []


def test_resolve_geometry_footprint_when_enabled_for_space() -> None:
    runtime = _runtime_with_geom(_FakeGeomModule(_SQUARE_VERTS, _SQUARE_FACES))
    warnings: list[AdapterWarning] = []
    geometry = _resolve_entity_geometry(
        product=_space(),
        ifc_type="IfcSpace",
        units=_UNITS,
        runtime=runtime,
        space_geometry_enabled=True,
        step_id_token="#1",
        warnings=warnings,
    )
    assert geometry["status"] == "present"
    assert geometry["kind"] == "polygon"
    assert warnings == []


def test_resolve_geometry_non_space_stays_stub_even_when_enabled() -> None:
    runtime = _runtime_with_geom(_FakeGeomModule(_SQUARE_VERTS, _SQUARE_FACES))
    warnings: list[AdapterWarning] = []
    geometry = _resolve_entity_geometry(
        product=_space("IfcWall"),
        ifc_type="IfcWall",
        units=_UNITS,
        runtime=runtime,
        space_geometry_enabled=True,
        step_id_token="#2",
        warnings=warnings,
    )
    assert geometry["status"] == "absent"
    assert warnings == []


def test_resolve_geometry_warns_when_space_footprint_unavailable() -> None:
    # Enabled + IfcSpace, but tessellation fails -> stub + a degradation warning.
    runtime = _runtime_with_geom(_FakeGeomModule(_SQUARE_VERTS, _SQUARE_FACES, fail=True))
    warnings: list[AdapterWarning] = []
    geometry = _resolve_entity_geometry(
        product=_space(),
        ifc_type="IfcSpace",
        units=_UNITS,
        runtime=runtime,
        space_geometry_enabled=True,
        step_id_token="#1",
        warnings=warnings,
    )
    assert geometry["status"] == "absent"
    assert [w.code for w in warnings] == ["ifc.space_geometry_unavailable"]
