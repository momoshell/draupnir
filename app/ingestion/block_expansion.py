"""Expand INSERT (block) instances into world-placed geometry (#541).

Block-heavy drawings (e.g. Revit-exported DWGs) keep most geometry inside block
definitions; the canonical retains that child geometry in ``blocks[*].entities``
(local coords) but only the INSERT *insertion points* are placed. This pass walks
every INSERT, composes its transform onto the referenced block's children (recursing
nested INSERTs), and appends the result as world-placed entities so the spatial model
is complete. The original entities + block definitions are preserved; expanded
entities are tagged in provenance (``origin = "block_expansion"``) for traceability.

Pure over ``canonical_json``; runs in finalization before validation/materialization.
"""

from __future__ import annotations

import math
from copy import deepcopy
from typing import Any

MAX_DEPTH = 16
_BLOCK_REF_KEYS = ("block_ref", "name", "block_handle")


class Affine2D:
    """2D affine transform ``[a c e; b d f]`` mapping local → world coordinates."""

    __slots__ = ("a", "b", "c", "d", "e", "f")

    def __init__(self, a: float, b: float, c: float, d: float, e: float, f: float) -> None:
        self.a, self.b, self.c, self.d, self.e, self.f = a, b, c, d, e, f

    @classmethod
    def identity(cls) -> Affine2D:
        return cls(1.0, 0.0, 0.0, 1.0, 0.0, 0.0)

    @classmethod
    def translate(cls, tx: float, ty: float) -> Affine2D:
        return cls(1.0, 0.0, 0.0, 1.0, tx, ty)

    @classmethod
    def scale(cls, sx: float, sy: float) -> Affine2D:
        return cls(sx, 0.0, 0.0, sy, 0.0, 0.0)

    @classmethod
    def rotate(cls, radians: float) -> Affine2D:
        cos_r, sin_r = math.cos(radians), math.sin(radians)
        return cls(cos_r, sin_r, -sin_r, cos_r, 0.0, 0.0)

    def matmul(self, other: Affine2D) -> Affine2D:
        """Return ``self ∘ other`` (apply ``other`` first, then ``self``)."""
        return Affine2D(
            self.a * other.a + self.c * other.b,
            self.b * other.a + self.d * other.b,
            self.a * other.c + self.c * other.d,
            self.b * other.c + self.d * other.d,
            self.a * other.e + self.c * other.f + self.e,
            self.b * other.e + self.d * other.f + self.f,
        )

    def apply(self, x: float, y: float) -> tuple[float, float]:
        return (self.a * x + self.c * y + self.e, self.b * x + self.d * y + self.f)

    def scale_factor(self) -> float:
        """Uniform scale magnitude (geometric mean for non-uniform)."""
        return math.sqrt(abs(self.a * self.d - self.b * self.c))

    def rotation_degrees(self) -> float:
        return math.degrees(math.atan2(self.b, self.a))

    def is_conformal(self) -> bool:
        """True when the transform is uniform scale + rotation (no shear/non-uniform)."""
        col1 = math.hypot(self.a, self.b)
        col2 = math.hypot(self.c, self.d)
        if col1 == 0 or col2 == 0:
            return False
        # Equal-length, orthogonal columns ⇒ similarity transform.
        orthogonal = abs(self.a * self.c + self.b * self.d) < 1e-9 * col1 * col2
        equal_scale = abs(col1 - col2) < 1e-9 * max(col1, col2)
        return orthogonal and equal_scale


def expand_block_instances(canonical_json: dict[str, Any]) -> dict[str, Any]:
    """Append world-placed copies of block-instance geometry to ``entities``.

    Mutates and returns ``canonical_json``. No-op when there are no blocks with
    child geometry (e.g. adapters that don't emit block contents yet).
    """

    blocks_by_ref = _index_blocks(canonical_json.get("blocks"))
    if not blocks_by_ref:
        return canonical_json

    entities = canonical_json.get("entities")
    if not isinstance(entities, list):
        return canonical_json

    placed: list[dict[str, Any]] = []
    for entity in entities:
        if _entity_type(entity) != "insert":
            continue
        block = _lookup_block(entity, blocks_by_ref)
        if block is None:
            continue
        _expand_insert(
            entity,
            parent=Affine2D.identity(),
            instance_id=_str(entity.get("entity_id")) or "insert",
            blocks_by_ref=blocks_by_ref,
            depth=0,
            path=frozenset(),
            placed=placed,
        )

    if placed:
        canonical_json["entities"] = entities + placed
    return canonical_json


def _expand_insert(
    insert: dict[str, Any],
    *,
    parent: Affine2D,
    instance_id: str,
    blocks_by_ref: dict[str, dict[str, Any]],
    depth: int,
    path: frozenset[str],
    placed: list[dict[str, Any]],
) -> None:
    ref = _block_ref(insert)
    block = blocks_by_ref.get(ref) if ref else None
    if block is None or ref is None or depth > MAX_DEPTH or ref in path:
        return

    transform = _transform(insert)
    base = _point(block.get("base_point")) or (0.0, 0.0)
    placement = _placement_affine(transform, base)
    next_path = path | {ref}

    for col, row in _array_cells(transform.get("array")):
        cell = parent.matmul(_array_offset(transform.get("array"), col, row)).matmul(placement)
        for index, child in enumerate(_as_entities(block.get("entities"))):
            child_id = f"{instance_id}#{col}.{row}:{_str(child.get('entity_id')) or index}"
            if _entity_type(child) == "insert":
                _expand_insert(
                    child,
                    parent=cell,
                    instance_id=child_id,
                    blocks_by_ref=blocks_by_ref,
                    depth=depth + 1,
                    path=next_path,
                    placed=placed,
                )
            else:
                placed.append(_place_entity(child, cell, child_id, source_block=ref))


def _place_entity(
    child: dict[str, Any], transform: Affine2D, instance_id: str, *, source_block: str
) -> dict[str, Any]:
    placed = deepcopy(child)
    geometry = placed.get("geometry")
    approximate = not transform.is_conformal()
    if isinstance(geometry, dict):
        _transform_geometry(
            geometry,
            transform,
            transform.scale_factor(),
            transform.rotation_degrees(),
            conformal=not approximate,
        )
    placed["entity_id"] = instance_id
    provenance = placed.get("provenance_json")
    provenance = dict(provenance) if isinstance(provenance, dict) else {}
    provenance["origin"] = "block_expansion"
    provenance["block_expansion"] = {
        "source_block": source_block,
        "source_entity_id": _str(child.get("entity_id")),
        "approximate_transform": approximate,
    }
    placed["provenance_json"] = provenance
    return placed


def _transform_geometry(
    node: Any, matrix: Affine2D, scale_factor: float, rotation_degrees: float, *, conformal: bool
) -> None:
    """Recursively map every point in a geometry payload into world coordinates."""
    if isinstance(node, dict):
        if _is_point(node):
            node["x"], node["y"] = matrix.apply(float(node["x"]), float(node["y"]))
            return
        for key, value in list(node.items()):
            if key == "bbox":
                node.pop("bbox", None)  # stale local bbox; recomputed downstream
            elif (
                key == "radius" and isinstance(value, (int, float)) and not isinstance(value, bool)
            ):
                node[key] = float(value) * scale_factor
            elif (
                key in ("start_angle_degrees", "end_angle_degrees")
                and conformal
                and isinstance(value, (int, float))
                and not isinstance(value, bool)
            ):
                node[key] = float(value) + rotation_degrees
            else:
                _transform_geometry(
                    value, matrix, scale_factor, rotation_degrees, conformal=conformal
                )
    elif isinstance(node, list):
        for item in node:
            _transform_geometry(item, matrix, scale_factor, rotation_degrees, conformal=conformal)


def _placement_affine(transform: dict[str, Any], base: tuple[float, float]) -> Affine2D:
    insertion = _point(transform.get("insertion_point")) or (0.0, 0.0)
    sx, sy = _scale(transform.get("scale"))
    rotation = _float(transform.get("rotation_radians")) or 0.0
    return (
        Affine2D.translate(insertion[0], insertion[1])
        .matmul(Affine2D.rotate(rotation))
        .matmul(Affine2D.scale(sx, sy))
        .matmul(Affine2D.translate(-base[0], -base[1]))
    )


def _array_cells(array: Any) -> list[tuple[int, int]]:
    if not isinstance(array, dict):
        return [(0, 0)]
    cols = max(1, int(_float(array.get("columns")) or 1))
    rows = max(1, int(_float(array.get("rows")) or 1))
    return [(c, r) for r in range(rows) for c in range(cols)]


def _array_offset(array: Any, col: int, row: int) -> Affine2D:
    if not isinstance(array, dict) or (col == 0 and row == 0):
        return Affine2D.identity()
    cs = _float(array.get("column_spacing")) or 0.0
    rs = _float(array.get("row_spacing")) or 0.0
    return Affine2D.translate(col * cs, row * rs)


def _index_blocks(blocks: Any) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    if not isinstance(blocks, (list, tuple)):
        return index
    for block in blocks:
        if not isinstance(block, dict) or not _as_entities(block.get("entities")):
            continue
        for key in _BLOCK_REF_KEYS:
            ref = _str(block.get(key))
            if ref is not None:
                index.setdefault(ref, block)
    return index


def _lookup_block(
    insert: dict[str, Any], blocks_by_ref: dict[str, dict[str, Any]]
) -> dict[str, Any] | None:
    ref = _block_ref(insert)
    return blocks_by_ref.get(ref) if ref else None


def _block_ref(insert: dict[str, Any]) -> str | None:
    return _str(insert.get("block_ref"))


def _transform(insert: dict[str, Any]) -> dict[str, Any]:
    geometry = insert.get("geometry")
    transform = geometry.get("transform") if isinstance(geometry, dict) else None
    return transform if isinstance(transform, dict) else {}


def _as_entities(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple)):
        return []
    return [item for item in value if isinstance(item, dict)]


def _entity_type(entity: Any) -> str | None:
    if not isinstance(entity, dict):
        return None
    return _str(entity.get("entity_type")) or _str(entity.get("kind")) or _str(entity.get("type"))


def _is_point(node: dict[str, Any]) -> bool:
    return (
        "x" in node
        and "y" in node
        and isinstance(node["x"], (int, float))
        and not isinstance(node["x"], bool)
        and isinstance(node["y"], (int, float))
        and not isinstance(node["y"], bool)
    )


def _point(value: Any) -> tuple[float, float] | None:
    if isinstance(value, dict) and _is_point(value):
        return (float(value["x"]), float(value["y"]))
    return None


def _scale(value: Any) -> tuple[float, float]:
    if isinstance(value, dict):
        sx = _float(value.get("x"))
        sy = _float(value.get("y"))
        return (sx if sx is not None else 1.0, sy if sy is not None else 1.0)
    return (1.0, 1.0)


def _float(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _str(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None
