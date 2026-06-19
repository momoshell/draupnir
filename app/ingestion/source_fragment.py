"""Re-derive the raw source fragment behind a canonical entity (#522).

Given a staged original source file and an entity's recorded source address, re-open
the file and return the raw native object (DXF entity, IFC product) so an agent can
verify the transform rather than trust it. Deterministic; reads the retained source
(no persistence). Formats without a stable, cheaply-addressable fragment return an
honest ``available: false`` with a reason instead of guessing.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

# Upload formats with in-process, stable-address re-derivation today.
REDERIVABLE_FORMATS = frozenset({"dxf", "ifc"})

_UNAVAILABLE_REASON = {
    "dwg": "DWG re-derivation requires the LibreDWG toolchain and is not yet implemented.",
    "pdf": "PDF drawing indices are not stable across re-parses; the raw fragment is unavailable.",
}


def fetch_source_fragment(
    *,
    file_path: Path,
    upload_format: str,
    source_identity: str | None,
    source_ref: str | None,
) -> dict[str, Any]:
    """Re-open ``file_path`` and return the raw fragment for the entity's address."""

    if upload_format == "dxf":
        return _fetch_dxf(file_path, source_identity, source_ref)
    if upload_format == "ifc":
        return _fetch_ifc(file_path, source_identity)
    return {"available": False, "reason": unsupported_reason(upload_format)}


def unsupported_reason(upload_format: str) -> str:
    """Explain why a format's raw fragment cannot be re-derived in v1."""
    return _UNAVAILABLE_REASON.get(
        upload_format, f"Raw source re-derivation is not supported for {upload_format!r}."
    )


def _fetch_dxf(
    file_path: Path, source_identity: str | None, source_ref: str | None
) -> dict[str, Any]:
    try:
        import ezdxf
    except ImportError:  # pragma: no cover - depends on optional extra
        return {"available": False, "reason": "The DXF adapter (ezdxf) is not installed."}

    handle = source_identity or _handle_from_ref(source_ref)
    if not handle:
        return {"available": False, "reason": "No source handle was recorded for this entity."}

    document = ezdxf.readfile(str(file_path))  # type: ignore[attr-defined]
    entity = document.entitydb.get(handle)
    if entity is None:
        return {
            "available": False,
            "reason": f"No DXF entity with handle {handle!r} was found in the source.",
            "handle": handle,
        }
    return {
        "available": True,
        "format": "dxf",
        "handle": handle,
        "native_type": str(entity.dxftype()),
        "attributes": {key: _jsonable(value) for key, value in entity.dxfattribs().items()},
    }


def _fetch_ifc(file_path: Path, source_identity: str | None) -> dict[str, Any]:
    try:
        import ifcopenshell
    except ImportError:  # pragma: no cover - depends on optional extra
        return {"available": False, "reason": "The IFC adapter (ifcopenshell) is not installed."}

    if not source_identity:
        return {"available": False, "reason": "No source identity was recorded for this entity."}

    model = ifcopenshell.open(str(file_path))
    product = _ifc_lookup(model, source_identity)
    if product is None:
        return {
            "available": False,
            "reason": f"No IFC product for identity {source_identity!r} was found in the source.",
        }
    return {
        "available": True,
        "format": "ifc",
        "ifc_type": product.is_a(),
        "global_id": getattr(product, "GlobalId", None),
        "attributes": _jsonable(product.get_info(recursive=False)),
    }


def _ifc_lookup(model: Any, source_identity: str) -> Any | None:
    token = source_identity.lstrip("#")
    if token.isdigit():
        try:
            return model.by_id(int(token))
        except (RuntimeError, ValueError):
            return None
    try:
        return model.by_guid(source_identity)
    except (RuntimeError, ValueError):
        return None


def _handle_from_ref(source_ref: str | None) -> str | None:
    """Extract a DXF handle from a source_ref like ``entities.LINE:1F``."""
    if not source_ref or ":" not in source_ref:
        return None
    handle = source_ref.rsplit(":", 1)[-1].strip()
    return handle or None


def _jsonable(value: Any) -> Any:
    """Coerce native adapter values (Vec3, tuples, …) into JSON-safe primitives."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    for attr in ("x", "y", "z"):
        if hasattr(value, attr):
            return {
                axis: _jsonable(getattr(value, axis))
                for axis in ("x", "y", "z")
                if hasattr(value, axis)
            }
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    return str(value)
