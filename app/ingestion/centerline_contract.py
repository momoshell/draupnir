"""Pure-Python contract types for centerline routed-length results.

No cv2/skimage/shapely/numpy imports — this module must remain importable on
the read path where heavy vision dependencies are absent.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CenterlineGeometry:
    """Geometric output of a single centerline run."""

    polylines: tuple[tuple[tuple[float, float], ...], ...]
    length_du: float


@dataclass(frozen=True)
class Centerline:
    """Contract value produced by a centerline producer for one group."""

    layer_ref: str | None
    colour_key: str | None
    geometry: CenterlineGeometry
    entity_count: int
    algo_version: str
    raster_params_hash: str
    producer_kind: str

    @property
    def group_key(self) -> tuple[str | None, str | None]:
        """Stable group key: (layer_ref, colour_key)."""
        return (self.layer_ref, self.colour_key)
