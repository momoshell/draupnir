from __future__ import annotations

from collections.abc import Iterable, Mapping

type Point3DMapping = Mapping[str, float]
type BBox3DMapping = dict[str, dict[str, float]]


def canonical_bbox_from_points(points: Iterable[Point3DMapping]) -> BBox3DMapping:
    iterator = iter(points)
    first_point = next(iterator)

    min_x = max_x = first_point["x"]
    min_y = max_y = first_point["y"]
    min_z = max_z = first_point["z"]

    for point in iterator:
        x = point["x"]
        y = point["y"]
        z = point["z"]

        if x < min_x:
            min_x = x
        if x > max_x:
            max_x = x
        if y < min_y:
            min_y = y
        if y > max_y:
            max_y = y
        if z < min_z:
            min_z = z
        if z > max_z:
            max_z = z

    return {
        "min": {"x": min_x, "y": min_y, "z": min_z},
        "max": {"x": max_x, "y": max_y, "z": max_z},
    }
