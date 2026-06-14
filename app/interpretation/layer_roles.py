"""Semantic role classification for pen-signature layers.

Tier-3 interpretation derived from (never mutating) the canonical model. #413 emits deterministic
pen-signature layers (``pen-<rrggbb>-w<width>``) whose names are stable identities but carry no
meaning. This module maps each to a coarse semantic role via an explicit, versioned rule table:

- screened/light pens (high lightness)        -> "background"   (floorplan / grid / screened xref)
- saturated colour pens                        -> "services"     (cyan/green/blue service systems)
- dark/near-black pens                         -> "foreground"   (active discipline linework)
- anything else (non-pen, OCG, DWG layers)     -> "unknown"      (cannot classify by pen colour)

The role is an attribute computed on top of the stable pen identity; it never renames the layer.
Execution is deterministic and the basis (which rule fired) is reported for every layer.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

#: Bump when the rule table changes so consumers can tell which logic produced a role.
RULE_VERSION = "1"

# Pen-signature layer name: pen-<rrggbb>-w<width> (see app/ingestion/adapters/pymupdf.py).
_PEN_NAME_RE = re.compile(r"^pen-([0-9a-fA-F]{6})-w")
# Colour saturation at/above this marks a service-system pen (cyan/green/blue), regardless of
# lightness; pure greys/black have zero saturation and fall through to the lightness rule.
_SERVICES_SATURATION = 0.3
# Lightness at/above this marks a screened/background pen.
_BACKGROUND_LIGHTNESS = 0.5

ROLE_BACKGROUND = "background"
ROLE_FOREGROUND = "foreground"
ROLE_SERVICES = "services"
ROLE_UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class LayerRole:
    """A layer's derived semantic role and the basis for it."""

    role: str
    basis: str


def classify_layer_role(layer_name: str | None) -> LayerRole:
    """Classify a layer name into a coarse semantic role with a deterministic rule basis."""

    match = _PEN_NAME_RE.match(layer_name or "")
    if match is None:
        return LayerRole(role=ROLE_UNKNOWN, basis="not a pen-signature layer")

    red = int(match.group(1)[0:2], 16) / 255.0
    green = int(match.group(1)[2:4], 16) / 255.0
    blue = int(match.group(1)[4:6], 16) / 255.0
    lightness = (red + green + blue) / 3.0
    channel_max = max(red, green, blue)
    channel_min = min(red, green, blue)
    saturation = (channel_max - channel_min) / channel_max if channel_max > 0.0 else 0.0

    if saturation >= _SERVICES_SATURATION:
        return LayerRole(
            role=ROLE_SERVICES,
            basis=f"saturation {saturation:.2f} >= {_SERVICES_SATURATION} (rule v{RULE_VERSION})",
        )
    if lightness >= _BACKGROUND_LIGHTNESS:
        return LayerRole(
            role=ROLE_BACKGROUND,
            basis=f"lightness {lightness:.2f} >= {_BACKGROUND_LIGHTNESS} (rule v{RULE_VERSION})",
        )
    return LayerRole(
        role=ROLE_FOREGROUND,
        basis=f"lightness {lightness:.2f} < {_BACKGROUND_LIGHTNESS} (rule v{RULE_VERSION})",
    )
