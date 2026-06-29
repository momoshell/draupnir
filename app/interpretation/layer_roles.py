"""Semantic role classification for pen-signature layers and DWG semantic layer names.

Tier-3 interpretation derived from (never mutating) the canonical model. #413 emits deterministic
pen-signature layers (``pen-<rrggbb>-w<width>``) whose names are stable identities but carry no
meaning. This module maps each to a coarse semantic role via an explicit, versioned rule table:

- screened/light pens (high lightness)        -> "background"   (floorplan / grid / screened xref)
- saturated colour pens                        -> "services"     (cyan/green/blue service systems)
- dark/near-black pens                         -> "foreground"   (active discipline linework)
- anything else (non-pen, OCG, DWG layers)     -> keyword rules  (DWG semantic names, rule v2)
- no keyword match                             -> "unknown"

The role is an attribute computed on top of the stable pen identity; it never renames the layer.
Execution is deterministic and the basis (which rule fired) is reported for every layer.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

#: Bump when the rule table changes so consumers can tell which logic produced a role.
RULE_VERSION = "2"

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


# ---------------------------------------------------------------------------
# DWG semantic-name keyword rules (applied when pen-signature match fails)
# ---------------------------------------------------------------------------
#
# Rule priority order — first match wins. The ordering is intentional:
#   1. background-architectural FIRST so that "Space Tags" hits "space" (background) before
#      the generic "tag" rule (foreground). Without this, "Space Tags" would misclassify.
#   2. foreground (tag / centerline) SECOND so that "Pipe Tags" hits "tag" (foreground)
#      before the "pipe" token in the services rule.
#   3. services LAST — catches everything that didn't match background or annotation.
#
# Matching strategy:
#   - Long, unambiguous tokens (e.g. "furniture", "sanitary", "fitting", "color fill") use
#     plain substring matching — false-positive risk is negligible.
#   - Short, ambiguous tokens ("vp", "cdp", "rwp", "rise", "drop") use word-boundary /
#     whole-token regex so that e.g. "enterprise" does NOT match "rise", "viewport" does
#     not match "vp", etc.
#
# Each entry: (role, basis_description, match_fn)


def _word(token: str) -> re.Pattern[str]:
    """Compile a word-boundary pattern for an ambiguous short token."""
    return re.compile(rf"(?<![a-z]){re.escape(token)}(?![a-z])")


# Short tokens that need word-boundary protection ("vent" guards against "event"/
# "prevent"/"inventory"). The services loop below iterates this dict directly, so this
# tuple is the single source of truth — adding a token here is enough.
_WORD_RE: dict[str, re.Pattern[str]] = {
    t: _word(t) for t in ("svp", "vp", "cdp", "rwp", "rise", "drop", "vent")
}


def _classify_dwg_name(name_lower: str) -> LayerRole | None:
    """Return a LayerRole for a DWG semantic layer name, or None if no rule fires.

    Priority: background-architectural → foreground → services.
    """

    # --- 1. Background: architectural / non-service annotation that should recede ---
    # "space" before "tag" so "Space Tags" stays background, not foreground.
    for token in ("furniture", "space", "data device", "color fill", "colour fill", "fill"):
        if token in name_lower:
            return LayerRole(
                role=ROLE_BACKGROUND,
                basis=f"DWG name '{token}' (rule v{RULE_VERSION})",
            )

    # --- 2. Foreground: annotation layers (tags, centre-lines) ---
    # "tag" after space/furniture so "Space Tags" already returned above.
    # "tag" before "pipe" so "Pipe Tags" returns foreground, not services.
    for token in ("tag", "center line", "centerline", "centre line"):
        if token in name_lower:
            return LayerRole(
                role=ROLE_FOREGROUND,
                basis=f"DWG name '{token}' (rule v{RULE_VERSION})",
            )

    # --- 3. Services: routed mechanical / drainage / plumbing elements ---
    # Long unambiguous tokens — plain substring.
    for token in ("pipe", "drain", "sanitary", "fitting", "waste", "stack", "gully"):
        if token in name_lower:
            return LayerRole(
                role=ROLE_SERVICES,
                basis=f"DWG name '{token}' (rule v{RULE_VERSION})",
            )
    # Short ambiguous tokens — whole-token / word-boundary match (sourced from _WORD_RE,
    # so adding a token to that dict is sufficient). "rise"/"drop" are vertical service runs.
    for token in _WORD_RE:
        if _WORD_RE[token].search(name_lower):
            if token in ("rise", "drop"):
                basis = f"DWG name '{token}' (vertical service run, rule v{RULE_VERSION})"
            else:
                basis = f"DWG name '{token}' (rule v{RULE_VERSION})"
            return LayerRole(role=ROLE_SERVICES, basis=basis)

    return None


def classify_layer_role(layer_name: str | None) -> LayerRole:
    """Classify a layer name into a coarse semantic role with a deterministic rule basis."""

    match = _PEN_NAME_RE.match(layer_name or "")
    if match is None:
        # Not a pen-signature layer — try DWG semantic-name keyword rules.
        if layer_name:
            dwg_role = _classify_dwg_name(layer_name.lower())
            if dwg_role is not None:
                return dwg_role
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
