"""Rooms identified from label clusters — name + number, independent of polygons (#549).

Real drawings are fully labeled even when their wall linework won't polygonize: a room
name (``PH Plantroom``) sits beside a room-number token (``0.9.01``) as two separate text
entities ~one text-height apart. We already extract all this text; here we pair each number
with its nearest name to recover a room identity (name + number + location) with no boundary.

When polygons exist, these identities enrich them by containment (handled in the pipeline);
when they don't, the labeled rooms are surfaced directly. Pure over :class:`RoomLabel`.
"""

from __future__ import annotations

import math
import re
from collections.abc import Sequence

from app.interpretation.rooms import (
    Room,
    RoomLabel,
    is_plausible_room_number,
    parse_room_number,
    room_from_label,
)

# Real room names are short labels: "Cooling Plantroom", "Heating & Hot Water",
# "Life Safety & Essential", "UPS LV plantroom". Spec-prose note text stamped by
# the wall-polygonizer ("ALL PIPEWORK SHALL BE PR", "ACCORDANCE WITH BUILDING
# REGULATIONS", "A WIRED THERMOSTAT CONTROL") is ALL-CAPS sentence fragment.
# Hard backstop: reject if the text exceeds this char length regardless.
_MAX_ROOM_NAME_CHARS = 40
# All-caps text with >= this many tokens (split on spaces OR hyphens) is
# note/spec-prose, not a room name.  Using 2 catches two-word service labels
# ("VRF CASSETTE", "MECHANICAL EQUIPMENT") and hyphenated service codes
# ("LTHW-VT-F") while keeping single-token abbreviations like "WC" and "(IPS)".
_ALL_CAPS_MIN_TOKENS = 2

ROOM_SOURCE_LABEL = "label_cluster"

# A number label is matched to the nearest name label within this distance (drawing units).
# Sized for the typical name-above-number tag stack (~one text height, ~0.5 m on 1:50 sheets).
DEFAULT_LABEL_CLUSTER_RADIUS = 1.5

# Multi-line room names (#582): a name can span several stacked text lines above the number
# (e.g. "Combined Air" / "Plantroom" / "0.9.05"). Gather every name line that sits in the band
# above the number — within the cluster radius, at/above the number's baseline, and horizontally
# aligned with it — then join top-to-bottom into the full name. A single-line name yields a
# one-element stack (unchanged), and a non-stacked / side-placed name falls back to nearest.
_NAME_STACK_VERTICAL_EPS = 0.1  # a name may sit fractionally below the number's baseline
_NAME_STACK_HORIZONTAL_ALIGN = 1.0  # max |dx| to count a line as part of the same tag stack

# Heuristic confidence for a label-only room (no geometry to corroborate the identity).
LABEL_ROOM_CONFIDENCE = 0.7

# Patterns that identify annotation/note text that should NOT be treated as room names.
# Conservative by design: real room names (letters, "&", "Plantroom", "Room", proper nouns)
# must pass. These patterns catch dimension specs, quantity phrases, callout leaders, and
# routing notes that Revit-export sheets place on the same text layer as room tags.
_ANNOTATION_DIMENSION_RE = re.compile(
    r"Ø"  # diameter symbol (Ø250 mm …)
    r"|\d+\s*mm"  # dimension with mm unit (100 mm, 250mm)
    r"|\d+\s*[xX]\s*\d+",  # cross-section dimension (100x50, 100 x 50)
    re.IGNORECASE,
)
_ANNOTATION_QUANTITY_RE = re.compile(r"^\s*\d+\s*[Nn][Oo]\.?")  # "6No." / "6 No" quantity prefix
_ANNOTATION_ROUTING_RE = re.compile(
    r"\bFROM\s+TOP\b"
    r"|\bTO\s+BOTTOM\b"
    r"|\bTO\s+LL\b"
    r"|\bLOW\s+LOS[SE]\s+HEADER\b"  # LOW LOSS / LOW LESS HEADER
    r"|\bHEADER\b"
    r"|\bTB\b",  # standalone "TB" abbreviation in routing notes
    re.IGNORECASE,
)


def _looks_like_room_name(text: str) -> bool:
    """Return True when *text* could plausibly be a room name.

    Rejects annotation/note strings that appear on the same text layer as room tags on
    Revit-export sheets (e.g. "Ø250 mm LOW LESS HEADER", "6No. OUTDOOR DX UNITS",
    "FROM TOP TO BOTTOM TB :"). Conservative — real room names with letters, spaces,
    "&", "Plantroom", "Room", etc. must pass.
    """
    stripped = text.strip()
    if not stripped:
        return False
    # Must contain at least one alphabetic character — pure digits/punctuation are not names.
    if not any(ch.isalpha() for ch in stripped):
        return False
    # Trailing colon → callout / leader line; trailing period → sentence fragment.
    if stripped.endswith((":", ".")):
        return False
    # AutoCAD mtext formatting codes (e.g. "\LLEGEND", "\LCHILLED WATER NOTES") are not names.
    if stripped.startswith("\\L"):
        return False
    if _ANNOTATION_DIMENSION_RE.search(stripped):
        return False
    if _ANNOTATION_QUANTITY_RE.match(stripped):
        return False
    if _ANNOTATION_ROUTING_RE.search(stripped):
        return False
    # Hard backstop: very long strings are never room names.
    if len(stripped) > _MAX_ROOM_NAME_CHARS:
        return False
    # Multi-token all-caps strings are spec-prose / note fragments, not room names.
    # Real room names use Title Case or mixed case ("Cooling Plantroom", "UPS LV plantroom").
    # Tokens are split on spaces and hyphens so that service codes like "LTHW-VT-F"
    # (3 hyphen-tokens, all caps) are rejected alongside sentence fragments.
    # Single-token all-caps abbreviations ("WC", "(IPS)") are kept.
    alpha_chars = [ch for ch in stripped if ch.isalpha()]
    all_caps = bool(alpha_chars) and all(ch.isupper() for ch in alpha_chars)
    if all_caps:
        token_count = len(re.split(r"[ -]+", stripped))
        if token_count >= _ALL_CAPS_MIN_TOKENS:
            return False
    return True


# Input family whose confirmed-room rule is stricter (#828 PR-3). Duplicated here (rather than
# imported from room_pipeline) to avoid a circular import — room_pipeline already imports this
# module. Kept in exact sync with room_pipeline.INPUT_FAMILY_PDF_VECTOR.
_INPUT_FAMILY_PDF_VECTOR = "pdf_vector"


def has_genuine_room_identity(room: Room, *, input_family: str | None = None) -> bool:
    """Return True when *room* has a genuine identity — a valid room number or a real name.

    Used as the shared presentation filter (#792) in both the ``/rooms`` route response
    and the ``/summary`` room counts, so the two endpoints always agree.

    Genuine = has a parseable room number (e.g. "1.9.01", "G.04") OR a name that passes
    the ``_looks_like_room_name`` gate (e.g. "Cooling Plantroom", "Heating & Hot Water").

    What is filtered out:
      - Anonymous wall-polygon cells (name=None AND number=None): internal registry
        geometry retained for Phase-R conservation and Voronoi byte-identity guard.
      - Spec-prose-named cells: polygon cells whose name was stamped from note text
        (e.g. "ALL PIPEWORK SHALL BE PR", "ACCORDANCE WITH BUILDING REGULATIONS").
    The registry is never modified; this predicate only affects the API presentation layer.

    ``input_family`` (#828 PR-3) tightens the rule for ``pdf_vector`` only — PDF has no
    sheet-scoping, so interior pipe-tag annotation (e.g. "RE", "U1", "100∅SVP AT HL DROPS TB")
    passes ``_looks_like_room_name`` and would otherwise be miscounted as a confirmed room:
    a PDF room is genuine only when its number passes the stricter
    :func:`~app.interpretation.rooms.is_plausible_room_number` — a name alone is never
    sufficient on PDF. Any other family (including ``None``, unknown origin) keeps the
    existing numbered-OR-named rule, byte-identical to pre-#828-PR-3 behaviour.
    """
    if input_family == _INPUT_FAMILY_PDF_VECTOR:
        return room.number is not None and is_plausible_room_number(room.number)
    return (room.number is not None and parse_room_number(room.number) is not None) or (
        room.name is not None and _looks_like_room_name(room.name)
    )


def room_label_layers(labels: Sequence[RoomLabel]) -> set[str | None] | None:
    """Layers that carry a room-number token — the room-label layers — or None to not restrict.

    Returns ``None`` (no restriction) when no label carries a number, so drawings/fixtures
    with name-only labels are unaffected. When numbers exist, restricting to their layers
    keeps room names + numbers (on the room-label layer) and drops device-tag text elsewhere.
    """
    number_layers = {label.layer for label in labels if parse_room_number(label.text)}
    return number_layers or None


def identify_rooms_from_labels(
    labels: Sequence[RoomLabel],
    *,
    cluster_radius: float = DEFAULT_LABEL_CLUSTER_RADIUS,
) -> list[Room]:
    """Pair room-number labels with their nearest name label into label-derived rooms.

    Restricts to the room-label layers (those bearing a number) so device-tag text on other
    layers can't masquerade as room names. Each number token (``parse_room_number``) is then
    matched to the closest non-number label within ``cluster_radius`` for its name. A label
    that itself carries both a name and a number (a multi-line tag) becomes a room on its own.
    Name labels with no number nearby are still surfaced (name-only). Deterministic: ties break
    by input order, and output is ordered by (number, name) so synthesized ids are stable.
    """
    allowed = room_label_layers(labels)
    if allowed is not None:
        labels = [label for label in labels if label.layer in allowed]

    numbers: list[tuple[str, RoomLabel]] = []
    names: list[RoomLabel] = []
    inline: list[Room] = []
    for label in labels:
        number = parse_room_number(label.text)
        residual = _strip_number(label.text, number) if number else label.text
        if number is not None and residual:
            # A single tag carrying both name and number.
            # Drop the residual text as name if it looks like an annotation note (keep the room).
            clean_name = residual if _looks_like_room_name(residual) else None
            inline.append(
                room_from_label(
                    "",
                    source=ROOM_SOURCE_LABEL,
                    location=label.point,
                    name=clean_name,
                    number=number,
                    confidence=LABEL_ROOM_CONFIDENCE,
                )
            )
        elif number is not None:
            numbers.append((number, label))
        elif label.text.strip() and _looks_like_room_name(label.text):
            names.append(label)

    used_names: set[int] = set()
    paired: list[Room] = []
    for number, number_label in numbers:
        name, name_indices = _gather_name_lines(number_label, names, used_names, cluster_radius)
        used_names.update(name_indices)
        paired.append(
            room_from_label(
                "",
                source=ROOM_SOURCE_LABEL,
                location=number_label.point,
                name=name,
                number=number,
                confidence=LABEL_ROOM_CONFIDENCE,
            )
        )

    name_only = [
        room_from_label(
            "",
            source=ROOM_SOURCE_LABEL,
            location=label.point,
            name=label.text.strip(),
            confidence=LABEL_ROOM_CONFIDENCE,
        )
        for index, label in enumerate(names)
        if index not in used_names
    ]

    rooms = [*inline, *paired, *name_only]
    rooms.sort(key=lambda room: (room.number or "~", room.name or "~", room.location or (0.0, 0.0)))
    return [
        room_from_label(
            f"label-room-{index}",
            source=room.source,
            location=room.location if room.location is not None else (0.0, 0.0),
            name=room.name,
            number=room.number,
            confidence=room.confidence,
        )
        for index, room in enumerate(rooms)
    ]


def _gather_name_lines(
    number_label: RoomLabel,
    names: Sequence[RoomLabel],
    used: set[int],
    radius: float,
) -> tuple[str | None, list[int]]:
    """Gather the stacked name line(s) above ``number_label`` into the full room name (#582).

    A name line joins the stack when it is unused, within ``radius`` of the number, at/above the
    number's baseline, and horizontally aligned with it. Lines are joined top-to-bottom (e.g.
    ``Combined Air`` + ``Plantroom``). When no line sits in that band — a side-placed or
    non-stacked name — fall back to the single nearest name in any direction (unchanged #549).
    """
    nx, ny = number_label.point
    stack: list[tuple[float, int, str]] = []
    for index, name_label in enumerate(names):
        if index in used:
            continue
        x, y = name_label.point
        dy = y - ny
        if dy <= -_NAME_STACK_VERTICAL_EPS:  # below the number baseline → not part of the stack
            continue
        if abs(x - nx) > _NAME_STACK_HORIZONTAL_ALIGN:  # off to the side → different label
            continue
        if math.hypot(x - nx, dy) > radius:  # beyond the cluster band
            continue
        stack.append((y, index, name_label.text.strip()))
    if stack:
        stack.sort(key=lambda item: item[0], reverse=True)  # topmost line first
        name = " ".join(text for _, _, text in stack if text)
        return (name or None), [index for _, index, _ in stack]

    nearest_label, nearest_index = _nearest_name(number_label, names, used, radius)
    if nearest_label is None or nearest_index is None:
        return None, []
    return nearest_label.text.strip(), [nearest_index]


def _nearest_name(
    number_label: RoomLabel,
    names: Sequence[RoomLabel],
    used: set[int],
    radius: float,
) -> tuple[RoomLabel | None, int | None]:
    best: tuple[float, int] | None = None
    nx, ny = number_label.point
    for index, name_label in enumerate(names):
        if index in used:
            continue
        distance = math.hypot(name_label.point[0] - nx, name_label.point[1] - ny)
        if distance <= radius and (best is None or distance < best[0]):
            best = (distance, index)
    if best is None:
        return None, None
    return names[best[1]], best[1]


def _strip_number(text: str, number: str) -> str:
    return text.replace(number, "").strip()
