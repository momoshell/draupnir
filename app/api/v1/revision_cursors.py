"""Revision cursor helpers."""

import base64
import binascii
from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ValidationError

from app.api.pagination import decode_cursor_payload, encode_cursor_payload, raise_invalid_cursor


class _DrawingRevisionCursor(BaseModel):
    """Opaque cursor payload for drawing revision pagination."""

    revision_sequence: int
    created_at: datetime
    id: UUID


class _GeneratedArtifactCursor(BaseModel):
    """Opaque cursor payload for generated artifact pagination."""

    created_at: datetime
    id: UUID


def _encode_cursor(payload: BaseModel) -> str:
    """Encode a pagination cursor payload as an opaque token."""

    return (
        base64.urlsafe_b64encode(payload.model_dump_json().encode("utf-8"))
        .decode("utf-8")
        .rstrip("=")
    )


def _decode_revision_cursor(cursor: str) -> _DrawingRevisionCursor:
    """Decode and validate an opaque drawing revision cursor."""

    try:
        decoded = base64.urlsafe_b64decode(f"{cursor}{'=' * (-len(cursor) % 4)}")
        return _DrawingRevisionCursor.model_validate_json(decoded)
    except (ValueError, ValidationError, binascii.Error) as exc:
        raise_invalid_cursor(exc)


def _decode_artifact_cursor(cursor: str) -> _GeneratedArtifactCursor:
    """Decode and validate an opaque generated artifact cursor."""

    try:
        decoded = base64.urlsafe_b64decode(f"{cursor}{'=' * (-len(cursor) % 4)}")
        return _GeneratedArtifactCursor.model_validate_json(decoded)
    except (ValueError, ValidationError, binascii.Error) as exc:
        raise_invalid_cursor(exc)


def _encode_materialization_cursor(sequence_index: int, row_id: UUID) -> str:
    """Encode a materialization cursor from sequence index and row identifier."""

    return encode_cursor_payload(
        {
            "sequence_index": sequence_index,
            "id": str(row_id),
        }
    )


def _encode_timestamp_cursor(created_at: datetime, row_id: UUID) -> str:
    """Encode an opaque timestamp/id pagination cursor."""

    return encode_cursor_payload({"created_at": created_at.isoformat(), "id": str(row_id)})


def _encode_estimate_item_cursor(line_number: int, row_id: UUID) -> str:
    """Encode an opaque line-number/id pagination cursor."""

    return encode_cursor_payload({"line_number": line_number, "id": str(row_id)})


def _encode_estimate_snapshot_entry_cursor(sort_order: int, row_id: UUID) -> str:
    """Encode an opaque sort-order/id pagination cursor."""

    return encode_cursor_payload({"sort_order": sort_order, "id": str(row_id)})


def _decode_timestamp_cursor(cursor: str) -> tuple[datetime, UUID]:
    """Decode a timestamp/id pagination cursor."""

    try:
        cursor_data = decode_cursor_payload(cursor)
        return datetime.fromisoformat(str(cursor_data["created_at"])), UUID(str(cursor_data["id"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise_invalid_cursor(exc)


def _decode_estimate_item_cursor(cursor: str) -> tuple[int, UUID]:
    """Decode a line-number/id pagination cursor."""

    try:
        cursor_data = decode_cursor_payload(cursor)
        return int(cursor_data["line_number"]), UUID(str(cursor_data["id"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise_invalid_cursor(exc)


def _decode_estimate_snapshot_entry_cursor(cursor: str) -> tuple[int, UUID]:
    """Decode a sort-order/id pagination cursor."""

    try:
        cursor_data = decode_cursor_payload(cursor)
        return int(cursor_data["sort_order"]), UUID(str(cursor_data["id"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise_invalid_cursor(exc)


def _decode_materialization_cursor(cursor: str) -> tuple[int, UUID]:
    """Decode a materialization cursor into typed values."""

    try:
        cursor_data = decode_cursor_payload(cursor)
        return int(cursor_data["sequence_index"]), UUID(str(cursor_data["id"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise_invalid_cursor(exc)
