"""Revision cursor helpers."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from app.api.pagination import (
    decode_cursor_payload,
    decode_keyset_cursor,
    encode_keyset_cursor,
    read_cursor_datetime,
    read_cursor_int,
    read_cursor_uuid,
)


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

    return encode_keyset_cursor(payload)


def _decode_revision_cursor(cursor: str) -> _DrawingRevisionCursor:
    """Decode and validate an opaque drawing revision cursor."""

    return decode_keyset_cursor(cursor, _DrawingRevisionCursor)


def _decode_artifact_cursor(cursor: str) -> _GeneratedArtifactCursor:
    """Decode and validate an opaque generated artifact cursor."""

    return decode_keyset_cursor(cursor, _GeneratedArtifactCursor)


def _encode_materialization_cursor(sequence_index: int, row_id: UUID) -> str:
    """Encode a materialization cursor from sequence index and row identifier."""

    return encode_keyset_cursor(
        {
            "sequence_index": sequence_index,
            "id": str(row_id),
        }
    )


def _encode_timestamp_cursor(created_at: datetime, row_id: UUID) -> str:
    """Encode an opaque timestamp/id pagination cursor."""

    return encode_keyset_cursor({"created_at": created_at.isoformat(), "id": str(row_id)})


def _encode_estimate_item_cursor(line_number: int, row_id: UUID) -> str:
    """Encode an opaque line-number/id pagination cursor."""

    return encode_keyset_cursor({"line_number": line_number, "id": str(row_id)})


def _encode_estimate_snapshot_entry_cursor(sort_order: int, row_id: UUID) -> str:
    """Encode an opaque sort-order/id pagination cursor."""

    return encode_keyset_cursor({"sort_order": sort_order, "id": str(row_id)})


def _decode_timestamp_cursor(cursor: str) -> tuple[datetime, UUID]:
    """Decode a timestamp/id pagination cursor."""

    cursor_data = decode_cursor_payload(cursor)
    return read_cursor_datetime(cursor_data, "created_at"), read_cursor_uuid(cursor_data, "id")


def _decode_estimate_item_cursor(cursor: str) -> tuple[int, UUID]:
    """Decode a line-number/id pagination cursor."""

    cursor_data = decode_cursor_payload(cursor)
    return read_cursor_int(cursor_data, "line_number"), read_cursor_uuid(cursor_data, "id")


def _decode_estimate_snapshot_entry_cursor(cursor: str) -> tuple[int, UUID]:
    """Decode a sort-order/id pagination cursor."""

    cursor_data = decode_cursor_payload(cursor)
    return read_cursor_int(cursor_data, "sort_order"), read_cursor_uuid(cursor_data, "id")


def _decode_materialization_cursor(cursor: str) -> tuple[int, UUID]:
    """Decode a materialization cursor into typed values."""

    cursor_data = decode_cursor_payload(cursor)
    return read_cursor_int(cursor_data, "sequence_index"), read_cursor_uuid(cursor_data, "id")
