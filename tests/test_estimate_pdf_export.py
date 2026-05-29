"""Integration tests for deterministic estimate PDF export rendering."""

from __future__ import annotations

import base64
import hashlib
import uuid
import zlib
from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

import app.db.session as session_module
from app.exports.estimate_pdf import (
    ESTIMATE_PDF_EXPORT_GENERATOR_NAME,
    ESTIMATE_PDF_EXPORT_GENERATOR_VERSION,
    ESTIMATE_PDF_EXPORT_MEDIA_TYPE,
    EstimatePdfExportError,
    render_estimate_pdf_export,
)
from tests.conftest import requires_database
from tests.test_csv_exports import _seed_export_fixture

pytestmark = [pytest.mark.asyncio, requires_database]

_EXPECTED_ESTIMATE_PDF_CHECKSUM = "375f6e7c221042ccd545ff16251f4a88078ffcf1358be8cff91a4b7f794155f0"
_EXPECTED_ESTIMATE_PDF_ID = "c51390cb148e9cbe6dba713ed68dea55"


@pytest_asyncio.fixture
async def db_session(cleanup_projects: None) -> AsyncGenerator[AsyncSession, None]:
    _ = cleanup_projects
    session_maker = session_module.AsyncSessionLocal
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        yield session
        await session.rollback()


async def test_render_estimate_pdf_export_is_deterministic_and_stable(
    db_session: AsyncSession,
) -> None:
    seeded = await _seed_export_fixture(db_session)

    first = await render_estimate_pdf_export(db_session, seeded.estimate_version.id)
    second = await render_estimate_pdf_export(db_session, seeded.estimate_version.id)

    assert first == second
    assert first.media_type == ESTIMATE_PDF_EXPORT_MEDIA_TYPE
    assert first.generator_name == ESTIMATE_PDF_EXPORT_GENERATOR_NAME
    assert first.generator_version == ESTIMATE_PDF_EXPORT_GENERATOR_VERSION
    assert first.options == {}
    assert first.size_bytes == len(first.content_bytes)
    assert first.checksum_sha256 == hashlib.sha256(first.content_bytes).hexdigest()
    assert first.checksum_sha256 == _EXPECTED_ESTIMATE_PDF_CHECKSUM

    content = first.content_bytes
    assert content.startswith(b"%PDF-")
    assert b"/CreationDate (D:20000101000000+00'00')" in content
    assert b"/ModDate (D:20000101000000+00'00')" in content
    expected_id_bytes = _EXPECTED_ESTIMATE_PDF_ID.encode("ascii")
    assert b"/ID [<" + expected_id_bytes + b"><" + expected_id_bytes + b">]" in content


async def test_render_estimate_pdf_export_contains_expected_uncompressed_labels(
    db_session: AsyncSession,
) -> None:
    seeded = await _seed_export_fixture(db_session)

    result = await render_estimate_pdf_export(db_session, seeded.estimate_version.id)
    page_content = _decode_primary_content_stream(result.content_bytes)

    assert f"Estimate {seeded.estimate_version.id}".encode("ascii") in page_content
    assert b"Totals" in page_content
    assert b"Line items" in page_content
    assert b"Subtotal" in page_content
    assert b"Tax" in page_content
    assert b"Total" in page_content
    assert b"Line" in page_content
    assert b"Type" in page_content
    assert b"Description" in page_content
    assert b"Qty" in page_content
    assert b"Unit" in page_content
    assert b"Rate" in page_content
    assert b"575.88" in page_content
    assert b"570.88" in page_content
    assert b"5.00" in page_content
    assert b"Allowance" in page_content
    assert b"secondary" in page_content

    assert page_content.index(b"5.00") < page_content.index(b"570.88")


async def test_render_estimate_pdf_export_raises_for_missing_estimate_version(
    db_session: AsyncSession,
) -> None:
    missing_id = uuid.uuid4()

    with pytest.raises(EstimatePdfExportError, match=str(missing_id)):
        await render_estimate_pdf_export(db_session, missing_id)


def _decode_primary_content_stream(content_bytes: bytes) -> bytes:
    content = content_bytes.replace(b"\r\n", b"\n")
    object_start = content.index(b"8 0 obj")
    stream_start = content.index(b"stream\n", object_start) + len(b"stream\n")
    stream_end = content.index(b"endstream", stream_start)

    encoded_stream = content[stream_start:stream_end]
    ascii85_decoded = base64.a85decode(encoded_stream, adobe=True)
    return zlib.decompress(ascii85_decoded)
