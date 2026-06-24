"""Pure-process import-boundary guard for ADR-008 (be-639b).

Two boundaries are guarded here:

1. app.ingestion.centerline_contract -- explicitly prohibits cv2, skimage, and
   shapely (it is the pure contract shared between the write-path producer and the
   read-path coordinator). Tested in complete isolation.

2. Read-path service modules (app.interpretation.service_takeoff_loaders,
   app.api.v1.revision_routes.service_takeoff) -- must not pull cv2 or skimage
   even after the full app module graph is loaded. shapely is a pre-existing
   transitive dependency of the interpretation layer (via geometry.py) and is
   therefore excluded from this check.

All checks run in fresh subprocesses so they are deterministic regardless of
what the parent test process has already imported.
"""

from __future__ import annotations

import subprocess
import sys

_CENTERLINE_CONTRACT_MODULE = "app.ingestion.centerline_contract"
_CENTERLINE_DWG_MODULE = "app.ingestion.centerline_dwg"
_CENTERLINE_PDF_MODULE = "app.ingestion.centerline_pdf"
_CENTERLINE_MATERIALIZATION_MODULE = "app.jobs.centerline_materialization"

_READ_PATH_MODULES = [
    "app.interpretation.service_takeoff_loaders",
    "app.interpretation.service_fill_takeoff",
    "app.interpretation.routed_connectivity",
    "app.api.v1.revision_routes.service_takeoff",
    "app.api.v1.revision_routes.canonical",
]

_HEAVY_CV_MODULES = ["cv2", "skimage"]
_ALL_HEAVY_MODULES = ["cv2", "skimage", "shapely"]


def _run_script(script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=60,
    )


def _check_after_isolated_import(
    modules: list[str],
    forbidden: list[str],
) -> subprocess.CompletedProcess[str]:
    """Import modules directly (isolated; no app bootstrap) then check sys.modules."""
    import_lines = "\n".join(f"import {m}" for m in modules)
    check = "any(k.startswith(m + '.') for k in sys.modules)"
    script = f"""\
import sys
{import_lines}
forbidden = {forbidden!r}
loaded = [m for m in forbidden if m in sys.modules or {check}]
if loaded:
    print("FORBIDDEN IMPORTS DETECTED:", loaded, file=sys.stderr)
    sys.exit(1)
"""
    return _run_script(script)


def _check_after_app_import(
    extra_modules: list[str],
    forbidden: list[str],
) -> subprocess.CompletedProcess[str]:
    """Bootstrap via app.main (which initialises the full module graph), then check.

    service_takeoff_loaders imports from app.api (for the scale resolver), creating a
    circular import when imported directly in isolation. Bootstrapping via app.main first
    primes the module cache so the circular dependency is resolved correctly.
    """
    extra_lines = "\n".join(f"import {m}" for m in extra_modules)
    check = "any(k.startswith(m + '.') for k in sys.modules)"
    script = f"""\
import sys
import app.main  # primes the full module graph
{extra_lines}
forbidden = {forbidden!r}
loaded = [m for m in forbidden if m in sys.modules or {check}]
if loaded:
    print("FORBIDDEN IMPORTS DETECTED:", loaded, file=sys.stderr)
    sys.exit(1)
"""
    return _run_script(script)


# ---------------------------------------------------------------------------
# Tests -- centerline_contract (isolated, all three banned)
# ---------------------------------------------------------------------------


def test_centerline_contract_does_not_import_cv2_in_isolation() -> None:
    """app.ingestion.centerline_contract must not pull cv2 when imported alone.

    Arrange: script imports only centerline_contract.
    Act:     run in fresh subprocess.
    Assert:  exit 0.
    """
    result = _check_after_isolated_import([_CENTERLINE_CONTRACT_MODULE], ["cv2"])
    assert result.returncode == 0, (
        f"cv2 imported by centerline_contract.\nstderr: {result.stderr}\nstdout: {result.stdout}"
    )


def test_centerline_contract_does_not_import_skimage_in_isolation() -> None:
    """app.ingestion.centerline_contract must not pull skimage when imported alone.

    Arrange: script imports only centerline_contract.
    Act:     run in fresh subprocess.
    Assert:  exit 0.
    """
    result = _check_after_isolated_import([_CENTERLINE_CONTRACT_MODULE], ["skimage"])
    assert result.returncode == 0, (
        "skimage imported by centerline_contract.\n"
        f"stderr: {result.stderr}\nstdout: {result.stdout}"
    )


def test_centerline_contract_does_not_import_shapely_in_isolation() -> None:
    """app.ingestion.centerline_contract must not pull shapely when imported alone.

    Arrange: script imports only centerline_contract.
    Act:     run in fresh subprocess.
    Assert:  exit 0.
    """
    result = _check_after_isolated_import([_CENTERLINE_CONTRACT_MODULE], ["shapely"])
    assert result.returncode == 0, (
        "shapely imported by centerline_contract.\n"
        f"stderr: {result.stderr}\nstdout: {result.stdout}"
    )


# ---------------------------------------------------------------------------
# Tests -- read-path service modules (cv2/skimage only; shapely is pre-existing)
# ---------------------------------------------------------------------------


def test_read_path_modules_do_not_import_cv2_after_app_bootstrap() -> None:
    """The read-path service modules must not pull cv2 after normal app startup.

    The check bootstraps app.main first to avoid the circular import that arises
    when service_takeoff_loaders is imported in isolation (it imports the scale
    resolver from app.api which re-imports service_takeoff, a known circular dep).

    Arrange: script imports app.main then the read-path modules.
    Act:     run in fresh subprocess.
    Assert:  exit 0.
    """
    result = _check_after_app_import(_READ_PATH_MODULES, ["cv2"])
    assert result.returncode == 0, (
        "cv2 imported by read-path service modules.\n"
        f"stderr: {result.stderr}\nstdout: {result.stdout}"
    )


def test_read_path_modules_do_not_import_skimage_after_app_bootstrap() -> None:
    """The read-path service modules must not pull skimage after normal app startup.

    Arrange: script imports app.main then the read-path modules.
    Act:     run in fresh subprocess.
    Assert:  exit 0.
    """
    result = _check_after_app_import(_READ_PATH_MODULES, ["skimage"])
    assert result.returncode == 0, (
        "skimage imported by read-path service modules.\n"
        f"stderr: {result.stderr}\nstdout: {result.stdout}"
    )


def test_read_path_modules_do_not_import_cv2_or_skimage_after_app_bootstrap() -> None:
    """Consolidated guard: no cv2 or skimage in the full read-path module set.

    Arrange: script imports app.main then all read-path modules.
    Act:     run in fresh subprocess.
    Assert:  exit 0.
    """
    result = _check_after_app_import(_READ_PATH_MODULES, _HEAVY_CV_MODULES)
    assert result.returncode == 0, (
        "cv2 or skimage imported by read-path service modules (ADR-008 violation).\n"
        f"stderr: {result.stderr}\nstdout: {result.stdout}"
    )


# ---------------------------------------------------------------------------
# Tests -- centerline_dwg (isolated; the DWG producer must stay pure too)
# ---------------------------------------------------------------------------


def test_centerline_dwg_does_not_import_cv2_in_isolation() -> None:
    """app.ingestion.centerline_dwg must not pull cv2 when imported alone.

    Arrange: script imports only centerline_dwg.
    Act:     run in fresh subprocess.
    Assert:  exit 0.
    """
    result = _check_after_isolated_import([_CENTERLINE_DWG_MODULE], ["cv2"])
    assert result.returncode == 0, (
        f"cv2 imported by centerline_dwg.\nstderr: {result.stderr}\nstdout: {result.stdout}"
    )


def test_centerline_dwg_does_not_import_skimage_in_isolation() -> None:
    """app.ingestion.centerline_dwg must not pull skimage when imported alone.

    Arrange: script imports only centerline_dwg.
    Act:     run in fresh subprocess.
    Assert:  exit 0.
    """
    result = _check_after_isolated_import([_CENTERLINE_DWG_MODULE], ["skimage"])
    assert result.returncode == 0, (
        f"skimage imported by centerline_dwg.\nstderr: {result.stderr}\nstdout: {result.stdout}"
    )


# ---------------------------------------------------------------------------
# Tests -- centerline_pdf (isolated; lazy cv2/skimage import must not leak)
# ---------------------------------------------------------------------------


def test_centerline_pdf_does_not_import_cv2_in_isolation() -> None:
    """app.ingestion.centerline_pdf must not pull cv2 at module import time.

    cv2 is only imported inside pdf_centerlines (lazy).  A bare
    ``import app.ingestion.centerline_pdf`` must not load cv2.

    Arrange: script imports only centerline_pdf.
    Act:     run in fresh subprocess.
    Assert:  exit 0.
    """
    result = _check_after_isolated_import([_CENTERLINE_PDF_MODULE], ["cv2"])
    assert result.returncode == 0, (
        f"cv2 imported by centerline_pdf at module level.\n"
        f"stderr: {result.stderr}\nstdout: {result.stdout}"
    )


def test_centerline_pdf_does_not_import_skimage_in_isolation() -> None:
    """app.ingestion.centerline_pdf must not pull skimage at module import time.

    skimage is only imported inside pdf_centerlines (lazy).  A bare
    ``import app.ingestion.centerline_pdf`` must not load skimage.

    Arrange: script imports only centerline_pdf.
    Act:     run in fresh subprocess.
    Assert:  exit 0.
    """
    result = _check_after_isolated_import([_CENTERLINE_PDF_MODULE], ["skimage"])
    assert result.returncode == 0, (
        f"skimage imported by centerline_pdf at module level.\n"
        f"stderr: {result.stderr}\nstdout: {result.stdout}"
    )


# ---------------------------------------------------------------------------
# Tests -- centerline_materialization dispatch (top-level import must stay cv2-free)
# ---------------------------------------------------------------------------


def test_centerline_materialization_dispatch_does_not_import_cv2_in_isolation() -> None:
    """Importing app.jobs.centerline_materialization must NOT pull cv2 into sys.modules.

    The dispatch adds ``from app.ingestion.centerline_pdf import pdf_centerlines``
    at module top, but cv2 is lazy inside pdf_centerlines (ADR-008).  This test
    proves the lazy guard holds end-to-end: the dispatcher can reference
    pdf_centerlines without loading cv2 at import time.

    Arrange: script imports only centerline_materialization (isolated subprocess).
    Act:     run in fresh subprocess.
    Assert:  exit 0.
    """
    result = _check_after_isolated_import([_CENTERLINE_MATERIALIZATION_MODULE], ["cv2"])
    assert result.returncode == 0, (
        f"cv2 imported by centerline_materialization at module level (ADR-008 violation).\n"
        f"stderr: {result.stderr}\nstdout: {result.stdout}"
    )


def test_centerline_materialization_dispatch_does_not_import_skimage_in_isolation() -> None:
    """Importing app.jobs.centerline_materialization must NOT pull skimage into sys.modules.

    Arrange: script imports only centerline_materialization (isolated subprocess).
    Act:     run in fresh subprocess.
    Assert:  exit 0.
    """
    result = _check_after_isolated_import([_CENTERLINE_MATERIALIZATION_MODULE], ["skimage"])
    assert result.returncode == 0, (
        f"skimage imported by centerline_materialization at module level (ADR-008 violation).\n"
        f"stderr: {result.stderr}\nstdout: {result.stdout}"
    )
