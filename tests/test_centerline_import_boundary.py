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
_FLOOR_REGISTRATION_MODULE = "app.interpretation.floor_registration"
_ROOM_VORONOI_MODULE = "app.interpretation.room_voronoi"

_READ_PATH_MODULES = [
    "app.interpretation.service_takeoff_loaders",
    "app.interpretation.service_fill_takeoff",
    "app.interpretation.routed_connectivity",
    "app.api.v1.revision_routes.service_takeoff",
    "app.api.v1.revision_routes.canonical",
]

_ROOM_PARTITION_MODULE = "app.interpretation.room_partition"
_ROOM_FUSION_MODULE = "app.interpretation.room_fusion"
_FLOOR_MEASURED_MODULE = "app.interpretation.floor_measured"

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

    Bootstrapping via app.main first primes the full module cache. (Historically this
    also dodged an interpretation->api circular import in service_takeoff_loaders; that
    cycle was removed in #705 — see test_read_path_modules_import_in_isolation_no_cycle —
    so isolation import now works too, but the app-bootstrap variant remains a valid
    after-startup check.)
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
# Tests -- interpretation->api import cycle (#705): the CENTERLINE job imports
# service_takeoff_loaders fresh in the celery worker (before app.api). A
# module-level api import there crashed the worker. These guard that the
# read-path loader + the centerline materialization job import in ISOLATION.
# ---------------------------------------------------------------------------


def test_read_path_modules_import_in_isolation_no_cycle() -> None:
    """service_takeoff_loaders must import standalone (no app.main bootstrap) (#705).

    The celery worker imports this module fresh when the lazy CENTERLINE job runs;
    a module-level ``from app.api...`` import created an interpretation->api->
    interpretation cycle that raised ImportError in the worker. Guard isolation import.

    Arrange: script imports only the loader (+ the route that closes the cycle).
    Act:     run in fresh subprocess (NO app.main bootstrap).
    Assert:  exit 0 (no circular ImportError).
    """
    script = (
        "import app.interpretation.service_takeoff_loaders\n"
        "import app.api.v1.revision_routes.service_takeoff\n"
        "print('ok')\n"
    )
    result = _run_script(script)
    assert result.returncode == 0, (
        "interpretation->api import cycle regressed (#705).\n"
        f"stderr: {result.stderr}\nstdout: {result.stdout}"
    )


def test_centerline_materialization_imports_loader_in_isolation_no_cycle() -> None:
    """The CENTERLINE job's materialization path must import in isolation (#705).

    Mirrors the worker: import app.jobs.centerline_materialization (which lazy-imports
    load_service_takeoff_inputs) with NO prior app.api import, then import the loader.

    Arrange: script imports centerline_materialization then the loader, isolated.
    Act:     run in fresh subprocess.
    Assert:  exit 0.
    """
    script = (
        "import app.jobs.centerline_materialization\n"
        "from app.interpretation.service_takeoff_loaders import load_service_takeoff_inputs\n"
        "print('ok')\n"
    )
    result = _run_script(script)
    assert result.returncode == 0, (
        "CENTERLINE job loader import cycle regressed (#705).\n"
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


# ---------------------------------------------------------------------------
# Tests -- floor_registration (pure; must be cv2/skimage/shapely-free in isolation)
# ---------------------------------------------------------------------------


def test_floor_registration_does_not_import_cv2_in_isolation() -> None:
    """app.interpretation.floor_registration must not pull cv2 when imported alone.

    floor_registration is a pure module (stdlib + grid_registration only).
    ADR-008: no heavy CV libs permitted.

    Arrange: script imports only floor_registration.
    Act:     run in fresh subprocess.
    Assert:  exit 0.
    """
    result = _check_after_isolated_import([_FLOOR_REGISTRATION_MODULE], ["cv2"])
    assert result.returncode == 0, (
        f"cv2 imported by floor_registration.\nstderr: {result.stderr}\nstdout: {result.stdout}"
    )


def test_floor_registration_does_not_import_skimage_in_isolation() -> None:
    """app.interpretation.floor_registration must not pull skimage when imported alone.

    Arrange: script imports only floor_registration.
    Act:     run in fresh subprocess.
    Assert:  exit 0.
    """
    result = _check_after_isolated_import([_FLOOR_REGISTRATION_MODULE], ["skimage"])
    assert result.returncode == 0, (
        f"skimage imported by floor_registration.\nstderr: {result.stderr}\nstdout: {result.stdout}"
    )


def test_floor_registration_does_not_import_shapely_in_isolation() -> None:
    """app.interpretation.floor_registration must not pull shapely when imported alone.

    Arrange: script imports only floor_registration.
    Act:     run in fresh subprocess.
    Assert:  exit 0.
    """
    result = _check_after_isolated_import([_FLOOR_REGISTRATION_MODULE], ["shapely"])
    assert result.returncode == 0, (
        f"shapely imported by floor_registration.\nstderr: {result.stderr}\nstdout: {result.stdout}"
    )


# ---------------------------------------------------------------------------
# Tests -- room_voronoi (isolated; pure stdlib module must not pull heavy deps)
# ---------------------------------------------------------------------------


def test_room_voronoi_does_not_import_cv2_in_isolation() -> None:
    """app.interpretation.room_voronoi must not pull cv2 when imported alone.

    Arrange: script imports only room_voronoi (isolated subprocess).
    Act:     run in fresh subprocess.
    Assert:  exit 0.
    """
    result = _check_after_isolated_import([_ROOM_VORONOI_MODULE], ["cv2"])
    assert result.returncode == 0, (
        f"cv2 imported by room_voronoi.\nstderr: {result.stderr}\nstdout: {result.stdout}"
    )


def test_room_voronoi_does_not_import_skimage_in_isolation() -> None:
    """app.interpretation.room_voronoi must not pull skimage when imported alone.

    Arrange: script imports only room_voronoi (isolated subprocess).
    Act:     run in fresh subprocess.
    Assert:  exit 0.
    """
    result = _check_after_isolated_import([_ROOM_VORONOI_MODULE], ["skimage"])
    assert result.returncode == 0, (
        f"skimage imported by room_voronoi.\nstderr: {result.stderr}\nstdout: {result.stdout}"
    )


def test_room_voronoi_does_not_import_shapely_in_isolation() -> None:
    """app.interpretation.room_voronoi must not pull shapely when imported alone.

    Arrange: script imports only room_voronoi (isolated subprocess).
    Act:     run in fresh subprocess.
    Assert:  exit 0.
    """
    result = _check_after_isolated_import([_ROOM_VORONOI_MODULE], ["shapely"])
    assert result.returncode == 0, (
        f"shapely imported by room_voronoi.\nstderr: {result.stderr}\nstdout: {result.stdout}"
    )


# ---------------------------------------------------------------------------
# Tests -- room_partition (isolated; cv2/skimage banned; shapely allowed, #716)
# ---------------------------------------------------------------------------


def test_room_partition_does_not_import_cv2_in_isolation() -> None:
    """app.interpretation.room_partition must not pull cv2 when imported alone.

    shapely is an expected transitive dependency and is therefore excluded from this
    check — only cv2 is guarded (ADR-008, #716).

    Arrange: script imports only room_partition.
    Act:     run in fresh subprocess.
    Assert:  exit 0.
    """
    result = _check_after_isolated_import([_ROOM_PARTITION_MODULE], ["cv2"])
    assert result.returncode == 0, (
        f"cv2 imported by room_partition.\nstderr: {result.stderr}\nstdout: {result.stdout}"
    )


def test_room_partition_does_not_import_skimage_in_isolation() -> None:
    """app.interpretation.room_partition must not pull skimage when imported alone.

    Arrange: script imports only room_partition.
    Act:     run in fresh subprocess.
    Assert:  exit 0.
    """
    result = _check_after_isolated_import([_ROOM_PARTITION_MODULE], ["skimage"])
    assert result.returncode == 0, (
        f"skimage imported by room_partition.\nstderr: {result.stderr}\nstdout: {result.stdout}"
    )


# ---------------------------------------------------------------------------
# Tests -- room_fusion (isolated; cv2/skimage banned; shapely allowed, #717)
# ---------------------------------------------------------------------------


def test_room_fusion_does_not_import_cv2_in_isolation() -> None:
    """app.interpretation.room_fusion must not pull cv2 when imported alone.

    shapely is an expected transitive dependency and is therefore excluded from
    this check — only cv2 is guarded (ADR-008, #717).

    Arrange: script imports only room_fusion.
    Act:     run in fresh subprocess.
    Assert:  exit 0.
    """
    result = _check_after_isolated_import([_ROOM_FUSION_MODULE], ["cv2"])
    assert result.returncode == 0, (
        f"cv2 imported by room_fusion.\nstderr: {result.stderr}\nstdout: {result.stdout}"
    )


def test_room_fusion_does_not_import_skimage_in_isolation() -> None:
    """app.interpretation.room_fusion must not pull skimage when imported alone.

    Arrange: script imports only room_fusion.
    Act:     run in fresh subprocess.
    Assert:  exit 0.
    """
    result = _check_after_isolated_import([_ROOM_FUSION_MODULE], ["skimage"])
    assert result.returncode == 0, (
        f"skimage imported by room_fusion.\nstderr: {result.stderr}\nstdout: {result.stdout}"
    )


# ---------------------------------------------------------------------------
# Tests -- floor_measured (isolated; cv2/skimage banned; shapely allowed, #718)
# ---------------------------------------------------------------------------


def test_floor_measured_does_not_import_cv2_in_isolation() -> None:
    """app.interpretation.floor_measured must not pull cv2 when imported alone.

    shapely is an expected transitive dependency and is therefore excluded from
    this check — only cv2 is guarded (ADR-008, #718).

    Arrange: script imports only floor_measured.
    Act:     run in fresh subprocess.
    Assert:  exit 0.
    """
    result = _check_after_isolated_import([_FLOOR_MEASURED_MODULE], ["cv2"])
    assert result.returncode == 0, (
        f"cv2 imported by floor_measured.\nstderr: {result.stderr}\nstdout: {result.stdout}"
    )


def test_floor_measured_does_not_import_skimage_in_isolation() -> None:
    """app.interpretation.floor_measured must not pull skimage when imported alone.

    Arrange: script imports only floor_measured.
    Act:     run in fresh subprocess.
    Assert:  exit 0.
    """
    result = _check_after_isolated_import([_FLOOR_MEASURED_MODULE], ["skimage"])
    assert result.returncode == 0, (
        f"skimage imported by floor_measured.\nstderr: {result.stderr}\nstdout: {result.stdout}"
    )
