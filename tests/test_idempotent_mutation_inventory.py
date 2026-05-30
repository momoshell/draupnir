from __future__ import annotations

import inspect
from importlib import import_module
from typing import Any

import pytest


def _load_callable(module_paths: tuple[str, ...], attr_name: str) -> Any:
    last_error: Exception | None = None
    for module_path in module_paths:
        try:
            module = import_module(module_path)
            return getattr(module, attr_name)
        except (ImportError, AttributeError) as exc:
            last_error = exc

    joined_paths = ", ".join(module_paths)
    raise AssertionError(
        f"Could not resolve callable '{attr_name}' from modules: {joined_paths}"
    ) from last_error


@pytest.mark.parametrize(
    ("module_paths", "attr_name", "label"),
    [
        (("app.api.v1.projects",), "create_project", "projects.create_project"),
        (("app.api.v1.projects",), "update_project", "projects.update_project"),
        (("app.api.v1.projects",), "delete_project", "projects.delete_project"),
        (("app.api.v1.files",), "upload_project_file", "files.upload_project_file"),
        (
            ("app.api.v1.files",),
            "reprocess_project_file",
            "files.reprocess_project_file",
        ),
        (("app.api.v1.jobs",), "cancel_job", "jobs.cancel_job"),
        (("app.api.v1.jobs",), "retry_job", "jobs.retry_job"),
        (("app.api.v1.estimation",), "_create_catalog_entry", "estimation._create_catalog_entry"),
        (
            ("app.api.v1.revision_routes.exports", "app.api.v1.exports"),
            "_create_export_route_job",
            "exports._create_export_route_job",
        ),
        (
            (
                "app.api.v1.revision_routes.quantity_takeoffs",
                "app.api.v1.quantity_takeoffs",
            ),
            "create_revision_quantity_takeoff",
            "quantity_takeoffs.create_revision_quantity_takeoff",
        ),
        (
            ("app.api.v1.revision_routes.estimates", "app.api.v1.estimates"),
            "create_revision_estimate_version",
            "estimates.create_revision_estimate_version",
        ),
    ],
)
def test_mutating_routes_use_run_idempotent_mutation(
    module_paths: tuple[str, ...], attr_name: str, label: str
) -> None:
    callable_obj = _load_callable(module_paths, attr_name)

    source = inspect.getsource(callable_obj)

    assert "run_idempotent_mutation" in source, (
        f"{label} must be wired through run_idempotent_mutation"
    )
