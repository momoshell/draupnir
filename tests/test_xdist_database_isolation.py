"""Tests for pytest-xdist database isolation helpers."""

from __future__ import annotations

from collections.abc import Mapping

import pytest

from tests import conftest

BASE_DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/draupnir_test"


def test_derive_xdist_database_config_uses_physical_worker_database() -> None:
    config = conftest._derive_xdist_database_config(
        f"{BASE_DATABASE_URL}?ssl=disable",
        "gw1",
    )

    assert config.worker_id == "gw1"
    assert config.worker_database_name == "draupnir_test_gw1"
    assert (
        config.worker_database_url
        == "postgresql+asyncpg://postgres:postgres@localhost:5432/draupnir_test_gw1?ssl=disable"
    )
    assert (
        config.maintenance_database_url
        == "postgresql+asyncpg://postgres:postgres@localhost:5432/postgres?ssl=disable"
    )


def test_derive_xdist_database_config_includes_testrun_uid_in_worker_database_name() -> None:
    config = conftest._derive_xdist_database_config(
        BASE_DATABASE_URL,
        "gw3",
        testrun_uid="run-123",
    )

    assert config.worker_database_name == "draupnir_test_run-123_gw3"
    assert config.worker_database_url == f"{BASE_DATABASE_URL}_run-123_gw3"


def test_configure_xdist_worker_database_updates_environment_after_create() -> None:
    environ = {
        "DATABASE_URL": BASE_DATABASE_URL,
        "PYTEST_XDIST_WORKER": "gw0",
    }
    created: list[conftest.XdistDatabaseConfig] = []
    migrated: list[tuple[str, str]] = []

    def create_database(config: conftest.XdistDatabaseConfig) -> None:
        created.append(config)

    def run_migrations(worker_database_url: str, migration_env: Mapping[str, str]) -> None:
        migrated.append((worker_database_url, migration_env["DATABASE_URL"]))

    worker_url = conftest._configure_xdist_worker_database(
        environ,
        create_database=create_database,
        run_migrations=run_migrations,
    )

    assert worker_url == f"{BASE_DATABASE_URL}_gw0"
    assert environ["DATABASE_URL"] == f"{BASE_DATABASE_URL}_gw0"
    assert [config.worker_database_name for config in created] == ["draupnir_test_gw0"]
    assert migrated == [(f"{BASE_DATABASE_URL}_gw0", f"{BASE_DATABASE_URL}_gw0")]


def test_configure_xdist_worker_database_uses_testrun_uid_when_available() -> None:
    environ = {
        "DATABASE_URL": BASE_DATABASE_URL,
        "PYTEST_XDIST_WORKER": "gw1",
        "PYTEST_XDIST_TESTRUNUID": "run_abc",
    }
    created: list[conftest.XdistDatabaseConfig] = []
    migrated: list[str] = []

    def run_migrations(worker_database_url: str, _migration_env: Mapping[str, str]) -> None:
        migrated.append(worker_database_url)

    worker_url = conftest._configure_xdist_worker_database(
        environ,
        create_database=lambda config: created.append(config),
        run_migrations=run_migrations,
    )

    assert worker_url == f"{BASE_DATABASE_URL}_run_abc_gw1"
    assert environ["DATABASE_URL"] == f"{BASE_DATABASE_URL}_run_abc_gw1"
    assert [config.worker_database_name for config in created] == ["draupnir_test_run_abc_gw1"]
    assert migrated == [f"{BASE_DATABASE_URL}_run_abc_gw1"]


@pytest.mark.parametrize(
    ("environ", "expected_url"),
    [
        ({}, None),
        ({"DATABASE_URL": BASE_DATABASE_URL}, None),
        ({"DATABASE_URL": BASE_DATABASE_URL, "PYTEST_XDIST_WORKER": "master"}, None),
    ],
)
def test_configure_xdist_worker_database_keeps_serial_behavior_unchanged(
    environ: dict[str, str],
    expected_url: str | None,
) -> None:
    worker_url = conftest._configure_xdist_worker_database(
        environ,
        create_database=lambda config: pytest.fail(f"unexpected create for {config!r}"),
        run_migrations=lambda worker_database_url, migration_env: pytest.fail(
            f"unexpected migration for {worker_database_url!r} with {migration_env!r}"
        ),
    )

    assert worker_url == expected_url


@pytest.mark.parametrize(
    ("database_url", "worker_id", "message"),
    [
        ("sqlite:///tmp/test.db", "gw0", "PostgreSQL"),
        (
            "postgresql+asyncpg://postgres:postgres@db.internal:5432/draupnir_test",
            "gw0",
            "localhost",
        ),
        (
            "postgresql+asyncpg://postgres:postgres@localhost:5432/draupnir",
            "gw0",
            "ending in '_test'",
        ),
        (BASE_DATABASE_URL, "worker-0", "unsupported pytest-xdist worker id"),
        (
            f"{BASE_DATABASE_URL}?database=override",
            "gw0",
            "unsupported query parameter",
        ),
        (
            f"{BASE_DATABASE_URL}?Host=db.internal",
            "gw0",
            "unsupported query parameter",
        ),
        (
            f"{BASE_DATABASE_URL}?user=postgres",
            "gw0",
            "unsupported query parameter",
        ),
        (
            f"{BASE_DATABASE_URL}?PASSWORD=secret",
            "gw0",
            "unsupported query parameter",
        ),
        (
            f"{BASE_DATABASE_URL}?service=readonly",
            "gw0",
            "unsupported query parameter",
        ),
        (
            f"postgresql+asyncpg://postgres:postgres@localhost:5432/{'a' * 58}_test",
            "gw0",
            "identifier limit",
        ),
    ],
)
def test_derive_xdist_database_config_rejects_unsafe_inputs(
    database_url: str,
    worker_id: str,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        conftest._derive_xdist_database_config(database_url, worker_id)


def test_configure_xdist_worker_database_raises_usage_error_for_unsafe_base_url() -> None:
    environ = {
        "DATABASE_URL": "postgresql+asyncpg://postgres:postgres@localhost:5432/draupnir",
        "PYTEST_XDIST_WORKER": "gw0",
    }

    with pytest.raises(pytest.UsageError, match="ending in '_test'"):
        conftest._configure_xdist_worker_database(
            environ,
            create_database=lambda config: pytest.fail(f"unexpected create for {config!r}"),
            run_migrations=lambda worker_database_url, migration_env: pytest.fail(
                f"unexpected migration for {worker_database_url!r} with {migration_env!r}"
            ),
        )


def test_run_xdist_worker_migrations_usage_error_redacts_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker_database_url = (
        "postgresql+asyncpg://postgres:super-secret@localhost:5432/draupnir_test_run_abc_gw2"
    )

    class _Result:
        returncode = 19

    def fake_subprocess_run(*_args: object, **_kwargs: object) -> _Result:
        return _Result()

    monkeypatch.setattr("tests.conftest.subprocess.run", fake_subprocess_run)

    with pytest.raises(pytest.UsageError) as exc_info:
        conftest._run_xdist_worker_migrations(
            worker_database_url,
            {"DATABASE_URL": worker_database_url},
        )

    message = str(exc_info.value)
    assert "localhost:5432/draupnir_test_run_abc_gw2" in message
    assert "super-secret" not in message
    assert worker_database_url not in message
