# =============================================================================
# Draupnir — Development Makefile
# =============================================================================
# Usage:
#   make sync        — Sync uv environment with local extras
#   make lint        — Run ruff linter on app/ and tests/
#   make typecheck   — Run mypy on app/ and tests/
#
# NOTE: app/ and tests/ directories are scaffolded in a later step (#20/#21).
# Lint and typecheck targets will report "no files found" until then.
#   make test        — Run pytest
#   make format      — Run ruff formatter on app/ and tests/
#   make check       — Run lint + typecheck + test
#   make hooks       — Install pre-commit hooks via uv
#   make up          — Start Docker Compose stack
#   make down        — Stop Docker Compose stack
#   make logs        — Follow Docker Compose logs
#   make ps          — Show Docker Compose service status
#   make shell-api   — Open shell in api container
#   make shell-worker— Open shell in worker container
#   make migrate     — Run Alembic migrations
# =============================================================================

UV_SYNC_ARGS = --extra db --extra jobs --extra dev --extra test

.PHONY: sync lint typecheck test format check hooks up down logs ps shell-api shell-worker migrate

sync:
	uv sync $(UV_SYNC_ARGS)

lint:
	uv run ruff check app tests

typecheck:
	uv run mypy app tests

test:
	uv run pytest

format:
	uv run ruff format app tests

check: lint typecheck test

hooks:
	uv run pre-commit install

# ---------------------------------------------------------------------------
# Docker Compose targets
# ---------------------------------------------------------------------------
up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

ps:
	docker compose ps

shell-api:
	docker compose exec api bash

shell-worker:
	docker compose exec worker bash

migrate:
	docker compose exec api alembic upgrade head
