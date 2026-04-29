# =============================================================================
# Draupnir — Development Makefile
# =============================================================================
# Usage:
#   make lint        — Run ruff linter on app/ and tests/
#   make typecheck   — Run mypy on app/ and tests/
#
# NOTE: app/ and tests/ directories are scaffolded in a later step (#20/#21).
# Lint and typecheck targets will report "no files found" until then.
#   make test        — Run pytest
#   make format      — Run ruff formatter on app/ and tests/
#   make check       — Run lint + typecheck + test
#   make hooks       — Install pre-commit hooks
# =============================================================================

.PHONY: lint typecheck test format check hooks

lint:
	ruff check app tests

typecheck:
	mypy app tests

test:
	pytest

format:
	ruff format app tests

check: lint typecheck test

hooks:
	pre-commit install
