# syntax=docker/dockerfile:1
FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.11.8 /uv /uvx /bin/

ENV PATH="/app/.venv/bin:$PATH"

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy dependency manifest first (cache-friendly layer)
COPY pyproject.toml uv.lock README.md ./

# Sync runtime dependencies before project source is available
RUN uv sync --locked --no-install-project --no-dev --extra db --extra jobs

# Copy application source
COPY app/ ./app/

# Install project and runtime extras into the project environment
RUN uv sync --locked --no-dev --extra db --extra jobs

# Create non-root user
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Expose port
EXPOSE 8000

# Default command
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
