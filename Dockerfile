# syntax=docker/dockerfile:1
FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy dependency manifest first (cache-friendly layer)
COPY pyproject.toml ./

# Install build dependencies before project source is available
RUN pip install --no-cache-dir build

# Copy application source
COPY app/ ./app/

# Install package in editable mode (needs app/ on disk for discovery)
RUN pip install --no-cache-dir -e ".[db,jobs]"

# Create non-root user
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Expose port
EXPOSE 8000

# Default command
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
