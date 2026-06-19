"""Draupnir MCP server — a standalone, agent-facing adapter over the HTTP API.

This package is intentionally separate from ``app``: it is a thin client that
talks to the running Draupnir API over HTTP and exposes it to AI agents as MCP
tools/resources (generated from the API's OpenAPI surface). It imports nothing
from ``app`` at runtime.
"""

__version__: str = "0.1.0"
