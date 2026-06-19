"""Configuration for the Draupnir MCP server."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MCPSettings(BaseSettings):
    """MCP server settings, loaded from ``DRAUPNIR_``-prefixed environment variables.

    The server is a thin adapter: it only needs to know where the HTTP API lives.
    """

    api_base_url: str = Field(
        default="http://localhost:8000",
        description="Base URL of the running Draupnir HTTP API (no trailing path).",
    )
    api_prefix: str = Field(default="/v1", description="API version path prefix.")
    request_timeout_seconds: float = Field(
        default=30.0, gt=0.0, description="Per-request timeout for calls to the API."
    )

    model_config = SettingsConfigDict(
        env_prefix="DRAUPNIR_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )
