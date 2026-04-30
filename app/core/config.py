"""Application configuration via pydantic-settings."""

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from app import __version__


class Settings(BaseSettings):
    """Application settings loaded from environment variables and .env file."""

    app_name: str = "Draupnir"
    app_version: str = __version__
    debug: bool = False
    api_prefix: str = "/v1"
    expose_version_in_health: bool = False

    # Logging configuration
    log_level: str = "INFO"
    service_name: str = "draupnir"

    # Database configuration
    database_url: str | None = None

    # Message broker configuration
    broker_url: str = "amqp://guest:guest@localhost:5672//"

    # Application settings
    max_upload_mb: int = 50

    @field_validator("api_prefix")
    @classmethod
    def validate_api_prefix(cls, value: str) -> str:
        if not value.startswith("/"):
            raise ValueError("api_prefix must start with '/'")
        if value != "/" and value.endswith("/"):
            raise ValueError("api_prefix must not end with '/'")
        return value

    @field_validator("max_upload_mb")
    @classmethod
    def validate_max_upload_mb(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("max_upload_mb must be positive")
        return value

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


settings = Settings()
