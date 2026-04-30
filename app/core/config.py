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

    @field_validator("api_prefix")
    @classmethod
    def validate_api_prefix(cls, value: str) -> str:
        if not value.startswith("/"):
            raise ValueError("api_prefix must start with '/'")
        if value != "/" and value.endswith("/"):
            raise ValueError("api_prefix must not end with '/'")
        return value

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


settings = Settings()
