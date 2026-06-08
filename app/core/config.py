"""Application configuration via pydantic-settings."""

from pydantic import AliasChoices, Field, field_validator
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

    # PDF intake service boundary (optional, off by default).
    # When unset the core reports the pdf_intake_service adapter as
    # disabled_by_config and never attempts a network probe. See ADR 0010.
    pdf_intake_service_url: str | None = None
    pdf_intake_service_timeout_seconds: float = 0.5

    # Application settings
    max_upload_mb: int = 200
    libredwg_max_output_mb: int = 32
    idempotency_key_hash_secret: str | None = None
    storage_local_root: str = Field(
        default="var/uploads",
        validation_alias=AliasChoices("storage_local_root", "upload_storage_root"),
    )

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

    @field_validator("libredwg_max_output_mb")
    @classmethod
    def validate_libredwg_max_output_mb(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("libredwg_max_output_mb must be positive")
        return value

    @field_validator("pdf_intake_service_timeout_seconds")
    @classmethod
    def validate_pdf_intake_service_timeout_seconds(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("pdf_intake_service_timeout_seconds must be positive")
        return value

    @field_validator("storage_local_root")
    @classmethod
    def validate_storage_local_root(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("storage_local_root must not be empty")
        return value

    @property
    def upload_storage_root(self) -> str:
        """Backward-compatible alias for the canonical local storage root."""
        return self.storage_local_root

    @upload_storage_root.setter
    def upload_storage_root(self, value: str) -> None:
        self.storage_local_root = value

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


settings = Settings()
