"""Application configuration via pydantic-settings."""

from pydantic import AliasChoices, Field, ValidationInfo, field_validator
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
    max_request_body_mb: int = 10
    libredwg_max_output_mb: int = 32
    # Ingestion adapter execution timeout. Raise for dense full-floor DWGs
    # (e.g. 207k+ entities / 447 MB dwgread JSON): set ADAPTER_TIMEOUT_SECONDS=600
    # together with LIBREDWG_MAX_OUTPUT_MB=700 and worker --concurrency=1 (~3 GB RAM
    # per sheet). Default preserves existing behaviour / fast-fail on normal inputs.
    adapter_timeout_seconds: float = 300.0
    # PyMuPDF vector extraction complexity caps. Sized well above real dense CAD
    # sheets (~16.5k path objects / single A0 page) but bounded to cap memory. On
    # exceeding a cap the adapter degrades to a partial, review-gated result with a
    # warning rather than failing the job. See issue #383.
    pymupdf_max_drawings_per_page: int = 64_000
    pymupdf_max_total_drawings: int = 250_000
    pymupdf_max_entities: int = 250_000
    # IFC extraction is semantic-first; shape tessellation is intentionally disabled
    # by default. Opt in to extract IfcSpace footprint geometry (for room
    # containment, issue #462) — this enables ifcopenshell.geom tessellation for
    # IfcSpace entities only. See ADR / issue #462.
    ifc_space_geometry_enabled: bool = False
    idempotency_key_hash_secret: str | None = None
    # How long an in-progress idempotency reservation may sit before an identical retry
    # is allowed to take it over (the original request crashed before completing).
    idempotency_in_progress_ttl_seconds: int = 900
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

    @field_validator("max_request_body_mb")
    @classmethod
    def validate_max_request_body_mb(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("max_request_body_mb must be positive")
        return value

    @field_validator("libredwg_max_output_mb")
    @classmethod
    def validate_libredwg_max_output_mb(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("libredwg_max_output_mb must be positive")
        return value

    @field_validator("adapter_timeout_seconds")
    @classmethod
    def validate_adapter_timeout_seconds(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("adapter_timeout_seconds must be positive")
        return value

    @field_validator(
        "pymupdf_max_drawings_per_page",
        "pymupdf_max_total_drawings",
        "pymupdf_max_entities",
    )
    @classmethod
    def validate_pymupdf_complexity_caps(cls, value: int, info: ValidationInfo) -> int:
        if value <= 0:
            raise ValueError(f"{info.field_name} must be positive")
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
