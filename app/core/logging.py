"""Structured logging configuration using structlog."""

import logging
import sys

import structlog
from structlog.types import Processor

# Global service name for logging
_service_name: str = "draupnir"


def add_service_name(
    _: structlog.types.WrappedLogger,
    __: str,
    event_dict: structlog.types.EventDict,
) -> structlog.types.EventDict:
    """Add service name to all log entries."""
    if "service" not in event_dict:
        event_dict["service"] = _service_name
    return event_dict


def ensure_msg_field(
    _: structlog.types.WrappedLogger,
    __: str,
    event_dict: structlog.types.EventDict,
) -> structlog.types.EventDict:
    """Ensure msg field exists (copy from event if needed)."""
    if "msg" not in event_dict and "event" in event_dict:
        event_dict["msg"] = event_dict["event"]
    return event_dict


def configure_logging(service_name: str = "draupnir", log_level: str = "INFO") -> None:
    """Configure structlog for JSON output with required fields.

    Args:
        service_name: Name of the service for log entries.
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    """
    global _service_name
    _service_name = service_name

    # Shared processors for all loggers
    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", key="ts"),
        structlog.processors.format_exc_info,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
        add_service_name,
        ensure_msg_field,
    ]

    # Configure structlog
    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Configure stdlib logging to use structlog formatter
    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.JSONRenderer(),
        ],
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(getattr(logging, log_level.upper()))


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Get a structured logger instance.

    Args:
        name: Logger name (usually __name__).

    Returns:
        A bound logger instance.
    """
    return structlog.get_logger(name)  # type: ignore[no-any-return]
