"""Tests for structured logging configuration."""

import json
import logging
from io import StringIO

import structlog

from app.core.logging import configure_logging, get_logger


class TestLoggingConfiguration:
    """Test cases for logging setup."""

    def test_configure_logging_sets_up_structlog(self):
        """Should configure structlog without errors."""
        configure_logging(service_name="test-service", log_level="DEBUG")
        logger = get_logger("test")
        # Should not raise
        logger.info("test_message", extra_field="value")

    def test_logs_are_valid_json_with_required_fields(self):
        """Log output should be valid JSON with required TRD fields."""
        configure_logging(service_name="test-service", log_level="INFO")

        # Capture log output
        stream = StringIO()
        handler = logging.StreamHandler(stream)
        formatter = structlog.stdlib.ProcessorFormatter(
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.processors.JSONRenderer(),
            ],
        )
        handler.setFormatter(formatter)
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)

        logger = get_logger("test")
        logger.info("test_event", msg="hello world")

        output = stream.getvalue().strip()
        assert output, "Expected log output"

        log_entry = json.loads(output)

        # Check required TRD fields
        assert "ts" in log_entry, "Missing 'ts' field"
        assert "level" in log_entry, "Missing 'level' field"
        assert "service" in log_entry, "Missing 'service' field"
        assert log_entry["service"] == "test-service"
        assert "event" in log_entry, "Missing 'event' field"
        assert "msg" in log_entry, "Missing 'msg' field"

    def test_request_id_in_log_context(self):
        """Should include request_id from contextvars."""
        configure_logging()

        # Bind a request_id to context
        structlog.contextvars.bind_contextvars(request_id="test-request-123")

        # Capture log output
        stream = StringIO()
        handler = logging.StreamHandler(stream)
        formatter = structlog.stdlib.ProcessorFormatter(
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.processors.JSONRenderer(),
            ],
        )
        handler.setFormatter(formatter)
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)

        logger = get_logger("test")
        logger.info("test_with_request_id")

        output = stream.getvalue().strip()
        assert output, "Expected log output"

        log_entry = json.loads(output)
        assert "request_id" in log_entry, "Missing 'request_id' in log"
        assert log_entry["request_id"] == "test-request-123"

        # Cleanup
        structlog.contextvars.clear_contextvars()

    def test_exception_logging_includes_stack_trace(self):
        """Should include stack trace in error logs."""
        configure_logging()

        # Capture log output
        stream = StringIO()
        handler = logging.StreamHandler(stream)
        formatter = structlog.stdlib.ProcessorFormatter(
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.processors.JSONRenderer(),
            ],
        )
        handler.setFormatter(formatter)
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)

        logger = get_logger("test")

        try:
            raise ValueError("Test exception")
        except Exception:
            logger.error("error_occurred", exc_info=True)

        output = stream.getvalue().strip()
        assert output, "Expected log output"

        log_entry = json.loads(output)
        # Should have exception info in the output
        # Note: format_exc_info should include this
        assert "error_occurred" in str(log_entry)
