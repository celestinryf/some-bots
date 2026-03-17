"""
Structured logging configuration using structlog.

All services call setup_logging() once at startup to get identical logging behavior:
- JSON output in production, pretty console output in development
- Automatic redaction of sensitive fields (api_key, password, token, etc.)
- Correlation ID binding for request tracing
- Timestamp, log level, service name in every log line

Usage:
    from shared.config.logging import setup_logging, get_logger, bind_correlation_id

    setup_logging("INFO")
    logger = get_logger("data-ingestion")
    bind_correlation_id()  # generates and binds a new correlation ID
    logger.info("ingestion started", city="MIAMI", source="nws")
"""

import datetime
import uuid
from typing import Any

import structlog

# Sensitive field substrings — case-insensitive match on log event dict keys.
# If any key contains one of these substrings, the value is replaced with [REDACTED].
_SENSITIVE_SUBSTRINGS: tuple[str, ...] = (
    "api_key",
    "password",
    "token",
    "authorization",
    "private_key",
    "secret",
    "credential",
    "pem",
)


def _redact_sensitive(
    logger: Any, method: str | None, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """Structlog processor that redacts sensitive field values."""
    for key in list(event_dict.keys()):
        if any(s in key.lower() for s in _SENSITIVE_SUBSTRINGS):
            event_dict[key] = "[REDACTED]"
    return event_dict


def _add_timestamp(
    logger: Any, method: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """Add ISO-8601 timestamp if not already present."""
    if "timestamp" not in event_dict:
        event_dict["timestamp"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    return event_dict


def setup_logging(log_level: str = "INFO", json_output: bool | None = None) -> None:
    """Configure structlog for the entire process.

    Args:
        log_level: Minimum log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        json_output: Force JSON (True) or console (False) output.
                     If None, uses JSON in production, console in development.
    """
    import logging
    import os

    if json_output is None:
        json_output = os.environ.get("ENVIRONMENT", "development") == "production"

    # Shared processors that run on every log line
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        _add_timestamp,
        _redact_sensitive,
        structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
    ]

    structlog.configure(
        processors=shared_processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Configure the stdlib logging handler with structlog formatting
    if json_output:
        formatter = structlog.stdlib.ProcessorFormatter(
            processor=structlog.processors.JSONRenderer(),
        )
    else:
        formatter = structlog.stdlib.ProcessorFormatter(
            processor=structlog.dev.ConsoleRenderer(colors=True),
        )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))


def get_logger(service_name: str) -> structlog.stdlib.BoundLogger:
    """Get a logger bound to a service name.

    Args:
        service_name: Identifies which service emitted the log (e.g., "data-ingestion").
    """
    return structlog.get_logger(service=service_name)


def generate_correlation_id() -> str:
    """Generate a new UUID4 correlation ID."""
    return str(uuid.uuid4())


def bind_correlation_id(correlation_id: str | None = None) -> str:
    """Bind a correlation ID to the current context (thread/async task).

    If no ID is provided, generates a new one. The ID will appear in
    all subsequent log lines from this context.

    Args:
        correlation_id: Existing ID to bind, or None to generate a new one.

    Returns:
        The correlation ID that was bound.
    """
    if correlation_id is None:
        correlation_id = generate_correlation_id()
    structlog.contextvars.bind_contextvars(correlation_id=correlation_id)
    return correlation_id


def clear_correlation_id() -> None:
    """Clear the correlation ID from the current context."""
    structlog.contextvars.unbind_contextvars("correlation_id")
