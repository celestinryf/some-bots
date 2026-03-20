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
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import structlog

# Sensitive field substrings — case-insensitive match on log event dict keys.
# If any key contains one of these substrings, the value is replaced with [REDACTED].
_SENSITIVE_SUBSTRINGS: tuple[str, ...] = (
    "api_key",
    "apikey",      # PirateWeather header (no underscore)
    "appid",       # OpenWeatherMap query param
    "password",
    "token",
    "authorization",
    "private_key",
    "secret",
    "credential",
    "pem",
)


def _is_sensitive_key(key: str) -> bool:
    return any(s in key.lower() for s in _SENSITIVE_SUBSTRINGS)


def _redact_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: ("[REDACTED]" if _is_sensitive_key(key) else _redact_value(item))
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_redact_value(item) for item in value)
    if isinstance(value, str):
        lower_value = value.lower()
        if any(f"{token}=" in lower_value for token in _SENSITIVE_SUBSTRINGS):
            return "[REDACTED]"
        return sanitize_url(value)
    return value


def _redact_sensitive(
    logger: Any, method: str | None, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """Structlog processor that redacts sensitive field values."""
    for key in list(event_dict.keys()):
        if _is_sensitive_key(key):
            event_dict[key] = "[REDACTED]"
            continue
        event_dict[key] = _redact_value(event_dict[key])
    return event_dict


def _add_timestamp(
    logger: Any, method: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """Add ISO-8601 timestamp if not already present."""
    if "timestamp" not in event_dict:
        event_dict["timestamp"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    return event_dict


def sanitize_url(url: str) -> str:
    """Redact sensitive query parameters before they are logged."""
    parsed = urlsplit(url)
    if not parsed.query:
        return url

    sanitized_query = urlencode(
        [
            (key, "[REDACTED]" if _is_sensitive_key(key) else value)
            for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        ],
        doseq=True,
    )
    return urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, sanitized_query, parsed.fragment)
    )


def build_external_failure_context(
    *,
    source: str,
    operation: str,
    error: Exception | str,
    error_category: str = "EXTERNAL_API_ERROR",
    url: str | None = None,
    http_status: int | None = None,
    **context: Any,
) -> dict[str, Any]:
    """Build a normalized context payload for external/API failure logs."""
    payload: dict[str, Any] = {
        "error_category": error_category,
        "source": source,
        "operation": operation,
        "error_message": str(error),
    }
    if url:
        payload["url"] = sanitize_url(url)
    if http_status is not None:
        payload["http_status"] = http_status

    for key, value in context.items():
        if value is not None:
            payload[key] = value

    return payload


def log_external_failure(
    logger: structlog.stdlib.BoundLogger,
    event: str,
    *,
    source: str,
    operation: str,
    error: Exception | str,
    error_category: str = "EXTERNAL_API_ERROR",
    url: str | None = None,
    http_status: int | None = None,
    **context: Any,
) -> None:
    """Emit a contract-consistent structured error log for external/API failures."""
    logger.error(
        event,
        **build_external_failure_context(
            source=source,
            operation=operation,
            error=error,
            error_category=error_category,
            url=url,
            http_status=http_status,
            **context,
        ),
    )


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
        try:
            from shared.config.settings import get_settings

            json_output = get_settings().is_production
        except Exception:
            # Keep logging boot resilient even if settings fail validation early.
            json_output = os.environ.get("ENVIRONMENT", "development").strip().lower() == "production"

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
