"""
Custom exception hierarchy for the Kalshi weather bot.

Every exception carries structured context (category, correlation_id, city, source)
so error logs always contain enough information to trace the root cause.

Usage:
    raise WeatherApiError(
        "HTTP 429 rate limited",
        correlation_id=corr_id,
        city="MIAMI",
        source="pirate_weather",
        http_status=429,
        retry_count=3,
    )
"""

from enum import StrEnum
from typing import Any


class ErrorCategory(StrEnum):
    """Structured error categories for log filtering and alerting."""

    WEATHER_API_ERROR = "WEATHER_API_ERROR"
    KALSHI_API_ERROR = "KALSHI_API_ERROR"
    PREDICTION_ERROR = "PREDICTION_ERROR"
    RECOMMENDATION_ERROR = "RECOMMENDATION_ERROR"
    PAPER_TRADE_ERROR = "PAPER_TRADE_ERROR"
    DB_ERROR = "DB_ERROR"
    NOTIFICATION_ERROR = "NOTIFICATION_ERROR"
    VALIDATION_ERROR = "VALIDATION_ERROR"


class WeatherBotError(Exception):
    """Base exception for all Kalshi weather bot errors.

    All subclasses carry structured context that gets automatically
    included in structured log output via the logging module.
    """

    category: ErrorCategory = ErrorCategory.VALIDATION_ERROR  # default, overridden by subclasses

    def __init__(
        self,
        message: str,
        *,
        correlation_id: str | None = None,
        city: str | None = None,
        source: str | None = None,
        **context: Any,
    ) -> None:
        self.correlation_id = correlation_id
        self.city = city
        self.source = source
        self.context: dict[str, Any] = context
        super().__init__(message)

    def to_log_dict(self) -> dict[str, object]:
        """Return a dict suitable for structured logging.

        Standard fields (error_category, error_message, etc.) always take
        precedence over arbitrary context keys to prevent accidental overwriting.
        """
        # Start with arbitrary context, then overlay standard fields so they always win
        result: dict[str, object] = {}
        if self.context:
            result.update(self.context)
        result["error_category"] = self.category
        result["error_message"] = str(self)
        if self.correlation_id:
            result["correlation_id"] = self.correlation_id
        if self.city:
            result["city"] = self.city
        if self.source:
            result["source"] = self.source
        return result


class WeatherApiError(WeatherBotError):
    """External weather API failures (HTTP errors, timeouts, invalid responses)."""

    category = ErrorCategory.WEATHER_API_ERROR


class KalshiApiError(WeatherBotError):
    """Kalshi REST/WebSocket failures (auth, rate limits, connection drops)."""

    category = ErrorCategory.KALSHI_API_ERROR


class PredictionError(WeatherBotError):
    """Model failures (insufficient data, NaN results, bracket sum mismatch)."""

    category = ErrorCategory.PREDICTION_ERROR


class RecommendationError(WeatherBotError):
    """Gap/EV calculation failures, missing market data."""

    category = ErrorCategory.RECOMMENDATION_ERROR


class PaperTradeError(WeatherBotError):
    """Settlement failures, missing outcomes, P&L calculation errors."""

    category = ErrorCategory.PAPER_TRADE_ERROR


class DatabaseError(WeatherBotError):
    """PostgreSQL connection, migration, constraint violations."""

    category = ErrorCategory.DB_ERROR


class NotificationError(WeatherBotError):
    """SendGrid failures, email template errors."""

    category = ErrorCategory.NOTIFICATION_ERROR


class ValidationError(WeatherBotError):
    """Input validation failures (API requests, weather data range checks)."""

    category = ErrorCategory.VALIDATION_ERROR
