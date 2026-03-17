"""Tests for shared.config.errors — exception hierarchy and structured context."""

import pytest

from shared.config.errors import (
    DatabaseError,
    ErrorCategory,
    KalshiApiError,
    NotificationError,
    PaperTradeError,
    PredictionError,
    RecommendationError,
    ValidationError,
    WeatherApiError,
    WeatherBotError,
)


class TestErrorCategories:
    """Verify each exception subclass has the correct category."""

    def test_weather_api_error_category(self):
        assert WeatherApiError.category == ErrorCategory.WEATHER_API_ERROR

    def test_kalshi_api_error_category(self):
        assert KalshiApiError.category == ErrorCategory.KALSHI_API_ERROR

    def test_prediction_error_category(self):
        assert PredictionError.category == ErrorCategory.PREDICTION_ERROR

    def test_recommendation_error_category(self):
        assert RecommendationError.category == ErrorCategory.RECOMMENDATION_ERROR

    def test_paper_trade_error_category(self):
        assert PaperTradeError.category == ErrorCategory.PAPER_TRADE_ERROR

    def test_database_error_category(self):
        assert DatabaseError.category == ErrorCategory.DB_ERROR

    def test_notification_error_category(self):
        assert NotificationError.category == ErrorCategory.NOTIFICATION_ERROR

    def test_validation_error_category(self):
        assert ValidationError.category == ErrorCategory.VALIDATION_ERROR


class TestWeatherBotErrorConstruction:
    """Verify base exception carries all structured context."""

    def test_message_is_accessible(self):
        err = WeatherBotError("something broke")
        assert str(err) == "something broke"

    def test_all_fields_stored(self):
        err = WeatherBotError(
            "rate limited",
            correlation_id="abc-123",
            city="MIAMI",
            source="pirate_weather",
            http_status=429,
            retry_count=3,
        )
        assert err.correlation_id == "abc-123"
        assert err.city == "MIAMI"
        assert err.source == "pirate_weather"
        assert err.context == {"http_status": 429, "retry_count": 3}

    def test_optional_fields_default_to_none(self):
        err = WeatherBotError("simple error")
        assert err.correlation_id is None
        assert err.city is None
        assert err.source is None
        assert err.context == {}


class TestToLogDict:
    """Verify to_log_dict() produces correct structured output."""

    def test_minimal_error(self):
        err = WeatherApiError("timeout")
        log = err.to_log_dict()
        assert log["error_category"] == ErrorCategory.WEATHER_API_ERROR
        assert log["error_message"] == "timeout"
        assert "correlation_id" not in log
        assert "city" not in log

    def test_full_context(self):
        err = KalshiApiError(
            "auth failed",
            correlation_id="xyz-789",
            city="NYC",
            source="kalshi_rest",
            http_status=401,
            endpoint="/markets",
        )
        log = err.to_log_dict()
        assert log["error_category"] == ErrorCategory.KALSHI_API_ERROR
        assert log["error_message"] == "auth failed"
        assert log["correlation_id"] == "xyz-789"
        assert log["city"] == "NYC"
        assert log["source"] == "kalshi_rest"
        assert log["http_status"] == 401
        assert log["endpoint"] == "/markets"

    def test_context_keys_dont_overwrite_standard_fields(self):
        """Extra context should not collide with standard fields like error_category."""
        err = WeatherBotError(
            "test",
            correlation_id="c1",
            error_category="SHOULD_NOT_OVERWRITE",  # type: ignore[arg-type]
        )
        log = err.to_log_dict()
        # error_category comes from the class, not from **context
        assert log["error_category"] == ErrorCategory.VALIDATION_ERROR


class TestExceptionInheritance:
    """Verify all exceptions are catchable as WeatherBotError."""

    @pytest.mark.parametrize(
        "exc_class",
        [
            WeatherApiError,
            KalshiApiError,
            PredictionError,
            RecommendationError,
            PaperTradeError,
            DatabaseError,
            NotificationError,
            ValidationError,
        ],
    )
    def test_subclass_caught_by_base(self, exc_class: type):
        with pytest.raises(WeatherBotError):
            raise exc_class("test error")

    @pytest.mark.parametrize(
        "exc_class",
        [
            WeatherApiError,
            KalshiApiError,
            PredictionError,
            RecommendationError,
            PaperTradeError,
            DatabaseError,
            NotificationError,
            ValidationError,
        ],
    )
    def test_subclass_also_catchable_as_exception(self, exc_class: type):
        with pytest.raises(Exception):
            raise exc_class("test error")
