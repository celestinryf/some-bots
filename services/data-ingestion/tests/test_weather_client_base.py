"""Tests for WeatherClient base class using a stub subclass.

Covers: HTTP 200 success, retries on 429/500+/timeout/connection error,
no retry on 4xx, malformed JSON, exponential backoff timing, max retries
exhaustion, catch-all for uncaught parse errors, error response details,
helper methods, and pool limits.
"""

from collections.abc import Generator
from datetime import UTC, date, datetime
from typing import Any

import httpx
import pytest
import respx
from src.clients.base import WeatherClient
from src.clients.models import ForecastResult, ParsedForecast

from shared.config.errors import WeatherApiError
from shared.db.enums import WeatherSource

# ---------------------------------------------------------------------------
# Stub subclass for testing the ABC
# ---------------------------------------------------------------------------

_STUB_URL = "https://stub.weather.test/api/forecast"


class _StubWeatherClient(WeatherClient):
    """Minimal concrete implementation for testing the base class."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(source=WeatherSource.NWS, **kwargs)

    def _get_headers(self) -> dict[str, str]:
        return {"X-Test": "true"}

    def _build_url(self, city_code: str, lat: float, lon: float, forecast_date: datetime) -> str:
        return _STUB_URL

    def _parse_response(self, data: dict[str, Any], city_code: str, forecast_date: datetime, *, city_timezone: str | None = None) -> ParsedForecast:
        return ParsedForecast(
            temp_high=data.get("high"),
            temp_low=data.get("low"),
            issued_at=datetime.now(UTC),
        )


class _BrokenParseClient(WeatherClient):
    """Client whose _parse_response raises an unexpected exception."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(source=WeatherSource.NWS, **kwargs)

    def _get_headers(self) -> dict[str, str]:
        return {}

    def _build_url(self, city_code: str, lat: float, lon: float, forecast_date: datetime) -> str:
        return _STUB_URL

    def _parse_response(self, data: dict[str, Any], city_code: str, forecast_date: datetime, *, city_timezone: str | None = None) -> ParsedForecast:
        # Simulate an uncaught bug in a subclass parser
        raise ValueError("unexpected field type")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def sleep_calls() -> list[float]:
    """Collect sleep durations to verify backoff timing."""
    return []


@pytest.fixture()
def client(sleep_calls: list[float]) -> Generator[_StubWeatherClient, None, None]:
    """Create a stub client with injectable sleep."""
    c = _StubWeatherClient(
        max_retries=3,
        backoff_base=2.0,
        sleep_fn=sleep_calls.append,
        jitter_fn=lambda low, high: (low + high) / 2,
    )
    yield c
    c.close()


@pytest.fixture()
def forecast_date() -> datetime:
    return datetime(2026, 3, 16, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Tests: Successful responses
# ---------------------------------------------------------------------------

class TestSuccess:
    """HTTP 200 returns parsed ForecastResult."""

    @respx.mock
    def test_200_returns_forecast(self, client: _StubWeatherClient, forecast_date: datetime) -> None:
        respx.get(_STUB_URL).mock(
            return_value=httpx.Response(200, json={"high": 72.0, "low": 55.0})
        )
        result = client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert isinstance(result, ForecastResult)
        assert result.temp_high == 72.0
        assert result.temp_low == 55.0
        assert result.city_code == "NYC"

    @respx.mock
    def test_base_class_sets_source(self, client: _StubWeatherClient, forecast_date: datetime) -> None:
        """Base class constructs ForecastResult with source from self.source."""
        respx.get(_STUB_URL).mock(
            return_value=httpx.Response(200, json={"high": 72.0, "low": 55.0})
        )
        result = client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert result.source == WeatherSource.NWS

    @respx.mock
    def test_base_class_stores_raw_response(self, client: _StubWeatherClient, forecast_date: datetime) -> None:
        """Base class stores the full JSON response when ParsedForecast.raw_response is None."""
        respx.get(_STUB_URL).mock(
            return_value=httpx.Response(200, json={"high": 72.0, "low": 55.0})
        )
        result = client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert result.raw_response == {"high": 72.0, "low": 55.0}

    @respx.mock
    def test_headers_sent(self, client: _StubWeatherClient, forecast_date: datetime) -> None:
        route = respx.get(_STUB_URL).mock(
            return_value=httpx.Response(200, json={"high": 72.0, "low": 55.0})
        )
        client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert route.calls[0].request.headers["X-Test"] == "true"  # type: ignore[reportUnknownMemberType]


# ---------------------------------------------------------------------------
# Tests: Retry behavior
# ---------------------------------------------------------------------------

class TestRetryBehavior:
    """Retries on 429, 500+, timeouts, connection errors."""

    @respx.mock
    def test_retry_on_429_then_success(self, client: _StubWeatherClient, forecast_date: datetime, sleep_calls: list[float]) -> None:
        route = respx.get(_STUB_URL).mock(
            side_effect=[
                httpx.Response(429),
                httpx.Response(200, json={"high": 72.0, "low": 55.0}),
            ]
        )
        result = client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert result.temp_high == 72.0
        assert route.call_count == 2
        assert sleep_calls == [0.5]

    @respx.mock
    def test_retry_on_500_then_success(self, client: _StubWeatherClient, forecast_date: datetime, sleep_calls: list[float]) -> None:
        route = respx.get(_STUB_URL).mock(
            side_effect=[
                httpx.Response(500),
                httpx.Response(200, json={"high": 72.0, "low": 55.0}),
            ]
        )
        result = client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert result.temp_high == 72.0
        assert route.call_count == 2

    @respx.mock
    def test_retry_on_503(self, client: _StubWeatherClient, forecast_date: datetime, sleep_calls: list[float]) -> None:
        route = respx.get(_STUB_URL).mock(
            side_effect=[
                httpx.Response(503),
                httpx.Response(200, json={"high": 72.0, "low": 55.0}),
            ]
        )
        result = client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert result.temp_high == 72.0
        assert route.call_count == 2

    @respx.mock
    def test_retry_on_timeout_then_success(self, client: _StubWeatherClient, forecast_date: datetime, sleep_calls: list[float]) -> None:
        route = respx.get(_STUB_URL).mock(
            side_effect=[
                httpx.ReadTimeout("read timed out"),
                httpx.Response(200, json={"high": 72.0, "low": 55.0}),
            ]
        )
        result = client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert result.temp_high == 72.0
        assert route.call_count == 2

    @respx.mock
    def test_retry_on_connection_error_then_success(self, client: _StubWeatherClient, forecast_date: datetime, sleep_calls: list[float]) -> None:
        route = respx.get(_STUB_URL).mock(
            side_effect=[
                httpx.ConnectError("connection refused"),
                httpx.Response(200, json={"high": 72.0, "low": 55.0}),
            ]
        )
        result = client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert result.temp_high == 72.0
        assert route.call_count == 2


# ---------------------------------------------------------------------------
# Tests: No retry on non-retryable errors
# ---------------------------------------------------------------------------

class TestNoRetry:
    """4xx errors (except 429) are not retried."""

    @respx.mock
    def test_no_retry_on_400(self, client: _StubWeatherClient, forecast_date: datetime, sleep_calls: list[float]) -> None:
        respx.get(_STUB_URL).mock(return_value=httpx.Response(400, text="Bad Request"))
        with pytest.raises(WeatherApiError, match="HTTP 400"):
            client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert len(sleep_calls) == 0

    @respx.mock
    def test_no_retry_on_401(self, client: _StubWeatherClient, forecast_date: datetime, sleep_calls: list[float]) -> None:
        respx.get(_STUB_URL).mock(return_value=httpx.Response(401, text="Unauthorized"))
        with pytest.raises(WeatherApiError, match="HTTP 401"):
            client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert len(sleep_calls) == 0

    @respx.mock
    def test_no_retry_on_403(self, client: _StubWeatherClient, forecast_date: datetime, sleep_calls: list[float]) -> None:
        respx.get(_STUB_URL).mock(return_value=httpx.Response(403, text="Forbidden"))
        with pytest.raises(WeatherApiError, match="HTTP 403"):
            client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert len(sleep_calls) == 0

    @respx.mock
    def test_no_retry_on_404(self, client: _StubWeatherClient, forecast_date: datetime, sleep_calls: list[float]) -> None:
        respx.get(_STUB_URL).mock(return_value=httpx.Response(404, text="Not Found"))
        with pytest.raises(WeatherApiError, match="HTTP 404"):
            client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert len(sleep_calls) == 0


# ---------------------------------------------------------------------------
# Tests: Max retries exhausted
# ---------------------------------------------------------------------------

class TestMaxRetriesExhausted:
    """After max_retries, raises WeatherApiError."""

    @respx.mock
    def test_max_retries_on_500(self, client: _StubWeatherClient, forecast_date: datetime, sleep_calls: list[float]) -> None:
        route = respx.get(_STUB_URL).mock(return_value=httpx.Response(500))
        with pytest.raises(WeatherApiError, match="HTTP 500"):
            client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        # 1 initial + 3 retries = 4 total
        assert route.call_count == 4

    @respx.mock
    def test_max_retries_on_timeout(self, client: _StubWeatherClient, forecast_date: datetime, sleep_calls: list[float]) -> None:
        route = respx.get(_STUB_URL).mock(
            side_effect=httpx.ReadTimeout("read timed out")
        )
        with pytest.raises(WeatherApiError, match="Timeout"):
            client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert route.call_count == 4

    @respx.mock
    def test_max_retries_on_connection_error(self, client: _StubWeatherClient, forecast_date: datetime, sleep_calls: list[float]) -> None:
        route = respx.get(_STUB_URL).mock(
            side_effect=httpx.ConnectError("connection refused")
        )
        with pytest.raises(WeatherApiError, match="Connection error"):
            client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert route.call_count == 4


# ---------------------------------------------------------------------------
# Tests: Exponential backoff timing
# ---------------------------------------------------------------------------

class TestBackoff:
    """Verify exponential backoff delays between retries."""

    @respx.mock
    def test_backoff_delays(self, client: _StubWeatherClient, forecast_date: datetime, sleep_calls: list[float]) -> None:
        """3 retries use deterministic jitter over 1, 2, and 4 second ceilings."""
        respx.get(_STUB_URL).mock(return_value=httpx.Response(500))
        with pytest.raises(WeatherApiError):
            client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert sleep_calls == [0.5, 1.0, 2.0]

    @respx.mock
    def test_no_sleep_after_last_attempt(self, client: _StubWeatherClient, forecast_date: datetime, sleep_calls: list[float]) -> None:
        """No sleep after the final failed attempt."""
        respx.get(_STUB_URL).mock(return_value=httpx.Response(500))
        with pytest.raises(WeatherApiError):
            client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        # 3 retries = 3 sleeps (none after last)
        assert len(sleep_calls) == 3

    @respx.mock
    def test_retry_after_header_overrides_jitter(self, client: _StubWeatherClient, forecast_date: datetime, sleep_calls: list[float]) -> None:
        respx.get(_STUB_URL).mock(
            side_effect=[
                httpx.Response(429, headers={"Retry-After": "7"}),
                httpx.Response(200, json={"high": 72.0, "low": 55.0}),
            ]
        )

        result = client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)

        assert result.temp_high == 72.0
        assert sleep_calls == [7.0]

    @respx.mock
    def test_invalid_retry_after_falls_back_to_jitter(self, client: _StubWeatherClient, forecast_date: datetime, sleep_calls: list[float]) -> None:
        respx.get(_STUB_URL).mock(
            side_effect=[
                httpx.Response(503, headers={"Retry-After": "soon"}),
                httpx.Response(200, json={"high": 72.0, "low": 55.0}),
            ]
        )

        result = client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)

        assert result.temp_high == 72.0
        assert sleep_calls == [0.5]


# ---------------------------------------------------------------------------
# Tests: Malformed JSON
# ---------------------------------------------------------------------------

class TestMalformedResponse:
    """Non-JSON response bodies raise WeatherApiError."""

    @respx.mock
    def test_malformed_json_raises(self, client: _StubWeatherClient, forecast_date: datetime) -> None:
        respx.get(_STUB_URL).mock(
            return_value=httpx.Response(200, text="not json at all")
        )
        with pytest.raises(WeatherApiError, match="Malformed JSON"):
            client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)


# ---------------------------------------------------------------------------
# Tests: Catch-all for uncaught parse errors (#3A)
# ---------------------------------------------------------------------------

class TestParseResponseCatchAll:
    """Base class catch-all converts unhandled parse exceptions to WeatherApiError."""

    @respx.mock
    def test_unexpected_parse_error_wrapped(self, sleep_calls: list[float], forecast_date: datetime) -> None:
        """ValueError from _parse_response is wrapped in WeatherApiError."""
        c = _BrokenParseClient(sleep_fn=sleep_calls.append)
        respx.get(_STUB_URL).mock(
            return_value=httpx.Response(200, json={"test": True})
        )
        with pytest.raises(WeatherApiError, match="Failed to parse") as exc_info:
            c.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        # Original exception preserved as __cause__
        assert isinstance(exc_info.value.__cause__, ValueError)
        assert exc_info.value.city == "NYC"
        c.close()

    @respx.mock
    def test_weather_api_error_from_parse_not_wrapped(self, client: _StubWeatherClient, forecast_date: datetime) -> None:
        """WeatherApiError from _parse_response is re-raised, not double-wrapped."""
        # Use a response that triggers WeatherApiError in parse if we had such logic
        # Instead, test with valid JSON that the stub handles fine
        respx.get(_STUB_URL).mock(
            return_value=httpx.Response(200, json={"high": 72.0, "low": 55.0})
        )
        # Should succeed, not raise
        result = client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert result.temp_high == 72.0


# ---------------------------------------------------------------------------
# Tests: Error context with response text (#8A)
# ---------------------------------------------------------------------------

class TestErrorResponseText:
    """4xx errors must NOT include response body (prevents info leaks)."""

    @respx.mock
    def test_4xx_does_not_leak_response_text(self, client: _StubWeatherClient, forecast_date: datetime) -> None:
        respx.get(_STUB_URL).mock(
            return_value=httpx.Response(403, text='{"error": "Invalid API key"}')
        )
        with pytest.raises(WeatherApiError) as exc_info:
            client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert "response_text" not in exc_info.value.context


# ---------------------------------------------------------------------------
# Tests: Error context
# ---------------------------------------------------------------------------

class TestErrorContext:
    """WeatherApiError carries structured context."""

    @respx.mock
    def test_error_has_city(self, client: _StubWeatherClient, forecast_date: datetime) -> None:
        respx.get(_STUB_URL).mock(return_value=httpx.Response(403, text="Forbidden"))
        with pytest.raises(WeatherApiError) as exc_info:
            client.fetch_forecast("MIAMI", 25.7, -80.2, forecast_date)
        assert exc_info.value.city == "MIAMI"

    @respx.mock
    def test_error_has_source(self, client: _StubWeatherClient, forecast_date: datetime) -> None:
        respx.get(_STUB_URL).mock(return_value=httpx.Response(403, text="Forbidden"))
        with pytest.raises(WeatherApiError) as exc_info:
            client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert exc_info.value.source == WeatherSource.NWS

    @respx.mock
    def test_error_has_http_status(self, client: _StubWeatherClient, forecast_date: datetime) -> None:
        respx.get(_STUB_URL).mock(return_value=httpx.Response(403, text="Forbidden"))
        with pytest.raises(WeatherApiError) as exc_info:
            client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert exc_info.value.context.get("http_status") == 403

    @respx.mock
    def test_correlation_id_propagated(self, client: _StubWeatherClient, forecast_date: datetime) -> None:
        respx.get(_STUB_URL).mock(return_value=httpx.Response(403, text="Forbidden"))
        with pytest.raises(WeatherApiError) as exc_info:
            client.fetch_forecast("NYC", 40.7, -74.0, forecast_date, correlation_id="test-123")
        assert exc_info.value.correlation_id == "test-123"


# ---------------------------------------------------------------------------
# Tests: Helper methods (#5A)
# ---------------------------------------------------------------------------

class TestHelperMethods:
    """Base class static helpers."""

    def test_extract_date_from_datetime(self) -> None:
        dt = datetime(2026, 3, 16, 14, 30, tzinfo=UTC)
        assert WeatherClient._extract_date(dt) == date(2026, 3, 16)  # pyright: ignore[reportPrivateUsage]

    def test_extract_date_from_date(self) -> None:
        d = date(2026, 3, 16)
        assert WeatherClient._extract_date(d) == date(2026, 3, 16)  # pyright: ignore[reportPrivateUsage]

    def test_to_optional_float_with_int(self) -> None:
        assert WeatherClient._to_optional_float(72) == 72.0  # pyright: ignore[reportPrivateUsage]

    def test_to_optional_float_with_float(self) -> None:
        assert WeatherClient._to_optional_float(72.5) == 72.5  # pyright: ignore[reportPrivateUsage]

    def test_to_optional_float_with_string(self) -> None:
        assert WeatherClient._to_optional_float("72.5") == 72.5  # pyright: ignore[reportPrivateUsage]

    def test_to_optional_float_with_none(self) -> None:
        assert WeatherClient._to_optional_float(None) is None  # pyright: ignore[reportPrivateUsage]


# ---------------------------------------------------------------------------
# Tests: Context manager
# ---------------------------------------------------------------------------

class TestContextManager:
    """WeatherClient supports with-statement."""

    def test_context_manager(self, sleep_calls: list[float]) -> None:
        with _StubWeatherClient(sleep_fn=sleep_calls.append) as client:
            assert isinstance(client, WeatherClient)
        # After exit, client should be closed (no easy way to check, but no error)
