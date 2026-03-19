"""
Abstract base class for all weather API clients.

Provides:
- Shared httpx.Client with connection pooling
- Retry with exponential backoff (hand-rolled)
- Structured logging with correlation ID
- Configurable timeouts per source
- Inter-request delay support (for NWS rate limiting)
- Helper methods: _extract_date(), _to_optional_float()

Subclasses implement: _build_url(), _get_headers(), _parse_response().
_parse_response returns ParsedForecast; the base class constructs ForecastResult.
"""

import random
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import date, datetime
from email.utils import parsedate_to_datetime
import math
from typing import Any

import httpx

from shared.config.errors import WeatherApiError, WeatherBotError
from shared.config.logging import get_logger
from shared.db.enums import WeatherSource

from .models import ForecastResult, ParsedForecast

logger = get_logger("weather-client")


class WeatherClient(ABC):
    """Base class for weather API clients.

    Args:
        source: Canonical source identifier.
        connect_timeout: TCP connection timeout in seconds.
        read_timeout: Response read timeout in seconds.
        max_retries: Max retry attempts on retryable errors (429, 500+, timeouts).
        backoff_base: Base for exponential backoff calculation (seconds).
        inter_request_delay: Seconds to wait between sequential city requests.
            NWS requires ~1s. Other sources default to 0.
            Stored for the orchestrator to read; not used internally by the base class.
        sleep_fn: Sleep function (injectable for testing).
    """

    def __init__(
        self,
        source: WeatherSource,
        *,
        connect_timeout: float = 5.0,
        read_timeout: float = 15.0,
        max_retries: int = 3,
        backoff_base: float = 2.0,
        inter_request_delay: float = 0.0,
        sleep_fn: Callable[[float], None] = time.sleep,
        jitter_fn: Callable[[float, float], float] = random.uniform,
    ) -> None:
        self.source = source
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.inter_request_delay = inter_request_delay
        self._sleep_fn = sleep_fn
        self._jitter_fn = jitter_fn

        self._client = httpx.Client(
            timeout=httpx.Timeout(connect=connect_timeout, read=read_timeout, write=5.0, pool=5.0),
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
            follow_redirects=True,
        )

    def close(self) -> None:
        """Close the underlying httpx client. Call on shutdown."""
        self._client.close()

    def __enter__(self) -> "WeatherClient":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Helper methods for subclasses
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_date(forecast_date: datetime | date) -> date:
        """Extract a date from a datetime, handling both types defensively."""
        return forecast_date.date() if isinstance(forecast_date, datetime) else forecast_date

    @staticmethod
    def _to_optional_float(value: float | int | str | None) -> float | None:
        """Convert a value to float, returning None if input is None."""
        return float(value) if value is not None else None

    @staticmethod
    def _parse_retry_after(retry_after: str | None) -> float | None:
        """Parse Retry-After seconds or HTTP-date into a non-negative delay."""
        if retry_after is None:
            return None

        candidate = retry_after.strip()
        if candidate == "":
            return None

        try:
            seconds = float(candidate)
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(candidate)
            except (TypeError, ValueError, IndexError, OverflowError):
                return None
            now = datetime.now(retry_at.tzinfo)
            seconds = (retry_at - now).total_seconds()

        if not math.isfinite(seconds):
            return None
        if seconds < 0:
            return 0.0
        return seconds

    def _compute_retry_delay(self, attempt: int, response: httpx.Response | None = None) -> float:
        """Compute the next retry delay using Retry-After or jittered backoff."""
        retry_after_seconds = None
        if response is not None:
            retry_after_seconds = self._parse_retry_after(response.headers.get("Retry-After"))

        if retry_after_seconds is not None:
            return retry_after_seconds

        backoff_ceiling = self.backoff_base ** attempt
        return self._jitter_fn(0.0, backoff_ceiling)

    # ------------------------------------------------------------------
    # Core fetch flow
    # ------------------------------------------------------------------

    def fetch_forecast(
        self,
        city_code: str,
        lat: float,
        lon: float,
        forecast_date: datetime,
        *,
        correlation_id: str | None = None,
        city_timezone: str | None = None,
    ) -> ForecastResult:
        """Fetch a forecast for a single city and date.

        Args:
            city_code: Kalshi ticker code (e.g., "NYC").
            lat: City latitude.
            lon: City longitude.
            forecast_date: The date to forecast for.
            correlation_id: Optional ID for log tracing.
            city_timezone: IANA timezone (e.g., "America/New_York") for
                sources that need local-date filtering (OWM).

        Returns:
            ForecastResult with parsed temperatures and raw response.

        Raises:
            WeatherApiError: On unrecoverable API errors (after retries).
            ValidationError: On invalid temperature data (via ForecastResult).
        """
        url = self._build_url(city_code, lat, lon, forecast_date)
        headers = self._get_headers()
        params = self._get_params(city_code, lat, lon, forecast_date)

        response = self._request_with_retry(url, headers, city_code, correlation_id, params=params)

        try:
            data = response.json()
        except Exception as exc:
            raise WeatherApiError(
                f"Malformed JSON from {self.source} for {city_code}",
                correlation_id=correlation_id,
                city=city_code,
                source=self.source,
                http_status=response.status_code,
            ) from exc

        try:
            parsed = self._parse_response(data, city_code, forecast_date, city_timezone=city_timezone)
        except WeatherBotError:
            raise
        except (KeyError, TypeError, ValueError, IndexError, OSError) as exc:
            raise WeatherApiError(
                f"Failed to parse {self.source} response for {city_code}",
                correlation_id=correlation_id,
                city=city_code,
                source=self.source,
            ) from exc

        return ForecastResult(
            source=self.source,
            city_code=city_code,
            forecast_date=forecast_date,
            issued_at=parsed.issued_at,
            temp_high=parsed.temp_high,
            temp_low=parsed.temp_low,
            raw_response=parsed.raw_response if parsed.raw_response is not None else data,
        )

    def _request_with_retry(
        self,
        url: str,
        headers: dict[str, str],
        city_code: str,
        correlation_id: str | None,
        *,
        params: dict[str, str] | None = None,
    ) -> httpx.Response:
        """Execute HTTP GET with exponential backoff on retryable errors.

        Retries on: HTTP 429, 500+, connection errors, timeouts.
        Does NOT retry on: 4xx (except 429).
        """
        last_exception: Exception | None = None

        for attempt in range(self.max_retries + 1):
            response: httpx.Response | None = None
            try:
                response = self._client.get(url, headers=headers, params=params)

                if response.status_code == 200:
                    return response

                if response.status_code == 429 or response.status_code >= 500:
                    logger.warning(
                        "retryable_http_error",
                        source=self.source,
                        city=city_code,
                        status=response.status_code,
                        attempt=attempt + 1,
                        max_retries=self.max_retries,
                        correlation_id=correlation_id,
                    )
                    last_exception = WeatherApiError(
                        f"HTTP {response.status_code} from {self.source} for {city_code}",
                        correlation_id=correlation_id,
                        city=city_code,
                        source=self.source,
                        http_status=response.status_code,
                    )
                else:
                    # Non-retryable client error (4xx except 429)
                    raise WeatherApiError(
                        f"HTTP {response.status_code} from {self.source} for {city_code}",
                        correlation_id=correlation_id,
                        city=city_code,
                        source=self.source,
                        http_status=response.status_code,
                    )

            except httpx.TimeoutException as exc:
                logger.warning(
                    "request_timeout",
                    source=self.source,
                    city=city_code,
                    attempt=attempt + 1,
                    max_retries=self.max_retries,
                    correlation_id=correlation_id,
                )
                last_exception = WeatherApiError(
                    f"Timeout from {self.source} for {city_code}",
                    correlation_id=correlation_id,
                    city=city_code,
                    source=self.source,
                )
                last_exception.__cause__ = exc

            except httpx.HTTPError as exc:
                logger.warning(
                    "connection_error",
                    source=self.source,
                    city=city_code,
                    attempt=attempt + 1,
                    max_retries=self.max_retries,
                    correlation_id=correlation_id,
                )
                last_exception = WeatherApiError(
                    f"Connection error from {self.source} for {city_code}",
                    correlation_id=correlation_id,
                    city=city_code,
                    source=self.source,
                )
                last_exception.__cause__ = exc

            except WeatherApiError:
                raise

            # Exponential backoff before retry (skip on last attempt)
            if attempt < self.max_retries:
                delay = self._compute_retry_delay(attempt, response)
                self._sleep_fn(delay)

        if last_exception is None:
            raise WeatherApiError(
                f"No retries attempted for {self.source}/{city_code} (max_retries={self.max_retries})",
                city=city_code,
                source=self.source,
            )
        raise last_exception

    @abstractmethod
    def _build_url(self, city_code: str, lat: float, lon: float, forecast_date: datetime) -> str:
        """Build the API request URL for a city and date.

        Do NOT embed API keys or secrets in the URL. Use _get_params() instead
        so credentials are passed via httpx's params= and never appear in log lines.
        """

    @abstractmethod
    def _get_headers(self) -> dict[str, str]:
        """Return HTTP headers for the request (API keys, User-Agent, etc.)."""

    def _get_params(self, city_code: str, lat: float, lon: float, forecast_date: datetime) -> dict[str, str] | None:
        """Return query parameters for the request.

        Prefer _get_headers() for API keys when the API supports it.
        When the API only supports query-param auth (PirateWeather,
        Visual Crossing, OWM), pass the key here with a comment
        documenting the constraint. Never embed keys in _build_url().
        """
        return None

    @abstractmethod
    def _parse_response(
        self,
        data: dict[str, Any],
        city_code: str,
        forecast_date: datetime,
        *,
        city_timezone: str | None = None,
    ) -> ParsedForecast:
        """Parse the API JSON response into a ParsedForecast.

        Return a ParsedForecast with temp_high, temp_low, and issued_at.
        Optionally set raw_response to override what gets stored (e.g., trimmed data).

        Args:
            city_timezone: IANA timezone for local-date filtering (used by OWM).
        """
