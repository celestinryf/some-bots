"""
NWS (National Weather Service) API client.

api.weather.gov is free, no API key required. Requires User-Agent header.
Two-step process: resolve lat/lon → gridpoint, then fetch forecast.
Gridpoints are cached in-memory and optionally persisted to a JSON file.

Rate limit: ~1 request per second (inter_request_delay=1.0).
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from shared.config.errors import WeatherApiError
from shared.config.logging import get_logger
from shared.db.enums import WeatherSource

from .base import WeatherClient
from .models import ForecastResult, ParsedForecast

logger = get_logger("nws-client")

# NWS API base URL
_BASE_URL = "https://api.weather.gov"


class NwsClient(WeatherClient):
    """National Weather Service forecast client.

    Args:
        user_agent: Required User-Agent string for NWS API.
            Format: "(appname, contact@email.com)"
        gridpoint_cache_path: Optional path to persist gridpoint cache as JSON.
            If None, cache is in-memory only (lost on restart).
        **kwargs: Passed to WeatherClient base class.
    """

    def __init__(
        self,
        *,
        user_agent: str,
        gridpoint_cache_path: Path | None = None,
        **kwargs: Any,
    ) -> None:
        if not user_agent.strip():
            raise ValueError("NWS user_agent is required — set NWS_USER_AGENT env var")
        super().__init__(
            source=WeatherSource.NWS,
            read_timeout=20.0,  # NWS can be slow
            inter_request_delay=1.0,  # Required: ~1s between requests
            **kwargs,
        )
        self._user_agent = user_agent
        self._cache_path = gridpoint_cache_path
        # Cache: city_code → forecast URL (resolved from gridpoint)
        self._gridpoint_cache: dict[str, str] = {}
        self._load_cache()

    # ------------------------------------------------------------------
    # Gridpoint cache persistence
    # ------------------------------------------------------------------

    def _load_cache(self) -> None:
        """Load gridpoint cache from file if configured and file exists."""
        if not self._cache_path or not self._cache_path.exists():
            return
        try:
            with open(self._cache_path) as f:
                self._gridpoint_cache = json.load(f)
            logger.info(
                "gridpoint_cache_loaded",
                count=len(self._gridpoint_cache),
                path=str(self._cache_path),
            )
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("gridpoint_cache_load_failed", error=str(exc))

    def _save_cache(self) -> None:
        """Persist gridpoint cache to file if configured."""
        if not self._cache_path:
            return
        try:
            self._cache_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._cache_path, "w") as f:
                json.dump(self._gridpoint_cache, f, indent=2)
        except OSError as exc:
            logger.warning("gridpoint_cache_save_failed", error=str(exc))

    # ------------------------------------------------------------------
    # Overridden fetch flow (two-step: gridpoint → forecast)
    # ------------------------------------------------------------------

    def fetch_forecast(
        self,
        city_code: str,
        lat: float,
        lon: float,
        forecast_date: datetime,
        *,
        correlation_id: str | None = None,
    ) -> ForecastResult:
        """Fetch forecast, resolving gridpoint first if not cached."""
        if city_code not in self._gridpoint_cache:
            self._resolve_gridpoint(city_code, lat, lon, correlation_id)
            # Respect NWS rate limit between gridpoint and forecast requests
            if self.inter_request_delay > 0:
                self._sleep_fn(self.inter_request_delay)
        return super().fetch_forecast(
            city_code, lat, lon, forecast_date, correlation_id=correlation_id
        )

    def _resolve_gridpoint(
        self, city_code: str, lat: float, lon: float, correlation_id: str | None
    ) -> None:
        """Resolve lat/lon to a NWS gridpoint and cache the forecast URL."""
        url = f"{_BASE_URL}/points/{lat},{lon}"
        headers = self._get_headers()

        response = self._request_with_retry(url, headers, city_code, correlation_id)

        try:
            data = response.json()
            forecast_url = data["properties"]["forecast"]
        except (KeyError, TypeError) as exc:
            raise WeatherApiError(
                f"Failed to resolve NWS gridpoint for {city_code} ({lat}, {lon})",
                correlation_id=correlation_id,
                city=city_code,
                source=self.source,
            ) from exc

        self._gridpoint_cache[city_code] = forecast_url
        self._save_cache()
        logger.info("gridpoint_resolved", city=city_code, forecast_url=forecast_url)

    # ------------------------------------------------------------------
    # Abstract method implementations
    # ------------------------------------------------------------------

    def _get_headers(self) -> dict[str, str]:
        return {"User-Agent": self._user_agent, "Accept": "application/geo+json"}

    def _build_url(self, city_code: str, lat: float, lon: float, forecast_date: datetime) -> str:
        """Return the cached forecast URL. Gridpoint is already resolved by fetch_forecast."""
        return self._gridpoint_cache[city_code]

    def _parse_response(self, data: dict[str, Any], city_code: str, forecast_date: datetime) -> ParsedForecast:
        """Parse NWS forecast response.

        NWS returns 12-hour periods:
        - Daytime (6AM-6PM): contains the day's HIGH temperature.
        - Overnight (6PM-6AM): contains the night's LOW temperature.
        Overnight periods cross midnight, so we match by checking if the
        period's start OR end date equals the target date.
        """
        try:
            periods = data["properties"]["periods"]
        except (KeyError, TypeError) as exc:
            raise WeatherApiError(
                f"Missing 'properties.periods' in NWS response for {city_code}",
                city=city_code,
                source=self.source,
            ) from exc

        target_date = self._extract_date(forecast_date)
        temp_high: float | None = None
        temp_low: float | None = None
        matched_periods: list[dict[str, Any]] = []

        for period in periods:
            try:
                start_str = period["startTime"]
                period_date = datetime.fromisoformat(start_str).date()
            except (KeyError, ValueError):
                continue

            is_daytime = period.get("isDaytime", True)

            # Check if this period belongs to the target date
            if period_date == target_date:
                pass  # Direct match
            elif not is_daytime:
                # Overnight periods cross midnight — check if end date matches
                try:
                    end_date = datetime.fromisoformat(period.get("endTime", "")).date()
                except ValueError:
                    continue
                if end_date != target_date:
                    continue
            else:
                continue

            matched_periods.append(period)

            temp = period.get("temperature")
            if temp is None:
                continue

            temp_val = float(temp)
            if is_daytime and temp_high is None:
                temp_high = temp_val
            elif not is_daytime and temp_low is None:
                temp_low = temp_val

        # Parse issued_at from updateTime (NWS provides real model issuance time)
        issued_at = datetime.now(timezone.utc)
        issued_at_str = data.get("properties", {}).get("updateTime")
        if issued_at_str:
            try:
                issued_at = datetime.fromisoformat(issued_at_str)
            except ValueError:
                logger.warning(
                    "issued_at_parse_failed",
                    city=city_code,
                    update_time=issued_at_str,
                    using="utcnow_fallback",
                )

        # Trim raw_response to only matched periods for storage efficiency
        trimmed_response: dict[str, Any] = {
            "properties": {
                "updateTime": data.get("properties", {}).get("updateTime"),
                "periods": matched_periods,
            }
        }

        return ParsedForecast(
            temp_high=temp_high,
            temp_low=temp_low,
            issued_at=issued_at,
            raw_response=trimmed_response,
        )
