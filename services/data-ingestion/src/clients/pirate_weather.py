"""
PirateWeather API client (Dark Sky-compatible).

Daily forecasts with explicit temperatureHigh/temperatureLow.
$2/mo plan. API key embedded in URL path.
"""

from datetime import datetime, timezone
from typing import Any

from shared.config.errors import WeatherApiError
from shared.db.enums import WeatherSource

from .base import WeatherClient
from .models import ParsedForecast

_BASE_URL = "https://api.pirateweather.net/forecast"


class PirateWeatherClient(WeatherClient):
    """PirateWeather forecast client.

    Args:
        api_key: PirateWeather API key.
        **kwargs: Passed to WeatherClient base class.
    """

    def __init__(self, *, api_key: str, **kwargs: Any) -> None:
        super().__init__(source=WeatherSource.PIRATE_WEATHER, **kwargs)
        self._api_key = api_key

    def _get_headers(self) -> dict[str, str]:
        return {}

    def _build_url(self, city_code: str, lat: float, lon: float, forecast_date: datetime) -> str:
        return f"{_BASE_URL}/{self._api_key}/{lat},{lon}"

    def _get_params(self, city_code: str, lat: float, lon: float, forecast_date: datetime) -> dict[str, str]:
        return {
            "units": "us",
            "exclude": "minutely,hourly,alerts",
        }

    def _parse_response(self, data: dict[str, Any], city_code: str, forecast_date: datetime) -> ParsedForecast:
        """Parse PirateWeather forecast response.

        Response has `daily.data` array with `temperatureHigh` and `temperatureLow`.
        Match target date by comparing Unix timestamp.
        """
        try:
            daily_data = data["daily"]["data"]
        except (KeyError, TypeError) as exc:
            raise WeatherApiError(
                f"Missing 'daily.data' in PirateWeather response for {city_code}",
                city=city_code,
                source=self.source,
            ) from exc

        if not daily_data:
            raise WeatherApiError(
                f"Empty 'daily.data' in PirateWeather response for {city_code}",
                city=city_code,
                source=self.source,
            )

        target_date = self._extract_date(forecast_date)

        # Find the day matching the target date
        matched_day = None
        for day in daily_data:
            day_ts = day.get("time")
            if day_ts is not None:
                day_date = datetime.fromtimestamp(day_ts, tz=timezone.utc).date()
                if day_date == target_date:
                    matched_day = day
                    break

        if matched_day is None:
            raise WeatherApiError(
                f"Target date {target_date} not found in PirateWeather daily data for {city_code}",
                city=city_code,
                source=self.source,
            )

        # Parse issued_at from currently.time (server-side timestamp)
        issued_at = datetime.now(timezone.utc)
        currently_time = data.get("currently", {}).get("time")
        if currently_time is not None:
            try:
                issued_at = datetime.fromtimestamp(currently_time, tz=timezone.utc)
            except (ValueError, OSError):
                pass

        # Trim raw_response to only the matched day for storage efficiency
        trimmed_response: dict[str, Any] = dict(data)
        trimmed_response["daily"] = {"data": [matched_day]}

        return ParsedForecast(
            temp_high=self._to_optional_float(matched_day.get("temperatureHigh")),
            temp_low=self._to_optional_float(matched_day.get("temperatureLow")),
            issued_at=issued_at,
            raw_response=trimmed_response,
        )
