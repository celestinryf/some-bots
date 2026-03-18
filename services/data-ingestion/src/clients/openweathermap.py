"""
OpenWeatherMap 5-day/3-hour forecast API client.

Free tier: 1M calls/month. API key as `appid` query parameter (no header auth
on the free 5-day endpoint). No daily summary — must aggregate 3-hour
intervals into daily high/low.
"""

from datetime import datetime, timezone
from typing import Any

from shared.config.errors import WeatherApiError
from shared.db.enums import WeatherSource

from .base import WeatherClient
from .models import ParsedForecast

_BASE_URL = "https://api.openweathermap.org/data/2.5/forecast"


class OpenWeatherMapClient(WeatherClient):
    """OpenWeatherMap 5-day forecast client.

    Args:
        api_key: OpenWeatherMap API key.
        **kwargs: Passed to WeatherClient base class.
    """

    def __init__(self, *, api_key: str, **kwargs: Any) -> None:
        super().__init__(source=WeatherSource.OPENWEATHER, **kwargs)
        self._api_key = api_key

    def _get_headers(self) -> dict[str, str]:
        return {}

    def _build_url(self, city_code: str, lat: float, lon: float, forecast_date: datetime) -> str:
        return _BASE_URL

    def _get_params(self, city_code: str, lat: float, lon: float, forecast_date: datetime) -> dict[str, str]:
        # OWM free 5-day endpoint only supports API key via query param.
        return {
            "lat": str(lat),
            "lon": str(lon),
            "appid": self._api_key,
            "units": "imperial",
        }

    def _parse_response(self, data: dict[str, Any], city_code: str, forecast_date: datetime) -> ParsedForecast:
        """Parse OWM 5-day/3-hour response.

        Filters 3-hour intervals to only the target date, then computes
        daily max(temp_max) and min(temp_min).
        """
        try:
            intervals = data["list"]
        except (KeyError, TypeError) as exc:
            raise WeatherApiError(
                f"Missing 'list' in OpenWeatherMap response for {city_code}",
                city=city_code,
                source=self.source,
            ) from exc

        if not intervals:
            raise WeatherApiError(
                f"Empty 'list' in OpenWeatherMap response for {city_code}",
                city=city_code,
                source=self.source,
            )

        target_date = self._extract_date(forecast_date)
        target_key = str(target_date)

        # Filter to target date only and collect temps
        highs: list[float] = []
        lows: list[float] = []
        matched_intervals: list[dict[str, Any]] = []

        for interval in intervals:
            dt_txt = interval.get("dt_txt", "")
            try:
                interval_date = datetime.strptime(dt_txt, "%Y-%m-%d %H:%M:%S").date()
            except ValueError:
                continue

            if str(interval_date) != target_key:
                continue

            matched_intervals.append(interval)
            main = interval.get("main", {})
            temp_max = main.get("temp_max")
            temp_min = main.get("temp_min")

            if temp_max is not None:
                highs.append(float(temp_max))
            if temp_min is not None:
                lows.append(float(temp_min))

        temp_high = max(highs) if highs else None
        temp_low = min(lows) if lows else None

        # Trim raw_response to only matched intervals for storage efficiency
        trimmed_response = dict(data)
        trimmed_response["list"] = matched_intervals

        return ParsedForecast(
            temp_high=temp_high,
            temp_low=temp_low,
            issued_at=datetime.now(timezone.utc),
            raw_response=trimmed_response,
        )
