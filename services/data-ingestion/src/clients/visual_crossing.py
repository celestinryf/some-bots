"""
Visual Crossing Weather API client.

Timeline endpoint provides daily forecasts with explicit tempmax/tempmin.
$35/mo plan. API key passed as query parameter.
"""

from datetime import datetime, timezone
from typing import Any

from shared.config.errors import WeatherApiError
from shared.db.enums import WeatherSource

from .base import WeatherClient
from .models import ParsedForecast

_BASE_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"


class VisualCrossingClient(WeatherClient):
    """Visual Crossing weather forecast client.

    Args:
        api_key: Visual Crossing API key.
        **kwargs: Passed to WeatherClient base class.
    """

    def __init__(self, *, api_key: str, **kwargs: Any) -> None:
        super().__init__(source=WeatherSource.VISUAL_CROSSING, **kwargs)
        self._api_key = api_key

    def _get_headers(self) -> dict[str, str]:
        return {}

    def _build_url(self, city_code: str, lat: float, lon: float, forecast_date: datetime) -> str:
        date_str = forecast_date.strftime("%Y-%m-%d")
        return f"{_BASE_URL}/{lat},{lon}/{date_str}"

    def _get_params(self, city_code: str, lat: float, lon: float, forecast_date: datetime) -> dict[str, str]:
        return {
            "key": self._api_key,
            "unitGroup": "us",
            "include": "days",
            "elements": "datetime,tempmax,tempmin",
        }

    def _parse_response(self, data: dict[str, Any], city_code: str, forecast_date: datetime) -> ParsedForecast:
        """Parse Visual Crossing timeline response.

        Response has a `days` array with explicit `tempmax` and `tempmin`.
        No model issuance time available — issued_at uses fetch time.
        """
        try:
            days = data["days"]
        except (KeyError, TypeError) as exc:
            raise WeatherApiError(
                f"Missing 'days' in Visual Crossing response for {city_code}",
                city=city_code,
                source=self.source,
            ) from exc

        if not days:
            raise WeatherApiError(
                f"Empty 'days' array in Visual Crossing response for {city_code}",
                city=city_code,
                source=self.source,
            )

        day = days[0]

        return ParsedForecast(
            temp_high=self._to_optional_float(day.get("tempmax")),
            temp_low=self._to_optional_float(day.get("tempmin")),
            issued_at=datetime.now(timezone.utc),
        )
