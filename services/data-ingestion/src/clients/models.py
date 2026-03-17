"""
Data types returned by weather API clients.

ForecastResult is the common return type for all WeatherClient subclasses.
It is decoupled from the ORM layer — the ingestion layer converts these
into WeatherForecast DB rows.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, NamedTuple

from shared.config.errors import ValidationError
from shared.db.enums import WeatherSource

# Temperature sanity bounds (Fahrenheit). Any value outside this range
# indicates a parsing error or corrupted API response.
MIN_TEMP_F = -50.0
MAX_TEMP_F = 150.0


@dataclass(frozen=True)
class ForecastResult:
    """Parsed forecast from a weather API.

    Immutable. Validated on construction — temp range checks and
    at-least-one-temp requirement enforced in __post_init__.
    """

    source: WeatherSource
    city_code: str
    forecast_date: datetime
    issued_at: datetime
    temp_high: float | None
    temp_low: float | None
    raw_response: dict[str, Any]

    def __post_init__(self) -> None:
        if self.temp_high is None and self.temp_low is None:
            raise ValidationError(
                f"Forecast for {self.city_code} from {self.source} has neither temp_high nor temp_low",
                city=self.city_code,
                source=self.source,
            )

        if self.temp_high is not None and not (MIN_TEMP_F <= self.temp_high <= MAX_TEMP_F):
            raise ValidationError(
                f"temp_high {self.temp_high}°F out of range [{MIN_TEMP_F}, {MAX_TEMP_F}]",
                city=self.city_code,
                source=self.source,
                temp_high=self.temp_high,
            )

        if self.temp_low is not None and not (MIN_TEMP_F <= self.temp_low <= MAX_TEMP_F):
            raise ValidationError(
                f"temp_low {self.temp_low}°F out of range [{MIN_TEMP_F}, {MAX_TEMP_F}]",
                city=self.city_code,
                source=self.source,
                temp_low=self.temp_low,
            )

        if (
            self.temp_high is not None
            and self.temp_low is not None
            and self.temp_high < self.temp_low
        ):
            raise ValidationError(
                f"temp_high ({self.temp_high}°F) < temp_low ({self.temp_low}°F)",
                city=self.city_code,
                source=self.source,
                temp_high=self.temp_high,
                temp_low=self.temp_low,
            )


class ParsedForecast(NamedTuple):
    """Intermediate result from _parse_response.

    The base class constructs ForecastResult from this.
    Set raw_response to override what gets stored (e.g., trimmed OWM response).
    If None, the full API response dict is stored.
    """

    temp_high: float | None
    temp_low: float | None
    issued_at: datetime
    raw_response: dict[str, Any] | None = None
