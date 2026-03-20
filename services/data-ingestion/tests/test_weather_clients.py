"""Tests for the 4 weather API client implementations.

Each client is tested for:
- Happy path parsing from JSON fixtures
- Missing/empty data handling
- Edge cases specific to each API format

Uses respx to mock HTTP responses with realistic JSON fixtures.
"""

import json
from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx
from src.clients.models import ForecastResult
from src.clients.nws import NwsClient
from src.clients.openweathermap import OpenWeatherMapClient
from src.clients.pirate_weather import PirateWeatherClient
from src.clients.visual_crossing import VisualCrossingClient

from shared.config.errors import ValidationError, WeatherApiError
from shared.db.enums import WeatherSource

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES_DIR / name).read_text())


def _noop_sleep(seconds: float) -> None:
    """No-op sleep for fast tests."""
    pass


# ===========================================================================
# NWS Client Tests
# ===========================================================================

_NWS_GRIDPOINT_URL = "https://api.weather.gov/points/40.7,-74.0"
_NWS_FORECAST_URL = "https://api.weather.gov/gridpoints/OKX/33,37/forecast"


class TestNwsClient:
    """NWS client: two-step gridpoint resolution + 12-hour period parsing."""

    @pytest.fixture()
    def client(self) -> Generator[NwsClient, None, None]:
        c = NwsClient(user_agent="(test, test@test.com)", sleep_fn=_noop_sleep)
        yield c
        c.close()

    @pytest.fixture()
    def forecast_date(self) -> datetime:
        return datetime(2026, 3, 16, tzinfo=UTC)

    @respx.mock
    def test_happy_path_full_assertions(self, client: NwsClient, forecast_date: datetime) -> None:
        """Happy path: assert ALL ForecastResult fields + frozenness."""
        gridpoint_data = _load_fixture("nws_gridpoint.json")
        forecast_data = _load_fixture("nws_forecast.json")

        respx.get(_NWS_GRIDPOINT_URL).mock(
            return_value=httpx.Response(200, json=gridpoint_data)
        )
        respx.get(_NWS_FORECAST_URL).mock(
            return_value=httpx.Response(200, json=forecast_data)
        )

        result = client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert isinstance(result, ForecastResult)
        assert result.source == WeatherSource.NWS
        assert result.city_code == "NYC"
        assert result.forecast_date == forecast_date
        assert result.temp_high == 72.0
        assert result.temp_low == 55.0
        # raw_response is trimmed to only matched periods for target date
        assert result.raw_response is not None
        assert result.raw_response["properties"]["updateTime"] == "2026-03-16T14:30:00+00:00"
        trimmed_periods = result.raw_response["properties"]["periods"]
        assert len(trimmed_periods) == 2
        assert trimmed_periods[0]["isDaytime"] is True
        assert trimmed_periods[0]["temperature"] == 72
        assert trimmed_periods[1]["isDaytime"] is False
        assert trimmed_periods[1]["temperature"] == 55
        # issued_at from updateTime
        assert result.issued_at.year == 2026
        assert result.issued_at.month == 3
        assert result.issued_at.day == 16
        # Frozen
        with pytest.raises(AttributeError):
            result.temp_high = 99.0  # type: ignore[misc]

    @respx.mock
    def test_gridpoint_cached(self, client: NwsClient, forecast_date: datetime) -> None:
        """Second call for same city skips gridpoint resolution."""
        gridpoint_data = _load_fixture("nws_gridpoint.json")
        forecast_data = _load_fixture("nws_forecast.json")

        gridpoint_route = respx.get(_NWS_GRIDPOINT_URL).mock(
            return_value=httpx.Response(200, json=gridpoint_data)
        )
        respx.get(_NWS_FORECAST_URL).mock(
            return_value=httpx.Response(200, json=forecast_data)
        )

        client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)

        assert gridpoint_route.call_count == 1  # Only resolved once

    @respx.mock
    def test_gridpoint_persisted_to_file(self, tmp_path: Path, forecast_date: datetime) -> None:
        """Gridpoint cache is saved to file and loaded on restart."""
        cache_path = tmp_path / "gridpoints.json"

        gridpoint_data = _load_fixture("nws_gridpoint.json")
        forecast_data = _load_fixture("nws_forecast.json")

        gridpoint_route = respx.get(_NWS_GRIDPOINT_URL).mock(
            return_value=httpx.Response(200, json=gridpoint_data)
        )
        respx.get(_NWS_FORECAST_URL).mock(
            return_value=httpx.Response(200, json=forecast_data)
        )

        # First client: resolves and persists
        c1 = NwsClient(
            user_agent="(test, test@test.com)",
            gridpoint_cache_path=cache_path,
            sleep_fn=_noop_sleep,
        )
        c1.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        c1.close()
        assert cache_path.exists()
        assert gridpoint_route.call_count == 1

        # Second client: loads from file, skips API resolution
        c2 = NwsClient(
            user_agent="(test, test@test.com)",
            gridpoint_cache_path=cache_path,
            sleep_fn=_noop_sleep,
        )
        c2.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        c2.close()
        # Gridpoint route NOT called again
        assert gridpoint_route.call_count == 1

    def test_corrupt_cache_file_is_handled(self, tmp_path: Path) -> None:
        """Corrupt gridpoint cache file doesn't crash — logged and ignored."""
        cache_path = tmp_path / "gridpoints.json"
        cache_path.write_text("NOT VALID JSON {{{")

        c = NwsClient(
            user_agent="(test, test@test.com)",
            gridpoint_cache_path=cache_path,
            sleep_fn=_noop_sleep,
        )
        # Client should start successfully with empty cache
        assert len(c._gridpoint_cache) == 0  # pyright: ignore[reportPrivateUsage]
        c.close()

    @respx.mock
    def test_missing_periods_raises(self, client: NwsClient, forecast_date: datetime) -> None:
        gridpoint_data = _load_fixture("nws_gridpoint.json")
        respx.get(_NWS_GRIDPOINT_URL).mock(
            return_value=httpx.Response(200, json=gridpoint_data)
        )
        respx.get(_NWS_FORECAST_URL).mock(
            return_value=httpx.Response(200, json={"properties": {}})
        )

        with pytest.raises(WeatherApiError, match="Missing 'properties.periods'"):
            client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)

    @respx.mock
    def test_gridpoint_resolution_failure(self, client: NwsClient, forecast_date: datetime) -> None:
        respx.get(_NWS_GRIDPOINT_URL).mock(
            return_value=httpx.Response(200, json={"properties": {}})
        )

        with pytest.raises(WeatherApiError, match="Failed to resolve NWS gridpoint"):
            client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)

    @respx.mock
    def test_no_matching_date_returns_none_temps(self, client: NwsClient) -> None:
        """If no periods match the target date, temps are None → ValidationError."""
        gridpoint_data = _load_fixture("nws_gridpoint.json")
        forecast_data = _load_fixture("nws_forecast.json")

        respx.get(_NWS_GRIDPOINT_URL).mock(
            return_value=httpx.Response(200, json=gridpoint_data)
        )
        respx.get(_NWS_FORECAST_URL).mock(
            return_value=httpx.Response(200, json=forecast_data)
        )

        far_future = datetime(2027, 1, 1, tzinfo=UTC)
        with pytest.raises(ValidationError, match="neither temp_high nor temp_low"):
            client.fetch_forecast("NYC", 40.7, -74.0, far_future)

    @respx.mock
    def test_user_agent_header_sent(self, client: NwsClient, forecast_date: datetime) -> None:
        gridpoint_data = _load_fixture("nws_gridpoint.json")
        forecast_data = _load_fixture("nws_forecast.json")

        gridpoint_route = respx.get(_NWS_GRIDPOINT_URL).mock(
            return_value=httpx.Response(200, json=gridpoint_data)
        )
        respx.get(_NWS_FORECAST_URL).mock(
            return_value=httpx.Response(200, json=forecast_data)
        )

        client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert gridpoint_route.calls[0].request.headers["User-Agent"] == "(test, test@test.com)"  # pyright: ignore[reportUnknownMemberType]


class TestNwsOvernightPeriods:
    """Parameterized tests for NWS overnight period matching across midnight."""

    @pytest.fixture()
    def client(self) -> Generator[NwsClient, None, None]:
        c = NwsClient(user_agent="(test, test@test.com)", sleep_fn=_noop_sleep)
        # Pre-populate gridpoint cache to skip resolution
        c._gridpoint_cache["NYC"] = _NWS_FORECAST_URL  # pyright: ignore[reportPrivateUsage]
        yield c
        c.close()

    @respx.mock
    @pytest.mark.parametrize(
        "periods,expected_high,expected_low",
        [
            pytest.param(
                # Daytime and overnight both on target date
                [  # pyright: ignore[reportUnknownArgumentType]
                    {"startTime": "2026-03-16T08:00:00-05:00", "endTime": "2026-03-16T18:00:00-05:00",
                     "isDaytime": True, "temperature": 72},
                    {"startTime": "2026-03-16T18:00:00-05:00", "endTime": "2026-03-17T06:00:00-05:00",
                     "isDaytime": False, "temperature": 55},
                ],
                72.0, 55.0,
                id="standard_day_and_night",
            ),
            pytest.param(
                # Only daytime period matches
                [  # pyright: ignore[reportUnknownArgumentType]
                    {"startTime": "2026-03-16T08:00:00-05:00", "endTime": "2026-03-16T18:00:00-05:00",
                     "isDaytime": True, "temperature": 72},
                ],
                72.0, None,
                id="daytime_only",
            ),
            pytest.param(
                # Overnight period starts BEFORE target date but ends ON target date
                [  # pyright: ignore[reportUnknownArgumentType]
                    {"startTime": "2026-03-15T18:00:00-05:00", "endTime": "2026-03-16T06:00:00-05:00",
                     "isDaytime": False, "temperature": 45},
                    {"startTime": "2026-03-16T08:00:00-05:00", "endTime": "2026-03-16T18:00:00-05:00",
                     "isDaytime": True, "temperature": 68},
                ],
                68.0, 45.0,
                id="overnight_crosses_midnight_into_target",
            ),
            pytest.param(
                # Daytime period has temperature=None — should be skipped
                [  # pyright: ignore[reportUnknownArgumentType]
                    {"startTime": "2026-03-16T08:00:00-05:00", "endTime": "2026-03-16T18:00:00-05:00",
                     "isDaytime": True, "temperature": None},
                    {"startTime": "2026-03-16T18:00:00-05:00", "endTime": "2026-03-17T06:00:00-05:00",
                     "isDaytime": False, "temperature": 55},
                ],
                None, 55.0,
                id="daytime_temp_null_skipped",
            ),
            pytest.param(
                # Multiple daytime periods — first one wins
                [  # pyright: ignore[reportUnknownArgumentType]
                    {"startTime": "2026-03-16T06:00:00-05:00", "endTime": "2026-03-16T12:00:00-05:00",
                     "isDaytime": True, "temperature": 68},
                    {"startTime": "2026-03-16T12:00:00-05:00", "endTime": "2026-03-16T18:00:00-05:00",
                     "isDaytime": True, "temperature": 75},
                    {"startTime": "2026-03-16T18:00:00-05:00", "endTime": "2026-03-17T06:00:00-05:00",
                     "isDaytime": False, "temperature": 52},
                ],
                68.0, 52.0,
                id="multiple_daytime_first_wins",
            ),
        ],
    )
    def test_period_matching(self, client: NwsClient, periods: list[dict[str, Any]], expected_high: float | None, expected_low: float | None) -> None:
        forecast_date = datetime(2026, 3, 16, tzinfo=UTC)
        data: dict[str, Any] = {"properties": {"updateTime": "2026-03-16T14:30:00+00:00", "periods": periods}}

        respx.get(_NWS_FORECAST_URL).mock(
            return_value=httpx.Response(200, json=data)
        )

        result = client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert result.temp_high == expected_high
        assert result.temp_low == expected_low


# ===========================================================================
# Visual Crossing Client Tests
# ===========================================================================

_VC_BASE = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"


class TestVisualCrossingClient:
    """Visual Crossing: simple days[0].tempmax/tempmin."""

    @pytest.fixture()
    def client(self) -> Generator[VisualCrossingClient, None, None]:
        c = VisualCrossingClient(api_key="test-vc-key", sleep_fn=_noop_sleep)
        yield c
        c.close()

    @pytest.fixture()
    def forecast_date(self) -> datetime:
        return datetime(2026, 3, 16, tzinfo=UTC)

    @pytest.fixture()
    def expected_url(self, forecast_date: datetime) -> str:
        date_str = forecast_date.strftime("%Y-%m-%d")
        return f"{_VC_BASE}/40.7,-74.0/{date_str}"

    @pytest.fixture()
    def expected_params(self) -> dict[str, str]:
        return {"key": "test-vc-key", "unitGroup": "us", "include": "days", "elements": "datetime,tempmax,tempmin"}

    @respx.mock
    def test_happy_path_full_assertions(self, client: VisualCrossingClient, forecast_date: datetime, expected_url: str, expected_params: dict[str, str]) -> None:
        fixture = _load_fixture("visual_crossing_forecast.json")
        respx.get(expected_url, params=expected_params).mock(
            return_value=httpx.Response(200, json=fixture)
        )

        result = client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert result.source == WeatherSource.VISUAL_CROSSING
        assert result.city_code == "NYC"
        assert result.forecast_date == forecast_date
        assert result.temp_high == 73.2
        assert result.temp_low == 54.8
        assert result.raw_response == fixture
        # VC has no model issuance time — issued_at should be ~now
        assert result.issued_at >= datetime.now(UTC) - timedelta(seconds=60)

    @respx.mock
    def test_missing_days_raises(self, client: VisualCrossingClient, forecast_date: datetime, expected_url: str, expected_params: dict[str, str]) -> None:
        respx.get(expected_url, params=expected_params).mock(
            return_value=httpx.Response(200, json={})
        )

        with pytest.raises(WeatherApiError, match="Missing 'days'"):
            client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)

    @respx.mock
    def test_empty_days_raises(self, client: VisualCrossingClient, forecast_date: datetime, expected_url: str, expected_params: dict[str, str]) -> None:
        respx.get(expected_url, params=expected_params).mock(
            return_value=httpx.Response(200, json={"days": []})
        )

        with pytest.raises(WeatherApiError, match="Empty 'days'"):
            client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)

    @respx.mock
    def test_api_key_passed_as_param(self, client: VisualCrossingClient, forecast_date: datetime, expected_url: str, expected_params: dict[str, str]) -> None:
        """API key is passed via query params, not embedded in the URL path."""
        fixture = _load_fixture("visual_crossing_forecast.json")
        route = respx.get(expected_url, params=expected_params).mock(
            return_value=httpx.Response(200, json=fixture)
        )

        client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        request_url = str(route.calls[0].request.url)  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
        assert "key=test-vc-key" in request_url

    @respx.mock
    def test_high_only(self, client: VisualCrossingClient, forecast_date: datetime, expected_url: str, expected_params: dict[str, str]) -> None:
        """tempmin missing → result has temp_low=None."""
        respx.get(expected_url, params=expected_params).mock(
            return_value=httpx.Response(200, json={
                "days": [{"datetime": "2026-03-16", "tempmax": 73.2}]
            })
        )

        result = client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert result.temp_high == 73.2
        assert result.temp_low is None


# ===========================================================================
# PirateWeather Client Tests
# ===========================================================================

_PW_URL = "https://api.pirateweather.net/forecast/key/40.7,-74.0"
_PW_PARAMS: dict[str, str] = {"units": "us", "exclude": "minutely,hourly,alerts"}


class TestPirateWeatherClient:
    """PirateWeather: daily.data matched by Unix timestamp."""

    @pytest.fixture()
    def client(self) -> Generator[PirateWeatherClient, None, None]:
        c = PirateWeatherClient(api_key="test-pw-key", sleep_fn=_noop_sleep)
        yield c
        c.close()

    @pytest.fixture()
    def forecast_date(self) -> datetime:
        # 1742108400 = 2025-03-16T05:00:00Z
        return datetime(2025, 3, 16, tzinfo=UTC)

    @respx.mock
    def test_happy_path_full_assertions(self, client: PirateWeatherClient, forecast_date: datetime) -> None:
        fixture = _load_fixture("pirate_weather_forecast.json")
        respx.get(_PW_URL, params=_PW_PARAMS).mock(
            return_value=httpx.Response(200, json=fixture)
        )

        result = client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert result.source == WeatherSource.PIRATE_WEATHER
        assert result.city_code == "NYC"
        assert result.forecast_date == forecast_date
        assert result.temp_high == 71.5
        assert result.temp_low == 53.2
        # raw_response is trimmed to only the matched day
        assert result.raw_response is not None
        assert len(result.raw_response["daily"]["data"]) == 1
        assert result.raw_response["daily"]["data"][0]["temperatureHigh"] == 71.5

    @respx.mock
    def test_issued_at_from_currently_time(self, client: PirateWeatherClient, forecast_date: datetime) -> None:
        """issued_at parsed from currently.time server timestamp."""
        fixture = _load_fixture("pirate_weather_forecast.json")
        respx.get(_PW_URL, params=_PW_PARAMS).mock(
            return_value=httpx.Response(200, json=fixture)
        )

        result = client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        # currently.time = 1742140800 = 2025-03-16T14:00:00Z
        assert result.issued_at.year == 2025
        assert result.issued_at.month == 3
        assert result.issued_at.day == 16

    @respx.mock
    def test_issued_at_fallback_when_no_currently(self, client: PirateWeatherClient, forecast_date: datetime) -> None:
        """When currently.time is missing, falls back to now()."""
        fixture = _load_fixture("pirate_weather_forecast.json")
        fixture.pop("currently", None)
        respx.get(_PW_URL, params=_PW_PARAMS).mock(
            return_value=httpx.Response(200, json=fixture)
        )

        result = client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        # Fallback to ~now when currently block is missing
        assert result.issued_at >= datetime.now(UTC) - timedelta(seconds=60)

    @respx.mock
    def test_missing_daily_data_raises(self, client: PirateWeatherClient, forecast_date: datetime) -> None:
        respx.get(_PW_URL, params=_PW_PARAMS).mock(
            return_value=httpx.Response(200, json={})
        )

        with pytest.raises(WeatherApiError, match="Missing 'daily.data'"):
            client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)

    @respx.mock
    def test_empty_daily_data_raises(self, client: PirateWeatherClient, forecast_date: datetime) -> None:
        respx.get(_PW_URL, params=_PW_PARAMS).mock(
            return_value=httpx.Response(200, json={"daily": {"data": []}})
        )

        with pytest.raises(WeatherApiError, match="Empty 'daily.data'"):
            client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)

    @respx.mock
    def test_raises_when_no_matching_date(self, client: PirateWeatherClient) -> None:
        """When no day matches target date, raises WeatherApiError."""
        fixture = _load_fixture("pirate_weather_forecast.json")
        respx.get(_PW_URL, params=_PW_PARAMS).mock(
            return_value=httpx.Response(200, json=fixture)
        )

        far_future = datetime(2027, 1, 1, tzinfo=UTC)
        with pytest.raises(WeatherApiError, match="default window covers"):
            client.fetch_forecast("NYC", 40.7, -74.0, far_future)

    @respx.mock
    def test_api_key_passed_as_header(self, client: PirateWeatherClient, forecast_date: datetime) -> None:
        """API key is passed via header, not in the URL path or query string."""
        fixture = _load_fixture("pirate_weather_forecast.json")
        route = respx.get(_PW_URL, params=_PW_PARAMS).mock(
            return_value=httpx.Response(200, json=fixture)
        )

        client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        request = route.calls[0].request  # pyright: ignore[reportUnknownMemberType]
        request_url = str(request.url)  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
        # Key must be in headers, NOT in URL path or query string
        assert request.headers.get("apikey") == "test-pw-key"  # pyright: ignore[reportUnknownMemberType]
        assert "/test-pw-key/" not in request_url
        assert "apikey=" not in request_url


# ===========================================================================
# OpenWeatherMap Client Tests
# ===========================================================================

_OWM_URL = "https://api.openweathermap.org/data/2.5/forecast"
_OWM_PARAMS: dict[str, str] = {"lat": "40.7", "lon": "-74.0", "appid": "test-owm-key", "units": "imperial"}


class TestOpenWeatherMapClient:
    """OWM: aggregates 3-hour intervals into daily high/low."""

    @pytest.fixture()
    def client(self) -> Generator[OpenWeatherMapClient, None, None]:
        c = OpenWeatherMapClient(api_key="test-owm-key", sleep_fn=_noop_sleep)
        yield c
        c.close()

    @pytest.fixture()
    def forecast_date(self) -> datetime:
        return datetime(2026, 3, 16, tzinfo=UTC)

    @respx.mock
    def test_happy_path_full_assertions(self, client: OpenWeatherMapClient, forecast_date: datetime) -> None:
        fixture = _load_fixture("openweathermap_forecast.json")
        respx.get(_OWM_URL, params=_OWM_PARAMS).mock(
            return_value=httpx.Response(200, json=fixture)
        )

        result = client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert result.source == WeatherSource.OPENWEATHER
        assert result.city_code == "NYC"
        assert result.forecast_date == forecast_date
        # From fixture: 4 intervals on 2026-03-16
        # temp_max values: 67.5, 72.8, 69.1, 61.5 → max = 72.8
        # temp_min values: 63.0, 68.5, 66.2, 57.8 → min = 57.8
        assert result.temp_high == 72.8
        assert result.temp_low == 57.8
        # OWM has no model issuance time — issued_at should be ~now
        assert result.issued_at >= datetime.now(UTC) - timedelta(seconds=60)

    @respx.mock
    def test_raw_response_trimmed_to_target_date(self, client: OpenWeatherMapClient, forecast_date: datetime) -> None:
        """raw_response only contains intervals for the target date."""
        fixture = _load_fixture("openweathermap_forecast.json")
        respx.get(_OWM_URL, params=_OWM_PARAMS).mock(
            return_value=httpx.Response(200, json=fixture)
        )

        result = client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        # Fixture has 4 intervals for 2026-03-16 and 2 for 2026-03-17
        # raw_response should only have the 4 intervals for 2026-03-16
        stored_intervals = result.raw_response["list"]
        assert len(stored_intervals) == 4
        for interval in stored_intervals:
            assert interval["dt_txt"].startswith("2026-03-16")

    @respx.mock
    def test_missing_list_raises(self, client: OpenWeatherMapClient, forecast_date: datetime) -> None:
        respx.get(_OWM_URL, params=_OWM_PARAMS).mock(
            return_value=httpx.Response(200, json={})
        )

        with pytest.raises(WeatherApiError, match="Missing 'list'"):
            client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)

    @respx.mock
    def test_empty_list_raises(self, client: OpenWeatherMapClient, forecast_date: datetime) -> None:
        respx.get(_OWM_URL, params=_OWM_PARAMS).mock(
            return_value=httpx.Response(200, json={"list": []})
        )

        with pytest.raises(WeatherApiError, match="Empty 'list'"):
            client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)

    @respx.mock
    def test_no_matching_date_returns_none_temps(self, client: OpenWeatherMapClient) -> None:
        """If no intervals match the target date, temps are None → ValidationError."""
        fixture = _load_fixture("openweathermap_forecast.json")
        respx.get(_OWM_URL, params=_OWM_PARAMS).mock(
            return_value=httpx.Response(200, json=fixture)
        )

        far_future = datetime(2027, 1, 1, tzinfo=UTC)
        with pytest.raises(ValidationError, match="neither temp_high nor temp_low"):
            client.fetch_forecast("NYC", 40.7, -74.0, far_future)

    @respx.mock
    def test_aggregation_across_intervals(self, client: OpenWeatherMapClient, forecast_date: datetime) -> None:
        """Verify max(temp_max) and min(temp_min) across all intervals for date."""
        data: dict[str, Any] = {
            "list": [
                {"dt_txt": "2026-03-16 06:00:00", "main": {"temp_max": 60.0, "temp_min": 50.0}},
                {"dt_txt": "2026-03-16 09:00:00", "main": {"temp_max": 65.0, "temp_min": 55.0}},
                {"dt_txt": "2026-03-16 12:00:00", "main": {"temp_max": 75.0, "temp_min": 52.0}},
                {"dt_txt": "2026-03-16 15:00:00", "main": {"temp_max": 70.0, "temp_min": 58.0}},
            ]
        }
        respx.get(_OWM_URL, params=_OWM_PARAMS).mock(
            return_value=httpx.Response(200, json=data)
        )

        result = client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert result.temp_high == 75.0  # max of [60, 65, 75, 70]
        assert result.temp_low == 50.0   # min of [50, 55, 52, 58]

    @respx.mock
    def test_partial_day_coverage(self, client: OpenWeatherMapClient, forecast_date: datetime) -> None:
        """Target date at end of range with only 1-2 intervals."""
        data: dict[str, Any] = {
            "list": [
                {"dt_txt": "2026-03-16 21:00:00", "main": {"temp_max": 62.0, "temp_min": 58.0}},
            ]
        }
        respx.get(_OWM_URL, params=_OWM_PARAMS).mock(
            return_value=httpx.Response(200, json=data)
        )

        result = client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert result.temp_high == 62.0
        assert result.temp_low == 58.0

    @respx.mock
    def test_missing_temp_fields_in_some_intervals(self, client: OpenWeatherMapClient, forecast_date: datetime) -> None:
        """Intervals with missing temp_max or temp_min are skipped for that field."""
        data: dict[str, Any] = {
            "list": [
                {"dt_txt": "2026-03-16 06:00:00", "main": {"temp_max": 65.0}},
                {"dt_txt": "2026-03-16 09:00:00", "main": {"temp_min": 50.0}},
                {"dt_txt": "2026-03-16 12:00:00", "main": {"temp_max": 72.0, "temp_min": 55.0}},
            ]
        }
        respx.get(_OWM_URL, params=_OWM_PARAMS).mock(
            return_value=httpx.Response(200, json=data)
        )

        result = client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert result.temp_high == 72.0  # max of [65, 72]
        assert result.temp_low == 50.0   # min of [50, 55]

    @respx.mock
    def test_skips_invalid_dt_txt(self, client: OpenWeatherMapClient, forecast_date: datetime) -> None:
        """Intervals with unparseable dt_txt are silently skipped."""
        data: dict[str, Any] = {
            "list": [
                {"dt_txt": "not-a-date", "main": {"temp_max": 99.0, "temp_min": 10.0}},
                {"dt_txt": "2026-03-16 12:00:00", "main": {"temp_max": 72.0, "temp_min": 55.0}},
            ]
        }
        respx.get(_OWM_URL, params=_OWM_PARAMS).mock(
            return_value=httpx.Response(200, json=data)
        )

        result = client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        assert result.temp_high == 72.0
        assert result.temp_low == 55.0

    @respx.mock
    def test_api_key_passed_as_param(self, client: OpenWeatherMapClient, forecast_date: datetime) -> None:
        """API key is passed via query params, not embedded in the URL path."""
        fixture = _load_fixture("openweathermap_forecast.json")
        route = respx.get(_OWM_URL, params=_OWM_PARAMS).mock(
            return_value=httpx.Response(200, json=fixture)
        )

        client.fetch_forecast("NYC", 40.7, -74.0, forecast_date)
        request_url = str(route.calls[0].request.url)  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
        assert "appid=test-owm-key" in request_url

    @respx.mock
    def test_local_timezone_captures_cross_utc_intervals(self, client: OpenWeatherMapClient) -> None:
        """UTC intervals that fall on the local target date are included.

        For LA (UTC-7 in March / PDT), 2026-03-16 local afternoon corresponds
        to 2026-03-17 00:00 UTC. Without timezone-aware filtering, that
        interval would be excluded, missing the afternoon high.
        """
        forecast_date = datetime(2026, 3, 16, tzinfo=UTC)
        data: dict[str, Any] = {
            "list": [
                # 2026-03-16 18:00 UTC = 2026-03-16 11:00 PDT (local Mar 16)
                {"dt_txt": "2026-03-16 18:00:00", "main": {"temp_max": 70.0, "temp_min": 60.0}},
                # 2026-03-16 21:00 UTC = 2026-03-16 14:00 PDT (local Mar 16)
                {"dt_txt": "2026-03-16 21:00:00", "main": {"temp_max": 78.0, "temp_min": 65.0}},
                # 2026-03-17 00:00 UTC = 2026-03-16 17:00 PDT (still local Mar 16!)
                {"dt_txt": "2026-03-17 00:00:00", "main": {"temp_max": 75.0, "temp_min": 62.0}},
                # 2026-03-17 03:00 UTC = 2026-03-16 20:00 PDT (still local Mar 16!)
                {"dt_txt": "2026-03-17 03:00:00", "main": {"temp_max": 68.0, "temp_min": 58.0}},
                # 2026-03-17 07:00 UTC = 2026-03-17 00:00 PDT (local Mar 17 — excluded)
                {"dt_txt": "2026-03-17 07:00:00", "main": {"temp_max": 55.0, "temp_min": 50.0}},
            ]
        }
        lax_params = {"lat": "34.05", "lon": "-118.24", "appid": "test-owm-key", "units": "imperial"}
        respx.get(_OWM_URL, params=lax_params).mock(
            return_value=httpx.Response(200, json=data)
        )

        result = client.fetch_forecast(
            "LAX", 34.05, -118.24, forecast_date,
            city_timezone="America/Los_Angeles",
        )
        # All 4 local Mar-16 intervals captured, including the cross-UTC ones
        assert result.temp_high == 78.0  # max of [70, 78, 75, 68]
        assert result.temp_low == 58.0   # min of [60, 65, 62, 58]
        # raw_response should have 4 intervals, not 2
        assert len(result.raw_response["list"]) == 4
