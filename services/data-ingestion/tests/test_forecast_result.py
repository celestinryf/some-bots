"""Tests for ForecastResult frozen dataclass validation."""

from datetime import UTC, datetime
from typing import Any

import pytest
from src.clients.models import MAX_TEMP_F, MIN_TEMP_F, ForecastResult

from shared.config.errors import ValidationError
from shared.db.enums import WeatherSource


def _make_result(**overrides: Any) -> ForecastResult:
    """Helper to build a ForecastResult with sensible defaults."""
    defaults: dict[str, Any] = {
        "source": WeatherSource.NWS,
        "city_code": "NYC",
        "forecast_date": datetime(2026, 3, 16, tzinfo=UTC),
        "issued_at": datetime(2026, 3, 16, 14, 30, tzinfo=UTC),
        "temp_high": 72.0,
        "temp_low": 55.0,
        "raw_response": {"test": True},
    }
    defaults.update(overrides)
    return ForecastResult(**defaults)


class TestForecastResultValid:
    """Test valid ForecastResult construction."""

    def test_both_temps(self):
        result = _make_result(temp_high=72.0, temp_low=55.0)
        assert result.temp_high == 72.0
        assert result.temp_low == 55.0

    def test_high_only(self):
        result = _make_result(temp_high=72.0, temp_low=None)
        assert result.temp_high == 72.0
        assert result.temp_low is None

    def test_low_only(self):
        result = _make_result(temp_high=None, temp_low=55.0)
        assert result.temp_high is None
        assert result.temp_low == 55.0

    def test_frozen_immutable(self):
        result = _make_result()
        with pytest.raises(AttributeError):
            result.temp_high = 99.0  # type: ignore[misc]

    def test_all_sources(self):
        for source in WeatherSource:
            result = _make_result(source=source)
            assert result.source == source

    def test_boundary_temps_valid(self):
        result = _make_result(temp_high=MAX_TEMP_F, temp_low=MIN_TEMP_F)
        assert result.temp_high == MAX_TEMP_F
        assert result.temp_low == MIN_TEMP_F

    def test_zero_temp(self):
        result = _make_result(temp_high=0.0, temp_low=-10.0)
        assert result.temp_high == 0.0
        assert result.temp_low == -10.0

    def test_negative_temp(self):
        result = _make_result(temp_high=10.0, temp_low=-45.0)
        assert result.temp_low == -45.0


class TestForecastResultInvalid:
    """Test ForecastResult validation rejects bad data."""

    def test_both_temps_none_raises(self):
        with pytest.raises(ValidationError, match="neither temp_high nor temp_low"):
            _make_result(temp_high=None, temp_low=None)

    def test_high_above_max_raises(self):
        with pytest.raises(ValidationError, match="temp_high.*out of range"):
            _make_result(temp_high=MAX_TEMP_F + 1)

    def test_high_below_min_raises(self):
        with pytest.raises(ValidationError, match="temp_high.*out of range"):
            _make_result(temp_high=MIN_TEMP_F - 1)

    def test_low_above_max_raises(self):
        with pytest.raises(ValidationError, match="temp_low.*out of range"):
            _make_result(temp_low=MAX_TEMP_F + 1)

    def test_low_below_min_raises(self):
        with pytest.raises(ValidationError, match="temp_low.*out of range"):
            _make_result(temp_low=MIN_TEMP_F - 1)

    def test_validation_error_has_city(self):
        with pytest.raises(ValidationError) as exc_info:
            _make_result(city_code="MIAMI", temp_high=None, temp_low=None)
        assert exc_info.value.city == "MIAMI"

    def test_validation_error_has_source(self):
        with pytest.raises(ValidationError) as exc_info:
            _make_result(source=WeatherSource.PIRATE_WEATHER, temp_high=200.0)
        assert exc_info.value.source == WeatherSource.PIRATE_WEATHER

    def test_high_less_than_low_raises(self):
        with pytest.raises(ValidationError, match="temp_high.*< temp_low"):
            _make_result(temp_high=60.0, temp_low=70.0)

    def test_high_equals_low_is_valid(self):
        result = _make_result(temp_high=65.0, temp_low=65.0)
        assert result.temp_high == 65.0
        assert result.temp_low == 65.0
