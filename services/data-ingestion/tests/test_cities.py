"""Tests for shared.config.cities — city configuration and validation."""

import pytest

from shared.config.cities import CITIES, CityConfig, all_cities, get_city


class TestCityConfigValidation:
    """Verify __post_init__ catches invalid city data at construction time."""

    def test_valid_city_creates_successfully(self):
        city = CityConfig("Test City", "TST", "KTST", "America/New_York", 40.0, -74.0)
        assert city.name == "Test City"
        assert city.kalshi_ticker_code == "TST"

    def test_empty_name_raises(self):
        with pytest.raises(ValueError, match="City name cannot be empty"):
            CityConfig("", "TST", "KTST", "America/New_York", 40.0, -74.0)

    def test_whitespace_name_raises(self):
        with pytest.raises(ValueError, match="City name cannot be empty"):
            CityConfig("   ", "TST", "KTST", "America/New_York", 40.0, -74.0)

    def test_empty_ticker_code_raises(self):
        with pytest.raises(ValueError, match="Kalshi ticker code cannot be empty"):
            CityConfig("Test City", "", "KTST", "America/New_York", 40.0, -74.0)

    def test_empty_nws_station_raises(self):
        with pytest.raises(ValueError, match="NWS station ID cannot be empty"):
            CityConfig("Test City", "TST", "", "America/New_York", 40.0, -74.0)

    def test_latitude_too_high_raises(self):
        with pytest.raises(ValueError, match="Latitude must be -90..90"):
            CityConfig("Test City", "TST", "KTST", "America/New_York", 91.0, -74.0)

    def test_latitude_too_low_raises(self):
        with pytest.raises(ValueError, match="Latitude must be -90..90"):
            CityConfig("Test City", "TST", "KTST", "America/New_York", -91.0, -74.0)

    def test_longitude_too_high_raises(self):
        with pytest.raises(ValueError, match="Longitude must be -180..180"):
            CityConfig("Test City", "TST", "KTST", "America/New_York", 40.0, 181.0)

    def test_longitude_too_low_raises(self):
        with pytest.raises(ValueError, match="Longitude must be -180..180"):
            CityConfig("Test City", "TST", "KTST", "America/New_York", 40.0, -181.0)

    def test_invalid_timezone_raises(self):
        with pytest.raises(ValueError, match="Invalid timezone"):
            CityConfig("Test City", "TST", "KTST", "Not/A/Timezone", 40.0, -74.0)

    def test_boundary_latitude_90_valid(self):
        city = CityConfig("North Pole", "NP", "KNP", "UTC", 90.0, 0.0)
        assert city.lat == 90.0

    def test_boundary_latitude_minus90_valid(self):
        city = CityConfig("South Pole", "SP", "KSP", "UTC", -90.0, 0.0)
        assert city.lat == -90.0

    def test_boundary_longitude_180_valid(self):
        city = CityConfig("Dateline", "DL", "KDL", "UTC", 0.0, 180.0)
        assert city.lon == 180.0

    def test_boundary_longitude_minus180_valid(self):
        city = CityConfig("Dateline West", "DW", "KDW", "UTC", 0.0, -180.0)
        assert city.lon == -180.0


class TestCitiesDict:
    """Verify the hardcoded CITIES dictionary is internally consistent."""

    def test_all_cities_have_valid_data(self):
        """If any city had invalid data, __post_init__ would have raised at import time.
        This test proves the import succeeded with all 43 cities intact."""
        assert len(CITIES) >= 43

    def test_dict_keys_match_ticker_codes(self):
        """Every dict key should match its CityConfig.kalshi_ticker_code."""
        for key, city in CITIES.items():
            assert key == city.kalshi_ticker_code, (
                f"Dict key {key!r} doesn't match ticker code {city.kalshi_ticker_code!r} "
                f"for {city.name}"
            )

    def test_no_duplicate_nws_stations(self):
        """Each city should map to a unique NWS station."""
        stations = [c.nws_station_id for c in CITIES.values()]
        duplicates = [s for s in stations if stations.count(s) > 1]
        assert not duplicates, f"Duplicate NWS station IDs: {set(duplicates)}"

    def test_no_duplicate_names(self):
        """Each city should have a unique display name."""
        names = [c.name for c in CITIES.values()]
        duplicates = [n for n in names if names.count(n) > 1]
        assert not duplicates, f"Duplicate city names: {set(duplicates)}"

    def test_all_latitudes_in_continental_us_range(self):
        """All Kalshi weather cities are in the continental US (roughly 24-49 lat)."""
        for code, city in CITIES.items():
            assert 24.0 <= city.lat <= 49.5, (
                f"{city.name} ({code}) lat={city.lat} outside continental US range"
            )

    def test_all_longitudes_in_continental_us_range(self):
        """All Kalshi weather cities are in the continental US (roughly -125 to -66 lon)."""
        for code, city in CITIES.items():
            assert -125.0 <= city.lon <= -66.0, (
                f"{city.name} ({code}) lon={city.lon} outside continental US range"
            )


class TestGetCity:
    def test_valid_code_returns_city(self):
        city = get_city("NYC")
        assert city.name == "New York"
        assert city.nws_station_id == "KJFK"

    def test_invalid_code_raises_keyerror(self):
        with pytest.raises(KeyError, match="Unknown city code"):
            get_city("INVALID")

    def test_keyerror_includes_valid_codes(self):
        with pytest.raises(KeyError, match="Valid codes"):
            get_city("ZZZ")


class TestAllCities:
    def test_returns_all_cities(self):
        cities = all_cities()
        assert len(cities) == len(CITIES)

    def test_sorted_by_name(self):
        cities = all_cities()
        names = [c.name for c in cities]
        assert names == sorted(names)

    def test_returns_list_not_dict_values(self):
        cities = all_cities()
        assert isinstance(cities, list)
        assert all(isinstance(c, CityConfig) for c in cities)
