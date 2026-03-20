"""Tests for client factory functions.

Decision #11: Real construction + close. httpx.Client is cheap to construct,
so we test the actual factory logic without mocking client constructors.
"""

from pathlib import Path
from unittest.mock import MagicMock

from src.clients.nws import NwsClient
from src.clients.openweathermap import OpenWeatherMapClient
from src.clients.pirate_weather import PirateWeatherClient
from src.clients.visual_crossing import VisualCrossingClient
from src.ingestion.factories import close_clients, create_kalshi_client, create_weather_clients

from shared.config.settings import Settings
from shared.db.enums import WeatherSource


def _noop_sleep(seconds: float) -> None:
    pass


# ---------------------------------------------------------------------------
# create_weather_clients
# ---------------------------------------------------------------------------


class TestCreateWeatherClients:
    def test_all_keys_creates_four_clients(self) -> None:
        settings = Settings(
            nws_user_agent="(test, test@test.com)",
            visual_crossing_api_key="vc-key",
            pirate_weather_api_key="pw-key",
            openweather_api_key="ow-key",
        )
        clients = create_weather_clients(settings, sleep_fn=_noop_sleep)
        try:
            assert len(clients) == 4
            sources = {c.source for c in clients}
            assert sources == {
                WeatherSource.NWS,
                WeatherSource.VISUAL_CROSSING,
                WeatherSource.PIRATE_WEATHER,
                WeatherSource.OPENWEATHER,
            }
        finally:
            for c in clients:
                c.close()

    def test_empty_key_skips_client(self) -> None:
        settings = Settings(
            nws_user_agent="(test, test@test.com)",
            visual_crossing_api_key="",
            pirate_weather_api_key="pw-key",
            openweather_api_key="ow-key",
        )
        clients = create_weather_clients(settings, sleep_fn=_noop_sleep)
        try:
            assert len(clients) == 3
            sources = {c.source for c in clients}
            assert WeatherSource.VISUAL_CROSSING not in sources
        finally:
            for c in clients:
                c.close()

    def test_no_keys_returns_empty_list(self) -> None:
        settings = Settings()
        clients = create_weather_clients(settings, sleep_fn=_noop_sleep)
        assert clients == []

    def test_whitespace_only_key_skips_client(self) -> None:
        settings = Settings(
            nws_user_agent="   ",
            visual_crossing_api_key="  ",
        )
        clients = create_weather_clients(settings, sleep_fn=_noop_sleep)
        assert clients == []

    def test_constructor_exception_skips_client(self) -> None:
        """NwsClient raises ValueError on blank user_agent after strip."""
        settings = Settings(
            nws_user_agent="   ",
            pirate_weather_api_key="pw-key",
        )
        clients = create_weather_clients(settings, sleep_fn=_noop_sleep)
        try:
            # NWS skipped (blank user_agent), PW succeeds
            assert len(clients) == 1
            assert clients[0].source == WeatherSource.PIRATE_WEATHER
        finally:
            for c in clients:
                c.close()

    def test_gridpoint_cache_path_passed_to_nws(self, tmp_path: Path) -> None:
        settings = Settings(nws_user_agent="(test, test@test.com)")
        cache_path = tmp_path / "gp.json"
        clients = create_weather_clients(
            settings, gridpoint_cache_path=cache_path, sleep_fn=_noop_sleep
        )
        try:
            assert len(clients) == 1
            nws = clients[0]
            assert isinstance(nws, NwsClient)
            assert nws._cache_path == cache_path  # pyright: ignore[reportPrivateUsage]
        finally:
            for c in clients:
                c.close()

    def test_client_types_match_sources(self) -> None:
        settings = Settings(
            nws_user_agent="(test, test@test.com)",
            visual_crossing_api_key="vc-key",
            pirate_weather_api_key="pw-key",
            openweather_api_key="ow-key",
        )
        clients = create_weather_clients(settings, sleep_fn=_noop_sleep)
        try:
            type_map = {c.source: type(c) for c in clients}
            assert type_map[WeatherSource.NWS] is NwsClient
            assert type_map[WeatherSource.VISUAL_CROSSING] is VisualCrossingClient
            assert type_map[WeatherSource.PIRATE_WEATHER] is PirateWeatherClient
            assert type_map[WeatherSource.OPENWEATHER] is OpenWeatherMapClient
        finally:
            for c in clients:
                c.close()


# ---------------------------------------------------------------------------
# create_kalshi_client
# ---------------------------------------------------------------------------


class TestCreateKalshiClient:
    def test_missing_key_id_returns_none(self) -> None:
        settings = Settings(kalshi_api_key_id="", kalshi_key_path="./key.pem")
        assert create_kalshi_client(settings) is None

    def test_missing_key_path_returns_none(self) -> None:
        settings = Settings(kalshi_api_key_id="test-id", kalshi_key_path="")
        assert create_kalshi_client(settings) is None

    def test_both_missing_returns_none(self) -> None:
        settings = Settings()
        assert create_kalshi_client(settings) is None

    def test_bad_pem_path_returns_none(self) -> None:
        """Non-existent PEM file causes construction failure → returns None."""
        settings = Settings(
            kalshi_api_key_id="test-id",
            kalshi_key_path="/nonexistent/path/key.pem",
        )
        result = create_kalshi_client(settings)
        assert result is None


# ---------------------------------------------------------------------------
# close_clients
# ---------------------------------------------------------------------------


class TestCloseClients:
    def test_closes_all_clients(self) -> None:
        mock_weather = [MagicMock(), MagicMock()]
        mock_kalshi = MagicMock()
        close_clients(mock_weather, mock_kalshi)
        for m in mock_weather:
            m.close.assert_called_once()
        mock_kalshi.close.assert_called_once()

    def test_handles_none_kalshi(self) -> None:
        close_clients([], None)  # Should not raise

    def test_exception_in_close_does_not_propagate(self) -> None:
        m = MagicMock()
        m.close.side_effect = RuntimeError("close failed")
        m.source = "test"
        close_clients([m], None)  # Should not raise

    def test_kalshi_close_exception_does_not_propagate(self) -> None:
        mock_kalshi = MagicMock()
        mock_kalshi.close.side_effect = RuntimeError("close failed")
        close_clients([], mock_kalshi)  # Should not raise

    def test_continues_closing_after_error(self) -> None:
        """If first client.close() fails, second still gets closed."""
        m1 = MagicMock()
        m1.close.side_effect = RuntimeError("oops")
        m1.source = "src1"
        m2 = MagicMock()
        m2.source = "src2"

        close_clients([m1, m2], None)
        m1.close.assert_called_once()
        m2.close.assert_called_once()
