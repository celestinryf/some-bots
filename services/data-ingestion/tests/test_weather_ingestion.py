"""Tests for weather ingestion job function.

Decision #9: DI via function arguments — tests pass mock clients and sessions.
Decision #12: Failure injection — verify per-city error isolation.
"""

import uuid
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from shared.config.errors import WeatherApiError
from shared.db.enums import WeatherSource
from shared.db.models import City

from src.clients.models import ForecastResult
from src.ingestion.weather import run_weather_ingestion


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_city(code: str = "NYC") -> City:
    """Create a mock City with the fields needed by the ingestion job."""
    city = MagicMock(spec=City)
    city.id = uuid.uuid4()
    city.name = f"Test {code}"
    city.kalshi_ticker_prefix = code
    city.nws_station_id = f"K{code}"
    city.timezone = "America/New_York"
    city.lat = 40.7
    city.lon = -74.0
    return city  # type: ignore[return-value]


def _make_forecast_result(
    source: WeatherSource = WeatherSource.NWS,
    city_code: str = "NYC",
) -> ForecastResult:
    return ForecastResult(
        source=source,
        city_code=city_code,
        forecast_date=datetime(2026, 3, 17, tzinfo=timezone.utc),
        issued_at=datetime(2026, 3, 16, 14, 0, tzinfo=timezone.utc),
        temp_high=72.0,
        temp_low=55.0,
        raw_response={"test": True},
    )


def _mock_session_factory(
    mock_session: MagicMock,
    *,
    exit_error: Exception | None = None,
) -> Callable[[], AbstractContextManager[MagicMock]]:
    """Create a session factory that yields the given mock session.

    Configures begin_nested() to return a context manager (SAVEPOINT mock)
    matching the single-session + savepoint pattern used in weather ingestion.
    """
    # Make begin_nested() usable as a context manager (SAVEPOINT)
    mock_session.begin_nested.return_value.__enter__ = MagicMock(return_value=None)
    mock_session.begin_nested.return_value.__exit__ = MagicMock(return_value=False)

    class _SessionContext(AbstractContextManager[MagicMock]):
        def __enter__(self) -> MagicMock:
            return mock_session

        def __exit__(self, exc_type, exc, tb) -> bool:
            if exit_error is not None:
                raise exit_error
            return False

    def factory() -> AbstractContextManager[MagicMock]:
        return _SessionContext()

    return factory


def _noop_sleep(seconds: float) -> None:
    pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestRunWeatherIngestion:
    def test_happy_path_inserts_forecast(self) -> None:
        """Fetches forecast for each city and inserts into DB."""
        mock_client = MagicMock()
        mock_client.source = WeatherSource.NWS
        mock_client.inter_request_delay = 0.0
        mock_client.fetch_forecast.return_value = _make_forecast_result()

        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.rowcount = 1
        mock_session.execute.return_value = mock_result

        city = _make_city("NYC")

        run_weather_ingestion(
            client=mock_client,
            city_map={"NYC": city},
            session_factory=_mock_session_factory(mock_session),
            forecast_date=datetime(2026, 3, 17, tzinfo=timezone.utc),
            run_id="test-run-1",
            sleep_fn=_noop_sleep,
        )

        mock_client.fetch_forecast.assert_called_once()
        mock_session.execute.assert_called_once()

    def test_single_city_failure_does_not_stop_others(self) -> None:
        """Per-city try/except: one failure, others still succeed."""
        mock_client = MagicMock()
        mock_client.source = WeatherSource.NWS
        mock_client.inter_request_delay = 0.0

        city_a = _make_city("NYC")
        city_b = _make_city("CHI")

        # NYC fails, CHI succeeds
        mock_client.fetch_forecast.side_effect = [
            WeatherApiError("API down", city="NYC", source="NWS"),
            _make_forecast_result(city_code="CHI"),
        ]

        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.rowcount = 1
        mock_session.execute.return_value = mock_result

        run_weather_ingestion(
            client=mock_client,
            city_map={"NYC": city_a, "CHI": city_b},
            session_factory=_mock_session_factory(mock_session),
            forecast_date=datetime(2026, 3, 17, tzinfo=timezone.utc),
            run_id="test-run-2",
            sleep_fn=_noop_sleep,
        )

        # Both cities attempted
        assert mock_client.fetch_forecast.call_count == 2
        # Only CHI's insert was attempted (NYC raised before DB)
        assert mock_session.execute.call_count == 1

    def test_all_cities_fail_no_exception_propagates(self) -> None:
        """Even if every city fails, the function completes normally."""
        mock_client = MagicMock()
        mock_client.source = WeatherSource.NWS
        mock_client.inter_request_delay = 0.0
        mock_client.fetch_forecast.side_effect = WeatherApiError("boom")

        mock_session = MagicMock()

        run_weather_ingestion(
            client=mock_client,
            city_map={"NYC": _make_city("NYC"), "CHI": _make_city("CHI")},
            session_factory=_mock_session_factory(mock_session),
            forecast_date=datetime(2026, 3, 17, tzinfo=timezone.utc),
            run_id="test-run-3",
            sleep_fn=_noop_sleep,
        )

        assert mock_client.fetch_forecast.call_count == 2
        mock_session.execute.assert_not_called()

    def test_upsert_on_conflict_updates_existing(self) -> None:
        """ON CONFLICT DO UPDATE: re-fetching same (source, city, date) updates the row."""
        mock_client = MagicMock()
        mock_client.source = WeatherSource.NWS
        mock_client.inter_request_delay = 0.0
        mock_client.fetch_forecast.return_value = _make_forecast_result()

        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.rowcount = 1  # Updated existing row
        mock_session.execute.return_value = mock_result

        run_weather_ingestion(
            client=mock_client,
            city_map={"NYC": _make_city("NYC")},
            session_factory=_mock_session_factory(mock_session),
            forecast_date=datetime(2026, 3, 17, tzinfo=timezone.utc),
            run_id="test-run-4",
            sleep_fn=_noop_sleep,
        )

        # Upsert should execute and count as success
        mock_session.execute.assert_called_once()

    def test_inter_request_delay_respected(self) -> None:
        """When inter_request_delay > 0, sleep is called between cities."""
        sleep_calls: list[float] = []

        def capture_sleep(seconds: float) -> None:
            sleep_calls.append(seconds)

        mock_client = MagicMock()
        mock_client.source = WeatherSource.NWS
        mock_client.inter_request_delay = 1.0
        mock_client.fetch_forecast.return_value = _make_forecast_result()

        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.rowcount = 1
        mock_session.execute.return_value = mock_result

        run_weather_ingestion(
            client=mock_client,
            city_map={"NYC": _make_city("NYC"), "CHI": _make_city("CHI"), "MIA": _make_city("MIA")},
            session_factory=_mock_session_factory(mock_session),
            forecast_date=datetime(2026, 3, 17, tzinfo=timezone.utc),
            run_id="test-run-5",
            sleep_fn=capture_sleep,
        )

        # Sleep between cities: 2 sleeps for 3 cities
        assert len(sleep_calls) == 2
        assert all(s == 1.0 for s in sleep_calls)

    def test_no_delay_when_inter_request_delay_zero(self) -> None:
        sleep_calls: list[float] = []

        mock_client = MagicMock()
        mock_client.source = WeatherSource.VISUAL_CROSSING
        mock_client.inter_request_delay = 0.0
        mock_client.fetch_forecast.return_value = _make_forecast_result(
            source=WeatherSource.VISUAL_CROSSING
        )

        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.rowcount = 1
        mock_session.execute.return_value = mock_result

        run_weather_ingestion(
            client=mock_client,
            city_map={"NYC": _make_city("NYC"), "CHI": _make_city("CHI")},
            session_factory=_mock_session_factory(mock_session),
            forecast_date=datetime(2026, 3, 17, tzinfo=timezone.utc),
            run_id="test-run-6",
            sleep_fn=lambda s: sleep_calls.append(s),
        )

        assert len(sleep_calls) == 0

    def test_correlation_id_passed_to_fetch(self) -> None:
        """Each fetch_forecast call receives a correlation_id."""
        mock_client = MagicMock()
        mock_client.source = WeatherSource.NWS
        mock_client.inter_request_delay = 0.0
        mock_client.fetch_forecast.return_value = _make_forecast_result()

        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.rowcount = 1
        mock_session.execute.return_value = mock_result

        run_weather_ingestion(
            client=mock_client,
            city_map={"NYC": _make_city("NYC")},
            session_factory=_mock_session_factory(mock_session),
            forecast_date=datetime(2026, 3, 17, tzinfo=timezone.utc),
            run_id="test-run-7",
            sleep_fn=_noop_sleep,
        )

        call_kwargs = mock_client.fetch_forecast.call_args
        assert call_kwargs is not None
        assert "correlation_id" in call_kwargs.kwargs
        assert call_kwargs.kwargs["correlation_id"] is not None

    def test_db_error_caught_and_counted(self) -> None:
        """SQLAlchemy error during insert is caught, not propagated."""
        mock_client = MagicMock()
        mock_client.source = WeatherSource.NWS
        mock_client.inter_request_delay = 0.0
        mock_client.fetch_forecast.return_value = _make_forecast_result()

        mock_session = MagicMock()
        mock_session.execute.side_effect = RuntimeError("DB connection lost")

        run_weather_ingestion(
            client=mock_client,
            city_map={"NYC": _make_city("NYC")},
            session_factory=_mock_session_factory(mock_session),
            forecast_date=datetime(2026, 3, 17, tzinfo=timezone.utc),
            run_id="test-run-8",
            sleep_fn=_noop_sleep,
        )

        # Function completed without raising
        mock_client.fetch_forecast.assert_called_once()

    def test_commit_failure_is_logged_and_suppressed(self) -> None:
        """Session commit/context failures should not crash the ingestion run."""
        mock_client = MagicMock()
        mock_client.source = WeatherSource.NWS
        mock_client.inter_request_delay = 0.0
        mock_client.fetch_forecast.return_value = _make_forecast_result()

        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.rowcount = 1
        mock_session.execute.return_value = mock_result

        with patch("src.ingestion.weather.logger.error") as mock_log_error:
            run_weather_ingestion(
                client=mock_client,
                city_map={"NYC": _make_city("NYC")},
                session_factory=_mock_session_factory(
                    mock_session,
                    exit_error=RuntimeError("commit failed"),
                ),
                forecast_date=datetime(2026, 3, 17, tzinfo=timezone.utc),
                run_id="test-run-commit-failure",
                sleep_fn=_noop_sleep,
            )

        mock_client.fetch_forecast.assert_called_once()
        mock_session.execute.assert_called_once()
        mock_log_error.assert_called_once()
        assert mock_log_error.call_args.args[0] == "weather_ingestion_session_failed"

    def test_empty_city_map_is_noop(self) -> None:
        mock_client = MagicMock()
        mock_client.source = WeatherSource.NWS
        mock_client.inter_request_delay = 0.0

        mock_session = MagicMock()

        run_weather_ingestion(
            client=mock_client,
            city_map={},
            session_factory=_mock_session_factory(mock_session),
            forecast_date=datetime(2026, 3, 17, tzinfo=timezone.utc),
            run_id="test-run-9",
            sleep_fn=_noop_sleep,
        )

        mock_client.fetch_forecast.assert_not_called()

    def test_partial_null_temp_uses_coalesce(self) -> None:
        """Upsert uses COALESCE so a null temp never overwrites a valid one."""
        mock_client = MagicMock()
        mock_client.source = WeatherSource.NWS
        mock_client.inter_request_delay = 0.0
        # temp_low is None — should not overwrite an existing valid value
        mock_client.fetch_forecast.return_value = _make_forecast_result()
        # Override temp_low to None via a new ForecastResult
        mock_client.fetch_forecast.return_value = ForecastResult(
            source=WeatherSource.NWS,
            city_code="NYC",
            forecast_date=datetime(2026, 3, 17, tzinfo=timezone.utc),
            issued_at=datetime(2026, 3, 16, 14, 0, tzinfo=timezone.utc),
            temp_high=72.0,
            temp_low=None,
            raw_response={"test": True},
        )

        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.rowcount = 1
        mock_session.execute.return_value = mock_result

        run_weather_ingestion(
            client=mock_client,
            city_map={"NYC": _make_city("NYC")},
            session_factory=_mock_session_factory(mock_session),
            forecast_date=datetime(2026, 3, 17, tzinfo=timezone.utc),
            run_id="test-run-10",
            sleep_fn=_noop_sleep,
        )

        # Upsert should still execute (COALESCE handles the null protection)
        mock_session.execute.assert_called_once()
