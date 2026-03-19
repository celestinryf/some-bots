"""Tests for date-target orchestration in the service entrypoint."""

import uuid
from datetime import date, datetime, timezone
from unittest.mock import MagicMock

from shared.db.models import City

from src import main


def _make_city(code: str, city_timezone: str) -> City:
    city = MagicMock(spec=City)
    city.id = uuid.uuid4()
    city.name = f"Test {code}"
    city.kalshi_ticker_prefix = code
    city.nws_station_id = f"K{code}"
    city.timezone = city_timezone
    city.lat = 0.0
    city.lon = 0.0
    return city  # type: ignore[return-value]


class TestWeatherTargetDates:
    def test_weather_job_wrapper_groups_cities_by_local_tomorrow(self, monkeypatch) -> None:
        monkeypatch.setattr(
            main,
            "_utc_now",
            lambda: datetime(2026, 3, 18, 7, 30, tzinfo=timezone.utc),
        )

        run_weather_ingestion = MagicMock()
        monkeypatch.setattr(main, "run_weather_ingestion", run_weather_ingestion)

        client = MagicMock()
        cycle_id_gen = MagicMock()
        cycle_id_gen.get.return_value = "cycle-1"

        city_map = {
            "NYC": _make_city("NYC", "America/New_York"),
            "HNL": _make_city("HNL", "Pacific/Honolulu"),
        }

        main._weather_job_wrapper(
            client=client,
            city_map=city_map,
            cycle_id_gen=cycle_id_gen,
        )

        assert run_weather_ingestion.call_count == 2

        first_call = run_weather_ingestion.call_args_list[0].kwargs
        assert first_call["forecast_date"] == datetime(2026, 3, 18, tzinfo=timezone.utc)
        assert set(first_call["city_map"]) == {"HNL"}

        second_call = run_weather_ingestion.call_args_list[1].kwargs
        assert second_call["forecast_date"] == datetime(2026, 3, 19, tzinfo=timezone.utc)
        assert set(second_call["city_map"]) == {"NYC"}

    def test_run_once_uses_city_local_tomorrow_batches(self, monkeypatch) -> None:
        monkeypatch.setattr(
            main,
            "_utc_now",
            lambda: datetime(2026, 3, 18, 7, 30, tzinfo=timezone.utc),
        )
        monkeypatch.setattr(main, "generate_correlation_id", lambda: "run-1")

        run_weather_ingestion = MagicMock()
        monkeypatch.setattr(main, "run_weather_ingestion", run_weather_ingestion)
        monkeypatch.setattr(main, "run_kalshi_snapshot_cleanup", MagicMock())

        city_map = {
            "NYC": _make_city("NYC", "America/New_York"),
            "HNL": _make_city("HNL", "Pacific/Honolulu"),
        }

        main._run_once(
            weather_clients=[MagicMock()],
            kalshi_client=None,
            city_map=city_map,
        )

        assert run_weather_ingestion.call_count == 2

        first_call = run_weather_ingestion.call_args_list[0].kwargs
        assert first_call["forecast_date"] == datetime(2026, 3, 18, tzinfo=timezone.utc)
        assert set(first_call["city_map"]) == {"HNL"}

        second_call = run_weather_ingestion.call_args_list[1].kwargs
        assert second_call["forecast_date"] == datetime(2026, 3, 19, tzinfo=timezone.utc)
        assert set(second_call["city_map"]) == {"NYC"}


class TestKalshiTargetDates:
    def test_kalshi_discovery_wrapper_groups_non_backfill_by_local_tomorrow(
        self, monkeypatch
    ) -> None:
        monkeypatch.setattr(
            main,
            "_utc_now",
            lambda: datetime(2026, 3, 18, 7, 30, tzinfo=timezone.utc),
        )

        run_kalshi_discovery = MagicMock()
        monkeypatch.setattr(main, "run_kalshi_discovery", run_kalshi_discovery)

        kalshi_client = MagicMock()
        cycle_id_gen = MagicMock()
        cycle_id_gen.get.return_value = "cycle-1"

        city_map = {
            "NYC": _make_city("NYC", "America/New_York"),
            "HNL": _make_city("HNL", "Pacific/Honolulu"),
        }

        main._kalshi_discovery_wrapper(
            kalshi_client=kalshi_client,
            city_map=city_map,
            cycle_id_gen=cycle_id_gen,
        )

        assert run_kalshi_discovery.call_count == 2

        first_call = run_kalshi_discovery.call_args_list[0].kwargs
        assert first_call["forecast_date"] == date(2026, 3, 18)
        assert set(first_call["city_map"]) == {"HNL"}

        second_call = run_kalshi_discovery.call_args_list[1].kwargs
        assert second_call["forecast_date"] == date(2026, 3, 19)
        assert set(second_call["city_map"]) == {"NYC"}

    def test_kalshi_discovery_wrapper_full_backfill_unchanged(self, monkeypatch) -> None:
        run_kalshi_discovery = MagicMock()
        monkeypatch.setattr(main, "run_kalshi_discovery", run_kalshi_discovery)

        kalshi_client = MagicMock()
        cycle_id_gen = MagicMock()
        cycle_id_gen.get.return_value = "cycle-1"

        city_map = {
            "NYC": _make_city("NYC", "America/New_York"),
            "HNL": _make_city("HNL", "Pacific/Honolulu"),
        }

        main._kalshi_discovery_wrapper(
            kalshi_client=kalshi_client,
            city_map=city_map,
            cycle_id_gen=cycle_id_gen,
            full_backfill=True,
        )

        run_kalshi_discovery.assert_called_once()
        assert run_kalshi_discovery.call_args.kwargs["forecast_date"] is None
        assert set(run_kalshi_discovery.call_args.kwargs["city_map"]) == {"NYC", "HNL"}
