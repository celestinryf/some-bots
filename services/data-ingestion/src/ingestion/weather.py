"""
Weather forecast ingestion job.

Fetches forecasts from a single weather source for all configured cities,
storing results in PostgreSQL with upsert via ON CONFLICT DO UPDATE.

Each call processes one source (NWS, VC, PW, or OWM). APScheduler runs
one job per source concurrently (Decision #13).
"""

import time
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import datetime, timezone

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from shared.config.logging import generate_correlation_id, get_logger
from shared.db.models import City, WeatherForecast

from src.clients.base import WeatherClient

logger = get_logger("weather-ingestion")


def run_weather_ingestion(
    *,
    client: WeatherClient,
    city_map: dict[str, City],
    session_factory: Callable[[], AbstractContextManager[Session]],
    forecast_date: datetime,
    run_id: str,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> None:
    """Ingest weather forecasts from one source for all cities.

    Per-city errors are caught and logged — one city's failure never
    kills the run. Uniqueness is enforced by the ``uq_forecast_dedup``
    constraint on (source, city_id, forecast_date). On conflict the
    existing row is updated with the latest issued_at, temps, and raw
    response so re-runs always store the freshest data.

    Args:
        client: Weather API client for one source.
        city_map: Dict of ticker_code → City ORM objects (loaded at startup).
        session_factory: Callable returning a context-managed DB session.
        forecast_date: Target forecast date (typically tomorrow at midnight UTC).
        run_id: Shared ID for the entire ingestion cycle.
        sleep_fn: Sleep function for inter-request delay (injectable for testing).
    """
    correlation_id = generate_correlation_id()
    source = str(client.source)

    logger.info(
        "weather_ingestion_start",
        source=source,
        run_id=run_id,
        correlation_id=correlation_id,
        city_count=len(city_map),
    )

    success_count = 0
    error_count = 0

    try:
        with session_factory() as session:
            for idx, (ticker_code, city) in enumerate(city_map.items()):
                # Inter-request delay between cities (e.g., NWS requires ~1s)
                if idx > 0 and client.inter_request_delay > 0:
                    sleep_fn(client.inter_request_delay)

                try:
                    result = client.fetch_forecast(
                        city_code=ticker_code,
                        lat=city.lat,
                        lon=city.lon,
                        forecast_date=forecast_date,
                        correlation_id=correlation_id,
                        city_timezone=city.timezone,
                    )

                    # SAVEPOINT per city so a single failure doesn't poison
                    # the session and roll back the entire batch.
                    with session.begin_nested():
                        stmt = pg_insert(WeatherForecast).values(
                            source=str(result.source),
                            city_id=city.id,
                            forecast_date=result.forecast_date,
                            issued_at=result.issued_at,
                            temp_high=result.temp_high,
                            temp_low=result.temp_low,
                            raw_response=result.raw_response,
                        ).on_conflict_do_update(
                            constraint="uq_forecast_dedup",
                            set_={
                                "issued_at": result.issued_at,
                                # COALESCE: never overwrite a valid temp with NULL
                                "temp_high": func.coalesce(
                                    result.temp_high, WeatherForecast.temp_high
                                ),
                                "temp_low": func.coalesce(
                                    result.temp_low, WeatherForecast.temp_low
                                ),
                                "raw_response": result.raw_response,
                                # Core INSERT bypasses ORM onupdate, so set explicitly
                                "updated_at": datetime.now(timezone.utc),
                            },
                        )
                        session.execute(stmt)

                    success_count += 1

                except Exception as exc:
                    error_count += 1
                    logger.error(
                        "weather_ingestion_city_error",
                        source=source,
                        city=ticker_code,
                        error=str(exc),
                        error_type=type(exc).__name__,
                        correlation_id=correlation_id,
                        run_id=run_id,
                    )
    except Exception as exc:
        logger.error(
            "weather_ingestion_session_failed",
            source=source,
            run_id=run_id,
            correlation_id=correlation_id,
            error=str(exc),
            error_type=type(exc).__name__,
            successful_cities_before_failure=success_count,
            errored_cities=error_count,
            total=len(city_map),
        )
        return

    logger.info(
        "weather_ingestion_complete",
        source=source,
        run_id=run_id,
        correlation_id=correlation_id,
        success=success_count,
        errors=error_count,
        total=len(city_map),
    )
