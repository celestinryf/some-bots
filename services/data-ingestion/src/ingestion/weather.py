"""
Weather forecast ingestion job.

Fetches forecasts from a single weather source for all configured cities,
storing results in PostgreSQL with deduplication via ON CONFLICT DO NOTHING.

Each call processes one source (NWS, VC, PW, or OWM). APScheduler runs
one job per source concurrently (Decision #13).
"""

import time
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import datetime

from sqlalchemy import CursorResult
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
    kills the run. Results are deduplicated via INSERT ON CONFLICT DO NOTHING
    on the ``uq_forecast_dedup`` constraint.

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
    skip_count = 0
    error_count = 0

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
            )

            with session_factory() as session:
                stmt = pg_insert(WeatherForecast).values(
                    source=str(result.source),
                    city_id=city.id,
                    forecast_date=result.forecast_date,
                    issued_at=result.issued_at,
                    temp_high=result.temp_high,
                    temp_low=result.temp_low,
                    raw_response=result.raw_response,
                ).on_conflict_do_nothing(
                    constraint="uq_forecast_dedup",
                )
                db_result: CursorResult[tuple[()]] = session.execute(stmt)  # type: ignore[assignment]

                if db_result.rowcount == 0:
                    skip_count += 1
                    logger.debug(
                        "forecast_dedup_skipped",
                        source=source,
                        city=ticker_code,
                        correlation_id=correlation_id,
                    )
                else:
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

    logger.info(
        "weather_ingestion_complete",
        source=source,
        run_id=run_id,
        correlation_id=correlation_id,
        success=success_count,
        skipped=skip_count,
        errors=error_count,
        total=len(city_map),
    )
