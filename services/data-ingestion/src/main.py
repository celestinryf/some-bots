"""
Data ingestion service entry point.

Runs scheduled jobs to fetch weather forecasts and Kalshi market data,
storing results in PostgreSQL. Supports two modes:

  python -m src.main              # APScheduler daemon (production)
  python -m src.main --run-once   # Run all jobs once, then exit (debug/testing)
"""

import argparse
import math
import signal
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import FrameType

from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore[import-untyped]
from dotenv import load_dotenv
from sqlalchemy import select

from shared.config.logging import generate_correlation_id, get_logger, setup_logging
from shared.config.settings import get_settings
from shared.db.models import City
from shared.db.seed import seed_cities
from shared.db.session import get_session

from src.clients.base import WeatherClient
from src.clients.kalshi import KalshiClient
from src.ingestion.factories import close_clients, create_kalshi_client, create_weather_clients
from src.ingestion.kalshi import (
    run_kalshi_discovery,
    run_kalshi_settlements,
    run_kalshi_snapshot_cleanup,
    run_kalshi_snapshots,
)
from src.ingestion.weather import run_weather_ingestion

logger = get_logger("data-ingestion")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def load_city_map() -> dict[str, City]:
    """Load all cities from DB, keyed by kalshi_ticker_prefix.

    Must be called after seed_cities() so all cities exist.
    The returned objects remain usable after the session closes
    because the session factory uses expire_on_commit=False.
    """
    with get_session() as session:
        cities = session.execute(select(City)).scalars().all()
        return {city.kalshi_ticker_prefix: city for city in cities}


def get_forecast_date() -> datetime:
    """Return tomorrow's date at midnight UTC as the forecast target."""
    now = datetime.now(timezone.utc)
    tomorrow = (now + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return tomorrow


class CycleIdGenerator:
    """Generates a shared run_id per scheduling cycle.

    Jobs that fire within the same ``interval_seconds`` window share one
    run_id so their log lines can be correlated (e.g., all four weather
    sources in the same 2-hour cycle).  Thread-safe.
    """

    def __init__(self, interval_seconds: float) -> None:
        self._interval = interval_seconds
        self._lock = threading.Lock()
        self._current_id: str = ""
        self._current_bucket: int = -1

    def get(self) -> str:
        bucket = math.floor(time.time() / self._interval)
        with self._lock:
            if bucket != self._current_bucket:
                self._current_bucket = bucket
                self._current_id = generate_correlation_id()
            return self._current_id


# ---------------------------------------------------------------------------
# APScheduler job wrappers
#
# These thin wrappers generate a run_id per invocation and call the
# actual job functions with real dependencies (get_session, etc.).
# ---------------------------------------------------------------------------


def _weather_job_wrapper(
    *,
    client: WeatherClient,
    city_map: dict[str, City],
    cycle_id_gen: CycleIdGenerator,
) -> None:
    run_id = cycle_id_gen.get()
    forecast_date = get_forecast_date()
    run_weather_ingestion(
        client=client,
        city_map=city_map,
        session_factory=get_session,
        forecast_date=forecast_date,
        run_id=run_id,
    )


def _kalshi_discovery_wrapper(
    *,
    kalshi_client: KalshiClient,
    city_map: dict[str, City],
    cycle_id_gen: CycleIdGenerator,
    full_backfill: bool = False,
) -> None:
    run_id = cycle_id_gen.get()
    # full_backfill=True passes forecast_date=None to discover all open
    # markets (not just tomorrow), catching multi-day-out markets that
    # Kalshi may open after the cold-start backfill.
    forecast_date = None if full_backfill else get_forecast_date().date()
    run_kalshi_discovery(
        kalshi_client=kalshi_client,
        city_map=city_map,
        session_factory=get_session,
        forecast_date=forecast_date,
        run_id=run_id,
    )


def _kalshi_snapshots_wrapper(
    *,
    kalshi_client: KalshiClient,
    snapshot_cycle_id_gen: CycleIdGenerator,
) -> None:
    run_id = snapshot_cycle_id_gen.get()
    run_kalshi_snapshots(
        kalshi_client=kalshi_client,
        session_factory=get_session,
        run_id=run_id,
    )


def _kalshi_settlements_wrapper(
    *,
    kalshi_client: KalshiClient,
    cycle_id_gen: CycleIdGenerator,
) -> None:
    run_id = cycle_id_gen.get()
    run_kalshi_settlements(
        kalshi_client=kalshi_client,
        session_factory=get_session,
        run_id=run_id,
    )


def _kalshi_snapshot_cleanup_wrapper(
    *,
    cleanup_cycle_id_gen: CycleIdGenerator,
) -> None:
    run_id = cleanup_cycle_id_gen.get()
    run_kalshi_snapshot_cleanup(
        session_factory=get_session,
        run_id=run_id,
    )


# ---------------------------------------------------------------------------
# Run modes
# ---------------------------------------------------------------------------


def _run_once(
    weather_clients: list[WeatherClient],
    kalshi_client: KalshiClient | None,
    city_map: dict[str, City],
) -> None:
    """Run all jobs once synchronously, then return."""
    run_id = generate_correlation_id()
    forecast_date = get_forecast_date()
    logger.info(
        "run_once_start",
        run_id=run_id,
        forecast_date=str(forecast_date),
    )

    for client in weather_clients:
        run_weather_ingestion(
            client=client,
            city_map=city_map,
            session_factory=get_session,
            forecast_date=forecast_date,
            run_id=run_id,
        )

    if kalshi_client is not None:
        # Discover all open markets (today + tomorrow) to avoid cold-start gaps
        run_kalshi_discovery(
            kalshi_client=kalshi_client,
            city_map=city_map,
            session_factory=get_session,
            forecast_date=None,
            run_id=run_id,
        )
        run_kalshi_snapshots(
            kalshi_client=kalshi_client,
            session_factory=get_session,
            run_id=run_id,
        )
        run_kalshi_settlements(
            kalshi_client=kalshi_client,
            session_factory=get_session,
            run_id=run_id,
        )

    # Snapshot retention cleanup (runs regardless of Kalshi client)
    run_kalshi_snapshot_cleanup(
        session_factory=get_session,
        run_id=run_id,
    )

    logger.info("run_once_complete", run_id=run_id)


def _run_scheduled(
    weather_clients: list[WeatherClient],
    kalshi_client: KalshiClient | None,
    city_map: dict[str, City],
    shutdown_event: threading.Event,
) -> None:
    """Start APScheduler with interval triggers. Blocks until SIGINT/SIGTERM.

    Args:
        shutdown_event: Event that is set by the signal handler registered
            in main() *before* this function is called, so SIGTERM/SIGINT
            during the cold-start phase triggers a graceful exit.
    """

    # Cold-start backfill: discover all open markets (today + tomorrow)
    # so today's already-active markets aren't missed on fresh deployment.
    if kalshi_client is not None and not shutdown_event.is_set():
        startup_run_id = generate_correlation_id()
        logger.info("cold_start_discovery", run_id=startup_run_id)
        run_kalshi_discovery(
            kalshi_client=kalshi_client,
            city_map=city_map,
            session_factory=get_session,
            forecast_date=None,
            run_id=startup_run_id,
        )

    if shutdown_event.is_set():
        logger.info("shutdown_before_scheduler_start")
        return

    scheduler = BackgroundScheduler(timezone="UTC")  # type: ignore[reportUnknownMemberType]
    now = datetime.now(timezone.utc)

    # Cycle ID generators — jobs on the same interval share a run_id so
    # log lines from the same scheduling cycle can be correlated.
    two_hour_cycle = CycleIdGenerator(interval_seconds=2 * 3600)
    five_min_cycle = CycleIdGenerator(interval_seconds=5 * 60)
    daily_cycle = CycleIdGenerator(interval_seconds=24 * 3600)

    # Weather jobs: one per source, every 2 hours, run immediately (Decision #13)
    for client in weather_clients:
        scheduler.add_job(  # type: ignore[reportUnknownMemberType]
            _weather_job_wrapper,
            "interval",
            hours=2,
            kwargs={"client": client, "city_map": city_map, "cycle_id_gen": two_hour_cycle},
            id=f"weather_{client.source}",
            name=f"Weather ingestion: {client.source}",
            max_instances=1,
            next_run_time=now,
        )

    # Kalshi jobs (Decision #4: discovery 2h, snapshots 5m)
    if kalshi_client is not None:
        scheduler.add_job(  # type: ignore[reportUnknownMemberType]
            _kalshi_discovery_wrapper,
            "interval",
            hours=2,
            kwargs={"kalshi_client": kalshi_client, "city_map": city_map, "cycle_id_gen": two_hour_cycle},
            id="kalshi_discovery",
            name="Kalshi market discovery",
            max_instances=1,
            next_run_time=now,
        )

        # Daily full-backfill discovery to catch multi-day-out markets that
        # Kalshi may open after the cold-start backfill.
        scheduler.add_job(  # type: ignore[reportUnknownMemberType]
            _kalshi_discovery_wrapper,
            "interval",
            hours=24,
            kwargs={
                "kalshi_client": kalshi_client,
                "city_map": city_map,
                "cycle_id_gen": daily_cycle,
                "full_backfill": True,
            },
            id="kalshi_discovery_full",
            name="Kalshi full market discovery (backfill)",
            max_instances=1,
            next_run_time=now + timedelta(minutes=1),
        )

        scheduler.add_job(  # type: ignore[reportUnknownMemberType]
            _kalshi_snapshots_wrapper,
            "interval",
            minutes=5,
            kwargs={"kalshi_client": kalshi_client, "snapshot_cycle_id_gen": five_min_cycle},
            id="kalshi_snapshots",
            name="Kalshi price snapshots",
            max_instances=1,
            # Delay 30s so discovery runs first
            next_run_time=now + timedelta(seconds=30),
        )

        scheduler.add_job(  # type: ignore[reportUnknownMemberType]
            _kalshi_settlements_wrapper,
            "interval",
            hours=2,
            kwargs={"kalshi_client": kalshi_client, "cycle_id_gen": two_hour_cycle},
            id="kalshi_settlements",
            name="Kalshi settlement tracking",
            max_instances=1,
            next_run_time=now + timedelta(seconds=15),
        )

    # Snapshot retention cleanup — daily, independent of Kalshi client
    scheduler.add_job(  # type: ignore[reportUnknownMemberType]
        _kalshi_snapshot_cleanup_wrapper,
        "interval",
        hours=24,
        kwargs={"cleanup_cycle_id_gen": daily_cycle},
        id="kalshi_snapshot_cleanup",
        name="Kalshi snapshot retention cleanup",
        max_instances=1,
        next_run_time=now + timedelta(minutes=5),
    )

    scheduler.start()  # type: ignore[reportUnknownMemberType]
    job_count = len(scheduler.get_jobs())  # type: ignore[reportUnknownMemberType]
    logger.info("scheduler_started", jobs=job_count)

    shutdown_event.wait()
    scheduler.shutdown(wait=True)  # type: ignore[reportUnknownMemberType]
    logger.info("scheduler_stopped")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    load_dotenv()
    settings = get_settings()
    setup_logging(settings.log_level)

    parser = argparse.ArgumentParser(description="Data ingestion service")
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Run all jobs once synchronously, then exit",
    )
    args = parser.parse_args()

    logger.info(
        "service_starting",
        environment=settings.environment,
        run_once=args.run_once,
    )

    # 1. Seed cities (idempotent)
    seed_cities()

    # 2. Load city map cache (Decision #3: load once at startup)
    city_map = load_city_map()
    logger.info("city_map_loaded", count=len(city_map))

    # 3. Create clients (Decision #5: factory functions)
    weather_clients = create_weather_clients(
        settings, gridpoint_cache_path=Path("./data/nws_gridpoints.json")
    )
    kalshi_client = create_kalshi_client(settings)

    if not weather_clients and kalshi_client is None:
        logger.error(
            "no_clients_configured",
            message="No weather or Kalshi clients available. Check API keys.",
        )
        sys.exit(1)

    # Register signal handlers early so SIGTERM/SIGINT during cold-start
    # discovery (or any startup phase) triggers a graceful exit instead
    # of Python's default immediate termination.
    shutdown_event = threading.Event()

    def _signal_handler(signum: int, frame: FrameType | None) -> None:
        logger.info("shutdown_signal_received", signal=signum)
        shutdown_event.set()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    try:
        if args.run_once:
            _run_once(weather_clients, kalshi_client, city_map)
        else:
            _run_scheduled(weather_clients, kalshi_client, city_map, shutdown_event)
    finally:
        close_clients(weather_clients, kalshi_client)
        logger.info("service_stopped")


if __name__ == "__main__":
    main()
