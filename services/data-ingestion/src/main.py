"""
Data ingestion service entry point.

Runs scheduled jobs to fetch weather forecasts and Kalshi market data,
storing results in PostgreSQL. Supports two modes:

  python -m src.main              # APScheduler daemon (production)
  python -m src.main --run-once   # Run all jobs once, then exit (debug/testing)
"""

import argparse
import math
import os
import signal
import sys
import threading
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import FrameType
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore[import-untyped]
from dotenv import load_dotenv
from sqlalchemy import select

from shared.config.logging import generate_correlation_id, get_logger, setup_logging
from shared.config.settings import get_settings
from shared.db.enums import WeatherSource
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

_DEFAULT_WEATHER_POLL_INTERVAL_SECONDS = 2 * 3600
_MIN_WEATHER_POLL_INTERVAL_SECONDS = 5 * 60
_MAX_WEATHER_POLL_INTERVAL_SECONDS = 24 * 3600


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


def _utc_now() -> datetime:
    """Return the current UTC time.

    Split out for deterministic tests around UTC/local day boundaries.
    """
    return datetime.now(timezone.utc)


def _get_city_local_tomorrow(city: City, *, now_utc: datetime | None = None) -> date:
    """Return the city's local tomorrow calendar date."""
    current_utc = now_utc or _utc_now()
    local_now = current_utc.astimezone(ZoneInfo(city.timezone))
    return local_now.date() + timedelta(days=1)


def _get_city_forecast_datetime(
    city: City,
    *,
    now_utc: datetime | None = None,
) -> datetime:
    """Return the city's target local-tomorrow date anchored at midnight UTC."""
    target_date = _get_city_local_tomorrow(city, now_utc=now_utc)
    return datetime(
        target_date.year,
        target_date.month,
        target_date.day,
        tzinfo=timezone.utc,
    )


def _group_cities_by_weather_target_date(
    city_map: dict[str, City],
    *,
    now_utc: datetime | None = None,
) -> list[tuple[datetime, dict[str, City]]]:
    """Group cities by the UTC-anchored datetime for each city's local tomorrow."""
    current_utc = now_utc or _utc_now()
    grouped: dict[datetime, dict[str, City]] = defaultdict(dict)

    for ticker_code, city in city_map.items():
        forecast_date = _get_city_forecast_datetime(city, now_utc=current_utc)
        grouped[forecast_date][ticker_code] = city

    return sorted(grouped.items(), key=lambda item: item[0])


def _group_cities_by_kalshi_target_date(
    city_map: dict[str, City],
    *,
    now_utc: datetime | None = None,
) -> list[tuple[date, dict[str, City]]]:
    """Group cities by the local-tomorrow calendar date used for Kalshi discovery."""
    current_utc = now_utc or _utc_now()
    grouped: dict[date, dict[str, City]] = defaultdict(dict)

    for ticker_code, city in city_map.items():
        forecast_date = _get_city_local_tomorrow(city, now_utc=current_utc)
        grouped[forecast_date][ticker_code] = city

    return sorted(grouped.items(), key=lambda item: item[0])


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


class DiscoveryCheckpoint:
    """Track successful Kalshi discovery runs by city/date for the current UTC day."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._completed_on: dict[tuple[str, date], date] = {}

    def filter_pending(
        self,
        city_map: dict[str, City],
        *,
        forecast_date: date,
        checkpoint_day: date,
    ) -> dict[str, City]:
        with self._lock:
            return {
                city_code: city
                for city_code, city in city_map.items()
                if self._completed_on.get((city_code, forecast_date)) != checkpoint_day
            }

    def mark_completed(
        self,
        city_codes: list[str],
        *,
        forecast_date: date,
        checkpoint_day: date,
    ) -> None:
        with self._lock:
            for city_code in city_codes:
                self._completed_on[(city_code, forecast_date)] = checkpoint_day

    def mark_local_tomorrow(
        self,
        city_map: dict[str, City],
        *,
        now_utc: datetime,
    ) -> None:
        checkpoint_day = now_utc.date()
        with self._lock:
            for city_code, city in city_map.items():
                forecast_date = _get_city_local_tomorrow(city, now_utc=now_utc)
                self._completed_on[(city_code, forecast_date)] = checkpoint_day


def _weather_interval_env_var(source: WeatherSource) -> str:
    return f"WEATHER_{source}_POLL_INTERVAL_SECONDS"


def _parse_poll_interval_seconds(
    raw_value: str,
    *,
    env_var: str,
    fallback_seconds: int,
) -> int:
    try:
        interval_seconds = int(raw_value)
    except ValueError:
        logger.warning(
            "weather_poll_interval_invalid",
            env_var=env_var,
            raw_value=raw_value,
            fallback_seconds=fallback_seconds,
        )
        return fallback_seconds

    if not (_MIN_WEATHER_POLL_INTERVAL_SECONDS <= interval_seconds <= _MAX_WEATHER_POLL_INTERVAL_SECONDS):
        logger.warning(
            "weather_poll_interval_out_of_bounds",
            env_var=env_var,
            raw_value=raw_value,
            min_seconds=_MIN_WEATHER_POLL_INTERVAL_SECONDS,
            max_seconds=_MAX_WEATHER_POLL_INTERVAL_SECONDS,
            fallback_seconds=fallback_seconds,
        )
        return fallback_seconds

    return interval_seconds


def _get_weather_poll_interval_seconds(source: WeatherSource) -> int:
    default_env_var = "WEATHER_DEFAULT_POLL_INTERVAL_SECONDS"
    default_seconds = _DEFAULT_WEATHER_POLL_INTERVAL_SECONDS
    default_raw = os.environ.get(default_env_var)
    if default_raw is not None:
        default_seconds = _parse_poll_interval_seconds(
            default_raw,
            env_var=default_env_var,
            fallback_seconds=_DEFAULT_WEATHER_POLL_INTERVAL_SECONDS,
        )

    source_env_var = _weather_interval_env_var(source)
    source_raw = os.environ.get(source_env_var)
    if source_raw is None:
        return default_seconds

    return _parse_poll_interval_seconds(
        source_raw,
        env_var=source_env_var,
        fallback_seconds=default_seconds,
    )


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
    for forecast_date, batch_city_map in _group_cities_by_weather_target_date(city_map):
        run_weather_ingestion(
            client=client,
            city_map=batch_city_map,
            session_factory=get_session,
            forecast_date=forecast_date,
            run_id=run_id,
        )


def _kalshi_discovery_wrapper(
    *,
    kalshi_client: KalshiClient,
    city_map: dict[str, City],
    cycle_id_gen: CycleIdGenerator,
    discovery_checkpoint: DiscoveryCheckpoint | None = None,
    full_backfill: bool = False,
) -> None:
    run_id = cycle_id_gen.get()
    now_utc = _utc_now()
    # full_backfill=True passes forecast_date=None to discover all open
    # markets (not just tomorrow), catching multi-day-out markets that
    # Kalshi may open after the cold-start backfill.
    if full_backfill:
        completed = run_kalshi_discovery(
            kalshi_client=kalshi_client,
            city_map=city_map,
            session_factory=get_session,
            forecast_date=None,
            run_id=run_id,
        )
        if completed and discovery_checkpoint is not None:
            discovery_checkpoint.mark_local_tomorrow(city_map, now_utc=now_utc)
        return

    checkpoint_day = now_utc.date()
    for forecast_date, batch_city_map in _group_cities_by_kalshi_target_date(
        city_map,
        now_utc=now_utc,
    ):
        pending_city_map = batch_city_map
        if discovery_checkpoint is not None:
            pending_city_map = discovery_checkpoint.filter_pending(
                batch_city_map,
                forecast_date=forecast_date,
                checkpoint_day=checkpoint_day,
            )
            if not pending_city_map:
                logger.debug(
                    "kalshi_discovery_checkpoint_skip",
                    forecast_date=str(forecast_date),
                    city_count=len(batch_city_map),
                    run_id=run_id,
                )
                continue

        completed = run_kalshi_discovery(
            kalshi_client=kalshi_client,
            city_map=pending_city_map,
            session_factory=get_session,
            forecast_date=forecast_date,
            run_id=run_id,
        )
        if completed and discovery_checkpoint is not None:
            discovery_checkpoint.mark_completed(
                list(pending_city_map.keys()),
                forecast_date=forecast_date,
                checkpoint_day=checkpoint_day,
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
    now_utc = _utc_now()
    weather_batches = _group_cities_by_weather_target_date(city_map, now_utc=now_utc)
    forecast_dates = [
        str(forecast_date)
        for forecast_date, _batch_city_map in weather_batches
    ]
    logger.info(
        "run_once_start",
        run_id=run_id,
        forecast_dates=forecast_dates,
    )

    for client in weather_clients:
        for forecast_date, batch_city_map in weather_batches:
            run_weather_ingestion(
                client=client,
                city_map=batch_city_map,
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

    discovery_checkpoint = DiscoveryCheckpoint()

    # Cold-start backfill: discover all open markets (today + tomorrow)
    # so today's already-active markets aren't missed on fresh deployment.
    if kalshi_client is not None and not shutdown_event.is_set():
        startup_run_id = generate_correlation_id()
        logger.info("cold_start_discovery", run_id=startup_run_id)
        completed = run_kalshi_discovery(
            kalshi_client=kalshi_client,
            city_map=city_map,
            session_factory=get_session,
            forecast_date=None,
            run_id=startup_run_id,
        )
        if completed:
            discovery_checkpoint.mark_local_tomorrow(city_map, now_utc=_utc_now())

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
    weather_cycle_gens: dict[int, CycleIdGenerator] = {}

    # Weather jobs: one per source, run immediately, with per-source intervals.
    for client in weather_clients:
        interval_seconds = _get_weather_poll_interval_seconds(client.source)
        cycle_id_gen = weather_cycle_gens.setdefault(
            interval_seconds,
            CycleIdGenerator(interval_seconds=interval_seconds),
        )
        scheduler.add_job(  # type: ignore[reportUnknownMemberType]
            _weather_job_wrapper,
            "interval",
            seconds=interval_seconds,
            kwargs={"client": client, "city_map": city_map, "cycle_id_gen": cycle_id_gen},
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
            kwargs={
                "kalshi_client": kalshi_client,
                "city_map": city_map,
                "cycle_id_gen": two_hour_cycle,
                "discovery_checkpoint": discovery_checkpoint,
            },
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
                "discovery_checkpoint": discovery_checkpoint,
                "full_backfill": True,
            },
            id="kalshi_discovery_full",
            name="Kalshi full market discovery (backfill)",
            max_instances=1,
            # Cold-start already does a full backfill; defer first run by 24h
            next_run_time=now + timedelta(hours=24),
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
