"""
Kalshi market data ingestion jobs.

Three jobs running on different schedules:
1. Discovery (2h) — find weather bracket markets, upsert into DB
2. Snapshots (5m) — poll prices for near-term markets (24-48h window)
3. Settlements (2h) — check for newly settled markets, update status
"""

from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import delete, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from shared.config.logging import generate_correlation_id, get_logger
from shared.db.enums import MarketStatus
from shared.db.models import City, KalshiMarket, KalshiMarketSnapshot

from src.clients.kalshi import KalshiClient

logger = get_logger("kalshi-ingestion")


# ---------------------------------------------------------------------------
# 1. Market Discovery
# ---------------------------------------------------------------------------


def run_kalshi_discovery(
    *,
    kalshi_client: KalshiClient,
    city_map: dict[str, City],
    session_factory: Callable[[], AbstractContextManager[Session]],
    forecast_date: date | None = None,
    run_id: str,
) -> None:
    """Discover weather bracket markets and upsert into the database.

    Uses INSERT ON CONFLICT DO UPDATE on the unique ``ticker`` field.
    Existing markets get their status and bracket bounds updated.

    Args:
        kalshi_client: Kalshi API client.
        city_map: Dict of ticker_code → City ORM objects.
        session_factory: Callable returning a context-managed DB session.
        forecast_date: Optional date filter. If None, discovers all dates.
        run_id: Shared ID for the entire ingestion cycle.
    """
    correlation_id = generate_correlation_id()

    logger.info(
        "kalshi_discovery_start",
        run_id=run_id,
        correlation_id=correlation_id,
        forecast_date=str(forecast_date) if forecast_date else "all",
    )

    try:
        discovered = kalshi_client.discover_markets(
            city_codes=list(city_map.keys()),
            forecast_date=forecast_date,
            correlation_id=correlation_id,
        )
    except Exception as exc:
        logger.error(
            "kalshi_discovery_failed",
            error=str(exc),
            error_type=type(exc).__name__,
            correlation_id=correlation_id,
            run_id=run_id,
        )
        return

    upsert_count = 0
    skip_count = 0
    error_count = 0

    with session_factory() as session:
        for market in discovered:
            city = city_map.get(market.city_code)
            if city is None:
                skip_count += 1
                logger.debug(
                    "kalshi_market_unknown_city",
                    ticker=market.market_ticker,
                    city_code=market.city_code,
                    correlation_id=correlation_id,
                )
                continue

            try:
                forecast_dt = datetime(
                    market.forecast_date.year,
                    market.forecast_date.month,
                    market.forecast_date.day,
                    tzinfo=timezone.utc,
                )

                # SAVEPOINT per market so a single failure doesn't poison
                # the session and roll back the entire batch.
                with session.begin_nested():
                    stmt = pg_insert(KalshiMarket).values(
                        event_id=market.event_ticker,
                        market_id=market.market_ticker,
                        ticker=market.market_ticker,
                        city_id=city.id,
                        forecast_date=forecast_dt,
                        market_type=market.market_type,
                        bracket_low=market.bracket_low,
                        bracket_high=market.bracket_high,
                        is_edge_bracket=market.is_edge_bracket,
                        status=market.status,
                    ).on_conflict_do_update(
                        index_elements=["ticker"],
                        set_={
                            "status": market.status,
                            "bracket_low": market.bracket_low,
                            "bracket_high": market.bracket_high,
                            "is_edge_bracket": market.is_edge_bracket,
                        },
                    )
                    session.execute(stmt)

                upsert_count += 1

            except Exception as exc:
                error_count += 1
                logger.error(
                    "kalshi_discovery_market_error",
                    ticker=market.market_ticker,
                    error=str(exc),
                    error_type=type(exc).__name__,
                    correlation_id=correlation_id,
                    run_id=run_id,
                )

    logger.info(
        "kalshi_discovery_complete",
        run_id=run_id,
        correlation_id=correlation_id,
        upserted=upsert_count,
        skipped=skip_count,
        errors=error_count,
        total_discovered=len(discovered),
    )


# ---------------------------------------------------------------------------
# 2. Price Snapshots
# ---------------------------------------------------------------------------


def run_kalshi_snapshots(
    *,
    kalshi_client: KalshiClient,
    session_factory: Callable[[], AbstractContextManager[Session]],
    run_id: str,
) -> None:
    """Fetch price snapshots for near-term markets (24-48h window).

    Only polls markets with ``forecast_date`` in [today, today+2d) that
    are still active (Decision #14). Inserts KalshiMarketSnapshot rows.

    Args:
        kalshi_client: Kalshi API client.
        session_factory: Callable returning a context-managed DB session.
        run_id: Shared ID for the entire ingestion cycle.
    """
    correlation_id = generate_correlation_id()

    # Build the 24-48h window
    now = datetime.now(timezone.utc)
    window_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    window_end = window_start + timedelta(days=2)

    # Query active markets in the window
    with session_factory() as session:
        active_markets = (
            session.execute(
                select(KalshiMarket).where(
                    KalshiMarket.status == MarketStatus.ACTIVE,
                    KalshiMarket.forecast_date >= window_start,
                    KalshiMarket.forecast_date < window_end,
                )
            )
            .scalars()
            .all()
        )

    if not active_markets:
        logger.debug(
            "kalshi_snapshots_no_active_markets",
            run_id=run_id,
            correlation_id=correlation_id,
        )
        return

    # Map ticker → DB primary key for linking snapshots
    ticker_to_market_id: dict[str, object] = {
        m.ticker: m.id for m in active_markets
    }
    tickers = list(ticker_to_market_id.keys())

    logger.info(
        "kalshi_snapshots_start",
        run_id=run_id,
        correlation_id=correlation_id,
        ticker_count=len(tickers),
    )

    try:
        snapshots = kalshi_client.fetch_snapshots(
            tickers=tickers,
            correlation_id=correlation_id,
        )
    except Exception as exc:
        logger.error(
            "kalshi_snapshots_fetch_failed",
            error=str(exc),
            error_type=type(exc).__name__,
            correlation_id=correlation_id,
            run_id=run_id,
        )
        return

    insert_count = 0
    skip_count = 0

    with session_factory() as session:
        for snapshot in snapshots:
            db_market_id = ticker_to_market_id.get(snapshot.ticker)
            if db_market_id is None:
                skip_count += 1
                logger.debug(
                    "kalshi_snapshot_unknown_ticker",
                    ticker=snapshot.ticker,
                    correlation_id=correlation_id,
                )
                continue

            session.add(
                KalshiMarketSnapshot(
                    market_id=db_market_id,  # type: ignore[arg-type]
                    timestamp=snapshot.timestamp,
                    yes_bid=snapshot.yes_bid,
                    yes_ask=snapshot.yes_ask,
                    no_bid=snapshot.no_bid,
                    no_ask=snapshot.no_ask,
                    volume=snapshot.volume,
                    open_interest=snapshot.open_interest,
                )
            )
            insert_count += 1

    logger.info(
        "kalshi_snapshots_complete",
        run_id=run_id,
        correlation_id=correlation_id,
        inserted=insert_count,
        skipped=skip_count,
        total_fetched=len(snapshots),
    )


# ---------------------------------------------------------------------------
# 3. Settlement Tracking
# ---------------------------------------------------------------------------


def run_kalshi_settlements(
    *,
    kalshi_client: KalshiClient,
    session_factory: Callable[[], AbstractContextManager[Session]],
    run_id: str,
) -> None:
    """Check for newly settled markets and update their status.

    Queries active markets with past forecast dates, then checks the
    Kalshi API for settlement outcomes.

    Args:
        kalshi_client: Kalshi API client.
        session_factory: Callable returning a context-managed DB session.
        run_id: Shared ID for the entire ingestion cycle.
    """
    correlation_id = generate_correlation_id()
    now = datetime.now(timezone.utc)

    # Find active markets that should have settled (forecast_date in the past)
    with session_factory() as session:
        unsettled = (
            session.execute(
                select(KalshiMarket).where(
                    KalshiMarket.status == MarketStatus.ACTIVE,
                    KalshiMarket.forecast_date < now,
                )
            )
            .scalars()
            .all()
        )

    if not unsettled:
        logger.debug(
            "kalshi_settlements_none_pending",
            run_id=run_id,
            correlation_id=correlation_id,
        )
        return

    tickers = [m.ticker for m in unsettled]

    logger.info(
        "kalshi_settlements_start",
        run_id=run_id,
        correlation_id=correlation_id,
        ticker_count=len(tickers),
    )

    try:
        settled_markets = kalshi_client.check_settlements(
            tickers=tickers,
            correlation_id=correlation_id,
        )
    except Exception as exc:
        logger.error(
            "kalshi_settlements_check_failed",
            error=str(exc),
            error_type=type(exc).__name__,
            correlation_id=correlation_id,
            run_id=run_id,
        )
        return

    update_count = 0
    error_count = 0

    with session_factory() as session:
        for settled in settled_markets:
            try:
                settlement_val = (
                    float(settled.settlement_value)
                    if settled.settlement_value is not None
                    else None
                )

                # SAVEPOINT per market so a single failure doesn't poison
                # the session and roll back the entire batch.
                with session.begin_nested():
                    session.execute(
                        update(KalshiMarket)
                        .where(KalshiMarket.ticker == settled.ticker)
                        .values(
                            status=settled.final_status,
                            settlement_value=settlement_val,
                            # Core UPDATE bypasses ORM onupdate, so set explicitly
                            updated_at=datetime.now(timezone.utc),
                        )
                    )

                update_count += 1

            except Exception as exc:
                error_count += 1
                logger.error(
                    "kalshi_settlement_update_error",
                    ticker=settled.ticker,
                    error=str(exc),
                    error_type=type(exc).__name__,
                    correlation_id=correlation_id,
                    run_id=run_id,
                )

    logger.info(
        "kalshi_settlements_complete",
        run_id=run_id,
        correlation_id=correlation_id,
        updated=update_count,
        errors=error_count,
        total_checked=len(tickers),
        total_settled=len(settled_markets),
    )


# ---------------------------------------------------------------------------
# 4. Snapshot Retention Cleanup
# ---------------------------------------------------------------------------


def run_kalshi_snapshot_cleanup(
    *,
    session_factory: Callable[[], AbstractContextManager[Session]],
    retention_days: int = 30,
    run_id: str,
) -> None:
    """Delete KalshiMarketSnapshot rows older than the retention window.

    Prevents unbounded accumulation of 5-minute snapshot data.

    Args:
        session_factory: Callable returning a context-managed DB session.
        retention_days: Number of days of snapshots to retain. Default 30.
        run_id: Shared ID for the entire ingestion cycle.
    """
    correlation_id = generate_correlation_id()
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)

    logger.info(
        "kalshi_snapshot_cleanup_start",
        run_id=run_id,
        correlation_id=correlation_id,
        retention_days=retention_days,
        cutoff=str(cutoff),
    )

    try:
        with session_factory() as session:
            result = session.execute(
                delete(KalshiMarketSnapshot).where(
                    KalshiMarketSnapshot.timestamp < cutoff,
                )
            )
            deleted_count = result.rowcount  # type: ignore[union-attr]

    except Exception as exc:
        logger.error(
            "kalshi_snapshot_cleanup_failed",
            error=str(exc),
            error_type=type(exc).__name__,
            correlation_id=correlation_id,
            run_id=run_id,
        )
        return

    logger.info(
        "kalshi_snapshot_cleanup_complete",
        run_id=run_id,
        correlation_id=correlation_id,
        deleted=deleted_count,
    )
