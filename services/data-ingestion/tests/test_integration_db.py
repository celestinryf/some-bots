"""Integration tests for database write operations.

These tests require a running PostgreSQL instance. Skipped by default.
Run with: pytest -m integration

Uses the db_session fixture from conftest.py which provides a
rollback-per-test transaction.
"""

import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import CursorResult, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from shared.db.enums import MarketStatus, MarketType
from shared.db.models import City, KalshiMarket, KalshiMarketSnapshot, WeatherForecast

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed_city(session: Session, code: str = "NYC") -> City:
    """Insert a test city and return it."""
    city = City(
        name=f"Test {code}",
        kalshi_ticker_prefix=code,
        nws_station_id=f"K{code}",
        timezone="America/New_York",
        lat=40.7,
        lon=-74.0,
    )
    session.add(city)
    session.flush()
    return city


def _seed_market(
    session: Session,
    city: City,
    ticker: str = "KXHIGHNYC-26MAR17-T72",
) -> KalshiMarket:
    """Insert a test Kalshi market and return it."""
    market = KalshiMarket(
        event_id="KXHIGHNYC-26MAR17",
        market_id=ticker,
        ticker=ticker,
        city_id=city.id,
        forecast_date=datetime(2026, 3, 17, tzinfo=UTC),
        market_type=MarketType.HIGH,
        bracket_low=72.0,
        bracket_high=73.0,
        is_edge_bracket=False,
        status=MarketStatus.ACTIVE,
    )
    session.add(market)
    session.flush()
    return market


# ---------------------------------------------------------------------------
# Weather forecast dedup tests
# ---------------------------------------------------------------------------


class TestWeatherForecastDedup:
    def test_insert_on_conflict_do_nothing(self, db_session: Session) -> None:
        """Duplicate (source, city_id, forecast_date, issued_at) is silently skipped."""
        city = _seed_city(db_session)

        values = {
            "source": "NWS",
            "city_id": city.id,
            "forecast_date": datetime(2026, 3, 17, tzinfo=UTC),
            "issued_at": datetime(2026, 3, 16, 14, 0, tzinfo=UTC),
            "temp_high": 72.0,
            "temp_low": 55.0,
        }

        # First insert
        stmt1 = pg_insert(WeatherForecast).values(**values).on_conflict_do_nothing(
            constraint="uq_forecast_dedup"
        )
        result1: CursorResult[tuple[()]] = db_session.execute(stmt1)  # type: ignore[assignment]
        db_session.flush()
        assert result1.rowcount == 1

        # Second insert — same values, should be skipped
        stmt2 = pg_insert(WeatherForecast).values(**values).on_conflict_do_nothing(
            constraint="uq_forecast_dedup"
        )
        result2: CursorResult[tuple[()]] = db_session.execute(stmt2)  # type: ignore[assignment]
        db_session.flush()
        assert result2.rowcount == 0

        # Only 1 row in DB
        count = db_session.scalar(
            select(func.count()).select_from(WeatherForecast)
        )
        assert count == 1

    def test_different_issued_at_creates_two_rows(self, db_session: Session) -> None:
        """Same source/city/date but different issued_at → 2 rows."""
        city = _seed_city(db_session)

        base = {
            "source": "NWS",
            "city_id": city.id,
            "forecast_date": datetime(2026, 3, 17, tzinfo=UTC),
            "temp_high": 72.0,
            "temp_low": 55.0,
        }

        stmt1 = pg_insert(WeatherForecast).values(
            **base,
            issued_at=datetime(2026, 3, 16, 14, 0, tzinfo=UTC),
        ).on_conflict_do_nothing(constraint="uq_forecast_dedup")
        db_session.execute(stmt1)

        stmt2 = pg_insert(WeatherForecast).values(
            **base,
            issued_at=datetime(2026, 3, 16, 16, 0, tzinfo=UTC),
        ).on_conflict_do_nothing(constraint="uq_forecast_dedup")
        db_session.execute(stmt2)
        db_session.flush()

        count = db_session.scalar(
            select(func.count()).select_from(WeatherForecast)
        )
        assert count == 2


# ---------------------------------------------------------------------------
# Kalshi market upsert tests
# ---------------------------------------------------------------------------


class TestKalshiMarketUpsert:
    def test_insert_then_update_same_ticker(self, db_session: Session) -> None:
        """First insert creates, second with same ticker updates fields."""
        city = _seed_city(db_session)
        ticker = "KXHIGHNYC-26MAR17-T72"

        values = {
            "event_id": "KXHIGHNYC-26MAR17",
            "market_id": ticker,
            "ticker": ticker,
            "city_id": city.id,
            "forecast_date": datetime(2026, 3, 17, tzinfo=UTC),
            "market_type": MarketType.HIGH,
            "bracket_low": 72.0,
            "bracket_high": 73.0,
            "is_edge_bracket": False,
            "status": MarketStatus.ACTIVE,
        }

        # Insert
        stmt1 = pg_insert(KalshiMarket).values(**values).on_conflict_do_update(
            index_elements=["ticker"],
            set_={"status": MarketStatus.ACTIVE, "bracket_low": 72.0},
        )
        db_session.execute(stmt1)
        db_session.flush()

        # Update — change status
        stmt2 = pg_insert(KalshiMarket).values(**values).on_conflict_do_update(
            index_elements=["ticker"],
            set_={"status": MarketStatus.SETTLED, "bracket_low": 72.0},
        )
        db_session.execute(stmt2)
        db_session.flush()

        # Still 1 row
        count = db_session.scalar(
            select(func.count()).select_from(KalshiMarket)
        )
        assert count == 1

        # Status was updated
        market = db_session.execute(
            select(KalshiMarket).where(KalshiMarket.ticker == ticker)
        ).scalar_one()
        assert market.status == MarketStatus.SETTLED


# ---------------------------------------------------------------------------
# Kalshi snapshot FK tests
# ---------------------------------------------------------------------------


class TestKalshiSnapshotFK:
    def test_insert_snapshot_linked_to_market(self, db_session: Session) -> None:
        city = _seed_city(db_session)
        market = _seed_market(db_session, city)

        snapshot = KalshiMarketSnapshot(
            market_id=market.id,
            timestamp=datetime.now(UTC),
            yes_bid=Decimal("0.54"),
            yes_ask=Decimal("0.56"),
            no_bid=Decimal("0.44"),
            no_ask=Decimal("0.46"),
            volume=100,
            open_interest=200,
        )
        db_session.add(snapshot)
        db_session.flush()

        count = db_session.scalar(
            select(func.count()).select_from(KalshiMarketSnapshot)
        )
        assert count == 1

    def test_snapshot_invalid_fk_raises(self, db_session: Session) -> None:
        """FK constraint: snapshot for nonexistent market_id raises."""
        snapshot = KalshiMarketSnapshot(
            market_id=uuid.uuid4(),  # Doesn't exist
            timestamp=datetime.now(UTC),
            yes_bid=Decimal("0.50"),
        )
        db_session.add(snapshot)

        with pytest.raises(Exception):  # IntegrityError
            db_session.flush()
