"""Integration tests for the prediction engine pipeline.

These tests require a running PostgreSQL instance. Skipped by default.
Run with: pytest -m integration

Uses the db_session fixture from conftest.py which provides a
rollback-per-test transaction.

Scenarios:
1. Happy path: forecasts + markets → prediction + recommendation + paper trade
2. No recommendation: model and market agree (gap < threshold)
3. Partial data: fewer sources → prediction with higher std, conservative
4. No markets: forecasts exist but no Kalshi markets → no predictions
5. Multiple recommendations: several brackets mispriced → one recommendation per market
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from src.config import PredictionConfig
from src.engine.prediction import run_prediction_cycle
from src.engine.recommendation import run_recommendation_cycle

from shared.db.enums import MarketStatus, MarketType
from shared.db.models import (
    City,
    KalshiMarket,
    KalshiMarketSnapshot,
    PaperTradeFixed,
    Prediction,
    Recommendation,
    WeatherForecast,
)

pytestmark = pytest.mark.integration

_FORECAST_DATE = datetime(2026, 3, 25, tzinfo=UTC)
_NOW = datetime(2026, 3, 24, 18, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed_city(
    session: Session,
    code: str = "NYC",
    *,
    name: str | None = None,
) -> City:
    city = City(
        name=name or f"Test {code}",
        kalshi_ticker_prefix=code,
        nws_station_id=f"K{code}",
        timezone="America/New_York",
        lat=Decimal("40.7"),
        lon=Decimal("-74.0"),
    )
    session.add(city)
    session.flush()
    return city


def _seed_forecast(
    session: Session,
    city: City,
    *,
    source: str,
    temp_high: Decimal,
    temp_low: Decimal,
    forecast_date: datetime = _FORECAST_DATE,
) -> WeatherForecast:
    forecast = WeatherForecast(
        source=source,
        city_id=city.id,
        forecast_date=forecast_date,
        issued_at=datetime(2026, 3, 24, 12, 0, tzinfo=UTC),
        temp_high=temp_high,
        temp_low=temp_low,
    )
    session.add(forecast)
    session.flush()
    return forecast


def _seed_market(
    session: Session,
    city: City,
    *,
    ticker: str,
    bracket_low: Decimal | None,
    bracket_high: Decimal | None,
    market_type: MarketType = MarketType.HIGH,
    forecast_date: datetime = _FORECAST_DATE,
    is_edge: bool = False,
) -> KalshiMarket:
    market = KalshiMarket(
        event_id=f"KX{market_type.value}{city.kalshi_ticker_prefix}-26MAR25",
        market_id=ticker,
        ticker=ticker,
        city_id=city.id,
        forecast_date=forecast_date,
        market_type=market_type,
        bracket_low=bracket_low,
        bracket_high=bracket_high,
        is_edge_bracket=is_edge,
        status=MarketStatus.ACTIVE,
    )
    session.add(market)
    session.flush()
    return market


def _seed_snapshot(
    session: Session,
    market: KalshiMarket,
    *,
    yes_ask: Decimal = Decimal("0.40"),
    no_ask: Decimal = Decimal("0.65"),
    volume: int = 100,
) -> KalshiMarketSnapshot:
    snapshot = KalshiMarketSnapshot(
        market_id=market.id,
        timestamp=datetime(2026, 3, 24, 17, 0, tzinfo=UTC),
        yes_bid=yes_ask - Decimal("0.02"),
        yes_ask=yes_ask,
        no_bid=no_ask - Decimal("0.02"),
        no_ask=no_ask,
        volume=volume,
        open_interest=200,
    )
    session.add(snapshot)
    session.flush()
    return snapshot


def _seed_full_bracket_set(
    session: Session,
    city: City,
    *,
    market_type: MarketType = MarketType.HIGH,
    forecast_date: datetime = _FORECAST_DATE,
) -> list[KalshiMarket]:
    """Create a full set of brackets covering (-inf, inf) like real Kalshi data.

    Brackets: (-inf, 55), [55, 60), [60, 65), [65, 70), [70, 75), [75, 80), [80, inf)
    """
    prefix = f"KX{market_type.value}{city.kalshi_ticker_prefix}-26MAR25"
    brackets: list[tuple[Decimal | None, Decimal | None, str, bool]] = [
        (None, Decimal("55.0000"), f"{prefix}-TU55", True),
        (Decimal("55.0000"), Decimal("60.0000"), f"{prefix}-T55", False),
        (Decimal("60.0000"), Decimal("65.0000"), f"{prefix}-T60", False),
        (Decimal("65.0000"), Decimal("70.0000"), f"{prefix}-T65", False),
        (Decimal("70.0000"), Decimal("75.0000"), f"{prefix}-T70", False),
        (Decimal("75.0000"), Decimal("80.0000"), f"{prefix}-T75", False),
        (Decimal("80.0000"), None, f"{prefix}-TO80", True),
    ]
    markets = []
    for low, high, ticker, is_edge in brackets:
        m = _seed_market(
            session,
            city,
            ticker=ticker,
            bracket_low=low,
            bracket_high=high,
            market_type=market_type,
            forecast_date=forecast_date,
            is_edge=is_edge,
        )
        markets.append(m)
    return markets


def _config(**overrides: object) -> PredictionConfig:
    defaults = {
        "gap_threshold": Decimal("0.15"),
        "min_ev_threshold": Decimal("0.05"),
        "min_sources_required": 2,
        "std_dev_floor": Decimal("1.50"),
    }
    defaults.update(overrides)
    return PredictionConfig(**defaults)  # type: ignore[arg-type]


def _session_factory(session: Session):
    """Create a fresh Session per engine cycle with savepoint isolation.

    Each call yields a new Session bound to the same connection but using its
    own SAVEPOINT, preventing ORM identity-map leakage between prediction and
    recommendation cycles while preserving rollback-per-test isolation.
    """
    bind = session.get_bind()

    @contextmanager
    def _factory():
        cycle_session = Session(
            bind=bind,
            join_transaction_mode="create_savepoint",
        )
        try:
            yield cycle_session
            cycle_session.commit()  # release savepoint
        except Exception:
            cycle_session.rollback()
            raise
        finally:
            cycle_session.close()

    return _factory


# ---------------------------------------------------------------------------
# Scenario 1: Happy path
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_full_pipeline_creates_prediction_recommendation_and_trade(
        self, db_session: Session
    ) -> None:
        """4 sources, 1 city, 1 market bracket → prediction + recommendation + paper trade."""
        city = _seed_city(db_session)

        # Seed 4 weather sources with temps clustered around 72°F
        for source, high, low in [
            ("NWS", Decimal("72.0"), Decimal("55.0")),
            ("visual_crossing", Decimal("73.0"), Decimal("56.0")),
            ("pirate_weather", Decimal("71.0"), Decimal("54.0")),
            ("openweather", Decimal("72.5"), Decimal("55.5")),
        ]:
            _seed_forecast(
                db_session, city, source=source, temp_high=high, temp_low=low
            )

        # Create full bracket set covering (-inf, inf)
        markets = _seed_full_bracket_set(db_session, city)

        # Snapshot every bracket. Misprice the [65, 70) bracket at 0.40 —
        # model predicts ~72°F mean so probability for that bracket is low,
        # creating a gap the engine should catch.
        for m in markets:
            yes_ask = Decimal("0.15")
            if m.bracket_low == Decimal("65.0000"):
                yes_ask = Decimal("0.40")  # mispriced
            elif m.bracket_low == Decimal("70.0000"):
                yes_ask = Decimal("0.45")  # near the mean
            _seed_snapshot(
                db_session, m, yes_ask=yes_ask, no_ask=Decimal("0.88")
            )

        config = _config()
        factory = _session_factory(db_session)

        # Run prediction cycle
        pred_stats = run_prediction_cycle(config, factory)
        db_session.flush()

        assert pred_stats["groups_seen"] == 1
        assert pred_stats["predictions_upserted"] == 1
        assert pred_stats["groups_skipped"] == 0

        # Verify prediction was created
        prediction = db_session.execute(
            select(Prediction).where(
                Prediction.city_id == city.id,
                Prediction.forecast_date == _FORECAST_DATE,
            )
        ).scalar_one()

        assert prediction.model_version == config.model_version
        assert prediction.predicted_temp is not None
        assert prediction.std_dev is not None
        assert prediction.probability_distribution is not None
        assert "brackets" in prediction.probability_distribution
        assert "mean" in prediction.probability_distribution

        # Run recommendation cycle
        rec_stats = run_recommendation_cycle(
            config, factory, now_fn=lambda: _NOW
        )
        db_session.flush()

        assert rec_stats["markets_seen"] == 7  # full bracket set

        # Check that at least one recommendation was created
        rec_count = db_session.scalar(
            select(func.count()).select_from(Recommendation)
        )
        trade_count = db_session.scalar(
            select(func.count()).select_from(PaperTradeFixed)
        )

        assert rec_stats["recommendations_created"] >= 1
        assert rec_count >= 1
        assert rec_count == trade_count  # 1:1 recommendation to trade

        recommendations = db_session.execute(
            select(Recommendation)
        ).scalars().all()
        for rec in recommendations:
            assert rec.prediction_id == prediction.id
            assert rec.risk_score is not None
            assert rec.risk_factors is not None
            assert len(rec.risk_factors) == 6

        trades = db_session.execute(
            select(PaperTradeFixed)
        ).scalars().all()
        for trade in trades:
            assert trade.contracts_qty == 1


# ---------------------------------------------------------------------------
# Scenario 2: No recommendation (model agrees with market)
# ---------------------------------------------------------------------------


class TestNoRecommendation:
    def test_model_agrees_with_market_no_recommendation(
        self, db_session: Session
    ) -> None:
        """When model probability closely matches market price, no recommendation."""
        city = _seed_city(db_session)

        # Temperatures tightly clustered at 72°F
        for source, high, low in [
            ("NWS", Decimal("72.0"), Decimal("55.0")),
            ("visual_crossing", Decimal("72.0"), Decimal("55.0")),
            ("pirate_weather", Decimal("72.0"), Decimal("55.0")),
        ]:
            _seed_forecast(
                db_session, city, source=source, temp_high=high, temp_low=low
            )

        # Full bracket set. Price each bracket so that neither BUY_YES nor
        # BUY_NO creates a gap exceeding the threshold.
        # Model predicts ~72°F with tight std, so most probability is in
        # the [70, 75) bracket. Price brackets near their true probabilities.
        markets = _seed_full_bracket_set(db_session, city)
        for m in markets:
            # Set yes_ask very low (0.02) and no_ask very high (0.99)
            # for extreme brackets so the gap is tiny (model prob ≈ 0
            # vs market price 0.02 → gap ≈ -0.02, no trigger)
            # For the central bracket, price near true probability.
            if m.bracket_low == Decimal("70.0000"):
                # Model prob ≈ 0.50 for [70,75), price close
                yes_ask = Decimal("0.48")
                no_ask = Decimal("0.55")
            else:
                # Far brackets: model prob near 0, price near 0
                yes_ask = Decimal("0.02")
                no_ask = Decimal("0.99")
            _seed_snapshot(
                db_session, m, yes_ask=yes_ask, no_ask=no_ask
            )

        # Use a very high gap threshold so no recommendation triggers
        config = _config(gap_threshold=Decimal("0.95"))
        factory = _session_factory(db_session)

        run_prediction_cycle(config, factory)
        db_session.flush()

        rec_stats = run_recommendation_cycle(
            config, factory, now_fn=lambda: _NOW
        )
        db_session.flush()

        assert rec_stats["markets_seen"] == 7  # full bracket set
        # Should be skipped (gap too small or thresholds not met)
        assert rec_stats["recommendations_created"] == 0

        rec_count = db_session.scalar(
            select(func.count()).select_from(Recommendation)
        )
        assert rec_count == 0


# ---------------------------------------------------------------------------
# Scenario 3: Partial data (fewer sources)
# ---------------------------------------------------------------------------


class TestPartialData:
    def test_two_sources_creates_prediction_with_higher_std(
        self, db_session: Session
    ) -> None:
        """2 of 4 sources → prediction still created but with wider std_dev."""
        city = _seed_city(db_session)

        # Only 2 sources (min_sources_required=2)
        _seed_forecast(
            db_session,
            city,
            source="NWS",
            temp_high=Decimal("70.0"),
            temp_low=Decimal("53.0"),
        )
        _seed_forecast(
            db_session,
            city,
            source="visual_crossing",
            temp_high=Decimal("74.0"),
            temp_low=Decimal("57.0"),
        )

        markets = _seed_full_bracket_set(db_session, city)
        for m in markets:
            _seed_snapshot(db_session, m)

        config = _config(min_sources_required=2)
        factory = _session_factory(db_session)

        pred_stats = run_prediction_cycle(config, factory)
        db_session.flush()

        assert pred_stats["predictions_upserted"] == 1

        prediction = db_session.execute(
            select(Prediction).where(Prediction.city_id == city.id)
        ).scalar_one()

        # With only 2 sources 4°F apart, std_dev should be >= floor
        assert prediction.std_dev >= config.std_dev_floor
        assert prediction.probability_distribution is not None

    def test_one_source_skipped(self, db_session: Session) -> None:
        """Only 1 source when min=2 → group is skipped entirely."""
        city = _seed_city(db_session, "CHI")

        _seed_forecast(
            db_session,
            city,
            source="NWS",
            temp_high=Decimal("65.0"),
            temp_low=Decimal("48.0"),
        )

        _seed_full_bracket_set(db_session, city)

        config = _config(min_sources_required=2)
        factory = _session_factory(db_session)

        pred_stats = run_prediction_cycle(config, factory)
        db_session.flush()

        assert pred_stats["groups_skipped"] == 1
        assert pred_stats["predictions_upserted"] == 0

        count = db_session.scalar(
            select(func.count()).select_from(Prediction)
        )
        assert count == 0


# ---------------------------------------------------------------------------
# Scenario 4: No markets
# ---------------------------------------------------------------------------


class TestNoMarkets:
    def test_forecasts_exist_but_no_markets(self, db_session: Session) -> None:
        """Forecasts without any active markets → no predictions at all."""
        city = _seed_city(db_session)

        for source, high, low in [
            ("NWS", Decimal("72.0"), Decimal("55.0")),
            ("visual_crossing", Decimal("73.0"), Decimal("56.0")),
        ]:
            _seed_forecast(
                db_session, city, source=source, temp_high=high, temp_low=low
            )

        # No markets seeded
        config = _config()
        factory = _session_factory(db_session)

        pred_stats = run_prediction_cycle(config, factory)
        db_session.flush()

        # No active market groups means nothing to predict
        assert pred_stats["groups_seen"] == 0
        assert pred_stats["predictions_upserted"] == 0

        count = db_session.scalar(
            select(func.count()).select_from(Prediction)
        )
        assert count == 0


# ---------------------------------------------------------------------------
# Scenario 5: Multiple brackets mispriced
# ---------------------------------------------------------------------------


class TestMultipleRecommendations:
    def test_multiple_mispriced_brackets_each_get_recommendation(
        self, db_session: Session
    ) -> None:
        """Several brackets mispriced → one recommendation per qualifying market."""
        city = _seed_city(db_session)

        # Model predicts ~72°F mean
        for source, high, low in [
            ("NWS", Decimal("72.0"), Decimal("55.0")),
            ("visual_crossing", Decimal("73.0"), Decimal("56.0")),
            ("pirate_weather", Decimal("71.0"), Decimal("54.0")),
        ]:
            _seed_forecast(
                db_session, city, source=source, temp_high=high, temp_low=low
            )

        # Full bracket set with all brackets mispriced at 0.40 — far brackets
        # have low model probability, creating large gaps
        markets = _seed_full_bracket_set(db_session, city)
        for m in markets:
            _seed_snapshot(
                db_session,
                m,
                yes_ask=Decimal("0.40"),
                no_ask=Decimal("0.65"),
                volume=50,
            )

        config = _config(
            gap_threshold=Decimal("0.10"),
            min_ev_threshold=Decimal("0.03"),
        )
        factory = _session_factory(db_session)

        pred_stats = run_prediction_cycle(config, factory)
        db_session.flush()

        assert pred_stats["groups_seen"] == 1
        assert pred_stats["predictions_upserted"] == 1

        rec_stats = run_recommendation_cycle(
            config, factory, now_fn=lambda: _NOW
        )
        db_session.flush()

        assert rec_stats["markets_seen"] == 7  # full bracket set

        # At least some brackets should have been mispriced enough to trigger
        rec_count = db_session.scalar(
            select(func.count()).select_from(Recommendation)
        )
        trade_count = db_session.scalar(
            select(func.count()).select_from(PaperTradeFixed)
        )

        # Each recommendation should have a paper trade
        assert rec_count == trade_count
        assert rec_count >= 1  # At least one mispriced bracket

        # Verify each recommendation links back to the same prediction
        prediction = db_session.execute(
            select(Prediction).where(Prediction.city_id == city.id)
        ).scalar_one()

        recommendations = db_session.execute(
            select(Recommendation)
        ).scalars().all()

        for rec in recommendations:
            assert rec.prediction_id == prediction.id
            assert rec.risk_score is not None
            assert rec.risk_factors is not None
            assert rec.gap is not None
            assert rec.expected_value is not None

        # Verify all paper trades have correct entry prices
        trades = db_session.execute(
            select(PaperTradeFixed)
        ).scalars().all()

        for trade in trades:
            assert trade.entry_price is not None
            assert Decimal("0") < trade.entry_price < Decimal("1")
            assert trade.contracts_qty == 1


# ---------------------------------------------------------------------------
# Unique constraint tests
# ---------------------------------------------------------------------------


class TestUniqueConstraints:
    def test_prediction_upsert_is_idempotent(
        self, db_session: Session
    ) -> None:
        """Running prediction cycle twice updates the same row, not a duplicate."""
        city = _seed_city(db_session)

        for source, high, low in [
            ("NWS", Decimal("72.0"), Decimal("55.0")),
            ("visual_crossing", Decimal("73.0"), Decimal("56.0")),
        ]:
            _seed_forecast(
                db_session, city, source=source, temp_high=high, temp_low=low
            )

        markets = _seed_full_bracket_set(db_session, city)
        for m in markets:
            _seed_snapshot(db_session, m)

        config = _config()
        factory = _session_factory(db_session)

        # Run twice
        run_prediction_cycle(config, factory)
        db_session.flush()
        run_prediction_cycle(config, factory)
        db_session.flush()

        # Should still be exactly 1 prediction
        count = db_session.scalar(
            select(func.count()).select_from(Prediction)
        )
        assert count == 1

    def test_recommendation_upsert_is_idempotent(
        self, db_session: Session
    ) -> None:
        """Running recommendation cycle twice reuses existing recommendation."""
        city = _seed_city(db_session)

        for source, high, low in [
            ("NWS", Decimal("72.0"), Decimal("55.0")),
            ("visual_crossing", Decimal("73.0"), Decimal("56.0")),
            ("pirate_weather", Decimal("71.0"), Decimal("54.0")),
        ]:
            _seed_forecast(
                db_session, city, source=source, temp_high=high, temp_low=low
            )

        markets = _seed_full_bracket_set(db_session, city)
        for m in markets:
            _seed_snapshot(
                db_session,
                m,
                yes_ask=Decimal("0.40"),
                no_ask=Decimal("0.65"),
            )

        config = _config(
            gap_threshold=Decimal("0.10"),
            min_ev_threshold=Decimal("0.03"),
        )
        factory = _session_factory(db_session)

        run_prediction_cycle(config, factory)
        db_session.flush()

        first_stats = run_recommendation_cycle(config, factory, now_fn=lambda: _NOW)
        db_session.flush()
        first_rec_count = db_session.scalar(
            select(func.count()).select_from(Recommendation)
        )

        second_stats = run_recommendation_cycle(config, factory, now_fn=lambda: _NOW)
        db_session.flush()
        second_rec_count = db_session.scalar(
            select(func.count()).select_from(Recommendation)
        )

        # Count should not grow
        assert first_rec_count == second_rec_count
        # Second run should reuse, not create
        assert first_stats["recommendations_created"] >= 1
        assert second_stats["recommendations_created"] == 0
        assert second_stats["recommendations_reused"] >= 1
