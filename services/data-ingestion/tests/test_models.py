"""
Database model integration tests.

Tests run against a real PostgreSQL instance — no mocks.
Each test uses a rolled-back transaction (db_session fixture).

Coverage: all 13 tables + join table, UUID generation, defaults,
constraints, relationships, enums, JSONB, Numeric precision, and indexes.
"""

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import Engine, inspect, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from shared.config.settings import Settings
from shared.db.enums import (
    Direction,
    MarketStatus,
    MarketType,
    SettlementOutcome,
    SizingStrategy,
    UserRole,
)
from shared.db.models import (
    City,
    EmailLog,
    KalshiMarket,
    KalshiMarketSnapshot,
    PaperPortfolio,
    PaperTradeFixed,
    PaperTradePortfolio,
    Prediction,
    Recommendation,
    RefreshToken,
    User,
    UserTrade,
    WeatherForecast,
)

pytestmark = pytest.mark.db


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_city(session: Session, *, code: str = "TST", name: str = "Test City", station: str = "KTST",
               tz: str = "America/New_York", lat: float = 40.0, lon: float = -74.0) -> City:
    city = City(
        name=name,
        kalshi_ticker_prefix=code,
        nws_station_id=station,
        timezone=tz,
        lat=lat,
        lon=lon,
    )
    session.add(city)
    session.flush()
    return city


def _make_market(session: Session, city: City, *, ticker: str | None = None, forecast_date: datetime | None = None) -> KalshiMarket:
    market = KalshiMarket(
        event_id="evt_001",
        market_id="mkt_001",
        ticker=ticker or f"KXHIGH{city.kalshi_ticker_prefix}-{uuid.uuid4().hex[:6]}",
        city_id=city.id,
        forecast_date=forecast_date or datetime.now(timezone.utc),
        market_type=MarketType.HIGH,
    )
    session.add(market)
    session.flush()
    return market


def _make_prediction(session: Session, city: City) -> Prediction:
    pred = Prediction(
        city_id=city.id,
        forecast_date=datetime.now(timezone.utc),
        market_type=MarketType.HIGH,
        model_version="tier1_v1",
        predicted_temp=72.5,
        std_dev=2.3,
        probability_distribution={"70-72": 0.3, "72-74": 0.5, "74-76": 0.2},
    )
    session.add(pred)
    session.flush()
    return pred


def _make_recommendation(session: Session, city: City) -> Recommendation:
    pred = _make_prediction(session, city)
    market = _make_market(session, city)
    rec = Recommendation(
        prediction_id=pred.id,
        market_id=market.id,
        direction=Direction.BUY_NO,
        model_probability=Decimal("0.1200"),
        kalshi_probability=Decimal("0.5400"),
        gap=Decimal("-0.4200"),
        expected_value=Decimal("0.1500"),
        risk_score=Decimal("3.0000"),
        risk_factors={"forecast_spread": 1, "source_agreement": 2},
    )
    session.add(rec)
    session.flush()
    return rec


def _make_user(session: Session, *, email: str | None = None) -> User:
    user = User(
        email=email or f"test-{uuid.uuid4().hex[:8]}@example.com",
        password_hash="$argon2id$v=19$m=65536,t=3,p=4$fakehash",
        role=UserRole.ADMIN,
    )
    session.add(user)
    session.flush()
    return user


# ---------------------------------------------------------------------------
# City
# ---------------------------------------------------------------------------

class TestCity:
    def test_insert_and_read(self, db_session: Session) -> None:
        city = _make_city(db_session)
        result = db_session.execute(select(City).where(City.id == city.id)).scalar_one()
        assert result.name == "Test City"
        assert result.kalshi_ticker_prefix == "TST"
        assert result.nws_station_id == "KTST"
        assert result.lat == 40.0
        assert result.lon == -74.0

    def test_uuid_auto_generated(self, db_session: Session) -> None:
        city = _make_city(db_session)
        assert isinstance(city.id, uuid.UUID)

    def test_unique_ticker_prefix(self, db_session: Session) -> None:
        _make_city(db_session, code="DUP", station="KAAA")
        with pytest.raises(IntegrityError):
            _make_city(db_session, code="DUP", station="KBBB")

    def test_unique_nws_station(self, db_session: Session) -> None:
        _make_city(db_session, code="AAA", station="KDUP")
        with pytest.raises(IntegrityError):
            _make_city(db_session, code="BBB", station="KDUP")

    def test_not_null_name(self, db_session: Session) -> None:
        city = City(
            name=None,
            kalshi_ticker_prefix="NUL",
            nws_station_id="KNUL",
            timezone="America/New_York",
            lat=40.0,
            lon=-74.0,
        )
        db_session.add(city)
        with pytest.raises(IntegrityError):
            db_session.flush()


# ---------------------------------------------------------------------------
# WeatherForecast
# ---------------------------------------------------------------------------

class TestWeatherForecast:
    def test_insert_with_all_fields(self, db_session: Session) -> None:
        city = _make_city(db_session)
        now = datetime.now(timezone.utc)
        forecast = WeatherForecast(
            source="nws",
            city_id=city.id,
            forecast_date=now,
            issued_at=now,
            temp_high=72.5,
            temp_low=58.0,
            raw_response={"properties": {"temperature": 72.5}},
        )
        db_session.add(forecast)
        db_session.flush()

        result = db_session.execute(select(WeatherForecast).where(WeatherForecast.id == forecast.id)).scalar_one()
        assert result.source == "nws"
        assert result.temp_high == 72.5
        assert result.raw_response is not None
        assert result.raw_response["properties"]["temperature"] == 72.5

    def test_created_at_default(self, db_session: Session) -> None:
        city = _make_city(db_session)
        now = datetime.now(timezone.utc)
        forecast = WeatherForecast(
            source="visual_crossing", city_id=city.id,
            forecast_date=now, issued_at=now,
        )
        db_session.add(forecast)
        db_session.flush()
        assert forecast.created_at is not None
        assert (datetime.now(timezone.utc) - forecast.created_at.replace(tzinfo=timezone.utc)).total_seconds() < 5

    def test_nullable_temps(self, db_session: Session) -> None:
        city = _make_city(db_session)
        now = datetime.now(timezone.utc)
        forecast = WeatherForecast(
            source="pirate_weather", city_id=city.id,
            forecast_date=now, issued_at=now,
            temp_high=None, temp_low=None,
        )
        db_session.add(forecast)
        db_session.flush()
        assert forecast.temp_high is None
        assert forecast.temp_low is None

    def test_fk_city(self, db_session: Session) -> None:
        fake_id = uuid.uuid4()
        forecast = WeatherForecast(
            source="nws", city_id=fake_id,
            forecast_date=datetime.now(timezone.utc),
            issued_at=datetime.now(timezone.utc),
        )
        db_session.add(forecast)
        with pytest.raises(IntegrityError):
            db_session.flush()

    def test_relationship_to_city(self, db_session: Session) -> None:
        city = _make_city(db_session)
        now = datetime.now(timezone.utc)
        forecast = WeatherForecast(
            source="nws", city_id=city.id,
            forecast_date=now, issued_at=now, temp_high=75.0,
        )
        db_session.add(forecast)
        db_session.flush()

        db_session.refresh(city)
        assert len(city.forecasts) == 1
        assert city.forecasts[0].source == "nws"


# ---------------------------------------------------------------------------
# KalshiMarket
# ---------------------------------------------------------------------------

class TestKalshiMarket:
    def test_insert_with_enums(self, db_session: Session) -> None:
        city = _make_city(db_session)
        market = _make_market(db_session, city)
        assert market.market_type == MarketType.HIGH
        assert market.status == MarketStatus.ACTIVE
        assert market.is_edge_bracket is False

    def test_unique_ticker(self, db_session: Session) -> None:
        city = _make_city(db_session)
        _make_market(db_session, city, ticker="KXHIGHTST-unique")
        with pytest.raises(IntegrityError):
            _make_market(db_session, city, ticker="KXHIGHTST-unique")

    def test_edge_bracket_and_settlement(self, db_session: Session) -> None:
        city = _make_city(db_session)
        market = KalshiMarket(
            event_id="evt_002", market_id="mkt_002",
            ticker="KXLOWTST-edge",
            city_id=city.id,
            forecast_date=datetime.now(timezone.utc),
            market_type=MarketType.LOW,
            bracket_low=None, bracket_high=Decimal("60"),
            is_edge_bracket=True,
            status=MarketStatus.SETTLED,
            settlement_value=Decimal("58"),
        )
        db_session.add(market)
        db_session.flush()
        assert market.is_edge_bracket is True
        assert market.status == MarketStatus.SETTLED
        assert market.bracket_low is None

    def test_updated_at_default(self, db_session: Session) -> None:
        city = _make_city(db_session)
        market = _make_market(db_session, city)
        assert market.updated_at is not None

    def test_relationship_snapshots(self, db_session: Session) -> None:
        city = _make_city(db_session)
        market = _make_market(db_session, city)
        snap = KalshiMarketSnapshot(
            market_id=market.id,
            timestamp=datetime.now(timezone.utc),
            yes_bid=Decimal("0.50"), yes_ask=Decimal("0.54"),
        )
        db_session.add(snap)
        db_session.flush()
        db_session.refresh(market)
        assert len(market.snapshots) == 1


# ---------------------------------------------------------------------------
# KalshiMarketSnapshot
# ---------------------------------------------------------------------------

class TestKalshiMarketSnapshot:
    def test_insert_with_nullable_fields(self, db_session: Session) -> None:
        city = _make_city(db_session)
        market = _make_market(db_session, city)
        snap = KalshiMarketSnapshot(
            market_id=market.id,
            timestamp=datetime.now(timezone.utc),
            yes_bid=Decimal("0.50"), yes_ask=Decimal("0.54"),
            no_bid=None, no_ask=None,
            volume=None, open_interest=None,
        )
        db_session.add(snap)
        db_session.flush()
        assert snap.volume is None

    def test_created_at_default(self, db_session: Session) -> None:
        city = _make_city(db_session)
        market = _make_market(db_session, city)
        snap = KalshiMarketSnapshot(
            market_id=market.id,
            timestamp=datetime.now(timezone.utc),
            yes_bid=Decimal("0.50"),
        )
        db_session.add(snap)
        db_session.flush()
        assert snap.created_at is not None

    def test_numeric_precision(self, db_session: Session) -> None:
        """Verify Numeric(10,4) preserves exact decimal values."""
        city = _make_city(db_session)
        market = _make_market(db_session, city)
        snap = KalshiMarketSnapshot(
            market_id=market.id,
            timestamp=datetime.now(timezone.utc),
            yes_bid=Decimal("0.5432"),
            yes_ask=Decimal("0.5567"),
        )
        db_session.add(snap)
        db_session.flush()
        result = db_session.execute(
            select(KalshiMarketSnapshot).where(KalshiMarketSnapshot.id == snap.id)
        ).scalar_one()
        assert result.yes_bid == Decimal("0.5432")
        assert result.yes_ask == Decimal("0.5567")

    def test_fk_market(self, db_session: Session) -> None:
        snap = KalshiMarketSnapshot(
            market_id=uuid.uuid4(),
            timestamp=datetime.now(timezone.utc),
        )
        db_session.add(snap)
        with pytest.raises(IntegrityError):
            db_session.flush()


# ---------------------------------------------------------------------------
# Prediction
# ---------------------------------------------------------------------------

class TestPrediction:
    def test_insert_with_jsonb(self, db_session: Session) -> None:
        city = _make_city(db_session)
        pred = _make_prediction(db_session, city)
        result = db_session.execute(select(Prediction).where(Prediction.id == pred.id)).scalar_one()
        assert result.predicted_temp == 72.5
        assert result.probability_distribution is not None
        assert result.probability_distribution["72-74"] == 0.5
        assert result.market_type == MarketType.HIGH

    def test_relationship_to_city(self, db_session: Session) -> None:
        city = _make_city(db_session)
        _make_prediction(db_session, city)
        db_session.refresh(city)
        assert len(city.predictions) == 1


# ---------------------------------------------------------------------------
# Recommendation
# ---------------------------------------------------------------------------

class TestRecommendation:
    def test_insert_with_all_fields(self, db_session: Session) -> None:
        city = _make_city(db_session)
        rec = _make_recommendation(db_session, city)
        result = db_session.execute(select(Recommendation).where(Recommendation.id == rec.id)).scalar_one()
        assert result.direction == Direction.BUY_NO
        assert result.gap == Decimal("-0.4200")
        assert result.risk_factors is not None
        assert result.risk_factors["forecast_spread"] == 1

    def test_numeric_precision_on_probabilities(self, db_session: Session) -> None:
        """Verify Numeric(10,4) preserves exact probability values."""
        city = _make_city(db_session)
        rec = _make_recommendation(db_session, city)
        result = db_session.execute(select(Recommendation).where(Recommendation.id == rec.id)).scalar_one()
        assert result.model_probability == Decimal("0.1200")
        assert result.kalshi_probability == Decimal("0.5400")
        assert result.expected_value == Decimal("0.1500")

    def test_relationships(self, db_session: Session) -> None:
        city = _make_city(db_session)
        rec = _make_recommendation(db_session, city)
        db_session.refresh(rec)
        assert rec.prediction is not None
        assert rec.market is not None


# ---------------------------------------------------------------------------
# PaperTradeFixed
# ---------------------------------------------------------------------------

class TestPaperTradeFixed:
    def test_unsettled_trade(self, db_session: Session) -> None:
        city = _make_city(db_session)
        rec = _make_recommendation(db_session, city)
        trade = PaperTradeFixed(
            recommendation_id=rec.id,
            entry_price=Decimal("0.5400"),
            contracts_qty=1,
        )
        db_session.add(trade)
        db_session.flush()
        assert trade.settled_at is None
        assert trade.settlement_outcome is None
        assert trade.pnl is None
        assert trade.contracts_qty == 1

    def test_settled_trade(self, db_session: Session) -> None:
        city = _make_city(db_session)
        rec = _make_recommendation(db_session, city)
        trade = PaperTradeFixed(
            recommendation_id=rec.id,
            entry_price=Decimal("0.5400"),
            settled_at=datetime.now(timezone.utc),
            settlement_outcome=SettlementOutcome.WIN,
            pnl=Decimal("0.4600"),
        )
        db_session.add(trade)
        db_session.flush()
        assert trade.settlement_outcome == SettlementOutcome.WIN
        assert trade.pnl == Decimal("0.4600")

    def test_unique_recommendation(self, db_session: Session) -> None:
        """Each recommendation gets exactly one fixed paper trade."""
        city = _make_city(db_session)
        rec = _make_recommendation(db_session, city)
        trade1 = PaperTradeFixed(recommendation_id=rec.id, entry_price=Decimal("0.54"))
        db_session.add(trade1)
        db_session.flush()

        trade2 = PaperTradeFixed(recommendation_id=rec.id, entry_price=Decimal("0.54"))
        db_session.add(trade2)
        with pytest.raises(IntegrityError):
            db_session.flush()

    def test_relationship_to_recommendation(self, db_session: Session) -> None:
        city = _make_city(db_session)
        rec = _make_recommendation(db_session, city)
        trade = PaperTradeFixed(recommendation_id=rec.id, entry_price=Decimal("0.54"))
        db_session.add(trade)
        db_session.flush()
        db_session.refresh(rec)
        assert rec.paper_trade_fixed is not None
        assert rec.paper_trade_fixed.entry_price == Decimal("0.5400")


# ---------------------------------------------------------------------------
# PaperTradePortfolio + PaperPortfolio
# ---------------------------------------------------------------------------

class TestPaperTradePortfolio:
    def test_insert_with_portfolio(self, db_session: Session) -> None:
        city = _make_city(db_session)
        rec = _make_recommendation(db_session, city)
        portfolio = PaperPortfolio(
            name="Default",
            initial_balance=Decimal("10000.0000"),
            current_balance=Decimal("10000.0000"),
            sizing_strategy=SizingStrategy.FIXED_PCT,
        )
        db_session.add(portfolio)
        db_session.flush()

        trade = PaperTradePortfolio(
            recommendation_id=rec.id,
            portfolio_id=portfolio.id,
            entry_price=Decimal("0.5400"),
            contracts_qty=5,
            position_size_usd=Decimal("2.7000"),
        )
        db_session.add(trade)
        db_session.flush()
        assert trade.position_size_usd == Decimal("2.7000")

    def test_portfolio_strategies(self, db_session: Session) -> None:
        for strategy in SizingStrategy:
            portfolio = PaperPortfolio(
                name=f"Test {strategy.value}",
                initial_balance=Decimal("10000.0000"),
                current_balance=Decimal("10000.0000"),
                sizing_strategy=strategy,
            )
            db_session.add(portfolio)
        db_session.flush()

        results = db_session.execute(select(PaperPortfolio)).scalars().all()
        strategies = {p.sizing_strategy for p in results}
        assert strategies == {SizingStrategy.FIXED_PCT, SizingStrategy.KELLY, SizingStrategy.CONFIDENCE_SCALED}

    def test_relationship_portfolio_to_trades(self, db_session: Session) -> None:
        city = _make_city(db_session)
        rec = _make_recommendation(db_session, city)
        portfolio = PaperPortfolio(
            name="Kelly", initial_balance=Decimal("10000.0000"),
            current_balance=Decimal("10000.0000"), sizing_strategy=SizingStrategy.KELLY,
        )
        db_session.add(portfolio)
        db_session.flush()

        trade = PaperTradePortfolio(
            recommendation_id=rec.id, portfolio_id=portfolio.id,
            entry_price=Decimal("0.30"), contracts_qty=10, position_size_usd=Decimal("3.00"),
        )
        db_session.add(trade)
        db_session.flush()
        db_session.refresh(portfolio)
        assert len(portfolio.trades) == 1


# ---------------------------------------------------------------------------
# User
# ---------------------------------------------------------------------------

class TestUser:
    def test_insert(self, db_session: Session) -> None:
        user = _make_user(db_session)
        assert user.role == UserRole.ADMIN
        assert isinstance(user.id, uuid.UUID)

    def test_unique_email(self, db_session: Session) -> None:
        _make_user(db_session, email="dupe@example.com")
        with pytest.raises(IntegrityError):
            _make_user(db_session, email="dupe@example.com")

    def test_default_role_user(self, db_session: Session) -> None:
        user = User(
            email="default-role@example.com",
            password_hash="$argon2id$fakehash",
        )
        db_session.add(user)
        db_session.flush()
        assert user.role == UserRole.USER

    def test_preferences_jsonb(self, db_session: Session) -> None:
        user = User(
            email="prefs@example.com",
            password_hash="$argon2id$fakehash",
            preferences={"email_enabled": True, "risk_threshold": 5, "cities": ["NYC", "MIA"]},
        )
        db_session.add(user)
        db_session.flush()
        result = db_session.execute(select(User).where(User.id == user.id)).scalar_one()
        assert result.preferences is not None
        assert result.preferences["risk_threshold"] == 5
        assert "MIA" in result.preferences["cities"]


# ---------------------------------------------------------------------------
# UserTrade
# ---------------------------------------------------------------------------

class TestUserTrade:
    def test_insert(self, db_session: Session) -> None:
        city = _make_city(db_session)
        market = _make_market(db_session, city)
        user = _make_user(db_session)
        trade = UserTrade(
            user_id=user.id,
            market_id=market.id,
            direction=Direction.BUY_YES,
            entry_price=Decimal("0.45"),
            contracts_qty=10,
            notes="Testing the water",
        )
        db_session.add(trade)
        db_session.flush()
        assert trade.notes == "Testing the water"
        assert trade.direction == Direction.BUY_YES

    def test_relationship_user_to_trades(self, db_session: Session) -> None:
        city = _make_city(db_session)
        market = _make_market(db_session, city)
        user = _make_user(db_session)
        trade = UserTrade(
            user_id=user.id, market_id=market.id,
            direction=Direction.BUY_YES, entry_price=Decimal("0.45"), contracts_qty=1,
        )
        db_session.add(trade)
        db_session.flush()
        db_session.refresh(user)
        assert len(user.trades) == 1


# ---------------------------------------------------------------------------
# RefreshToken
# ---------------------------------------------------------------------------

class TestRefreshToken:
    def test_insert(self, db_session: Session) -> None:
        user = _make_user(db_session)
        token = RefreshToken(
            user_id=user.id,
            token_hash="sha256_abcdef1234567890",
            expires_at=datetime.now(timezone.utc) + timedelta(days=14),
        )
        db_session.add(token)
        db_session.flush()
        assert token.revoked_at is None

    def test_unique_token_hash(self, db_session: Session) -> None:
        user = _make_user(db_session)
        t1 = RefreshToken(
            user_id=user.id, token_hash="same_hash",
            expires_at=datetime.now(timezone.utc) + timedelta(days=14),
        )
        db_session.add(t1)
        db_session.flush()
        t2 = RefreshToken(
            user_id=user.id, token_hash="same_hash",
            expires_at=datetime.now(timezone.utc) + timedelta(days=14),
        )
        db_session.add(t2)
        with pytest.raises(IntegrityError):
            db_session.flush()

    def test_revoke(self, db_session: Session) -> None:
        user = _make_user(db_session)
        token = RefreshToken(
            user_id=user.id, token_hash="revoke_me",
            expires_at=datetime.now(timezone.utc) + timedelta(days=14),
        )
        db_session.add(token)
        db_session.flush()
        token.revoked_at = datetime.now(timezone.utc)
        db_session.flush()
        assert token.revoked_at is not None

    def test_relationship_user_to_tokens(self, db_session: Session) -> None:
        user = _make_user(db_session)
        t = RefreshToken(
            user_id=user.id, token_hash="token_rel_test",
            expires_at=datetime.now(timezone.utc) + timedelta(days=14),
        )
        db_session.add(t)
        db_session.flush()
        db_session.refresh(user)
        assert len(user.refresh_tokens) == 1


# ---------------------------------------------------------------------------
# EmailLog + join table
# ---------------------------------------------------------------------------

class TestEmailLog:
    def test_insert(self, db_session: Session) -> None:
        user = _make_user(db_session)
        log = EmailLog(
            user_id=user.id,
            email_type="daily_digest",
            sent_at=datetime.now(timezone.utc),
        )
        db_session.add(log)
        db_session.flush()
        assert isinstance(log.id, uuid.UUID)

    def test_many_to_many_recommendations(self, db_session: Session) -> None:
        city = _make_city(db_session)
        rec1 = _make_recommendation(db_session, city)

        # Need a second recommendation with a different prediction + market
        pred2 = Prediction(
            city_id=city.id, forecast_date=datetime.now(timezone.utc),
            market_type=MarketType.LOW, model_version="tier1_v1",
            predicted_temp=58.0, std_dev=2.0,
        )
        db_session.add(pred2)
        db_session.flush()

        market2 = _make_market(db_session, city)
        rec2 = Recommendation(
            prediction_id=pred2.id, market_id=market2.id,
            direction=Direction.BUY_YES, model_probability=Decimal("0.65"),
            kalshi_probability=Decimal("0.40"), gap=Decimal("0.25"),
            expected_value=Decimal("0.20"), risk_score=Decimal("4.0"),
        )
        db_session.add(rec2)
        db_session.flush()

        user = _make_user(db_session)
        log = EmailLog(
            user_id=user.id, email_type="daily_digest",
            sent_at=datetime.now(timezone.utc),
        )
        log.recommendations.append(rec1)
        log.recommendations.append(rec2)
        db_session.add(log)
        db_session.flush()

        db_session.refresh(log)
        assert len(log.recommendations) == 2

    def test_relationship_user_to_email_logs(self, db_session: Session) -> None:
        user = _make_user(db_session)
        log = EmailLog(
            user_id=user.id, email_type="weekly_report",
            sent_at=datetime.now(timezone.utc),
        )
        db_session.add(log)
        db_session.flush()
        db_session.refresh(user)
        assert len(user.email_logs) == 1


# ---------------------------------------------------------------------------
# Enum values stored correctly in PostgreSQL
# ---------------------------------------------------------------------------

class TestEnumStorage:
    def test_market_type_enum_values(self, db_session: Session) -> None:
        city = _make_city(db_session)
        for mt in MarketType:
            market = KalshiMarket(
                event_id=f"evt_{mt.value}", market_id=f"mkt_{mt.value}",
                ticker=f"KX{mt.value}TST-{uuid.uuid4().hex[:6]}",
                city_id=city.id,
                forecast_date=datetime.now(timezone.utc),
                market_type=mt,
            )
            db_session.add(market)
        db_session.flush()

        results = db_session.execute(select(KalshiMarket)).scalars().all()
        types = {m.market_type for m in results}
        assert types == {MarketType.HIGH, MarketType.LOW}

    def test_direction_enum_values(self, db_session: Session) -> None:
        city = _make_city(db_session)
        for d in Direction:
            rec = _make_recommendation(db_session, city)
            rec.direction = d
        db_session.flush()

    def test_settlement_outcome_enum_values(self, db_session: Session) -> None:
        city = _make_city(db_session)
        rec = _make_recommendation(db_session, city)
        for outcome in SettlementOutcome:
            trade = PaperTradeFixed(
                recommendation_id=rec.id, entry_price=Decimal("0.50"),
                settlement_outcome=outcome,
                settled_at=datetime.now(timezone.utc),
                pnl=Decimal("0.50") if outcome == SettlementOutcome.WIN else Decimal("-0.50"),
            )
            db_session.add(trade)
            try:
                db_session.flush()
            except IntegrityError:
                db_session.rollback()
                # Unique constraint on recommendation_id — expected after first


# ---------------------------------------------------------------------------
# Indexes exist
# ---------------------------------------------------------------------------

class TestIndexes:
    def test_expected_indexes_exist(self, db_engine: Engine) -> None:
        inspector = inspect(db_engine)
        all_indexes: dict[str, list[str | None]] = {}
        for table_name in inspector.get_table_names():
            all_indexes[table_name] = [idx["name"] for idx in inspector.get_indexes(table_name)]

        assert "ix_forecasts_city_date_source" in all_indexes.get("weather_forecasts", [])
        assert "ix_forecasts_source" in all_indexes.get("weather_forecasts", [])
        assert "ix_markets_city_date" in all_indexes.get("kalshi_markets", [])
        assert "ix_snapshots_market_time" in all_indexes.get("kalshi_market_snapshots", [])
        assert "ix_recommendations_created" in all_indexes.get("recommendations", [])
        assert "ix_paper_fixed_unsettled" in all_indexes.get("paper_trades_fixed", [])
        assert "ix_paper_portfolio_unsettled" in all_indexes.get("paper_trades_portfolio", [])


# ---------------------------------------------------------------------------
# Session module
# ---------------------------------------------------------------------------

class TestSessionModule:
    def test_get_session_context_manager(self, db_settings: Settings) -> None:
        """Test that get_session works as a context manager with real DB."""
        from shared.config.settings import reset_settings
        from shared.db.session import get_session, reset_engine

        reset_settings(db_settings)
        reset_engine()

        try:
            with get_session() as session:
                result = session.execute(text("SELECT 1")).scalar()
                assert result == 1
        finally:
            reset_engine()
            reset_settings()

    def test_engine_pool_configuration(self, db_settings: Settings) -> None:
        """Verify engine pool size and pre-ping settings."""
        from shared.config.settings import reset_settings
        from shared.db.session import get_engine, reset_engine

        reset_settings(db_settings)
        reset_engine()

        try:
            engine = get_engine()
            assert engine.pool.size() == 5  # type: ignore[attr-defined]
            assert engine.pool._max_overflow == 5  # type: ignore[attr-defined]
            assert engine.pool._pre_ping is True
        finally:
            reset_engine()
            reset_settings()
