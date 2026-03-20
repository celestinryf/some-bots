"""Test object factories for prediction-engine tests.

Centralised ``make_*`` functions that return ``MagicMock(spec=Model)``
objects with sensible defaults and keyword overrides.  Use these instead
of inline helpers to keep tests DRY and readable.

Usage:
    market = make_market(bracket_low=Decimal("65"), bracket_high=Decimal("70"))
    forecast = make_forecast(source="NWS", temp_high=Decimal("72.0"))
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

from shared.db.enums import Direction, MarketStatus, MarketType
from shared.db.models import (
    KalshiMarket,
    KalshiMarketSnapshot,
    PaperTradeFixed,
    Prediction,
    Recommendation,
    WeatherForecast,
)

_DEFAULT_CITY_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")
_DEFAULT_FORECAST_DATE = datetime(2026, 3, 20, tzinfo=UTC)


def make_forecast(
    *,
    city_id: uuid.UUID | None = None,
    source: str = "NWS",
    forecast_date: datetime | None = None,
    issued_at: datetime | None = None,
    temp_high: Decimal | None = Decimal("72.00"),
    temp_low: Decimal | None = Decimal("55.00"),
) -> WeatherForecast:
    f = MagicMock(spec=WeatherForecast)
    f.id = uuid.uuid4()
    f.city_id = city_id or _DEFAULT_CITY_ID
    f.source = source
    f.forecast_date = forecast_date or _DEFAULT_FORECAST_DATE
    f.issued_at = issued_at or datetime(2026, 3, 19, 12, 0, tzinfo=UTC)
    f.temp_high = temp_high
    f.temp_low = temp_low
    f.created_at = datetime(2026, 3, 19, 12, 0, tzinfo=UTC)
    f.updated_at = datetime(2026, 3, 19, 12, 0, tzinfo=UTC)
    return f


def make_market(
    *,
    market_id: uuid.UUID | None = None,
    city_id: uuid.UUID | None = None,
    forecast_date: datetime | None = None,
    market_type: MarketType = MarketType.HIGH,
    bracket_low: Decimal | None = Decimal("65.0000"),
    bracket_high: Decimal | None = Decimal("70.0000"),
    is_edge_bracket: bool = False,
    status: MarketStatus = MarketStatus.ACTIVE,
    ticker: str = "KXHIGHNYC-26MAR20-T65",
) -> KalshiMarket:
    m = MagicMock(spec=KalshiMarket)
    m.id = market_id or uuid.uuid4()
    m.city_id = city_id or _DEFAULT_CITY_ID
    m.forecast_date = forecast_date or _DEFAULT_FORECAST_DATE
    m.market_type = market_type
    m.bracket_low = bracket_low
    m.bracket_high = bracket_high
    m.is_edge_bracket = is_edge_bracket
    m.status = status
    m.ticker = ticker
    return m


def make_snapshot(
    *,
    market_id: uuid.UUID | None = None,
    yes_bid: Decimal | None = Decimal("0.4000"),
    yes_ask: Decimal | None = Decimal("0.4500"),
    no_bid: Decimal | None = Decimal("0.5000"),
    no_ask: Decimal | None = Decimal("0.5500"),
    volume: int | None = 50,
    open_interest: int | None = 20,
    timestamp: datetime | None = None,
) -> KalshiMarketSnapshot:
    s = MagicMock(spec=KalshiMarketSnapshot)
    s.id = uuid.uuid4()
    s.market_id = market_id or uuid.uuid4()
    s.yes_bid = yes_bid
    s.yes_ask = yes_ask
    s.no_bid = no_bid
    s.no_ask = no_ask
    s.volume = volume
    s.open_interest = open_interest
    s.timestamp = timestamp or datetime(2026, 3, 20, 10, 0, tzinfo=UTC)
    s.created_at = datetime(2026, 3, 20, 10, 0, tzinfo=UTC)
    return s


def make_prediction(
    *,
    prediction_id: uuid.UUID | None = None,
    city_id: uuid.UUID | None = None,
    forecast_date: datetime | None = None,
    market_type: MarketType = MarketType.HIGH,
    model_version: str = "tier1_equal_weight_v1",
    predicted_temp: Decimal = Decimal("68.50"),
    std_dev: Decimal = Decimal("2.00"),
    probability_distribution: dict | None = None,
) -> Prediction:
    p = MagicMock(spec=Prediction)
    p.id = prediction_id or uuid.uuid4()
    p.city_id = city_id or _DEFAULT_CITY_ID
    p.forecast_date = forecast_date or _DEFAULT_FORECAST_DATE
    p.market_type = market_type
    p.model_version = model_version
    p.predicted_temp = predicted_temp
    p.std_dev = std_dev
    p.probability_distribution = probability_distribution or {
        "brackets": {"[65.0000, 70.0000)": 0.45},
        "mean": 68.5,
        "std_dev": 2.0,
        "source_temps": {"NWS": 68.0, "VC": 69.0},
        "sum_check": 1.0,
    }
    return p


def make_recommendation(
    *,
    prediction_id: uuid.UUID | None = None,
    market_id: uuid.UUID | None = None,
    direction: Direction = Direction.BUY_YES,
    model_probability: Decimal = Decimal("0.4500"),
    kalshi_probability: Decimal = Decimal("0.3000"),
    gap: Decimal = Decimal("0.1500"),
    expected_value: Decimal = Decimal("0.1000"),
    risk_score: Decimal = Decimal("3.5"),
) -> Recommendation:
    r = MagicMock(spec=Recommendation)
    r.id = uuid.uuid4()
    r.prediction_id = prediction_id or uuid.uuid4()
    r.market_id = market_id or uuid.uuid4()
    r.direction = direction
    r.model_probability = model_probability
    r.kalshi_probability = kalshi_probability
    r.gap = gap
    r.expected_value = expected_value
    r.risk_score = risk_score
    r.risk_factors = {}
    return r


def make_paper_trade(
    *,
    recommendation_id: uuid.UUID | None = None,
    entry_price: Decimal = Decimal("0.4500"),
    contracts_qty: int = 1,
) -> PaperTradeFixed:
    t = MagicMock(spec=PaperTradeFixed)
    t.id = uuid.uuid4()
    t.recommendation_id = recommendation_id or uuid.uuid4()
    t.entry_price = entry_price
    t.contracts_qty = contracts_qty
    t.settled_at = None
    t.settlement_outcome = None
    t.pnl = None
    return t
