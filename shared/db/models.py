"""
SQLAlchemy ORM models for all database tables.

14 tables total (13 from plan + email_log_recommendations join table).
All use UUID4 primary keys. Enums are PostgreSQL native ENUMs.
Financial fields use Numeric(10,4) for exact decimal arithmetic.

This is the single source of truth for the database schema.
Alembic auto-generates migrations from these model definitions.
"""

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Table,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from shared.db.enums import (
    Direction,
    MarketStatus,
    MarketType,
    SettlementOutcome,
    SizingStrategy,
    UserRole,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _new_uuid() -> uuid.UUID:
    return uuid.uuid4()


class Base(DeclarativeBase):
    """Base class for all models. Provides common type mappings."""
    pass


# ---------------------------------------------------------------------------
# Join table: email_logs <-> recommendations (many-to-many)
# ---------------------------------------------------------------------------

email_log_recommendations = Table(
    "email_log_recommendations",
    Base.metadata,
    Column("email_log_id", UUID(as_uuid=True), ForeignKey("email_logs.id"), primary_key=True),
    Column("recommendation_id", UUID(as_uuid=True), ForeignKey("recommendations.id"), primary_key=True),
)


# ---------------------------------------------------------------------------
# Core reference data
# ---------------------------------------------------------------------------

class City(Base):
    __tablename__ = "cities"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    kalshi_ticker_prefix: Mapped[str] = mapped_column(String(10), unique=True, nullable=False)
    nws_station_id: Mapped[str] = mapped_column(String(10), unique=True, nullable=False)
    timezone: Mapped[str] = mapped_column(String(50), nullable=False)
    lat: Mapped[float] = mapped_column(Float, nullable=False)
    lon: Mapped[float] = mapped_column(Float, nullable=False)

    # Relationships
    forecasts: Mapped[list["WeatherForecast"]] = relationship(back_populates="city")
    markets: Mapped[list["KalshiMarket"]] = relationship(back_populates="city")
    predictions: Mapped[list["Prediction"]] = relationship(back_populates="city")


# ---------------------------------------------------------------------------
# Weather data
# ---------------------------------------------------------------------------

class WeatherForecast(Base):
    __tablename__ = "weather_forecasts"
    __table_args__ = (
        Index("ix_forecasts_city_date_source", "city_id", "forecast_date", "source"),
        Index("ix_forecasts_source", "source"),
        UniqueConstraint("source", "city_id", "forecast_date", name="uq_forecast_dedup"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    source: Mapped[str] = mapped_column(String(50), nullable=False)
    city_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("cities.id"), nullable=False)
    forecast_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    issued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    temp_high: Mapped[Decimal | None] = mapped_column(Numeric(6, 2), nullable=True)
    temp_low: Mapped[Decimal | None] = mapped_column(Numeric(6, 2), nullable=True)
    raw_response: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    # Relationships
    city: Mapped["City"] = relationship(back_populates="forecasts")


# ---------------------------------------------------------------------------
# Kalshi market data
# ---------------------------------------------------------------------------

class KalshiMarket(Base):
    __tablename__ = "kalshi_markets"
    __table_args__ = (
        Index("ix_markets_city_date", "city_id", "forecast_date"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    event_id: Mapped[str] = mapped_column(String(100), nullable=False)
    market_id: Mapped[str] = mapped_column(String(100), nullable=False)
    ticker: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    city_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("cities.id"), nullable=False)
    forecast_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    market_type: Mapped[MarketType] = mapped_column(
        Enum(MarketType, name="market_type_enum", create_constraint=True),
        nullable=False,
    )
    bracket_low: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    bracket_high: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    is_edge_bracket: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    status: Mapped[MarketStatus] = mapped_column(
        Enum(MarketStatus, name="market_status_enum", create_constraint=True),
        default=MarketStatus.ACTIVE,
        nullable=False,
    )
    settlement_value: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, onupdate=_utcnow, nullable=False)

    # Relationships
    city: Mapped["City"] = relationship(back_populates="markets")
    snapshots: Mapped[list["KalshiMarketSnapshot"]] = relationship(back_populates="market")
    recommendations: Mapped[list["Recommendation"]] = relationship(back_populates="market")


class KalshiMarketSnapshot(Base):
    __tablename__ = "kalshi_market_snapshots"
    __table_args__ = (
        Index("ix_snapshots_market_time", "market_id", "timestamp"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    market_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("kalshi_markets.id"), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    yes_bid: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    yes_ask: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    no_bid: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    no_ask: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    last_price: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    volume: Mapped[int | None] = mapped_column(Integer, nullable=True)
    open_interest: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    # Relationships
    market: Mapped["KalshiMarket"] = relationship(back_populates="snapshots")


# ---------------------------------------------------------------------------
# Model outputs
# ---------------------------------------------------------------------------

class Prediction(Base):
    __tablename__ = "predictions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    city_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("cities.id"), nullable=False)
    forecast_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    market_type: Mapped[MarketType] = mapped_column(
        Enum(MarketType, name="market_type_enum", create_constraint=True, create_type=False),
        nullable=False,
    )
    model_version: Mapped[str] = mapped_column(String(50), nullable=False)
    predicted_temp: Mapped[float] = mapped_column(Float, nullable=False)
    std_dev: Mapped[float] = mapped_column(Float, nullable=False)
    probability_distribution: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    # Relationships
    city: Mapped["City"] = relationship(back_populates="predictions")
    recommendations: Mapped[list["Recommendation"]] = relationship(back_populates="prediction")


# ---------------------------------------------------------------------------
# Recommendations
# ---------------------------------------------------------------------------

class Recommendation(Base):
    __tablename__ = "recommendations"
    __table_args__ = (
        Index("ix_recommendations_created", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    prediction_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("predictions.id"), nullable=False)
    market_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("kalshi_markets.id"), nullable=False)
    direction: Mapped[Direction] = mapped_column(
        Enum(Direction, name="direction_enum", create_constraint=True),
        nullable=False,
    )
    model_probability: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    kalshi_probability: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    gap: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    expected_value: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    risk_score: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    risk_factors: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    # Relationships
    prediction: Mapped["Prediction"] = relationship(back_populates="recommendations")
    market: Mapped["KalshiMarket"] = relationship(back_populates="recommendations")
    paper_trade_fixed: Mapped["PaperTradeFixed | None"] = relationship(back_populates="recommendation")
    paper_trades_portfolio: Mapped[list["PaperTradePortfolio"]] = relationship(back_populates="recommendation")
    email_logs: Mapped[list["EmailLog"]] = relationship(
        secondary="email_log_recommendations", back_populates="recommendations"
    )


# ---------------------------------------------------------------------------
# Paper trading (dual mode)
# ---------------------------------------------------------------------------

class PaperTradeFixed(Base):
    __tablename__ = "paper_trades_fixed"
    __table_args__ = (
        Index("ix_paper_fixed_unsettled", "settled_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    recommendation_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("recommendations.id"), unique=True, nullable=False)
    entry_price: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    contracts_qty: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    settled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    settlement_outcome: Mapped[SettlementOutcome | None] = mapped_column(
        Enum(SettlementOutcome, name="settlement_outcome_enum", create_constraint=True),
        nullable=True,
    )
    pnl: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    # Relationships
    recommendation: Mapped["Recommendation"] = relationship(back_populates="paper_trade_fixed")


class PaperTradePortfolio(Base):
    __tablename__ = "paper_trades_portfolio"
    __table_args__ = (
        Index("ix_paper_portfolio_unsettled", "settled_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    recommendation_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("recommendations.id"), nullable=False)
    portfolio_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("paper_portfolios.id"), nullable=False)
    entry_price: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    contracts_qty: Mapped[int] = mapped_column(Integer, nullable=False)
    position_size_usd: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    settled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    settlement_outcome: Mapped[SettlementOutcome | None] = mapped_column(
        Enum(SettlementOutcome, name="settlement_outcome_enum", create_constraint=True, create_type=False),
        nullable=True,
    )
    pnl: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    # Relationships
    recommendation: Mapped["Recommendation"] = relationship(back_populates="paper_trades_portfolio")
    portfolio: Mapped["PaperPortfolio"] = relationship(back_populates="trades")


class PaperPortfolio(Base):
    __tablename__ = "paper_portfolios"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    initial_balance: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    current_balance: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    sizing_strategy: Mapped[SizingStrategy] = mapped_column(
        Enum(SizingStrategy, name="sizing_strategy_enum", create_constraint=True),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    # Relationships
    trades: Mapped[list["PaperTradePortfolio"]] = relationship(back_populates="portfolio")


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------

class User(Base):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[UserRole] = mapped_column(
        Enum(UserRole, name="user_role_enum", create_constraint=True),
        default=UserRole.USER,
        nullable=False,
    )
    preferences: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    # Relationships
    trades: Mapped[list["UserTrade"]] = relationship(back_populates="user")
    refresh_tokens: Mapped[list["RefreshToken"]] = relationship(back_populates="user")
    email_logs: Mapped[list["EmailLog"]] = relationship(back_populates="user")


class UserTrade(Base):
    __tablename__ = "user_trades"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), nullable=False)
    market_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("kalshi_markets.id"), nullable=False)
    direction: Mapped[Direction] = mapped_column(
        Enum(Direction, name="direction_enum", create_constraint=True, create_type=False),
        nullable=False,
    )
    entry_price: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    contracts_qty: Mapped[int] = mapped_column(Integer, nullable=False)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    settled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    settlement_outcome: Mapped[SettlementOutcome | None] = mapped_column(
        Enum(SettlementOutcome, name="settlement_outcome_enum", create_constraint=True, create_type=False),
        nullable=True,
    )
    pnl: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    # Relationships
    user: Mapped["User"] = relationship(back_populates="trades")
    market: Mapped["KalshiMarket"] = relationship()


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

class RefreshToken(Base):
    __tablename__ = "refresh_tokens"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), nullable=False)
    token_hash: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Relationships
    user: Mapped["User"] = relationship(back_populates="refresh_tokens")


# ---------------------------------------------------------------------------
# Notifications
# ---------------------------------------------------------------------------

class EmailLog(Base):
    __tablename__ = "email_logs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), nullable=False)
    email_type: Mapped[str] = mapped_column(String(50), nullable=False)
    sent_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    # Relationships
    user: Mapped["User"] = relationship(back_populates="email_logs")
    recommendations: Mapped[list["Recommendation"]] = relationship(
        secondary="email_log_recommendations", back_populates="email_logs"
    )
