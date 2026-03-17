"""
Python enums that map to PostgreSQL native ENUM types.

These are used in SQLAlchemy model column definitions and ensure type safety
at both the Python and database level.
"""

from enum import StrEnum


class MarketType(StrEnum):
    HIGH = "HIGH"
    LOW = "LOW"


class Direction(StrEnum):
    BUY_YES = "BUY_YES"
    BUY_NO = "BUY_NO"


class SettlementOutcome(StrEnum):
    WIN = "WIN"
    LOSS = "LOSS"


class UserRole(StrEnum):
    ADMIN = "ADMIN"
    USER = "USER"


class SizingStrategy(StrEnum):
    FIXED_PCT = "FIXED_PCT"
    KELLY = "KELLY"
    CONFIDENCE_SCALED = "CONFIDENCE_SCALED"


class MarketStatus(StrEnum):
    """Kalshi market lifecycle states."""
    ACTIVE = "ACTIVE"
    CLOSED = "CLOSED"
    SETTLED = "SETTLED"


class WeatherSource(StrEnum):
    """Canonical weather data source identifiers."""
    NWS = "NWS"
    VISUAL_CROSSING = "VISUAL_CROSSING"
    PIRATE_WEATHER = "PIRATE_WEATHER"
    OPENWEATHER = "OPENWEATHER"
