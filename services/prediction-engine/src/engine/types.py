"""Intermediate pipeline types for prediction and recommendation flows.

These frozen dataclasses carry data between pipeline stages without
coupling to ORM models or DB sessions.  They are the "language" of the
engine layer.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from shared.db.enums import Direction, MarketType


@dataclass(frozen=True)
class PredictionGroup:
    """A unique (city, date, market_type) group of markets to predict."""

    city_id: uuid.UUID
    forecast_date: datetime
    market_type: MarketType
    market_ids: tuple[uuid.UUID, ...]


@dataclass(frozen=True)
class RecommendationCandidate:
    """A potential trade that passed gap + EV thresholds."""

    direction: Direction
    model_probability: Decimal
    kalshi_probability: Decimal
    gap: Decimal
    expected_value: Decimal
    entry_price: Decimal
