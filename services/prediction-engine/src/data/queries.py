"""Bulk database queries for the prediction and recommendation pipelines.

All functions accept an open ``Session`` (not a factory) so the caller
controls transaction boundaries and can compose multiple queries in a
single read-consistent view.

Query functions return Python dicts keyed by entity IDs for O(1) lookup
in the pipeline loops.
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from datetime import datetime
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, aliased

from shared.config.logging import get_logger
from shared.db.enums import Direction, MarketStatus, MarketType
from shared.db.models import (
    KalshiMarket,
    KalshiMarketSnapshot,
    PaperTradeFixed,
    Prediction,
    Recommendation,
    WeatherForecast,
)

from src.engine.types import PredictionGroup

logger = get_logger("prediction-queries")


# ---------------------------------------------------------------------------
# Forecast queries
# ---------------------------------------------------------------------------


def fetch_source_temperatures(
    session: Session,
    *,
    city_id: uuid.UUID,
    forecast_date: datetime,
    market_type: MarketType,
) -> dict[str, Decimal]:
    """Load the latest temperature per weather source for a city+date.

    For each source, picks the most recently issued forecast and extracts
    ``temp_high`` (for HIGH markets) or ``temp_low`` (for LOW markets).

    Args:
        session: Open SQLAlchemy session.
        city_id: Target city UUID.
        forecast_date: Target forecast date.
        market_type: HIGH or LOW — determines which temp field to extract.

    Returns:
        ``{source_name: temperature}`` dict.  Sources with NULL temps
        are excluded.
    """
    forecasts = (
        session.execute(
            select(WeatherForecast)
            .where(
                WeatherForecast.city_id == city_id,
                WeatherForecast.forecast_date == forecast_date,
            )
            .order_by(
                WeatherForecast.source,
                WeatherForecast.issued_at.desc(),
                WeatherForecast.updated_at.desc(),
                WeatherForecast.created_at.desc(),
            )
        )
        .scalars()
        .all()
    )

    latest_by_source: dict[str, Decimal] = {}
    for forecast in forecasts:
        if forecast.source in latest_by_source:
            continue
        temp = (
            forecast.temp_high
            if market_type == MarketType.HIGH
            else forecast.temp_low
        )
        if temp is not None:
            latest_by_source[forecast.source] = temp

    return latest_by_source


def fetch_all_source_temperatures(
    session: Session,
    *,
    groups: list[PredictionGroup],
) -> dict[tuple[uuid.UUID, datetime, MarketType], dict[str, Decimal]]:
    """Bulk-load source temperatures for all prediction groups.

    Issues one query per group (N queries for N groups).  For the current
    scale (~88 groups = 44 cities × 2 market types) this is acceptable.
    A future optimisation could batch into a single query if needed.

    Returns:
        Dict keyed by ``(city_id, forecast_date, market_type)`` →
        ``{source: temperature}``.
    """
    result: dict[tuple[uuid.UUID, datetime, MarketType], dict[str, Decimal]] = {}
    for group in groups:
        temps = fetch_source_temperatures(
            session,
            city_id=group.city_id,
            forecast_date=group.forecast_date,
            market_type=group.market_type,
        )
        result[(group.city_id, group.forecast_date, group.market_type)] = temps
    return result


# ---------------------------------------------------------------------------
# Market queries
# ---------------------------------------------------------------------------


def fetch_active_market_groups(
    session: Session,
) -> list[PredictionGroup]:
    """Load all active markets and group by (city, date, market_type).

    Returns:
        Sorted list of ``PredictionGroup`` objects.  Markets within each
        group are sorted by bracket bounds for deterministic ordering.
    """
    markets = (
        session.execute(
            select(KalshiMarket).where(
                KalshiMarket.status == MarketStatus.ACTIVE,
            )
        )
        .scalars()
        .all()
    )

    grouped: dict[
        tuple[uuid.UUID, datetime, MarketType], list[KalshiMarket]
    ] = defaultdict(list)
    for market in markets:
        key = (market.city_id, market.forecast_date, market.market_type)
        grouped[key].append(market)

    groups: list[PredictionGroup] = []
    for (city_id, forecast_date, market_type), group_markets in grouped.items():
        sorted_markets = sorted(
            group_markets,
            key=lambda m: (
                m.bracket_low if m.bracket_low is not None else Decimal("-9999"),
                m.bracket_high if m.bracket_high is not None else Decimal("9999"),
            ),
        )
        groups.append(
            PredictionGroup(
                city_id=city_id,
                forecast_date=forecast_date,
                market_type=market_type,
                market_ids=tuple(m.id for m in sorted_markets),
            )
        )

    return sorted(
        groups,
        key=lambda g: (str(g.city_id), g.forecast_date, g.market_type),
    )


def fetch_markets_by_ids(
    session: Session,
    market_ids: tuple[uuid.UUID, ...],
) -> list[KalshiMarket]:
    """Load markets by their primary keys, preserving input order."""
    if not market_ids:
        return []
    markets = (
        session.execute(
            select(KalshiMarket).where(KalshiMarket.id.in_(market_ids))
        )
        .scalars()
        .all()
    )
    order = {mid: idx for idx, mid in enumerate(market_ids)}
    return sorted(markets, key=lambda m: order.get(m.id, 0))


# ---------------------------------------------------------------------------
# Snapshot queries
# ---------------------------------------------------------------------------


def fetch_latest_snapshot_map(
    session: Session,
) -> dict[uuid.UUID, KalshiMarketSnapshot]:
    """Load the latest snapshot per active market using a window function.

    Uses ``ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY timestamp DESC)``
    to efficiently pick the most recent snapshot for each market in a
    single query.

    Returns:
        ``{market_id: snapshot}`` dict.
    """
    ranked = (
        select(
            KalshiMarketSnapshot,
            func.row_number()
            .over(
                partition_by=KalshiMarketSnapshot.market_id,
                order_by=(
                    KalshiMarketSnapshot.timestamp.desc(),
                    KalshiMarketSnapshot.created_at.desc(),
                    KalshiMarketSnapshot.id.asc(),
                ),
            )
            .label("row_num"),
        )
        .subquery()
    )
    latest_alias = aliased(KalshiMarketSnapshot, ranked)
    snapshots = (
        session.execute(
            select(latest_alias).where(ranked.c.row_num == 1)
        )
        .scalars()
        .all()
    )

    return {snapshot.market_id: snapshot for snapshot in snapshots}


# ---------------------------------------------------------------------------
# Prediction queries
# ---------------------------------------------------------------------------


def fetch_predictions_for_model(
    session: Session,
    model_version: str,
) -> dict[tuple[uuid.UUID, datetime, MarketType], Prediction]:
    """Load all predictions for a model version, keyed for O(1) lookup.

    Returns:
        ``{(city_id, forecast_date, market_type): prediction}`` dict.
    """
    predictions = (
        session.execute(
            select(Prediction).where(
                Prediction.model_version == model_version,
            )
        )
        .scalars()
        .all()
    )
    return {
        (p.city_id, p.forecast_date, p.market_type): p
        for p in predictions
    }


# ---------------------------------------------------------------------------
# Upsert helpers (narrow get-or-create with unique-race recovery)
# ---------------------------------------------------------------------------


def get_or_create_prediction(
    session: Session,
    *,
    city_id: uuid.UUID,
    forecast_date: datetime,
    market_type: MarketType,
    model_version: str,
) -> tuple[Prediction, bool]:
    """Get existing prediction or create a new one.

    Handles the unique constraint race condition: if a concurrent process
    inserts between our SELECT and INSERT, we catch the IntegrityError
    and re-query.

    Returns:
        ``(prediction, created)`` tuple.
    """
    existing = session.execute(
        select(Prediction).where(
            Prediction.city_id == city_id,
            Prediction.forecast_date == forecast_date,
            Prediction.market_type == market_type,
            Prediction.model_version == model_version,
        )
    ).scalars().first()

    if existing is not None:
        return existing, False

    try:
        with session.begin_nested():
            prediction = Prediction(
                city_id=city_id,
                forecast_date=forecast_date,
                market_type=market_type,
                model_version=model_version,
                predicted_temp=Decimal("0.00"),
                std_dev=Decimal("0.00"),
                probability_distribution={},
            )
            session.add(prediction)
            session.flush()
            return prediction, True
    except IntegrityError:
        existing = session.execute(
            select(Prediction).where(
                Prediction.city_id == city_id,
                Prediction.forecast_date == forecast_date,
                Prediction.market_type == market_type,
                Prediction.model_version == model_version,
            )
        ).scalar_one()
        return existing, False


def get_or_create_recommendation(
    session: Session,
    *,
    prediction_id: uuid.UUID,
    market_id: uuid.UUID,
) -> tuple[Recommendation, bool]:
    """Get existing recommendation or create a new one.

    Returns:
        ``(recommendation, created)`` tuple.
    """
    existing = session.execute(
        select(Recommendation).where(
            Recommendation.prediction_id == prediction_id,
            Recommendation.market_id == market_id,
        )
    ).scalars().first()

    if existing is not None:
        return existing, False

    try:
        with session.begin_nested():
            recommendation = Recommendation(
                prediction_id=prediction_id,
                market_id=market_id,
                direction=Direction.BUY_YES,
                model_probability=Decimal("0.0000"),
                kalshi_probability=Decimal("0.0000"),
                gap=Decimal("0.0000"),
                expected_value=Decimal("0.0000"),
                risk_score=Decimal("1.0"),
                risk_factors={},
            )
            session.add(recommendation)
            session.flush()
            return recommendation, True
    except IntegrityError:
        existing = session.execute(
            select(Recommendation).where(
                Recommendation.prediction_id == prediction_id,
                Recommendation.market_id == market_id,
            )
        ).scalar_one()
        return existing, False


def get_or_create_paper_trade(
    session: Session,
    *,
    recommendation_id: uuid.UUID,
    entry_price: Decimal,
) -> tuple[PaperTradeFixed, bool]:
    """Get existing paper trade or create a new one.

    Returns:
        ``(paper_trade, created)`` tuple.
    """
    existing = session.execute(
        select(PaperTradeFixed).where(
            PaperTradeFixed.recommendation_id == recommendation_id,
        )
    ).scalar_one_or_none()

    if existing is not None:
        return existing, False

    try:
        with session.begin_nested():
            trade = PaperTradeFixed(
                recommendation_id=recommendation_id,
                entry_price=entry_price,
                contracts_qty=1,
            )
            session.add(trade)
            session.flush()
            return trade, True
    except IntegrityError:
        trade = session.execute(
            select(PaperTradeFixed).where(
                PaperTradeFixed.recommendation_id == recommendation_id,
            )
        ).scalar_one()
        return trade, False
