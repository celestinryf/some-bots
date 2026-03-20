"""Recommendation cycle orchestrator.

Coordinates the full recommendation pipeline:
1. Load predictions + active markets + latest snapshots
2. For each market: extract bracket probability, compute gap/EV, filter
3. Compute risk score, upsert Recommendation, auto-create PaperTrade

All DB access is injected via session_factory for testability.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import select as sa_select
from sqlalchemy.orm import Session

from shared.config.errors import RecommendationError
from shared.config.logging import get_logger
from shared.db.enums import Direction, MarketStatus
from shared.db.models import (
    KalshiMarket,
    KalshiMarketSnapshot,
    Prediction,
)

from src.config import PredictionConfig
from src.data.queries import (
    fetch_latest_snapshot_map,
    fetch_predictions_for_model,
    get_or_create_paper_trade,
    get_or_create_recommendation,
)
from src.engine.decimal_utils import (
    decimal_from_json,
    quantize_cents,
    quantize_probability,
)
from src.engine.fees import expected_value, kalshi_taker_fee
from src.engine.probability import BracketDef
from src.engine.risk import (
    bracket_edge_score,
    city_accuracy_score,
    compute_risk_score,
    forecast_spread_score,
    lead_time_score,
    liquidity_score,
    source_agreement_score,
)
from src.engine.types import RecommendationCandidate

logger = get_logger("recommendation-cycle")


def _is_valid_price(price: Decimal | None) -> bool:
    """Return True if price is a tradeable Kalshi price (0, 1) exclusive."""
    return price is not None and Decimal("0") < price < Decimal("1")


def _get_prediction_probability(
    prediction: Prediction,
    market: KalshiMarket,
) -> Decimal:
    """Extract bracket probability from a prediction's JSONB distribution."""
    distribution = prediction.probability_distribution or {}
    raw_brackets = distribution.get("brackets")
    if not isinstance(raw_brackets, dict):
        raise RecommendationError(
            f"Prediction {prediction.id} missing bracket probabilities",
            source="recommendation-cycle",
        )

    bracket = BracketDef(
        low=market.bracket_low,
        high=market.bracket_high,
        market_id=str(market.id),
    )
    raw_probability = raw_brackets.get(bracket.key)
    if raw_probability is None:
        raise RecommendationError(
            f"Prediction {prediction.id} has no probability for "
            f"bracket {bracket.key}",
            source="recommendation-cycle",
        )

    probability = decimal_from_json(
        raw_probability, source="recommendation-cycle"
    )
    if probability < 0 or probability > 1:
        raise RecommendationError(
            f"Prediction {prediction.id} has invalid bracket "
            f"probability {probability}",
            source="recommendation-cycle",
        )
    return quantize_probability(probability)


def _build_candidate(
    direction: Direction,
    *,
    model_win_probability: Decimal,
    entry_price: Decimal,
) -> RecommendationCandidate:
    """Build a recommendation candidate with fee and EV calculation."""
    fee = kalshi_taker_fee(contracts=1, price=entry_price)
    trade_ev = expected_value(model_win_probability, entry_price, fee)
    gap = model_win_probability - entry_price
    return RecommendationCandidate(
        direction=direction,
        model_probability=quantize_probability(model_win_probability),
        kalshi_probability=quantize_probability(entry_price),
        gap=quantize_probability(gap),
        expected_value=quantize_probability(trade_ev),
        entry_price=quantize_probability(entry_price),
    )


def _select_best_candidate(
    *,
    model_probability: Decimal,
    snapshot: KalshiMarketSnapshot,
    config: PredictionConfig,
) -> RecommendationCandidate | None:
    """Find the best trade direction that exceeds gap + EV thresholds."""
    candidates: list[RecommendationCandidate] = []

    if _is_valid_price(snapshot.yes_ask):
        candidates.append(
            _build_candidate(
                Direction.BUY_YES,
                model_win_probability=model_probability,
                entry_price=snapshot.yes_ask,
            )
        )

    if _is_valid_price(snapshot.no_ask):
        candidates.append(
            _build_candidate(
                Direction.BUY_NO,
                model_win_probability=Decimal("1") - model_probability,
                entry_price=snapshot.no_ask,
            )
        )

    eligible = [
        c
        for c in candidates
        if c.gap >= config.gap_threshold
        and c.expected_value >= config.min_ev_threshold
    ]
    if not eligible:
        return None

    return max(eligible, key=lambda c: (c.expected_value, c.gap))


def _load_source_temps_from_prediction(
    prediction: Prediction,
) -> list[Decimal]:
    """Extract source temperatures from prediction JSONB for risk scoring."""
    distribution = prediction.probability_distribution or {}
    raw_source_temps = distribution.get("source_temps")
    if not isinstance(raw_source_temps, dict):
        return []

    return [
        quantize_cents(decimal_from_json(value, source="recommendation-cycle"))
        for value in raw_source_temps.values()
    ]


def _compute_risk(
    *,
    prediction: Prediction,
    market: KalshiMarket,
    snapshot: KalshiMarketSnapshot,
    config: PredictionConfig,
    now: datetime,
) -> tuple[Decimal, dict[str, Decimal]]:
    """Compute the 6-factor risk score for a recommendation."""
    source_temps = _load_source_temps_from_prediction(prediction)
    factors = {
        "forecast_spread": forecast_spread_score(source_temps),
        "source_agreement": source_agreement_score(
            source_temps,
            market.bracket_low,
            market.bracket_high,
        ),
        "city_accuracy": city_accuracy_score(None),
        "liquidity": liquidity_score(snapshot.volume or 0),
        "bracket_edge": bracket_edge_score(
            prediction.predicted_temp,
            market.bracket_low,
            market.bracket_high,
        ),
        "lead_time": lead_time_score(
            market.forecast_date.date(),
            now.date(),
        ),
    }
    return compute_risk_score(factors, config.risk_weights), factors


def _now_utc() -> datetime:
    """Return current UTC time. Extracted for test injection."""
    return datetime.now(UTC)


def run_recommendation_cycle(
    config: PredictionConfig,
    session_factory: Callable[[], AbstractContextManager[Session]],
    *,
    now_fn: Callable[[], datetime] = _now_utc,
) -> dict[str, int]:
    """Run one full recommendation cycle.

    For each active market with a matching prediction and snapshot:
    1. Extract bracket probability from prediction distribution
    2. Evaluate BUY_YES and BUY_NO candidates
    3. Filter by gap + EV thresholds
    4. Compute 6-factor risk score
    5. Upsert Recommendation and auto-create PaperTradeFixed

    Args:
        config: Prediction configuration.
        session_factory: Callable that returns a context-managed Session.
        now_fn: Clock function for risk scoring (injectable for tests).

    Returns:
        Stats dict with counts of markets seen, created, skipped, etc.
    """
    stats = {
        "markets_seen": 0,
        "recommendations_created": 0,
        "recommendations_reused": 0,
        "paper_trades_created": 0,
        "markets_skipped": 0,
        "markets_errored": 0,
    }

    now = now_fn()

    with session_factory() as session:
        prediction_map = fetch_predictions_for_model(
            session, config.model_version
        )
        latest_snapshots = fetch_latest_snapshot_map(session)
        markets = (
            session.execute(
                sa_select(KalshiMarket).where(
                    KalshiMarket.status == MarketStatus.ACTIVE
                )
            )
            .scalars()
            .all()
        )

        for market in markets:
            stats["markets_seen"] += 1

            prediction = prediction_map.get(
                (market.city_id, market.forecast_date, market.market_type)
            )
            snapshot = latest_snapshots.get(market.id)

            if prediction is None or snapshot is None:
                stats["markets_skipped"] += 1
                logger.info(
                    "recommendation_skipped_missing_inputs",
                    market_id=str(market.id),
                    has_prediction=prediction is not None,
                    has_snapshot=snapshot is not None,
                )
                continue

            try:
                model_probability = _get_prediction_probability(
                    prediction, market
                )
                candidate = _select_best_candidate(
                    model_probability=model_probability,
                    snapshot=snapshot,
                    config=config,
                )

                if candidate is None:
                    stats["markets_skipped"] += 1
                    logger.info(
                        "recommendation_skipped_thresholds",
                        market_id=str(market.id),
                        prediction_id=str(prediction.id),
                    )
                    continue

                risk_score, risk_factors = _compute_risk(
                    prediction=prediction,
                    market=market,
                    snapshot=snapshot,
                    config=config,
                    now=now,
                )

                recommendation, created = get_or_create_recommendation(
                    session,
                    prediction_id=prediction.id,
                    market_id=market.id,
                )

                recommendation.direction = candidate.direction
                recommendation.model_probability = candidate.model_probability
                recommendation.kalshi_probability = candidate.kalshi_probability
                recommendation.gap = candidate.gap
                recommendation.expected_value = candidate.expected_value
                recommendation.risk_score = risk_score
                recommendation.risk_factors = {}
                for name, value in risk_factors.items():
                    fvalue = float(value)
                    if not math.isfinite(fvalue):
                        raise RecommendationError(
                            f"Risk factor '{name}' produced non-finite "
                            f"value: {fvalue}",
                            source="recommendation-cycle",
                        )
                    recommendation.risk_factors[name] = fvalue

                trade, trade_created = get_or_create_paper_trade(
                    session,
                    recommendation_id=recommendation.id,
                    entry_price=candidate.entry_price,
                )

                if created:
                    stats["recommendations_created"] += 1
                else:
                    stats["recommendations_reused"] += 1
                if trade_created:
                    stats["paper_trades_created"] += 1

                logger.info(
                    "recommendation_upserted",
                    recommendation_id=str(recommendation.id),
                    prediction_id=str(prediction.id),
                    market_id=str(market.id),
                    direction=recommendation.direction,
                    paper_trade_id=str(trade.id),
                    created=created,
                    paper_trade_created=trade_created,
                )

            except Exception as exc:
                stats["markets_errored"] += 1
                logger.error(
                    "recommendation_market_failed",
                    market_id=str(market.id),
                    error=str(exc),
                    error_type=type(exc).__name__,
                )

    return stats
