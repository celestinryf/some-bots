"""Prediction cycle orchestrator.

Coordinates the full prediction pipeline:
1. Load active market groups from DB
2. For each group: load source temperatures, run the model, map brackets
3. Upsert Prediction rows with probability distributions

All DB access is injected via session_factory for testability.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager

from sqlalchemy.orm import Session
from src.config import PredictionConfig
from src.data.queries import (
    fetch_active_market_groups,
    fetch_all_source_temperatures,
    fetch_markets_by_ids,
    get_or_create_prediction,
)
from src.engine.decimal_utils import decimal_from_json, quantize_cents
from src.engine.probability import BracketDef, build_probability_distribution
from src.models.base import PredictionModel
from src.models.selector import select_model

from shared.config.errors import PredictionError
from shared.config.logging import get_logger
from shared.db.models import KalshiMarket

logger = get_logger("prediction-cycle")


def _build_brackets(markets: list[KalshiMarket]) -> list[BracketDef]:
    """Convert market bracket bounds to BracketDef objects."""
    return [
        BracketDef(
            low=market.bracket_low,
            high=market.bracket_high,
            market_id=str(market.id),
        )
        for market in markets
    ]


def run_prediction_cycle(
    config: PredictionConfig,
    session_factory: Callable[[], AbstractContextManager[Session]],
    *,
    model: PredictionModel | None = None,
) -> dict[str, int]:
    """Run one full prediction cycle.

    For each active (city, date, market_type) group:
    1. Load latest source temperatures
    2. Skip if below min_sources_required
    3. Run the model to get (mean, std_dev)
    4. Map to bracket probabilities via Gaussian CDF
    5. Upsert the Prediction row

    Args:
        config: Prediction configuration.
        session_factory: Callable that returns a context-managed Session.
        model: Optional model override (defaults to selector).

    Returns:
        Stats dict with counts of groups seen, upserted, skipped, errored.
    """
    prediction_model = model or select_model()
    stats = {
        "groups_seen": 0,
        "predictions_upserted": 0,
        "groups_skipped": 0,
        "groups_errored": 0,
    }

    with session_factory() as session:
        groups = fetch_active_market_groups(session)
        all_source_temps = fetch_all_source_temperatures(
            session, groups=groups
        )

        for group in groups:
            stats["groups_seen"] += 1

            try:
                source_temps = all_source_temps.get(
                    (group.city_id, group.forecast_date, group.market_type),
                    {},
                )

                if len(source_temps) < config.min_sources_required:
                    stats["groups_skipped"] += 1
                    logger.info(
                        "prediction_skipped_insufficient_sources",
                        city_id=str(group.city_id),
                        forecast_date=group.forecast_date.isoformat(),
                        market_type=group.market_type,
                        source_count=len(source_temps),
                        min_required=config.min_sources_required,
                    )
                    continue

                temps = list(source_temps.values())
                predicted_temp, std_dev = prediction_model.predict(temps, config)

                markets = fetch_markets_by_ids(session, group.market_ids)
                brackets = _build_brackets(markets)

                distribution = build_probability_distribution(
                    temps=temps,
                    brackets=brackets,
                    source_temps=source_temps,
                    std_dev_floor=config.std_dev_floor,
                    probability_sum_tolerance=config.probability_sum_tolerance,
                )

                prediction, _created = get_or_create_prediction(
                    session,
                    city_id=group.city_id,
                    forecast_date=group.forecast_date,
                    market_type=group.market_type,
                    model_version=prediction_model.version,
                )
                prediction.predicted_temp = quantize_cents(
                    decimal_from_json(
                        distribution["mean"], source="prediction-cycle"
                    )
                )
                prediction.std_dev = quantize_cents(
                    decimal_from_json(
                        distribution["std_dev"], source="prediction-cycle"
                    )
                )
                prediction.probability_distribution = distribution
                stats["predictions_upserted"] += 1

                logger.info(
                    "prediction_upserted",
                    prediction_id=str(prediction.id),
                    city_id=str(group.city_id),
                    forecast_date=group.forecast_date.isoformat(),
                    market_type=group.market_type,
                    model_version=prediction_model.version,
                    source_count=len(source_temps),
                )

            except PredictionError as exc:
                stats["groups_errored"] += 1
                logger.error(
                    "prediction_group_failed",
                    city_id=str(group.city_id),
                    forecast_date=group.forecast_date.isoformat(),
                    market_type=group.market_type,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )

    return stats
