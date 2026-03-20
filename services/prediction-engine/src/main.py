"""
Prediction and recommendation service entry point.

Runs one of two explicit roles:

  python -m src.main                           # prediction daemon
  python -m src.main --role recommendation     # recommendation daemon
  python -m src.main --run-once                # single prediction cycle
"""

from __future__ import annotations

import argparse
import signal
import threading
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from types import FrameType
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, aliased

from shared.config.errors import RecommendationError, WeatherBotError
from shared.config.logging import (
    bind_correlation_id,
    clear_correlation_id,
    generate_correlation_id,
    get_logger,
    setup_logging,
)
from shared.config.settings import get_settings
from shared.db.enums import Direction, MarketStatus, MarketType
from shared.db.models import (
    KalshiMarket,
    KalshiMarketSnapshot,
    PaperTradeFixed,
    Prediction,
    Recommendation,
    WeatherForecast,
)
from shared.db.session import get_session

from src.config import PredictionConfig, load_prediction_config
from src.engine.fees import expected_value, kalshi_taker_fee
from src.engine.probability import BracketDef, build_probability_distribution
from src.engine.risk import (
    bracket_edge_score,
    city_accuracy_score,
    compute_risk_score,
    forecast_spread_score,
    lead_time_score,
    liquidity_score,
    source_agreement_score,
)

logger = get_logger("prediction-engine")

_CENT = Decimal("0.01")
_BASIS_POINT = Decimal("0.0001")


class ServiceRole(StrEnum):
    PREDICTION = "prediction"
    RECOMMENDATION = "recommendation"


@dataclass(frozen=True)
class PredictionGroup:
    city_id: Any
    forecast_date: datetime
    market_type: MarketType
    markets: tuple[KalshiMarket, ...]


@dataclass(frozen=True)
class RecommendationCandidate:
    direction: Direction
    model_probability: Decimal
    kalshi_probability: Decimal
    gap: Decimal
    expected_value: Decimal
    entry_price: Decimal


def _quantize_cents(value: Decimal) -> Decimal:
    return value.quantize(_CENT)


def _quantize_probability(value: Decimal) -> Decimal:
    return value.quantize(_BASIS_POINT)


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _market_sort_key(market: KalshiMarket) -> tuple[Decimal, Decimal]:
    low = market.bracket_low if market.bracket_low is not None else Decimal("-9999")
    high = market.bracket_high if market.bracket_high is not None else Decimal("9999")
    return (low, high)


def _format_bracket_key(low: Decimal | None, high: Decimal | None) -> str:
    low_str = str(low) if low is not None else "-inf"
    high_str = str(high) if high is not None else "inf"
    left = "(" if low is None else "["
    return f"{left}{low_str}, {high_str})"


def _decimal_from_json(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int | float | str):
        try:
            return Decimal(str(value))
        except InvalidOperation as exc:
            raise RecommendationError(
                f"Invalid decimal payload: {value!r}",
                source="prediction-engine",
            ) from exc
    raise RecommendationError(
        f"Unsupported decimal payload type: {type(value).__name__}",
        source="prediction-engine",
    )


def _is_valid_price(price: Decimal | None) -> bool:
    return price is not None and Decimal("0") < price < Decimal("1")


def _group_markets_for_predictions(session: Session) -> list[PredictionGroup]:
    markets = session.execute(
        select(KalshiMarket).where(KalshiMarket.status == MarketStatus.ACTIVE)
    ).scalars().all()

    grouped: dict[tuple[Any, datetime, MarketType], list[KalshiMarket]] = {}
    for market in markets:
        key = (market.city_id, market.forecast_date, market.market_type)
        grouped.setdefault(key, []).append(market)

    groups: list[PredictionGroup] = []
    for (city_id, forecast_date, market_type), group_markets in grouped.items():
        groups.append(
            PredictionGroup(
                city_id=city_id,
                forecast_date=forecast_date,
                market_type=market_type,
                markets=tuple(sorted(group_markets, key=_market_sort_key)),
            )
        )
    return sorted(groups, key=lambda group: (str(group.city_id), group.forecast_date, group.market_type))


def _load_source_temperatures(
    session: Session,
    *,
    city_id: Any,
    forecast_date: datetime,
    market_type: MarketType,
) -> dict[str, Decimal]:
    forecasts = session.execute(
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
    ).scalars().all()

    latest_by_source: dict[str, Decimal] = {}
    for forecast in forecasts:
        if forecast.source in latest_by_source:
            continue
        temp = forecast.temp_high if market_type == MarketType.HIGH else forecast.temp_low
        if temp is not None:
            latest_by_source[forecast.source] = temp

    return latest_by_source


def _load_existing_predictions(
    session: Session,
    *,
    city_id: Any,
    forecast_date: datetime,
    market_type: MarketType,
    model_version: str,
) -> list[Prediction]:
    return session.execute(
        select(Prediction)
        .where(
            Prediction.city_id == city_id,
            Prediction.forecast_date == forecast_date,
            Prediction.market_type == market_type,
            Prediction.model_version == model_version,
        )
        .order_by(Prediction.created_at.asc(), Prediction.id.asc())
    ).scalars().all()


def _get_or_create_prediction(
    session: Session,
    *,
    city_id: Any,
    forecast_date: datetime,
    market_type: MarketType,
    model_version: str,
) -> Prediction:
    existing = _load_existing_predictions(
        session,
        city_id=city_id,
        forecast_date=forecast_date,
        market_type=market_type,
        model_version=model_version,
    )
    if existing:
        if len(existing) > 1:
            logger.warning(
                "duplicate_predictions_detected",
                city_id=str(city_id),
                forecast_date=forecast_date.isoformat(),
                market_type=market_type,
                model_version=model_version,
                duplicate_count=len(existing),
            )
        return existing[0]

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
            return prediction
    except IntegrityError:
        existing = _load_existing_predictions(
            session,
            city_id=city_id,
            forecast_date=forecast_date,
            market_type=market_type,
            model_version=model_version,
        )
        if not existing:
            raise
        if len(existing) > 1:
            logger.warning(
                "duplicate_predictions_detected",
                city_id=str(city_id),
                forecast_date=forecast_date.isoformat(),
                market_type=market_type,
                model_version=model_version,
                duplicate_count=len(existing),
            )
        return existing[0]


def run_prediction_cycle(config: PredictionConfig) -> dict[str, int]:
    stats = {
        "groups_seen": 0,
        "predictions_upserted": 0,
        "groups_skipped": 0,
    }

    with get_session() as session:
        for group in _group_markets_for_predictions(session):
            stats["groups_seen"] += 1
            source_temps = _load_source_temperatures(
                session,
                city_id=group.city_id,
                forecast_date=group.forecast_date,
                market_type=group.market_type,
            )
            if len(source_temps) < config.min_sources_required:
                stats["groups_skipped"] += 1
                logger.info(
                    "prediction_skipped_insufficient_sources",
                    city_id=str(group.city_id),
                    forecast_date=group.forecast_date.isoformat(),
                    market_type=group.market_type,
                    source_count=len(source_temps),
                    min_sources_required=config.min_sources_required,
                )
                continue

            brackets = [
                BracketDef(
                    low=market.bracket_low,
                    high=market.bracket_high,
                    market_id=str(market.id),
                )
                for market in group.markets
            ]
            distribution = build_probability_distribution(
                temps=list(source_temps.values()),
                brackets=brackets,
                source_temps=source_temps,
                std_dev_floor=config.std_dev_floor,
                probability_sum_tolerance=config.probability_sum_tolerance,
            )

            prediction = _get_or_create_prediction(
                session,
                city_id=group.city_id,
                forecast_date=group.forecast_date,
                market_type=group.market_type,
                model_version=config.model_version,
            )
            prediction.predicted_temp = _quantize_cents(_decimal_from_json(distribution["mean"]))
            prediction.std_dev = _quantize_cents(_decimal_from_json(distribution["std_dev"]))
            prediction.probability_distribution = distribution
            stats["predictions_upserted"] += 1

            logger.info(
                "prediction_upserted",
                prediction_id=str(prediction.id),
                city_id=str(group.city_id),
                forecast_date=group.forecast_date.isoformat(),
                market_type=group.market_type,
                source_count=len(source_temps),
            )

    return stats


def _load_latest_snapshot_map(session: Session) -> dict[Any, KalshiMarketSnapshot]:
    ranked_snapshots = select(
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
    ).subquery()
    latest_snapshot = aliased(KalshiMarketSnapshot, ranked_snapshots)
    snapshots = session.execute(
        select(latest_snapshot).where(ranked_snapshots.c.row_num == 1)
    ).scalars().all()

    latest_by_market: dict[Any, KalshiMarketSnapshot] = {}
    for snapshot in snapshots:
        latest_by_market.setdefault(snapshot.market_id, snapshot)
    return latest_by_market


def _get_prediction_probability(prediction: Prediction, market: KalshiMarket) -> Decimal:
    distribution = prediction.probability_distribution or {}
    raw_brackets = distribution.get("brackets")
    if not isinstance(raw_brackets, dict):
        raise RecommendationError(
            f"Prediction {prediction.id} missing bracket probabilities",
            source="prediction-engine",
        )

    bracket_key = _format_bracket_key(market.bracket_low, market.bracket_high)
    raw_probability = raw_brackets.get(bracket_key)
    if raw_probability is None:
        raise RecommendationError(
            f"Prediction {prediction.id} has no probability for market {market.id}",
            source="prediction-engine",
        )

    probability = _decimal_from_json(raw_probability)
    if probability < 0 or probability > 1:
        raise RecommendationError(
            f"Prediction {prediction.id} has invalid bracket probability {probability}",
            source="prediction-engine",
        )
    return _quantize_probability(probability)


def _build_recommendation_candidate(
    direction: Direction,
    *,
    model_win_probability: Decimal,
    entry_price: Decimal,
) -> RecommendationCandidate:
    fee = kalshi_taker_fee(contracts=1, price=entry_price)
    trade_ev = expected_value(model_win_probability, entry_price, fee)
    gap = model_win_probability - entry_price
    return RecommendationCandidate(
        direction=direction,
        model_probability=_quantize_probability(model_win_probability),
        kalshi_probability=_quantize_probability(entry_price),
        gap=_quantize_probability(gap),
        expected_value=_quantize_probability(trade_ev),
        entry_price=_quantize_probability(entry_price),
    )


def _select_recommendation_candidate(
    *,
    model_probability: Decimal,
    snapshot: KalshiMarketSnapshot,
    config: PredictionConfig,
) -> RecommendationCandidate | None:
    candidates: list[RecommendationCandidate] = []

    if _is_valid_price(snapshot.yes_ask):
        candidates.append(
            _build_recommendation_candidate(
                Direction.BUY_YES,
                model_win_probability=model_probability,
                entry_price=snapshot.yes_ask,
            )
        )

    if _is_valid_price(snapshot.no_ask):
        candidates.append(
            _build_recommendation_candidate(
                Direction.BUY_NO,
                model_win_probability=Decimal("1") - model_probability,
                entry_price=snapshot.no_ask,
            )
        )

    eligible = [
        candidate
        for candidate in candidates
        if candidate.gap >= config.gap_threshold
        and candidate.expected_value >= config.min_ev_threshold
    ]
    if not eligible:
        return None

    return max(
        eligible,
        key=lambda candidate: (candidate.expected_value, candidate.gap),
    )


def _load_source_temps_from_prediction(prediction: Prediction) -> list[Decimal]:
    distribution = prediction.probability_distribution or {}
    raw_source_temps = distribution.get("source_temps")
    if not isinstance(raw_source_temps, dict):
        return []

    temps: list[Decimal] = []
    for value in raw_source_temps.values():
        temps.append(_quantize_cents(_decimal_from_json(value)))
    return temps


def _compute_risk_payload(
    *,
    prediction: Prediction,
    market: KalshiMarket,
    snapshot: KalshiMarketSnapshot,
    config: PredictionConfig,
) -> tuple[Decimal, dict[str, Decimal]]:
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
            _now_utc().date(),
        ),
    }
    return compute_risk_score(factors, config.risk_weights), factors


def _get_or_create_recommendation(
    session: Session,
    *,
    prediction_id: Any,
    market_id: Any,
) -> tuple[Recommendation, bool]:
    existing = session.execute(
        select(Recommendation)
        .where(
            Recommendation.prediction_id == prediction_id,
            Recommendation.market_id == market_id,
        )
        .order_by(Recommendation.created_at.asc())
    ).scalars().all()
    if existing:
        if len(existing) > 1:
            logger.warning(
                "duplicate_recommendations_detected",
                prediction_id=str(prediction_id),
                market_id=str(market_id),
                duplicate_count=len(existing),
            )
        return existing[0], False

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
        recommendation = session.execute(
            select(Recommendation)
            .where(
                Recommendation.prediction_id == prediction_id,
                Recommendation.market_id == market_id,
            )
            .order_by(Recommendation.created_at.asc())
        ).scalar_one()
        return recommendation, False


def _ensure_fixed_paper_trade(
    session: Session,
    *,
    recommendation: Recommendation,
    entry_price: Decimal,
) -> tuple[PaperTradeFixed, bool]:
    existing = session.execute(
        select(PaperTradeFixed).where(
            PaperTradeFixed.recommendation_id == recommendation.id
        )
    ).scalar_one_or_none()

    if existing is not None:
        return existing, False

    try:
        with session.begin_nested():
            trade = PaperTradeFixed(
                recommendation_id=recommendation.id,
                entry_price=entry_price,
                contracts_qty=1,
            )
            session.add(trade)
            session.flush()
            return trade, True
    except IntegrityError:
        trade = session.execute(
            select(PaperTradeFixed).where(
                PaperTradeFixed.recommendation_id == recommendation.id
            )
        ).scalar_one()
        return trade, False


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return parsed


def run_recommendation_cycle(config: PredictionConfig) -> dict[str, int]:
    stats = {
        "markets_seen": 0,
        "recommendations_created": 0,
        "recommendations_reused": 0,
        "paper_trades_created": 0,
        "markets_skipped": 0,
    }

    with get_session() as session:
        predictions = session.execute(
            select(Prediction).where(Prediction.model_version == config.model_version)
        ).scalars().all()
        prediction_map = {
            (prediction.city_id, prediction.forecast_date, prediction.market_type): prediction
            for prediction in predictions
        }
        latest_snapshots = _load_latest_snapshot_map(session)

        markets = session.execute(
            select(KalshiMarket).where(KalshiMarket.status == MarketStatus.ACTIVE)
        ).scalars().all()

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

            model_probability = _get_prediction_probability(prediction, market)
            candidate = _select_recommendation_candidate(
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

            risk_score, risk_factors = _compute_risk_payload(
                prediction=prediction,
                market=market,
                snapshot=snapshot,
                config=config,
            )
            recommendation, created = _get_or_create_recommendation(
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
            recommendation.risk_factors = {
                name: float(value)
                for name, value in risk_factors.items()
            }

            trade, trade_created = _ensure_fixed_paper_trade(
                session,
                recommendation=recommendation,
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

    return stats


def _run_cycle(role: ServiceRole, config: PredictionConfig) -> dict[str, int]:
    if role == ServiceRole.PREDICTION:
        return run_prediction_cycle(config)
    return run_recommendation_cycle(config)


def _run_service_loop(
    *,
    role: ServiceRole,
    config: PredictionConfig,
    run_once: bool,
    interval_seconds: int,
) -> int:
    shutdown_event = threading.Event()

    def _signal_handler(signum: int, frame: FrameType | None) -> None:
        logger.info("shutdown_signal_received", role=role, signal=signum)
        shutdown_event.set()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    while not shutdown_event.is_set():
        run_id = bind_correlation_id(generate_correlation_id())
        logger.info("cycle_started", role=role, run_id=run_id)
        try:
            stats = _run_cycle(role, config)
            logger.info("cycle_completed", role=role, run_id=run_id, **stats)
        except WeatherBotError as exc:
            logger.exception("cycle_failed", role=role, run_id=run_id, **exc.to_log_dict())
            if run_once:
                clear_correlation_id()
                return 1
        except Exception:
            logger.exception("cycle_failed_unexpected", role=role, run_id=run_id)
            if run_once:
                clear_correlation_id()
                return 1
        finally:
            clear_correlation_id()

        if run_once:
            break
        shutdown_event.wait(interval_seconds)

    logger.info("service_stopped", role=role)
    return 0


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prediction engine service")
    parser.add_argument(
        "--role",
        choices=[role.value for role in ServiceRole],
        default=ServiceRole.PREDICTION.value,
        help="Execution role for this process",
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Run one cycle and exit",
    )
    parser.add_argument(
        "--interval-seconds",
        type=_positive_int,
        default=300,
        help="Polling interval for daemon mode",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    load_dotenv()
    settings = get_settings()
    setup_logging(settings.log_level)
    config = load_prediction_config()
    args = parse_args(argv)
    role = ServiceRole(args.role)

    logger.info(
        "service_starting",
        role=role,
        run_once=args.run_once,
        interval_seconds=args.interval_seconds,
        environment=settings.environment,
    )

    return _run_service_loop(
        role=role,
        config=config,
        run_once=args.run_once,
        interval_seconds=args.interval_seconds,
    )


if __name__ == "__main__":
    raise SystemExit(main())
