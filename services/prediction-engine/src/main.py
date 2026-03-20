"""Prediction and recommendation service entry point.

Thin CLI that delegates to the extracted engine modules:
- engine.prediction.run_prediction_cycle
- engine.recommendation.run_recommendation_cycle

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
from enum import StrEnum
from types import FrameType

from dotenv import load_dotenv
from src.config import PredictionConfig, load_prediction_config
from src.engine.prediction import run_prediction_cycle
from src.engine.recommendation import run_recommendation_cycle

from shared.config.errors import WeatherBotError
from shared.config.logging import (
    bind_correlation_id,
    clear_correlation_id,
    generate_correlation_id,
    get_logger,
    setup_logging,
)
from shared.config.settings import get_settings
from shared.db.session import get_session

logger = get_logger("prediction-engine")


class ServiceRole(StrEnum):
    PREDICTION = "prediction"
    RECOMMENDATION = "recommendation"


def _run_cycle(
    role: ServiceRole,
    config: PredictionConfig,
) -> dict[str, int]:
    if role == ServiceRole.PREDICTION:
        return run_prediction_cycle(config, get_session)
    return run_recommendation_cycle(config, get_session)


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return parsed


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
            logger.exception(
                "cycle_failed", role=role, run_id=run_id, **exc.to_log_dict()
            )
            if run_once:
                clear_correlation_id()
                return 1
        except Exception:
            logger.exception(
                "cycle_failed_unexpected", role=role, run_id=run_id
            )
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
