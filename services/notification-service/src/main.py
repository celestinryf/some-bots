"""
Notification service entry point.

Runs a simple digest poller that inspects recommendations and users on a
fixed interval. Email delivery stays configuration-gated by SendGrid.
"""

from __future__ import annotations

import argparse
import signal
import threading
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from types import FrameType

from dotenv import load_dotenv
from sqlalchemy import func, select

from shared.config.errors import WeatherBotError
from shared.config.logging import (
    bind_correlation_id,
    clear_correlation_id,
    generate_correlation_id,
    get_logger,
    setup_logging,
)
from shared.config.settings import get_settings
from shared.db.models import Recommendation, User
from shared.db.session import get_session

logger = get_logger("notification-service")


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return parsed


def run_notification_cycle(*, digest_window_hours: int) -> dict[str, int | bool]:
    window_start = _now_utc() - timedelta(hours=digest_window_hours)

    with get_session() as session:
        recommendation_count = session.execute(
            select(func.count())
            .select_from(Recommendation)
            .where(Recommendation.created_at >= window_start)
        ).scalar_one()
        user_count = session.execute(
            select(func.count()).select_from(User)
        ).scalar_one()

    settings = get_settings()
    sendgrid_configured = bool(settings.sendgrid_api_key)
    logger.info(
        "notification_digest_scanned",
        digest_window_hours=digest_window_hours,
        recommendation_count=recommendation_count,
        user_count=user_count,
        sendgrid_configured=sendgrid_configured,
    )

    if not sendgrid_configured:
        logger.info("notification_delivery_disabled", reason="missing_sendgrid_api_key")

    return {
        "recommendation_count": int(recommendation_count),
        "user_count": int(user_count),
        "sendgrid_configured": sendgrid_configured,
    }


def _run_service_loop(
    *,
    run_once: bool,
    interval_seconds: int,
    digest_window_hours: int,
) -> int:
    shutdown_event = threading.Event()

    def _signal_handler(signum: int, frame: FrameType | None) -> None:
        logger.info("shutdown_signal_received", signal=signum)
        shutdown_event.set()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    while not shutdown_event.is_set():
        run_id = bind_correlation_id(generate_correlation_id())
        logger.info("cycle_started", run_id=run_id)
        try:
            stats = run_notification_cycle(digest_window_hours=digest_window_hours)
            logger.info("cycle_completed", run_id=run_id, **stats)
        except WeatherBotError as exc:
            logger.exception("cycle_failed", run_id=run_id, **exc.to_log_dict())
            if run_once:
                clear_correlation_id()
                return 1
        except Exception:
            logger.exception("cycle_failed_unexpected", run_id=run_id)
            if run_once:
                clear_correlation_id()
                return 1
        finally:
            clear_correlation_id()

        if run_once:
            break
        shutdown_event.wait(interval_seconds)

    logger.info("service_stopped")
    return 0


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Notification service")
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Run one digest scan and exit",
    )
    parser.add_argument(
        "--interval-seconds",
        type=_positive_int,
        default=3600,
        help="Polling interval for daemon mode",
    )
    parser.add_argument(
        "--digest-window-hours",
        type=_positive_int,
        default=24,
        help="Recommendation lookback window",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    load_dotenv()
    settings = get_settings()
    setup_logging(settings.log_level)
    args = parse_args(argv)

    logger.info(
        "service_starting",
        run_once=args.run_once,
        interval_seconds=args.interval_seconds,
        digest_window_hours=args.digest_window_hours,
        environment=settings.environment,
    )

    return _run_service_loop(
        run_once=args.run_once,
        interval_seconds=args.interval_seconds,
        digest_window_hours=args.digest_window_hours,
    )


if __name__ == "__main__":
    raise SystemExit(main())
