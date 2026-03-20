"""
Factory functions for constructing API clients from Settings.

Validates credentials and returns only clients whose API keys are configured.
If a client constructor raises, the error is logged and that client is skipped.
"""

import time
from collections.abc import Callable, Sequence
from pathlib import Path

from src.clients.base import WeatherClient
from src.clients.kalshi import KalshiClient
from src.clients.nws import NwsClient
from src.clients.openweathermap import OpenWeatherMapClient
from src.clients.pirate_weather import PirateWeatherClient
from src.clients.visual_crossing import VisualCrossingClient

from shared.config.errors import KalshiApiError
from shared.config.logging import get_logger
from shared.config.settings import Settings
from shared.db.enums import WeatherSource

logger = get_logger("factories")

# Default gridpoint cache path for NWS client
_DEFAULT_GRIDPOINT_CACHE = Path("./data/nws_gridpoints.json")


def create_weather_clients(
    settings: Settings,
    *,
    gridpoint_cache_path: Path | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> list[WeatherClient]:
    """Create weather API clients for all configured sources.

    Only returns clients whose credentials are present and whose constructors
    succeed. Clients with empty API keys are silently skipped.

    Args:
        settings: Application settings with API keys.
        gridpoint_cache_path: NWS gridpoint cache file path.
            Defaults to ``./data/nws_gridpoints.json``.
        sleep_fn: Sleep function injected into clients (for testing).

    Returns:
        List of successfully created weather clients.
    """
    cache_path = gridpoint_cache_path or _DEFAULT_GRIDPOINT_CACHE
    clients: list[WeatherClient] = []

    # (credential, constructor kwargs, source label)
    client_specs: list[tuple[str, Callable[[], WeatherClient], str]] = [
        (
            settings.nws_user_agent,
            lambda: NwsClient(
                user_agent=settings.nws_user_agent,
                gridpoint_cache_path=cache_path,
                sleep_fn=sleep_fn,
            ),
            WeatherSource.NWS,
        ),
        (
            settings.visual_crossing_api_key,
            lambda: VisualCrossingClient(
                api_key=settings.visual_crossing_api_key,
                sleep_fn=sleep_fn,
            ),
            WeatherSource.VISUAL_CROSSING,
        ),
        (
            settings.pirate_weather_api_key,
            lambda: PirateWeatherClient(
                api_key=settings.pirate_weather_api_key,
                sleep_fn=sleep_fn,
            ),
            WeatherSource.PIRATE_WEATHER,
        ),
        (
            settings.openweather_api_key,
            lambda: OpenWeatherMapClient(
                api_key=settings.openweather_api_key,
                sleep_fn=sleep_fn,
            ),
            WeatherSource.OPENWEATHER,
        ),
    ]

    for credential, constructor, source_label in client_specs:
        if not credential.strip():
            logger.debug("weather_client_skipped", source=source_label, reason="empty_credential")
            continue
        try:
            clients.append(constructor())
        except Exception as exc:
            logger.warning(
                "weather_client_creation_failed",
                source=source_label,
                error=str(exc),
            )

    logger.info(
        "weather_clients_created",
        count=len(clients),
        sources=[c.source for c in clients],
    )
    return clients


def create_kalshi_client(settings: Settings) -> KalshiClient | None:
    """Create a Kalshi API client if credentials are configured.

    Returns:
        KalshiClient instance, or None if credentials are missing or invalid.
    """
    if not settings.kalshi_api_key_id.strip() or not settings.kalshi_key_path.strip():
        logger.info("kalshi_client_skipped", reason="missing_credentials")
        return None

    try:
        client = KalshiClient.from_settings(
            api_key_id=settings.kalshi_api_key_id,
            private_key_path=settings.kalshi_key_path,
        )
    except (KalshiApiError, Exception) as exc:
        logger.warning("kalshi_client_creation_failed", error=str(exc))
        return None

    logger.info("kalshi_client_created")
    return client


def close_clients(
    weather_clients: Sequence[WeatherClient],
    kalshi_client: KalshiClient | None,
) -> None:
    """Close all API clients gracefully.

    Errors during close are logged but never propagated.
    """
    for client in weather_clients:
        try:
            client.close()
        except Exception as exc:
            logger.warning("client_close_error", source=client.source, error=str(exc))

    if kalshi_client is not None:
        try:
            kalshi_client.close()
        except Exception as exc:
            logger.warning("kalshi_close_error", error=str(exc))
