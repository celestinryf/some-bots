"""
API clients for weather data and Kalshi market data.

Weather clients inherit from WeatherClient and return ForecastResult.
KalshiClient provides market discovery, price snapshots, and settlement tracking.
"""

from .base import WeatherClient
from .kalshi import (
    DiscoveredMarket,
    KalshiClient,
    MarketSnapshot,
    SettledMarket,
)
from .models import ForecastResult, ParsedForecast
from .nws import NwsClient
from .openweathermap import OpenWeatherMapClient
from .pirate_weather import PirateWeatherClient
from .visual_crossing import VisualCrossingClient

__all__ = [
    "DiscoveredMarket",
    "ForecastResult",
    "KalshiClient",
    "MarketSnapshot",
    "NwsClient",
    "OpenWeatherMapClient",
    "ParsedForecast",
    "PirateWeatherClient",
    "SettledMarket",
    "VisualCrossingClient",
    "WeatherClient",
]
