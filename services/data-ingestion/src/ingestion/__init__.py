"""
Ingestion orchestration: weather forecast collection and Kalshi market tracking.

Job functions accept their dependencies as arguments for testability.
The main.py entrypoint constructs real objects and passes them in.
"""

from .factories import close_clients, create_kalshi_client, create_weather_clients
from .kalshi import run_kalshi_discovery, run_kalshi_settlements, run_kalshi_snapshot_cleanup, run_kalshi_snapshots
from .weather import run_weather_ingestion

__all__ = [
    "close_clients",
    "create_kalshi_client",
    "create_weather_clients",
    "run_kalshi_discovery",
    "run_kalshi_settlements",
    "run_kalshi_snapshot_cleanup",
    "run_kalshi_snapshots",
    "run_weather_ingestion",
]
