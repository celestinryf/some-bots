"""
Centralized settings loaded from environment variables.

All secrets and configuration flow through here. Services import get_settings()
rather than reading os.environ directly — single source of truth, easy to test.
"""

import os
from dataclasses import dataclass
from enum import StrEnum

from sqlalchemy.engine import URL


class Environment(StrEnum):
    DEVELOPMENT = "development"
    TEST = "test"
    PRODUCTION = "production"


def _parse_environment(raw: str | Environment) -> Environment:
    if isinstance(raw, Environment):
        return raw

    normalized = raw.strip().lower()
    try:
        return Environment(normalized)
    except ValueError as exc:
        valid_values = ", ".join(environment.value for environment in Environment)
        raise ValueError(
            f"ENVIRONMENT must be one of: {valid_values}; got {raw!r}"
        ) from exc


@dataclass(frozen=True)
class Settings:
    # Kalshi
    kalshi_api_key_id: str = ""
    kalshi_key_path: str = ""

    # Weather APIs
    visual_crossing_api_key: str = ""
    pirate_weather_api_key: str = ""
    openweather_api_key: str = ""
    nws_user_agent: str = ""

    # Database
    db_host: str = "postgres"
    db_port: int = 5432
    db_name: str = "kalshi_weather"
    db_user: str = "kalshi"
    db_password: str = ""

    # SendGrid (Sprint 3)
    sendgrid_api_key: str = ""

    # JWT (Sprint 4)
    jwt_secret: str = ""

    # App
    environment: Environment = Environment.DEVELOPMENT
    log_level: str = "INFO"

    def __post_init__(self) -> None:
        environment = _parse_environment(self.environment)
        object.__setattr__(self, "environment", environment)

        if environment is Environment.PRODUCTION:
            missing = [
                name
                for name in (
                    "db_password",
                    "visual_crossing_api_key",
                    "pirate_weather_api_key",
                    "openweather_api_key",
                    "nws_user_agent",
                    "kalshi_api_key_id",
                    "kalshi_key_path",
                )
                if not getattr(self, name)
            ]
            if missing:
                raise ValueError(
                    f"Required secrets missing in production: {', '.join(missing)}"
                )

    @property
    def database_url(self) -> URL:
        """SQLAlchemy URL object — redacts password in str()/repr()/logs."""
        return URL.create(
            drivername="postgresql",
            username=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port,
            database=self.db_name,
        )

    @property
    def database_url_with_ssl(self) -> URL:
        return self.database_url.set(query={"sslmode": "require"})

    @property
    def is_production(self) -> bool:
        return self.environment is Environment.PRODUCTION


def _parse_db_port() -> int:
    """Parse and validate DB_PORT from environment."""
    raw = os.environ.get("DB_PORT", "5432")
    try:
        port = int(raw)
    except ValueError:
        raise ValueError(f"DB_PORT must be an integer, got: {raw!r}") from None
    if not (1 <= port <= 65535):
        raise ValueError(f"DB_PORT must be 1-65535, got: {port}")
    return port


def _load_from_env() -> Settings:
    """Load settings from environment variables. Missing vars get defaults."""
    return Settings(
        kalshi_api_key_id=os.environ.get("KALSHI_API_KEY_ID", ""),
        kalshi_key_path=os.environ.get("KALSHI_KEY_PATH", ""),
        visual_crossing_api_key=os.environ.get("VISUAL_CROSSING_API_KEY", ""),
        pirate_weather_api_key=os.environ.get("PIRATE_WEATHER_API_KEY", ""),
        openweather_api_key=os.environ.get("OPENWEATHER_API_KEY", ""),
        nws_user_agent=os.environ.get("NWS_USER_AGENT", ""),
        db_host=os.environ.get("DB_HOST", "postgres"),
        db_port=_parse_db_port(),
        db_name=os.environ.get("DB_NAME", "kalshi_weather"),
        db_user=os.environ.get("DB_USER", "kalshi"),
        db_password=os.environ.get("DB_PASSWORD", ""),
        sendgrid_api_key=os.environ.get("SENDGRID_API_KEY", ""),
        jwt_secret=os.environ.get("JWT_SECRET", ""),
        environment=_parse_environment(os.environ.get("ENVIRONMENT", "development")),
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
    )


_settings: Settings | None = None


def get_settings() -> Settings:
    """Return cached settings instance. Loads from env on first call."""
    global _settings
    if _settings is None:
        _settings = _load_from_env()
    return _settings


def reset_settings(override: Settings | None = None) -> None:
    """Reset cached settings. Pass an override for testing, or None to reload from env."""
    global _settings
    _settings = override
