"""
Fixtures for database integration tests.

Requires a running PostgreSQL instance. Uses the DB from Settings (override
with DB_* env vars or reset_settings). Each test runs in a rolled-back
transaction so tests don't interfere with each other.
"""

import os
from collections.abc import Generator

import pytest
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import Session

from shared.config.settings import Settings
from shared.db.models import Base


@pytest.fixture(scope="session")
def db_engine() -> Generator[Engine, None, None]:
    """Create a test engine once per test session.

    Reads connection details from DB_* env vars so the fixture respects CI
    and containerised overrides (e.g. ``DB_HOST=postgres``).
    """
    settings = Settings(
        db_host=os.environ.get("DB_HOST", "localhost"),
        db_port=int(os.environ.get("DB_PORT", "5432")),
        db_name=os.environ.get("DB_NAME", "kalshi_weather_test"),
        db_user=os.environ.get("DB_USER", "kalshi"),
        db_password=os.environ.get("DB_PASSWORD", "test"),
    )
    if settings.db_name != "kalshi_weather_test":
        raise RuntimeError(
            f"Refusing to run integration tests against non-test database "
            f"{settings.db_name!r}"
        )
    engine = create_engine(settings.database_url, echo=False)

    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))

    # Ensure tables exist as a fallback (IF NOT EXISTS — safe alongside Alembic)
    Base.metadata.create_all(engine)

    # Clear residual data for a clean test session
    with engine.begin() as conn:
        for table in reversed(Base.metadata.sorted_tables):
            conn.execute(table.delete())

    yield engine

    engine.dispose()


@pytest.fixture()
def db_session(db_engine: Engine) -> Generator[Session, None, None]:
    """Yield a session wrapped in a transaction that rolls back after each test."""
    connection = db_engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection)

    yield session

    session.close()
    if transaction.is_active:
        transaction.rollback()
    connection.close()


@pytest.fixture()
def db_settings() -> Settings:
    """Return Settings pointing to the test database."""
    return Settings(
        db_host=os.environ.get("DB_HOST", "localhost"),
        db_port=int(os.environ.get("DB_PORT", "5432")),
        db_name=os.environ.get("DB_NAME", "kalshi_weather_test"),
        db_user=os.environ.get("DB_USER", "kalshi"),
        db_password=os.environ.get("DB_PASSWORD", "test"),
    )
