"""
Fixtures for prediction-engine database integration tests.

Requires a running PostgreSQL instance. Uses the DB from Settings (override
with DB_* env vars). Each test runs in a rolled-back transaction so tests
don't interfere with each other.
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
    """Create a test engine once per test session. Drops/creates all tables."""
    settings = Settings(
        db_host="localhost",
        db_port=5432,
        db_name="kalshi_weather_test",
        db_user="kalshi",
        db_password=os.environ.get("DB_PASSWORD", "k4lsh1_w34th3r_b0t_2026"),
    )
    engine = create_engine(settings.database_url, echo=False)

    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))

    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    yield engine

    Base.metadata.drop_all(engine)
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
