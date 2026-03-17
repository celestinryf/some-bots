"""
Tests for the city seed script.

Verifies idempotent upsert behavior: insert on first run,
update on config change, no duplicate rows.

NOTE: These tests commit real data (seed_cities manages its own transactions),
so each test truncates the cities table for isolation.
"""

from collections.abc import Generator

import pytest
from sqlalchemy import Engine, select, text

from shared.config.cities import CITIES
from shared.config.settings import Settings, reset_settings
from shared.db.models import City
from shared.db.seed import seed_cities
from shared.db.session import get_session, reset_engine

pytestmark = pytest.mark.db


class TestSeedCities:
    @pytest.fixture(autouse=True)
    def _setup_and_cleanup(self, db_settings: Settings, db_engine: Engine) -> Generator[None, None, None]:
        """Point get_settings() at the test DB and truncate cities between tests."""
        reset_settings(db_settings)
        reset_engine()
        # Truncate before each test so seed starts fresh
        with db_engine.connect() as conn:
            conn.execute(text("TRUNCATE cities CASCADE"))
            conn.commit()
        yield
        # Truncate after each test for cleanliness
        with db_engine.connect() as conn:
            conn.execute(text("TRUNCATE cities CASCADE"))
            conn.commit()
        reset_engine()
        reset_settings()

    def test_first_run_inserts_all(self):
        inserted, updated = seed_cities()
        assert inserted == len(CITIES)
        assert updated == 0

        with get_session() as session:
            count = len(session.execute(select(City)).scalars().all())
        assert count == len(CITIES)

    def test_second_run_is_noop(self):
        seed_cities()
        inserted, updated = seed_cities()
        assert inserted == 0
        assert updated == 0

    def test_dry_run_makes_no_changes(self):
        inserted, _updated = seed_cities(dry_run=True)
        assert inserted == len(CITIES)

        with get_session() as session:
            count = len(session.execute(select(City)).scalars().all())
        assert count == 0

    def test_update_on_name_change(self):
        seed_cities()

        # Manually change a city name in the DB
        with get_session() as session:
            city = session.execute(
                select(City).where(City.kalshi_ticker_prefix == "NYC")
            ).scalar_one()
            city.name = "Old York"

        inserted, updated = seed_cities()
        assert inserted == 0
        assert updated == 1

        with get_session() as session:
            refreshed = session.execute(
                select(City).where(City.kalshi_ticker_prefix == "NYC")
            ).scalar_one()
            assert refreshed.name == "New York"
