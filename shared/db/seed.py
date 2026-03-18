"""
Seed the cities table from the canonical cities.py config.

Idempotent: uses kalshi_ticker_prefix as the natural key. Existing rows are
updated if the source data changed; new rows are inserted. No rows are deleted
(a city removed from CITIES stays in the DB to preserve FK integrity).

Usage:
    python -m shared.db.seed          # seeds from .env database
    python -m shared.db.seed --dry-run  # prints what would change without writing
"""

import argparse

from sqlalchemy import select

from shared.config.cities import CITIES
from shared.config.logging import get_logger, setup_logging
from shared.db.models import City
from shared.db.session import get_session

logger = get_logger("db-seed")


class _DryRunRollback(Exception):
    """Raised inside get_session() to trigger rollback instead of commit."""


def seed_cities(*, dry_run: bool = False) -> tuple[int, int]:
    """Upsert all cities from CITIES config into the database.

    Returns:
        (inserted, updated) counts.
    """
    inserted = 0
    updated = 0

    try:
        with get_session() as session:
            existing = {
                city.kalshi_ticker_prefix: city
                for city in session.execute(select(City)).scalars().all()
            }

            for code, cfg in CITIES.items():
                db_city = existing.get(code)

                if db_city is None:
                    if not dry_run:
                        session.add(City(
                            name=cfg.name,
                            kalshi_ticker_prefix=cfg.kalshi_ticker_code,
                            nws_station_id=cfg.nws_station_id,
                            timezone=cfg.timezone,
                            lat=cfg.lat,
                            lon=cfg.lon,
                        ))
                    inserted += 1
                    logger.info("insert_city", city=cfg.name, code=code, dry_run=dry_run)
                else:
                    changed = False
                    for field, attr in [
                        ("name", "name"),
                        ("nws_station_id", "nws_station_id"),
                        ("timezone", "timezone"),
                        ("lat", "lat"),
                        ("lon", "lon"),
                    ]:
                        new_val = getattr(cfg, field)
                        if getattr(db_city, attr) != new_val:
                            if not dry_run:
                                setattr(db_city, attr, new_val)
                            changed = True

                    if changed:
                        updated += 1
                        logger.info("update_city", city=cfg.name, code=code, dry_run=dry_run)

            if dry_run:
                raise _DryRunRollback()
    except _DryRunRollback:
        pass  # get_session() rolled back the transaction

    logger.info("seed_complete", inserted=inserted, updated=updated, dry_run=dry_run)
    return inserted, updated


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed cities table from cities.py config")
    parser.add_argument("--dry-run", action="store_true", help="Print changes without writing to DB")
    args = parser.parse_args()

    setup_logging("INFO")
    inserted, updated = seed_cities(dry_run=args.dry_run)

    if args.dry_run:
        print(f"[DRY RUN] Would insert {inserted}, update {updated} cities")
    else:
        print(f"Seeded {inserted} new cities, updated {updated} existing cities")


if __name__ == "__main__":
    main()
