"""fix_forecast_dedup_constraint_drop_issued_at

Drop issued_at from the uq_forecast_dedup unique constraint so that
deduplication works correctly for sources that use datetime.now() as
issued_at (OWM, Visual Crossing, PirateWeather). Also update the
compound index to match.

Revision ID: c5d0f3e4a7b8
Revises: b4c9e2d3f5a6
Create Date: 2026-03-17 00:02:00.000000+00:00
"""

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text


# revision identifiers, used by Alembic.
revision: str = 'c5d0f3e4a7b8'
down_revision: Union[str, None] = 'b4c9e2d3f5a6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Block concurrent writes while we dedup and swap the constraint.
    op.execute(text("LOCK TABLE weather_forecasts IN ACCESS EXCLUSIVE MODE"))

    # Deduplicate existing rows before tightening the constraint.
    # Keep the row with the latest issued_at per (source, city_id,
    # forecast_date) group; delete the rest.
    op.execute(text("""
        DELETE FROM weather_forecasts
        WHERE id NOT IN (
            SELECT DISTINCT ON (source, city_id, forecast_date) id
            FROM weather_forecasts
            ORDER BY source, city_id, forecast_date,
                     issued_at DESC, created_at DESC
        )
    """))

    # Drop old constraint and index that included issued_at
    op.drop_constraint('uq_forecast_dedup', 'weather_forecasts', type_='unique')
    op.drop_index('ix_forecasts_city_date_source', table_name='weather_forecasts')

    # Recreate without issued_at
    op.create_unique_constraint(
        'uq_forecast_dedup',
        'weather_forecasts',
        ['source', 'city_id', 'forecast_date'],
    )
    op.create_index(
        'ix_forecasts_city_date_source',
        'weather_forecasts',
        ['city_id', 'forecast_date', 'source'],
        unique=False,
    )


def downgrade() -> None:
    # Restore issued_at in constraint and index
    op.drop_constraint('uq_forecast_dedup', 'weather_forecasts', type_='unique')
    op.drop_index('ix_forecasts_city_date_source', table_name='weather_forecasts')

    op.create_unique_constraint(
        'uq_forecast_dedup',
        'weather_forecasts',
        ['source', 'city_id', 'forecast_date', 'issued_at'],
    )
    op.create_index(
        'ix_forecasts_city_date_source',
        'weather_forecasts',
        ['city_id', 'forecast_date', 'source', 'issued_at'],
        unique=False,
    )
