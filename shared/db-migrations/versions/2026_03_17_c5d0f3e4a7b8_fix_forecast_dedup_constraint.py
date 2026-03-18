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


# revision identifiers, used by Alembic.
revision: str = 'c5d0f3e4a7b8'
down_revision: Union[str, None] = 'b4c9e2d3f5a6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
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
