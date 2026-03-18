"""temp_columns_float_to_numeric

Change temp_high and temp_low from Float to Numeric(6,2) in
weather_forecasts for consistency with Numeric bracket columns
in kalshi_markets, avoiding float/Decimal comparison errors.

Revision ID: e7f2g5h6i9j0
Revises: d6e1f4g5h8i9
Create Date: 2026-03-18 00:01:00.000000+00:00
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'e7f2g5h6i9j0'
down_revision: Union[str, None] = 'd6e1f4g5h8i9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        'weather_forecasts',
        'temp_high',
        existing_type=sa.Float(),
        type_=sa.Numeric(6, 2),
        existing_nullable=True,
        postgresql_using='temp_high::numeric(6,2)',
    )
    op.alter_column(
        'weather_forecasts',
        'temp_low',
        existing_type=sa.Float(),
        type_=sa.Numeric(6, 2),
        existing_nullable=True,
        postgresql_using='temp_low::numeric(6,2)',
    )


def downgrade() -> None:
    op.alter_column(
        'weather_forecasts',
        'temp_low',
        existing_type=sa.Numeric(6, 2),
        type_=sa.Float(),
        existing_nullable=True,
    )
    op.alter_column(
        'weather_forecasts',
        'temp_high',
        existing_type=sa.Numeric(6, 2),
        type_=sa.Float(),
        existing_nullable=True,
    )
