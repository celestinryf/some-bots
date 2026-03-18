"""bracket_columns_float_to_numeric

Change bracket_low and bracket_high from Float to Numeric(10,4) in
kalshi_markets for exact decimal arithmetic, consistent with other
financial columns (settlement_value, yes_bid, etc.).

Revision ID: d6e1f4g5h8i9
Revises: c5d0f3e4a7b8
Create Date: 2026-03-17 00:03:00.000000+00:00
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'd6e1f4g5h8i9'
down_revision: Union[str, None] = 'c5d0f3e4a7b8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        'kalshi_markets',
        'bracket_low',
        existing_type=sa.Float(),
        type_=sa.Numeric(10, 4),
        existing_nullable=True,
        postgresql_using='bracket_low::numeric(10,4)',
    )
    op.alter_column(
        'kalshi_markets',
        'bracket_high',
        existing_type=sa.Float(),
        type_=sa.Numeric(10, 4),
        existing_nullable=True,
        postgresql_using='bracket_high::numeric(10,4)',
    )


def downgrade() -> None:
    op.alter_column(
        'kalshi_markets',
        'bracket_high',
        existing_type=sa.Numeric(10, 4),
        type_=sa.Float(),
        existing_nullable=True,
    )
    op.alter_column(
        'kalshi_markets',
        'bracket_low',
        existing_type=sa.Numeric(10, 4),
        type_=sa.Float(),
        existing_nullable=True,
    )
