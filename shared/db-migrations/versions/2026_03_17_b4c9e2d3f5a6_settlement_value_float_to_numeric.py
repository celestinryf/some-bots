"""settlement_value_float_to_numeric

Revision ID: b4c9e2d3f5a6
Revises: a3b8f1c2d4e5
Create Date: 2026-03-17 00:01:00.000000+00:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b4c9e2d3f5a6'
down_revision: Union[str, None] = 'a3b8f1c2d4e5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        'kalshi_markets',
        'settlement_value',
        existing_type=sa.Float(),
        type_=sa.Numeric(precision=10, scale=4),
        existing_nullable=True,
        postgresql_using='settlement_value::numeric(10,4)',
    )


def downgrade() -> None:
    op.alter_column(
        'kalshi_markets',
        'settlement_value',
        existing_type=sa.Numeric(precision=10, scale=4),
        type_=sa.Float(),
        existing_nullable=True,
        postgresql_using='settlement_value::double precision',
    )
