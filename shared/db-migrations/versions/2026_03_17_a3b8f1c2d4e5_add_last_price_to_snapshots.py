"""add_last_price_to_kalshi_market_snapshots

Revision ID: a3b8f1c2d4e5
Revises: 6d75472ec153
Create Date: 2026-03-17 00:00:00.000000+00:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a3b8f1c2d4e5'
down_revision: Union[str, None] = '6d75472ec153'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'kalshi_market_snapshots',
        sa.Column('last_price', sa.Numeric(precision=10, scale=4), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('kalshi_market_snapshots', 'last_price')
