"""add_market_status_index

Add index on kalshi_markets.status to avoid full-table scans in the
snapshot (5-min) and settlement (2-hour) polling queries.

Revision ID: h0i5j8k9l2m3
Revises: g9h4i7j8k1l2
Create Date: 2026-03-18 00:04:00.000000+00:00
"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "h0i5j8k9l2m3"
down_revision: Union[str, None] = "g9h4i7j8k1l2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index("ix_markets_status", "kalshi_markets", ["status"])


def downgrade() -> None:
    op.drop_index("ix_markets_status", table_name="kalshi_markets")
