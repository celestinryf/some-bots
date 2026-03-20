"""snapshot_unique_market_timestamp

Add unique constraint on (market_id, timestamp) to kalshi_market_snapshots
to prevent duplicate snapshot rows from overlapping ingestion runs.

Revision ID: g9h4i7j8k1l2
Revises: f8g3h6i7j0k1
Create Date: 2026-03-18 00:03:00.000000+00:00
"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "g9h4i7j8k1l2"
down_revision: str | None = "f8g3h6i7j0k1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_snapshot_market_time",
        "kalshi_market_snapshots",
        ["market_id", "timestamp"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_snapshot_market_time",
        "kalshi_market_snapshots",
        type_="unique",
    )
