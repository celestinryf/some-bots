"""add_snapshot_timestamp_index

Add a timestamp-focused index to kalshi_market_snapshots so retention
cleanup and time-window scans can avoid full-table scans.

Revision ID: j2k7l0m1n4o
Revises: i1j6k9l0m3n4
Create Date: 2026-03-18 00:06:00.000000+00:00
"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "j2k7l0m1n4o"
down_revision: str | None = "i1j6k9l0m3n4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "ix_snapshots_timestamp",
        "kalshi_market_snapshots",
        ["timestamp"],
    )


def downgrade() -> None:
    op.drop_index("ix_snapshots_timestamp", table_name="kalshi_market_snapshots")
