"""add_forecast_updated_at

Add updated_at column to weather_forecasts so operators and monitoring
can detect stale or failed ingestion cycles.

Revision ID: i1j6k9l0m3n4
Revises: h0i5j8k9l2m3
Create Date: 2026-03-18 00:05:00.000000+00:00
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "i1j6k9l0m3n4"
down_revision: Union[str, None] = "h0i5j8k9l2m3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "weather_forecasts",
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )


def downgrade() -> None:
    op.drop_column("weather_forecasts", "updated_at")
