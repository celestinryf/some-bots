"""prediction_columns_float_to_numeric

Change predicted_temp and std_dev from Float to Numeric(6,2) in
predictions for consistency with Numeric temp and bracket columns,
avoiding float/Decimal comparison errors in Sprint 2.

Revision ID: f8g3h6i7j0k1
Revises: e7f2g5h6i9j0
Create Date: 2026-03-18 00:02:00.000000+00:00
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'f8g3h6i7j0k1'
down_revision: Union[str, None] = 'e7f2g5h6i9j0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        'predictions',
        'predicted_temp',
        existing_type=sa.Float(),
        type_=sa.Numeric(6, 2),
        existing_nullable=False,
        postgresql_using='predicted_temp::numeric(6,2)',
    )
    op.alter_column(
        'predictions',
        'std_dev',
        existing_type=sa.Float(),
        type_=sa.Numeric(6, 2),
        existing_nullable=False,
        postgresql_using='std_dev::numeric(6,2)',
    )


def downgrade() -> None:
    op.alter_column(
        'predictions',
        'std_dev',
        existing_type=sa.Numeric(6, 2),
        type_=sa.Float(),
        existing_nullable=False,
    )
    op.alter_column(
        'predictions',
        'predicted_temp',
        existing_type=sa.Numeric(6, 2),
        type_=sa.Float(),
        existing_nullable=False,
    )
