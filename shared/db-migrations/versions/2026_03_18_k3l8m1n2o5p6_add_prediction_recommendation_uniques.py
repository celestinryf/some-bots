"""add_prediction_recommendation_uniques

Enforce idempotent upserts for prediction/recommendation runtime workers.

Revision ID: k3l8m1n2o5p6
Revises: j2k7l0m1n4o
Create Date: 2026-03-18 00:32:00.000000+00:00
"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "k3l8m1n2o5p6"
down_revision: str | None = "j2k7l0m1n4o"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Re-point child rows to canonical parents before removing duplicates.
    op.execute(
        """
        WITH ranked_predictions AS (
            SELECT
                id,
                FIRST_VALUE(id) OVER (
                    PARTITION BY city_id, forecast_date, market_type, model_version
                    ORDER BY created_at ASC, id ASC
                ) AS canonical_id,
                ROW_NUMBER() OVER (
                    PARTITION BY city_id, forecast_date, market_type, model_version
                    ORDER BY created_at ASC, id ASC
                ) AS row_num
            FROM predictions
        )
        UPDATE recommendations r
        SET prediction_id = ranked_predictions.canonical_id
        FROM ranked_predictions
        WHERE r.prediction_id = ranked_predictions.id
          AND ranked_predictions.row_num > 1;
        """
    )
    op.execute(
        """
        WITH ranked_predictions AS (
            SELECT
                id,
                ROW_NUMBER() OVER (
                    PARTITION BY city_id, forecast_date, market_type, model_version
                    ORDER BY created_at ASC, id ASC
                ) AS row_num
            FROM predictions
        )
        DELETE FROM predictions p
        USING ranked_predictions
        WHERE p.id = ranked_predictions.id
          AND ranked_predictions.row_num > 1;
        """
    )
    op.execute(
        """
        WITH ranked_recommendations AS (
            SELECT
                id,
                FIRST_VALUE(id) OVER (
                    PARTITION BY prediction_id, market_id
                    ORDER BY created_at ASC, id ASC
                ) AS canonical_id,
                ROW_NUMBER() OVER (
                    PARTITION BY prediction_id, market_id
                    ORDER BY created_at ASC, id ASC
                ) AS row_num
            FROM recommendations
        )
        , fixed_trade_targets AS (
            SELECT
                f.id,
                COALESCE(ranked_recommendations.canonical_id, f.recommendation_id) AS target_recommendation_id,
                ROW_NUMBER() OVER (
                    PARTITION BY COALESCE(ranked_recommendations.canonical_id, f.recommendation_id)
                    ORDER BY f.created_at ASC, f.id ASC
                ) AS row_num
            FROM paper_trades_fixed f
            LEFT JOIN ranked_recommendations
              ON ranked_recommendations.id = f.recommendation_id
             AND ranked_recommendations.row_num > 1
        )
        DELETE FROM paper_trades_fixed f
        USING fixed_trade_targets
        WHERE f.id = fixed_trade_targets.id
          AND fixed_trade_targets.row_num > 1;
        """
    )
    op.execute(
        """
        WITH ranked_recommendations AS (
            SELECT
                id,
                FIRST_VALUE(id) OVER (
                    PARTITION BY prediction_id, market_id
                    ORDER BY created_at ASC, id ASC
                ) AS canonical_id,
                ROW_NUMBER() OVER (
                    PARTITION BY prediction_id, market_id
                    ORDER BY created_at ASC, id ASC
                ) AS row_num
            FROM recommendations
        )
        UPDATE paper_trades_fixed f
        SET recommendation_id = ranked_recommendations.canonical_id
        FROM ranked_recommendations
        WHERE f.recommendation_id = ranked_recommendations.id
          AND ranked_recommendations.row_num > 1;
        """
    )
    op.execute(
        """
        WITH ranked_recommendations AS (
            SELECT
                id,
                FIRST_VALUE(id) OVER (
                    PARTITION BY prediction_id, market_id
                    ORDER BY created_at ASC, id ASC
                ) AS canonical_id,
                ROW_NUMBER() OVER (
                    PARTITION BY prediction_id, market_id
                    ORDER BY created_at ASC, id ASC
                ) AS row_num
            FROM recommendations
        )
        UPDATE paper_trades_portfolio p
        SET recommendation_id = ranked_recommendations.canonical_id
        FROM ranked_recommendations
        WHERE p.recommendation_id = ranked_recommendations.id
          AND ranked_recommendations.row_num > 1;
        """
    )
    op.execute(
        """
        WITH ranked_recommendations AS (
            SELECT
                id,
                FIRST_VALUE(id) OVER (
                    PARTITION BY prediction_id, market_id
                    ORDER BY created_at ASC, id ASC
                ) AS canonical_id,
                ROW_NUMBER() OVER (
                    PARTITION BY prediction_id, market_id
                    ORDER BY created_at ASC, id ASC
                ) AS row_num
            FROM recommendations
        )
        , email_targets AS (
            SELECT
                e.email_log_id,
                e.recommendation_id,
                COALESCE(ranked_recommendations.canonical_id, e.recommendation_id) AS target_recommendation_id,
                ROW_NUMBER() OVER (
                    PARTITION BY e.email_log_id, COALESCE(ranked_recommendations.canonical_id, e.recommendation_id)
                    ORDER BY e.recommendation_id ASC
                ) AS row_num
            FROM email_log_recommendations e
            LEFT JOIN ranked_recommendations
              ON ranked_recommendations.id = e.recommendation_id
             AND ranked_recommendations.row_num > 1
        )
        DELETE FROM email_log_recommendations e
        USING email_targets
        WHERE e.email_log_id = email_targets.email_log_id
          AND e.recommendation_id = email_targets.recommendation_id
          AND email_targets.row_num > 1;
        """
    )
    op.execute(
        """
        WITH ranked_recommendations AS (
            SELECT
                id,
                FIRST_VALUE(id) OVER (
                    PARTITION BY prediction_id, market_id
                    ORDER BY created_at ASC, id ASC
                ) AS canonical_id,
                ROW_NUMBER() OVER (
                    PARTITION BY prediction_id, market_id
                    ORDER BY created_at ASC, id ASC
                ) AS row_num
            FROM recommendations
        )
        UPDATE email_log_recommendations e
        SET recommendation_id = ranked_recommendations.canonical_id
        FROM ranked_recommendations
        WHERE e.recommendation_id = ranked_recommendations.id
          AND ranked_recommendations.row_num > 1;
        """
    )
    op.execute(
        """
        WITH ranked_recommendations AS (
            SELECT
                id,
                ROW_NUMBER() OVER (
                    PARTITION BY prediction_id, market_id
                    ORDER BY created_at ASC, id ASC
                ) AS row_num
            FROM recommendations
        )
        DELETE FROM recommendations r
        USING ranked_recommendations
        WHERE r.id = ranked_recommendations.id
          AND ranked_recommendations.row_num > 1;
        """
    )

    op.create_unique_constraint(
        "uq_prediction_city_date_type_model",
        "predictions",
        ["city_id", "forecast_date", "market_type", "model_version"],
    )
    op.create_unique_constraint(
        "uq_recommendation_prediction_market",
        "recommendations",
        ["prediction_id", "market_id"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_recommendation_prediction_market",
        "recommendations",
        type_="unique",
    )
    op.drop_constraint(
        "uq_prediction_city_date_type_model",
        "predictions",
        type_="unique",
    )
