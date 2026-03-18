"""
Prediction engine configuration.

All tunable parameters live here in a single frozen dataclass. Values
have conservative defaults suitable for cold-start (zero historical data).
Override via environment variables for tuning without code changes.

No secrets are stored here — this is pure application configuration.
Secrets (DB password, API keys) live in shared/config/settings.py.
"""

import os
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation


def _env_decimal(name: str, default: Decimal) -> Decimal:
    """Read a Decimal from an environment variable, or return the default."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return Decimal(raw)
    except InvalidOperation:
        raise ValueError(
            f"{name} must be a valid decimal, got: {raw!r}"
        ) from None


def _env_int(name: str, default: int) -> int:
    """Read an int from an environment variable, or return the default."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(
            f"{name} must be an integer, got: {raw!r}"
        ) from None
    return value


@dataclass(frozen=True)
class PredictionConfig:
    """Configuration for the prediction and recommendation engine.

    All values have conservative cold-start defaults. As the system
    accumulates settlement data and measured accuracy, thresholds
    can be relaxed toward their production targets.

    Cold-start defaults vs production targets:
        gap_threshold:    0.20 → 0.15 (after calibration)
        min_ev_threshold: 0.08 → 0.05 (after calibration)
    """

    # --- Recommendation thresholds ---
    gap_threshold: Decimal = Decimal("0.20")
    min_ev_threshold: Decimal = Decimal("0.08")

    # --- Risk factor weights (must sum to 1.0) ---
    risk_weight_forecast_spread: Decimal = Decimal("0.25")
    risk_weight_source_agreement: Decimal = Decimal("0.20")
    risk_weight_city_accuracy: Decimal = Decimal("0.15")
    risk_weight_liquidity: Decimal = Decimal("0.10")
    risk_weight_bracket_edge: Decimal = Decimal("0.15")
    risk_weight_lead_time: Decimal = Decimal("0.15")

    # --- Model settings ---
    model_version: str = "tier1_equal_weight_v1"
    min_sources_required: int = 2
    std_dev_floor: Decimal = Decimal("1.50")

    # --- Probability validation ---
    probability_sum_tolerance: Decimal = Decimal("0.01")

    def __post_init__(self) -> None:
        """Validate configuration at construction time."""
        weight_sum = (
            self.risk_weight_forecast_spread
            + self.risk_weight_source_agreement
            + self.risk_weight_city_accuracy
            + self.risk_weight_liquidity
            + self.risk_weight_bracket_edge
            + self.risk_weight_lead_time
        )
        if abs(weight_sum - Decimal("1.0")) > Decimal("0.001"):
            raise ValueError(
                f"Risk weights must sum to 1.0, got {weight_sum}"
            )

        if self.gap_threshold <= 0:
            raise ValueError(
                f"gap_threshold must be positive, got {self.gap_threshold}"
            )

        if self.min_ev_threshold <= 0:
            raise ValueError(
                f"min_ev_threshold must be positive, got {self.min_ev_threshold}"
            )

        if self.min_sources_required < 1:
            raise ValueError(
                f"min_sources_required must be >= 1, got {self.min_sources_required}"
            )

        if self.std_dev_floor <= 0:
            raise ValueError(
                f"std_dev_floor must be positive, got {self.std_dev_floor}"
            )

        if self.probability_sum_tolerance <= 0:
            raise ValueError(
                f"probability_sum_tolerance must be positive, "
                f"got {self.probability_sum_tolerance}"
            )

    @property
    def risk_weights(self) -> dict[str, Decimal]:
        """Return risk factor weights as a dict for compute_risk_score()."""
        return {
            "forecast_spread": self.risk_weight_forecast_spread,
            "source_agreement": self.risk_weight_source_agreement,
            "city_accuracy": self.risk_weight_city_accuracy,
            "liquidity": self.risk_weight_liquidity,
            "bracket_edge": self.risk_weight_bracket_edge,
            "lead_time": self.risk_weight_lead_time,
        }


def load_prediction_config() -> PredictionConfig:
    """Load prediction config from environment variables with defaults."""
    return PredictionConfig(
        gap_threshold=_env_decimal(
            "PREDICTION_GAP_THRESHOLD", Decimal("0.20")
        ),
        min_ev_threshold=_env_decimal(
            "PREDICTION_MIN_EV_THRESHOLD", Decimal("0.08")
        ),
        risk_weight_forecast_spread=_env_decimal(
            "PREDICTION_RISK_WEIGHT_FORECAST_SPREAD", Decimal("0.25")
        ),
        risk_weight_source_agreement=_env_decimal(
            "PREDICTION_RISK_WEIGHT_SOURCE_AGREEMENT", Decimal("0.20")
        ),
        risk_weight_city_accuracy=_env_decimal(
            "PREDICTION_RISK_WEIGHT_CITY_ACCURACY", Decimal("0.15")
        ),
        risk_weight_liquidity=_env_decimal(
            "PREDICTION_RISK_WEIGHT_LIQUIDITY", Decimal("0.10")
        ),
        risk_weight_bracket_edge=_env_decimal(
            "PREDICTION_RISK_WEIGHT_BRACKET_EDGE", Decimal("0.15")
        ),
        risk_weight_lead_time=_env_decimal(
            "PREDICTION_RISK_WEIGHT_LEAD_TIME", Decimal("0.15")
        ),
        min_sources_required=_env_int(
            "PREDICTION_MIN_SOURCES_REQUIRED", 2
        ),
        std_dev_floor=_env_decimal(
            "PREDICTION_STD_DEV_FLOOR", Decimal("1.50")
        ),
        probability_sum_tolerance=_env_decimal(
            "PREDICTION_PROBABILITY_SUM_TOLERANCE", Decimal("0.01")
        ),
    )
