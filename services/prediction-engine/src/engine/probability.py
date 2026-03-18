"""
Pure math functions for temperature probability distributions.

Converts ensemble temperature forecasts into Gaussian probability
distributions, then maps those distributions onto Kalshi market brackets.

All functions are pure — no DB access, no state, no side effects.
Inputs go in, outputs come out, everything is explicitly typed.

Key concepts:
    - Ensemble mean: equal-weight average of all source temperatures
    - Ensemble std: standard deviation across sources (with configurable floor)
    - Bracket probability: integral of Gaussian PDF over each bracket range
    - Edge brackets: open-ended ranges like (-inf, 65) or [75, inf)
"""

import math
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from scipy.stats import norm

from shared.config.errors import PredictionError, ValidationError


@dataclass(frozen=True)
class BracketDef:
    """Definition of a single Kalshi market bracket.

    For edge brackets:
        - Lower edge: low is None, e.g. BracketDef(low=None, high=Decimal("65"))
        - Upper edge: high is None, e.g. BracketDef(low=Decimal("75"), high=None)
    """

    low: Decimal | None
    high: Decimal | None
    market_id: str  # UUID as string for serialization

    def __post_init__(self) -> None:
        if not self.market_id:
            raise ValidationError(
                "market_id must be a non-empty string",
                source="probability",
            )
        if self.low is None and self.high is None:
            raise ValidationError(
                "Bracket must have at least one bound (low or high)",
                source="probability",
            )
        if self.low is not None and self.high is not None:
            if self.low >= self.high:
                raise ValidationError(
                    f"Bracket low ({self.low}) must be less than high ({self.high})",
                    source="probability",
                )

    @property
    def key(self) -> str:
        """Human-readable bracket key for JSONB storage.

        Examples: '(-inf, 65)', '[65, 66)', '[75, inf)'
        """
        low_str = str(self.low) if self.low is not None else "-inf"
        high_str = str(self.high) if self.high is not None else "inf"
        left = "(" if self.low is None else "["
        return f"{left}{low_str}, {high_str})"


def compute_ensemble_mean(temps: list[Decimal]) -> Decimal:
    """Compute equal-weight average of temperature forecasts.

    Args:
        temps: Non-empty list of temperature values from different sources.

    Returns:
        Mean temperature as Decimal.

    Raises:
        PredictionError: If temps is empty.
    """
    if not temps:
        raise PredictionError(
            "Cannot compute ensemble mean from empty temperature list",
            source="probability",
        )
    return sum(temps, Decimal("0")) / len(temps)


def compute_ensemble_std(
    temps: list[Decimal],
    floor: Decimal,
) -> Decimal:
    """Compute standard deviation of temperature forecasts with a floor.

    The floor prevents overconfident predictions when sources agree
    exactly (std=0) or when only one source is available. A floor of
    1.5 degrees F means we never claim more than ~95% confidence in
    a 3-degree window, which is realistic for day-ahead forecasting.

    Args:
        temps: Non-empty list of temperature values.
        floor: Minimum std_dev to return (e.g., Decimal("1.50")).

    Returns:
        max(computed_std, floor) as Decimal.

    Raises:
        PredictionError: If temps is empty.
        ValueError: If floor is not positive.
    """
    if floor <= 0:
        raise ValueError(f"floor must be positive, got {floor}")

    if not temps:
        raise PredictionError(
            "Cannot compute ensemble std from empty temperature list",
            source="probability",
        )

    if len(temps) == 1:
        return floor

    mean = compute_ensemble_mean(temps)
    variance = sum((t - mean) ** 2 for t in temps) / len(temps)

    # Decimal doesn't have sqrt, so convert to float and back.
    # Guard against overflow (extremely large variance → float inf).
    variance_f = float(variance)
    if not math.isfinite(variance_f):
        raise PredictionError(
            f"Variance overflow: {variance}",
            source="probability",
        )
    std = Decimal(str(variance_f ** 0.5))

    return max(std, floor)


def bracket_probability(
    mean: float,
    std: float,
    low: float | None,
    high: float | None,
) -> float:
    """Compute probability that a Gaussian random variable falls in [low, high).

    Uses scipy.stats.norm.cdf for the Gaussian cumulative distribution.

    Args:
        mean: Distribution mean (predicted temperature).
        std: Distribution standard deviation (must be > 0).
        low: Lower bracket bound, or None for lower edge bracket (-inf, high).
        high: Upper bracket bound, or None for upper edge bracket [low, inf).

    Returns:
        Probability as a float in [0, 1].

    Raises:
        PredictionError: If std <= 0.
    """
    if not math.isfinite(mean):
        raise PredictionError(
            f"mean must be finite, got {mean}",
            source="probability",
        )
    if not math.isfinite(std) or std <= 0:
        raise PredictionError(
            f"Standard deviation must be finite and positive, got {std}",
            source="probability",
        )

    if low is None and high is None:
        return 1.0

    if low is None:
        # Lower edge bracket: P(X < high)
        return float(norm.cdf(high, loc=mean, scale=std))

    if high is None:
        # Upper edge bracket: P(X >= low)
        return float(1.0 - norm.cdf(low, loc=mean, scale=std))

    # Normal bracket: P(low <= X < high)
    # Clamp to 0.0 in case of floating-point imprecision or direct call with low > high
    prob = float(norm.cdf(high, loc=mean, scale=std) - norm.cdf(low, loc=mean, scale=std))
    return max(0.0, prob)


def map_brackets(
    mean: float,
    std: float,
    brackets: list[BracketDef],
) -> dict[str, float]:
    """Map a Gaussian distribution onto Kalshi market brackets.

    Computes the probability for each bracket and returns a dict
    keyed by bracket range string (e.g., "[65.0, 66.0)").

    Args:
        mean: Predicted temperature (distribution mean).
        std: Standard deviation (must be > 0).
        brackets: List of bracket definitions (may be empty).

    Returns:
        Dict mapping bracket key strings to probabilities.
        Empty dict if brackets list is empty.
    """
    if not brackets:
        return {}

    result: dict[str, float] = {}
    for bracket in brackets:
        low_f = float(bracket.low) if bracket.low is not None else None
        high_f = float(bracket.high) if bracket.high is not None else None
        prob = bracket_probability(mean, std, low_f, high_f)
        result[bracket.key] = prob

    return result


def verify_probability_sum(
    probs: dict[str, float],
    tolerance: float = 0.01,
) -> bool:
    """Verify that bracket probabilities sum to approximately 1.0.

    Args:
        probs: Non-empty dict of bracket_key → probability.
        tolerance: Maximum allowed deviation from 1.0 (must be in (0, 1)).

    Returns:
        True if sum is within tolerance of 1.0.
        False if probs is empty (empty set cannot sum to 1.0).
    """
    if tolerance <= 0 or tolerance >= 1:
        raise ValueError(f"tolerance must be in (0, 1), got {tolerance}")

    if not probs:
        return False

    total = sum(probs.values())
    return abs(total - 1.0) <= tolerance


def build_probability_distribution(
    temps: list[Decimal],
    brackets: list[BracketDef],
    source_temps: dict[str, Decimal],
    std_dev_floor: Decimal,
    probability_sum_tolerance: float = 0.01,
) -> dict[str, Any]:
    """Build a complete probability distribution JSONB payload.

    This is the top-level function that orchestrates the full pipeline:
    compute ensemble stats → map to brackets → verify → package for storage.

    Args:
        temps: Temperature values from all available sources.
        brackets: Kalshi bracket definitions for this market.
        source_temps: Dict of source_name → temperature for audit trail.
        std_dev_floor: Minimum std_dev (from PredictionConfig).
        probability_sum_tolerance: Max deviation from 1.0 for sum check.

    Returns:
        Dict ready for JSONB storage with keys:
            brackets: {bracket_key: probability}
            mean: float
            std_dev: float
            source_temps: {source: temp}
            sum_check: float

    Raises:
        PredictionError: If probability sum is outside tolerance.
    """
    mean = compute_ensemble_mean(temps)
    std = compute_ensemble_std(temps, std_dev_floor)

    mean_f = float(mean)
    std_f = float(std)

    bracket_probs = map_brackets(mean_f, std_f, brackets)

    if bracket_probs and not verify_probability_sum(bracket_probs, probability_sum_tolerance):
        prob_sum = sum(bracket_probs.values())
        raise PredictionError(
            f"Bracket probabilities sum to {prob_sum:.6f}, "
            f"expected 1.0 ± {probability_sum_tolerance}",
            source="probability",
        )

    prob_sum = sum(bracket_probs.values()) if bracket_probs else 0.0

    # Validate source_temps values are finite
    for source_name, temp_val in source_temps.items():
        temp_f = float(temp_val)
        if not math.isfinite(temp_f):
            raise PredictionError(
                f"Source '{source_name}' has non-finite temperature: {temp_val}",
                source="probability",
            )

    return {
        "brackets": bracket_probs,
        "mean": mean_f,
        "std_dev": std_f,
        "source_temps": {k: float(v) for k, v in source_temps.items()},
        "sum_check": round(prob_sum, 6),
    }
