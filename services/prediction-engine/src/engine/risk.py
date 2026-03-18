"""
Pure math functions for the 6-factor risk scoring system.

Each recommendation gets a risk score from 1 (lowest risk) to 10
(highest risk) based on weighted factors that capture forecast
confidence, market conditions, and model reliability.

Risk categories:
    1-3:  HIGH CONFIDENCE — safe to trade
    4-6:  MODERATE — proceed with caution
    7-10: HIGH RISK — flagged with warning

All functions are pure — no DB access, no state, no side effects.
Each factor function maps its input to a 1-10 score independently,
then compute_risk_score() combines them with configurable weights.
"""

from datetime import date
from decimal import Decimal


def _clamp(value: Decimal, low: Decimal, high: Decimal) -> Decimal:
    """Clamp value to [low, high] range."""
    return max(low, min(value, high))


_SCORE_MIN = Decimal("1")
_SCORE_MAX = Decimal("10")


def forecast_spread_score(temps: list[Decimal]) -> Decimal:
    """Score based on temperature spread across sources.

    Low spread (all sources agree) = low risk.
    High spread (sources disagree) = high risk.

    Mapping:
        0-1°F spread  → 1 (sources nearly identical)
        2°F spread    → 3
        5°F spread    → 6 (HIGH SPREAD flag threshold)
        7°F+ spread   → 10

    Args:
        temps: Temperature values from all available sources.

    Returns:
        Risk score from 1 to 10. Returns 1 if fewer than 2 sources
        (can't compute spread, not a risk signal).
    """
    if len(temps) < 2:
        return _SCORE_MIN

    spread = max(temps) - min(temps)

    if spread <= Decimal("1"):
        return Decimal("1")
    if spread <= Decimal("2"):
        return Decimal("3")
    if spread <= Decimal("3"):
        return Decimal("4")
    if spread <= Decimal("5"):
        return Decimal("6")
    if spread <= Decimal("7"):
        return Decimal("8")
    return Decimal("10")


def source_agreement_score(
    temps: list[Decimal],
    bracket_low: Decimal | None,
    bracket_high: Decimal | None,
) -> Decimal:
    """Score based on how many sources place the temp in the same bracket.

    Measures directional agreement: how many sources predict a temperature
    that falls within the given bracket range.

    Mapping:
        4/4 agree → 1 (all sources in bracket)
        3/4 agree → 3
        2/4 agree → 5
        1/4 agree → 8
        0/4 agree → 10

    Args:
        temps: Temperature values from all sources.
        bracket_low: Lower bound of bracket, or None for lower edge.
        bracket_high: Upper bound of bracket, or None for upper edge.

    Returns:
        Risk score from 1 to 10.
    """
    if not temps:
        return _SCORE_MAX

    in_bracket = 0
    for t in temps:
        below_ok = bracket_low is None or t >= bracket_low
        above_ok = bracket_high is None or t < bracket_high
        if below_ok and above_ok:
            in_bracket += 1

    fraction = Decimal(in_bracket) / Decimal(len(temps))

    if fraction >= Decimal("0.9"):
        return Decimal("1")
    if fraction >= Decimal("0.7"):
        return Decimal("3")
    if fraction >= Decimal("0.5"):
        return Decimal("5")
    if fraction >= Decimal("0.25"):
        return Decimal("8")
    return Decimal("10")


def city_accuracy_score(city_accuracy: Decimal | None) -> Decimal:
    """Score based on historical model accuracy for this city.

    If no historical data is available (cold start), returns a neutral
    mid-range score rather than penalizing or rewarding.

    Mapping:
        accuracy >= 80% → 1
        accuracy >= 70% → 3
        accuracy >= 60% → 5
        accuracy >= 50% → 7
        accuracy < 50%  → 10
        no data         → 5 (neutral)

    Args:
        city_accuracy: Historical accuracy as a decimal (0.75 = 75%),
            or None if insufficient historical data.

    Returns:
        Risk score from 1 to 10.
    """
    if city_accuracy is None:
        return Decimal("5")

    if city_accuracy >= Decimal("0.80"):
        return Decimal("1")
    if city_accuracy >= Decimal("0.70"):
        return Decimal("3")
    if city_accuracy >= Decimal("0.60"):
        return Decimal("5")
    if city_accuracy >= Decimal("0.50"):
        return Decimal("7")
    return Decimal("10")


def liquidity_score(volume: int) -> Decimal:
    """Score based on market trading volume (liquidity proxy).

    Low volume markets have wider bid-ask spreads and higher
    execution risk. Paper trades assume taker prices, so thin
    markets still work, but real trading would face slippage.

    Mapping:
        volume > 100 → 1
        volume > 50  → 3
        volume > 20  → 5
        volume > 10  → 7
        volume <= 10 → 10

    Args:
        volume: Total contracts traded on this market.

    Returns:
        Risk score from 1 to 10.
    """
    if volume < 0:
        raise ValueError(f"volume must be non-negative, got {volume}")

    if volume > 100:
        return Decimal("1")
    if volume > 50:
        return Decimal("3")
    if volume > 20:
        return Decimal("5")
    if volume > 10:
        return Decimal("7")
    return Decimal("10")


def bracket_edge_score(
    predicted_temp: Decimal,
    bracket_low: Decimal | None,
    bracket_high: Decimal | None,
) -> Decimal:
    """Score based on how close the predicted temp is to a bracket edge.

    If the predicted temperature is near a bracket boundary, a small
    forecast error could flip the outcome. Center-of-bracket predictions
    are safer.

    Mapping:
        distance > 2.0°F from nearest edge → 1
        distance > 1.0°F → 3
        distance > 0.5°F → 6
        distance <= 0.5°F → 9

    For edge brackets (only one bound), distance is measured from
    the single boundary. For unbounded edge brackets, returns low risk.

    Args:
        predicted_temp: The model's predicted temperature.
        bracket_low: Lower bound of bracket, or None for lower edge.
        bracket_high: Upper bound of bracket, or None for upper edge.

    Returns:
        Risk score from 1 to 10.
    """
    distances: list[Decimal] = []

    if bracket_low is not None:
        distances.append(abs(predicted_temp - bracket_low))
    if bracket_high is not None:
        distances.append(abs(predicted_temp - bracket_high))

    if not distances:
        # Both bounds are None — fully unbounded, shouldn't happen
        # but handle gracefully
        return _SCORE_MIN

    min_distance = min(distances)

    if min_distance > Decimal("2"):
        return Decimal("1")
    if min_distance > Decimal("1"):
        return Decimal("3")
    if min_distance > Decimal("0.5"):
        return Decimal("6")
    return Decimal("9")


def lead_time_score(
    forecast_date: date,
    current_date: date,
) -> Decimal:
    """Score based on how far out the forecast is.

    Same-day forecasts are much more accurate than 2+ day forecasts.
    Kalshi weather markets typically settle the next day, so lead_time=1
    is the common case.

    Mapping:
        same day (0)    → 1
        next day (1)    → 2
        2 days out      → 5
        3+ days out     → 8

    Args:
        forecast_date: The date being forecast.
        current_date: Today's date.

    Returns:
        Risk score from 1 to 10.
    """
    days_out = (forecast_date - current_date).days

    if days_out <= 0:
        return Decimal("1")
    if days_out == 1:
        return Decimal("2")
    if days_out == 2:
        return Decimal("5")
    return Decimal("8")


def compute_risk_score(
    factors: dict[str, Decimal],
    weights: dict[str, Decimal],
) -> Decimal:
    """Compute weighted risk score from individual factor scores.

    Args:
        factors: Dict of factor_name → score (each 1-10).
            Expected keys: forecast_spread, source_agreement,
            city_accuracy, liquidity, bracket_edge, lead_time.
        weights: Dict of factor_name → weight (must sum to 1.0).
            Same keys as factors.

    Returns:
        Weighted risk score clamped to [1, 10], rounded to 1 decimal.

    Raises:
        ValueError: If factors and weights have mismatched keys.
    """
    if set(factors.keys()) != set(weights.keys()):
        missing_factors = set(weights.keys()) - set(factors.keys())
        extra_factors = set(factors.keys()) - set(weights.keys())
        parts = []
        if missing_factors:
            parts.append(f"missing factors: {sorted(missing_factors)}")
        if extra_factors:
            parts.append(f"unexpected factors: {sorted(extra_factors)}")
        raise ValueError(
            f"Factor/weight key mismatch: {', '.join(parts)}"
        )

    for key, weight in weights.items():
        if weight < 0:
            raise ValueError(f"Weight '{key}' must be non-negative, got {weight}")

    weight_sum = sum(weights.values())
    if abs(weight_sum - Decimal("1")) > Decimal("0.001"):
        raise ValueError(f"Weights must sum to 1.0, got {weight_sum}")

    for key, score in factors.items():
        if score < _SCORE_MIN or score > _SCORE_MAX:
            raise ValueError(
                f"Factor '{key}' score must be in [{_SCORE_MIN}, {_SCORE_MAX}], got {score}"
            )

    weighted_sum = sum(
        factors[key] * weights[key]
        for key in weights
    )

    clamped = _clamp(weighted_sum, _SCORE_MIN, _SCORE_MAX)
    return clamped.quantize(Decimal("0.1"))
