"""
Pure math functions for Kalshi fee calculation and expected value.

Kalshi charges taker fees using the formula:
    fee = ceil(0.07 * contracts * price * (1 - price))

This creates a parabolic fee curve that peaks at price=0.50 (maximum
uncertainty) and approaches zero at prices near 0.00 or 1.00.

All functions are pure — no DB access, no state, no side effects.
"""

from decimal import ROUND_CEILING, Decimal


# Kalshi taker fee rate (7% of notional risk)
_FEE_RATE = Decimal("0.07")
_ONE_CENT = Decimal("0.01")


def kalshi_taker_fee(contracts: int, price: Decimal) -> Decimal:
    """Calculate Kalshi taker fee for a trade.

    Fee formula: ceil(0.07 * contracts * price * (1 - price))
    The fee is per-trade, applied to the total position.

    Args:
        contracts: Number of contracts (must be >= 1, integer).
        price: Entry price per contract in dollars (0 < price < 1).

    Returns:
        Fee in dollars as Decimal, rounded up to nearest cent.

    Raises:
        ValueError: If contracts < 1 or price is outside (0, 1).
        TypeError: If contracts is not an int.
    """
    if not isinstance(contracts, int):
        raise TypeError(f"contracts must be an int, got {type(contracts).__name__}")

    if contracts < 1:
        raise ValueError(f"contracts must be >= 1, got {contracts}")

    if not isinstance(price, Decimal):
        raise TypeError(f"price must be a Decimal, got {type(price).__name__}")

    if price <= 0 or price >= 1:
        raise ValueError(
            f"price must be between 0 and 1 exclusive, got {price}"
        )

    raw_fee = _FEE_RATE * contracts * price * (Decimal("1") - price)

    # Ceil to nearest cent using pure Decimal arithmetic (no float conversion)
    return raw_fee.quantize(_ONE_CENT, rounding=ROUND_CEILING)


def expected_value(
    model_prob: Decimal,
    cost: Decimal,
    fee: Decimal,
) -> Decimal:
    """Calculate expected value of a trade.

    For a binary contract that pays $1.00 on win:
        EV = (probability_of_winning * $1.00) - cost - fee

    For BUY YES: probability_of_winning = model_prob, cost = yes_ask
    For BUY NO:  probability_of_winning = 1 - model_prob, cost = no_ask

    Args:
        model_prob: Our model's probability that this contract wins.
            For BUY YES: probability the bracket contains the actual temp.
            For BUY NO: 1 - bracket probability (probability it doesn't).
        cost: Entry price (yes_ask for BUY YES, no_ask for BUY NO).
        fee: Kalshi taker fee for this trade.

    Returns:
        Expected value in dollars. Positive = edge in our favor.

    Raises:
        ValueError: If model_prob is outside [0, 1] or cost/fee are negative.
    """
    if not isinstance(model_prob, Decimal):
        raise TypeError(f"model_prob must be a Decimal, got {type(model_prob).__name__}")
    if not isinstance(cost, Decimal):
        raise TypeError(f"cost must be a Decimal, got {type(cost).__name__}")
    if not isinstance(fee, Decimal):
        raise TypeError(f"fee must be a Decimal, got {type(fee).__name__}")

    if model_prob < 0 or model_prob > 1:
        raise ValueError(
            f"model_prob must be in [0, 1], got {model_prob}"
        )

    if cost < 0 or cost > 1:
        raise ValueError(f"cost must be in [0, 1], got {cost}")

    if fee < 0:
        raise ValueError(f"fee must be non-negative, got {fee}")

    payout = model_prob * Decimal("1.00")
    return payout - cost - fee
