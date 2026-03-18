"""
Tests for Kalshi fee calculation and expected value functions.

Covers:
    - Fee formula correctness at multiple price points
    - Ceiling behavior (always round up to nearest cent)
    - Edge cases: prices near 0 and 1, invalid inputs
    - Expected value computation with known inputs
"""

from decimal import Decimal

import pytest

from src.engine.fees import expected_value, kalshi_taker_fee


# ---------------------------------------------------------------------------
# kalshi_taker_fee — parameterized with hand-computed values
# ---------------------------------------------------------------------------


class TestKalshiTakerFee:
    @pytest.mark.parametrize(
        "contracts,price,expected_fee",
        [
            # fee = ceil(0.07 * 1 * 0.50 * 0.50) = ceil(0.0175) = $0.02
            (1, Decimal("0.50"), Decimal("0.02")),
            # fee = ceil(0.07 * 1 * 0.99 * 0.01) = ceil(0.000693) = $0.01
            (1, Decimal("0.99"), Decimal("0.01")),
            # fee = ceil(0.07 * 1 * 0.01 * 0.99) = ceil(0.000693) = $0.01
            (1, Decimal("0.01"), Decimal("0.01")),
            # fee = ceil(0.07 * 1 * 0.75 * 0.25) = ceil(0.013125) = $0.02
            (1, Decimal("0.75"), Decimal("0.02")),
            # fee = ceil(0.07 * 1 * 0.25 * 0.75) = ceil(0.013125) = $0.02
            (1, Decimal("0.25"), Decimal("0.02")),
            # fee = ceil(0.07 * 10 * 0.50 * 0.50) = ceil(0.175) = $0.18
            (10, Decimal("0.50"), Decimal("0.18")),
            # fee = ceil(0.07 * 5 * 0.60 * 0.40) = ceil(0.084) = $0.09
            (5, Decimal("0.60"), Decimal("0.09")),
            # fee = ceil(0.07 * 1 * 0.54 * 0.46) = ceil(0.017388) = $0.02
            (1, Decimal("0.54"), Decimal("0.02")),
        ],
        ids=[
            "1_contract_at_50c",
            "1_contract_at_99c",
            "1_contract_at_1c",
            "1_contract_at_75c",
            "1_contract_at_25c",
            "10_contracts_at_50c",
            "5_contracts_at_60c",
            "1_contract_at_54c",
        ],
    )
    def test_fee_calculation(
        self, contracts: int, price: Decimal, expected_fee: Decimal
    ) -> None:
        result = kalshi_taker_fee(contracts, price)
        assert result == expected_fee

    def test_fee_is_symmetric(self) -> None:
        """Fee at price p should equal fee at price (1-p)."""
        fee_30 = kalshi_taker_fee(1, Decimal("0.30"))
        fee_70 = kalshi_taker_fee(1, Decimal("0.70"))
        assert fee_30 == fee_70

    def test_fee_peaks_at_50_cents(self) -> None:
        """Fee is highest at price=0.50 (maximum uncertainty)."""
        fee_50 = kalshi_taker_fee(1, Decimal("0.50"))
        fee_30 = kalshi_taker_fee(1, Decimal("0.30"))
        fee_80 = kalshi_taker_fee(1, Decimal("0.80"))
        assert fee_50 >= fee_30
        assert fee_50 >= fee_80

    def test_fee_minimum_is_one_cent(self) -> None:
        """Even for extreme prices, fee is at least $0.01."""
        fee = kalshi_taker_fee(1, Decimal("0.01"))
        assert fee >= Decimal("0.01")

    def test_invalid_zero_contracts(self) -> None:
        with pytest.raises(ValueError, match="contracts must be >= 1"):
            kalshi_taker_fee(0, Decimal("0.50"))

    def test_invalid_negative_contracts(self) -> None:
        with pytest.raises(ValueError, match="contracts must be >= 1"):
            kalshi_taker_fee(-1, Decimal("0.50"))

    def test_invalid_price_zero(self) -> None:
        with pytest.raises(ValueError, match="between 0 and 1"):
            kalshi_taker_fee(1, Decimal("0"))

    def test_invalid_price_one(self) -> None:
        with pytest.raises(ValueError, match="between 0 and 1"):
            kalshi_taker_fee(1, Decimal("1"))

    def test_invalid_price_negative(self) -> None:
        with pytest.raises(ValueError, match="between 0 and 1"):
            kalshi_taker_fee(1, Decimal("-0.50"))

    def test_invalid_price_above_one(self) -> None:
        with pytest.raises(ValueError, match="between 0 and 1"):
            kalshi_taker_fee(1, Decimal("1.50"))


# ---------------------------------------------------------------------------
# expected_value — parameterized with hand-computed values
# ---------------------------------------------------------------------------


class TestExpectedValue:
    @pytest.mark.parametrize(
        "model_prob,cost,fee,expected_ev",
        [
            # Perfect prediction: prob=1.0, cost=0.50, fee=0.02
            # EV = 1.00 - 0.50 - 0.02 = 0.48
            (Decimal("1.00"), Decimal("0.50"), Decimal("0.02"), Decimal("0.48")),
            # Edge case: prob=0, guaranteed loss
            # EV = 0.00 - 0.50 - 0.02 = -0.52
            (Decimal("0.00"), Decimal("0.50"), Decimal("0.02"), Decimal("-0.52")),
            # Realistic scenario: prob=0.70, cost=0.54, fee=0.02
            # EV = 0.70 - 0.54 - 0.02 = 0.14
            (Decimal("0.70"), Decimal("0.54"), Decimal("0.02"), Decimal("0.14")),
            # Break-even: prob=0.56, cost=0.54, fee=0.02
            # EV = 0.56 - 0.54 - 0.02 = 0.00
            (Decimal("0.56"), Decimal("0.54"), Decimal("0.02"), Decimal("0.00")),
            # Negative EV (bad trade):
            # EV = 0.40 - 0.54 - 0.02 = -0.16
            (Decimal("0.40"), Decimal("0.54"), Decimal("0.02"), Decimal("-0.16")),
            # Zero cost, zero fee:
            # EV = 0.50 - 0 - 0 = 0.50
            (Decimal("0.50"), Decimal("0"), Decimal("0"), Decimal("0.50")),
        ],
        ids=[
            "perfect_prediction",
            "zero_probability",
            "positive_ev",
            "break_even",
            "negative_ev",
            "zero_cost_fee",
        ],
    )
    def test_expected_value(
        self,
        model_prob: Decimal,
        cost: Decimal,
        fee: Decimal,
        expected_ev: Decimal,
    ) -> None:
        result = expected_value(model_prob, cost, fee)
        assert result == expected_ev

    def test_invalid_prob_negative(self) -> None:
        with pytest.raises(ValueError, match="model_prob"):
            expected_value(Decimal("-0.1"), Decimal("0.50"), Decimal("0.02"))

    def test_invalid_prob_above_one(self) -> None:
        with pytest.raises(ValueError, match="model_prob"):
            expected_value(Decimal("1.1"), Decimal("0.50"), Decimal("0.02"))

    def test_invalid_negative_cost(self) -> None:
        with pytest.raises(ValueError, match="cost"):
            expected_value(Decimal("0.50"), Decimal("-0.10"), Decimal("0.02"))

    def test_invalid_negative_fee(self) -> None:
        with pytest.raises(ValueError, match="fee"):
            expected_value(Decimal("0.50"), Decimal("0.50"), Decimal("-0.01"))
