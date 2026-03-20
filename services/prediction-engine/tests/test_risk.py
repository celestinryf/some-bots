"""
Tests for the 6-factor risk scoring system.

Covers:
    - Each individual factor function with boundary inputs
    - Weighted combination via compute_risk_score
    - Clamping and rounding behavior
    - Key mismatches between factors and weights
"""

from datetime import date
from decimal import Decimal

import pytest
from src.engine.risk import (
    bracket_edge_score,
    city_accuracy_score,
    compute_risk_score,
    forecast_spread_score,
    lead_time_score,
    liquidity_score,
    source_agreement_score,
)

# ---------------------------------------------------------------------------
# forecast_spread_score
# ---------------------------------------------------------------------------


class TestForecastSpreadScore:
    @pytest.mark.parametrize(
        "temps,expected",
        [
            # All sources identical → 1
            ([Decimal("70"), Decimal("70"), Decimal("70")], Decimal("1")),
            # 1°F spread → 1
            ([Decimal("70"), Decimal("71")], Decimal("1")),
            # 2°F spread → 3
            ([Decimal("69"), Decimal("71")], Decimal("3")),
            # 3°F spread → 4
            ([Decimal("68"), Decimal("71")], Decimal("4")),
            # 5°F spread → 6 (HIGH SPREAD flag threshold)
            ([Decimal("67"), Decimal("72")], Decimal("6")),
            # 7°F spread → 8
            ([Decimal("66"), Decimal("73")], Decimal("8")),
            # 10°F spread → 10
            ([Decimal("65"), Decimal("75")], Decimal("10")),
        ],
        ids=[
            "identical",
            "1f_spread",
            "2f_spread",
            "3f_spread",
            "5f_spread_high_flag",
            "7f_spread",
            "10f_spread",
        ],
    )
    def test_spread_score(self, temps: list[Decimal], expected: Decimal) -> None:
        assert forecast_spread_score(temps) == expected

    def test_single_source_returns_min(self) -> None:
        assert forecast_spread_score([Decimal("70")]) == Decimal("1")

    def test_empty_list_returns_min(self) -> None:
        assert forecast_spread_score([]) == Decimal("1")

    def test_negative_temps(self) -> None:
        # Spread = 4°F → score 6 (3-5°F range)
        result = forecast_spread_score([Decimal("-8"), Decimal("-4")])
        assert result == Decimal("6")

    def test_nan_temp_raises(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            forecast_spread_score([Decimal("70"), Decimal("NaN")])


# ---------------------------------------------------------------------------
# source_agreement_score
# ---------------------------------------------------------------------------


class TestSourceAgreementScore:
    def test_all_in_bracket(self) -> None:
        temps = [Decimal("70"), Decimal("70.5"), Decimal("71"), Decimal("71.5")]
        assert source_agreement_score(temps, Decimal("70"), Decimal("72")) == Decimal("1")

    def test_three_of_four(self) -> None:
        temps = [Decimal("70"), Decimal("70.5"), Decimal("71"), Decimal("73")]
        assert source_agreement_score(temps, Decimal("70"), Decimal("72")) == Decimal("3")

    def test_half_agree(self) -> None:
        temps = [Decimal("70"), Decimal("71"), Decimal("74"), Decimal("75")]
        assert source_agreement_score(temps, Decimal("70"), Decimal("72")) == Decimal("5")

    def test_one_of_four(self) -> None:
        temps = [Decimal("71"), Decimal("74"), Decimal("75"), Decimal("76")]
        assert source_agreement_score(temps, Decimal("70"), Decimal("72")) == Decimal("8")

    def test_none_agree(self) -> None:
        temps = [Decimal("60"), Decimal("61"), Decimal("62"), Decimal("63")]
        assert source_agreement_score(temps, Decimal("70"), Decimal("72")) == Decimal("10")

    def test_empty_temps(self) -> None:
        assert source_agreement_score([], Decimal("70"), Decimal("72")) == Decimal("10")

    def test_lower_edge_bracket(self) -> None:
        # lower edge: (-inf, 65). All temps below 65 → all agree
        temps = [Decimal("60"), Decimal("62"), Decimal("64")]
        assert source_agreement_score(temps, None, Decimal("65")) == Decimal("1")

    def test_upper_edge_bracket(self) -> None:
        # upper edge: [75, inf). All temps >= 75 → all agree
        temps = [Decimal("76"), Decimal("77"), Decimal("80")]
        assert source_agreement_score(temps, Decimal("75"), None) == Decimal("1")

    def test_nan_temp_raises(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            source_agreement_score(
                [Decimal("NaN")], Decimal("65"), Decimal("70")
            )

    def test_nan_bracket_bound_raises(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            source_agreement_score(
                [Decimal("67")], Decimal("NaN"), Decimal("70")
            )


# ---------------------------------------------------------------------------
# city_accuracy_score
# ---------------------------------------------------------------------------


class TestCityAccuracyScore:
    @pytest.mark.parametrize(
        "accuracy,expected",
        [
            (Decimal("0.85"), Decimal("1")),
            (Decimal("0.80"), Decimal("1")),
            (Decimal("0.75"), Decimal("3")),
            (Decimal("0.70"), Decimal("3")),
            (Decimal("0.65"), Decimal("5")),
            (Decimal("0.55"), Decimal("7")),
            (Decimal("0.45"), Decimal("10")),
            (None, Decimal("5")),  # neutral when no data
        ],
        ids=["85pct", "80pct", "75pct", "70pct", "65pct", "55pct", "45pct", "no_data"],
    )
    def test_accuracy_score(self, accuracy: Decimal | None, expected: Decimal) -> None:
        assert city_accuracy_score(accuracy) == expected

    def test_nan_accuracy_raises(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            city_accuracy_score(Decimal("NaN"))

    @pytest.mark.parametrize(
        "accuracy",
        [Decimal("-0.1"), Decimal("1.1"), Decimal("1.5")],
        ids=["negative", "above_one", "well_above_one"],
    )
    def test_out_of_range_accuracy_raises(self, accuracy: Decimal) -> None:
        with pytest.raises(ValueError):
            city_accuracy_score(accuracy)


# ---------------------------------------------------------------------------
# liquidity_score
# ---------------------------------------------------------------------------


class TestLiquidityScore:
    @pytest.mark.parametrize(
        "volume,expected",
        [
            (200, Decimal("1")),
            (101, Decimal("1")),
            (100, Decimal("3")),
            (51, Decimal("3")),
            (50, Decimal("5")),
            (21, Decimal("5")),
            (20, Decimal("7")),
            (11, Decimal("7")),
            (10, Decimal("10")),
            (0, Decimal("10")),
        ],
        ids=["200", "101", "100", "51", "50", "21", "20", "11", "10", "0"],
    )
    def test_liquidity_score(self, volume: int, expected: Decimal) -> None:
        assert liquidity_score(volume) == expected

    def test_bool_volume_raises(self) -> None:
        with pytest.raises(TypeError, match="int"):
            liquidity_score(True)  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "volume",
        [10.0, Decimal("10"), "10"],
        ids=["float", "decimal", "str"],
    )
    def test_non_int_volume_raises(self, volume: object) -> None:
        with pytest.raises(TypeError, match="int"):
            liquidity_score(volume)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# bracket_edge_score
# ---------------------------------------------------------------------------


class TestBracketEdgeScore:
    def test_center_of_bracket(self) -> None:
        # Predicted 70.5, bracket [69, 72] → distance to nearest edge = 1.5
        assert bracket_edge_score(Decimal("70.5"), Decimal("69"), Decimal("72")) == Decimal("3")

    def test_far_from_edge(self) -> None:
        # Predicted 70, bracket [65, 75] → distance = 5
        assert bracket_edge_score(Decimal("70"), Decimal("65"), Decimal("75")) == Decimal("1")

    def test_near_lower_edge(self) -> None:
        # Predicted 69.3, bracket [69, 72] → distance to lower = 0.3
        assert bracket_edge_score(Decimal("69.3"), Decimal("69"), Decimal("72")) == Decimal("9")

    def test_near_upper_edge(self) -> None:
        # Predicted 71.7, bracket [69, 72] → distance to upper = 0.3
        assert bracket_edge_score(Decimal("71.7"), Decimal("69"), Decimal("72")) == Decimal("9")

    def test_exactly_on_edge(self) -> None:
        # Predicted 69, bracket [69, 72] → distance = 0
        assert bracket_edge_score(Decimal("69"), Decimal("69"), Decimal("72")) == Decimal("9")

    def test_lower_edge_bracket(self) -> None:
        # lower edge: (-inf, 65). Predicted 63 → distance to 65 = 2, score 3 (>1.0)
        assert bracket_edge_score(Decimal("63"), None, Decimal("65")) == Decimal("3")

    def test_upper_edge_bracket(self) -> None:
        # upper edge: [75, inf). Predicted 75.3 → distance to 75 = 0.3
        assert bracket_edge_score(Decimal("75.3"), Decimal("75"), None) == Decimal("9")

    def test_both_none_returns_min(self) -> None:
        assert bracket_edge_score(Decimal("70"), None, None) == Decimal("1")

    @pytest.mark.parametrize(
        "predicted_temp, bracket_low, bracket_high",
        [
            (Decimal("NaN"), Decimal("69"), Decimal("72")),
            (Decimal("70"), Decimal("Infinity"), Decimal("72")),
            (Decimal("70"), Decimal("69"), Decimal("-Infinity")),
        ],
        ids=["predicted_nan", "low_inf", "high_neg_inf"],
    )
    def test_non_finite_inputs_raise_value_error(
        self,
        predicted_temp: Decimal,
        bracket_low: Decimal | None,
        bracket_high: Decimal | None,
    ) -> None:
        with pytest.raises(ValueError, match="finite"):
            bracket_edge_score(predicted_temp, bracket_low, bracket_high)


# ---------------------------------------------------------------------------
# liquidity_score — negative volume
# ---------------------------------------------------------------------------


class TestLiquidityScoreValidation:
    def test_negative_volume_raises(self) -> None:
        with pytest.raises(ValueError, match="non-negative"):
            liquidity_score(-1)


# ---------------------------------------------------------------------------
# lead_time_score
# ---------------------------------------------------------------------------


class TestLeadTimeScore:
    def test_same_day(self) -> None:
        assert lead_time_score(date(2026, 3, 19), date(2026, 3, 19)) == Decimal("1")

    def test_next_day(self) -> None:
        assert lead_time_score(date(2026, 3, 20), date(2026, 3, 19)) == Decimal("2")

    def test_two_days(self) -> None:
        assert lead_time_score(date(2026, 3, 21), date(2026, 3, 19)) == Decimal("5")

    def test_three_plus_days(self) -> None:
        assert lead_time_score(date(2026, 3, 22), date(2026, 3, 19)) == Decimal("8")

    def test_past_date_raises(self) -> None:
        with pytest.raises(ValueError, match="before current_date"):
            lead_time_score(date(2026, 3, 18), date(2026, 3, 19))


# ---------------------------------------------------------------------------
# compute_risk_score — weighted combination
# ---------------------------------------------------------------------------


class TestComputeRiskScore:
    def _default_weights(self) -> dict[str, Decimal]:
        return {
            "forecast_spread": Decimal("0.25"),
            "source_agreement": Decimal("0.20"),
            "city_accuracy": Decimal("0.15"),
            "liquidity": Decimal("0.10"),
            "bracket_edge": Decimal("0.15"),
            "lead_time": Decimal("0.15"),
        }

    def test_all_low_risk(self) -> None:
        factors = {
            "forecast_spread": Decimal("1"),
            "source_agreement": Decimal("1"),
            "city_accuracy": Decimal("1"),
            "liquidity": Decimal("1"),
            "bracket_edge": Decimal("1"),
            "lead_time": Decimal("1"),
        }
        result = compute_risk_score(factors, self._default_weights())
        assert result == Decimal("1.0")

    def test_all_high_risk(self) -> None:
        factors = {
            "forecast_spread": Decimal("10"),
            "source_agreement": Decimal("10"),
            "city_accuracy": Decimal("10"),
            "liquidity": Decimal("10"),
            "bracket_edge": Decimal("10"),
            "lead_time": Decimal("10"),
        }
        result = compute_risk_score(factors, self._default_weights())
        assert result == Decimal("10.0")

    def test_mixed_risk(self) -> None:
        factors = {
            "forecast_spread": Decimal("3"),   # 3 * 0.25 = 0.75
            "source_agreement": Decimal("3"),  # 3 * 0.20 = 0.60
            "city_accuracy": Decimal("5"),     # 5 * 0.15 = 0.75
            "liquidity": Decimal("1"),         # 1 * 0.10 = 0.10
            "bracket_edge": Decimal("6"),      # 6 * 0.15 = 0.90
            "lead_time": Decimal("2"),         # 2 * 0.15 = 0.30
        }
        # Total: 0.75 + 0.60 + 0.75 + 0.10 + 0.90 + 0.30 = 3.40
        result = compute_risk_score(factors, self._default_weights())
        assert result == Decimal("3.4")

    def test_result_is_quantized_to_one_decimal(self) -> None:
        factors = {
            "forecast_spread": Decimal("3"),
            "source_agreement": Decimal("4"),
            "city_accuracy": Decimal("5"),
            "liquidity": Decimal("3"),
            "bracket_edge": Decimal("6"),
            "lead_time": Decimal("2"),
        }
        result = compute_risk_score(factors, self._default_weights())
        # Verify it has at most 1 decimal place
        assert result == result.quantize(Decimal("0.1"))

    def test_missing_factor_raises(self) -> None:
        factors = {
            "forecast_spread": Decimal("3"),
            # missing other factors
        }
        with pytest.raises(ValueError, match="mismatch"):
            compute_risk_score(factors, self._default_weights())

    def test_extra_factor_raises(self) -> None:
        factors = {
            "forecast_spread": Decimal("3"),
            "source_agreement": Decimal("3"),
            "city_accuracy": Decimal("5"),
            "liquidity": Decimal("1"),
            "bracket_edge": Decimal("6"),
            "lead_time": Decimal("2"),
            "extra_factor": Decimal("5"),
        }
        with pytest.raises(ValueError, match="mismatch"):
            compute_risk_score(factors, self._default_weights())

    def test_factor_score_below_min_raises(self) -> None:
        factors = {
            "forecast_spread": Decimal("0"),  # below minimum of 1
            "source_agreement": Decimal("3"),
            "city_accuracy": Decimal("5"),
            "liquidity": Decimal("1"),
            "bracket_edge": Decimal("6"),
            "lead_time": Decimal("2"),
        }
        with pytest.raises(ValueError, match="forecast_spread"):
            compute_risk_score(factors, self._default_weights())

    def test_factor_score_above_max_raises(self) -> None:
        factors = {
            "forecast_spread": Decimal("3"),
            "source_agreement": Decimal("3"),
            "city_accuracy": Decimal("5"),
            "liquidity": Decimal("100"),  # above maximum of 10
            "bracket_edge": Decimal("6"),
            "lead_time": Decimal("2"),
        }
        with pytest.raises(ValueError, match="liquidity"):
            compute_risk_score(factors, self._default_weights())

    def test_weights_not_summing_to_one_raises(self) -> None:
        factors = {
            "forecast_spread": Decimal("3"),
            "source_agreement": Decimal("3"),
            "city_accuracy": Decimal("5"),
            "liquidity": Decimal("1"),
            "bracket_edge": Decimal("6"),
            "lead_time": Decimal("2"),
        }
        bad_weights = {k: Decimal("0.10") for k in factors}  # sum = 0.60
        with pytest.raises(ValueError, match="sum to 1.0"):
            compute_risk_score(factors, bad_weights)

    def test_negative_weight_raises(self) -> None:
        factors = {
            "forecast_spread": Decimal("3"),
            "source_agreement": Decimal("3"),
            "city_accuracy": Decimal("5"),
            "liquidity": Decimal("1"),
            "bracket_edge": Decimal("6"),
            "lead_time": Decimal("2"),
        }
        bad_weights = self._default_weights()
        bad_weights["forecast_spread"] = Decimal("-0.05")
        with pytest.raises(ValueError, match="non-negative"):
            compute_risk_score(factors, bad_weights)

    def test_non_integer_factor_scores(self) -> None:
        factors = {
            "forecast_spread": Decimal("2.5"),
            "source_agreement": Decimal("3.7"),
            "city_accuracy": Decimal("5.0"),
            "liquidity": Decimal("1.2"),
            "bracket_edge": Decimal("6.8"),
            "lead_time": Decimal("2.3"),
        }
        # 2.5*0.25 + 3.7*0.20 + 5.0*0.15 + 1.2*0.10 + 6.8*0.15 + 2.3*0.15
        # = 0.625 + 0.740 + 0.750 + 0.120 + 1.020 + 0.345 = 3.600
        result = compute_risk_score(factors, self._default_weights())
        assert result == Decimal("3.6")
