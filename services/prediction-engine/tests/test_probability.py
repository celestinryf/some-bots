"""
Tests for probability math functions.

Covers:
    - Ensemble mean and std computation
    - Gaussian CDF bracket probability mapping
    - Edge brackets, boundary conditions, negative temps
    - Probability sum verification
    - Full distribution builder
    - Hypothesis property-based invariant checks
"""

import math
from decimal import Decimal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from scipy.stats import norm

from shared.config.errors import PredictionError, ValidationError
from src.engine.probability import (
    BracketDef,
    bracket_probability,
    build_probability_distribution,
    compute_ensemble_mean,
    compute_ensemble_std,
    map_brackets,
    verify_probability_sum,
)


# ---------------------------------------------------------------------------
# BracketDef
# ---------------------------------------------------------------------------


class TestBracketDef:
    def test_normal_bracket_key(self) -> None:
        b = BracketDef(low=Decimal("65"), high=Decimal("66"), market_id="m1")
        assert b.key == "[65, 66)"

    def test_lower_edge_bracket_key(self) -> None:
        b = BracketDef(low=None, high=Decimal("65"), market_id="m1")
        assert b.key == "(-inf, 65)"

    def test_upper_edge_bracket_key(self) -> None:
        b = BracketDef(low=Decimal("75"), high=None, market_id="m1")
        assert b.key == "[75, inf)"

    def test_decimal_key_preserves_precision(self) -> None:
        b = BracketDef(low=Decimal("65.10"), high=Decimal("66.20"), market_id="m1")
        assert b.key == "[65.10, 66.20)"

    def test_invalid_bracket_low_ge_high(self) -> None:
        with pytest.raises(ValidationError, match="must be less than"):
            BracketDef(low=Decimal("70"), high=Decimal("70"), market_id="m1")

    def test_invalid_bracket_low_gt_high(self) -> None:
        with pytest.raises(ValidationError, match="must be less than"):
            BracketDef(low=Decimal("72"), high=Decimal("70"), market_id="m1")

    def test_invalid_both_none(self) -> None:
        with pytest.raises(ValidationError, match="at least one bound"):
            BracketDef(low=None, high=None, market_id="m1")

    def test_invalid_empty_market_id(self) -> None:
        with pytest.raises(ValidationError, match="non-empty"):
            BracketDef(low=Decimal("65"), high=Decimal("66"), market_id="")

    def test_nan_low_raises(self) -> None:
        with pytest.raises(ValidationError, match="finite"):
            BracketDef(low=Decimal("NaN"), high=Decimal("70"), market_id="m1")

    def test_nan_high_raises(self) -> None:
        with pytest.raises(ValidationError, match="finite"):
            BracketDef(low=Decimal("65"), high=Decimal("NaN"), market_id="m1")

    def test_inf_low_raises(self) -> None:
        with pytest.raises(ValidationError, match="finite"):
            BracketDef(low=Decimal("Infinity"), high=Decimal("70"), market_id="m1")

    def test_inf_high_raises(self) -> None:
        with pytest.raises(ValidationError, match="finite"):
            BracketDef(low=Decimal("65"), high=Decimal("Infinity"), market_id="m1")


# ---------------------------------------------------------------------------
# compute_ensemble_mean
# ---------------------------------------------------------------------------


class TestComputeEnsembleMean:
    def test_single_value(self) -> None:
        result = compute_ensemble_mean([Decimal("72")])
        assert result == Decimal("72")

    def test_multiple_values(self) -> None:
        temps = [Decimal("70"), Decimal("72"), Decimal("71"), Decimal("71")]
        result = compute_ensemble_mean(temps)
        assert result == Decimal("71")

    def test_non_integer_mean(self) -> None:
        temps = [Decimal("70"), Decimal("73")]
        result = compute_ensemble_mean(temps)
        assert result == Decimal("71.5")

    def test_negative_temps(self) -> None:
        temps = [Decimal("-5"), Decimal("-3"), Decimal("-7")]
        result = compute_ensemble_mean(temps)
        assert result == Decimal("-5")

    def test_empty_list_raises(self) -> None:
        with pytest.raises(PredictionError, match="empty temperature list"):
            compute_ensemble_mean([])


# ---------------------------------------------------------------------------
# compute_ensemble_std
# ---------------------------------------------------------------------------


class TestComputeEnsembleStd:
    def test_identical_temps_returns_floor(self) -> None:
        temps = [Decimal("70"), Decimal("70"), Decimal("70")]
        result = compute_ensemble_std(temps, floor=Decimal("1.50"))
        assert result == Decimal("1.50")

    def test_single_source_returns_floor(self) -> None:
        result = compute_ensemble_std([Decimal("72")], floor=Decimal("1.50"))
        assert result == Decimal("1.50")

    def test_std_above_floor(self) -> None:
        # [68, 72]: mean=70, sample variance = (4+4)/1 = 8, std = sqrt(8) ≈ 2.83
        temps = [Decimal("68"), Decimal("72")]
        result = compute_ensemble_std(temps, floor=Decimal("1.50"))
        assert float(result) == pytest.approx(2.8284, abs=0.01)

    def test_std_below_floor_uses_floor(self) -> None:
        # [70, 71]: mean=70.5, sample variance = 0.5, std = sqrt(0.5) ≈ 0.71 → floor
        temps = [Decimal("70"), Decimal("71")]
        result = compute_ensemble_std(temps, floor=Decimal("1.50"))
        assert result == Decimal("1.50")

    def test_wide_spread(self) -> None:
        # [60, 80]: mean=70, sample variance = 200, std = sqrt(200) ≈ 14.14
        temps = [Decimal("60"), Decimal("80")]
        result = compute_ensemble_std(temps, floor=Decimal("1.50"))
        assert float(result) == pytest.approx(14.1421, abs=0.01)

    def test_empty_list_raises(self) -> None:
        with pytest.raises(PredictionError, match="empty temperature list"):
            compute_ensemble_std([], floor=Decimal("1.50"))


# ---------------------------------------------------------------------------
# bracket_probability — parameterized tests with known analytical values
# ---------------------------------------------------------------------------


class TestBracketProbability:
    def test_std_zero_raises(self) -> None:
        with pytest.raises(PredictionError, match="positive"):
            bracket_probability(70.0, 0.0, 69.0, 71.0)

    def test_negative_std_raises(self) -> None:
        with pytest.raises(PredictionError, match="positive"):
            bracket_probability(70.0, -1.0, 69.0, 71.0)

    def test_both_none_returns_one(self) -> None:
        assert bracket_probability(70.0, 2.0, None, None) == 1.0

    @pytest.mark.parametrize(
        "mean,std,low,high,expected_approx",
        [
            # Normal bracket centered on mean: [-1σ, +1σ] ≈ 68.27%
            (70.0, 2.0, 68.0, 72.0, 0.6827),
            # Normal bracket: [mean, mean+1σ] ≈ 34.13%
            (70.0, 2.0, 70.0, 72.0, 0.3413),
            # Bracket far from mean: very low probability
            (70.0, 2.0, 80.0, 82.0, 0.0000),
            # Narrow bracket at mean: small but nonzero
            (70.0, 2.0, 69.5, 70.5, 0.1974),
            # Negative temperatures
            (-5.0, 3.0, -8.0, -2.0, 0.6827),
        ],
        ids=[
            "centered_1sigma",
            "half_sigma_right",
            "far_from_mean",
            "narrow_at_mean",
            "negative_temps",
        ],
    )
    def test_normal_bracket(
        self, mean: float, std: float, low: float, high: float, expected_approx: float
    ) -> None:
        result = bracket_probability(mean, std, low, high)
        assert result == pytest.approx(expected_approx, abs=0.001)

    @pytest.mark.parametrize(
        "mean,std,high,expected_approx",
        [
            # Lower edge: P(X < mean) = 50%
            (70.0, 2.0, 70.0, 0.5),
            # Lower edge: P(X < mean - 2σ) ≈ 2.28%
            (70.0, 2.0, 66.0, 0.0228),
            # Lower edge: P(X < mean + 2σ) ≈ 97.72%
            (70.0, 2.0, 74.0, 0.9772),
        ],
        ids=["at_mean", "2sigma_below", "2sigma_above"],
    )
    def test_lower_edge_bracket(
        self, mean: float, std: float, high: float, expected_approx: float
    ) -> None:
        result = bracket_probability(mean, std, None, high)
        assert result == pytest.approx(expected_approx, abs=0.001)

    @pytest.mark.parametrize(
        "mean,std,low,expected_approx",
        [
            # Upper edge: P(X >= mean) = 50%
            (70.0, 2.0, 70.0, 0.5),
            # Upper edge: P(X >= mean + 2σ) ≈ 2.28%
            (70.0, 2.0, 74.0, 0.0228),
            # Upper edge: P(X >= mean - 2σ) ≈ 97.72%
            (70.0, 2.0, 66.0, 0.9772),
        ],
        ids=["at_mean", "2sigma_above", "2sigma_below"],
    )
    def test_upper_edge_bracket(
        self, mean: float, std: float, low: float, expected_approx: float
    ) -> None:
        result = bracket_probability(mean, std, low, None)
        assert result == pytest.approx(expected_approx, abs=0.001)

    def test_mean_on_bracket_boundary(self) -> None:
        """When mean is exactly on a boundary, the bracket starting at mean
        should get approximately 50% minus the upper tail."""
        # mean=70, bracket [70, 71) with std=2
        prob = bracket_probability(70.0, 2.0, 70.0, 71.0)
        expected = norm.cdf(71.0, 70.0, 2.0) - norm.cdf(70.0, 70.0, 2.0)
        assert prob == pytest.approx(expected, abs=0.0001)

    def test_extreme_mean_outside_brackets(self) -> None:
        """When mean=90 but brackets span 60-80, nearly all probability
        should be in the upper edge bracket."""
        prob_below = bracket_probability(90.0, 2.0, None, 80.0)
        prob_above = bracket_probability(90.0, 2.0, 80.0, None)
        assert prob_below < 0.0001
        assert prob_above > 0.9999

    def test_very_wide_std(self) -> None:
        """Wide std = flat distribution, each bracket gets similar probability."""
        # std=100, bracket [69, 71] should be roughly 2/std*sqrt(2pi) ≈ small
        prob = bracket_probability(70.0, 100.0, 69.0, 71.0)
        assert 0.005 < prob < 0.015

    def test_nan_mean_raises(self) -> None:
        with pytest.raises(PredictionError, match="finite"):
            bracket_probability(float("nan"), 2.0, 69.0, 71.0)

    def test_inf_mean_raises(self) -> None:
        with pytest.raises(PredictionError, match="finite"):
            bracket_probability(float("inf"), 2.0, 69.0, 71.0)

    def test_nan_std_raises(self) -> None:
        with pytest.raises(PredictionError, match="finite"):
            bracket_probability(70.0, float("nan"), 69.0, 71.0)

    def test_inf_std_raises(self) -> None:
        with pytest.raises(PredictionError, match="finite"):
            bracket_probability(70.0, float("inf"), 69.0, 71.0)


# ---------------------------------------------------------------------------
# map_brackets
# ---------------------------------------------------------------------------


class TestMapBrackets:
    def test_empty_brackets_returns_empty(self) -> None:
        assert map_brackets(70.0, 2.0, []) == {}

    def test_single_bracket(self) -> None:
        brackets = [BracketDef(Decimal("69"), Decimal("71"), "m1")]
        result = map_brackets(70.0, 2.0, brackets)
        assert len(result) == 1
        assert "[69, 71)" in result
        assert result["[69, 71)"] == pytest.approx(0.3829, abs=0.001)

    def test_complete_bracket_set_sums_to_one(self) -> None:
        """A complete set of brackets (lower edge + normal + upper edge)
        should sum to 1.0."""
        brackets = [
            BracketDef(None, Decimal("68"), "m1"),       # (-inf, 68)
            BracketDef(Decimal("68"), Decimal("70"), "m2"),  # [68, 70)
            BracketDef(Decimal("70"), Decimal("72"), "m3"),  # [70, 72)
            BracketDef(Decimal("72"), None, "m4"),       # [72, inf)
        ]
        result = map_brackets(70.0, 2.0, brackets)
        assert len(result) == 4
        total = sum(result.values())
        assert total == pytest.approx(1.0, abs=0.001)


# ---------------------------------------------------------------------------
# verify_probability_sum
# ---------------------------------------------------------------------------


class TestVerifyProbabilitySum:
    def test_exact_one(self) -> None:
        assert verify_probability_sum({"a": 0.5, "b": 0.5}) is True

    def test_within_tolerance(self) -> None:
        assert verify_probability_sum({"a": 0.505, "b": 0.5}, tolerance=0.01) is True

    def test_outside_tolerance(self) -> None:
        assert verify_probability_sum({"a": 0.6, "b": 0.6}, tolerance=0.01) is False

    def test_empty_dict_returns_false(self) -> None:
        assert verify_probability_sum({}) is False

    def test_empty_dict_with_invalid_tolerance_still_raises(self) -> None:
        with pytest.raises(ValueError, match="tolerance"):
            verify_probability_sum({}, tolerance=-5.0)

    def test_below_one(self) -> None:
        assert verify_probability_sum({"a": 0.4, "b": 0.4}, tolerance=0.01) is False

    def test_invalid_tolerance_zero(self) -> None:
        with pytest.raises(ValueError, match="tolerance"):
            verify_probability_sum({"a": 0.5, "b": 0.5}, tolerance=0.0)

    def test_invalid_tolerance_negative(self) -> None:
        with pytest.raises(ValueError, match="tolerance"):
            verify_probability_sum({"a": 0.5, "b": 0.5}, tolerance=-0.01)

    def test_invalid_tolerance_ge_one(self) -> None:
        with pytest.raises(ValueError, match="tolerance"):
            verify_probability_sum({"a": 0.5, "b": 0.5}, tolerance=1.0)

    @pytest.mark.parametrize(
        "tolerance",
        [
            Decimal("NaN"),
            float("nan"),
            Decimal("Infinity"),
            float("inf"),
            float("-inf"),
        ],
        ids=[
            "decimal_nan",
            "float_nan",
            "decimal_inf",
            "float_inf",
            "float_neg_inf",
        ],
    )
    def test_non_finite_tolerance_raises(self, tolerance: Decimal | float) -> None:
        with pytest.raises(ValueError, match="tolerance"):
            verify_probability_sum({"a": 0.5, "b": 0.5}, tolerance=tolerance)


# ---------------------------------------------------------------------------
# build_probability_distribution
# ---------------------------------------------------------------------------


class TestBuildProbabilityDistribution:
    def _make_brackets(self) -> list[BracketDef]:
        """Create a realistic bracket set for testing."""
        return [
            BracketDef(None, Decimal("68"), "m1"),
            BracketDef(Decimal("68"), Decimal("70"), "m2"),
            BracketDef(Decimal("70"), Decimal("72"), "m3"),
            BracketDef(Decimal("72"), None, "m4"),
        ]

    def test_non_finite_temps_raises(self) -> None:
        brackets = self._make_brackets()
        with pytest.raises(PredictionError, match="non-finite"):
            build_probability_distribution(
                temps=[Decimal("70"), Decimal("inf")],
                brackets=brackets,
                source_temps={"NWS": Decimal("70"), "BAD": Decimal("inf")},
                std_dev_floor=Decimal("1.50"),
            )

    def test_nan_temps_raises(self) -> None:
        brackets = self._make_brackets()
        with pytest.raises(PredictionError, match="non-finite"):
            build_probability_distribution(
                temps=[Decimal("70"), Decimal("nan")],
                brackets=brackets,
                source_temps={"NWS": Decimal("70"), "BAD": Decimal("nan")},
                std_dev_floor=Decimal("1.50"),
            )

    def test_happy_path(self) -> None:
        temps = [Decimal("70"), Decimal("72"), Decimal("71"), Decimal("69")]
        source_temps = {"NWS": Decimal("70"), "VC": Decimal("72"),
                        "PW": Decimal("71"), "OWM": Decimal("69")}
        brackets = self._make_brackets()

        result = build_probability_distribution(
            temps=temps,
            brackets=brackets,
            source_temps=source_temps,
            std_dev_floor=Decimal("1.50"),
        )

        assert "brackets" in result
        assert "mean" in result
        assert "std_dev" in result
        assert "source_temps" in result
        assert "sum_check" in result

        assert result["mean"] == pytest.approx(70.5, abs=0.01)
        assert result["std_dev"] > 0
        assert abs(result["sum_check"] - 1.0) < 0.01
        assert len(result["brackets"]) == 4

    def test_single_source_uses_floor(self) -> None:
        temps = [Decimal("70")]
        source_temps = {"NWS": Decimal("70")}
        brackets = self._make_brackets()

        result = build_probability_distribution(
            temps=temps,
            brackets=brackets,
            source_temps=source_temps,
            std_dev_floor=Decimal("1.50"),
        )

        assert result["std_dev"] == pytest.approx(1.5, abs=0.01)

    def test_empty_brackets_no_error(self) -> None:
        temps = [Decimal("70"), Decimal("72")]
        result = build_probability_distribution(
            temps=temps,
            brackets=[],
            source_temps={"NWS": Decimal("70"), "VC": Decimal("72")},
            std_dev_floor=Decimal("1.50"),
        )
        assert result["brackets"] == {}
        assert result["sum_check"] == 0.0

    def test_bad_probability_sum_raises(self) -> None:
        """When brackets don't cover the full range, sum deviates from 1.0."""
        temps = [Decimal("70"), Decimal("72")]
        # Only one bracket — won't sum to 1.0
        brackets = [BracketDef(Decimal("69"), Decimal("71"), "m1")]
        source_temps = {"NWS": Decimal("70"), "VC": Decimal("72")}

        with pytest.raises(PredictionError, match="sum to"):
            build_probability_distribution(
                temps=temps,
                brackets=brackets,
                source_temps=source_temps,
                std_dev_floor=Decimal("1.50"),
                probability_sum_tolerance=0.001,
            )

    def test_non_finite_source_temp_raises(self) -> None:
        temps = [Decimal("70"), Decimal("72")]
        brackets = self._make_brackets()
        with pytest.raises(PredictionError, match="non-finite"):
            build_probability_distribution(
                temps=temps,
                brackets=brackets,
                source_temps={"NWS": Decimal("70"), "BAD": Decimal("inf")},
                std_dev_floor=Decimal("1.50"),
            )

    def test_source_temps_in_output(self) -> None:
        temps = [Decimal("70"), Decimal("72")]
        source_temps = {"NWS": Decimal("70"), "OWM": Decimal("72")}
        brackets = self._make_brackets()

        result = build_probability_distribution(
            temps=temps,
            brackets=brackets,
            source_temps=source_temps,
            std_dev_floor=Decimal("1.50"),
        )

        assert result["source_temps"]["NWS"] == 70.0
        assert result["source_temps"]["OWM"] == 72.0


# ---------------------------------------------------------------------------
# Hypothesis property-based tests
# ---------------------------------------------------------------------------


class TestProbabilityProperties:
    @given(
        mean=st.floats(min_value=-50, max_value=150),
        std=st.floats(min_value=0.1, max_value=50),
        low=st.floats(min_value=-60, max_value=140),
        width=st.floats(min_value=0.1, max_value=20),
    )
    @settings(max_examples=200)
    def test_probability_always_non_negative(
        self, mean: float, std: float, low: float, width: float
    ) -> None:
        """Bracket probability is always >= 0."""
        high = low + width
        prob = bracket_probability(mean, std, low, high)
        assert prob >= 0

    @given(
        mean=st.floats(min_value=-50, max_value=150),
        std=st.floats(min_value=0.1, max_value=50),
        low=st.floats(min_value=-60, max_value=140),
        width=st.floats(min_value=0.1, max_value=20),
    )
    @settings(max_examples=200)
    def test_probability_always_le_one(
        self, mean: float, std: float, low: float, width: float
    ) -> None:
        """Bracket probability is always <= 1."""
        high = low + width
        prob = bracket_probability(mean, std, low, high)
        assert prob <= 1.0 + 1e-10  # tiny float tolerance

    @given(
        mean=st.floats(min_value=-50, max_value=150),
        std=st.floats(min_value=0.1, max_value=50),
    )
    @settings(max_examples=100)
    def test_edge_brackets_sum_to_one(self, mean: float, std: float) -> None:
        """Lower edge + upper edge at same point = 1.0."""
        split = mean  # split at the mean
        lower = bracket_probability(mean, std, None, split)
        upper = bracket_probability(mean, std, split, None)
        assert lower + upper == pytest.approx(1.0, abs=1e-10)

    @given(
        temps=st.lists(
            st.decimals(min_value=-50, max_value=150, places=1, allow_nan=False, allow_infinity=False),
            min_size=2,
            max_size=7,
        ),
    )
    @settings(max_examples=100)
    def test_ensemble_mean_between_extremes(self, temps: list[Decimal]) -> None:
        """Ensemble mean is always between min and max of inputs."""
        mean = compute_ensemble_mean(temps)
        assert mean >= min(temps)
        assert mean <= max(temps)

    @given(
        temps=st.lists(
            st.decimals(min_value=-50, max_value=150, places=1, allow_nan=False, allow_infinity=False),
            min_size=1,
            max_size=7,
        ),
    )
    @settings(max_examples=100)
    def test_ensemble_std_at_least_floor(self, temps: list[Decimal]) -> None:
        """Ensemble std is always >= floor."""
        floor = Decimal("1.50")
        std = compute_ensemble_std(temps, floor)
        assert std >= floor

    @given(
        mean=st.floats(min_value=-20, max_value=120),
        std=st.floats(min_value=0.5, max_value=20),
    )
    @settings(max_examples=100)
    def test_complete_bracket_set_sums_to_one(self, mean: float, std: float) -> None:
        """A full set of contiguous brackets sums to 1.0."""
        base = round(mean - 5)
        brackets = [
            BracketDef(None, Decimal(str(base)), "m0"),
            BracketDef(Decimal(str(base)), Decimal(str(base + 2)), "m1"),
            BracketDef(Decimal(str(base + 2)), Decimal(str(base + 5)), "m2"),
            BracketDef(Decimal(str(base + 5)), Decimal(str(base + 8)), "m3"),
            BracketDef(Decimal(str(base + 8)), Decimal(str(base + 10)), "m4"),
            BracketDef(Decimal(str(base + 10)), None, "m5"),
        ]
        probs = map_brackets(mean, std, brackets)
        total = sum(probs.values())
        assert total == pytest.approx(1.0, abs=0.001)
