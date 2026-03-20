"""Tests for prediction model layer (models/)."""

from __future__ import annotations

from decimal import Decimal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from shared.config.errors import PredictionError

from src.config import PredictionConfig
from src.models.equal_weight import EqualWeightModel
from src.models.performance_weighted import PerformanceWeightedModel
from src.models.selector import select_model


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _config(**overrides: object) -> PredictionConfig:
    """Build a PredictionConfig with optional overrides."""
    defaults = {
        "min_sources_required": 2,
        "std_dev_floor": Decimal("1.50"),
    }
    defaults.update(overrides)
    return PredictionConfig(**defaults)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# EqualWeightModel
# ---------------------------------------------------------------------------


class TestEqualWeightModel:
    def test_version(self) -> None:
        assert EqualWeightModel().version == "tier1_equal_weight_v1"

    def test_two_sources_high(self) -> None:
        model = EqualWeightModel()
        config = _config()
        mean, std = model.predict(
            [Decimal("70"), Decimal("74")], config
        )
        assert mean == Decimal("72")
        assert std >= config.std_dev_floor

    def test_four_sources(self) -> None:
        model = EqualWeightModel()
        config = _config()
        temps = [Decimal("68"), Decimal("70"), Decimal("72"), Decimal("74")]
        mean, std = model.predict(temps, config)
        expected_mean = Decimal("71")
        assert mean == expected_mean
        assert std >= config.std_dev_floor

    def test_single_source_with_min_1(self) -> None:
        model = EqualWeightModel()
        config = _config(min_sources_required=1)
        mean, std = model.predict([Decimal("65")], config)
        assert mean == Decimal("65")
        assert std == config.std_dev_floor

    def test_identical_temps_uses_floor(self) -> None:
        model = EqualWeightModel()
        config = _config()
        temps = [Decimal("70"), Decimal("70"), Decimal("70")]
        mean, std = model.predict(temps, config)
        assert mean == Decimal("70")
        assert std == config.std_dev_floor

    def test_negative_temps(self) -> None:
        model = EqualWeightModel()
        config = _config()
        temps = [Decimal("-5"), Decimal("-3")]
        mean, std = model.predict(temps, config)
        assert mean == Decimal("-4")
        assert std >= config.std_dev_floor

    def test_extreme_spread(self) -> None:
        model = EqualWeightModel()
        config = _config()
        temps = [Decimal("30"), Decimal("100")]
        mean, std = model.predict(temps, config)
        assert mean == Decimal("65")
        assert std > config.std_dev_floor

    def test_below_min_sources_raises(self) -> None:
        model = EqualWeightModel()
        config = _config(min_sources_required=3)
        with pytest.raises(PredictionError, match="Need >= 3"):
            model.predict([Decimal("70"), Decimal("72")], config)

    def test_empty_temps_raises(self) -> None:
        model = EqualWeightModel()
        config = _config(min_sources_required=1)
        with pytest.raises(PredictionError):
            model.predict([], config)

    @pytest.mark.parametrize(
        "temps",
        [
            [Decimal("50"), Decimal("60")],
            [Decimal("80"), Decimal("80"), Decimal("80")],
            [Decimal("-10"), Decimal("10"), Decimal("30"), Decimal("50")],
        ],
    )
    def test_mean_between_min_and_max(self, temps: list[Decimal]) -> None:
        model = EqualWeightModel()
        config = _config()
        mean, _std = model.predict(temps, config)
        assert min(temps) <= mean <= max(temps)

    @pytest.mark.parametrize(
        "temps",
        [
            [Decimal("50"), Decimal("60")],
            [Decimal("80"), Decimal("80"), Decimal("80")],
            [Decimal("-10"), Decimal("10"), Decimal("30"), Decimal("50")],
        ],
    )
    def test_std_gte_floor(self, temps: list[Decimal]) -> None:
        model = EqualWeightModel()
        config = _config()
        _mean, std = model.predict(temps, config)
        assert std >= config.std_dev_floor


class TestEqualWeightHypothesis:
    @given(
        temps=st.lists(
            st.decimals(
                min_value=-50,
                max_value=150,
                allow_nan=False,
                allow_infinity=False,
                places=2,
            ),
            min_size=2,
            max_size=6,
        )
    )
    @settings(max_examples=50)
    def test_mean_always_between_min_max(self, temps: list[Decimal]) -> None:
        model = EqualWeightModel()
        config = _config()
        mean, _std = model.predict(temps, config)
        assert min(temps) <= mean <= max(temps)

    @given(
        temps=st.lists(
            st.decimals(
                min_value=-50,
                max_value=150,
                allow_nan=False,
                allow_infinity=False,
                places=2,
            ),
            min_size=2,
            max_size=6,
        )
    )
    @settings(max_examples=50)
    def test_std_always_gte_floor(self, temps: list[Decimal]) -> None:
        model = EqualWeightModel()
        config = _config()
        _mean, std = model.predict(temps, config)
        assert std >= config.std_dev_floor


# ---------------------------------------------------------------------------
# PerformanceWeightedModel (stub)
# ---------------------------------------------------------------------------


class TestPerformanceWeightedModel:
    def test_version(self) -> None:
        assert PerformanceWeightedModel().version == "tier2_performance_weighted_v1"

    def test_predict_raises_not_implemented(self) -> None:
        model = PerformanceWeightedModel()
        config = _config()
        with pytest.raises(PredictionError, match="not yet implemented"):
            model.predict([Decimal("70"), Decimal("72")], config)


# ---------------------------------------------------------------------------
# ModelSelector
# ---------------------------------------------------------------------------


class TestModelSelector:
    def test_default_returns_equal_weight(self) -> None:
        model = select_model()
        assert isinstance(model, EqualWeightModel)
        assert model.version == "tier1_equal_weight_v1"

    def test_none_settlement_count_returns_tier1(self) -> None:
        model = select_model(settlement_count=None)
        assert isinstance(model, EqualWeightModel)

    def test_low_settlement_count_returns_tier1(self) -> None:
        model = select_model(settlement_count=5)
        assert isinstance(model, EqualWeightModel)

    def test_zero_settlement_count_returns_tier1(self) -> None:
        model = select_model(settlement_count=0)
        assert isinstance(model, EqualWeightModel)
