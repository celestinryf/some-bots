"""
Tests for PredictionConfig validation and loading.
"""

from decimal import Decimal

import pytest

from src.config import PredictionConfig


class TestPredictionConfig:
    def test_defaults_are_valid(self) -> None:
        config = PredictionConfig()
        assert config.gap_threshold == Decimal("0.20")
        assert config.min_ev_threshold == Decimal("0.08")
        assert config.min_sources_required == 2
        assert config.std_dev_floor == Decimal("1.50")

    def test_risk_weights_sum_to_one(self) -> None:
        config = PredictionConfig()
        weight_sum = sum(config.risk_weights.values())
        assert weight_sum == Decimal("1.00")

    def test_risk_weights_property(self) -> None:
        config = PredictionConfig()
        weights = config.risk_weights
        assert len(weights) == 6
        assert "forecast_spread" in weights
        assert "source_agreement" in weights
        assert "city_accuracy" in weights
        assert "liquidity" in weights
        assert "bracket_edge" in weights
        assert "lead_time" in weights

    def test_invalid_risk_weights_not_summing_to_one(self) -> None:
        with pytest.raises(ValueError, match="sum to 1.0"):
            PredictionConfig(risk_weight_forecast_spread=Decimal("0.50"))

    def test_invalid_gap_threshold_zero(self) -> None:
        with pytest.raises(ValueError, match="gap_threshold must be positive"):
            PredictionConfig(gap_threshold=Decimal("0"))

    def test_invalid_gap_threshold_negative(self) -> None:
        with pytest.raises(ValueError, match="gap_threshold must be positive"):
            PredictionConfig(gap_threshold=Decimal("-0.10"))

    def test_invalid_min_ev_threshold_zero(self) -> None:
        with pytest.raises(ValueError, match="min_ev_threshold must be positive"):
            PredictionConfig(min_ev_threshold=Decimal("0"))

    def test_invalid_min_sources_zero(self) -> None:
        with pytest.raises(ValueError, match="min_sources_required must be >= 1"):
            PredictionConfig(min_sources_required=0)

    def test_invalid_std_dev_floor_zero(self) -> None:
        with pytest.raises(ValueError, match="std_dev_floor must be positive"):
            PredictionConfig(std_dev_floor=Decimal("0"))

    def test_invalid_probability_sum_tolerance_zero(self) -> None:
        with pytest.raises(ValueError, match="probability_sum_tolerance must be positive"):
            PredictionConfig(probability_sum_tolerance=Decimal("0"))

    def test_frozen(self) -> None:
        config = PredictionConfig()
        with pytest.raises(AttributeError):
            config.gap_threshold = Decimal("0.15")  # type: ignore[misc]

    def test_custom_values(self) -> None:
        config = PredictionConfig(
            gap_threshold=Decimal("0.15"),
            min_ev_threshold=Decimal("0.05"),
            std_dev_floor=Decimal("2.00"),
        )
        assert config.gap_threshold == Decimal("0.15")
        assert config.min_ev_threshold == Decimal("0.05")
        assert config.std_dev_floor == Decimal("2.00")
